// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(feature = "cluster")]

use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

struct Shimmed {
    dir: TempDir,
}

impl Shimmed {
    fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).expect("shim bin dir");
        let log = dir.path().join("calls.log");
        for (name, reply) in [
            ("pgrep", "echo '99999 mqdb agent start'"),
            ("pkill", "true"),
        ] {
            let script = format!(
                "#!/bin/sh\necho \"{name} $*\" >> '{}'\n{reply}\n",
                log.display()
            );
            let path = bin.join(name);
            std::fs::write(&path, script).expect("write shim");
            let status = Command::new("chmod")
                .args(["+x"])
                .arg(&path)
                .status()
                .expect("chmod shim");
            assert!(status.success());
        }
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn run(&self, args: &[&str]) -> Output {
        let path = format!(
            "{}:{}",
            self.dir.path().join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        Command::new(env!("CARGO_BIN_EXE_mqdb"))
            .args(args)
            .current_dir(self.dir.path())
            .env("PATH", path)
            .env_remove("MQDB_BROKER")
            .output()
            .expect("run mqdb")
    }

    fn calls(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("calls.log")).unwrap_or_default()
    }
}

fn free_port_block() -> u16 {
    loop {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind probe");
        let port = listener.local_addr().expect("probe addr").port();
        let transport_free = TcpListener::bind((Ipv4Addr::LOCALHOST, port + 100)).is_ok()
            && std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, port + 100)).is_ok();
        if (1024..60000).contains(&port) && transport_free {
            return port;
        }
    }
}

#[test]
fn profile_with_an_unknown_tool_touches_no_process() {
    let shim = Shimmed::new();
    let db = shim.path().join("db");
    let db_arg = db.to_str().expect("utf-8 path");
    let output = shim.run(&["dev", "profile", "--tool", "bogus", "--db", db_arg, "db"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("Unknown profiling tool: bogus"), "{stderr}");
    assert_eq!(shim.calls(), "", "profile looked up or signalled processes");
    assert!(
        !db.exists(),
        "profile created its db before checking the tool"
    );
}

#[test]
fn profile_refuses_a_port_that_is_already_taken() {
    let shim = Shimmed::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy a port");
    let port = listener.local_addr().expect("addr").port().to_string();
    let db = shim.path().join("db");
    let db_arg = db.to_str().expect("utf-8 path");
    let output = shim.run(&[
        "dev", "profile", "--tool", "sample", "--db", db_arg, "--port", &port, "db",
    ]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains(&format!("port {port} is already in use")),
        "{stderr}"
    );
    assert_eq!(shim.calls(), "");
    drop(listener);
}

#[test]
fn a_suite_cluster_signals_nothing_outside_its_own_nodes() {
    let shim = Shimmed::new();
    let base_port = free_port_block().to_string();
    let prefix = shim.path().join("t");
    let prefix_arg = prefix.to_str().expect("utf-8 path");
    let output = shim.run(&[
        "dev",
        "test",
        "--ownership",
        "--nodes",
        "1",
        "--base-port",
        &base_port,
        "--db-prefix",
        prefix_arg,
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("=== Ownership Enforcement Test"),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!("Starting node 1 on port {base_port}")),
        "{stdout}"
    );
    assert!(stdout.contains("did not accept connections"), "{stdout}");
    assert_eq!(
        shim.calls(),
        "",
        "the suite looked up or signalled processes"
    );
    assert!(!Path::new(&format!("{prefix_arg}-own-passwd")).exists());
    assert!(!Path::new(&format!("{prefix_arg}-own-1/mqdb.pid")).exists());
}

#[test]
fn a_suite_refuses_ports_another_process_holds() {
    let shim = Shimmed::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy a port");
    let base_port = listener.local_addr().expect("addr").port().to_string();
    let prefix = shim.path().join("t");
    let prefix_arg = prefix.to_str().expect("utf-8 path");
    let output = shim.run(&[
        "dev",
        "test",
        "--sharing",
        "--nodes",
        "1",
        "--base-port",
        &base_port,
        "--db-prefix",
        prefix_arg,
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("already in use"), "{stdout}");
    assert!(!stdout.contains("Starting node"), "{stdout}");
    assert!(!Path::new(&format!("{prefix_arg}-share-1")).exists());
    assert_eq!(shim.calls(), "");
    drop(listener);
}
