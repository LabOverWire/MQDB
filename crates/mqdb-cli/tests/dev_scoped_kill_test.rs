// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(feature = "cluster")]

use std::io::Write;
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

use tempfile::TempDir;

fn install_shim(path: &Path, script: &str) {
    let mut writer = Command::new("sh")
        .args(["-c", "cat > \"$1\" && chmod +x \"$1\"", "install-shim"])
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn shim writer");
    writer
        .stdin
        .take()
        .expect("shim writer stdin")
        .write_all(script.as_bytes())
        .expect("write shim");
    assert!(writer.wait().expect("shim writer").success());
}

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
            install_shim(&bin.join(name), &script);
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
            .env_remove("MQDB_LICENSE")
            .env_remove("MQDB_LICENSE_FILE")
            .output()
            .expect("run mqdb")
    }

    fn calls(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("calls.log")).unwrap_or_default()
    }
}

const TRANSPORT_PORT_OFFSET: u16 = 50;

fn free_port_block() -> u16 {
    loop {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind probe");
        let port = listener.local_addr().expect("probe addr").port();
        let Some(transport) = port.checked_add(TRANSPORT_PORT_OFFSET) else {
            continue;
        };
        let transport_free = TcpListener::bind((Ipv4Addr::LOCALHOST, transport)).is_ok()
            && UdpSocket::bind((Ipv4Addr::LOCALHOST, transport)).is_ok();
        if port >= 1024 && transport_free {
            return port;
        }
    }
}

struct StandIn(Child);

impl Drop for StandIn {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn stand_in_node(db: &Path) -> StandIn {
    stand_in(db, "cluster")
}

fn stand_in(db: &Path, mode: &str) -> StandIn {
    StandIn(
        Command::new("sh")
            .args([
                "-c",
                "while :; do sleep 1; done",
                "stand-in",
                "mqdb",
                mode,
                "start",
            ])
            .args(["--node-id", "1", "--db"])
            .arg(db)
            .spawn()
            .expect("spawn a stand-in"),
    )
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
    assert!(stdout.contains("node 1 exited"), "{stdout}");
    assert!(!output.status.success(), "a failed suite exited 0");
    assert_eq!(
        shim.calls(),
        "",
        "the suite looked up or signalled processes"
    );
    assert!(!Path::new(&format!("{prefix_arg}-own-{base_port}-passwd")).exists());
    assert!(Path::new(&format!("{prefix_arg}-own-{base_port}-1")).is_dir());
    assert!(!Path::new(&format!("{prefix_arg}-own-{base_port}-1/mqdb.pid")).exists());
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
    assert!(!output.status.success(), "a refused suite exited 0");
    assert!(stdout.contains("already in use"), "{stdout}");
    assert!(!stdout.contains("Starting node"), "{stdout}");
    assert!(!Path::new(&format!("{prefix_arg}-share-{base_port}-1")).exists());
    assert!(!Path::new(&format!("{prefix_arg}-share-{base_port}-passwd")).exists());
    assert_eq!(shim.calls(), "");
    drop(listener);
}

#[test]
fn a_suite_leaves_another_runs_nodes_under_the_same_prefix_alone() {
    let shim = Shimmed::new();
    let prefix = shim.path().join("t");
    let prefix_arg = prefix.to_str().expect("utf-8 path");
    let other_dir = shim.path().join("t-own-1");
    std::fs::create_dir_all(&other_dir).expect("other run's node dir");
    let mut other = stand_in_node(&other_dir);
    let other_pid = other.0.id().to_string();
    std::fs::write(other_dir.join("mqdb.pid"), &other_pid).expect("other run's pid file");

    let base_port = free_port_block().to_string();
    let other_port = std::iter::repeat_with(free_port_block)
        .find(|port| port.to_string() != base_port)
        .expect("a second free port")
        .to_string();
    let same_layout_dir = shim.path().join(format!("t-own-{other_port}-1"));
    std::fs::create_dir_all(&same_layout_dir).expect("other port's node dir");
    let mut same_layout = stand_in_node(&same_layout_dir);
    let same_layout_pid = same_layout.0.id().to_string();
    std::fs::write(same_layout_dir.join("mqdb.pid"), &same_layout_pid).expect("pid file");

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
        stdout.contains(&format!("Starting node 1 on port {base_port}")),
        "{stdout}"
    );
    assert!(
        other.0.try_wait().expect("poll stand-in").is_none(),
        "the suite stopped another run's node"
    );
    assert!(
        same_layout.0.try_wait().expect("poll stand-in").is_none(),
        "the suite stopped the node of a run on another port"
    );
    assert_eq!(
        std::fs::read_to_string(same_layout_dir.join("mqdb.pid")).expect("pid file"),
        same_layout_pid
    );
    assert_eq!(
        std::fs::read_to_string(other_dir.join("mqdb.pid")).expect("other pid file"),
        other_pid
    );
    assert!(other_dir.is_dir());
    assert_eq!(shim.calls(), "");
}

fn alive(pid: &str) -> bool {
    Command::new("ps")
        .args(["-p", pid, "-o", "stat="])
        .output()
        .is_ok_and(|o| {
            let stat = String::from_utf8_lossy(&o.stdout);
            let stat = stat.trim();
            !stat.is_empty() && !stat.starts_with('Z')
        })
}

fn recorded_pid(path: &Path) -> Option<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if let Ok(pid) = std::fs::read_to_string(path)
            && !pid.trim().is_empty()
        {
            return Some(pid.trim().to_string());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    None
}

#[test]
fn profile_stops_its_profiler_tree_when_the_benchmark_cannot_start() {
    let shim = Shimmed::new();
    let bin = shim.path().join("bin");
    let samply_pid = shim.path().join("samply.pid");
    let helper_pid = shim.path().join("helper.pid");
    for (name, body) in [
        ("cargo", "exit 0".to_string()),
        (
            "samply",
            format!(
                "sleep 8 </dev/null >/dev/null 2>&1 &\necho $! > '{}'\necho $$ > '{}'\nexec sleep 8 </dev/null >/dev/null 2>&1",
                helper_pid.display(),
                samply_pid.display()
            ),
        ),
    ] {
        install_shim(&bin.join(name), &format!("#!/bin/sh\n{body}\n"));
    }
    let profiling = shim.path().join("target/profiling");
    std::fs::create_dir_all(&profiling).expect("profiling dir");
    std::fs::write(profiling.join("mqdb"), "not executable").expect("fake binary");

    let port = free_port_block().to_string();
    let db = shim.path().join("db");
    let db_arg = db.to_str().expect("utf-8 path");
    let output = shim.run(&[
        "dev", "profile", "--tool", "samply", "--db", db_arg, "--port", &port, "--output", "out",
        "db",
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!output.status.success());
    assert!(
        stdout.contains("Starting agent under samply profiler")
            && stdout.contains("Stopping agent to save profile"),
        "the profiler was never started and stopped: {stdout}"
    );
    for (what, pid_file) in [("profiler", &samply_pid), ("profiler's child", &helper_pid)] {
        if let Some(pid) = recorded_pid(pid_file) {
            assert!(
                !alive(&pid),
                "dev profile left its {what} (pid {pid}) running: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    assert_eq!(shim.calls(), "");
}

#[test]
fn suites_that_start_a_cluster_are_not_mixed_with_suites_that_need_one() {
    let shim = Shimmed::new();
    let base_port = free_port_block().to_string();
    let prefix = shim.path().join("t");
    let prefix_arg = prefix.to_str().expect("utf-8 path");
    let output = shim.run(&[
        "dev",
        "test",
        "--db",
        "--ownership",
        "--nodes",
        "1",
        "--base-port",
        &base_port,
        "--db-prefix",
        prefix_arg,
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("cannot run together"), "{stderr}");
    assert!(!stdout.contains("Starting node"), "{stdout}");
    assert!(!stdout.contains("Waiting for cluster"), "{stdout}");
    assert_eq!(shim.calls(), "");
}

#[test]
fn bench_and_profile_refuse_a_db_another_agent_uses() {
    let shim = Shimmed::new();
    let db = shim.path().join("busy-db");
    std::fs::create_dir_all(&db).expect("db dir");
    std::fs::write(db.join("mqdb.log"), "other run's log").expect("log");
    let mut agent = stand_in(&db, "agent");
    let db_arg = db.to_str().expect("utf-8 path");
    let port = free_port_block().to_string();

    for args in [
        vec![
            "dev",
            "bench",
            "--db",
            db_arg,
            "--port",
            &port,
            "db",
            "--operations",
            "1",
        ],
        vec![
            "dev", "profile", "--tool", "sample", "--db", db_arg, "--port", &port, "db",
        ],
    ] {
        let output = shim.run(&args);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{args:?}: {stderr}");
        assert!(
            stderr.contains("another mqdb process uses --db"),
            "{args:?}: {stderr}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(db.join("mqdb.log")).expect("log"),
        "other run's log"
    );
    assert!(agent.0.try_wait().expect("poll stand-in").is_none());
    assert_eq!(shim.calls(), "");
}

#[test]
fn clean_keeps_a_live_node_dir_whose_path_contains_a_double_dash() {
    let shim = Shimmed::new();
    let parent = shim.path().join("odd --dir");
    let node_dir = parent.join("c-1");
    std::fs::create_dir_all(&node_dir).expect("node dir");
    let mut node = stand_in_node(&node_dir);
    let prefix = parent.join("c");
    let prefix_arg = prefix.to_str().expect("utf-8 path");

    let output = shim.run(&["dev", "clean", "--db-prefix", prefix_arg]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stdout}{stderr}");
    assert!(stderr.contains("still use"), "{stdout}{stderr}");
    assert!(node_dir.is_dir(), "dev clean removed a live node's dir");
    assert!(node.0.try_wait().expect("poll stand-in").is_none());
    assert_eq!(shim.calls(), "");
}
