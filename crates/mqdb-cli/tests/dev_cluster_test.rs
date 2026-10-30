// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(feature = "cluster")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use tempfile::TempDir;

struct StandInNode {
    child: Child,
}

impl StandInNode {
    fn spawn(db_path: &Path) -> Self {
        std::fs::create_dir_all(db_path).expect("create the stand-in node dir");
        let child = Command::new("sh")
            .args([
                "-c",
                "while :; do sleep 1; done",
                "stand-in",
                "cluster",
                "start",
                "--db",
            ])
            .arg(db_path)
            .spawn()
            .expect("spawn a stand-in node");
        Self { child }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn has_exited(&mut self) -> bool {
        self.child
            .try_wait()
            .expect("query the stand-in node")
            .is_some()
    }
}

impl Drop for StandInNode {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn mqdb(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mqdb"))
        .args(args)
        .output()
        .expect("run mqdb")
}

fn pid_file(db_prefix: &Path, node_id: u8) -> PathBuf {
    PathBuf::from(format!("{}-{node_id}/mqdb.pid", db_prefix.display()))
}

fn write_pid_file(db_prefix: &Path, node_id: u8, pid: u32) {
    let path = pid_file(db_prefix, node_id);
    std::fs::create_dir_all(path.parent().expect("pid file has a node dir"))
        .expect("create the node dir");
    std::fs::write(path, pid.to_string()).expect("write the pid file");
}

fn kill_cluster(db_prefix: &Path) -> Output {
    mqdb(&[
        "dev",
        "kill",
        "--db-prefix",
        &db_prefix.display().to_string(),
    ])
}

#[test]
fn dev_kill_with_db_prefix_stops_only_the_nodes_of_that_cluster() {
    let dir = TempDir::new().expect("temp dir");
    let ours = dir.path().join("e2e");
    let theirs = dir.path().join("e2e-x");
    let mut our_node = StandInNode::spawn(&ours.with_file_name("e2e-1"));
    let mut their_node = StandInNode::spawn(&theirs.with_file_name("e2e-x-1"));
    write_pid_file(&ours, 1, our_node.pid());
    write_pid_file(&ours, 2, their_node.pid());
    write_pid_file(&theirs, 1, their_node.pid());

    let output = kill_cluster(&ours);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "dev kill --db-prefix must succeed: {stdout} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        our_node.has_exited(),
        "node 1 of the cluster must stop: {stdout}"
    );
    assert!(
        !their_node.has_exited(),
        "a pid file naming another cluster's node must not kill it: {stdout}"
    );
    assert!(
        !pid_file(&ours, 1).exists(),
        "a stopped node's pid file is removed"
    );
    assert!(!pid_file(&ours, 2).exists(), "a stale pid file is removed");
    assert!(
        pid_file(&theirs, 1).exists(),
        "the other cluster keeps its pid file"
    );

    let output = kill_cluster(&theirs);
    assert!(output.status.success());
    assert!(
        their_node.has_exited(),
        "the other cluster stops on its own prefix"
    );
}

#[test]
fn dev_start_cluster_numbers_nodes_from_the_base_port_and_records_their_pids() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let prefix_arg = prefix.display().to_string();

    let output = mqdb(&[
        "dev",
        "start-cluster",
        "--nodes",
        "2",
        "--base-port",
        "32040",
        "--no-quic",
        "--db-prefix",
        &prefix_arg,
    ]);
    let pids = [1, 2].map(|node_id| std::fs::read_to_string(pid_file(&prefix, node_id)).ok());
    let killed = kill_cluster(&prefix);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "dev start-cluster must succeed: {stdout} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Starting node 1 on port 32040"), "{stdout}");
    assert!(stdout.contains("Starting node 2 on port 32041"), "{stdout}");
    for pid in pids {
        let pid = pid.expect("every started node has a pid file");
        assert!(
            pid.trim().parse::<u32>().is_ok(),
            "pid file holds a pid: {pid}"
        );
    }
    assert!(
        killed.status.success(),
        "dev kill --db-prefix must succeed: {}",
        String::from_utf8_lossy(&killed.stderr)
    );
    for node_id in [1, 2] {
        assert!(!pid_file(&prefix, node_id).exists());
    }
}
