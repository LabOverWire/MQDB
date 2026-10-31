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
        Self::spawn_running(db_path, "while :; do sleep 1; done")
    }

    fn spawn_running(db_path: &Path, script: &str) -> Self {
        std::fs::create_dir_all(db_path).expect("create the stand-in node dir");
        let child = Command::new("sh")
            .args(["-c", script, "stand-in", "cluster", "start", "--db"])
            .arg(db_path)
            .spawn()
            .expect("spawn a stand-in node");
        Self { child }
    }

    fn spawn_args(cwd: &Path, args: &[&str]) -> Self {
        let child = Command::new("sh")
            .args(["-c", "while :; do sleep 1; done", "stand-in"])
            .args(args)
            .current_dir(cwd)
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

    let output = mqdb_command(&[
        "dev",
        "start-cluster",
        "--nodes",
        "2",
        "--base-port",
        "32040",
        "--no-quic",
        "--db-prefix",
        &prefix_arg,
    ])
    .env_remove("MQDB_LICENSE")
    .env_remove("MQDB_LICENSE_FILE")
    .output()
    .expect("run mqdb");
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
    assert!(
        killed.status.success(),
        "dev kill --db-prefix must succeed: {}",
        String::from_utf8_lossy(&killed.stderr)
    );
    let killed_stdout = String::from_utf8_lossy(&killed.stdout);
    for (node_id, pid) in [1, 2].into_iter().zip(pids) {
        let pid = pid.expect("every started node has a pid file");
        let pid = pid.trim();
        assert!(pid.parse::<u32>().is_ok(), "pid file holds a pid: {pid}");
        let node_line = format!("Node {node_id} (pid {pid}) ");
        assert!(
            killed_stdout
                .lines()
                .any(|line| line.starts_with(&node_line)),
            "dev kill reports node {node_id} with its recorded pid: {killed_stdout}"
        );
        assert!(!pid_file(&prefix, node_id).exists());
    }
}

fn mqdb_command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mqdb"));
    command.args(args);
    command
}

fn output_text(output: &Output) -> String {
    format!(
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn dev_kill_with_a_relative_db_prefix_stops_a_node_recorded_with_an_absolute_path() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());

    let output = mqdb_command(&["dev", "kill", "--db-prefix", "e2e"])
        .current_dir(dir.path())
        .output()
        .expect("run mqdb");

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(node.has_exited(), "{}", output_text(&output));
}

#[test]
fn dev_kill_reads_the_whole_command_line_of_a_node_when_columns_is_narrow() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());

    let output = mqdb_command(&["dev", "kill", "--db-prefix", &prefix.display().to_string()])
        .env("COLUMNS", "40")
        .output()
        .expect("run mqdb");

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(node.has_exited(), "{}", output_text(&output));
}

#[test]
fn dev_kill_stops_the_remaining_nodes_when_one_pid_file_is_unreadable() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-2"));
    std::fs::create_dir_all(prefix.with_file_name("e2e-1")).expect("create node 1 dir");
    std::fs::write(pid_file(&prefix, 1), "not-a-pid").expect("write a bad pid file");
    write_pid_file(&prefix, 2, node.pid());

    let output = kill_cluster(&prefix);

    assert!(
        node.has_exited(),
        "node 2 must stop although node 1's pid file is bad: {}",
        output_text(&output)
    );
    assert!(
        !output.status.success(),
        "the bad pid file is reported as a failure: {}",
        output_text(&output)
    );
}

#[test]
fn dev_kill_fails_for_a_node_that_has_no_pid_file() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());

    let output = mqdb(&[
        "dev",
        "kill",
        "--db-prefix",
        &prefix.display().to_string(),
        "--node",
        "7",
    ]);

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(pid_file(&prefix, 1).exists());
}

#[test]
fn dev_clean_removes_only_this_prefix_and_refuses_while_one_of_its_dirs_is_in_use() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let prefix_arg = prefix.display().to_string();
    std::fs::create_dir_all(prefix.with_file_name("e2e-1")).expect("create node 1 dir");
    std::fs::write(prefix.with_file_name("e2e-passwd"), "admin:x\n").expect("write passwd");
    std::fs::create_dir_all(prefix.with_file_name("e2ex-1")).expect("create another prefix dir");
    std::fs::create_dir_all(prefix.with_file_name("e2e-x-1"))
        .expect("create a stopped cluster dir");
    std::fs::write(prefix.with_file_name("e2e-x-passwd"), "admin:x\n").expect("write its passwd");
    let their_node = StandInNode::spawn(&prefix.with_file_name("e2e-y-1"));

    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix_arg]);
    assert!(output.status.success(), "{}", output_text(&output));
    assert!(!prefix.with_file_name("e2e-1").exists());
    assert!(!prefix.with_file_name("e2e-passwd").exists());
    for kept in ["e2ex-1", "e2e-x-1", "e2e-x-passwd", "e2e-y-1"] {
        assert!(
            prefix.with_file_name(kept).exists(),
            "{kept} belongs to another prefix and must survive: {}",
            output_text(&output)
        );
    }
    drop(their_node);

    let our_node = StandInNode::spawn(&prefix.with_file_name("e2e-2"));
    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix_arg]);
    assert!(
        !output.status.success(),
        "clean must refuse while a node runs on one of its dirs, pid file or not: {}",
        output_text(&output)
    );
    assert!(prefix.with_file_name("e2e-2").exists());
    drop(our_node);
}

#[test]
fn dev_clean_refuses_while_an_agent_uses_one_of_its_dirs() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(&node_dir).expect("create node dir");
    let node_arg = node_dir.display().to_string();
    let agent = StandInNode::spawn_args(dir.path(), &["agent", "start", "--db", &node_arg]);

    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix.display().to_string()]);

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(node_dir.exists());
    drop(agent);
}

#[test]
fn dev_kill_stops_nodes_started_with_db_equals_or_a_relative_db() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    for node_id in [1, 2] {
        std::fs::create_dir_all(prefix.with_file_name(format!("e2e-{node_id}")))
            .expect("create node dir");
    }
    let equals_arg = format!("--db={}", prefix.with_file_name("e2e-1").display());
    let mut equals_node = StandInNode::spawn_args(
        dir.path(),
        &["cluster", "start", &equals_arg, "--bind", "x"],
    );
    let mut relative_node =
        StandInNode::spawn_args(dir.path(), &["cluster", "start", "--db", "e2e-2"]);
    write_pid_file(&prefix, 1, equals_node.pid());
    write_pid_file(&prefix, 2, relative_node.pid());

    let output = kill_cluster(&prefix);

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(equals_node.has_exited(), "{}", output_text(&output));
    assert!(relative_node.has_exited(), "{}", output_text(&output));
}

#[test]
fn dev_start_cluster_spawns_nothing_when_it_cannot_record_the_pid() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(node_dir.join("mqdb.pid").join("blocker")).expect("block the pid file");

    let output = start_one_node(&prefix.display().to_string(), "32056");

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(
        !node_dir.join("mqdb.log").exists(),
        "no node is started without a pid file: {}",
        output_text(&output)
    );
}

#[test]
fn dev_start_cluster_leaves_no_pid_file_when_it_cannot_open_the_log() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(node_dir.join("mqdb.log").join("blocker")).expect("block the log");

    let output = start_one_node(&prefix.display().to_string(), "32060");

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("mqdb.log"),
        "start-cluster failed for another reason: {}",
        output_text(&output)
    );
    assert!(
        !node_dir.join("mqdb.pid").exists(),
        "a node that never started left a pid file: {}",
        output_text(&output)
    );
}

#[test]
fn dev_start_cluster_reports_a_stale_passwd_it_cannot_remove() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    std::fs::create_dir_all(prefix.with_file_name("e2e-passwd").join("blocker"))
        .expect("block the passwd file");

    let output = start_one_node(&prefix.display().to_string(), "32058");

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("remove stale"),
        "{}",
        output_text(&output)
    );
}

#[test]
fn dev_logs_follow_with_a_pattern_leaves_no_tail_behind() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(&node_dir).expect("create node dir");
    let log = node_dir.join("mqdb.log");
    std::fs::write(&log, "match 0\n").expect("write log");
    let matched = dir.path().join("matched.out");
    let pipeline = format!(
        "'{}' dev logs -f --pattern match --db-prefix '{}' | head -n 3 > '{}'",
        env!("CARGO_BIN_EXE_mqdb"),
        prefix.display(),
        matched.display()
    );
    let mut follower = ProcessGroup {
        child: std::os::unix::process::CommandExt::process_group(
            Command::new("sh").args(["-c", &pipeline]),
            0,
        )
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn the pipeline"),
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut line = 1;
    while follower
        .child
        .try_wait()
        .expect("query the pipeline")
        .is_none()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the pipeline never finished"
        );
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .expect("open log");
        std::io::Write::write_all(&mut file, format!("match {line}\n").as_bytes())
            .expect("append to log");
        line += 1;
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    let tails = Command::new("pgrep")
        .args(["-f", &log.display().to_string()])
        .output()
        .expect("run pgrep");
    let leftover_tails = String::from_utf8_lossy(&tails.stdout).trim().to_string();
    let followed = std::fs::read_to_string(&matched).unwrap_or_default();
    for pid in leftover_tails.lines() {
        let _ = Command::new("kill").arg(pid).status();
    }
    assert!(
        leftover_tails.is_empty(),
        "tail -f must not outlive dev logs: {leftover_tails}"
    );
    assert_eq!(
        followed
            .lines()
            .filter(|line| line.starts_with("match "))
            .count(),
        2,
        "the pipeline must have followed matching lines: {followed}"
    );
}

#[test]
fn dev_clean_keeps_a_stopped_cluster_whose_prefix_ends_in_a_suite_name() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("foo");
    std::fs::create_dir_all(prefix.with_file_name("foo-1")).expect("create node dir");
    std::fs::create_dir_all(prefix.with_file_name("foo-own-1")).expect("create other cluster dir");
    std::fs::write(prefix.with_file_name("foo-presence-passwd"), "admin:x\n")
        .expect("write other cluster passwd");

    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix.display().to_string()]);

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(!prefix.with_file_name("foo-1").exists());
    for kept in ["foo-own-1", "foo-presence-passwd"] {
        assert!(
            prefix.with_file_name(kept).exists(),
            "{kept} belongs to the stopped cluster --db-prefix foo-own or foo-presence: {}",
            output_text(&output)
        );
    }
}

#[test]
fn dev_start_cluster_refuses_a_privileged_or_zero_base_port() {
    for base_port in ["0", "1023"] {
        let dir = TempDir::new().expect("temp dir");
        let prefix_arg = dir.path().join("e2e").display().to_string();
        let output = mqdb(&[
            "dev",
            "start-cluster",
            "--nodes",
            "1",
            "--base-port",
            base_port,
            "--no-quic",
            "--db-prefix",
            &prefix_arg,
        ]);
        assert!(
            !output.status.success(),
            "--base-port {base_port}: {}",
            output_text(&output)
        );
        assert!(!dir.path().join("e2e-1").exists());
    }
}

#[test]
fn dev_logs_shows_only_the_nodes_of_its_prefix() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    for (node_dir, line) in [("e2e-1", "ours"), ("e2e-x-1", "theirs")] {
        let node_dir = prefix.with_file_name(node_dir);
        std::fs::create_dir_all(&node_dir).expect("create node dir");
        std::fs::write(node_dir.join("mqdb.log"), format!("{line}\n")).expect("write log");
    }

    let output = mqdb(&["dev", "logs", "--db-prefix", &prefix.display().to_string()]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(stdout.contains("=== Node 1 "), "{stdout}");
    assert!(stdout.contains("ours"), "{stdout}");
    assert!(
        !stdout.contains("theirs"),
        "another prefix's log must not be shown: {stdout}"
    );
}

fn send_term(pid: u32) {
    let status = Command::new("kill")
        .arg(pid.to_string())
        .status()
        .expect("run kill");
    assert!(status.success(), "signal the stand-in node");
}

fn tool_dir(dir: &Path, tools: &[(&str, &str)]) -> PathBuf {
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).expect("create the tool dir");
    for (name, script) in tools {
        let path = bin.join(name);
        std::fs::write(&path, script).expect("write the tool");
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .expect("make the tool executable");
    }
    bin
}

fn start_one_node(prefix_arg: &str, base_port: &str) -> Output {
    mqdb_command(&[
        "dev",
        "start-cluster",
        "--nodes",
        "1",
        "--base-port",
        base_port,
        "--no-quic",
        "--db-prefix",
        prefix_arg,
    ])
    .env_remove("MQDB_LICENSE")
    .env_remove("MQDB_LICENSE_FILE")
    .output()
    .expect("run mqdb")
}

#[test]
fn dev_clean_waits_for_a_stopping_node_to_exit() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn_running(
        &prefix.with_file_name("e2e-1"),
        "trap 'sleep 5; exit 0' TERM; touch \"$4/trap-set\"; while :; do sleep 1; done",
    );
    write_pid_file(&prefix, 1, node.pid());
    let trap_set = prefix.with_file_name("e2e-1").join("trap-set");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !trap_set.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the stand-in node never set its TERM trap"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    send_term(node.pid());

    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix.display().to_string()]);

    assert!(
        output.status.success(),
        "clean must wait for a node that is shutting down: {}",
        output_text(&output)
    );
    assert!(node.has_exited());
    assert!(!prefix.with_file_name("e2e-1").exists());
}

#[test]
fn dev_kill_keeps_the_pid_file_when_ps_cannot_run() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());
    let empty_path = tool_dir(dir.path(), &[]);

    let output = mqdb_command(&["dev", "kill", "--db-prefix", &prefix.display().to_string()])
        .env("PATH", &empty_path)
        .output()
        .expect("run mqdb");

    assert!(
        !output.status.success(),
        "a node whose state is unknown is not reported as stopped: {}",
        output_text(&output)
    );
    assert!(pid_file(&prefix, 1).exists(), "{}", output_text(&output));
    assert!(!node.has_exited());
}

#[test]
fn dev_kill_reports_a_signal_that_could_not_be_sent() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());
    let failing_kill = tool_dir(
        dir.path(),
        &[
            ("ps", "#!/bin/sh\nexec /bin/ps \"$@\"\n"),
            (
                "kill",
                "#!/bin/sh\necho 'kill: Operation not permitted' >&2\nexit 1\n",
            ),
        ],
    );

    let output = mqdb_command(&["dev", "kill", "--db-prefix", &prefix.display().to_string()])
        .env("PATH", &failing_kill)
        .output()
        .expect("run mqdb");
    let text = output_text(&output);

    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("Operation not permitted") && !text.contains("did not stop"),
        "the failed signal is the reported error: {text}"
    );
    assert!(pid_file(&prefix, 1).exists());
    assert!(!node.has_exited());
}

#[test]
fn dev_start_cluster_refuses_while_a_recorded_node_runs() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());
    let log = prefix.with_file_name("e2e-1").join("mqdb.log");
    std::fs::write(&log, "running node log\n").expect("write the node log");

    let output = start_one_node(&prefix.display().to_string(), "32050");

    assert!(
        !output.status.success(),
        "a second start over a running node must refuse: {}",
        output_text(&output)
    );
    assert_eq!(
        std::fs::read_to_string(pid_file(&prefix, 1)).expect("pid file"),
        node.pid().to_string()
    );
    assert_eq!(
        std::fs::read_to_string(&log).expect("node log"),
        "running node log\n"
    );
}

#[test]
fn dev_start_cluster_treats_a_trailing_slash_as_the_same_prefix() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("foo");

    let output = start_one_node(&format!("{}/", prefix.display()), "32052");
    let killed = kill_cluster(&prefix);

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        !dir.path().join("foo").join("-1").exists(),
        "a trailing slash must not create node dirs inside the prefix: {}",
        output_text(&output)
    );
    assert!(
        String::from_utf8_lossy(&killed.stdout).contains("Node 1 (pid"),
        "kill without the slash finds the node: {}",
        output_text(&killed)
    );
}

#[test]
fn dev_kill_treats_a_node_that_exits_before_the_signal_as_stopped() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let mut node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());
    let racing_kill = tool_dir(
        dir.path(),
        &[
            ("ps", "#!/bin/sh\nexec /bin/ps \"$@\"\n"),
            (
                "kill",
                "#!/bin/sh\n/bin/kill -9 \"$1\"\n/bin/sleep 1\necho \"kill: $1: No such process\" >&2\nexit 1\n",
            ),
        ],
    );

    let output = mqdb_command(&["dev", "kill", "--db-prefix", &prefix.display().to_string()])
        .env("PATH", &racing_kill)
        .output()
        .expect("run mqdb");

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(node.has_exited());
    assert!(!pid_file(&prefix, 1).exists(), "{}", output_text(&output));
}

#[test]
fn dev_start_cluster_without_clean_refuses_a_running_node_without_waiting() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node = StandInNode::spawn(&prefix.with_file_name("e2e-1"));
    write_pid_file(&prefix, 1, node.pid());

    let output = start_one_node(&prefix.display().to_string(), "32054");

    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(
        !output_text(&output).contains("Waiting"),
        "nothing is stopping, so there is nothing to wait for: {}",
        output_text(&output)
    );
}

#[test]
fn dev_kill_by_node_id_reports_a_pkill_that_cannot_run() {
    let dir = TempDir::new().expect("temp dir");
    let empty_path = tool_dir(dir.path(), &[]);

    let output = mqdb_command(&["dev", "kill", "--node", "9999"])
        .env("PATH", &empty_path)
        .output()
        .expect("run mqdb");

    assert!(
        !output.status.success(),
        "a pkill that never ran is not 'Done': {}",
        output_text(&output)
    );
}

struct ProcessGroup {
    child: Child,
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let group = format!("-{}", self.child.id());
            let _ = Command::new("kill").args(["--", &group]).status();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn dev_logs_follow_tails_every_node_of_the_prefix() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    for node_id in [1, 2] {
        let node_dir = prefix.with_file_name(format!("e2e-{node_id}"));
        std::fs::create_dir_all(&node_dir).expect("create node dir");
        std::fs::write(
            node_dir.join("mqdb.log"),
            format!("line from node {node_id}\n"),
        )
        .expect("write log");
    }
    let captured = dir.path().join("follow.out");
    let stdout = std::fs::File::create(&captured).expect("create capture file");

    let follower = ProcessGroup {
        child: std::os::unix::process::CommandExt::process_group(
            &mut mqdb_command(&[
                "dev",
                "logs",
                "-f",
                "--db-prefix",
                &prefix.display().to_string(),
            ]),
            0,
        )
        .stdout(stdout)
        .spawn()
        .expect("spawn dev logs -f"),
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut seen = String::new();
    while std::time::Instant::now() < deadline
        && !(seen.contains("line from node 1") && seen.contains("line from node 2"))
    {
        std::thread::sleep(std::time::Duration::from_millis(100));
        seen = std::fs::read_to_string(&captured).unwrap_or_default();
    }
    drop(follower);

    assert!(seen.contains("line from node 1"), "{seen}");
    assert!(
        seen.contains("line from node 2"),
        "follow must tail node 2 too: {seen}"
    );
}

#[test]
fn dev_start_cluster_leaves_no_pid_file_for_a_node_it_could_not_spawn() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let linked_exe = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mqdb-spawn-failure-{}", std::process::id()));
    std::fs::hard_link(env!("CARGO_BIN_EXE_mqdb"), &linked_exe).expect("link the mqdb binary");
    let start = Command::new(&linked_exe)
        .args([
            "dev",
            "start-cluster",
            "--nodes",
            "2",
            "--base-port",
            "32060",
            "--no-quic",
            "--db-prefix",
            &prefix.display().to_string(),
        ])
        .env_remove("MQDB_LICENSE")
        .env_remove("MQDB_LICENSE_FILE")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn dev start-cluster");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while std::fs::read_to_string(pid_file(&prefix, 1))
        .unwrap_or_default()
        .is_empty()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "node 1 was never recorded"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    std::fs::remove_file(&linked_exe).expect("remove the linked binary");
    let output = start
        .wait_with_output()
        .expect("wait for dev start-cluster");

    assert!(
        !output.status.success(),
        "node 2 cannot be spawned once the binary is gone"
    );
    assert!(
        !pid_file(&prefix, 2).exists(),
        "a node that never started leaves no pid file: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn dev_clean_ignores_a_relative_db_node_that_exits_before_its_cwd_is_read() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    std::fs::create_dir_all(prefix.with_file_name("e2e-1")).expect("create node dir");
    let vanishing = StandInNode::spawn_args(
        dir.path(),
        &["cluster", "start", "--db", "vanishing-node-relative-db"],
    );
    let racing_lsof = tool_dir(
        dir.path(),
        &[
            ("ps", "#!/bin/sh\nexec /bin/ps \"$@\"\n"),
            (
                "lsof",
                "#!/bin/sh\nif [ \"$3\" = \"$VANISHING_PID\" ]; then\n  /bin/kill -9 \"$3\"\n  /bin/sleep 1\n  exit 1\nfi\nPATH=/usr/sbin:/usr/bin:/bin exec lsof \"$@\"\n",
            ),
        ],
    );

    let output = mqdb_command(&["dev", "clean", "--db-prefix", &prefix.display().to_string()])
        .env("PATH", &racing_lsof)
        .env("VANISHING_PID", vanishing.pid().to_string())
        .output()
        .expect("run mqdb");

    assert!(
        output.status.success(),
        "a node that is gone does not block clean: {}",
        output_text(&output)
    );
    assert!(!prefix.with_file_name("e2e-1").exists());
    drop(vanishing);
}

#[test]
fn dev_clean_keeps_a_passwd_file_a_running_node_uses() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let passwd = prefix.with_file_name("e2e-passwd");
    std::fs::write(&passwd, "admin:x\n").expect("write passwd");
    let elsewhere = dir.path().join("elsewhere-1");
    std::fs::create_dir_all(&elsewhere).expect("create the node dir");
    let elsewhere_arg = elsewhere.display().to_string();
    let passwd_arg = passwd.display().to_string();
    let node = StandInNode::spawn_args(
        dir.path(),
        &[
            "cluster",
            "start",
            "--db",
            &elsewhere_arg,
            "--passwd",
            &passwd_arg,
        ],
    );

    let output = mqdb(&["dev", "clean", "--db-prefix", &prefix.display().to_string()]);

    assert!(
        !output.status.success(),
        "clean must refuse while a node reads the passwd file: {}",
        output_text(&output)
    );
    assert!(passwd.exists());
    drop(node);
}

#[test]
fn dev_logs_follow_matches_the_pattern_literally() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(&node_dir).expect("create node dir");
    std::fs::write(node_dir.join("mqdb.log"), "aXb regex only\na.b literal\n").expect("write log");
    let captured = dir.path().join("follow.out");
    let stdout = std::fs::File::create(&captured).expect("create capture file");

    let follower = ProcessGroup {
        child: std::os::unix::process::CommandExt::process_group(
            &mut mqdb_command(&[
                "dev",
                "logs",
                "-f",
                "--pattern",
                "a.b",
                "--db-prefix",
                &prefix.display().to_string(),
            ]),
            0,
        )
        .stdout(stdout)
        .spawn()
        .expect("spawn dev logs -f"),
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut seen = String::new();
    while std::time::Instant::now() < deadline && !seen.contains("a.b literal") {
        std::thread::sleep(std::time::Duration::from_millis(100));
        seen = std::fs::read_to_string(&captured).unwrap_or_default();
    }
    drop(follower);

    assert!(seen.contains("a.b literal"), "{seen}");
    assert!(
        !seen.contains("aXb regex only"),
        "--pattern is a literal substring in follow mode too: {seen}"
    );
}

#[test]
fn dev_logs_follow_reports_a_failing_grep() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let node_dir = prefix.with_file_name("e2e-1");
    std::fs::create_dir_all(&node_dir).expect("create node dir");
    std::fs::write(node_dir.join("mqdb.log"), "line\n").expect("write log");
    let failing_grep = tool_dir(
        dir.path(),
        &[
            ("tail", "#!/bin/sh\nexec /usr/bin/tail \"$@\"\n"),
            ("grep", "#!/bin/sh\necho 'grep: broken' >&2\nexit 2\n"),
        ],
    );

    let output = mqdb_command(&[
        "dev",
        "logs",
        "-f",
        "--pattern",
        "line",
        "--db-prefix",
        &prefix.display().to_string(),
    ])
    .env("PATH", &failing_grep)
    .output()
    .expect("run mqdb");

    assert!(
        !output.status.success(),
        "a grep that fails is not a successful follow: {}",
        output_text(&output)
    );
}

#[test]
fn dev_start_cluster_keeps_a_passwd_file_a_running_node_uses() {
    let dir = TempDir::new().expect("temp dir");
    let prefix = dir.path().join("e2e");
    let passwd = prefix.with_file_name("e2e-passwd");
    std::fs::write(&passwd, "admin:x\nalice:y\n").expect("write passwd");
    let elsewhere = dir.path().join("agentdb");
    std::fs::create_dir_all(&elsewhere).expect("create the agent dir");
    let elsewhere_arg = elsewhere.display().to_string();
    let passwd_arg = passwd.display().to_string();
    let agent = StandInNode::spawn_args(
        dir.path(),
        &[
            "agent",
            "start",
            "--db",
            &elsewhere_arg,
            "--passwd",
            &passwd_arg,
        ],
    );

    let output = start_one_node(&prefix.display().to_string(), "32062");

    assert!(
        !output.status.success(),
        "start must refuse while a node reads the passwd file it would replace: {}",
        output_text(&output)
    );
    assert_eq!(
        std::fs::read_to_string(&passwd).expect("passwd"),
        "admin:x\nalice:y\n"
    );
    drop(agent);
}
