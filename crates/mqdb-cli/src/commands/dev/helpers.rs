// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

pub(crate) const NODE_PID_FILE: &str = "mqdb.pid";
const NODE_STOP_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn cmd_dev_ps() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("pgrep").args(["-fl", "mqdb"]).output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.is_empty() {
        println!("No MQDB processes running");
        return Ok(());
    }

    println!("{:<8} {:<10} DETAILS", "PID", "TYPE");
    println!("{}", "-".repeat(60));

    for line in stdout.lines() {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() < 2 {
            continue;
        }
        let pid = parts[0];
        let cmd = parts[1];

        let proc_type = if cmd.contains("cluster start") {
            "cluster"
        } else if cmd.contains("agent start") {
            "agent"
        } else {
            "other"
        };

        let details = if let Some(node_pos) = cmd.find("--node-id") {
            let rest = &cmd[node_pos + 10..];
            let node_id: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if let Some(bind_pos) = cmd.find("--bind") {
                let bind_rest = &cmd[bind_pos + 7..];
                let bind: String = bind_rest
                    .chars()
                    .take_while(|c| !c.is_whitespace())
                    .collect();
                format!("node={node_id} bind={bind}")
            } else {
                format!("node={node_id}")
            }
        } else if let Some(bind_pos) = cmd.find("--bind") {
            let bind_rest = &cmd[bind_pos + 7..];
            let bind: String = bind_rest
                .chars()
                .take_while(|c| !c.is_whitespace())
                .collect();
            format!("bind={bind}")
        } else {
            String::new()
        };

        println!("{pid:<8} {proc_type:<10} {details}");
    }

    Ok(())
}

fn db_argument(command: &str) -> Option<&str> {
    let (_, rest) = command.split_once(" --db ")?;
    Some(rest.split_once(" --").map_or(rest, |(db, _)| db).trim())
}

fn is_cluster_node(command: &str, node_dir: &Path) -> bool {
    let same_dir = db_argument(command)
        .and_then(|db| std::fs::canonicalize(db).ok())
        .is_some_and(|db| std::fs::canonicalize(node_dir).is_ok_and(|dir| dir == db));
    format!(" {command} ").contains(" cluster start ") && same_dir
}

fn process_command(pid: u32) -> Result<Option<String>, String> {
    let output = Command::new("ps")
        .args(["-ww", "-p", &pid.to_string(), "-o", "command="])
        .output()
        .map_err(|e| format!("run ps: {e}"))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    match (output.status.success(), stderr.trim()) {
        (true, _) => Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        )),
        (false, "") => Ok(None),
        (false, error) => Err(format!("ps -p {pid}: {error}")),
    }
}

enum NodeState {
    Running,
    Stopped,
}

fn node_state(pid: u32, node_dir: &Path) -> Result<NodeState, String> {
    match process_command(pid)? {
        Some(command) if is_cluster_node(&command, node_dir) => Ok(NodeState::Running),
        Some(_) | None => Ok(NodeState::Stopped),
    }
}

fn live_node_dirs() -> Result<Vec<PathBuf>, String> {
    let output = Command::new("ps")
        .args(["-ww", "-ax", "-o", "command="])
        .output()
        .map_err(|e| format!("run ps: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps -ax: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let commands = String::from_utf8_lossy(&output.stdout);
    Ok(commands
        .lines()
        .filter(|command| format!(" {command} ").contains(" cluster start "))
        .filter_map(db_argument)
        .filter_map(|db| std::fs::canonicalize(db).ok())
        .collect())
}

fn dirs_in_use(dirs: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let live = live_node_dirs()?;
    Ok(dirs
        .iter()
        .filter(|dir| std::fs::canonicalize(dir).is_ok_and(|dir| live.contains(&dir)))
        .cloned()
        .collect())
}

fn listed(dirs: &[PathBuf]) -> String {
    dirs.iter()
        .map(|dir| dir.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn refuse_if_in_use(db_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dirs: Vec<PathBuf> = node_dirs(db_prefix)?
        .into_iter()
        .map(|(_, dir)| dir)
        .collect();
    let in_use = dirs_in_use(&dirs)?;
    if in_use.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "nodes are still running on {}; stop them with 'mqdb dev kill --db-prefix {db_prefix}'",
            listed(&in_use)
        )
        .into())
    }
}

fn wait_until_unused(dirs: &[PathBuf], db_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut announced = false;
    loop {
        let in_use = dirs_in_use(dirs)?;
        if in_use.is_empty() {
            return Ok(());
        }
        if start.elapsed() > NODE_STOP_TIMEOUT {
            return Err(format!(
                "nodes are still running on {}; stop them with 'mqdb dev kill --db-prefix {db_prefix}'",
                listed(&in_use)
            )
            .into());
        }
        if !announced {
            println!(
                "Waiting up to {NODE_STOP_TIMEOUT:?} for the nodes on {} to exit...",
                listed(&in_use)
            );
            announced = true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn signal(pid: u32) -> Result<(), String> {
    let output = Command::new("kill")
        .arg(pid.to_string())
        .output()
        .map_err(|e| format!("run kill: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "kill {pid}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

pub(crate) fn absolute_prefix(db_prefix: &str) -> Result<String, Box<dyn std::error::Error>> {
    let path: PathBuf = std::path::absolute(db_prefix)?.components().collect();
    Ok(path
        .to_str()
        .ok_or("--db-prefix is not valid UTF-8")?
        .to_string())
}

fn node_dirs(db_prefix: &str) -> Result<Vec<(u16, PathBuf)>, Box<dyn std::error::Error>> {
    let node_name_prefix = Path::new(db_prefix)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(|name| format!("{name}-"))
        .ok_or("--db-prefix must end in a file name")?;
    let pattern = format!("{}-*", glob::Pattern::escape(db_prefix));
    let mut dirs: Vec<(u16, PathBuf)> = glob::glob(&pattern)?
        .filter_map(Result::ok)
        .filter(|path| path.is_dir())
        .filter_map(|path| {
            let node_id = path
                .file_name()?
                .to_str()?
                .strip_prefix(&node_name_prefix)?
                .parse()
                .ok()?;
            Some((node_id, path))
        })
        .collect();
    dirs.sort_unstable_by_key(|(node_id, _)| *node_id);
    Ok(dirs)
}

struct RecordedNode {
    node_id: u16,
    dir: PathBuf,
    pid_file: PathBuf,
}

fn recorded_nodes(db_prefix: &str) -> Result<Vec<RecordedNode>, Box<dyn std::error::Error>> {
    Ok(node_dirs(db_prefix)?
        .into_iter()
        .map(|(node_id, dir)| RecordedNode {
            node_id,
            pid_file: dir.join(NODE_PID_FILE),
            dir,
        })
        .filter(|node| node.pid_file.exists())
        .collect())
}

fn read_pid(pid_file: &Path) -> Result<u32, String> {
    std::fs::read_to_string(pid_file)
        .map_err(|e| format!("{}: {e}", pid_file.display()))?
        .trim()
        .parse()
        .map_err(|e| format!("{}: {e}", pid_file.display()))
}

fn forget_node(node: &RecordedNode, pid: u32, state: &str) -> Result<(), String> {
    println!("Node {} (pid {pid}) {state}", node.node_id);
    std::fs::remove_file(&node.pid_file).map_err(|e| format!("{}: {e}", node.pid_file.display()))
}

fn kill_cluster(db_prefix: &str, node: Option<u16>) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = absolute_prefix(db_prefix)?;
    let recorded = recorded_nodes(&prefix)?;
    let targets: Vec<RecordedNode> = match node {
        Some(wanted) => {
            let selected: Vec<RecordedNode> = recorded
                .into_iter()
                .filter(|recorded| recorded.node_id == wanted)
                .collect();
            if selected.is_empty() {
                return Err(
                    format!("node {wanted} has no pid file under --db-prefix {prefix}").into(),
                );
            }
            selected
        }
        None => recorded,
    };
    if targets.is_empty() {
        println!("No nodes started with --db-prefix {prefix}");
        return Ok(());
    }

    let mut failures = Vec::new();
    let mut signalled = Vec::new();
    for target in targets {
        let pid = match read_pid(&target.pid_file) {
            Ok(pid) => pid,
            Err(e) => {
                failures.push(format!("node {}: {e}", target.node_id));
                continue;
            }
        };
        match node_state(pid, &target.dir) {
            Ok(NodeState::Running) => match signal(pid) {
                Ok(()) => signalled.push((target, pid)),
                Err(e) => match node_state(pid, &target.dir) {
                    Ok(NodeState::Stopped) => {
                        if let Err(e) = forget_node(&target, pid, "stopped") {
                            failures.push(e);
                        }
                    }
                    Ok(NodeState::Running) | Err(_) => {
                        failures.push(format!("node {}: {e}", target.node_id));
                    }
                },
            },
            Ok(NodeState::Stopped) => {
                if let Err(e) = forget_node(&target, pid, "is not running; removed its pid file") {
                    failures.push(e);
                }
            }
            Err(e) => failures.push(format!("node {}: {e}", target.node_id)),
        }
    }

    let start = Instant::now();
    while !signalled.is_empty() && start.elapsed() <= NODE_STOP_TIMEOUT {
        let mut waiting = Vec::new();
        for (target, pid) in signalled {
            match node_state(pid, &target.dir) {
                Ok(NodeState::Running) => waiting.push((target, pid)),
                Ok(NodeState::Stopped) => {
                    if let Err(e) = forget_node(&target, pid, "stopped") {
                        failures.push(e);
                    }
                }
                Err(e) => failures.push(format!("node {}: {e}", target.node_id)),
            }
        }
        signalled = waiting;
        if !signalled.is_empty() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    for (target, pid) in signalled {
        failures.push(format!(
            "node {} (pid {pid}) did not stop within {NODE_STOP_TIMEOUT:?}",
            target.node_id
        ));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn legacy_node_pattern(node_id: u16) -> String {
    format!("mqdb cluster.*--node-id {node_id}( |$)")
}

fn pkill(pattern: &str) -> Result<(), String> {
    let status = Command::new("pkill")
        .args(["-f", pattern])
        .status()
        .map_err(|e| format!("run pkill: {e}"))?;
    match status.code() {
        Some(0 | 1) => Ok(()),
        _ => Err(format!("pkill -f '{pattern}' failed: {status}")),
    }
}

pub(crate) fn cmd_dev_kill(
    all: bool,
    node: Option<u16>,
    agent: bool,
    db_prefix: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    match (db_prefix, agent, node, all) {
        (Some(prefix), _, _, _) => return kill_cluster(prefix, node),
        (None, true, _, _) => {
            println!("Killing MQDB agent...");
            pkill("mqdb agent")?;
        }
        (None, false, Some(node_id), _) => {
            println!("Killing MQDB cluster node {node_id}...");
            pkill(&legacy_node_pattern(node_id))?;
        }
        (None, false, None, true) => {
            println!("Killing all MQDB processes...");
            pkill("mqdb cluster")?;
            pkill("mqdb agent")?;
        }
        (None, false, None, false) => {
            println!("Killing all MQDB cluster nodes...");
            pkill("mqdb cluster")?;
        }
    }
    println!("Done");
    Ok(())
}

fn is_dev_entry(rest: &str, is_dir: bool) -> bool {
    let tail_matches = |tail: &str| {
        if is_dir {
            tail.parse::<u16>().is_ok()
        } else {
            tail == "passwd"
        }
    };
    match rest.rsplit_once('-') {
        None => tail_matches(rest),
        Some((suite, tail)) => {
            !suite.is_empty() && suite.chars().all(|c| c.is_ascii_lowercase()) && tail_matches(tail)
        }
    }
}

fn clean_targets(db_prefix: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let name_prefix = Path::new(db_prefix)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(|name| format!("{name}-"))
        .ok_or("--db-prefix must end in a file name")?;
    let pattern = format!("{}-*", glob::Pattern::escape(db_prefix));
    let mut targets: Vec<PathBuf> = glob::glob(&pattern)?
        .filter_map(Result::ok)
        .filter(|path| {
            path.file_name()
                .and_then(std::ffi::OsStr::to_str)
                .and_then(|name| name.strip_prefix(&name_prefix))
                .is_some_and(|rest| {
                    (path.is_dir() || path.is_file()) && is_dev_entry(rest, path.is_dir())
                })
        })
        .collect();
    targets.sort();
    Ok(targets)
}

pub(crate) fn cmd_dev_clean(db_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = absolute_prefix(db_prefix)?;
    let entries = clean_targets(&prefix)?;
    let dirs: Vec<PathBuf> = entries
        .iter()
        .filter(|entry| entry.is_dir())
        .cloned()
        .collect();
    wait_until_unused(&dirs, &prefix)?;

    if entries.is_empty() {
        println!("No databases under --db-prefix {prefix}");
        return Ok(());
    }

    println!("Cleaning {} database(s):", entries.len());
    for entry in &entries {
        println!("  Removing: {}", entry.display());
        if entry.is_dir() {
            std::fs::remove_dir_all(entry)?;
        } else {
            std::fs::remove_file(entry)?;
        }
    }
    println!("Done");
    Ok(())
}

pub(crate) fn cmd_dev_logs(
    node: Option<u16>,
    pattern: Option<&str>,
    follow: bool,
    last: usize,
    db_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = absolute_prefix(db_prefix)?;
    let log_files: Vec<(u16, PathBuf)> = node_dirs(&prefix)?
        .into_iter()
        .filter(|(node_id, _)| node.is_none_or(|wanted| wanted == *node_id))
        .map(|(node_id, dir)| (node_id, dir.join("mqdb.log")))
        .filter(|(_, log_file)| log_file.is_file())
        .collect();

    if log_files.is_empty() {
        match node {
            Some(node_id) => eprintln!("Log file not found: {prefix}-{node_id}/mqdb.log"),
            None => println!("No log files found under --db-prefix {prefix}"),
        }
        return Ok(());
    }

    if follow {
        for (node_id, log_file) in &log_files {
            println!("=== Node {node_id} ({}) ===", log_file.display());
        }
        let mut cmd = Command::new("tail");
        cmd.args(["-f", "-n", &last.to_string()]);
        cmd.args(log_files.iter().map(|(_, log_file)| log_file));
        if let Some(pat) = pattern {
            let child = cmd.stdout(std::process::Stdio::piped()).spawn()?;
            let tail_output = child.stdout.ok_or("tail has no stdout")?;
            Command::new("grep")
                .args(["--line-buffered", pat])
                .stdin(tail_output)
                .status()?;
        } else {
            cmd.status()?;
        }
        return Ok(());
    }

    for (node_id, log_file) in &log_files {
        println!("=== Node {node_id} ({}) ===", log_file.display());
        let content = std::fs::read_to_string(log_file)?;
        let lines: Vec<&str> = content.lines().collect();
        let start = lines.len().saturating_sub(last);

        for line in &lines[start..] {
            if let Some(pat) = pattern {
                if line.contains(pat) {
                    println!("{line}");
                }
            } else {
                println!("{line}");
            }
        }
        println!();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_cluster_node, is_dev_entry, legacy_node_pattern};
    use std::process::{Child, Command};

    struct Sleeper(Child);

    impl Drop for Sleeper {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn sleeper(node_id: &str) -> Sleeper {
        Sleeper(
            Command::new("sh")
                .args([
                    "-c",
                    "while :; do sleep 1; done",
                    "stand-in",
                    "mqdb",
                    "cluster",
                    "start",
                    "--node-id",
                    node_id,
                    "--db",
                    "/nonexistent",
                ])
                .spawn()
                .expect("spawn a stand-in node"),
        )
    }

    #[test]
    fn the_legacy_node_pattern_matches_only_that_node_id() {
        let node_1 = sleeper("1");
        let node_10 = sleeper("10");
        let output = Command::new("pgrep")
            .args(["-f", &legacy_node_pattern(1)])
            .output()
            .expect("run pgrep");
        let pids: Vec<String> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::to_string)
            .collect();
        assert!(pids.contains(&node_1.0.id().to_string()), "{pids:?}");
        assert!(!pids.contains(&node_10.0.id().to_string()), "{pids:?}");
    }

    #[test]
    fn clean_keeps_entries_that_dev_does_not_create() {
        assert!(is_dev_entry("1", true));
        assert!(is_dev_entry("passwd", false));
        assert!(is_dev_entry("own-2", true));
        assert!(is_dev_entry("ownership-passwd", false));
        assert!(!is_dev_entry("passwd", true));
        assert!(!is_dev_entry("1", false));
        assert!(!is_dev_entry("own-1x", true));
        assert!(!is_dev_entry("Own-1", true));
        assert!(!is_dev_entry("-1", true));
        assert!(!is_dev_entry("notes.txt", false));
    }

    #[test]
    fn a_node_matches_only_its_own_db_dir() {
        let root = tempfile::TempDir::new().expect("temp dir");
        let node_1 = root.path().join("e2e-1");
        let node_10 = root.path().join("e2e-10");
        std::fs::create_dir_all(&node_1).expect("create node 1 dir");
        std::fs::create_dir_all(&node_10).expect("create node 10 dir");
        let node_1_arg = node_1.display();
        let command =
            format!("/bin/mqdb cluster start --node-id 1 --db {node_1_arg} --bind 127.0.0.1:32000");
        assert!(is_cluster_node(&command, &node_1));
        assert!(!is_cluster_node(&command, &node_10));
        assert!(!is_cluster_node(&command, root.path()));
        assert!(!is_cluster_node(
            &format!("/bin/mqdb agent start --db {node_1_arg}"),
            &node_1
        ));
        assert!(is_cluster_node(
            &format!("/bin/mqdb cluster start --db {node_1_arg}"),
            &node_1
        ));
    }
}
