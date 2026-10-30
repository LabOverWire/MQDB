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

fn process_command(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-ww", "-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn runs_node(pid: u32, node_dir: &Path) -> bool {
    process_command(pid).is_some_and(|command| is_cluster_node(&command, node_dir))
}

pub(crate) fn absolute_prefix(db_prefix: &str) -> Result<String, Box<dyn std::error::Error>> {
    let path = std::path::absolute(db_prefix)?;
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
        match read_pid(&target.pid_file) {
            Err(e) => failures.push(format!("node {}: {e}", target.node_id)),
            Ok(pid) if !runs_node(pid, &target.dir) => {
                if let Err(e) = forget_node(&target, pid, "is not running; removed its pid file") {
                    failures.push(e);
                }
            }
            Ok(pid) => {
                Command::new("kill").arg(pid.to_string()).status()?;
                signalled.push((target, pid));
            }
        }
    }

    let start = Instant::now();
    while !signalled.is_empty() && start.elapsed() <= NODE_STOP_TIMEOUT {
        let (stopped, running): (Vec<_>, Vec<_>) = signalled
            .into_iter()
            .partition(|(target, pid)| !runs_node(*pid, &target.dir));
        for (target, pid) in stopped {
            if let Err(e) = forget_node(&target, pid, "stopped") {
                failures.push(e);
            }
        }
        signalled = running;
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

pub(crate) fn cmd_dev_kill(
    all: bool,
    node: Option<u16>,
    agent: bool,
    db_prefix: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(prefix) = db_prefix {
        return kill_cluster(prefix, node);
    }

    if agent {
        println!("Killing MQDB agent...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return Ok(());
    }

    if let Some(node_id) = node {
        println!("Killing MQDB cluster node {node_id}...");
        let pattern = format!("mqdb cluster.*--node-id {node_id}");
        let _ = Command::new("pkill").args(["-f", &pattern]).status();
        println!("Done");
        return Ok(());
    }

    if all {
        println!("Killing all MQDB processes...");
        let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return Ok(());
    }

    println!("Killing all MQDB cluster nodes...");
    let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
    println!("Done");
    Ok(())
}

pub(crate) fn cmd_dev_clean(db_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = absolute_prefix(db_prefix)?;
    let running: Vec<String> = recorded_nodes(&prefix)?
        .into_iter()
        .filter(|node| read_pid(&node.pid_file).is_ok_and(|pid| runs_node(pid, &node.dir)))
        .map(|node| node.node_id.to_string())
        .collect();
    if !running.is_empty() {
        return Err(format!(
            "nodes {} of --db-prefix {prefix} are still running; stop them with 'mqdb dev kill --db-prefix {prefix}'",
            running.join(", ")
        )
        .into());
    }

    let passwd = PathBuf::from(format!("{prefix}-passwd"));
    let entries: Vec<PathBuf> = node_dirs(&prefix)?
        .into_iter()
        .map(|(_, dir)| dir)
        .chain(passwd.is_file().then_some(passwd))
        .collect();

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

    for (node_id, log_file) in &log_files {
        println!("=== Node {node_id} ({}) ===", log_file.display());

        if follow {
            let mut cmd = Command::new("tail");
            cmd.args(["-f", "-n", &last.to_string()]);
            cmd.arg(log_file);
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
        } else {
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
        }
        println!();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_cluster_node;

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
