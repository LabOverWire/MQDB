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

fn is_cluster_node(command: &str, db_path: &str) -> bool {
    let padded = format!(" {command} ");
    padded.contains(" cluster start ") && padded.contains(&format!(" --db {db_path} "))
}

fn process_command(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn runs_node(pid: u32, db_path: &str) -> bool {
    process_command(pid).is_some_and(|command| is_cluster_node(&command, db_path))
}

fn started_nodes(
    db_prefix: &str,
    node: Option<u16>,
) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
    let node_name_prefix = Path::new(db_prefix)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(|name| format!("{name}-"))
        .ok_or("--db-prefix must end in a file name")?;
    let pattern = format!("{}-*/{NODE_PID_FILE}", glob::Pattern::escape(db_prefix));
    let mut node_ids: Vec<u16> = glob::glob(&pattern)?
        .filter_map(Result::ok)
        .filter_map(|pid_file| {
            let node_dir = pid_file.parent()?.file_name()?.to_str()?;
            node_dir.strip_prefix(&node_name_prefix)?.parse().ok()
        })
        .filter(|node_id| node.is_none_or(|wanted| wanted == *node_id))
        .collect();
    node_ids.sort_unstable();
    Ok(node_ids)
}

enum NodeStop {
    Stopped,
    NotRunning,
    StillRunning,
}

fn stop_node(pid: u32, db_path: &str) -> Result<NodeStop, Box<dyn std::error::Error>> {
    if !runs_node(pid, db_path) {
        return Ok(NodeStop::NotRunning);
    }
    Command::new("kill").arg(pid.to_string()).status()?;
    let start = Instant::now();
    while runs_node(pid, db_path) {
        if start.elapsed() > NODE_STOP_TIMEOUT {
            return Ok(NodeStop::StillRunning);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(NodeStop::Stopped)
}

fn read_pid(pid_file: &Path) -> Result<u32, Box<dyn std::error::Error>> {
    let pid = std::fs::read_to_string(pid_file)?
        .trim()
        .parse()
        .map_err(|e| format!("{}: {e}", pid_file.display()))?;
    Ok(pid)
}

fn kill_cluster(db_prefix: &str, node: Option<u16>) -> Result<(), Box<dyn std::error::Error>> {
    let node_ids = started_nodes(db_prefix, node)?;
    if node_ids.is_empty() {
        println!("No nodes started with --db-prefix {db_prefix}");
        return Ok(());
    }
    let mut still_running = Vec::new();
    for node_id in node_ids {
        let db_path = format!("{db_prefix}-{node_id}");
        let pid_file = PathBuf::from(format!("{db_path}/{NODE_PID_FILE}"));
        let pid = read_pid(&pid_file)?;
        match stop_node(pid, &db_path)? {
            NodeStop::Stopped => {
                println!("Stopped node {node_id} (pid {pid})");
                std::fs::remove_file(&pid_file)?;
            }
            NodeStop::NotRunning => {
                println!("Node {node_id} (pid {pid}) is not running; removing its pid file");
                std::fs::remove_file(&pid_file)?;
            }
            NodeStop::StillRunning => {
                eprintln!("Node {node_id} (pid {pid}) did not stop within {NODE_STOP_TIMEOUT:?}");
                still_running.push(node_id.to_string());
            }
        }
    }
    if still_running.is_empty() {
        Ok(())
    } else {
        Err(format!("nodes still running: {}", still_running.join(", ")).into())
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
    let pattern = format!("{db_prefix}-*");
    let entries: Vec<_> = glob::glob(&pattern)?.filter_map(Result::ok).collect();

    if entries.is_empty() {
        println!("No databases matching {pattern}");
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
    let log_files: Vec<PathBuf> = if let Some(node_id) = node {
        let log_path = PathBuf::from(format!("{db_prefix}-{node_id}/mqdb.log"));
        if log_path.exists() {
            vec![log_path]
        } else {
            eprintln!("Log file not found: {}", log_path.display());
            return Ok(());
        }
    } else {
        glob::glob(&format!("{db_prefix}-*/mqdb.log"))?
            .filter_map(Result::ok)
            .collect()
    };

    if log_files.is_empty() {
        println!("No log files found matching {db_prefix}-*/mqdb.log");
        return Ok(());
    }

    for log_file in &log_files {
        let node_id = log_file
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .and_then(|s| {
                s.strip_prefix(&format!(
                    "{}-",
                    db_prefix.rsplit('/').next().unwrap_or("mqdb-test")
                ))
            })
            .unwrap_or("?");

        println!("=== Node {node_id} ({}) ===", log_file.display());

        if follow {
            let mut cmd = Command::new("tail");
            cmd.args(["-f", "-n", &last.to_string()]);
            cmd.arg(log_file);
            if let Some(pat) = pattern {
                let child = cmd.stdout(std::process::Stdio::piped()).spawn()?;
                Command::new("grep")
                    .args(["--line-buffered", pat])
                    .stdin(child.stdout.unwrap())
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
    fn a_node_matches_only_its_own_db_path() {
        let command = "/bin/mqdb cluster start --node-id 1 --db /tmp/e2e-1 --bind 127.0.0.1:32000";
        assert!(is_cluster_node(command, "/tmp/e2e-1"));
        assert!(!is_cluster_node(command, "/tmp/e2e-10"));
        assert!(!is_cluster_node(command, "/tmp/e2e"));
        assert!(!is_cluster_node(
            "/bin/mqdb agent start --db /tmp/e2e-1",
            "/tmp/e2e-1"
        ));
        assert!(is_cluster_node(
            "/bin/mqdb cluster start --db /tmp/e2e-1",
            "/tmp/e2e-1"
        ));
    }
}
