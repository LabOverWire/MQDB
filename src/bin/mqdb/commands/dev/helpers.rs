use std::path::PathBuf;
use std::process::Command;

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

pub(crate) fn cmd_dev_kill(all: bool, node: Option<u16>, agent: bool) {
    if agent {
        println!("Killing MQDB agent...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return;
    }

    if let Some(node_id) = node {
        println!("Killing MQDB cluster node {node_id}...");
        let pattern = format!("mqdb cluster.*--node-id {node_id}");
        let _ = Command::new("pkill").args(["-f", &pattern]).status();
        println!("Done");
        return;
    }

    if all {
        println!("Killing all MQDB processes...");
        let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return;
    }

    println!("Killing all MQDB cluster nodes...");
    let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
    println!("Done");
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
