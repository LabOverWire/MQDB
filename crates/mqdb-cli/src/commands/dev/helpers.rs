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

fn until_next_flag(rest: &str) -> &str {
    rest.split_once(" --").map_or(rest, |(db, _)| db).trim()
}

fn db_argument(command: &str) -> Option<&str> {
    command
        .split_once(" --db=")
        .or_else(|| command.split_once(" --db "))
        .map(|(_, rest)| until_next_flag(rest))
}

fn is_node_command(command: &str) -> bool {
    let padded = format!(" {command} ");
    padded.contains(" cluster start ") || padded.contains(" agent start ")
}

enum DbLocation {
    Dir(PathBuf),
    Gone,
    Unresolved,
}

struct NodeProcess {
    pid: u32,
    db: DbLocation,
}

fn process_cwd(pid: u32) -> Option<PathBuf> {
    let output = Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "-d", "cwd", "-Fn"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix('n'))
        .map(PathBuf::from)
}

fn locate_db(pid: u32, db: &str) -> DbLocation {
    let db = Path::new(db);
    let full = if db.is_absolute() {
        Some(db.to_path_buf())
    } else {
        process_cwd(pid).map(|cwd| cwd.join(db))
    };
    match full {
        Some(full) => std::fs::canonicalize(full).map_or(DbLocation::Gone, DbLocation::Dir),
        None if process_exists(pid) => DbLocation::Unresolved,
        None => DbLocation::Gone,
    }
}

fn is_zombie(state: &str) -> bool {
    state.starts_with('Z')
}

fn process_exists(pid: u32) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output()
        .is_ok_and(|output| {
            let state = String::from_utf8_lossy(&output.stdout);
            let state = state.trim();
            !state.is_empty() && !is_zombie(state)
        })
}

fn node_processes() -> Result<Vec<NodeProcess>, String> {
    let output = Command::new("ps")
        .args(["-ww", "-ax", "-o", "pid=,stat=,command="])
        .output()
        .map_err(|e| format!("run ps: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps -ax: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let table = String::from_utf8_lossy(&output.stdout);
    Ok(table
        .lines()
        .filter_map(|line| {
            let (pid, rest) = line.trim_start().split_once(char::is_whitespace)?;
            let (state, command) = rest.trim_start().split_once(char::is_whitespace)?;
            let pid = pid.parse().ok().filter(|_| !is_zombie(state))?;
            let db = db_argument(command).filter(|_| is_node_command(command))?;
            Some(NodeProcess {
                pid,
                db: locate_db(pid, db),
            })
        })
        .collect())
}

enum NodeState {
    Running,
    Stopped,
}

fn node_state(processes: &[NodeProcess], pid: u32, node_dir: &Path) -> Result<NodeState, String> {
    match processes.iter().find(|process| process.pid == pid) {
        Some(NodeProcess {
            db: DbLocation::Unresolved,
            ..
        }) => Err(format!(
            "pid {pid} runs an mqdb node whose relative --db cannot be resolved"
        )),
        Some(NodeProcess {
            db: DbLocation::Dir(db),
            ..
        }) if std::fs::canonicalize(node_dir).is_ok_and(|dir| &dir == db) => Ok(NodeState::Running),
        Some(_) | None => Ok(NodeState::Stopped),
    }
}

fn dirs_in_use(dirs: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let processes = node_processes()?;
    let unresolved: Vec<String> = processes
        .iter()
        .filter(|process| matches!(process.db, DbLocation::Unresolved))
        .map(|process| process.pid.to_string())
        .collect();
    if !unresolved.is_empty() {
        return Err(format!(
            "cannot tell which directory mqdb pids {} use (relative --db); refusing",
            unresolved.join(", ")
        ));
    }
    let live: Vec<&PathBuf> = processes
        .iter()
        .filter_map(|process| match &process.db {
            DbLocation::Dir(dir) => Some(dir),
            DbLocation::Gone | DbLocation::Unresolved => None,
        })
        .collect();
    Ok(dirs
        .iter()
        .filter(|dir| std::fs::canonicalize(dir).is_ok_and(|dir| live.contains(&&dir)))
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

fn prefix_entries(db_prefix: &str) -> Result<Vec<(String, PathBuf)>, Box<dyn std::error::Error>> {
    let name_prefix = Path::new(db_prefix)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(|name| format!("{name}-"))
        .ok_or("--db-prefix must end in a file name")?;
    let pattern = format!("{}-*", glob::Pattern::escape(db_prefix));
    let mut entries: Vec<(String, PathBuf)> = glob::glob(&pattern)?
        .filter_map(Result::ok)
        .filter_map(|path| {
            let rest = path
                .file_name()?
                .to_str()?
                .strip_prefix(&name_prefix)?
                .to_string();
            Some((rest, path))
        })
        .collect();
    entries.sort();
    Ok(entries)
}

fn node_dirs(db_prefix: &str) -> Result<Vec<(u16, PathBuf)>, Box<dyn std::error::Error>> {
    let mut dirs: Vec<(u16, PathBuf)> = prefix_entries(db_prefix)?
        .into_iter()
        .filter(|(_, path)| path.is_dir())
        .filter_map(|(rest, path)| Some((rest.parse().ok()?, path)))
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

    let processes = node_processes()?;
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
        match node_state(&processes, pid, &target.dir) {
            Ok(NodeState::Running) => match signal(pid) {
                Ok(()) => signalled.push((target, pid)),
                Err(e) => match node_processes()
                    .and_then(|processes| node_state(&processes, pid, &target.dir))
                {
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
        let processes = match node_processes() {
            Ok(processes) => processes,
            Err(e) => {
                failures.push(e);
                break;
            }
        };
        let mut waiting = Vec::new();
        for (target, pid) in signalled {
            match node_state(&processes, pid, &target.dir) {
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

const DEV_TEST_SUITE_DIRS: [&str; 3] = ["own", "share", "presence"];
const DEV_TEST_SUITE_PASSWDS: [&str; 3] = ["ownership", "sharing", "presence"];

fn is_dev_test_prefix(db_prefix: &str) -> bool {
    let prefix = Path::new(db_prefix);
    let same_parent = prefix
        .parent()
        .and_then(|parent| std::fs::canonicalize(parent).ok())
        .is_some_and(|parent| std::fs::canonicalize("/tmp").is_ok_and(|tmp| parent == tmp));
    same_parent && prefix.file_name().is_some_and(|name| name == "mqdb-test")
}

fn is_dev_entry(rest: &str, is_dir: bool, with_dev_test_leftovers: bool) -> bool {
    match (rest.rsplit_once('-'), is_dir) {
        (None, true) => rest.parse::<u16>().is_ok(),
        (None, false) => rest == "passwd",
        (Some((suite, node_id)), true) => {
            with_dev_test_leftovers
                && DEV_TEST_SUITE_DIRS.contains(&suite)
                && node_id.parse::<u16>().is_ok()
        }
        (Some((suite, tail)), false) => {
            with_dev_test_leftovers && DEV_TEST_SUITE_PASSWDS.contains(&suite) && tail == "passwd"
        }
    }
}

fn clean_targets(db_prefix: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let with_dev_test_leftovers = is_dev_test_prefix(db_prefix);
    Ok(prefix_entries(db_prefix)?
        .into_iter()
        .filter(|(rest, path)| {
            (path.is_dir() || path.is_file())
                && is_dev_entry(rest, path.is_dir(), with_dev_test_leftovers)
        })
        .map(|(_, path)| path)
        .collect())
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
            let mut tail = cmd.stdout(std::process::Stdio::piped()).spawn()?;
            let tail_output = tail.stdout.take().ok_or("tail has no stdout")?;
            let grep = Command::new("grep")
                .args(["--line-buffered", pat])
                .stdin(tail_output)
                .status();
            if tail.try_wait()?.is_none() {
                tail.kill()?;
            }
            tail.wait()?;
            grep?;
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
    use super::{
        DbLocation, NodeProcess, NodeState, db_argument, is_dev_entry, is_dev_test_prefix,
        is_node_command, legacy_node_pattern, node_state,
    };
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
        assert!(is_dev_entry("1", true, false));
        assert!(is_dev_entry("passwd", false, false));
        assert!(!is_dev_entry("own-2", true, false));
        assert!(!is_dev_entry("presence-passwd", false, false));
        assert!(is_dev_entry("own-2", true, true));
        assert!(is_dev_entry("share-1", true, true));
        assert!(is_dev_entry("presence-3", true, true));
        assert!(is_dev_entry("ownership-passwd", false, true));
        assert!(is_dev_entry("sharing-passwd", false, true));
        assert!(is_dev_entry("presence-passwd", false, true));
        assert!(!is_dev_entry("x-1", true, true));
        assert!(!is_dev_entry("x-passwd", false, true));
        assert!(!is_dev_entry("passwd", true, true));
        assert!(!is_dev_entry("1", false, true));
        assert!(!is_dev_entry("own-1x", true, true));
        assert!(!is_dev_entry("notes.txt", false, true));
    }

    #[test]
    fn only_the_default_prefix_owns_the_dev_test_leftovers() {
        assert!(is_dev_test_prefix("/tmp/mqdb-test"));
        assert!(!is_dev_test_prefix("/tmp/mqdb-test2"));
        assert!(!is_dev_test_prefix("/tmp/foo"));
        assert!(!is_dev_test_prefix("/tmp/sub/mqdb-test"));
    }

    #[test]
    fn a_node_matches_only_its_own_db_dir() {
        let root = tempfile::TempDir::new().expect("temp dir");
        let node_1 = root.path().join("e2e-1");
        let node_10 = root.path().join("e2e-10");
        std::fs::create_dir_all(&node_1).expect("create node 1 dir");
        std::fs::create_dir_all(&node_10).expect("create node 10 dir");
        let processes = [
            NodeProcess {
                pid: 1,
                db: DbLocation::Dir(std::fs::canonicalize(&node_1).expect("canonical dir")),
            },
            NodeProcess {
                pid: 2,
                db: DbLocation::Unresolved,
            },
        ];
        assert!(matches!(
            node_state(&processes, 1, &node_1),
            Ok(NodeState::Running)
        ));
        assert!(matches!(
            node_state(&processes, 1, &node_10),
            Ok(NodeState::Stopped)
        ));
        assert!(matches!(
            node_state(&processes, 3, &node_1),
            Ok(NodeState::Stopped)
        ));
        assert!(node_state(&processes, 2, &node_1).is_err());
    }

    #[test]
    fn the_db_argument_is_read_in_both_forms() {
        assert_eq!(
            db_argument("mqdb cluster start --db /a/e2e-1 --bind x"),
            Some("/a/e2e-1")
        );
        assert_eq!(
            db_argument("mqdb cluster start --db=/a/e2e-1 --bind x"),
            Some("/a/e2e-1")
        );
        assert_eq!(db_argument("mqdb agent start --db rel"), Some("rel"));
        assert_eq!(db_argument("mqdb cluster start --bind x"), None);
        assert!(is_node_command("mqdb agent start --db x"));
        assert!(is_node_command("mqdb cluster start --db x"));
        assert!(!is_node_command("mqdb dev clean --db-prefix x"));
    }
}
