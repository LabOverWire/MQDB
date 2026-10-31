// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};

use crate::cli_types::ConnectionArgs;
use crate::common::connect_with_timeout;

pub(super) fn broker_listening(port: u16) -> bool {
    let local = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    TcpStream::connect_timeout(&local, Duration::from_millis(300)).is_ok()
}

pub(super) fn agent_args(db: &str, port: u16) -> Vec<String> {
    let mut args: Vec<String> = ["agent", "start", "--db", db, "--bind"]
        .iter()
        .map(ToString::to_string)
        .collect();
    args.push(format!("127.0.0.1:{port}"));
    #[cfg(feature = "dev-insecure")]
    args.push("--anonymous".to_string());
    args
}

pub(super) fn start_agent_for_bench(
    exe: &std::path::Path,
    db: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(db)?;
    let log_file = std::fs::File::create(format!("{db}/mqdb.log"))?;

    Command::new(exe)
        .args(agent_args(db, port))
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()?;

    Ok(())
}

struct ProcessEntry {
    pid: u32,
    ppid: u32,
    executable: String,
}

fn process_table() -> Result<Vec<ProcessEntry>, String> {
    let output = Command::new("ps")
        .args(["-ww", "-ax", "-o", "pid=,ppid=,ucomm="])
        .output()
        .map_err(|e| format!("run ps: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps -ax: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let (pid, rest) = line.trim_start().split_once(char::is_whitespace)?;
            let (ppid, executable) = rest.trim_start().split_once(char::is_whitespace)?;
            Some(ProcessEntry {
                pid: pid.parse().ok()?,
                ppid: ppid.parse().ok()?,
                executable: executable.trim().to_string(),
            })
        })
        .collect())
}

fn is_mqdb_binary(executable: &str) -> bool {
    Path::new(executable)
        .file_name()
        .is_some_and(|name| name == "mqdb")
}

fn descendants_of(table: &[ProcessEntry], root: u32) -> Vec<&ProcessEntry> {
    let mut frontier = vec![root];
    let mut found = Vec::new();
    while let Some(parent) = frontier.pop() {
        for entry in table.iter().filter(|entry| entry.ppid == parent) {
            frontier.push(entry.pid);
            found.push(entry);
        }
    }
    found
}

fn mqdb_processes_below(table: &[ProcessEntry], root: u32) -> Vec<u32> {
    descendants_of(table, root)
        .into_iter()
        .filter(|entry| is_mqdb_binary(&entry.executable))
        .map(|entry| entry.pid)
        .collect()
}

fn is_running(pid: u32) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output()
        .is_ok_and(|output| {
            let state = String::from_utf8_lossy(&output.stdout);
            let state = state.trim();
            !state.is_empty() && !state.starts_with('Z')
        })
}

fn send(signal: &str, pid: u32) -> Result<(), String> {
    let status = Command::new("kill")
        .args([signal, &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("run kill: {e}"))?;
    if status.success() || !is_running(pid) {
        Ok(())
    } else {
        Err(format!("kill {signal} {pid}: {status}"))
    }
}

fn send_each(signal: &str, pids: &[u32]) -> Result<(), String> {
    let failures: Vec<String> = pids
        .iter()
        .filter_map(|&pid| send(signal, pid).err())
        .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

pub(super) fn mqdb_agents_below(root: u32) -> Result<Vec<u32>, String> {
    Ok(mqdb_processes_below(&process_table()?, root))
}

pub(super) fn only_mqdb_binaries(pids: &[u32]) -> Result<Vec<u32>, String> {
    let table = process_table()?;
    Ok(pids
        .iter()
        .copied()
        .filter(|pid| {
            table
                .iter()
                .any(|entry| entry.pid == *pid && is_mqdb_binary(&entry.executable))
        })
        .collect())
}

pub(super) fn signal_pids(signal: &str, pids: &[u32]) -> Result<(), String> {
    send_each(signal, pids)
}

pub(super) fn kill_processes_below(root: u32) -> Result<(), String> {
    let table = process_table()?;
    let below: Vec<u32> = descendants_of(&table, root)
        .into_iter()
        .map(|entry| entry.pid)
        .collect();
    send_each("-KILL", &below)
}

pub(super) async fn wait_for_broker_ready(
    conn: &ConnectionArgs,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    print!("Waiting for broker...");
    std::io::Write::flush(&mut std::io::stdout())?;

    loop {
        if start.elapsed() > timeout {
            println!(" timeout!");
            return Err(format!("Broker not ready after {timeout_secs}s").into());
        }

        let client_id = format!("bench-ready-{}", uuid::Uuid::new_v4());
        let client = MqttClient::new(&client_id);
        let connected = connect_with_timeout(&client, &client_id, conn)
            .await
            .is_ok();

        if connected {
            let response_topic = format!("bench-ready/{}", uuid::Uuid::new_v4());
            let (payload_tx, payload_rx) = flume::bounded::<Vec<u8>>(1);

            let sub_ok = client
                .subscribe(&response_topic, move |msg| {
                    let _ = payload_tx.try_send(msg.payload.clone());
                })
                .await
                .is_ok();

            if sub_ok {
                let opts = PublishOptions {
                    properties: PublishProperties {
                        response_topic: Some(response_topic.clone()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                let _ = client
                    .publish_with_options("$DB/_health", b"{}".to_vec(), opts)
                    .await;

                if let Ok(Ok(payload)) =
                    tokio::time::timeout(Duration::from_secs(2), payload_rx.recv_async()).await
                    && let Ok(json) = serde_json::from_slice::<serde_json::Value>(&payload)
                    && json
                        .get("data")
                        .and_then(|d| d.get("ready"))
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                {
                    let _ = client.disconnect().await;
                    println!(" ready!");
                    return Ok(());
                }
            }
            let _ = client.disconnect().await;
        }

        print!(".");
        std::io::Write::flush(&mut std::io::stdout())?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(super) fn chrono_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    format!("{now}")
}

#[derive(Default)]
pub(super) struct BenchMetrics {
    pub(super) messages_sent: std::sync::atomic::AtomicU64,
    pub(super) messages_received: std::sync::atomic::AtomicU64,
    pub(super) latencies_ns: std::sync::Mutex<Vec<u64>>,
    pub(super) errors: std::sync::atomic::AtomicU64,
}

impl BenchMetrics {
    pub(super) fn record_latency(&self, ns: u64) {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            latencies.push(ns);
        }
    }

    pub(super) fn calculate_percentile(&self, percent: usize) -> u64 {
        self.latencies_ns.lock().map_or(0, |mut latencies| {
            latencies.sort_unstable();
            let idx = latencies.len() * percent / 100;
            latencies
                .get(idx.min(latencies.len().saturating_sub(1)))
                .copied()
                .unwrap_or(0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessEntry, mqdb_processes_below};

    fn entry(pid: u32, ppid: u32, executable: &str) -> ProcessEntry {
        ProcessEntry {
            pid,
            ppid,
            executable: executable.to_string(),
        }
    }

    #[test]
    fn only_mqdb_binaries_below_the_profiler_are_found() {
        let table = [
            entry(10, 1, "/Users/me/.cargo/bin/samply"),
            entry(11, 10, "/repos/My Disk/target/profiling/mqdb"),
            entry(12, 10, "/x/mqdb-helper"),
            entry(20, 1, "/Users/me/.cargo/bin/cargo"),
            entry(21, 20, "/Users/me/.cargo/bin/cargo-flamegraph"),
            entry(22, 21, "/usr/bin/xctrace"),
            entry(23, 22, "/repos/My Disk/target/profiling/mqdb"),
            entry(30, 1, "/y/target/release/mqdb"),
            entry(40, 1, "perf"),
            entry(41, 40, "mqdb"),
        ];
        assert_eq!(mqdb_processes_below(&table, 10), [11]);
        assert_eq!(mqdb_processes_below(&table, 20), [23]);
        assert_eq!(mqdb_processes_below(&table, 40), [41]);
        assert!(mqdb_processes_below(&table, 30).is_empty());
        assert!(mqdb_processes_below(&table, 99).is_empty());
    }
}
