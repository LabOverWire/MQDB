// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::net::{Ipv4Addr, SocketAddr, TcpStream};
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
    command: String,
}

fn process_table() -> Result<Vec<ProcessEntry>, String> {
    let output = Command::new("ps")
        .args(["-ww", "-ax", "-o", "pid=,ppid=,command="])
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
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let ppid = fields.next()?.parse().ok()?;
            let command = fields.collect::<Vec<_>>().join(" ");
            Some(ProcessEntry { pid, ppid, command })
        })
        .collect())
}

fn is_mqdb_agent(command: &str) -> bool {
    command
        .split_once(" agent start ")
        .is_some_and(|(program, _)| program == "mqdb" || program.ends_with("/mqdb"))
}

fn agents_below(table: &[ProcessEntry], root: u32) -> Vec<u32> {
    let mut frontier = vec![root];
    let mut found = Vec::new();
    while let Some(parent) = frontier.pop() {
        for entry in table.iter().filter(|entry| entry.ppid == parent) {
            frontier.push(entry.pid);
            if is_mqdb_agent(&entry.command) {
                found.push(entry.pid);
            }
        }
    }
    found
}

pub(super) fn interrupt_agents_under(root: u32) -> Result<Vec<u32>, String> {
    let agents = agents_below(&process_table()?, root);
    for pid in &agents {
        let status = Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()
            .map_err(|e| format!("run kill: {e}"))?;
        if !status.success() {
            return Err(format!("kill -INT {pid}: {status}"));
        }
    }
    Ok(agents)
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
    use super::{ProcessEntry, agents_below};

    fn entry(pid: u32, ppid: u32, command: &str) -> ProcessEntry {
        ProcessEntry {
            pid,
            ppid,
            command: command.to_string(),
        }
    }

    #[test]
    fn only_agents_below_the_profiler_are_found() {
        let table = [
            entry(10, 1, "samply record -- mqdb agent start --db /a"),
            entry(
                11,
                10,
                "/repos/My Disk/target/profiling/mqdb agent start --db /a",
            ),
            entry(12, 10, "/x/mqdb-helper agent start --db /a"),
            entry(20, 1, "cargo flamegraph -- agent start --db /b"),
            entry(21, 20, "dtrace -c mqdb agent start --db /b"),
            entry(22, 21, "/x/mqdb agent start --db /b --bind 127.0.0.1:1884"),
            entry(
                30,
                1,
                "/y/mqdb agent start --db /other --bind 127.0.0.1:1883",
            ),
        ];
        assert_eq!(agents_below(&table, 10), [11]);
        assert_eq!(agents_below(&table, 20), [22]);
        assert!(agents_below(&table, 30).is_empty());
        assert!(agents_below(&table, 99).is_empty());
    }
}
