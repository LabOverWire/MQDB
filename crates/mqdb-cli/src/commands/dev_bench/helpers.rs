// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::process::Command;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};

use crate::cli_types::ConnectionArgs;

pub(super) fn is_agent_running() -> bool {
    Command::new("pgrep")
        .args(["-f", "mqdb agent"])
        .output()
        .is_ok_and(|o| !o.stdout.is_empty())
}

pub(super) fn start_agent_for_bench(
    exe: &std::path::Path,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(db)?;
    let log_file = std::fs::File::create(format!("{db}/mqdb.log"))?;

    #[cfg(feature = "dev-insecure")]
    let args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    Command::new(exe)
        .args(&args)
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()?;

    Ok(())
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
        if conn.insecure {
            client.set_insecure_tls(true).await;
        }
        let connected = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
            let opts = ConnectOptions::new(&client_id).with_credentials(user.clone(), pass.clone());
            Box::pin(client.connect_with_options(&conn.broker, opts))
                .await
                .is_ok()
        } else {
            client.connect(&conn.broker).await.is_ok()
        };

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
        .unwrap()
        .as_secs();
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

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    pub(super) fn calculate_percentile(&self, p: f64) -> u64 {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            if latencies.is_empty() {
                return 0;
            }
            latencies.sort_unstable();
            let idx = ((latencies.len() as f64) * p / 100.0) as usize;
            latencies[idx.min(latencies.len() - 1)]
        } else {
            0
        }
    }
}
