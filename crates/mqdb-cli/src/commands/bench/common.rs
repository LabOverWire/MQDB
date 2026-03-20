// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use crate::cli_types::{ConnectionArgs, OutputFormat};

pub(crate) struct BenchDbArgs {
    pub(crate) operations: u64,
    pub(crate) entity: String,
    pub(crate) op: String,
    pub(crate) concurrency: usize,
    pub(crate) fields: usize,
    pub(crate) field_size: usize,
    pub(crate) warmup: u64,
    pub(crate) cleanup: bool,
    pub(crate) seed: u64,
    pub(crate) no_latency: bool,
    pub(crate) async_mode: bool,
    pub(crate) qos: u8,
    pub(crate) duration: Option<u64>,
    pub(crate) conn: ConnectionArgs,
    pub(crate) format: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DbOp {
    Insert,
    Get,
    Update,
    Delete,
    List,
    Mixed,
}

impl DbOp {
    pub(super) fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "insert" => Some(Self::Insert),
            "get" => Some(Self::Get),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            "list" => Some(Self::List),
            "mixed" => Some(Self::Mixed),
            _ => None,
        }
    }
}

#[derive(Default)]
pub(super) struct DbBenchMetrics {
    pub(super) inserts: std::sync::atomic::AtomicU64,
    pub(super) gets: std::sync::atomic::AtomicU64,
    pub(super) updates: std::sync::atomic::AtomicU64,
    pub(super) deletes: std::sync::atomic::AtomicU64,
    pub(super) lists: std::sync::atomic::AtomicU64,
    pub(super) errors: std::sync::atomic::AtomicU64,
    pub(super) latencies_ns: std::sync::Mutex<Vec<u64>>,
}

impl DbBenchMetrics {
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

    pub(super) fn total_ops(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.inserts.load(Ordering::Relaxed)
            + self.gets.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
            + self.lists.load(Ordering::Relaxed)
    }
}

pub(crate) fn generate_record(fields: usize, field_size: usize, id: u64) -> Value {
    let mut record = serde_json::Map::new();
    let value_template: String = (0..field_size).map(|_| 'x').collect();
    for i in 0..fields {
        record.insert(
            format!("field_{i}"),
            json!(format!("{value_template}_{id}")),
        );
    }
    Value::Object(record)
}

pub(crate) async fn wait_for_broker_ready(
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
