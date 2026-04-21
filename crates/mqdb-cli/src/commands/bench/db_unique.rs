// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use super::common::{BenchDbArgs, wait_for_broker_ready};
use crate::cli_types::OutputFormat;

const UNIQUE_FIELD_NAME: &str = "bench_unique_field";
const UNIQUE_FIELD_VALUE: &str = "contested-value";

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) async fn cmd_bench_db_unique(
    args: BenchDbArgs,
    attempts_per_client: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_broker_ready(&args.conn, 30).await?;

    println!(
        "Unique-contention benchmark: concurrency={}, attempts/client={}, entity={}",
        args.concurrency, attempts_per_client, args.entity
    );

    register_unique_constraint(&args).await?;

    let successes = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let success_latencies: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let conflict_latencies: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let start = Instant::now();
    let mut handles = Vec::new();

    for client_idx in 0..args.concurrency {
        let conn = args.conn.clone();
        let entity = args.entity.clone();
        let successes = Arc::clone(&successes);
        let conflicts = Arc::clone(&conflicts);
        let errors = Arc::clone(&errors);
        let success_latencies = Arc::clone(&success_latencies);
        let conflict_latencies = Arc::clone(&conflict_latencies);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-unique-{client_idx}-{}", uuid::Uuid::new_v4());
            let client = MqttClient::new(client_id.clone());
            if conn.insecure {
                client.set_insecure_tls(true).await;
            }
            if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                let opts = ConnectOptions::new(client_id.clone())
                    .with_credentials(user.clone(), pass.clone());
                if Box::pin(client.connect_with_options(&conn.broker, opts))
                    .await
                    .is_err()
                {
                    return;
                }
            } else if client.connect(&conn.broker).await.is_err() {
                return;
            }

            for attempt in 0..attempts_per_client {
                let response_topic =
                    format!("bench-unique-{client_idx}/resp/{}", uuid::Uuid::new_v4());
                let (tx, rx) = flume::bounded::<Vec<u8>>(1);
                let tx_clone = tx.clone();
                if client
                    .subscribe(&response_topic, move |msg| {
                        let _ = tx_clone.try_send(msg.payload.clone());
                    })
                    .await
                    .is_err()
                {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let record = json!({
                    UNIQUE_FIELD_NAME: UNIQUE_FIELD_VALUE,
                    "attempt": attempt,
                    "client_idx": client_idx,
                });
                let topic = format!("$DB/{entity}/create");
                let opts = PublishOptions {
                    properties: PublishProperties {
                        response_topic: Some(response_topic.clone()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                let op_start = Instant::now();
                if client
                    .publish_with_options(&topic, serde_json::to_vec(&record).unwrap(), opts)
                    .await
                    .is_err()
                {
                    errors.fetch_add(1, Ordering::Relaxed);
                    let _ = client.unsubscribe(&response_topic).await;
                    continue;
                }

                let resp = tokio::time::timeout(Duration::from_secs(10), rx.recv_async()).await;
                let latency_ns = op_start.elapsed().as_nanos() as u64;
                let _ = client.unsubscribe(&response_topic).await;

                match resp {
                    Ok(Ok(payload)) => {
                        let parsed: Option<Value> = serde_json::from_slice(&payload).ok();
                        let status = parsed
                            .as_ref()
                            .and_then(|v| v.get("status"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if status == "ok" {
                            successes.fetch_add(1, Ordering::Relaxed);
                            if let Ok(mut v) = success_latencies.lock() {
                                v.push(latency_ns);
                            }
                        } else {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            if let Ok(mut v) = conflict_latencies.lock() {
                                v.push(latency_ns);
                            }
                        }
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            let _ = client.disconnect().await;
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let successes_total = successes.load(Ordering::Relaxed);
    let conflicts_total = conflicts.load(Ordering::Relaxed);
    let errors_total = errors.load(Ordering::Relaxed);
    let attempts_total = successes_total + conflicts_total + errors_total;

    let (sp50, sp95, sp99) = percentiles(&success_latencies);
    let (cp50, cp95, cp99) = percentiles(&conflict_latencies);

    let result = json!({
        "op": "unique",
        "attempts_total": attempts_total,
        "successes_total": successes_total,
        "conflicts_total": conflicts_total,
        "errors_total": errors_total,
        "duration_secs": elapsed,
        "throughput_ops_sec": attempts_total as f64 / elapsed,
        "latency_success_p50_us": sp50 / 1000,
        "latency_success_p95_us": sp95 / 1000,
        "latency_success_p99_us": sp99 / 1000,
        "latency_conflict_p50_us": cp50 / 1000,
        "latency_conflict_p95_us": cp95 / 1000,
        "latency_conflict_p99_us": cp99 / 1000,
        "config": {
            "operations": attempts_total,
            "concurrency": args.concurrency,
            "op": "unique",
            "attempts_per_client": attempts_per_client,
        }
    });

    if matches!(args.format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("\n┌───────────────────────────────────────────┐");
        println!("│        UNIQUE-CONTENTION BENCHMARK        │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Attempts:           {attempts_total:>20} │");
        println!("│ Successes:          {successes_total:>20} │");
        println!("│ Conflicts:          {conflicts_total:>20} │");
        println!("│ Errors:             {errors_total:>20} │");
        println!("├───────────────────────────────────────────┤");
        let sp50_us = sp50 / 1000;
        let cp50_us = cp50 / 1000;
        println!("│ Success p50:        {sp50_us:>17} µs │");
        println!("│ Conflict p50:       {cp50_us:>17} µs │");
        println!("└───────────────────────────────────────────┘");
    }

    Ok(())
}

async fn register_unique_constraint(args: &BenchDbArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client_id = format!("bench-unique-admin-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(client_id.clone());
    if args.conn.insecure {
        client.set_insecure_tls(true).await;
    }
    if let (Some(user), Some(pass)) = (&args.conn.user, &args.conn.pass) {
        let opts = ConnectOptions::new(client_id).with_credentials(user.clone(), pass.clone());
        Box::pin(client.connect_with_options(&args.conn.broker, opts)).await?;
    } else {
        client.connect(&args.conn.broker).await?;
    }

    let response_topic = format!("bench-unique-admin/resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    let tx_clone = tx.clone();
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx_clone.try_send(msg.payload.clone());
        })
        .await?;

    let payload = json!({
        "type": "unique",
        "fields": [UNIQUE_FIELD_NAME],
    });
    let topic = format!("$DB/_admin/constraint/{}/add", args.entity);
    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    client
        .publish_with_options(&topic, serde_json::to_vec(&payload)?, opts)
        .await?;

    let _ = tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await;
    let _ = client.unsubscribe(&response_topic).await;
    let _ = client.disconnect().await;

    Ok(())
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn percentiles(latencies_ns: &Arc<std::sync::Mutex<Vec<u64>>>) -> (u64, u64, u64) {
    let Ok(mut v) = latencies_ns.lock() else {
        return (0, 0, 0);
    };
    if v.is_empty() {
        return (0, 0, 0);
    }
    v.sort_unstable();
    let pct = |p: f64| {
        let idx = ((v.len() as f64) * p / 100.0) as usize;
        v[idx.min(v.len() - 1)]
    };
    (pct(50.0), pct(95.0), pct(99.0))
}
