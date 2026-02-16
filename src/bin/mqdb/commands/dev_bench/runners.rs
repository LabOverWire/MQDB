// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};

use super::DevBenchResult;
use super::helpers::{BenchMetrics, chrono_timestamp};
use crate::cli_types::{ConnectionArgs, DevBenchScenario};

#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
pub(super) async fn run_pubsub_benchmark(
    conn: &ConnectionArgs,
    publishers: usize,
    subscribers: usize,
    duration: u64,
    size: usize,
    _qos: u8,
) -> Result<DevBenchResult, Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let metrics = Arc::new(BenchMetrics::default());
    let running = Arc::new(AtomicBool::new(true));

    println!(
        "\nRunning pub/sub benchmark: {publishers} pubs, {subscribers} subs, {duration}s, {size} bytes"
    );

    let mut sub_handles = Vec::new();
    for sub_id in 0..subscribers {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-sub-{sub_id}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            let metrics_clone = Arc::clone(&metrics);
            let _ = client
                .subscribe("bench/test", move |msg| {
                    let payload = &msg.payload;
                    if payload.len() >= 8
                        && let Ok(bytes) = payload[0..8].try_into()
                    {
                        let sent_ns = u64::from_be_bytes(bytes);
                        let recv_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64;
                        if recv_time > sent_ns {
                            metrics_clone.record_latency(recv_time - sent_ns);
                        }
                    }
                    metrics_clone
                        .messages_received
                        .fetch_add(1, Ordering::Relaxed);
                })
                .await;

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = client.disconnect().await;
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let start_time = Instant::now();
    let measure_duration = Duration::from_secs(duration);

    let mut pub_handles = Vec::new();
    for pub_id in 0..publishers {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-pub-{pub_id}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            while running.load(Ordering::Relaxed) {
                let mut payload = vec![0u8; size];
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                payload[0..8].copy_from_slice(&now.to_be_bytes());

                let _ = client.publish("bench/test", payload).await;
                metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
            }

            let _ = client.disconnect().await;
        });
        pub_handles.push(handle);
    }

    tokio::time::sleep(measure_duration).await;

    running.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;

    for handle in pub_handles {
        handle.abort();
    }
    for handle in sub_handles {
        handle.abort();
    }

    let elapsed = start_time.elapsed();
    let sent = metrics.messages_sent.load(Ordering::Relaxed);
    let throughput = sent as f64 / elapsed.as_secs_f64();
    let p50 = metrics.calculate_percentile(50.0) / 1000;
    let p95 = metrics.calculate_percentile(95.0) / 1000;
    let p99 = metrics.calculate_percentile(99.0) / 1000;

    Ok(DevBenchResult {
        scenario: "pubsub".to_string(),
        timestamp: chrono_timestamp(),
        throughput,
        latency_p50_us: p50,
        latency_p95_us: p95,
        latency_p99_us: p99,
        config: serde_json::json!({
            "publishers": publishers,
            "subscribers": subscribers,
            "duration_secs": duration,
            "size": size
        }),
    })
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub(super) async fn run_db_benchmark(
    conn: &ConnectionArgs,
    operations: u64,
    concurrency: usize,
    op: &str,
) -> Result<DevBenchResult, Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    println!("\nRunning DB benchmark: {operations} ops, {concurrency} concurrency, op={op}");

    let metrics = Arc::new(BenchMetrics::default());
    let ops_per_client = operations / concurrency.max(1) as u64;

    let start_time = Instant::now();

    let mut handles = Vec::new();
    for client_id_num in 0..concurrency {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let op = op.to_string();

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-db-{client_id_num}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            for i in 0..ops_per_client {
                let op_start = Instant::now();
                let entity = "dev_bench_entity";
                let id = format!("{client_id_num}-{i}");

                let result = match op.as_str() {
                    "insert" | "mixed" => {
                        let data = serde_json::json!({"field": "value", "num": i});
                        db_create(&client, entity, &data).await
                    }
                    "get" => db_read(&client, entity, &id).await,
                    _ => Ok(serde_json::Value::Null),
                };

                if result.is_ok() {
                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                    metrics.record_latency(op_start.elapsed().as_nanos() as u64);
                } else {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
            }

            let _ = client.disconnect().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    let completed = metrics.messages_sent.load(Ordering::Relaxed);
    let throughput = completed as f64 / elapsed.as_secs_f64();
    let p50 = metrics.calculate_percentile(50.0) / 1000;
    let p95 = metrics.calculate_percentile(95.0) / 1000;
    let p99 = metrics.calculate_percentile(99.0) / 1000;

    Ok(DevBenchResult {
        scenario: "db".to_string(),
        timestamp: chrono_timestamp(),
        throughput,
        latency_p50_us: p50,
        latency_p95_us: p95,
        latency_p99_us: p99,
        config: serde_json::json!({
            "operations": operations,
            "concurrency": concurrency,
            "op": op
        }),
    })
}

async fn db_create(
    client: &MqttClient,
    entity: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let topic = format!("$DB/{entity}");
    let payload = serde_json::to_vec(data)?;
    client.publish_with_options(&topic, payload, opts).await?;

    match tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await {
        Ok(Ok(payload)) => Ok(serde_json::from_slice(&payload)?),
        _ => Err("timeout".into()),
    }
}

async fn db_read(
    client: &MqttClient,
    entity: &str,
    id: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let topic = format!("$DB/{entity}/{id}");
    client
        .publish_with_options(&topic, Vec::new(), opts)
        .await?;

    match tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await {
        Ok(Ok(payload)) => Ok(serde_json::from_slice(&payload)?),
        _ => Err("timeout".into()),
    }
}

pub(super) fn print_bench_result(result: &DevBenchResult) {
    println!("\n┌───────────────────────────────────────────┐");
    println!(
        "│        DEV BENCH RESULTS ({:>6})        │",
        result.scenario.to_uppercase()
    );
    println!("├───────────────────────────────────────────┤");
    println!("│ Throughput:       {:>16.0} ops/s │", result.throughput);
    println!("├───────────────────────────────────────────┤");
    println!("│ Latency p50:        {:>17} µs │", result.latency_p50_us);
    println!("│ Latency p95:        {:>17} µs │", result.latency_p95_us);
    println!("│ Latency p99:        {:>17} µs │", result.latency_p99_us);
    println!("└───────────────────────────────────────────┘");
}

#[allow(clippy::cast_possible_wrap)]
pub(super) fn compare_with_baseline(
    current: &DevBenchResult,
    baseline_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline_json = std::fs::read_to_string(baseline_path)?;
    let baseline: DevBenchResult = serde_json::from_str(&baseline_json)?;

    let throughput_delta = (current.throughput - baseline.throughput) / baseline.throughput * 100.0;
    let p50_delta = current.latency_p50_us as i64 - baseline.latency_p50_us as i64;
    let p95_delta = current.latency_p95_us as i64 - baseline.latency_p95_us as i64;
    let p99_delta = current.latency_p99_us as i64 - baseline.latency_p99_us as i64;

    println!("\n┌───────────────────────────────────────────┐");
    println!("│           COMPARISON TO BASELINE          │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Throughput:          {throughput_delta:>+16.1}%  │");
    println!("│ Latency p50:         {p50_delta:>+16} µs │");
    println!("│ Latency p95:         {p95_delta:>+16} µs │");
    println!("│ Latency p99:         {p99_delta:>+16} µs │");
    println!("└───────────────────────────────────────────┘");

    Ok(())
}

pub(super) fn build_bench_args(scenario: &DevBenchScenario) -> Vec<String> {
    match scenario {
        DevBenchScenario::Pubsub {
            publishers,
            subscribers,
            duration,
            size,
            qos,
        } => vec![
            "bench".to_string(),
            "pubsub".to_string(),
            "--publishers".to_string(),
            publishers.to_string(),
            "--subscribers".to_string(),
            subscribers.to_string(),
            "--duration".to_string(),
            duration.to_string(),
            "--size".to_string(),
            size.to_string(),
            "--qos".to_string(),
            qos.to_string(),
        ],
        DevBenchScenario::Db {
            operations,
            concurrency,
            op,
        } => vec![
            "bench".to_string(),
            "db".to_string(),
            "--operations".to_string(),
            operations.to_string(),
            "--concurrency".to_string(),
            concurrency.to_string(),
            "--op".to_string(),
            op.clone(),
        ],
    }
}
