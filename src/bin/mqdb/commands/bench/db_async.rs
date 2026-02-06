// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::time::Duration;

use mqtt5::QoS;
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde_json::{Value, json};

use super::common::{BenchDbArgs, DbOp, generate_record};

const RAMP_INITIAL_RATE: u64 = 500;
const RAMP_INITIAL_STEP_PCT: f64 = 0.50;
const RAMP_MIN_STEP_PCT: f64 = 0.05;
const RAMP_INTERVAL_MS: u64 = 2000;
const RAMP_THROUGHPUT_THRESHOLD: f64 = 0.90;
const RAMP_LATENCY_SLOWDOWN_FACTOR: f64 = 1.5;
const OVERLOAD_STEPS: u32 = 2;
const STABILIZATION_SECS: u64 = 3;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
pub(super) async fn cmd_bench_db_async(
    args: &BenchDbArgs,
    op: DbOp,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;

    if op == DbOp::Mixed {
        return Err("Async mode does not support mixed operations".into());
    }

    let record_size_approx = args.fields * (args.field_size + 20);
    let duration_secs = args.duration.unwrap_or(30);

    let qos_str = match args.qos {
        0 => "QoS 0",
        1 => "QoS 1",
        _ => "QoS 2",
    };
    println!(
        "DB Benchmark (ASYNC): {duration_secs}s duration, op={op:?}, ~{record_size_approx} bytes/record, {qos_str}"
    );

    let client_id = format!("bench-db-async-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(client_id.clone());
    if args.conn.insecure {
        client.set_insecure_tls(true).await;
    }
    client.connect(&args.conn.broker).await?;

    let seeded_ids: Arc<Vec<String>> = if matches!(op, DbOp::Get | DbOp::Update | DbOp::Delete) {
        let seed_count = args.seed.max(1000);
        println!("Seeding {seed_count} records for {op:?} benchmark...");
        let mut ids = Vec::with_capacity(seed_count as usize);
        for i in 0..seed_count {
            let record = generate_record(args.fields, args.field_size, i);
            let topic = format!("$DB/{}/create", args.entity);
            let response_topic = format!("{client_id}/resp/{i}");

            let (tx, rx) = flume::bounded::<Option<String>>(1);
            let tx_clone = tx.clone();
            client
                .subscribe(&response_topic, move |msg| {
                    let actual_id =
                        serde_json::from_slice::<Value>(&msg.payload)
                            .ok()
                            .and_then(|v| {
                                v.get("id")
                                    .and_then(|id| id.as_str().map(String::from))
                                    .or_else(|| {
                                        v.get("data")?.get("id")?.as_str().map(String::from)
                                    })
                            });
                    let _ = tx_clone.try_send(actual_id);
                })
                .await?;

            let opts = PublishOptions {
                properties: PublishProperties {
                    response_topic: Some(response_topic.clone()),
                    ..Default::default()
                },
                ..Default::default()
            };
            client
                .publish_with_options(&topic, serde_json::to_vec(&record).unwrap(), opts)
                .await?;

            if let Some(actual_id) = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
            {
                ids.push(actual_id);
            }
            let _ = client.unsubscribe(&response_topic).await;

            if (i + 1) % 500 == 0 {
                println!("  Seeded {} records...", i + 1);
            }
        }
        println!("Seeding complete. {} records ready.", ids.len());
        Arc::new(ids)
    } else {
        Arc::new(Vec::new())
    };

    let response_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let response_count_clone = Arc::clone(&response_count);
    let error_count_clone = Arc::clone(&error_count);

    let pending_times: Arc<std::sync::Mutex<HashMap<u64, Instant>>> =
        Arc::new(std::sync::Mutex::new(HashMap::with_capacity(10000)));
    let latencies: Arc<std::sync::Mutex<Vec<f64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(100_000)));
    let pending_times_resp = Arc::clone(&pending_times);
    let latencies_resp = Arc::clone(&latencies);

    let response_topic = format!("{client_id}/responses/#");
    client
        .subscribe(&response_topic, move |msg| {
            let corr_id = msg
                .topic
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u64>().ok());

            if let Some(id) = corr_id
                && let Ok(mut pending) = pending_times_resp.lock()
                && let Some(sent_at) = pending.remove(&id)
            {
                let latency_ms = sent_at.elapsed().as_secs_f64() * 1000.0;
                if let Ok(mut lats) = latencies_resp.lock() {
                    lats.push(latency_ms);
                }
            }

            if let Ok(v) = serde_json::from_slice::<Value>(&msg.payload) {
                if v.get("status").and_then(Value::as_str) == Some("ok") {
                    response_count_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    error_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                error_count_clone.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await?;

    println!("Ramping up from {RAMP_INITIAL_RATE} ops/s...");

    let stop_flag = Arc::new(AtomicBool::new(false));
    let published_count = Arc::new(AtomicU64::new(0));
    let target_rate = Arc::new(AtomicU64::new(RAMP_INITIAL_RATE));
    let delete_index = Arc::new(AtomicU64::new(0));

    let stop_clone = Arc::clone(&stop_flag);
    let published_clone = Arc::clone(&published_count);
    let target_rate_clone = Arc::clone(&target_rate);
    let delete_index_clone = Arc::clone(&delete_index);
    let pending_times_pub = Arc::clone(&pending_times);
    let client_clone = client.clone();
    let entity = args.entity.clone();
    let fields = args.fields;
    let field_size = args.field_size;
    let client_id_clone = client_id.clone();
    let qos = args.qos;
    let seeded_ids_clone = Arc::clone(&seeded_ids);

    let publisher_handle = tokio::spawn(async move {
        let mut i = 0u64;
        let mut interval_start = Instant::now();
        let mut ops_this_interval = 0u64;

        while !stop_clone.load(Ordering::Relaxed) {
            let current_rate = target_rate_clone.load(Ordering::Relaxed);
            let interval_elapsed = interval_start.elapsed().as_secs_f64();
            let expected_ops = (current_rate as f64 * interval_elapsed) as u64;

            if ops_this_interval < expected_ops {
                let (topic, payload) = match op {
                    DbOp::Insert => {
                        let record = generate_record(fields, field_size, i);
                        (
                            format!("$DB/{entity}/create"),
                            serde_json::to_vec(&record).unwrap(),
                        )
                    }
                    DbOp::Get => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let id = &seeded_ids_clone[i as usize % seeded_ids_clone.len()];
                        (format!("$DB/{entity}/{id}"), b"{}".to_vec())
                    }
                    DbOp::Update => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let id = &seeded_ids_clone[i as usize % seeded_ids_clone.len()];
                        let record = generate_record(fields, field_size, i);
                        (
                            format!("$DB/{entity}/{id}/update"),
                            serde_json::to_vec(&record).unwrap(),
                        )
                    }
                    DbOp::Delete => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let idx = delete_index_clone.fetch_add(1, Ordering::Relaxed) as usize;
                        if idx >= seeded_ids_clone.len() {
                            break;
                        }
                        let id = &seeded_ids_clone[idx];
                        (format!("$DB/{entity}/{id}/delete"), b"{}".to_vec())
                    }
                    DbOp::List => (
                        format!("$DB/{entity}/list"),
                        serde_json::to_vec(&json!({"limit": 10})).unwrap(),
                    ),
                    DbOp::Mixed => unreachable!(),
                };

                let correlation_id = format!("{i}");
                let individual_response = format!("{client_id_clone}/responses/{correlation_id}");

                let qos_level = match qos {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    _ => QoS::ExactlyOnce,
                };
                let opts = PublishOptions {
                    qos: qos_level,
                    properties: PublishProperties {
                        response_topic: Some(individual_response),
                        correlation_data: Some(correlation_id.into_bytes()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                if let Ok(mut pending) = pending_times_pub.lock() {
                    pending.insert(i, Instant::now());
                }

                if client_clone
                    .publish_with_options(&topic, payload, opts)
                    .await
                    .is_ok()
                {
                    published_clone.fetch_add(1, Ordering::Relaxed);
                    ops_this_interval += 1;
                }
                i += 1;
            } else {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            if interval_elapsed >= 1.0 {
                interval_start = Instant::now();
                ops_this_interval = 0;
            }
        }
    });

    let start_time = Instant::now();
    let mut last_response_count = 0u64;
    let mut last_latency_count = 0usize;
    let mut current_rate = RAMP_INITIAL_RATE;
    let mut current_step_pct = RAMP_INITIAL_STEP_PCT;
    let mut saturation_rate: Option<f64> = None;
    let mut overload_remaining = 0u32;
    let mut peak_rate = 0u64;
    let mut baseline_latency: Option<f64> = None;
    let backlog_threshold_secs = 2.0;

    while start_time.elapsed().as_secs() < duration_secs {
        tokio::time::sleep(Duration::from_millis(RAMP_INTERVAL_MS)).await;

        let current_published = published_count.load(Ordering::Relaxed);
        let current_responses = response_count.load(Ordering::Relaxed);
        let backlog = current_published.saturating_sub(current_responses);
        let interval_responses = current_responses.saturating_sub(last_response_count);
        let interval_throughput = interval_responses as f64 / (RAMP_INTERVAL_MS as f64 / 1000.0);
        let backlog_limit = (current_rate as f64 * backlog_threshold_secs) as u64;

        let (interval_p50, interval_p95) = if let Ok(lats) = latencies.lock() {
            let new_count = lats.len();
            if new_count > last_latency_count {
                let mut interval_lats: Vec<f64> = lats[last_latency_count..new_count].to_vec();
                interval_lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let p50_idx = interval_lats.len() / 2;
                let p95_idx = (interval_lats.len() as f64 * 0.95) as usize;
                let p50 = interval_lats.get(p50_idx).copied().unwrap_or(0.0);
                let p95 = interval_lats
                    .get(p95_idx.min(interval_lats.len().saturating_sub(1)))
                    .copied()
                    .unwrap_or(0.0);
                last_latency_count = new_count;
                (p50, p95)
            } else {
                (0.0, 0.0)
            }
        } else {
            (0.0, 0.0)
        };

        if baseline_latency.is_none() && interval_p50 > 0.0 {
            baseline_latency = Some(interval_p50);
        }

        let phase = if saturation_rate.is_none() {
            "ramp"
        } else if overload_remaining > 0 {
            "overload"
        } else {
            "stable"
        };
        println!(
            "  [{phase}] Rate: {current_rate} target, {interval_throughput:.0} actual, backlog: {backlog}, p50: {interval_p50:.1}ms, p95: {interval_p95:.1}ms"
        );

        if saturation_rate.is_none() {
            let throughput_ok =
                interval_throughput >= current_rate as f64 * RAMP_THROUGHPUT_THRESHOLD;
            let backlog_ok = backlog < backlog_limit;
            let latency_ok = baseline_latency
                .is_none_or(|base| interval_p50 < base * RAMP_LATENCY_SLOWDOWN_FACTOR);

            if throughput_ok && backlog_ok && latency_ok {
                let step = (current_rate as f64 * current_step_pct) as u64;
                let step = step.max(50);
                current_rate += step;
                target_rate.store(current_rate, Ordering::Relaxed);
            } else if !latency_ok && throughput_ok && backlog_ok {
                current_step_pct = (current_step_pct / 2.0).max(RAMP_MIN_STEP_PCT);
                let step = (current_rate as f64 * current_step_pct) as u64;
                let step = step.max(50);
                current_rate += step;
                target_rate.store(current_rate, Ordering::Relaxed);
                println!(
                    "  Latency increasing, reducing step to {:.0}%",
                    current_step_pct * 100.0
                );
            } else {
                let reason = if throughput_ok {
                    "backlog growth"
                } else {
                    "throughput drop"
                };
                saturation_rate = Some(interval_throughput);
                println!("Saturation detected ({reason}) at {interval_throughput:.0} ops/s");
                overload_remaining = OVERLOAD_STEPS;
                let step = (current_rate as f64 * RAMP_MIN_STEP_PCT) as u64;
                current_rate += step.max(50);
                target_rate.store(current_rate, Ordering::Relaxed);
                println!("Entering overload phase at {current_rate} ops/s...");
            }
        } else if overload_remaining > 0 {
            overload_remaining -= 1;
            if overload_remaining > 0 {
                let step = (current_rate as f64 * RAMP_MIN_STEP_PCT) as u64;
                current_rate += step.max(50);
                target_rate.store(current_rate, Ordering::Relaxed);
            } else {
                peak_rate = current_rate;
                let stable_rate = saturation_rate.unwrap_or(interval_throughput) as u64;
                println!(
                    "Overload complete (peak {peak_rate}), stabilizing at {stable_rate} ops/s..."
                );
                target_rate.store(stable_rate, Ordering::Relaxed);
            }
        }

        last_response_count = current_responses;

        if saturation_rate.is_some() && overload_remaining == 0 {
            tokio::time::sleep(Duration::from_secs(STABILIZATION_SECS)).await;
            break;
        }
    }

    stop_flag.store(true, Ordering::Relaxed);
    let _ = publisher_handle.await;

    let published = published_count.load(Ordering::Relaxed);
    println!("Published {published} operations, waiting for responses...");

    let wait_timeout = Duration::from_secs(10);
    let wait_start = Instant::now();
    loop {
        let received = response_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);
        let total = received + errors;

        if total >= published {
            break;
        }

        if wait_start.elapsed() > wait_timeout {
            println!("Timeout: {received}/{published} responses received");
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let total_elapsed = start_time.elapsed();
    let received = response_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    let _ = client.unsubscribe(&response_topic).await;
    let _ = client.disconnect().await;

    let throughput = received as f64 / total_elapsed.as_secs_f64();
    let saturation_display = saturation_rate.unwrap_or(throughput);
    let duration = total_elapsed.as_secs_f64();

    let (lat_min, lat_p50, lat_p95, lat_p99, lat_max) = if let Ok(mut lats) = latencies.lock() {
        if lats.is_empty() {
            (0.0, 0.0, 0.0, 0.0, 0.0)
        } else {
            lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let len = lats.len();
            let p50 = lats[len / 2];
            let p95 = lats[(len as f64 * 0.95) as usize];
            let p99 = lats[(len as f64 * 0.99) as usize];
            (lats[0], p50, p95, p99, lats[len - 1])
        }
    } else {
        (0.0, 0.0, 0.0, 0.0, 0.0)
    };

    println!("\n┌───────────────────────────────────────────┐");
    println!("│           DB Benchmark Results            │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Mode:                              ASYNC  │");
    println!("│ Duration:              {duration:>10.2} s      │");
    println!("│ Published:             {published:>10}         │");
    println!("│ Successful:            {received:>10}         │");
    println!("│ Errors:                {errors:>10}         │");
    println!("│ Saturation Point:      {saturation_display:>10.0} ops/s  │");
    if peak_rate > 0 {
        println!("│ Peak Rate Tested:      {peak_rate:>10} ops/s  │");
    }
    println!("│ Throughput:            {throughput:>10.0} ops/s  │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Latency (ms):                             │");
    println!("│   Min:                 {lat_min:>10.2} ms      │");
    println!("│   p50:                 {lat_p50:>10.2} ms      │");
    println!("│   p95:                 {lat_p95:>10.2} ms      │");
    println!("│   p99:                 {lat_p99:>10.2} ms      │");
    println!("│   Max:                 {lat_max:>10.2} ms      │");
    println!("└───────────────────────────────────────────┘");

    Ok(())
}
