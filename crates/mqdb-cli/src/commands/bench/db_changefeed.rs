// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqtt5::client::MqttClient;
use mqtt5::types::ConnectOptions;
use serde_json::{Value, json};

use super::common::{BenchDbArgs, generate_record, wait_for_broker_ready};
use crate::cli_types::OutputFormat;

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) async fn cmd_bench_db_changefeed(
    args: BenchDbArgs,
    write_rate: u64,
    duration_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_broker_ready(&args.conn, 30).await?;

    println!(
        "Change-feed benchmark: {} writes/s for {}s, entity={}",
        write_rate, duration_secs, args.entity
    );

    let sub_client_name = format!("bench-changefeed-sub-{}", uuid::Uuid::new_v4());
    let sub_client = MqttClient::new(sub_client_name.clone());
    connect_client(&sub_client, &sub_client_name, &args).await?;

    let pub_client_name = format!("bench-changefeed-pub-{}", uuid::Uuid::new_v4());
    let pub_client = MqttClient::new(pub_client_name.clone());
    connect_client(&pub_client, &pub_client_name, &args).await?;

    let write_sent_ns: Arc<std::sync::Mutex<HashMap<String, u64>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let latencies_ns: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_received = Arc::new(AtomicU64::new(0));

    let events_topic = format!("$DB/{}/events/#", args.entity);
    let write_sent_ns_sub = Arc::clone(&write_sent_ns);
    let latencies_sub = Arc::clone(&latencies_ns);
    let events_received_sub = Arc::clone(&events_received);
    let bench_start = Instant::now();

    sub_client
        .subscribe(&events_topic, move |msg| {
            let recv_ns = bench_start.elapsed().as_nanos() as u64;
            let Ok(payload) = serde_json::from_slice::<Value>(&msg.payload) else {
                return;
            };
            let Some(id) = extract_event_id(&payload) else {
                return;
            };
            let sent_ns_opt = {
                let Ok(mut map) = write_sent_ns_sub.lock() else {
                    return;
                };
                map.remove(&id)
            };
            if let Some(sent_ns) = sent_ns_opt {
                let latency = recv_ns.saturating_sub(sent_ns);
                if let Ok(mut v) = latencies_sub.lock() {
                    v.push(latency);
                }
                events_received_sub.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let writes_planned = write_rate * duration_secs;
    let interval_ns = 1_000_000_000u64 / write_rate.max(1);
    let writes_sent = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));

    let writer_entity = args.entity.clone();
    let writer_fields = args.fields;
    let writer_field_size = args.field_size;
    let writer_client = pub_client.clone();
    let writer_write_sent_ns = Arc::clone(&write_sent_ns);
    let writer_writes_sent = Arc::clone(&writes_sent);
    let writer_write_errors = Arc::clone(&write_errors);

    let writer_handle = tokio::spawn(async move {
        let loop_start = Instant::now();
        for i in 0..writes_planned {
            let target = Duration::from_nanos(interval_ns * i);
            let elapsed = loop_start.elapsed();
            if let Some(wait) = target.checked_sub(elapsed) {
                tokio::time::sleep(wait).await;
            }
            let id = uuid::Uuid::now_v7().to_string();
            let mut record = generate_record(writer_fields, writer_field_size, i);
            if let Value::Object(ref mut map) = record {
                map.insert("id".to_string(), json!(id.clone()));
            }
            let topic = format!("$DB/{writer_entity}/create");
            let sent_ns = bench_start.elapsed().as_nanos() as u64;
            if let Ok(mut guard) = writer_write_sent_ns.lock() {
                guard.insert(id.clone(), sent_ns);
            }
            if writer_client
                .publish(&topic, serde_json::to_vec(&record).unwrap())
                .await
                .is_err()
            {
                writer_write_errors.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut guard) = writer_write_sent_ns.lock() {
                    guard.remove(&id);
                }
            } else {
                writer_writes_sent.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let _ = writer_handle.await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = sub_client.disconnect().await;
    let _ = pub_client.disconnect().await;

    let writes_sent_total = writes_sent.load(Ordering::Relaxed);
    let events_total = events_received.load(Ordering::Relaxed);
    let write_errors_total = write_errors.load(Ordering::Relaxed);
    let missing = writes_sent_total.saturating_sub(events_total);

    let (p50, p95, p99) = percentiles(&latencies_ns);

    let result = json!({
        "op": "changefeed",
        "writes_planned": writes_planned,
        "writes_sent": writes_sent_total,
        "write_errors": write_errors_total,
        "events_received": events_total,
        "events_missing": missing,
        "duration_secs": duration_secs as f64,
        "write_rate_target": write_rate,
        "latency_p50_us": p50 / 1000,
        "latency_p95_us": p95 / 1000,
        "latency_p99_us": p99 / 1000,
        "throughput_ops_sec": events_total as f64 / duration_secs as f64,
        "config": {
            "operations": writes_planned,
            "concurrency": 1,
            "op": "changefeed",
            "fields": args.fields,
            "field_size": args.field_size,
            "write_rate": write_rate,
            "duration_secs": duration_secs
        }
    });

    if matches!(args.format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let p50_us = p50 / 1000;
        let p95_us = p95 / 1000;
        let p99_us = p99 / 1000;
        println!("\n┌───────────────────────────────────────────┐");
        println!("│        CHANGE-FEED BENCHMARK              │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Writes sent:        {writes_sent_total:>20} │");
        println!("│ Events received:    {events_total:>20} │");
        println!("│ Events missing:     {missing:>20} │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Delivery p50:       {p50_us:>17} µs │");
        println!("│ Delivery p95:       {p95_us:>17} µs │");
        println!("│ Delivery p99:       {p99_us:>17} µs │");
        println!("└───────────────────────────────────────────┘");
    }

    Ok(())
}

async fn connect_client(
    client: &MqttClient,
    client_id: &str,
    args: &BenchDbArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.conn.insecure {
        client.set_insecure_tls(true).await;
    }
    if let (Some(user), Some(pass)) = (&args.conn.user, &args.conn.pass) {
        let opts = ConnectOptions::new(client_id).with_credentials(user.clone(), pass.clone());
        Box::pin(client.connect_with_options(&args.conn.broker, opts)).await?;
    } else {
        client.connect(&args.conn.broker).await?;
    }
    Ok(())
}

fn extract_event_id(payload: &Value) -> Option<String> {
    if let Some(s) = payload.get("id").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(s) = payload
        .get("data")
        .and_then(|d| d.get("id"))
        .and_then(Value::as_str)
    {
        return Some(s.to_string());
    }
    if let Some(s) = payload.get("record_id").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    None
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
