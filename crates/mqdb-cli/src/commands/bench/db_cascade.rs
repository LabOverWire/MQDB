// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use super::common::{BenchDbArgs, wait_for_broker_ready};
use crate::cli_types::{ConnectionArgs, OutputFormat};

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) async fn cmd_bench_db_cascade(
    args: BenchDbArgs,
    children: u64,
    runs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_broker_ready(&args.conn, 30).await?;

    let parent_entity = format!("{}_parent", args.entity);
    let child_entity = format!("{}_child", args.entity);

    println!(
        "Cascade benchmark: children={children}, runs={runs}, parent={parent_entity}, child={child_entity}"
    );

    register_fk_cascade(&args.conn, &parent_entity, &child_entity).await?;

    let setup_client_id = format!("bench-cascade-setup-{}", uuid::Uuid::new_v4());
    let setup_client = MqttClient::new(setup_client_id.clone());
    connect_client(&setup_client, &setup_client_id, &args.conn).await?;

    let delete_latencies_ns: Vec<u64> = Vec::new();
    let propagation_latencies_ns: Vec<u64> = Vec::new();
    let delete_latencies = Arc::new(std::sync::Mutex::new(delete_latencies_ns));
    let propagation_latencies = Arc::new(std::sync::Mutex::new(propagation_latencies_ns));
    let total_events_received = Arc::new(AtomicU64::new(0));
    let total_events_missing = Arc::new(AtomicU64::new(0));

    let bench_start = Instant::now();

    for run in 0..runs {
        let parent_id = format!("parent-{}-{}", run, uuid::Uuid::new_v4());

        publish_create(
            &setup_client,
            &parent_entity,
            &json!({ "id": parent_id }),
            5,
        )
        .await?;

        let mut child_ids = Vec::with_capacity(children as usize);
        for i in 0..children {
            let child_id = format!("child-{run}-{i}-{}", uuid::Uuid::new_v4());
            let record = json!({
                "id": child_id,
                "parent_id": parent_id,
                "seq": i,
            });
            publish_create(&setup_client, &child_entity, &record, 5).await?;
            child_ids.push(child_id);
        }

        let sub_client_id = format!("bench-cascade-sub-{run}-{}", uuid::Uuid::new_v4());
        let sub_client = MqttClient::new(sub_client_id.clone());
        connect_client(&sub_client, &sub_client_id, &args.conn).await?;

        let events_topic = format!("$DB/{child_entity}/events/#");
        let pending_children: Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(std::sync::Mutex::new(child_ids.iter().cloned().collect()));
        let (done_tx, done_rx) = flume::bounded::<()>(1);
        let pending_sub = Arc::clone(&pending_children);
        let propagation_sub = Arc::clone(&propagation_latencies);
        let events_received_sub = Arc::clone(&total_events_received);
        let delete_sent_slot: Arc<std::sync::Mutex<Option<u64>>> =
            Arc::new(std::sync::Mutex::new(None));
        let delete_sent_sub = Arc::clone(&delete_sent_slot);

        sub_client
            .subscribe(&events_topic, move |msg| {
                let recv_ns = bench_start.elapsed().as_nanos() as u64;
                let Ok(payload) = serde_json::from_slice::<Value>(&msg.payload) else {
                    return;
                };
                let op = payload
                    .get("operation")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if op != "delete" {
                    return;
                }
                let Some(id) = extract_event_id(&payload) else {
                    return;
                };

                let removed = match pending_sub.lock() {
                    Ok(mut set) => set.remove(&id),
                    Err(_) => false,
                };
                if !removed {
                    return;
                }

                let sent_ns_opt = match delete_sent_sub.lock() {
                    Ok(guard) => *guard,
                    Err(_) => None,
                };
                if let Some(sent_ns) = sent_ns_opt {
                    let latency = recv_ns.saturating_sub(sent_ns);
                    if let Ok(mut v) = propagation_sub.lock() {
                        v.push(latency);
                    }
                }
                events_received_sub.fetch_add(1, Ordering::Relaxed);

                let remaining = match pending_sub.lock() {
                    Ok(set) => set.len(),
                    Err(_) => return,
                };
                if remaining == 0 {
                    let _ = done_tx.try_send(());
                }
            })
            .await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let delete_start = Instant::now();
        if let Ok(mut guard) = delete_sent_slot.lock() {
            *guard = Some(bench_start.elapsed().as_nanos() as u64);
        }
        publish_delete(&setup_client, &parent_entity, &parent_id, 30).await?;
        let delete_elapsed = delete_start.elapsed().as_nanos() as u64;
        if let Ok(mut v) = delete_latencies.lock() {
            v.push(delete_elapsed);
        }

        let wait_result = tokio::time::timeout(Duration::from_secs(60), done_rx.recv_async()).await;
        if wait_result.is_err() {
            let remaining = pending_children.lock().map(|s| s.len()).unwrap_or(0);
            total_events_missing.fetch_add(remaining as u64, Ordering::Relaxed);
        }

        let _ = sub_client.unsubscribe(&events_topic).await;
        let _ = sub_client.disconnect().await;
    }

    let _ = setup_client.disconnect().await;

    let elapsed = bench_start.elapsed().as_secs_f64();
    let events_total = total_events_received.load(Ordering::Relaxed);
    let missing_total = total_events_missing.load(Ordering::Relaxed);
    let expected_events = runs * children;

    let (dp50, dp95, dp99) = percentiles(&delete_latencies);
    let (pp50, pp95, pp99) = percentiles(&propagation_latencies);

    let result = json!({
        "op": "cascade",
        "runs": runs,
        "children_per_run": children,
        "expected_events": expected_events,
        "events_received": events_total,
        "events_missing": missing_total,
        "duration_secs": elapsed,
        "delete_latency_p50_us": dp50 / 1000,
        "delete_latency_p95_us": dp95 / 1000,
        "delete_latency_p99_us": dp99 / 1000,
        "propagation_latency_p50_us": pp50 / 1000,
        "propagation_latency_p95_us": pp95 / 1000,
        "propagation_latency_p99_us": pp99 / 1000,
        "throughput_events_sec": events_total as f64 / elapsed,
        "config": {
            "operations": expected_events,
            "concurrency": 1,
            "op": "cascade",
            "children": children,
            "runs": runs,
        }
    });

    if matches!(args.format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let dp50_us = dp50 / 1000;
        let dp95_us = dp95 / 1000;
        let pp50_us = pp50 / 1000;
        let pp95_us = pp95 / 1000;
        println!("\n┌───────────────────────────────────────────┐");
        println!("│          CASCADE BENCHMARK                │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Runs:               {runs:>20} │");
        println!("│ Children/run:       {children:>20} │");
        println!("│ Events received:    {events_total:>20} │");
        println!("│ Events missing:     {missing_total:>20} │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Delete p50:         {dp50_us:>17} µs │");
        println!("│ Delete p95:         {dp95_us:>17} µs │");
        println!("│ Propagation p50:    {pp50_us:>17} µs │");
        println!("│ Propagation p95:    {pp95_us:>17} µs │");
        println!("└───────────────────────────────────────────┘");
    }

    Ok(())
}

async fn connect_client(
    client: &MqttClient,
    client_id: &str,
    conn: &ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    if conn.insecure {
        client.set_insecure_tls(true).await;
    }
    if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
        let opts = ConnectOptions::new(client_id).with_credentials(user.clone(), pass.clone());
        Box::pin(client.connect_with_options(&conn.broker, opts)).await?;
    } else {
        client.connect(&conn.broker).await?;
    }
    Ok(())
}

async fn register_fk_cascade(
    conn: &ConnectionArgs,
    parent_entity: &str,
    child_entity: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_id = format!("bench-cascade-admin-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(client_id.clone());
    connect_client(&client, &client_id, conn).await?;

    let response_topic = format!("bench-cascade-admin/resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(4);
    let tx_clone = tx.clone();
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx_clone.try_send(msg.payload.clone());
        })
        .await?;

    let payload = json!({
        "type": "foreign_key",
        "name": format!("fk_{child_entity}_parent_id"),
        "field": "parent_id",
        "target_entity": parent_entity,
        "target_field": "id",
        "on_delete": "cascade"
    });
    let topic = format!("$DB/_admin/constraint/{child_entity}/add");
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

async fn publish_create(
    client: &MqttClient,
    entity: &str,
    record: &Value,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let response_topic = format!("bench-cascade/resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    let tx_clone = tx.clone();
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx_clone.try_send(msg.payload.clone());
        })
        .await?;

    let topic = format!("$DB/{entity}/create");
    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    client
        .publish_with_options(&topic, serde_json::to_vec(record)?, opts)
        .await?;

    let _ = tokio::time::timeout(Duration::from_secs(timeout_secs), rx.recv_async()).await;
    let _ = client.unsubscribe(&response_topic).await;
    Ok(())
}

async fn publish_delete(
    client: &MqttClient,
    entity: &str,
    id: &str,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let response_topic = format!("bench-cascade/resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    let tx_clone = tx.clone();
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx_clone.try_send(msg.payload.clone());
        })
        .await?;

    let topic = format!("$DB/{entity}/{id}/delete");
    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    client
        .publish_with_options(&topic, b"{}".to_vec(), opts)
        .await?;

    let _ = tokio::time::timeout(Duration::from_secs(timeout_secs), rx.recv_async()).await;
    let _ = client.unsubscribe(&response_topic).await;
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
