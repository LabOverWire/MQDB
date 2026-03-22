// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use super::common::{BenchDbArgs, DbBenchMetrics, DbOp, generate_record, wait_for_broker_ready};
use super::db_async::cmd_bench_db_async;
use crate::cli_types::OutputFormat;

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub(crate) async fn cmd_bench_db(args: BenchDbArgs) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    wait_for_broker_ready(&args.conn, 30).await?;

    let op = DbOp::from_str(&args.op).ok_or_else(|| {
        format!(
            "Invalid operation '{}'. Use: insert, get, update, delete, list, mixed",
            args.op
        )
    })?;

    if args.async_mode {
        return cmd_bench_db_async(&args, op).await;
    }

    let record_size_approx = args.fields * (args.field_size + 20);
    let latency_mode = if args.no_latency {
        " (throughput only)"
    } else {
        ""
    };
    println!(
        "DB Benchmark: {} ops, {} concurrent, op={:?}, ~{} bytes/record{}",
        args.operations, args.concurrency, op, record_size_approx, latency_mode
    );

    let metrics = Arc::new(DbBenchMetrics::default());
    let inserted_ids: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let id_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    if args.seed > 0 {
        println!("Seeding {} records...", args.seed);
        let seeder_id = "bench-db-seeder".to_string();
        let client = MqttClient::new(seeder_id.clone());
        if let (Some(user), Some(pass)) = (&args.conn.user, &args.conn.pass) {
            let opts = ConnectOptions::new(seeder_id).with_credentials(user.clone(), pass.clone());
            Box::pin(client.connect_with_options(&args.conn.broker, opts)).await?;
        } else {
            client.connect(&args.conn.broker).await?;
        }

        for i in 0..args.seed {
            let id = id_counter.fetch_add(1, Ordering::Relaxed);
            let record = generate_record(args.fields, args.field_size, id);
            let topic = format!("$DB/{}/create", args.entity);
            let response_topic = format!("bench-db-seeder/resp/{}", uuid::Uuid::new_v4());

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
                && let Ok(mut ids) = inserted_ids.lock()
            {
                ids.push(actual_id);
            }
            let _ = client.unsubscribe(&response_topic).await;

            if (i + 1) % 1000 == 0 {
                println!("  Seeded {} records...", i + 1);
            }
        }

        let _ = client.disconnect().await;
        println!("Seeding complete. {} records ready.", args.seed);
    }

    let ops_per_client = args.operations / args.concurrency.max(1) as u64;
    let warmup_per_client = args.warmup / args.concurrency.max(1) as u64;

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for client_id in 0..args.concurrency {
        let conn = args.conn.clone();
        let entity = args.entity.clone();
        let metrics = Arc::clone(&metrics);
        let inserted_ids = Arc::clone(&inserted_ids);
        let id_counter = Arc::clone(&id_counter);
        let fields = args.fields;
        let field_size = args.field_size;
        let no_latency = args.no_latency;

        let handle = tokio::spawn(async move {
            let client_name = format!("bench-db-{client_id}");
            let client = MqttClient::new(client_name.clone());

            if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                let opts =
                    ConnectOptions::new(client_name).with_credentials(user.clone(), pass.clone());
                if let Err(e) = Box::pin(client.connect_with_options(&conn.broker, opts)).await {
                    eprintln!("Client {client_id} connect failed: {e}");
                    return;
                }
            } else if let Err(e) = client.connect(&conn.broker).await {
                eprintln!("Client {client_id} connect failed: {e}");
                return;
            }

            for i in 0..(warmup_per_client + ops_per_client) {
                let is_warmup = i < warmup_per_client;
                let current_op = if op == DbOp::Mixed {
                    match i % 5 {
                        1 => DbOp::Get,
                        2 => DbOp::Update,
                        3 => DbOp::List,
                        4 => DbOp::Delete,
                        _ => DbOp::Insert,
                    }
                } else {
                    op
                };

                let op_start = if no_latency {
                    None
                } else {
                    Some(Instant::now())
                };
                let success = match current_op {
                    DbOp::Insert => {
                        let id = id_counter.fetch_add(1, Ordering::Relaxed);
                        let record = generate_record(fields, field_size, id);
                        let topic = format!("$DB/{entity}/create");
                        let response_topic =
                            format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                        let (tx, rx) = flume::bounded::<Option<String>>(1);
                        let tx_clone = tx.clone();
                        if client
                            .subscribe(&response_topic, move |msg| {
                                let actual_id = serde_json::from_slice::<Value>(&msg.payload)
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
                            .await
                            .is_err()
                        {
                            false
                        } else {
                            let opts = PublishOptions {
                                properties: PublishProperties {
                                    response_topic: Some(response_topic.clone()),
                                    ..Default::default()
                                },
                                ..Default::default()
                            };
                            if client
                                .publish_with_options(
                                    &topic,
                                    serde_json::to_vec(&record).unwrap(),
                                    opts,
                                )
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let actual_id =
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                                        .await
                                        .ok()
                                        .and_then(Result::ok)
                                        .flatten();
                                let success = actual_id.is_some();
                                if let Some(actual_id) = actual_id
                                    && let Ok(mut ids) = inserted_ids.lock()
                                {
                                    ids.push(actual_id);
                                }
                                let _ = client.unsubscribe(&response_topic).await;
                                success
                            }
                        }
                    }
                    DbOp::Get => {
                        let id = {
                            let ids = inserted_ids.lock().ok();
                            ids.and_then(|ids| {
                                if ids.is_empty() {
                                    None
                                } else {
                                    Some(ids[i as usize % ids.len()].clone())
                                }
                            })
                        };
                        if let Some(id) = id {
                            let topic = format!("$DB/{entity}/{id}");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(&topic, b"{}".to_vec(), opts)
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::Update => {
                        let id = {
                            let ids = inserted_ids.lock().ok();
                            ids.and_then(|ids| {
                                if ids.is_empty() {
                                    None
                                } else {
                                    Some(ids[i as usize % ids.len()].clone())
                                }
                            })
                        };
                        if let Some(id) = id {
                            let record = generate_record(fields, field_size, i);
                            let topic = format!("$DB/{entity}/{id}/update");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(
                                        &topic,
                                        serde_json::to_vec(&record).unwrap(),
                                        opts,
                                    )
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::Delete => {
                        let id = {
                            let mut ids = inserted_ids.lock().ok();
                            ids.as_mut().and_then(|ids| ids.pop())
                        };
                        if let Some(id) = id {
                            let topic = format!("$DB/{entity}/{id}/delete");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(&topic, b"{}".to_vec(), opts)
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::List => {
                        let topic = format!("$DB/{entity}/list");
                        let response_topic =
                            format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                        let (tx, rx) = flume::bounded::<bool>(1);
                        let tx_clone = tx.clone();
                        if client
                            .subscribe(&response_topic, move |_msg| {
                                let _ = tx_clone.try_send(true);
                            })
                            .await
                            .is_err()
                        {
                            false
                        } else {
                            let opts = PublishOptions {
                                properties: PublishProperties {
                                    response_topic: Some(response_topic.clone()),
                                    ..Default::default()
                                },
                                ..Default::default()
                            };
                            let payload = json!({"limit": 10});
                            if client
                                .publish_with_options(
                                    &topic,
                                    serde_json::to_vec(&payload).unwrap(),
                                    opts,
                                )
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let result =
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                                        .await
                                        .ok()
                                        .and_then(Result::ok)
                                        .unwrap_or(false);
                                let _ = client.unsubscribe(&response_topic).await;
                                result
                            }
                        }
                    }
                    DbOp::Mixed => unreachable!(),
                };

                if !is_warmup {
                    if success {
                        if let Some(start) = op_start {
                            let latency_ns = start.elapsed().as_nanos() as u64;
                            metrics.record_latency(latency_ns);
                        }
                        match current_op {
                            DbOp::Insert => metrics.inserts.fetch_add(1, Ordering::Relaxed),
                            DbOp::Get => metrics.gets.fetch_add(1, Ordering::Relaxed),
                            DbOp::Update => metrics.updates.fetch_add(1, Ordering::Relaxed),
                            DbOp::Delete => metrics.deletes.fetch_add(1, Ordering::Relaxed),
                            DbOp::List => metrics.lists.fetch_add(1, Ordering::Relaxed),
                            DbOp::Mixed => 0,
                        };
                    } else {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                    }
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
    let elapsed_secs = elapsed.as_secs_f64();

    let total = metrics.total_ops();
    let inserts = metrics.inserts.load(Ordering::Relaxed);
    let gets = metrics.gets.load(Ordering::Relaxed);
    let updates = metrics.updates.load(Ordering::Relaxed);
    let deletes = metrics.deletes.load(Ordering::Relaxed);
    let lists = metrics.lists.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);

    let p50 = metrics.calculate_percentile(50.0);
    let p95 = metrics.calculate_percentile(95.0);
    let p99 = metrics.calculate_percentile(99.0);

    let throughput = total as f64 / elapsed_secs;

    if args.cleanup {
        println!("Cleaning up test entity...");
        let ids_to_cleanup: Vec<String> = inserted_ids
            .lock()
            .map(|ids| ids.clone())
            .unwrap_or_default();
        if !ids_to_cleanup.is_empty() {
            let cleanup_id = "bench-cleanup".to_string();
            let client = MqttClient::new(cleanup_id.clone());
            let connected = if let (Some(user), Some(pass)) = (&args.conn.user, &args.conn.pass) {
                let opts =
                    ConnectOptions::new(cleanup_id).with_credentials(user.clone(), pass.clone());
                Box::pin(client.connect_with_options(&args.conn.broker, opts))
                    .await
                    .is_ok()
            } else {
                client.connect(&args.conn.broker).await.is_ok()
            };
            if connected {
                for id in &ids_to_cleanup {
                    let topic = format!("$DB/{}/{id}/delete", args.entity);
                    let _ = client.publish(&topic, b"{}".to_vec()).await;
                }
                let _ = client.disconnect().await;
            }
        }
    }

    if let OutputFormat::Json = args.format {
        let result = json!({
            "total_ops": total,
            "inserts": inserts,
            "gets": gets,
            "updates": updates,
            "deletes": deletes,
            "lists": lists,
            "errors": errors,
            "duration_secs": elapsed_secs,
            "throughput_ops_sec": throughput,
            "latency_p50_us": p50 / 1000,
            "latency_p95_us": p95 / 1000,
            "latency_p99_us": p99 / 1000,
            "config": {
                "operations": args.operations,
                "concurrency": args.concurrency,
                "op": args.op,
                "fields": args.fields,
                "field_size": args.field_size,
                "record_size_approx": record_size_approx
            }
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let p50_us = p50 / 1000;
        let p95_us = p95 / 1000;
        let p99_us = p99 / 1000;
        println!("\n┌───────────────────────────────────────────┐");
        println!("│           DB BENCHMARK RESULTS            │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Total operations:   {total:>20} │");
        println!("│   Inserts:          {inserts:>20} │");
        println!("│   Gets:             {gets:>20} │");
        println!("│   Updates:          {updates:>20} │");
        println!("│   Deletes:          {deletes:>20} │");
        println!("│   Lists:            {lists:>20} │");
        println!("│ Errors:             {errors:>20} │");
        println!("│ Duration:           {elapsed_secs:>17.2} s │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Throughput:       {throughput:>16.0} ops/s │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Latency p50:        {p50_us:>17} µs │");
        println!("│ Latency p95:        {p95_us:>17} µs │");
        println!("│ Latency p99:        {p99_us:>17} µs │");
        println!("└───────────────────────────────────────────┘");
    }

    Ok(())
}
