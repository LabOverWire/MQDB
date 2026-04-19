// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, Message, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use super::common::{BenchDbArgs, DbBenchMetrics, DbOp, generate_record, wait_for_broker_ready};
use super::db_async::cmd_bench_db_async;
use crate::cli_types::OutputFormat;

type PendingMap = Arc<std::sync::Mutex<HashMap<u64, flume::Sender<Option<String>>>>>;

struct ResponseRouter {
    pending: PendingMap,
    counter: AtomicU64,
    response_topic: String,
}

impl ResponseRouter {
    fn new(client_id: &str) -> Self {
        Self {
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            counter: AtomicU64::new(0),
            response_topic: format!("{client_id}/resp"),
        }
    }

    fn register(&self) -> (u64, flume::Receiver<Option<String>>) {
        let corr_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = flume::bounded(1);
        if let Ok(mut map) = self.pending.lock() {
            map.insert(corr_id, tx);
        }
        (corr_id, rx)
    }

    fn remove(&self, corr_id: u64) {
        if let Ok(mut map) = self.pending.lock() {
            map.remove(&corr_id);
        }
    }

    fn make_callback(&self) -> impl Fn(Message) + Send + Sync + 'static {
        let pending = Arc::clone(&self.pending);
        move |msg: Message| {
            if let Some(corr_bytes) = &msg.properties.correlation_data
                && corr_bytes.len() == 8
                && let Ok(bytes) = <[u8; 8]>::try_from(&corr_bytes[..8])
            {
                let corr_id = u64::from_be_bytes(bytes);
                if let Ok(mut map) = pending.lock()
                    && let Some(tx) = map.remove(&corr_id)
                {
                    let id = serde_json::from_slice::<Value>(&msg.payload)
                        .ok()
                        .and_then(|v| {
                            v.get("id")
                                .and_then(|id| id.as_str().map(String::from))
                                .or_else(|| {
                                    v.get("data")?.get("id")?.as_str().map(String::from)
                                })
                        })
                        .unwrap_or_default();
                    let _ = tx.send(Some(id));
                }
            }
        }
    }

    fn publish_opts(&self, corr_id: u64) -> PublishOptions {
        PublishOptions {
            properties: PublishProperties {
                response_topic: Some(self.response_topic.clone()),
                correlation_data: Some(corr_id.to_be_bytes().to_vec()),
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

async fn request_response(
    router: &ResponseRouter,
    client: &MqttClient,
    topic: &str,
    payload: Vec<u8>,
) -> Option<String> {
    let (corr_id, rx) = router.register();
    let opts = router.publish_opts(corr_id);
    if client
        .publish_with_options(topic, payload, opts)
        .await
        .is_err()
    {
        router.remove(corr_id);
        return None;
    }
    if let Ok(Ok(result)) = tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await {
        result
    } else {
        router.remove(corr_id);
        None
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub(crate) async fn cmd_bench_db(args: BenchDbArgs) -> Result<(), Box<dyn std::error::Error>> {
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
    let id_counter = Arc::new(AtomicU64::new(0));

    if args.seed > 0 {
        println!("Seeding {} records...", args.seed);
        let seeder_id = "bench-db-seeder".to_string();
        let client = MqttClient::new(seeder_id.clone());
        if let (Some(user), Some(pass)) = (&args.conn.user, &args.conn.pass) {
            let opts = ConnectOptions::new(seeder_id.clone())
                .with_credentials(user.clone(), pass.clone());
            Box::pin(client.connect_with_options(&args.conn.broker, opts)).await?;
        } else {
            client.connect(&args.conn.broker).await?;
        }

        let router = ResponseRouter::new(&seeder_id);
        client
            .subscribe(&router.response_topic, router.make_callback())
            .await?;

        for i in 0..args.seed {
            let id = id_counter.fetch_add(1, Ordering::Relaxed);
            let record = generate_record(args.fields, args.field_size, id);
            let topic = format!("$DB/{}/create", args.entity);

            if let Some(actual_id) =
                request_response(&router, &client, &topic, serde_json::to_vec(&record).unwrap())
                    .await
                && !actual_id.is_empty()
                && let Ok(mut ids) = inserted_ids.lock()
            {
                ids.push(actual_id);
            }

            if (i + 1) % 1000 == 0 {
                println!("  Seeded {} records...", i + 1);
            }
        }

        let _ = client.unsubscribe(&router.response_topic).await;
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
                    ConnectOptions::new(client_name.clone())
                        .with_credentials(user.clone(), pass.clone());
                if let Err(e) = Box::pin(client.connect_with_options(&conn.broker, opts)).await {
                    eprintln!("Client {client_id} connect failed: {e}");
                    return;
                }
            } else if let Err(e) = client.connect(&conn.broker).await {
                eprintln!("Client {client_id} connect failed: {e}");
                return;
            }

            let router = ResponseRouter::new(&client_name);
            if client
                .subscribe(&router.response_topic, router.make_callback())
                .await
                .is_err()
            {
                eprintln!("Client {client_id} subscribe failed");
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
                        let result = request_response(
                            &router,
                            &client,
                            &topic,
                            serde_json::to_vec(&record).unwrap(),
                        )
                        .await;
                        if let Some(actual_id) = &result
                            && !actual_id.is_empty()
                            && let Ok(mut ids) = inserted_ids.lock()
                        {
                            ids.push(actual_id.clone());
                        }
                        result.is_some()
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
                            request_response(&router, &client, &topic, b"{}".to_vec())
                                .await
                                .is_some()
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
                            request_response(
                                &router,
                                &client,
                                &topic,
                                serde_json::to_vec(&record).unwrap(),
                            )
                            .await
                            .is_some()
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
                            request_response(&router, &client, &topic, b"{}".to_vec())
                                .await
                                .is_some()
                        } else {
                            true
                        }
                    }
                    DbOp::List => {
                        let topic = format!("$DB/{entity}/list");
                        let payload = json!({"limit": 10});
                        request_response(
                            &router,
                            &client,
                            &topic,
                            serde_json::to_vec(&payload).unwrap(),
                        )
                        .await
                        .is_some()
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

            let _ = client.unsubscribe(&router.response_topic).await;
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
