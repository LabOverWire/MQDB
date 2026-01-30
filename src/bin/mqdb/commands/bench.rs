use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use mqtt5::QoS;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};

use crate::cli_types::{ConnectionArgs, OutputFormat};

pub(crate) struct BenchPubsubArgs {
    pub(crate) publishers: usize,
    pub(crate) subscribers: usize,
    pub(crate) duration: u64,
    pub(crate) size: usize,
    pub(crate) qos: u8,
    pub(crate) topic: String,
    pub(crate) topics: usize,
    pub(crate) wildcard: bool,
    pub(crate) warmup: u64,
    pub(crate) conn: ConnectionArgs,
    pub(crate) pub_broker: Option<String>,
    pub(crate) sub_broker: Option<String>,
    pub(crate) format: OutputFormat,
}

#[derive(Default)]
struct BenchMetrics {
    messages_sent: std::sync::atomic::AtomicU64,
    messages_received: std::sync::atomic::AtomicU64,
    latencies_ns: std::sync::Mutex<Vec<u64>>,
    errors: std::sync::atomic::AtomicU64,
}

impl BenchMetrics {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn calculate_percentile(&self, p: f64) -> u64 {
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

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub(crate) async fn cmd_bench_pubsub(
    args: BenchPubsubArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let sub_broker = args.sub_broker.as_ref().unwrap_or(&args.conn.broker);
    let pub_broker = args.pub_broker.as_ref().unwrap_or(&args.conn.broker);
    let is_cross_node = sub_broker != pub_broker;

    let sub_conn = ConnectionArgs {
        broker: sub_broker.clone(),
        user: args.conn.user.clone(),
        pass: args.conn.pass.clone(),
        timeout: args.conn.timeout,
        insecure: args.conn.insecure,
    };
    wait_for_broker_ready(&sub_conn, 30).await?;

    if is_cross_node {
        let pub_conn = ConnectionArgs {
            broker: pub_broker.clone(),
            user: args.conn.user.clone(),
            pass: args.conn.pass.clone(),
            timeout: args.conn.timeout,
            insecure: args.conn.insecure,
        };
        wait_for_broker_ready(&pub_conn, 30).await?;
    }

    let metrics = Arc::new(BenchMetrics::default());
    let running = Arc::new(AtomicBool::new(true));

    let topics_str = if args.topics > 1 {
        let sub_mode = if args.wildcard {
            "wildcard"
        } else {
            "individual"
        };
        format!(", {} topics ({})", args.topics, sub_mode)
    } else {
        String::new()
    };

    if is_cross_node {
        println!(
            "Benchmark (cross-node): {} publishers @ {}, {} subscribers @ {}, {}s duration ({}s warmup), {} byte payload, QoS {}{}",
            args.publishers,
            pub_broker,
            args.subscribers,
            sub_broker,
            args.duration,
            args.warmup,
            args.size,
            args.qos,
            topics_str
        );
    } else {
        println!(
            "Benchmark: {} publishers, {} subscribers, {}s duration ({}s warmup), {} byte payload, QoS {}{}",
            args.publishers,
            args.subscribers,
            args.duration,
            args.warmup,
            args.size,
            args.qos,
            topics_str
        );
    }

    let mut sub_handles = Vec::new();
    let mut pub_handles = Vec::new();

    let use_wildcard = args.wildcard;
    let topic_count = args.topics;
    let topic_base = args.topic.clone();

    for sub_id in 0..args.subscribers {
        let broker = sub_broker.clone();
        let conn = args.conn.clone();
        let topic_base = topic_base.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-sub-{sub_id}");
            let client = MqttClient::new(&client_id);

            let opts = ConnectOptions::new(&client_id)
                .with_clean_start(true)
                .with_keep_alive(Duration::from_secs(30));
            let opts = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                opts.with_credentials(user.clone(), pass.clone())
            } else {
                opts
            };

            if let Err(e) = Box::pin(client.connect_with_options(&broker, opts)).await {
                eprintln!("Subscriber {sub_id} connect failed: {e}");
                return;
            }

            if use_wildcard && topic_count > 1 {
                let wildcard_topic = format!("{topic_base}/#");
                let metrics_clone = Arc::clone(&metrics);
                if let Err(e) = client
                    .subscribe(&wildcard_topic, move |_| {
                        metrics_clone
                            .messages_received
                            .fetch_add(1, Ordering::Relaxed);
                    })
                    .await
                {
                    eprintln!("Subscriber {sub_id} wildcard subscribe failed: {e}");
                    return;
                }
            } else if topic_count > 1 {
                for i in 0..topic_count {
                    let topic = format!("{topic_base}/{i}");
                    let metrics_clone = Arc::clone(&metrics);
                    if let Err(e) = client
                        .subscribe(&topic, move |_| {
                            metrics_clone
                                .messages_received
                                .fetch_add(1, Ordering::Relaxed);
                        })
                        .await
                    {
                        eprintln!("Subscriber {sub_id} subscribe to {topic} failed: {e}");
                        return;
                    }
                }
            } else {
                let metrics_clone = Arc::clone(&metrics);
                if let Err(e) = client
                    .subscribe(&topic_base, move |_| {
                        metrics_clone
                            .messages_received
                            .fetch_add(1, Ordering::Relaxed);
                    })
                    .await
                {
                    eprintln!("Subscriber {sub_id} subscribe failed: {e}");
                    return;
                }
            }

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let _ = client.disconnect().await;
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload: Arc<[u8]> = vec![0u8; args.size].into();
    let warmup_duration = Duration::from_secs(args.warmup);
    let measure_duration = Duration::from_secs(args.duration);

    for pub_id in 0..args.publishers {
        let broker = pub_broker.clone();
        let conn = args.conn.clone();
        let topic_base = topic_base.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);
        let payload = Arc::clone(&payload);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-pub-{pub_id}");
            let client = MqttClient::new(&client_id);

            let opts = ConnectOptions::new(&client_id)
                .with_clean_start(true)
                .with_keep_alive(Duration::from_secs(30));
            let opts = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                opts.with_credentials(user.clone(), pass.clone())
            } else {
                opts
            };

            if let Err(e) = Box::pin(client.connect_with_options(&broker, opts)).await {
                eprintln!("Publisher {pub_id} connect failed: {e}");
                return;
            }

            let mut topic_idx: usize = 0;
            while running.load(Ordering::Relaxed) {
                let topic = if topic_count > 1 {
                    format!("{}/{}", topic_base, topic_idx % topic_count)
                } else {
                    topic_base.clone()
                };
                if client.publish(&topic, payload.to_vec()).await.is_ok() {
                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                }
                topic_idx = topic_idx.wrapping_add(1);
            }

            let _ = client.disconnect().await;
        });
        pub_handles.push(handle);
    }

    if args.warmup > 0 {
        println!("Warming up for {}s...", args.warmup);
        tokio::time::sleep(warmup_duration).await;
    }

    metrics.messages_sent.store(0, Ordering::SeqCst);
    metrics.messages_received.store(0, Ordering::SeqCst);

    println!("Measuring for {}s...", args.duration);
    let start_time = Instant::now();

    tokio::time::sleep(measure_duration).await;

    let elapsed = start_time.elapsed();
    running.store(false, Ordering::Release);

    tokio::time::sleep(Duration::from_millis(200)).await;

    for handle in pub_handles {
        handle.abort();
    }
    for handle in sub_handles {
        handle.abort();
    }

    let elapsed_secs = elapsed.as_secs_f64();

    let sent = metrics.messages_sent.load(Ordering::Relaxed);
    let received = metrics.messages_received.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);

    let p50 = metrics.calculate_percentile(50.0);
    let p95 = metrics.calculate_percentile(95.0);
    let p99 = metrics.calculate_percentile(99.0);

    let throughput = received as f64 / elapsed_secs;
    let bytes_received = received * args.size as u64;
    let bandwidth_mbps = (bytes_received as f64 * 8.0) / (elapsed_secs * 1_000_000.0);

    if let OutputFormat::Json = args.format {
        let result = json!({
            "messages_sent": sent,
            "messages_received": received,
            "errors": errors,
            "duration_secs": elapsed_secs,
            "throughput_msg_sec": throughput,
            "bandwidth_mbps": bandwidth_mbps,
            "latency_p50_us": p50 / 1000,
            "latency_p95_us": p95 / 1000,
            "latency_p99_us": p99 / 1000,
            "config": {
                "publishers": args.publishers,
                "subscribers": args.subscribers,
                "duration_secs": args.duration,
                "warmup_secs": args.warmup,
                "payload_size": args.size,
                "qos": args.qos
            }
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let p50_us = p50 / 1000;
        let p95_us = p95 / 1000;
        let p99_us = p99 / 1000;
        println!("\n┌─────────────────────────────────────────┐");
        println!("│           BENCHMARK RESULTS             │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Messages sent:     {sent:>19} │");
        println!("│ Messages received: {received:>19} │");
        println!("│ Errors:            {errors:>19} │");
        println!("│ Duration:          {elapsed_secs:>16.2} s │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Throughput:     {throughput:>16.0} msg/s │");
        println!("│ Bandwidth:      {bandwidth_mbps:>16.2} Mbps │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Latency p50:      {p50_us:>17} µs │");
        println!("│ Latency p95:      {p95_us:>17} µs │");
        println!("│ Latency p99:      {p99_us:>17} µs │");
        println!("└─────────────────────────────────────────┘");
    }

    Ok(())
}

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
enum DbOp {
    Insert,
    Get,
    Update,
    Delete,
    List,
    Mixed,
}

impl DbOp {
    fn from_str(s: &str) -> Option<Self> {
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
struct DbBenchMetrics {
    inserts: std::sync::atomic::AtomicU64,
    gets: std::sync::atomic::AtomicU64,
    updates: std::sync::atomic::AtomicU64,
    deletes: std::sync::atomic::AtomicU64,
    lists: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    latencies_ns: std::sync::Mutex<Vec<u64>>,
}

impl DbBenchMetrics {
    fn record_latency(&self, ns: u64) {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            latencies.push(ns);
        }
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn calculate_percentile(&self, p: f64) -> u64 {
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

    fn total_ops(&self) -> u64 {
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
async fn cmd_bench_db_async(
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
        let client = MqttClient::new("bench-db-seeder".to_string());
        client.connect(&args.conn.broker).await?;

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
            let client = MqttClient::new("bench-cleanup");
            if client.connect(&args.conn.broker).await.is_ok() {
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

async fn wait_for_broker_ready(
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
