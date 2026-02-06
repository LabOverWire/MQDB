// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::ConnectOptions;
use serde_json::json;

use super::common::wait_for_broker_ready;
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
