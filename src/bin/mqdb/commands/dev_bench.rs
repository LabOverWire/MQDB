use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};

use crate::cli_types::{ConnectionArgs, DevBaselineAction, DevBenchScenario};

#[derive(serde::Serialize, serde::Deserialize)]
struct DevBenchResult {
    scenario: String,
    timestamp: String,
    throughput: f64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_p99_us: u64,
    config: serde_json::Value,
}

pub(crate) async fn cmd_dev_bench(
    scenario: DevBenchScenario,
    output: Option<PathBuf>,
    baseline: Option<PathBuf>,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let conn = ConnectionArgs {
        broker: "127.0.0.1:1883".to_string(),
        user: None,
        pass: None,
        timeout: 30,
        insecure: false,
    };

    if !is_agent_running() {
        println!("Starting agent for benchmark...");
        start_agent_for_bench(&exe, db)?;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    wait_for_broker_ready(&conn, 30).await?;

    let result = match &scenario {
        DevBenchScenario::Pubsub {
            publishers,
            subscribers,
            duration,
            size,
            qos,
        } => run_pubsub_benchmark(&conn, *publishers, *subscribers, *duration, *size, *qos).await?,
        DevBenchScenario::Db {
            operations,
            concurrency,
            op,
        } => run_db_benchmark(&conn, *operations, *concurrency, op).await?,
    };

    print_bench_result(&result);

    if let Some(baseline_path) = baseline {
        compare_with_baseline(&result, &baseline_path)?;
    }

    if let Some(output_path) = output {
        let json = serde_json::to_string_pretty(&result)?;
        std::fs::write(&output_path, json)?;
        println!("\nResults saved to: {}", output_path.display());
    }

    Ok(())
}

fn is_agent_running() -> bool {
    Command::new("pgrep")
        .args(["-f", "mqdb agent"])
        .output()
        .is_ok_and(|o| !o.stdout.is_empty())
}

fn start_agent_for_bench(
    exe: &std::path::Path,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(db)?;
    let log_file = std::fs::File::create(format!("{db}/mqdb.log"))?;

    #[cfg(feature = "dev-insecure")]
    let args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    Command::new(exe)
        .args(&args)
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()?;

    Ok(())
}

#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn run_pubsub_benchmark(
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
async fn run_db_benchmark(
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

fn chrono_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{now}")
}

fn print_bench_result(result: &DevBenchResult) {
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
fn compare_with_baseline(
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

fn build_bench_args(scenario: &DevBenchScenario) -> Vec<String> {
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

fn profile_with_samply(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");
    let symbol_dir = cwd.join("target/profiling");
    println!("Starting agent under samply profiler...");

    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut samply = Command::new("samply")
        .args([
            "record",
            "--save-only",
            "--unstable-presymbolicate",
            "--symbol-dir",
            symbol_dir.to_str().unwrap_or("."),
            "-o",
            output_path.to_str().unwrap_or("profile"),
            "--",
        ])
        .arg(&profiling_exe)
        .args(&agent_args)
        .spawn()?;

    std::thread::sleep(Duration::from_secs(3));

    println!("\nRunning benchmark...");
    let bench_status = Command::new(&profiling_exe).args(bench_args).status()?;
    if !bench_status.success() {
        println!("Benchmark failed with status: {bench_status}");
    }

    println!("\nStopping agent to save profile...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match samply.wait() {
        Ok(status) => println!("Samply exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for samply: {e}"),
    }

    println!("\nProfile saved: {}", output_path.display());
    println!("Load with: samply load {}", output_path.display());
    Ok(())
}

fn profile_with_flamegraph(
    exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    println!("Starting agent under flamegraph profiler...\n");

    let output_str = output_path.to_str().unwrap_or("flamegraph.svg");
    #[cfg(feature = "dev-insecure")]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
    ];

    let mut flamegraph = Command::new("cargo").args(&flamegraph_args).spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    println!("Running benchmark...");
    let _ = Command::new(exe).args(bench_args).status();

    println!("\nStopping agent to save flamegraph...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match flamegraph.wait() {
        Ok(status) => println!("Flamegraph exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for flamegraph: {e}"),
    }

    println!("\nFlamegraph saved: {}", output_path.display());
    Ok(())
}

fn profile_with_sample(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
    duration: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");

    println!("Starting agent...");
    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut agent = Command::new(&profiling_exe)
        .args(&agent_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    let agent_pid = agent.id();
    println!("Agent started with PID: {agent_pid}");

    println!("\nRunning benchmark in background...");
    let mut bench = Command::new(&profiling_exe)
        .args(bench_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_millis(500));

    println!("Sampling for {duration} seconds...");
    let sample_output = output_path.with_extension("txt");
    let sample_status = Command::new("sample")
        .args([
            &agent_pid.to_string(),
            &duration.to_string(),
            "-f",
            sample_output.to_str().unwrap_or("profile.txt"),
        ])
        .status()?;

    if !sample_status.success() {
        println!("Warning: sample command failed with status: {sample_status}");
    }

    println!("\nStopping benchmark and agent...");
    let _ = bench.kill();
    let _ = agent.kill();
    let _ = bench.wait();
    let _ = agent.wait();

    println!("\nProfile saved: {}", sample_output.display());

    if sample_output.exists() {
        println!("\nGenerating analysis report...\n");
        analyze_sample_output(&sample_output)?;
    }

    Ok(())
}

mod profile_analysis {
    use std::collections::HashMap;

    type CategoryFilter = fn(&str) -> bool;

    fn is_waiting(f: &str) -> bool {
        f.contains("__psynch_cvwait") || f.contains("__psynch_mutexwait")
    }
    fn is_io_wait(f: &str) -> bool {
        f.contains("kevent")
    }
    fn is_network_io(f: &str) -> bool {
        f.contains("__sendto") || f.contains("__recvfrom")
    }
    fn is_mqdb(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqdb::")
    }
    fn is_mqtt5(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqtt5::")
    }
    fn is_allocation(f: &str) -> bool {
        f.contains("malloc") || f.contains("free") || f.contains("_xzm_")
    }
    fn is_sync(f: &str) -> bool {
        f.contains("pthread_mutex") || f.contains("pthread_cond") || f.contains("parking_lot::")
    }
    fn is_channel(f: &str) -> bool {
        f.contains("flume::")
    }

    const CATEGORIES: &[(&str, CategoryFilter)] = &[
        ("WAITING", is_waiting),
        ("IO_WAIT", is_io_wait),
        ("NETWORK_IO", is_network_io),
        ("MQDB", is_mqdb),
        ("MQTT5", is_mqtt5),
        ("ALLOCATION", is_allocation),
        ("SYNC", is_sync),
        ("CHANNEL", is_channel),
    ];

    fn pct(num: u64, denom: u64) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let result = (num as f64 / denom as f64) * 100.0;
        result
    }

    fn compute_exclusive(call_graph: &[(usize, u64, String)]) -> HashMap<String, u64> {
        let mut exclusive: HashMap<String, u64> = HashMap::new();
        for (i, (depth, count, key)) in call_graph.iter().enumerate() {
            let is_leaf = call_graph
                .iter()
                .skip(i + 1)
                .take_while(|(d, _, _)| *d > *depth)
                .next()
                .is_none();
            if is_leaf {
                *exclusive.entry(key.clone()).or_insert(0) += count;
            }
        }
        exclusive
    }

    fn print_category_breakdown(exclusive: &HashMap<String, u64>, total_excl: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("EXCLUSIVE TIME BY CATEGORY");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        for (cat_name, filter) in CATEGORIES {
            let cat_total: u64 = exclusive
                .iter()
                .filter(|(k, _)| filter(k))
                .map(|(_, v)| v)
                .sum();
            if cat_total > 0 {
                println!(
                    "{cat_name:15} {cat_total:8} samples ({:5.1}%)",
                    pct(cat_total, total_excl)
                );
            }
        }
    }

    fn print_top_functions(inclusive: &HashMap<String, u64>, total_samples: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("TOP MQTT5/MQDB FUNCTIONS (by inclusive time)");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        let mut mqtt_funcs: Vec<_> = inclusive
            .iter()
            .filter(|(k, v)| {
                (k.contains("mqtt5::") || k.contains("mqdb::"))
                    && !k.to_lowercase().contains("main")
                    && **v > total_samples / 100
            })
            .collect();
        mqtt_funcs.sort_by(|a, b| b.1.cmp(a.1));

        for (func, count) in mqtt_funcs.iter().take(12) {
            let short = if func.len() > 70 { &func[..70] } else { func };
            println!("{count:6} ({:5.2}%) {short}", pct(**count, total_samples));
        }
    }

    pub fn analyze(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::BufRead;

        let file = std::fs::File::open(filepath)?;
        let reader = std::io::BufReader::new(file);

        let mut inclusive: HashMap<String, u64> = HashMap::new();
        let mut thread_totals: Vec<u64> = Vec::new();
        let mut call_graph_lines: Vec<(usize, u64, String)> = Vec::new();
        let mut in_call_graph = false;

        let thread_re = regex::Regex::new(r"^\s*(\d+)\s+Thread_\d+")?;
        let call_re = regex::Regex::new(r"^(\s*[+!|:\s]*)(\d+)\s+(.+?)\s+\(in\s+([^)]+)\)")?;

        for line in reader.lines() {
            let line = line?;

            if line.contains("Call graph:") {
                in_call_graph = true;
                continue;
            }
            if in_call_graph && line.trim().starts_with("Total number") {
                break;
            }
            if let Some(caps) = thread_re.captures(&line) {
                if let Ok(count) = caps[1].parse::<u64>() {
                    thread_totals.push(count);
                }
                continue;
            }
            if !in_call_graph {
                continue;
            }
            if let Some(caps) = call_re.captures(&line) {
                let prefix = &caps[1];
                let count: u64 = caps[2].parse().unwrap_or(0);
                let func_name = super::demangle_rust_symbol(&caps[3]);
                let library = &caps[4];

                let key = format!("{func_name} [{library}]");
                let depth = prefix.replace(' ', "").len();

                call_graph_lines.push((depth, count, key.clone()));
                let entry = inclusive.entry(key).or_insert(0);
                *entry = (*entry).max(count);
            }
        }

        let exclusive = compute_exclusive(&call_graph_lines);
        let total_samples = thread_totals.iter().max().copied().unwrap_or(1);
        let total_excl: u64 = exclusive.values().sum();

        println!(
            "================================================================================"
        );
        println!("MQDB PROFILE ANALYSIS REPORT");
        println!(
            "================================================================================"
        );
        println!("\nTotal samples (per thread): {total_samples}");
        println!("Total exclusive samples: {total_excl}");

        print_category_breakdown(&exclusive, total_excl);
        print_top_functions(&inclusive, total_samples);

        let waiting: u64 = exclusive
            .iter()
            .filter(|(k, _)| {
                k.contains("__psynch_cvwait")
                    || k.contains("__psynch_mutexwait")
                    || k.contains("kevent")
            })
            .map(|(_, v)| v)
            .sum();
        let active = total_excl.saturating_sub(waiting);

        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("SUMMARY");
        println!(
            "--------------------------------------------------------------------------------"
        );
        println!(
            "\nActive work: {active} samples ({:.1}%)",
            pct(active, total_excl)
        );
        println!(
            "Waiting/idle: {waiting} samples ({:.1}%)",
            pct(waiting, total_excl)
        );

        if active < total_excl / 10 {
            println!("\n[INFO] System is mostly idle - try longer benchmark or more clients");
        }

        println!(
            "\n================================================================================"
        );
        Ok(())
    }
}

fn analyze_sample_output(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    profile_analysis::analyze(filepath)
}

fn demangle_rust_symbol(name: &str) -> String {
    let mut s = name.to_string();
    s = s.replace("::$u7b$$u7b$closure$u7d$$u7d$", "::{{closure}}");
    s = s.replace("$LT$", "<");
    s = s.replace("$GT$", ">");
    s = s.replace("$u20$", " ");
    if let Some(pos) = s.rfind("::h")
        && s.len() - pos == 19
        && s[pos + 3..].chars().all(|c| c.is_ascii_hexdigit())
    {
        s.truncate(pos);
    }
    s
}

pub(crate) fn cmd_dev_profile(
    scenario: &DevBenchScenario,
    tool: &str,
    duration: u64,
    output: Option<PathBuf>,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;

    if is_agent_running() {
        println!("Killing existing agent for profiling...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        std::thread::sleep(Duration::from_secs(1));
    }

    std::fs::create_dir_all(db)?;

    let timestamp = chrono_timestamp();
    let output_path = output.unwrap_or_else(|| {
        let dir = PathBuf::from(".claude/profiles");
        let _ = std::fs::create_dir_all(&dir);
        match tool {
            "flamegraph" => dir.join(format!("profile_{timestamp}.svg")),
            _ => dir.join(format!("profile_{timestamp}")),
        }
    });

    println!("Profiling with {tool}...");
    println!("Output will be saved to: {}", output_path.display());

    let bench_args = build_bench_args(scenario);

    match tool {
        "samply" => profile_with_samply(&exe, db, &output_path, &bench_args)?,
        "flamegraph" => profile_with_flamegraph(&exe, db, &output_path, &bench_args)?,
        "sample" => profile_with_sample(&exe, db, &output_path, &bench_args, duration)?,
        _ => {
            return Err(format!(
                "Unknown profiling tool: {tool}. Use 'sample', 'samply', or 'flamegraph'"
            )
            .into());
        }
    }

    Ok(())
}

pub(crate) fn cmd_dev_baseline(action: DevBaselineAction) -> Result<(), Box<dyn std::error::Error>> {
    let baselines_dir = PathBuf::from(".claude/benchmarks/baselines");

    match action {
        DevBaselineAction::Save { name, scenario } => {
            std::fs::create_dir_all(&baselines_dir)?;

            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            println!("To save baseline, first run benchmark with --output:");
            println!(
                "  mqdb dev bench {scenario_name} --output {}",
                path.display()
            );
        }
        DevBaselineAction::List => {
            if !baselines_dir.exists() {
                println!("No baselines saved yet.");
                println!("Create with: mqdb dev baseline save <name> pubsub|db");
                return Ok(());
            }

            println!("Saved baselines:");
            for entry in std::fs::read_dir(&baselines_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "json")
                    && let Some(name) = path.file_stem()
                {
                    println!("  {}", name.to_string_lossy());
                }
            }
        }
        DevBaselineAction::Compare { name, scenario } => {
            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            if !path.exists() {
                return Err(format!("Baseline not found: {}", path.display()).into());
            }

            println!("To compare against baseline, run benchmark with --baseline:");
            println!(
                "  mqdb dev bench {scenario_name} --baseline {}",
                path.display()
            );
        }
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

#[derive(Default)]
struct BenchMetrics {
    messages_sent: std::sync::atomic::AtomicU64,
    messages_received: std::sync::atomic::AtomicU64,
    latencies_ns: std::sync::Mutex<Vec<u64>>,
    errors: std::sync::atomic::AtomicU64,
}

impl BenchMetrics {
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
}
