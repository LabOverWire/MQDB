use mqdb::Database;
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("./data/benchmark_db").await?;

    println!("MQDB Performance Benchmark");
    println!("============================\n");

    benchmark_writes(&db, 10_000).await?;
    benchmark_reads(&db, 10_000).await?;
    benchmark_updates(&db, 5_000).await?;
    benchmark_list(&db, 1_000).await?;

    println!("\nBenchmark completed!");

    Ok(())
}

async fn benchmark_writes(db: &Database, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!("Write Performance Test");
    println!("----------------------");

    let start = Instant::now();
    let mut latencies = Vec::with_capacity(count);

    for i in 0..count {
        let record_start = Instant::now();

        let data = json!({
            "name": format!("User {}", i),
            "email": format!("user{}@example.com", i),
            "status": if i % 2 == 0 { "active" } else { "inactive" },
            "value": i
        });

        db.create("users".into(), data).await?;

        let record_elapsed = record_start.elapsed();
        latencies.push(record_elapsed);
    }

    let total_elapsed = start.elapsed();

    latencies.sort();
    let p50 = latencies[count / 2];
    let p95 = latencies[count * 95 / 100];
    let p99 = latencies[count * 99 / 100];

    #[allow(clippy::cast_precision_loss)]
    let throughput = count as f64 / total_elapsed.as_secs_f64();

    println!("Total records: {count}");
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("Throughput: {throughput:.0} writes/sec");
    println!("Latency p50: {:.2}ms", p50.as_secs_f64() * 1000.0);
    println!("Latency p95: {:.2}ms", p95.as_secs_f64() * 1000.0);
    println!("Latency p99: {:.2}ms", p99.as_secs_f64() * 1000.0);

    let target_met = p50 < Duration::from_millis(5) && throughput > 5000.0;
    println!("Target met (p50 < 5ms, throughput > 5k/s): {target_met}");

    println!();

    Ok(())
}

async fn benchmark_reads(db: &Database, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!("Read Performance Test");
    println!("---------------------");

    let start = Instant::now();
    let mut latencies = Vec::with_capacity(count);

    for i in 1..=count {
        let record_start = Instant::now();

        let _ = db.read("users".into(), i.to_string(), vec![], None).await;

        let record_elapsed = record_start.elapsed();
        latencies.push(record_elapsed);
    }

    let total_elapsed = start.elapsed();

    latencies.sort();
    let p50 = latencies[count / 2];
    let p95 = latencies[count * 95 / 100];
    let p99 = latencies[count * 99 / 100];

    #[allow(clippy::cast_precision_loss)]
    let throughput = count as f64 / total_elapsed.as_secs_f64();

    println!("Total reads: {count}");
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("Throughput: {throughput:.0} reads/sec");
    println!("Latency p50: {:.2}ms", p50.as_secs_f64() * 1000.0);
    println!("Latency p95: {:.2}ms", p95.as_secs_f64() * 1000.0);
    println!("Latency p99: {:.2}ms", p99.as_secs_f64() * 1000.0);

    let target_met = p50 < Duration::from_millis(1);
    println!("Target met (p50 < 1ms): {target_met}");

    println!();

    Ok(())
}

async fn benchmark_updates(db: &Database, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!("Update Performance Test");
    println!("-----------------------");

    let start = Instant::now();
    let mut latencies = Vec::with_capacity(count);

    for i in 1..=count {
        let record_start = Instant::now();

        let updates = json!({
            "name": format!("Updated User {}", i)
        });

        let _ = db.update("users".into(), i.to_string(), updates).await;

        let record_elapsed = record_start.elapsed();
        latencies.push(record_elapsed);
    }

    let total_elapsed = start.elapsed();

    latencies.sort();
    let p50 = latencies[count / 2];
    let p95 = latencies[count * 95 / 100];
    let p99 = latencies[count * 99 / 100];

    let throughput = count as f64 / total_elapsed.as_secs_f64();

    println!("Total updates: {}", count);
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("Throughput: {:.0} updates/sec", throughput);
    println!("Latency p50: {:.2}ms", p50.as_secs_f64() * 1000.0);
    println!("Latency p95: {:.2}ms", p95.as_secs_f64() * 1000.0);
    println!("Latency p99: {:.2}ms", p99.as_secs_f64() * 1000.0);

    let target_met = p50 < Duration::from_millis(5);
    println!("Target met (p50 < 5ms): {}", target_met);

    println!();

    Ok(())
}

async fn benchmark_list(
    db: &Database,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("List/Scan Performance Test");
    println!("--------------------------");

    db.add_index("users".into(), vec!["status".into()]).await;

    let start = Instant::now();

    for _ in 0..iterations {
        let _ = db
            .list("users".into(), vec![], vec![], None, vec![], None)
            .await;
    }

    let total_elapsed = start.elapsed();
    let throughput = iterations as f64 / total_elapsed.as_secs_f64();

    println!("Total scans: {}", iterations);
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("Throughput: {:.0} scans/sec", throughput);
    println!(
        "Avg latency: {:.2}ms",
        (total_elapsed.as_secs_f64() * 1000.0) / iterations as f64
    );

    println!();

    Ok(())
}
