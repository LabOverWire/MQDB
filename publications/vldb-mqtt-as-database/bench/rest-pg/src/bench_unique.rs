use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use tokio::sync::Mutex;

use crate::BenchArgs;
use crate::bench::{percentile, wait_for_ready};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn run(args: BenchArgs) -> Result<(), BoxError> {
    eprintln!(
        "REST+PG Unique Contention: concurrency={}, attempts_per_client={}, entity={}",
        args.concurrency, args.attempts_per_client, args.entity
    );

    let client = reqwest::Client::new();
    wait_for_ready(&client, &args.url).await?;

    let constraint_payload = json!({
        "type": "unique",
        "fields": ["contested_value"],
    });
    let resp = client
        .post(format!("{}/db/{}/constraint", args.url, args.entity))
        .body(serde_json::to_vec(&constraint_payload).unwrap())
        .header("content-type", "application/json")
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("failed to register unique constraint: {}", resp.status()).into());
    }

    let success_latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let conflict_latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let successes: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let conflicts: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let errors: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    let wall_started = Instant::now();
    let mut handles = Vec::new();
    for worker in 0..args.concurrency {
        let client = client.clone();
        let url = args.url.clone();
        let entity = args.entity.clone();
        let attempts = args.attempts_per_client;
        let s_lat = Arc::clone(&success_latencies);
        let c_lat = Arc::clone(&conflict_latencies);
        let s_count = Arc::clone(&successes);
        let c_count = Arc::clone(&conflicts);
        let e_count = Arc::clone(&errors);
        handles.push(tokio::spawn(async move {
            for attempt in 0..attempts {
                let record = json!({
                    "id": format!("uniq-w{worker}-a{attempt}"),
                    "contested_value": "hot_key",
                });
                let start = Instant::now();
                let resp = client
                    .post(format!("{url}/db/{entity}"))
                    .body(serde_json::to_vec(&record).unwrap())
                    .header("content-type", "application/json")
                    .send()
                    .await;
                let elapsed_ns = start.elapsed().as_nanos() as u64;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let mut lat = s_lat.lock().await;
                        lat.push(elapsed_ns);
                        let mut n = s_count.lock().await;
                        *n += 1;
                    }
                    Ok(r) if r.status().as_u16() == 409 => {
                        let mut lat = c_lat.lock().await;
                        lat.push(elapsed_ns);
                        let mut n = c_count.lock().await;
                        *n += 1;
                    }
                    _ => {
                        let mut n = e_count.lock().await;
                        *n += 1;
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let wall_secs = wall_started.elapsed().as_secs_f64();

    let (mut s_lats, mut c_lats) = {
        let s = success_latencies.lock().await;
        let c = conflict_latencies.lock().await;
        (s.clone(), c.clone())
    };
    s_lats.sort_unstable();
    c_lats.sort_unstable();

    let s_total = *successes.lock().await;
    let c_total = *conflicts.lock().await;
    let e_total = *errors.lock().await;
    let total_attempts = args.concurrency * args.attempts_per_client;

    let result = json!({
        "op": "unique",
        "successes_total": s_total,
        "conflicts_total": c_total,
        "errors_total": e_total,
        "total_attempts": total_attempts,
        "wall_secs": wall_secs,
        "latency_success_p50_us": percentile(&s_lats, 50.0) / 1000,
        "latency_success_p95_us": percentile(&s_lats, 95.0) / 1000,
        "latency_success_p99_us": percentile(&s_lats, 99.0) / 1000,
        "latency_conflict_p50_us": percentile(&c_lats, 50.0) / 1000,
        "latency_conflict_p95_us": percentile(&c_lats, 95.0) / 1000,
        "latency_conflict_p99_us": percentile(&c_lats, 99.0) / 1000,
        "config": {
            "operations": total_attempts,
            "concurrency": args.concurrency,
            "op": "unique",
            "attempts_per_client": args.attempts_per_client,
        }
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
