use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::BenchArgs;
use crate::bench::{generate_record, percentile, wait_for_ready};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn run(args: BenchArgs) -> Result<(), BoxError> {
    eprintln!(
        "REST+PG Changefeed Benchmark: write_rate={} ops/s, duration={}s, entity={}",
        args.write_rate, args.duration, args.entity
    );

    let client = reqwest::Client::new();
    wait_for_ready(&client, &args.url).await?;

    let write_sent_ns: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let latencies_ns: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let poller_handle = {
        let client = client.clone();
        let url = args.url.clone();
        let entity = args.entity.clone();
        let write_sent_ns = Arc::clone(&write_sent_ns);
        let latencies_ns = Arc::clone(&latencies_ns);
        let duration = args.duration;
        tokio::spawn(async move {
            let started = Instant::now();
            let stop_after = Duration::from_secs(duration) + Duration::from_secs(5);
            let mut cursor: i64 = 0;
            while started.elapsed() < stop_after {
                let url_req = format!(
                    "{url}/db/{entity}/since/{cursor}?limit=500&wait_ms=1000"
                );
                let Ok(resp) = client.get(&url_req).send().await else {
                    continue;
                };
                let Ok(body) = resp.json::<Value>().await else {
                    continue;
                };
                let recv_ns = started.elapsed().as_nanos() as u64;
                if let Some(next) = body.get("cursor").and_then(Value::as_i64) {
                    cursor = next;
                }
                let Some(records) = body.get("data").and_then(Value::as_array) else {
                    continue;
                };
                let mut map = write_sent_ns.lock().await;
                let mut lat = latencies_ns.lock().await;
                for rec in records {
                    if let Some(id) = rec.get("id").and_then(Value::as_str)
                        && let Some(sent) = map.remove(id)
                        && recv_ns >= sent
                    {
                        lat.push(recv_ns - sent);
                    }
                }
            }
        })
    };

    let writer_started = Instant::now();
    let total_writes = args.write_rate * args.duration;
    let interval = Duration::from_nanos(1_000_000_000 / args.write_rate);
    let mut writes_sent: u64 = 0;
    let mut errors: u64 = 0;

    for i in 0..total_writes {
        let target = interval * u32::try_from(i).unwrap_or(u32::MAX);
        let elapsed = writer_started.elapsed();
        if let Some(wait) = target.checked_sub(elapsed) {
            sleep(wait).await;
        }

        let rec_id = format!("cf-{i}");
        let mut record = generate_record(args.fields, args.field_size, i);
        if let Value::Object(ref mut map) = record {
            map.insert("id".to_string(), json!(rec_id.clone()));
        }

        let send_ns = writer_started.elapsed().as_nanos() as u64;
        {
            let mut map = write_sent_ns.lock().await;
            map.insert(rec_id.clone(), send_ns);
        }

        match client
            .post(format!("{}/db/{}", args.url, args.entity))
            .body(serde_json::to_vec(&record).unwrap())
            .header("content-type", "application/json")
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => writes_sent += 1,
            _ => {
                errors += 1;
                let mut map = write_sent_ns.lock().await;
                map.remove(&rec_id);
            }
        }
    }

    sleep(Duration::from_secs(5)).await;
    poller_handle.abort();
    let _ = poller_handle.await;

    let mut lats = {
        let guard = latencies_ns.lock().await;
        guard.clone()
    };
    lats.sort_unstable();
    let p50 = percentile(&lats, 50.0) / 1000;
    let p95 = percentile(&lats, 95.0) / 1000;
    let p99 = percentile(&lats, 99.0) / 1000;

    let events_received = lats.len() as u64;
    let events_missing = writes_sent.saturating_sub(events_received);

    let result = json!({
        "op": "changefeed",
        "writes_sent": writes_sent,
        "events_received": events_received,
        "events_missing": events_missing,
        "errors": errors,
        "write_rate": args.write_rate,
        "duration_secs": args.duration,
        "latency_p50_us": p50,
        "latency_p95_us": p95,
        "latency_p99_us": p99,
        "config": {
            "operations": total_writes,
            "concurrency": 1,
            "op": "changefeed",
            "fields": args.fields,
            "field_size": args.field_size,
            "write_rate": args.write_rate,
            "duration": args.duration,
        }
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
