use std::time::Instant;

use serde_json::{Value, json};

use crate::BenchArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Insert,
    Get,
    Update,
    Delete,
    List,
    Mixed,
}

impl Op {
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

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[allow(clippy::too_many_lines, clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub async fn run(args: BenchArgs) -> Result<(), BoxError> {
    let op = Op::from_str(&args.op)
        .ok_or_else(|| format!("invalid op '{}'. use: insert, get, update, delete, list, mixed", args.op))?;

    let record_size_approx = args.fields * (args.field_size + 20);
    eprintln!(
        "REST+PG Benchmark: {} ops, op={:?}, ~{} bytes/record",
        args.operations, op, record_size_approx
    );

    let client = reqwest::Client::new();

    wait_for_ready(&client, &args.url).await?;

    let mut inserted_ids: Vec<String> = Vec::new();
    let mut id_counter: u64 = 0;

    if args.seed > 0 {
        eprintln!("Seeding {} records...", args.seed);
        for s in 0..args.seed {
            let record = generate_record(args.fields, args.field_size, id_counter);
            id_counter += 1;

            let resp = client
                .post(format!("{}/db/{}", args.url, args.entity))
                .body(serde_json::to_vec(&record).unwrap())
                .header("content-type", "application/json")
                .send()
                .await?;

            let body: Value = resp.json().await?;
            if let Some(id) = body.get("data").and_then(|d| d.get("id")).and_then(Value::as_str) {
                inserted_ids.push(id.to_string());
            }

            if (s + 1) % 1000 == 0 {
                eprintln!("  Seeded {} records...", s + 1);
            }
        }
        eprintln!("Seeding complete. {} records ready.", args.seed);
    }

    let mut latencies_ns: Vec<u64> = Vec::with_capacity(args.operations as usize);
    let mut inserts: u64 = 0;
    let mut gets: u64 = 0;
    let mut updates: u64 = 0;
    let mut deletes: u64 = 0;
    let mut lists: u64 = 0;
    let mut errors: u64 = 0;

    let start_time = Instant::now();

    for i in 0..args.operations {
        let current_op = if op == Op::Mixed {
            match i % 5 {
                1 => Op::Get,
                2 => Op::Update,
                3 => Op::List,
                4 => Op::Delete,
                _ => Op::Insert,
            }
        } else {
            op
        };

        let op_start = Instant::now();

        let success = match current_op {
            Op::Insert => {
                let record = generate_record(args.fields, args.field_size, id_counter);
                id_counter += 1;

                let resp = client
                    .post(format!("{}/db/{}", args.url, args.entity))
                    .body(serde_json::to_vec(&record).unwrap())
                    .header("content-type", "application/json")
                    .send()
                    .await;

                match resp {
                    Ok(r) => {
                        if let Ok(body) = r.json::<Value>().await {
                            if let Some(id) = body.get("data").and_then(|d| d.get("id")).and_then(Value::as_str) {
                                inserted_ids.push(id.to_string());
                            }
                            body.get("status").and_then(Value::as_str) == Some("ok")
                        } else {
                            false
                        }
                    }
                    Err(_) => false,
                }
            }
            Op::Get => {
                if inserted_ids.is_empty() {
                    true
                } else {
                    let id = &inserted_ids[i as usize % inserted_ids.len()];
                    let resp = client
                        .get(format!("{}/db/{}/{}", args.url, args.entity, id))
                        .send()
                        .await;
                    matches!(resp, Ok(r) if r.status().is_success())
                }
            }
            Op::Update => {
                if inserted_ids.is_empty() {
                    true
                } else {
                    let id = &inserted_ids[i as usize % inserted_ids.len()];
                    let record = generate_record(args.fields, args.field_size, i);
                    let resp = client
                        .put(format!("{}/db/{}/{}", args.url, args.entity, id))
                        .body(serde_json::to_vec(&record).unwrap())
                        .header("content-type", "application/json")
                        .send()
                        .await;
                    matches!(resp, Ok(r) if r.status().is_success())
                }
            }
            Op::Delete => {
                if let Some(id) = inserted_ids.pop() {
                    let resp = client
                        .delete(format!("{}/db/{}/{}", args.url, args.entity, id))
                        .send()
                        .await;
                    matches!(resp, Ok(r) if r.status().is_success())
                } else {
                    true
                }
            }
            Op::List => {
                let resp = client
                    .get(format!("{}/db/{}", args.url, args.entity))
                    .send()
                    .await;
                matches!(resp, Ok(r) if r.status().is_success())
            }
            Op::Mixed => unreachable!(),
        };

        if success {
            latencies_ns.push(op_start.elapsed().as_nanos() as u64);
            match current_op {
                Op::Insert => inserts += 1,
                Op::Get => gets += 1,
                Op::Update => updates += 1,
                Op::Delete => deletes += 1,
                Op::List => lists += 1,
                Op::Mixed => {}
            }
        } else {
            errors += 1;
        }
    }

    let elapsed_secs = start_time.elapsed().as_secs_f64();
    let total = inserts + gets + updates + deletes + lists;
    let throughput = total as f64 / elapsed_secs;

    latencies_ns.sort_unstable();
    let p50 = percentile(&latencies_ns, 50.0) / 1000;
    let p95 = percentile(&latencies_ns, 95.0) / 1000;
    let p99 = percentile(&latencies_ns, 99.0) / 1000;

    if args.cleanup && !inserted_ids.is_empty() {
        eprintln!("Cleaning up {} remaining records...", inserted_ids.len());
        for id in &inserted_ids {
            let _ = client
                .delete(format!("{}/db/{}/{}", args.url, args.entity, id))
                .send()
                .await;
        }
    }

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
        "latency_p50_us": p50,
        "latency_p95_us": p95,
        "latency_p99_us": p99,
        "config": {
            "operations": args.operations,
            "concurrency": 1,
            "op": args.op,
            "fields": args.fields,
            "field_size": args.field_size,
            "record_size_approx": record_size_approx
        }
    });
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(crate) fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub(crate) async fn wait_for_ready(client: &reqwest::Client, url: &str) -> Result<(), BoxError> {
    eprint!("Waiting for REST server...");
    for _ in 0..60 {
        if let Ok(resp) = client.get(format!("{url}/health")).send().await
            && resp.status().is_success()
            && let Ok(body) = resp.json::<Value>().await
            && body.get("data").and_then(|d| d.get("ready")).and_then(Value::as_bool) == Some(true)
        {
            eprintln!(" ready!");
            return Ok(());
        }
        eprint!(".");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Err("REST server not ready after 30s".into())
}
