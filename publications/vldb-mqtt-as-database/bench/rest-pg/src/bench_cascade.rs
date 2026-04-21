use std::collections::HashSet;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

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
    let parent_entity = format!("{}_parent", args.entity);
    let child_entity = format!("{}_child", args.entity);

    eprintln!(
        "REST+PG Cascade Benchmark: parent={}, child={}, children={}, runs={}",
        parent_entity, child_entity, args.children, args.runs
    );

    let client = reqwest::Client::new();
    wait_for_ready(&client, &args.url).await?;

    let fk_payload = json!({
        "type": "foreign_key",
        "field": "parent_id",
        "target_entity": parent_entity,
        "target_field": "id",
        "on_delete": "cascade",
    });
    let resp = client
        .post(format!("{}/db/{}/constraint", args.url, child_entity))
        .body(serde_json::to_vec(&fk_payload).unwrap())
        .header("content-type", "application/json")
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("failed to register FK constraint: {}", resp.status()).into());
    }

    let mut delete_latencies_ns: Vec<u64> = Vec::with_capacity(args.runs as usize);
    let mut propagation_latencies_ns: Vec<u64> = Vec::with_capacity(args.runs as usize);
    let mut total_events_received: u64 = 0;
    let mut total_events_missing: u64 = 0;

    for run_idx in 0..args.runs {
        let parent_id = format!("cas-parent-{run_idx}");
        let parent_record = json!({"id": parent_id.clone(), "name": "parent"});
        let resp = client
            .post(format!("{}/db/{}", args.url, parent_entity))
            .body(serde_json::to_vec(&parent_record).unwrap())
            .header("content-type", "application/json")
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("failed to create parent: {}", resp.status()).into());
        }

        let mut expected_children: HashSet<String> = HashSet::new();
        let cursor_before_children: i64 = get_max_seq(&client, &args.url, &child_entity).await?;

        for c in 0..args.children {
            let child_id = format!("cas-child-{run_idx}-{c}");
            expected_children.insert(child_id.clone());
            let child_record = json!({
                "id": child_id,
                "parent_id": parent_id.clone(),
                "index": c,
            });
            let resp = client
                .post(format!("{}/db/{}", args.url, child_entity))
                .body(serde_json::to_vec(&child_record).unwrap())
                .header("content-type", "application/json")
                .send()
                .await?;
            if !resp.status().is_success() {
                return Err(format!("failed to create child {c}: {}", resp.status()).into());
            }
        }

        let _cursor_after_inserts: i64 = get_max_seq(&client, &args.url, &child_entity).await?;

        let delete_start = Instant::now();
        let resp = client
            .delete(format!("{}/db/{}/{}", args.url, parent_entity, parent_id))
            .send()
            .await?;
        let delete_elapsed_ns = delete_start.elapsed().as_nanos() as u64;
        delete_latencies_ns.push(delete_elapsed_ns);
        if !resp.status().is_success() {
            return Err(format!("failed to delete parent: {}", resp.status()).into());
        }

        let body: Value = resp.json().await?;
        let mut deleted_ids: HashSet<String> = HashSet::new();
        if let Some(items) = body
            .get("data")
            .and_then(|d| d.get("deleted"))
            .and_then(Value::as_array)
        {
            for item in items {
                if let Some(arr) = item.as_array()
                    && arr.len() == 2
                    && let Some(ent) = arr[0].as_str()
                    && let Some(idv) = arr[1].as_str()
                    && ent == child_entity
                {
                    deleted_ids.insert(idv.to_string());
                }
            }
        }

        let propagation_started = Instant::now();
        let mut remaining: HashSet<String> = expected_children
            .iter()
            .filter(|id| !deleted_ids.contains(*id))
            .cloned()
            .collect();

        let mut cursor = cursor_before_children;
        let timeout = Duration::from_secs(60);
        while !remaining.is_empty() && propagation_started.elapsed() < timeout {
            let url_req = format!(
                "{}/db/{}/since/{cursor}?limit=1000&wait_ms=500",
                args.url, child_entity
            );
            let Ok(resp) = client.get(&url_req).send().await else {
                continue;
            };
            let Ok(body) = resp.json::<Value>().await else {
                continue;
            };
            if let Some(next) = body.get("cursor").and_then(Value::as_i64) {
                cursor = next;
            }
            let probe = get_present_ids(&client, &args.url, &child_entity, &expected_children).await?;
            for id in &expected_children {
                if !probe.contains(id) {
                    remaining.remove(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let prop_ns = propagation_started.elapsed().as_nanos() as u64;
        propagation_latencies_ns.push(prop_ns);

        let observed = (expected_children.len() - remaining.len()) as u64;
        total_events_received += observed;
        total_events_missing += remaining.len() as u64;
    }

    delete_latencies_ns.sort_unstable();
    propagation_latencies_ns.sort_unstable();

    let result = json!({
        "op": "cascade",
        "runs": args.runs,
        "children_per_parent": args.children,
        "events_received": total_events_received,
        "events_missing": total_events_missing,
        "delete_latency_p50_us": percentile(&delete_latencies_ns, 50.0) / 1000,
        "delete_latency_p95_us": percentile(&delete_latencies_ns, 95.0) / 1000,
        "delete_latency_p99_us": percentile(&delete_latencies_ns, 99.0) / 1000,
        "propagation_latency_p50_us": percentile(&propagation_latencies_ns, 50.0) / 1000,
        "propagation_latency_p95_us": percentile(&propagation_latencies_ns, 95.0) / 1000,
        "propagation_latency_p99_us": percentile(&propagation_latencies_ns, 99.0) / 1000,
        "config": {
            "operations": args.runs * args.children,
            "concurrency": 1,
            "op": "cascade",
            "children": args.children,
            "runs": args.runs,
        }
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn get_max_seq(
    client: &reqwest::Client,
    url: &str,
    entity: &str,
) -> Result<i64, BoxError> {
    let url_req = format!("{url}/db/{entity}/since/0?limit=1000&wait_ms=0");
    let resp = client.get(&url_req).send().await?;
    let body: Value = resp.json().await?;
    Ok(body.get("cursor").and_then(Value::as_i64).unwrap_or(0))
}

async fn get_present_ids(
    client: &reqwest::Client,
    url: &str,
    entity: &str,
    expected: &HashSet<String>,
) -> Result<HashSet<String>, BoxError> {
    let mut present: HashSet<String> = HashSet::new();
    for id in expected {
        let resp = client
            .get(format!("{url}/db/{entity}/{id}"))
            .send()
            .await?;
        if resp.status().is_success() {
            present.insert(id.clone());
        }
    }
    Ok(present)
}
