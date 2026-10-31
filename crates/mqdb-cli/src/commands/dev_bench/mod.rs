// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod baseline;
mod helpers;
mod profiling;
mod runners;

use std::path::PathBuf;
use std::time::Duration;

use crate::cli_types::{ConnectionArgs, DevBenchScenario};

pub(crate) use baseline::cmd_dev_baseline;
use helpers::{broker_listening, start_agent_for_bench, wait_for_broker_ready};
pub(crate) use profiling::{ProfileRun, cmd_dev_profile};
use runners::{compare_with_baseline, print_bench_result, run_db_benchmark, run_pubsub_benchmark};

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
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let conn = ConnectionArgs {
        broker: format!("127.0.0.1:{port}"),
        user: None,
        pass: None,
        timeout: 30,
        insecure: false,
    };

    if !broker_listening(port) {
        println!("Starting agent for benchmark on port {port}...");
        start_agent_for_bench(&exe, db, port)?;
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
