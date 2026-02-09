// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum DevAction {
    Ps,
    Kill {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        node: Option<u16>,
        #[arg(long)]
        agent: bool,
    },
    Clean {
        #[arg(long, default_value = "/tmp/mqdb-test")]
        db_prefix: String,
    },
    Logs {
        #[arg(long)]
        node: Option<u16>,
        #[arg(long)]
        pattern: Option<String>,
        #[arg(long, short = 'f')]
        follow: bool,
        #[arg(long, default_value = "50")]
        last: usize,
        #[arg(long, default_value = "/tmp/mqdb-test")]
        db_prefix: String,
    },
    Test {
        #[arg(long)]
        pubsub: bool,
        #[arg(long)]
        db: bool,
        #[arg(long)]
        constraints: bool,
        #[arg(long)]
        wildcards: bool,
        #[arg(long)]
        retained: bool,
        #[arg(long)]
        lwt: bool,
        #[arg(long)]
        ownership: bool,
        #[arg(long)]
        stress_constraints: bool,
        #[arg(long)]
        all: bool,
        #[arg(long, default_value = "3")]
        nodes: u8,
    },
    StartCluster {
        #[arg(long, default_value = "3")]
        nodes: u8,
        #[arg(long)]
        clean: bool,
        #[arg(long, default_value = "test_certs/server.pem")]
        quic_cert: PathBuf,
        #[arg(long, default_value = "test_certs/server.key")]
        quic_key: PathBuf,
        #[arg(long, default_value = "test_certs/ca.pem")]
        quic_ca: PathBuf,
        #[arg(long)]
        no_quic: bool,
        #[arg(long, default_value = "/tmp/mqdb-test")]
        db_prefix: String,
        #[arg(
            long,
            default_value = "127.0.0.1",
            help = "Host to bind (use 0.0.0.0 for external access)"
        )]
        bind_host: String,
        #[arg(
            long,
            value_name = "TYPE",
            help = "Topology: partial (default), upper, or full"
        )]
        topology: Option<String>,
        #[arg(
            long,
            help = "Use Out-only bridge direction (default: Both for partial/upper, Out for full)"
        )]
        bridge_out: bool,
        #[arg(
            long,
            help = "Use Both bridge direction even for full topology (may cause amplification)"
        )]
        no_bridge_out: bool,
        #[arg(long, help = "Path to password file for authentication")]
        passwd: Option<PathBuf>,
        #[arg(
            long,
            help = "Ownership config: entity=field pairs (e.g. diagrams=userId)"
        )]
        ownership: Option<String>,
    },
    #[command(about = "Run benchmarks with auto-start and result saving")]
    Bench {
        #[command(subcommand)]
        scenario: DevBenchScenario,
        #[arg(long, help = "Save results to file")]
        output: Option<PathBuf>,
        #[arg(long, help = "Compare against baseline file")]
        baseline: Option<PathBuf>,
        #[arg(long, default_value = "/tmp/mqdb-dev-bench")]
        db: String,
    },
    #[command(about = "Profile with samply or flamegraph")]
    Profile {
        #[command(subcommand)]
        scenario: DevBenchScenario,
        #[arg(
            long,
            default_value = "samply",
            help = "Profiling tool: samply or flamegraph"
        )]
        tool: String,
        #[arg(long, default_value = "30", help = "Profile duration in seconds")]
        duration: u64,
        #[arg(long, help = "Output file for profile data")]
        output: Option<PathBuf>,
        #[arg(long, default_value = "/tmp/mqdb-dev-profile")]
        db: String,
    },
    #[command(about = "Manage benchmark baselines")]
    Baseline {
        #[command(subcommand)]
        action: DevBaselineAction,
    },
}

#[derive(Subcommand, Clone)]
pub(crate) enum DevBenchScenario {
    #[command(about = "Pub/sub throughput benchmark")]
    Pubsub {
        #[arg(long, default_value = "4")]
        publishers: usize,
        #[arg(long, default_value = "4")]
        subscribers: usize,
        #[arg(long, default_value = "10")]
        duration: u64,
        #[arg(long, default_value = "64")]
        size: usize,
        #[arg(long, default_value = "0")]
        qos: u8,
    },
    #[command(about = "Database CRUD benchmark")]
    Db {
        #[arg(long, default_value = "10000")]
        operations: u64,
        #[arg(long, default_value = "4")]
        concurrency: usize,
        #[arg(long, default_value = "mixed")]
        op: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum DevBaselineAction {
    #[command(about = "Save current benchmark as baseline")]
    Save {
        name: String,
        #[command(subcommand)]
        scenario: DevBenchScenario,
    },
    #[command(about = "List saved baselines")]
    List,
    #[command(about = "Compare current results to a baseline")]
    Compare {
        name: String,
        #[command(subcommand)]
        scenario: DevBenchScenario,
    },
}
