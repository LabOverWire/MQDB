use clap::Subcommand;

use super::base::{ConnectionArgs, OutputFormat};

#[derive(Subcommand)]
pub(crate) enum BenchAction {
    #[command(about = "Benchmark pub/sub throughput and latency")]
    Pubsub {
        #[arg(long, default_value = "1", help = "Number of publisher tasks")]
        publishers: usize,
        #[arg(long, default_value = "1", help = "Number of subscriber tasks")]
        subscribers: usize,
        #[arg(
            long,
            default_value = "10",
            help = "Duration in seconds to run at max speed"
        )]
        duration: u64,
        #[arg(long, default_value = "64", help = "Payload size in bytes")]
        size: usize,
        #[arg(long, default_value = "0", help = "MQTT QoS level (0, 1, or 2)")]
        qos: u8,
        #[arg(long, default_value = "bench/test", help = "Topic base pattern")]
        topic: String,
        #[arg(
            long,
            default_value = "1",
            help = "Number of topics to spread load across"
        )]
        topics: usize,
        #[arg(
            long,
            help = "Use wildcard subscription (topic/#) instead of individual subscriptions"
        )]
        wildcard: bool,
        #[arg(long, default_value = "1", help = "Warmup duration in seconds")]
        warmup: u64,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, help = "Broker for publishers (for cross-node testing)")]
        pub_broker: Option<String>,
        #[arg(long, help = "Broker for subscribers (for cross-node testing)")]
        sub_broker: Option<String>,
        #[arg(long, default_value = "table", help = "Output format")]
        format: OutputFormat,
    },
    #[command(about = "Benchmark database CRUD operations")]
    Db {
        #[arg(long, default_value = "1000", help = "Number of operations to perform")]
        operations: u64,
        #[arg(long, default_value = "bench_entity", help = "Entity name to use")]
        entity: String,
        #[arg(
            long,
            default_value = "mixed",
            help = "Operation type: insert, get, update, delete, list, mixed"
        )]
        op: String,
        #[arg(long, default_value = "1", help = "Number of concurrent clients")]
        concurrency: usize,
        #[arg(long, default_value = "5", help = "Number of fields per record")]
        fields: usize,
        #[arg(
            long,
            default_value = "100",
            help = "Approximate size of field values in bytes"
        )]
        field_size: usize,
        #[arg(long, help = "Warmup operations before measuring")]
        warmup: Option<u64>,
        #[arg(long, help = "Clean up test entity after benchmark")]
        cleanup: bool,
        #[arg(
            long,
            default_value = "0",
            help = "Seed records before benchmark (for get/update/delete ops)"
        )]
        seed: u64,
        #[arg(
            long,
            help = "Disable latency tracking for pure throughput measurement"
        )]
        no_latency: bool,
        #[arg(
            long,
            help = "Use async pipelined mode (subscribe once, fire all ops, collect responses)"
        )]
        r#async: bool,
        #[arg(
            long,
            default_value = "1",
            help = "MQTT QoS level (0, 1, or 2) for async mode. QoS 1 recommended for reliability"
        )]
        qos: u8,
        #[arg(
            long,
            help = "Duration in seconds (async mode only, overrides --operations)"
        )]
        duration: Option<u64>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "table", help = "Output format")]
        format: OutputFormat,
    },
}
