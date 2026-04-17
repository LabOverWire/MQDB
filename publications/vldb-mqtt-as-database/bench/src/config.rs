use clap::Parser;

#[derive(Parser)]
#[command(name = "mqdb-baseline-bench")]
#[command(about = "Mosquitto + PostgreSQL/Redis baseline bridge for MQDB benchmarks")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
    Bridge(BridgeArgs),
    BridgeRedis(BridgeRedisArgs),
}

#[derive(Parser)]
pub struct BridgeArgs {
    #[arg(long, default_value = "127.0.0.1:1884")]
    pub broker: String,

    #[arg(long, default_value = "postgres://postgres@127.0.0.1:5433/mqdb_bench")]
    pub pg: String,

    #[arg(long, default_value = "4")]
    pub pool_size: usize,
}

#[derive(Parser)]
pub struct BridgeRedisArgs {
    #[arg(long, default_value = "127.0.0.1:1885")]
    pub broker: String,

    #[arg(long, default_value = "redis://127.0.0.1:6380")]
    pub redis_url: String,
}
