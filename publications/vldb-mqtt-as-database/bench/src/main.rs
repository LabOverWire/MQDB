mod bridge;
mod config;

use bridge::store::Store;
use clap::Parser;
use config::{Cli, Command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("mqdb_baseline_bench=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Bridge(args) => {
            let pool = bridge::pg::create_pool(&args.pg, args.pool_size).await?;
            bridge::run(&args.broker, Store::pg(pool)).await?;
        }
        Command::BridgeRedis(args) => {
            let conn = bridge::redis_store::connect(&args.redis_url).await?;
            bridge::run(&args.broker, Store::redis(conn)).await?;
        }
    }

    Ok(())
}
