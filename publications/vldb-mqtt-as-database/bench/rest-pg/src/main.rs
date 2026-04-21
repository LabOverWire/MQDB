mod bench;
mod bench_cascade;
mod bench_changefeed;
mod bench_unique;
mod server;

use clap::Parser;

#[derive(Parser)]
#[command(name = "rest-pg-bench")]
enum Cli {
    Serve(ServeArgs),
    Bench(BenchArgs),
}

#[derive(Parser)]
struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: String,

    #[arg(long, default_value = "postgres://postgres@127.0.0.1:5433/mqdb_bench")]
    pg: String,

    #[arg(long, default_value = "4")]
    pool_size: usize,
}

#[derive(Parser, Clone)]
struct BenchArgs {
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    url: String,

    #[arg(long, default_value = "1000")]
    operations: u64,

    #[arg(long, default_value = "orders")]
    entity: String,

    #[arg(long, default_value = "insert")]
    op: String,

    #[arg(long, default_value = "0")]
    seed: u64,

    #[arg(long, default_value = "5")]
    fields: usize,

    #[arg(long, default_value = "100")]
    field_size: usize,

    #[arg(long)]
    cleanup: bool,

    #[arg(long, default_value = "500")]
    write_rate: u64,

    #[arg(long, default_value = "30")]
    duration: u64,

    #[arg(long, default_value = "16")]
    concurrency: u64,

    #[arg(long, default_value = "100")]
    attempts_per_client: u64,

    #[arg(long, default_value = "100")]
    children: u64,

    #[arg(long, default_value = "5")]
    runs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rest_pg_bench=info".parse().unwrap()),
        )
        .init();

    match Cli::parse() {
        Cli::Serve(args) => server::run(&args.bind, &args.pg, args.pool_size).await,
        Cli::Bench(args) => match args.op.to_lowercase().as_str() {
            "changefeed" => bench_changefeed::run(args).await,
            "unique" => bench_unique::run(args).await,
            "cascade" => bench_cascade::run(args).await,
            _ => bench::run(args).await,
        },
    }
}
