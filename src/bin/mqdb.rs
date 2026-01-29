use bebytes::BeBytes;
use clap::{Args, Parser, Subcommand, ValueEnum};
use mqdb::cluster::db::DbEntity;
use mqdb::cluster::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
use mqdb::{ClusterConfig, ClusteredAgent, Database, MqdbAgent, PeerConfig};
use mqtt5::QoS;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "mqdb")]
#[command(about = "MQDB command-line interface")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    Passwd {
        username: String,
        #[arg(short, long)]
        batch: Option<String>,
        #[arg(short = 'D', long)]
        delete: bool,
        #[arg(short = 'n', long)]
        stdout: bool,
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    #[command(about = "Manage SCRAM-SHA-256 credentials")]
    Scram {
        #[arg(help = "Username to add/update/delete")]
        username: String,
        #[arg(short, long, help = "Batch mode - password on command line")]
        batch: Option<String>,
        #[arg(short = 'D', long, help = "Delete the specified user")]
        delete: bool,
        #[arg(short = 'n', long, help = "Output credentials to stdout")]
        stdout: bool,
        #[arg(short, long, help = "SCRAM credentials file path")]
        file: Option<PathBuf>,
        #[arg(
            long,
            short = 'i',
            default_value = "310000",
            help = "PBKDF2 iteration count"
        )]
        iterations: u32,
    },
    #[command(about = "Manage ACL rules and roles")]
    Acl {
        #[command(subcommand)]
        action: AclAction,
    },
    Create {
        entity: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Read {
        entity: String,
        id: String,
        #[arg(long)]
        projection: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Update {
        entity: String,
        id: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Delete {
        entity: String,
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    List {
        entity: String,
        #[arg(short, long)]
        filter: Vec<String>,
        #[arg(short, long)]
        sort: Option<String>,
        #[arg(short, long)]
        limit: Option<usize>,
        #[arg(short, long)]
        offset: Option<usize>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Watch {
        entity: String,
        #[arg(short, long)]
        filter: Vec<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    Constraint {
        #[command(subcommand)]
        action: ConstraintAction,
    },
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    Restore {
        #[arg(short, long)]
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Subscribe {
        pattern: String,
        #[arg(long)]
        entity: Option<String>,
        #[arg(long)]
        group: Option<String>,
        #[arg(long, default_value = "broadcast")]
        mode: SubscriptionModeArg,
        #[arg(long, default_value = "10")]
        heartbeat_interval: u64,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    ConsumerGroup {
        #[command(subcommand)]
        action: ConsumerGroupAction,
    },
    Db {
        #[command(subcommand)]
        action: DbAction,
    },
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    Bench {
        #[command(subcommand)]
        action: BenchAction,
    },
}

#[derive(Subcommand)]
enum DbAction {
    Create {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Read {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Update {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Delete {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
enum BackupAction {
    Create {
        #[arg(short, long, default_value = "backup")]
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
enum DevAction {
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
enum DevBenchScenario {
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
enum DevBaselineAction {
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

#[derive(Subcommand)]
enum BenchAction {
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

#[derive(Subcommand)]
enum ConsumerGroupAction {
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Show {
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Clone, Copy, Default, ValueEnum)]
enum DurabilityArg {
    Immediate,
    #[default]
    Periodic,
    None,
}

#[derive(Clone, Copy, ValueEnum)]
enum JwtAlgorithmArg {
    Hs256,
    Rs256,
    Es256,
}

#[derive(Args)]
struct AuthArgs {
    #[arg(long, help = "Path to password file")]
    passwd: Option<PathBuf>,
    #[arg(long, help = "Path to ACL file")]
    acl: Option<PathBuf>,
    #[cfg(feature = "dev-insecure")]
    #[arg(long, help = "Allow anonymous connections (dev only)")]
    anonymous: bool,
    #[arg(long, help = "Path to SCRAM-SHA-256 credentials file")]
    scram_file: Option<PathBuf>,
    #[arg(long, help = "JWT algorithm: hs256, rs256, es256")]
    jwt_algorithm: Option<JwtAlgorithmArg>,
    #[arg(long, requires = "jwt_algorithm", help = "Path to JWT secret/key file")]
    jwt_key: Option<PathBuf>,
    #[arg(long, help = "JWT issuer claim")]
    jwt_issuer: Option<String>,
    #[arg(long, help = "JWT audience claim")]
    jwt_audience: Option<String>,
    #[arg(
        long,
        default_value = "60",
        help = "JWT clock skew tolerance in seconds"
    )]
    jwt_clock_skew: u64,
    #[arg(long, conflicts_with_all = ["jwt_algorithm"], help = "Path to federated JWT config JSON")]
    federated_jwt_config: Option<PathBuf>,
    #[arg(long, help = "Path to certificate auth file")]
    cert_auth_file: Option<PathBuf>,
    #[arg(long, help = "Disable authentication rate limiting")]
    no_rate_limit: bool,
    #[arg(long, default_value = "5", help = "Rate limit max failed attempts")]
    rate_limit_max_attempts: u32,
    #[arg(long, default_value = "60", help = "Rate limit window in seconds")]
    rate_limit_window_secs: u64,
    #[arg(
        long,
        default_value = "300",
        help = "Rate limit lockout duration in seconds"
    )]
    rate_limit_lockout_secs: u64,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Comma-separated list of admin usernames"
    )]
    admin_users: Vec<String>,
}

#[derive(Args, Clone)]
struct OAuthArgs {
    #[arg(long, help = "HTTP server bind address for OAuth (e.g. 0.0.0.0:8081)")]
    http_bind: Option<SocketAddr>,
    #[arg(long, help = "Path to file containing Google OAuth client secret")]
    oauth_client_secret: Option<PathBuf>,
    #[arg(
        long,
        help = "OAuth redirect URI (default: http://localhost:{http_port}/oauth/callback)"
    )]
    oauth_redirect_uri: Option<String>,
    #[arg(long, help = "URI to redirect browser after OAuth completes")]
    oauth_frontend_redirect: Option<String>,
    #[arg(
        long,
        default_value = "30",
        help = "Ticket JWT expiry in seconds (default: 30)"
    )]
    ticket_expiry_secs: u64,
    #[arg(long, help = "Set Secure flag on session cookies (requires HTTPS)")]
    cookie_secure: bool,
    #[arg(
        long,
        help = "CORS allowed origin for auth endpoints (e.g. http://localhost:8000)"
    )]
    cors_origin: Option<String>,
    #[arg(
        long,
        default_value = "10",
        help = "Max ticket requests per minute per session"
    )]
    ticket_rate_limit: u32,
}

#[derive(Subcommand)]
enum AclAction {
    #[command(about = "Add a user ACL rule")]
    Add {
        username: String,
        topic: String,
        permission: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Remove user ACL rules")]
    Remove {
        username: String,
        topic: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Add a role rule")]
    RoleAdd {
        role_name: String,
        topic: String,
        permission: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Remove a role or role rule")]
    RoleRemove {
        role_name: String,
        topic: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List roles")]
    RoleList {
        role_name: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Assign a role to a user")]
    Assign {
        username: String,
        role: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Unassign a role from a user")]
    Unassign {
        username: String,
        role: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List ACL rules")]
    List {
        #[arg(long)]
        user: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Check if a user can perform an action on a topic")]
    Check {
        username: String,
        topic: String,
        action: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List roles assigned to a user")]
    UserRoles {
        username: String,
        #[arg(short, long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
enum AgentAction {
    Start {
        #[arg(long, default_value = "127.0.0.1:1883")]
        bind: SocketAddr,
        #[arg(long)]
        db: PathBuf,
        #[command(flatten)]
        auth: Box<AuthArgs>,
        #[arg(
            long,
            default_value = "periodic",
            help = "Durability mode: immediate (fsync every write), periodic (fsync periodically), none (no fsync)"
        )]
        durability: DurabilityArg,
        #[arg(
            long,
            default_value = "10",
            help = "Fsync interval in ms when using periodic durability"
        )]
        durability_ms: u64,
        #[arg(long, help = "Path to QUIC/TLS certificate file (PEM format)")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC/TLS private key file (PEM format)")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "WebSocket bind address (e.g. 0.0.0.0:8080)")]
        ws_bind: Option<SocketAddr>,
        #[command(flatten)]
        oauth: Box<OAuthArgs>,
    },
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
enum ClusterAction {
    #[command(about = "Start a cluster node")]
    Start {
        #[arg(long, help = "Unique node ID (1-65535)")]
        node_id: u16,
        #[arg(long, help = "Human-readable node name")]
        node_name: Option<String>,
        #[arg(
            long,
            default_value = "0.0.0.0:1883",
            help = "Address to bind MQTT listener"
        )]
        bind: SocketAddr,
        #[arg(long, help = "Path to database directory")]
        db: PathBuf,
        #[arg(long, value_delimiter = ',', help = "Peer nodes: id@host:port,...")]
        peers: Vec<String>,
        #[command(flatten)]
        auth: Box<AuthArgs>,
        #[arg(long, help = "Path to QUIC TLS certificate")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC TLS private key")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "Path to CA certificate for QUIC peer verification")]
        quic_ca: Option<PathBuf>,
        #[arg(long, help = "Disable QUIC transport")]
        no_quic: bool,
        #[arg(
            long,
            help = "Disable store persistence (data will not survive restarts)"
        )]
        no_persist_stores: bool,
        #[arg(
            long,
            default_value = "periodic",
            help = "Durability mode for stores: immediate (fsync every write), periodic (fsync periodically), none (no fsync). Raft always uses immediate."
        )]
        durability: DurabilityArg,
        #[arg(
            long,
            default_value = "10",
            help = "Fsync interval in ms when using periodic durability"
        )]
        durability_ms: u64,
        #[arg(
            long,
            help = "Use outgoing-only bridge direction (for full mesh topology)"
        )]
        bridge_out: bool,
        #[arg(
            long,
            default_value = "100",
            help = "Port offset for cluster listener (bridges connect to main_port + offset)"
        )]
        cluster_port_offset: u16,
        #[cfg(feature = "dev-insecure")]
        #[arg(
            long,
            help = "Skip TLS certificate verification for direct QUIC (dev only)"
        )]
        quic_insecure: bool,
        #[arg(long, help = "WebSocket bind address (e.g. 0.0.0.0:8080)")]
        ws_bind: Option<SocketAddr>,
        #[command(flatten)]
        oauth: Box<OAuthArgs>,
    },
    #[command(about = "Trigger partition rebalancing across cluster nodes")]
    Rebalance {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    #[command(about = "Show cluster status and partition assignments")]
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
enum SchemaAction {
    Set {
        entity: String,
        #[arg(short, long)]
        file: PathBuf,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Get {
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
enum ConstraintAction {
    Add {
        entity: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        unique: Option<String>,
        #[arg(long)]
        fk: Option<String>,
        #[arg(long)]
        not_null: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    List {
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Clone, clap::Args)]
struct ConnectionArgs {
    #[arg(long, env = "MQDB_BROKER", default_value = "127.0.0.1:1883")]
    broker: String,
    #[arg(long, env = "MQDB_USER")]
    user: Option<String>,
    #[arg(long, env = "MQDB_PASS")]
    pass: Option<String>,
    #[arg(long, default_value = "30")]
    timeout: u64,
    #[arg(
        long,
        help = "Skip TLS certificate verification (for self-signed certs)"
    )]
    insecure: bool,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Json,
    Table,
    Csv,
}

#[derive(Clone, ValueEnum)]
enum SubscriptionModeArg {
    Broadcast,
    LoadBalanced,
    Ordered,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    dispatch_command(cli.command).await
}

/// # Errors
/// Returns an error if the dispatched command fails.
async fn dispatch_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Agent { action } => dispatch_agent(action).await?,
        Commands::Cluster { action } => dispatch_cluster(action).await?,
        Commands::Passwd {
            username,
            batch,
            delete,
            stdout: _,
            file,
        } => cmd_passwd(&username, batch, delete, file)?,
        Commands::Scram {
            username,
            batch,
            delete,
            stdout,
            file,
            iterations,
        } => cmd_scram(&username, batch, delete, stdout, file, iterations)?,
        Commands::Acl { action } => Box::pin(cmd_acl(action)).await?,
        Commands::Create {
            entity,
            data,
            conn,
            format,
        } => Box::pin(cmd_create(entity, data, conn, format)).await?,
        Commands::Read {
            entity,
            id,
            projection,
            conn,
            format,
        } => Box::pin(cmd_read(entity, id, projection, conn, format)).await?,
        Commands::Update {
            entity,
            id,
            data,
            conn,
            format,
        } => Box::pin(cmd_update(entity, id, data, conn, format)).await?,
        Commands::Delete {
            entity,
            id,
            conn,
            format,
        } => Box::pin(cmd_delete(entity, id, conn, format)).await?,
        Commands::List {
            entity,
            filter,
            sort,
            limit,
            offset,
            conn,
            format,
        } => Box::pin(cmd_list(entity, filter, sort, limit, offset, conn, format)).await?,
        Commands::Watch {
            entity,
            filter,
            conn,
            format,
        } => Box::pin(cmd_watch(entity, filter, conn, format)).await?,
        Commands::Schema { action } => dispatch_schema(action).await?,
        Commands::Constraint { action } => dispatch_constraint(action).await?,
        Commands::Backup { action } => dispatch_backup(action).await?,
        Commands::Restore { name, conn } => Box::pin(cmd_restore(&name, &conn)).await?,
        Commands::Subscribe {
            pattern,
            entity,
            group,
            mode,
            heartbeat_interval,
            conn,
            format,
        } => {
            Box::pin(cmd_subscribe(
                pattern,
                entity,
                group,
                mode,
                heartbeat_interval,
                conn,
                format,
            ))
            .await?;
        }
        Commands::ConsumerGroup { action } => dispatch_consumer_group(action).await?,
        Commands::Db { action } => dispatch_db(action).await?,
        Commands::Dev { action } => dispatch_dev(action).await?,
        Commands::Bench { action } => dispatch_bench(action).await?,
    }
    Ok(())
}

/// # Errors
/// Returns an error if the agent action fails.
async fn dispatch_agent(action: AgentAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        AgentAction::Start {
            bind,
            db,
            auth,
            durability,
            durability_ms,
            quic_cert,
            quic_key,
            ws_bind,
            oauth,
        } => {
            cmd_agent_start(AgentStartArgs {
                bind,
                db_path: db,
                auth: *auth,
                durability,
                durability_ms,
                quic_cert,
                quic_key,
                ws_bind,
                oauth: *oauth,
            })
            .await
        }
        AgentAction::Status { conn } => Box::pin(cmd_agent_status(conn)).await,
    }
}

/// # Errors
/// Returns an error if the cluster action fails.
async fn dispatch_cluster(action: ClusterAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ClusterAction::Start {
            node_id,
            node_name,
            bind,
            db,
            peers,
            auth,
            quic_cert,
            quic_key,
            quic_ca,
            no_quic,
            no_persist_stores,
            durability,
            durability_ms,
            bridge_out,
            cluster_port_offset,
            #[cfg(feature = "dev-insecure")]
            quic_insecure,
            ws_bind,
            oauth,
        } => {
            Box::pin(cmd_cluster_start(ClusterStartArgs {
                node_id,
                node_name,
                bind,
                db_path: db,
                peers,
                auth: *auth,
                quic_cert,
                quic_key,
                quic_ca,
                no_quic,
                no_persist_stores,
                durability,
                durability_ms,
                bridge_out,
                cluster_port_offset,
                #[cfg(feature = "dev-insecure")]
                quic_insecure,
                ws_bind,
                oauth: *oauth,
            }))
            .await
        }
        ClusterAction::Rebalance { conn } => Box::pin(cmd_cluster_rebalance(conn)).await,
        ClusterAction::Status { conn } => Box::pin(cmd_cluster_status(conn)).await,
    }
}

/// # Errors
/// Returns an error if the schema action fails.
async fn dispatch_schema(action: SchemaAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SchemaAction::Set { entity, file, conn } => {
            Box::pin(cmd_schema_set(entity, file, conn)).await
        }
        SchemaAction::Get {
            entity,
            conn,
            format,
        } => Box::pin(cmd_schema_get(entity, conn, format)).await,
    }
}

/// # Errors
/// Returns an error if the constraint action fails.
async fn dispatch_constraint(action: ConstraintAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ConstraintAction::Add {
            entity,
            name,
            unique,
            fk,
            not_null,
            conn,
        } => Box::pin(cmd_constraint_add(entity, name, unique, fk, not_null, conn)).await,
        ConstraintAction::List {
            entity,
            conn,
            format,
        } => Box::pin(cmd_constraint_list(entity, conn, format)).await,
    }
}

/// # Errors
/// Returns an error if the backup action fails.
async fn dispatch_backup(action: BackupAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        BackupAction::Create { name, conn } => Box::pin(cmd_backup_create(&name, &conn)).await,
        BackupAction::List { conn } => Box::pin(cmd_backup_list(&conn)).await,
    }
}

/// # Errors
/// Returns an error if the consumer group action fails.
async fn dispatch_consumer_group(
    action: ConsumerGroupAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ConsumerGroupAction::List { conn, format } => {
            Box::pin(cmd_consumer_group_list(conn, format)).await
        }
        ConsumerGroupAction::Show { name, conn, format } => {
            Box::pin(cmd_consumer_group_show(name, conn, format)).await
        }
    }
}

/// # Errors
/// Returns an error if the DB action fails.
async fn dispatch_db(action: DbAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        DbAction::Create {
            partition,
            entity,
            data,
            conn,
        } => Box::pin(cmd_db_create(partition, entity, data, conn)).await,
        DbAction::Read {
            partition,
            entity,
            id,
            conn,
        } => Box::pin(cmd_db_read(partition, entity, id, conn)).await,
        DbAction::Update {
            partition,
            entity,
            id,
            data,
            conn,
        } => Box::pin(cmd_db_update(partition, entity, id, data, conn)).await,
        DbAction::Delete {
            partition,
            entity,
            id,
            conn,
        } => Box::pin(cmd_db_delete(partition, entity, id, conn)).await,
    }
}

/// # Errors
/// Returns an error if the dev action fails.
async fn dispatch_dev(action: DevAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        DevAction::Ps => cmd_dev_ps()?,
        DevAction::Kill { all, node, agent } => cmd_dev_kill(all, node, agent),
        DevAction::Clean { db_prefix } => cmd_dev_clean(&db_prefix)?,
        DevAction::Logs {
            node,
            pattern,
            follow,
            last,
            db_prefix,
        } => cmd_dev_logs(node, pattern.as_deref(), follow, last, &db_prefix)?,
        DevAction::Test {
            pubsub,
            db,
            constraints,
            wildcards,
            retained,
            lwt,
            all,
            nodes,
        } => cmd_dev_test(
            pubsub,
            db,
            constraints,
            wildcards,
            retained,
            lwt,
            all,
            nodes,
        ),
        DevAction::StartCluster {
            nodes,
            clean,
            quic_cert,
            quic_key,
            quic_ca,
            no_quic,
            db_prefix,
            bind_host,
            topology,
            bridge_out,
            no_bridge_out,
        } => cmd_dev_start_cluster(
            nodes,
            clean,
            &quic_cert,
            &quic_key,
            &quic_ca,
            no_quic,
            &db_prefix,
            &bind_host,
            topology.as_deref(),
            bridge_out,
            no_bridge_out,
        )?,
        DevAction::Bench {
            scenario,
            output,
            baseline,
            db,
        } => Box::pin(cmd_dev_bench(scenario, output, baseline, &db)).await?,
        DevAction::Profile {
            scenario,
            tool,
            duration,
            output,
            db,
        } => cmd_dev_profile(&scenario, &tool, duration, output, &db)?,
        DevAction::Baseline { action } => cmd_dev_baseline(action)?,
    }
    Ok(())
}

/// # Errors
/// Returns an error if the bench action fails.
async fn dispatch_bench(action: BenchAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        BenchAction::Pubsub {
            publishers,
            subscribers,
            duration,
            size,
            qos,
            topic,
            topics,
            wildcard,
            warmup,
            conn,
            pub_broker,
            sub_broker,
            format,
        } => {
            Box::pin(cmd_bench_pubsub(BenchPubsubArgs {
                publishers,
                subscribers,
                duration,
                size,
                qos,
                topic,
                topics,
                wildcard,
                warmup,
                conn,
                pub_broker,
                sub_broker,
                format,
            }))
            .await
        }
        BenchAction::Db {
            operations,
            entity,
            op,
            concurrency,
            fields,
            field_size,
            warmup,
            cleanup,
            seed,
            no_latency,
            r#async,
            qos,
            duration,
            conn,
            format,
        } => {
            Box::pin(cmd_bench_db(BenchDbArgs {
                operations,
                entity,
                op,
                concurrency,
                fields,
                field_size,
                warmup: warmup.unwrap_or(0),
                cleanup,
                seed,
                no_latency,
                async_mode: r#async,
                qos,
                duration,
                conn,
                format,
            }))
            .await
        }
    }
}

struct AgentStartArgs {
    bind: SocketAddr,
    db_path: PathBuf,
    auth: AuthArgs,
    durability: DurabilityArg,
    durability_ms: u64,
    quic_cert: Option<PathBuf>,
    quic_key: Option<PathBuf>,
    ws_bind: Option<SocketAddr>,
    oauth: OAuthArgs,
}

async fn cmd_agent_start(args: AgentStartArgs) -> Result<(), Box<dyn std::error::Error>> {
    use mqdb::config::{DatabaseConfig, DurabilityMode};

    let durability_mode = match args.durability {
        DurabilityArg::Immediate => DurabilityMode::Immediate,
        DurabilityArg::Periodic => DurabilityMode::PeriodicMs(args.durability_ms),
        DurabilityArg::None => DurabilityMode::None,
    };

    let config = DatabaseConfig::new(&args.db_path).with_durability(durability_mode);
    let db = Database::open_with_config(config).await?;

    let auth_setup = build_auth_setup_config(&args.auth)?;
    let mut agent = MqdbAgent::new(db)
        .with_bind_address(args.bind)
        .with_auth_setup(auth_setup);

    if let (Some(cert), Some(key)) = (args.quic_cert, args.quic_key) {
        agent = agent.with_quic_certs(cert, key);
    }
    if let Some(ws_addr) = args.ws_bind {
        agent = agent.with_ws_bind_address(ws_addr);
    }

    if let Some(http_bind) = args.oauth.http_bind {
        let http_config = build_http_config(http_bind, &args.auth, &args.oauth)?;
        agent = agent.with_http_config(http_config);
    }

    let agent = Arc::new(agent);
    agent.run().await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn cmd_agent_status(conn: ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(&conn)).await?;
    println!("Connected to broker at {}", conn.broker);
    client.disconnect().await?;
    Ok(())
}

#[allow(clippy::struct_excessive_bools)]
struct ClusterStartArgs {
    node_id: u16,
    node_name: Option<String>,
    bind: SocketAddr,
    db_path: PathBuf,
    peers: Vec<String>,
    auth: AuthArgs,
    quic_cert: Option<PathBuf>,
    quic_key: Option<PathBuf>,
    quic_ca: Option<PathBuf>,
    no_quic: bool,
    no_persist_stores: bool,
    durability: DurabilityArg,
    durability_ms: u64,
    bridge_out: bool,
    cluster_port_offset: u16,
    #[cfg(feature = "dev-insecure")]
    quic_insecure: bool,
    ws_bind: Option<SocketAddr>,
    oauth: OAuthArgs,
}

async fn cmd_cluster_start(args: ClusterStartArgs) -> Result<(), Box<dyn std::error::Error>> {
    use mqdb::config::DurabilityMode;

    let peer_configs = parse_peer_configs(&args.peers)?;

    let stores_durability = match args.durability {
        DurabilityArg::Immediate => DurabilityMode::Immediate,
        DurabilityArg::Periodic => DurabilityMode::PeriodicMs(args.durability_ms),
        DurabilityArg::None => DurabilityMode::None,
    };

    let passwd_file = args.auth.passwd.clone();
    let acl_file = args.auth.acl.clone();
    let auth_setup = build_auth_setup_config(&args.auth)?;

    let mut config = ClusterConfig::new(args.node_id, args.db_path, peer_configs);
    config = config.with_bind_address(args.bind);
    config = config.with_stores_durability(stores_durability);

    if let Some(name) = args.node_name {
        config = config.with_node_name(name);
    }
    if let Some(pf) = passwd_file {
        config = config.with_password_file(pf);
    }
    if let Some(af) = acl_file {
        config = config.with_acl_file(af);
    }
    config = config.with_auth_setup(auth_setup);
    if let (Some(cert), Some(key)) = (args.quic_cert, args.quic_key) {
        config = config.with_quic_certs(cert, key);
    }
    if let Some(ca) = args.quic_ca {
        config = config.with_quic_ca(ca);
    }
    if args.no_quic {
        config = config.with_quic(false);
    }
    if args.no_persist_stores {
        config = config.with_persist_stores(false);
    }
    if args.bridge_out {
        config = config.with_bridge_out_only(true);
    }
    config = config.with_cluster_port_offset(args.cluster_port_offset);
    #[cfg(feature = "dev-insecure")]
    if args.quic_insecure {
        config = config.with_quic_insecure(true);
    }
    if let Some(ws_addr) = args.ws_bind {
        config = config.with_ws_bind_address(ws_addr);
    }
    if let Some(http_bind) = args.oauth.http_bind {
        let http_config = build_http_config(http_bind, &args.auth, &args.oauth)?;
        config = config.with_http_config(http_config);
    }

    let mut agent = ClusteredAgent::new(config).map_err(|e| e.clone())?;
    Box::pin(agent.run()).await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn cmd_cluster_rebalance(conn: ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$SYS/mqdb/cluster/rebalance";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        let proposed = response
            .get("data")
            .and_then(|d| d.get("proposed_changes"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);

        if proposed > 0 {
            println!("Rebalance initiated: {proposed} partition changes proposed");
        } else {
            println!("Cluster already balanced, no changes needed");
        }
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_cluster_status(conn: ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$SYS/mqdb/cluster/status";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if !is_ok {
        if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
            eprintln!("Error: {error}");
        } else {
            output_response(&response, &OutputFormat::Json);
        }
        return Ok(());
    }

    let Some(data) = response.get("data") else {
        return Ok(());
    };

    let node_id = data
        .get("node_id")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let node_name = data
        .get("node_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let is_leader = data
        .get("is_raft_leader")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let raft_term = data
        .get("raft_term")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    println!("┌─────────────────────────────────────────┐");
    println!("│             CLUSTER STATUS              │");
    println!("├─────────────────────────────────────────┤");
    println!(
        "│ Node:     {:<29} │",
        format!("{node_name} (id={node_id})")
    );
    println!(
        "│ Role:     {:<29} │",
        if is_leader { "Leader" } else { "Follower" }
    );
    println!("│ Term:     {raft_term:<29} │");
    println!("├─────────────────────────────────────────┤");

    if let Some(alive) = data
        .get("alive_nodes")
        .and_then(serde_json::Value::as_array)
    {
        let nodes: Vec<String> = alive
            .iter()
            .filter_map(serde_json::Value::as_u64)
            .map(|n| n.to_string())
            .collect();
        let node_count = nodes.len() + 1;
        println!(
            "│ Nodes:    {:<29} │",
            format!("{node_count} total ({node_id} + [{}])", nodes.join(", "))
        );
    } else {
        println!("│ Nodes:    1 (this node only)            │");
    }

    if let Some(partitions) = data.get("partitions").and_then(serde_json::Value::as_array) {
        let mut primary_counts: std::collections::HashMap<u64, usize> =
            std::collections::HashMap::new();
        let mut with_replicas = 0;

        for p in partitions {
            if let Some(primary) = p.get("primary").and_then(serde_json::Value::as_u64) {
                *primary_counts.entry(primary).or_insert(0) += 1;
            }
            if let Some(replicas) = p.get("replicas").and_then(serde_json::Value::as_array)
                && !replicas.is_empty()
            {
                with_replicas += 1;
            }
        }

        println!("├─────────────────────────────────────────┤");
        println!(
            "│ Partitions: {:<27} │",
            format!("{} total", partitions.len())
        );

        let mut dist: Vec<_> = primary_counts.iter().collect();
        dist.sort_by_key(|(k, _)| *k);
        for (node, count) in dist {
            println!("│   Node {}: {:<30} │", node, format!("{count} primary"));
        }
        println!(
            "│   Replicated: {:<25} │",
            format!("{with_replicas}/{}", partitions.len())
        );
    }

    println!("└─────────────────────────────────────────┘");

    Ok(())
}

fn parse_peer_configs(peers: &[String]) -> Result<Vec<PeerConfig>, Box<dyn std::error::Error>> {
    peers
        .iter()
        .map(|peer| {
            let parts: Vec<&str> = peer.split('@').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "invalid peer format '{peer}': expected 'node_id@address:port'"
                )
                .into());
            }
            let node_id: u16 = parts[0]
                .parse()
                .map_err(|_| format!("invalid node_id in peer '{peer}'"))?;
            let address = parts[1].to_string();
            Ok(PeerConfig::new(node_id, address))
        })
        .collect()
}

fn cmd_passwd(
    username: &str,
    batch: Option<String>,
    delete: bool,
    file: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if delete {
        let path = file.ok_or("--file is required for --delete")?;
        let contents = std::fs::read_to_string(&path)?;
        let prefix = format!("{username}:");
        let remaining: Vec<&str> = contents
            .lines()
            .filter(|line| !line.starts_with(&prefix))
            .collect();
        std::fs::write(&path, remaining.join("\n") + "\n")?;
        eprintln!("Deleted user '{username}' from {}", path.display());
        return Ok(());
    }

    let password = if let Some(p) = batch {
        p
    } else {
        eprint!("Password: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    };

    let hash = mqtt5::broker::auth::PasswordAuthProvider::hash_password(&password)?;

    if let Some(path) = file {
        let mut contents = std::fs::read_to_string(&path).unwrap_or_default();
        let prefix = format!("{username}:");
        let mut found = false;
        let updated: Vec<String> = contents
            .lines()
            .map(|line| {
                if line.starts_with(&prefix) {
                    found = true;
                    format!("{username}:{hash}")
                } else {
                    line.to_string()
                }
            })
            .collect();
        contents = updated.join("\n") + "\n";
        if !found {
            use std::fmt::Write;
            let _ = writeln!(contents, "{username}:{hash}");
        }
        std::fs::write(&path, &contents)?;
        eprintln!(
            "{} user '{username}' in {}",
            if found { "Updated" } else { "Added" },
            path.display()
        );
    } else {
        println!("{username}:{hash}");
    }

    Ok(())
}

fn build_auth_setup_config(
    auth: &AuthArgs,
) -> Result<mqdb::auth_config::AuthSetupConfig, Box<dyn std::error::Error>> {
    use mqtt5::broker::config::{JwtAlgorithm, JwtConfig, RateLimitConfig};

    let jwt_config = if let Some(alg) = auth.jwt_algorithm {
        let key_path = auth
            .jwt_key
            .clone()
            .ok_or("--jwt-key is required when --jwt-algorithm is set")?;
        let algorithm = match alg {
            JwtAlgorithmArg::Hs256 => JwtAlgorithm::HS256,
            JwtAlgorithmArg::Rs256 => JwtAlgorithm::RS256,
            JwtAlgorithmArg::Es256 => JwtAlgorithm::ES256,
        };
        let mut cfg = JwtConfig::new(algorithm, key_path).with_clock_skew(auth.jwt_clock_skew);
        if let Some(ref issuer) = auth.jwt_issuer {
            cfg = cfg.with_issuer(issuer);
        }
        if let Some(ref audience) = auth.jwt_audience {
            cfg = cfg.with_audience(audience);
        }
        Some(cfg)
    } else {
        None
    };

    let federated_jwt_config = if let Some(ref path) = auth.federated_jwt_config {
        let content = std::fs::read_to_string(path)?;
        let config: mqtt5::broker::config::FederatedJwtConfig = serde_json::from_str(&content)?;
        Some(config)
    } else {
        None
    };

    let rate_limit = if auth.no_rate_limit {
        None
    } else {
        Some(RateLimitConfig {
            enabled: true,
            max_attempts: auth.rate_limit_max_attempts,
            window_secs: auth.rate_limit_window_secs,
            lockout_secs: auth.rate_limit_lockout_secs,
        })
    };

    #[cfg(feature = "dev-insecure")]
    let allow_anonymous = auth.anonymous;
    #[cfg(not(feature = "dev-insecure"))]
    let allow_anonymous = false;

    Ok(mqdb::auth_config::AuthSetupConfig {
        password_file: auth.passwd.clone(),
        acl_file: auth.acl.clone(),
        allow_anonymous,
        scram_file: auth.scram_file.clone(),
        jwt_config,
        federated_jwt_config,
        cert_auth_file: auth.cert_auth_file.clone(),
        rate_limit,
        no_rate_limit: auth.no_rate_limit,
        admin_users: auth.admin_users.iter().cloned().collect(),
    })
}

fn build_http_config(
    http_bind: SocketAddr,
    auth: &AuthArgs,
    oauth: &OAuthArgs,
) -> Result<mqdb::http::HttpServerConfig, Box<dyn std::error::Error>> {
    let client_secret = match oauth.oauth_client_secret.as_ref() {
        Some(path) => std::fs::read_to_string(path)?.trim().to_string(),
        None => return Err("--oauth-client-secret is required when --http-bind is set".into()),
    };

    let jwt_key_path = auth
        .jwt_key
        .as_ref()
        .ok_or("--jwt-key is required for OAuth JWT signing")?;
    let jwt_key_bytes = std::fs::read_to_string(jwt_key_path)?
        .trim()
        .as_bytes()
        .to_vec();

    let client_id = if let Some(ref fed_config_path) = auth.federated_jwt_config {
        let content = std::fs::read_to_string(fed_config_path)?;
        let config: serde_json::Value = serde_json::from_str(&content)?;
        config
            .get("providers")
            .and_then(|p| p.as_array())
            .and_then(|arr| arr.first())
            .and_then(|p| p.get("audience"))
            .and_then(|a| a.as_str())
            .map(String::from)
            .ok_or("federated JWT config must have providers[0].audience for OAuth client_id")?
    } else if let Some(ref aud) = auth.jwt_audience {
        aud.clone()
    } else {
        return Err("--jwt-audience or --federated-jwt-config required for OAuth client_id".into());
    };

    let redirect_uri = oauth.oauth_redirect_uri.as_ref().map_or_else(
        || format!("http://localhost:{}/oauth/callback", http_bind.port()),
        String::from,
    );

    let issuer = auth
        .jwt_issuer
        .clone()
        .unwrap_or_else(|| "mqdb".to_string());
    let audience = auth
        .jwt_audience
        .clone()
        .or_else(|| Some(client_id.clone()));

    Ok(mqdb::http::HttpServerConfig {
        bind_address: http_bind,
        oauth_config: mqdb::http::OAuthConfig {
            client_id,
            client_secret,
            redirect_uri,
        },
        jwt_config: mqdb::http::JwtSigningConfig {
            algorithm: mqdb::http::JwtSigningAlgorithm::HS256,
            key_bytes: jwt_key_bytes,
            issuer,
            audience,
            expiry_secs: 3600,
        },
        frontend_redirect_uri: oauth.oauth_frontend_redirect.clone(),
        ticket_expiry_secs: oauth.ticket_expiry_secs,
        cookie_secure: oauth.cookie_secure,
        cors_origin: oauth.cors_origin.clone(),
        ticket_rate_limit: oauth.ticket_rate_limit,
    })
}

fn cmd_scram(
    username: &str,
    batch: Option<String>,
    delete: bool,
    stdout: bool,
    file: Option<PathBuf>,
    iterations: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqtt5::broker::auth_mechanisms::{
        generate_scram_credential_line, generate_scram_credential_line_with_iterations,
    };
    use std::collections::HashMap;

    if username.contains(':') {
        return Err("username cannot contain ':' character".into());
    }
    if iterations < 10000 {
        return Err("iteration count must be at least 10000".into());
    }

    if delete {
        let path = file.ok_or("--file is required for --delete")?;
        let contents = std::fs::read_to_string(&path)?;
        let prefix = format!("{username}:");
        let remaining: Vec<&str> = contents
            .lines()
            .filter(|line| !line.starts_with(&prefix))
            .collect();
        std::fs::write(&path, remaining.join("\n") + "\n")?;
        eprintln!("Deleted user '{username}' from {}", path.display());
        return Ok(());
    }

    let password = if let Some(p) = batch {
        p
    } else {
        eprint!("Password: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    };

    let line = if iterations == 310_000 {
        generate_scram_credential_line(username, &password)?
    } else {
        generate_scram_credential_line_with_iterations(username, &password, iterations)?
    };

    if stdout || file.is_none() {
        println!("{line}");
        return Ok(());
    }

    let path = file.unwrap();
    let mut users: HashMap<String, String> = HashMap::new();

    if path.exists() {
        let contents = std::fs::read_to_string(&path)?;
        for file_line in contents.lines() {
            let trimmed = file_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some(uname) = trimmed.split(':').next() {
                users.insert(uname.to_string(), trimmed.to_string());
            }
        }
    }

    let action = if users.contains_key(username) {
        "Updated"
    } else {
        "Added"
    };
    users.insert(username.to_string(), line);

    let mut sorted_names: Vec<&String> = users.keys().collect();
    sorted_names.sort();
    let mut output = String::new();
    for name in sorted_names {
        if let Some(l) = users.get(name) {
            output.push_str(l);
            output.push('\n');
        }
    }
    std::fs::write(&path, &output)?;
    eprintln!("{action} user '{username}' in {}", path.display());

    Ok(())
}

async fn cmd_acl(action: AclAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        AclAction::Add {
            username,
            topic,
            permission,
            file,
        } => cmd_acl_add(&username, &topic, &permission, &file).await,
        AclAction::Remove {
            username,
            topic,
            file,
        } => cmd_acl_remove(&username, topic.as_deref(), &file).await,
        AclAction::RoleAdd {
            role_name,
            topic,
            permission,
            file,
        } => cmd_acl_role_add(&role_name, &topic, &permission, &file).await,
        AclAction::RoleRemove {
            role_name,
            topic,
            file,
        } => cmd_acl_role_remove(&role_name, topic.as_deref(), &file).await,
        AclAction::RoleList { role_name, file } => {
            cmd_acl_role_list(role_name.as_deref(), &file).await
        }
        AclAction::Assign {
            username,
            role,
            file,
        } => cmd_acl_assign(&username, &role, &file).await,
        AclAction::Unassign {
            username,
            role,
            file,
        } => cmd_acl_unassign(&username, &role, &file).await,
        AclAction::List { user, file } => cmd_acl_list(user.as_deref(), &file).await,
        AclAction::Check {
            username,
            topic,
            action,
            file,
        } => cmd_acl_check(&username, &topic, &action, &file).await,
        AclAction::UserRoles { username, file } => cmd_acl_user_roles(&username, &file).await,
    }
}

async fn read_acl_lines(path: &PathBuf) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let content = tokio::fs::read_to_string(path).await?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(String::from)
        .collect())
}

async fn write_acl_lines(
    path: &PathBuf,
    lines: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::AsyncWriteExt;
    let mut content = String::new();
    for line in lines {
        content.push_str(line);
        content.push('\n');
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(content.as_bytes()).await?;
    Ok(())
}

async fn cmd_acl_add(
    username: &str,
    topic: &str,
    permission: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let valid = ["read", "write", "readwrite", "deny"];
    if !valid.contains(&permission) {
        return Err(format!(
            "invalid permission '{permission}': must be read, write, readwrite, or deny"
        )
        .into());
    }
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let rule = format!("user {username} topic {topic} permission {permission}");
    if rules.iter().any(|r| r == &rule) {
        println!("Rule already exists: {rule}");
        return Ok(());
    }
    rules.push(rule.clone());
    write_acl_lines(file, &rules).await?;
    println!("Added ACL rule: {rule}");
    Ok(())
}

async fn cmd_acl_remove(
    username: &str,
    topic: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let original = rules.len();
    rules.retain(|rule| {
        let parts: Vec<&str> = rule.split_whitespace().collect();
        if parts.len() != 6 || parts[0] != "user" || parts[2] != "topic" || parts[4] != "permission"
        {
            return true;
        }
        if parts[1] != username {
            return true;
        }
        if let Some(t) = topic {
            parts[3] != t
        } else {
            false
        }
    });
    let removed = original - rules.len();
    if removed == 0 {
        return Err(format!("no rules found for user '{username}'").into());
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed {removed} rule(s) for user '{username}'");
    Ok(())
}

async fn cmd_acl_role_add(
    role_name: &str,
    topic: &str,
    permission: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let valid = ["read", "write", "readwrite", "deny"];
    if !valid.contains(&permission) {
        return Err(format!("invalid permission '{permission}'").into());
    }
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let rule = format!("role {role_name} topic {topic} permission {permission}");
    if rules.iter().any(|r| r == &rule) {
        println!("Role rule already exists: {rule}");
        return Ok(());
    }
    rules.push(rule.clone());
    write_acl_lines(file, &rules).await?;
    println!("Added role rule: {rule}");
    Ok(())
}

async fn cmd_acl_role_remove(
    role_name: &str,
    topic: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let original = rules.len();
    rules.retain(|rule| {
        let parts: Vec<&str> = rule.split_whitespace().collect();
        if parts.len() != 6 || parts[0] != "role" || parts[2] != "topic" || parts[4] != "permission"
        {
            return true;
        }
        if parts[1] != role_name {
            return true;
        }
        if let Some(t) = topic {
            parts[3] != t
        } else {
            false
        }
    });
    let removed = original - rules.len();
    if removed == 0 {
        return Err(format!("no rules found for role '{role_name}'").into());
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed {removed} rule(s) from role '{role_name}'");
    Ok(())
}

async fn cmd_acl_role_list(
    role_name: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules = read_acl_lines(file).await?;
    let filtered: Vec<&String> = rules
        .iter()
        .filter(|r| {
            let parts: Vec<&str> = r.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "role" {
                return false;
            }
            role_name.is_none_or(|name| parts[1] == name)
        })
        .collect();
    if filtered.is_empty() {
        println!("No role rules found");
    } else {
        for rule in &filtered {
            println!("  {rule}");
        }
        println!("\nTotal: {} rule(s)", filtered.len());
    }
    Ok(())
}

async fn cmd_acl_assign(
    username: &str,
    role_name: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let line = format!("assign {username} {role_name}");
    if rules.iter().any(|r| r == &line) {
        println!("Assignment already exists");
        return Ok(());
    }
    rules.push(line);
    write_acl_lines(file, &rules).await?;
    println!("Assigned role '{role_name}' to user '{username}'");
    Ok(())
}

async fn cmd_acl_unassign(
    username: &str,
    role_name: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let line = format!("assign {username} {role_name}");
    let original = rules.len();
    rules.retain(|r| r != &line);
    if rules.len() == original {
        return Err(
            format!("no assignment found for user '{username}' with role '{role_name}'").into(),
        );
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed role '{role_name}' from user '{username}'");
    Ok(())
}

async fn cmd_acl_list(
    user: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules = read_acl_lines(file).await?;
    let filtered: Vec<&String> = if let Some(u) = user {
        rules
            .iter()
            .filter(|r| {
                let parts: Vec<&str> = r.split_whitespace().collect();
                parts.len() >= 2 && parts[0] == "user" && parts[1] == u
            })
            .collect()
    } else {
        rules.iter().collect()
    };
    if filtered.is_empty() {
        println!("No ACL rules found");
    } else {
        for rule in &filtered {
            println!("  {rule}");
        }
        println!("\nTotal: {} rule(s)", filtered.len());
    }
    Ok(())
}

async fn cmd_acl_check(
    username: &str,
    topic: &str,
    action: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqtt5::broker::acl::AclManager;

    if action != "read" && action != "write" {
        return Err(format!("action must be 'read' or 'write', got: {action}").into());
    }
    let acl_manager = AclManager::from_file(file).await?;
    let allowed = if action == "read" {
        acl_manager.check_subscribe(Some(username), topic).await
    } else {
        acl_manager.check_publish(Some(username), topic).await
    };
    if allowed {
        println!("ALLOWED: user '{username}' can {action} topic '{topic}'");
    } else {
        println!("DENIED: user '{username}' cannot {action} topic '{topic}'");
    }
    Ok(())
}

async fn cmd_acl_user_roles(
    username: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let lines = read_acl_lines(file).await?;
    let assigned_roles: Vec<&str> = lines
        .iter()
        .filter_map(|r| {
            let parts: Vec<&str> = r.split_whitespace().collect();
            if parts.len() == 3 && parts[0] == "assign" && parts[1] == username {
                Some(parts[2])
            } else {
                None
            }
        })
        .collect();
    if assigned_roles.is_empty() {
        println!("User '{username}' has no assigned roles");
    } else {
        println!("Roles for user '{username}':");
        for role in &assigned_roles {
            println!("  {role}");
        }
    }
    Ok(())
}

async fn cmd_create(
    entity: String,
    data: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Value = serde_json::from_str(&data)?;
    let topic = format!("$DB/{entity}/create");
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_read(
    entity: String,
    id: String,
    projection: Option<String>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/{id}");
    let payload = if let Some(proj) = projection {
        let fields: Vec<&str> = proj.split(',').collect();
        json!({ "projection": fields })
    } else {
        json!({})
    };
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_update(
    entity: String,
    id: String,
    data: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Value = serde_json::from_str(&data)?;
    let topic = format!("$DB/{entity}/{id}/update");
    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_delete(
    entity: String,
    id: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/{id}/delete");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_list(
    entity: String,
    filters: Vec<String>,
    sort: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/{entity}/list");

    let mut payload = json!({});

    if !filters.is_empty() {
        let parsed_filters: Vec<Value> = filters.iter().flat_map(|f| parse_filters(f)).collect();
        payload["filters"] = json!(parsed_filters);
    }

    if let Some(sort_str) = sort {
        let sort_orders: Vec<Value> = sort_str
            .split(',')
            .map(|s| {
                let parts: Vec<&str> = s.split(':').collect();
                let field = parts[0];
                let direction = parts.get(1).unwrap_or(&"asc");
                json!({ "field": field, "direction": direction })
            })
            .collect();
        payload["sort"] = json!(sort_orders);
    }

    if limit.is_some() || offset.is_some() {
        payload["pagination"] = json!({
            "limit": limit.unwrap_or(100),
            "offset": offset.unwrap_or(0)
        });
    }

    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_watch(
    entity: String,
    filters: Vec<String>,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed_filters: Vec<Value> = filters.iter().flat_map(|f| parse_filters(f)).collect();

    let client = Box::pin(connect_client(&conn)).await?;
    let topic = format!("$DB/{entity}/events/#");

    let (tx, rx) = flume::bounded::<Value>(100);

    client
        .subscribe(&topic, move |msg| {
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send(value);
            }
        })
        .await?;

    eprintln!("Watching {entity} events (Ctrl+C to stop)...");

    while let Ok(event) = rx.recv_async().await {
        if !parsed_filters.is_empty() {
            let data = event.get("data").unwrap_or(&event);
            if !matches_filters(data, &parsed_filters) {
                continue;
            }
        }
        output_response(&event, &format);
    }

    Ok(())
}

fn matches_filters(data: &Value, filters: &[Value]) -> bool {
    filters.iter().all(|filter| {
        let field = filter.get("field").and_then(Value::as_str).unwrap_or("");
        let op = filter.get("op").and_then(Value::as_str).unwrap_or("eq");
        let expected = filter.get("value").cloned().unwrap_or(Value::Null);

        let actual = data.get(field).cloned().unwrap_or(Value::Null);

        match op {
            "eq" => actual == expected,
            "neq" => actual != expected,
            "gt" => {
                compare_values(&actual, &expected).is_some_and(|o| o == std::cmp::Ordering::Greater)
            }
            "lt" => {
                compare_values(&actual, &expected).is_some_and(|o| o == std::cmp::Ordering::Less)
            }
            "gte" => {
                compare_values(&actual, &expected).is_some_and(|o| o != std::cmp::Ordering::Less)
            }
            "lte" => {
                compare_values(&actual, &expected).is_some_and(|o| o != std::cmp::Ordering::Greater)
            }
            "like" => {
                if let (Some(a), Some(p)) = (actual.as_str(), expected.as_str()) {
                    glob_match(p, a)
                } else {
                    false
                }
            }
            "is_null" => actual.is_null(),
            "is_not_null" => !actual.is_null(),
            _ => false,
        }
    })
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }
    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if let Some(found) = text[pos..].find(part) {
            if i == 0 && found != 0 {
                return false;
            }
            pos += found + part.len();
        } else {
            return false;
        }
    }
    if parts.last().unwrap_or(&"").is_empty() {
        true
    } else {
        pos == text.len()
    }
}

async fn cmd_schema_set(
    entity: String,
    file: PathBuf,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = tokio::fs::read_to_string(&file).await?;
    let schema: Value = serde_json::from_str(&content)?;
    let topic = format!("$DB/_admin/schema/{entity}/set");
    let response = Box::pin(execute_request(&conn, &topic, schema)).await?;
    output_response(&response, &OutputFormat::Json);
    Ok(())
}

async fn cmd_schema_get(
    entity: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/schema/{entity}/get");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_constraint_add(
    entity: String,
    name: Option<String>,
    unique: Option<String>,
    fk: Option<String>,
    not_null: Option<String>,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/constraint/{entity}/add");

    let payload = if let Some(field) = unique {
        let constraint_name = name.unwrap_or_else(|| format!("unique_{entity}_{field}"));
        json!({ "type": "unique", "name": constraint_name, "field": field })
    } else if let Some(fk_spec) = fk {
        let parts: Vec<&str> = fk_spec.split(':').collect();
        if parts.len() < 3 {
            return Err("FK format: field:target_entity:target_field[:action]".into());
        }
        let constraint_name = name.unwrap_or_else(|| format!("fk_{entity}_{}", parts[0]));
        json!({
            "type": "foreign_key",
            "name": constraint_name,
            "field": parts[0],
            "target_entity": parts[1],
            "target_field": parts[2],
            "on_delete": parts.get(3).unwrap_or(&"restrict")
        })
    } else if let Some(field) = not_null {
        let constraint_name = name.unwrap_or_else(|| format!("not_null_{entity}_{field}"));
        json!({ "type": "not_null", "name": constraint_name, "field": field })
    } else {
        return Err("Must specify --unique, --fk, or --not-null".into());
    };

    let response = Box::pin(execute_request(&conn, &topic, payload)).await?;
    output_response(&response, &OutputFormat::Json);
    Ok(())
}

async fn cmd_constraint_list(
    entity: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/constraint/{entity}/list");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_backup_create(
    name: &str,
    conn: &ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/backup";
    let payload = json!({"name": name});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        if let Some(data) = response.get("data")
            && let Some(msg) = data.get("message").and_then(serde_json::Value::as_str)
        {
            println!("{msg}");
        }
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_backup_list(conn: &ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/backup/list";
    let payload = json!({});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        if let Some(backups) = response.get("data").and_then(|v| v.as_array()) {
            if backups.is_empty() {
                println!("No backups found");
            } else {
                println!("Available backups:");
                for backup in backups {
                    if let Some(name) = backup.as_str() {
                        println!("  {name}");
                    }
                }
            }
        }
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_restore(name: &str, conn: &ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/restore";
    let payload = json!({"name": name});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        println!("Restore initiated");
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn connect_client(conn: &ConnectionArgs) -> Result<MqttClient, Box<dyn std::error::Error>> {
    let client = MqttClient::new("mqdb-cli");

    if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
        let options = ConnectOptions::new("mqdb-cli").with_credentials(user.clone(), pass.clone());
        Box::pin(client.connect_with_options(&conn.broker, options)).await?;
    } else {
        client.connect(&conn.broker).await?;
    }

    Ok(client)
}

async fn execute_request(
    conn: &ConnectionArgs,
    topic: &str,
    payload: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(conn)).await?;

    let response_topic = format!("mqdb-cli/responses/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Value>(1);

    client
        .subscribe(&response_topic, move |msg| {
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send(value);
            }
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    client
        .publish_with_options(topic, serde_json::to_vec(&payload)?, opts)
        .await?;

    let timeout = Duration::from_secs(conn.timeout);
    let response = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .map_err(|_| "Request timed out")?
        .map_err(|_| "No response received")?;

    client.disconnect().await?;

    Ok(response)
}

fn parse_filters(filter_str: &str) -> Vec<Value> {
    filter_str
        .split(',')
        .filter_map(|part| {
            let part = part.trim();

            let ops = ["<>", "!=", ">=", "<=", "!?", ">", "<", "=", "~", "?"];

            for op in ops {
                if let Some(pos) = part.find(op) {
                    let field = part[..pos].trim();
                    let value_str = part[pos + op.len()..].trim();

                    let filter_op = match op {
                        "!=" | "<>" => "neq",
                        ">" => "gt",
                        "<" => "lt",
                        ">=" => "gte",
                        "<=" => "lte",
                        "~" => "like",
                        "?" => "is_null",
                        "!?" => "is_not_null",
                        _ => "eq",
                    };

                    let value: Value = if op == "?" || op == "!?" {
                        Value::Bool(true)
                    } else if let Ok(n) = value_str.parse::<i64>() {
                        Value::Number(n.into())
                    } else if let Ok(f) = value_str.parse::<f64>() {
                        serde_json::Number::from_f64(f)
                            .map(Value::Number)
                            .unwrap_or(Value::String(value_str.to_string()))
                    } else if value_str == "true" {
                        Value::Bool(true)
                    } else if value_str == "false" {
                        Value::Bool(false)
                    } else {
                        Value::String(value_str.to_string())
                    };

                    return Some(json!({
                        "field": field,
                        "op": filter_op,
                        "value": value
                    }));
                }
            }
            None
        })
        .collect()
}

fn output_response(response: &Value, format: &OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(response).unwrap());
        }
        OutputFormat::Table => {
            if let Some(data) = response.get("data") {
                if let Some(arr) = data.as_array() {
                    output_table(arr);
                } else if data.is_object() {
                    output_table(std::slice::from_ref(data));
                } else {
                    println!("{}", serde_json::to_string_pretty(response).unwrap());
                }
            } else {
                println!("{}", serde_json::to_string_pretty(response).unwrap());
            }
        }
        OutputFormat::Csv => {
            if let Some(data) = response.get("data") {
                if let Some(arr) = data.as_array() {
                    output_csv(arr);
                } else if data.is_object() {
                    output_csv(std::slice::from_ref(data));
                } else {
                    println!("{}", serde_json::to_string_pretty(response).unwrap());
                }
            } else {
                println!("{}", serde_json::to_string_pretty(response).unwrap());
            }
        }
    }
}

fn output_table(data: &[Value]) {
    use comfy_table::{ContentArrangement, Table};

    if data.is_empty() {
        println!("(no results)");
        return;
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let first = &data[0];
    if let Some(obj) = first.as_object() {
        let headers: Vec<&str> = obj.keys().map(String::as_str).collect();
        table.set_header(&headers);

        for item in data {
            if let Some(obj) = item.as_object() {
                let row: Vec<String> = headers
                    .iter()
                    .map(|h| {
                        obj.get(*h)
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                Value::Null => "null".to_string(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                table.add_row(row);
            }
        }
    }

    println!("{table}");
}

fn output_csv(data: &[Value]) {
    if data.is_empty() {
        return;
    }

    let first = &data[0];
    if let Some(obj) = first.as_object() {
        let headers: Vec<&str> = obj.keys().map(String::as_str).collect();
        println!("{}", headers.join(","));

        for item in data {
            if let Some(obj) = item.as_object() {
                let row: Vec<String> = headers
                    .iter()
                    .map(|h| {
                        obj.get(*h)
                            .map(|v| match v {
                                Value::String(s) => {
                                    if s.contains(',') || s.contains('"') {
                                        format!("\"{}\"", s.replace('"', "\"\""))
                                    } else {
                                        s.clone()
                                    }
                                }
                                Value::Null => String::new(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                println!("{}", row.join(","));
            }
        }
    }
}

async fn cmd_subscribe(
    pattern: String,
    entity: Option<String>,
    group: Option<String>,
    mode: SubscriptionModeArg,
    heartbeat_interval: u64,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_sub/subscribe";
    let mode_str = match mode {
        SubscriptionModeArg::Broadcast => "broadcast",
        SubscriptionModeArg::LoadBalanced => "load-balanced",
        SubscriptionModeArg::Ordered => "ordered",
    };

    let payload = json!({
        "pattern": pattern,
        "entity": entity,
        "group": group,
        "mode": mode_str
    });

    let response = Box::pin(execute_request(&conn, topic, payload)).await?;

    let is_ok = response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| s == "ok");

    if is_ok {
        let data = response.get("data").unwrap_or(&Value::Null);
        let sub_id = data
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        eprintln!("Subscription ID: {sub_id}");
        if let Some(partitions) = data.get("partitions").and_then(|v| v.as_array())
            && !partitions.is_empty()
        {
            eprintln!("Assigned partitions: {partitions:?}");
        }

        let event_pattern = if let Some(ref ent) = entity {
            format!("$DB/{ent}/events/#")
        } else {
            "$DB/+/events/#".to_string()
        };

        let client = Box::pin(connect_client(&conn)).await?;
        let (tx, rx) = flume::bounded::<Value>(100);

        client
            .subscribe(&event_pattern, move |msg| {
                if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                    let _ = tx.try_send(value);
                }
            })
            .await?;

        eprintln!("Watching events (Ctrl+C to stop)...");

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_clone.store(true, Ordering::SeqCst);
        });

        let heartbeat_conn = conn.clone();
        let heartbeat_sub_id = sub_id.to_string();
        let heartbeat_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
            while !heartbeat_shutdown.load(Ordering::SeqCst) {
                interval.tick().await;
                if heartbeat_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let topic = format!("$DB/_sub/{heartbeat_sub_id}/heartbeat");
                let _ = Box::pin(execute_request(&heartbeat_conn, &topic, json!({}))).await;
            }
        });

        while !shutdown.load(Ordering::SeqCst) {
            tokio::select! {
                event = rx.recv_async() => {
                    if let Ok(event) = event {
                        output_response(&event, &format);
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }

        eprintln!("\nUnsubscribing...");
        let unsub_topic = format!("$DB/_sub/{sub_id}/unsubscribe");
        let _ = Box::pin(execute_request(&conn, &unsub_topic, json!({}))).await;
        client.disconnect().await?;
    } else if let Some(error) = response.get("message").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_consumer_group_list(
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/consumer-groups";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

async fn cmd_consumer_group_show(
    name: String,
    conn: ConnectionArgs,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/consumer-groups/{name}");
    let response = Box::pin(execute_request(&conn, &topic, json!({}))).await?;
    output_response(&response, &format);
    Ok(())
}

async fn execute_db_request(
    conn: &ConnectionArgs,
    topic: &str,
    payload: Vec<u8>,
) -> Result<DbResponse, Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(conn)).await?;

    let response_topic = format!("$DB/_resp/mqdb-cli-{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    client.publish_with_options(topic, payload, opts).await?;

    let timeout = Duration::from_secs(conn.timeout);
    let response_bytes = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .map_err(|_| "Request timed out")?
        .map_err(|_| "No response received")?;

    client.disconnect().await?;

    let (response, _) = DbResponse::try_from_be_bytes(&response_bytes)
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    Ok(response)
}

#[allow(clippy::cast_possible_truncation)]
async fn cmd_db_create(
    partition: u16,
    entity: String,
    data: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let request = DbWriteRequest::create(data.as_bytes(), timestamp_ms);
    let topic = format!("$DB/p{partition}/{entity}/create");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "Created: {} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Created successfully");
            }
        }
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

async fn cmd_db_read(
    partition: u16,
    entity: String,
    id: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = DbReadRequest::create();
    let topic = format!("$DB/p{partition}/{entity}/{id}");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "{} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Data: {:?}", response.data);
            }
        }
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
async fn cmd_db_update(
    partition: u16,
    entity: String,
    id: String,
    data: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let request = DbWriteRequest::create(data.as_bytes(), timestamp_ms);
    let topic = format!("$DB/p{partition}/{entity}/{id}/update");

    let response: DbResponse =
        Box::pin(execute_db_request(&conn, &topic, request.to_be_bytes())).await?;

    match response.status() {
        DbStatus::Ok => {
            if let Ok((db_entity, _)) = DbEntity::try_from_be_bytes(&response.data) {
                println!(
                    "Updated: {} {} {}",
                    db_entity.entity_str(),
                    db_entity.id_str(),
                    String::from_utf8_lossy(&db_entity.data)
                );
            } else {
                println!("Updated successfully");
            }
        }
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

async fn cmd_db_delete(
    partition: u16,
    entity: String,
    id: String,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/p{partition}/{entity}/{id}/delete");

    let response: DbResponse = Box::pin(execute_db_request(&conn, &topic, Vec::new())).await?;

    match response.status() {
        DbStatus::Ok => println!("Deleted: {entity}/{id}"),
        DbStatus::NotFound => eprintln!("Not found"),
        status => eprintln!("Error: {status:?}"),
    }

    Ok(())
}

fn cmd_dev_ps() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("pgrep").args(["-fl", "mqdb"]).output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.is_empty() {
        println!("No MQDB processes running");
        return Ok(());
    }

    println!("{:<8} {:<10} DETAILS", "PID", "TYPE");
    println!("{}", "-".repeat(60));

    for line in stdout.lines() {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() < 2 {
            continue;
        }
        let pid = parts[0];
        let cmd = parts[1];

        let proc_type = if cmd.contains("cluster start") {
            "cluster"
        } else if cmd.contains("agent start") {
            "agent"
        } else {
            "other"
        };

        let details = if let Some(node_pos) = cmd.find("--node-id") {
            let rest = &cmd[node_pos + 10..];
            let node_id: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if let Some(bind_pos) = cmd.find("--bind") {
                let bind_rest = &cmd[bind_pos + 7..];
                let bind: String = bind_rest
                    .chars()
                    .take_while(|c| !c.is_whitespace())
                    .collect();
                format!("node={node_id} bind={bind}")
            } else {
                format!("node={node_id}")
            }
        } else if let Some(bind_pos) = cmd.find("--bind") {
            let bind_rest = &cmd[bind_pos + 7..];
            let bind: String = bind_rest
                .chars()
                .take_while(|c| !c.is_whitespace())
                .collect();
            format!("bind={bind}")
        } else {
            String::new()
        };

        println!("{pid:<8} {proc_type:<10} {details}");
    }

    Ok(())
}

fn cmd_dev_kill(all: bool, node: Option<u16>, agent: bool) {
    if agent {
        println!("Killing MQDB agent...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return;
    }

    if let Some(node_id) = node {
        println!("Killing MQDB cluster node {node_id}...");
        let pattern = format!("mqdb cluster.*--node-id {node_id}");
        let _ = Command::new("pkill").args(["-f", &pattern]).status();
        println!("Done");
        return;
    }

    if all {
        println!("Killing all MQDB processes...");
        let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        println!("Done");
        return;
    }

    println!("Killing all MQDB cluster nodes...");
    let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
    println!("Done");
}

fn cmd_dev_clean(db_prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pattern = format!("{db_prefix}-*");
    let entries: Vec<_> = glob::glob(&pattern)?.filter_map(Result::ok).collect();

    if entries.is_empty() {
        println!("No databases matching {pattern}");
        return Ok(());
    }

    println!("Cleaning {} database(s):", entries.len());
    for entry in &entries {
        println!("  Removing: {}", entry.display());
        if entry.is_dir() {
            std::fs::remove_dir_all(entry)?;
        } else {
            std::fs::remove_file(entry)?;
        }
    }
    println!("Done");
    Ok(())
}

fn cmd_dev_logs(
    node: Option<u16>,
    pattern: Option<&str>,
    follow: bool,
    last: usize,
    db_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let log_files: Vec<PathBuf> = if let Some(node_id) = node {
        let log_path = PathBuf::from(format!("{db_prefix}-{node_id}/mqdb.log"));
        if log_path.exists() {
            vec![log_path]
        } else {
            eprintln!("Log file not found: {}", log_path.display());
            return Ok(());
        }
    } else {
        glob::glob(&format!("{db_prefix}-*/mqdb.log"))?
            .filter_map(Result::ok)
            .collect()
    };

    if log_files.is_empty() {
        println!("No log files found matching {db_prefix}-*/mqdb.log");
        return Ok(());
    }

    for log_file in &log_files {
        let node_id = log_file
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .and_then(|s| {
                s.strip_prefix(&format!(
                    "{}-",
                    db_prefix.rsplit('/').next().unwrap_or("mqdb-test")
                ))
            })
            .unwrap_or("?");

        println!("=== Node {node_id} ({}) ===", log_file.display());

        if follow {
            let mut cmd = Command::new("tail");
            cmd.args(["-f", "-n", &last.to_string()]);
            cmd.arg(log_file);
            if let Some(pat) = pattern {
                let child = cmd.stdout(std::process::Stdio::piped()).spawn()?;
                Command::new("grep")
                    .args(["--line-buffered", pat])
                    .stdin(child.stdout.unwrap())
                    .status()?;
            } else {
                cmd.status()?;
            }
        } else {
            let content = std::fs::read_to_string(log_file)?;
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(last);

            for line in &lines[start..] {
                if let Some(pat) = pattern {
                    if line.contains(pat) {
                        println!("{line}");
                    }
                } else {
                    println!("{line}");
                }
            }
        }
        println!();
    }

    Ok(())
}

fn wait_for_cluster_ready(nodes: u8, timeout_secs: u64) -> bool {
    use std::time::{Duration, Instant};

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    println!("Waiting for cluster to be ready ({nodes} nodes)...");

    while start.elapsed() < timeout {
        let mut all_ready = true;

        for i in 0..nodes {
            let port = 1883 + u16::from(i);
            let output = Command::new("timeout")
                .args([
                    "1",
                    "mosquitto_pub",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &port.to_string(),
                    "-t",
                    "ping",
                    "-m",
                    "pong",
                ])
                .output();

            if output.is_err() || !output.unwrap().status.success() {
                all_ready = false;
                break;
            }
        }

        if all_ready {
            std::thread::sleep(Duration::from_secs(2));
            println!("Cluster ready!");
            return true;
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    println!("Warning: cluster readiness check timed out");
    false
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn cmd_dev_test(
    pubsub: bool,
    db: bool,
    constraints: bool,
    wildcards: bool,
    retained: bool,
    lwt: bool,
    all: bool,
    nodes: u8,
) {
    let run_all = all || (!pubsub && !db && !constraints && !wildcards && !retained && !lwt);

    wait_for_cluster_ready(nodes, 10);
    let ports: Vec<u16> = (0..nodes).map(|i| 1883 + u16::from(i)).collect();

    if pubsub || run_all {
        run_test_pubsub(nodes, &ports);
    }

    if db || run_all {
        run_test_db(nodes, &ports);
    }

    if constraints || run_all {
        run_test_constraints(nodes, &ports);
    }

    if wildcards || run_all {
        run_test_wildcards(nodes, &ports);
    }

    if retained || run_all {
        run_test_retained(nodes, &ports);
    }

    if lwt || run_all {
        run_test_lwt(nodes, &ports);
    }
}

fn run_test_pubsub(nodes: u8, ports: &[u16]) {
    println!("=== Cross-Node Pub/Sub Matrix Test ({nodes} nodes) ===\n");

    let mut passed = 0;
    let mut failed = 0;

    for (src_idx, &src_port) in ports.iter().enumerate() {
        for (dst_idx, &dst_port) in ports.iter().enumerate() {
            if src_idx == dst_idx {
                continue;
            }

            let src_node = src_idx + 1;
            let dst_node = dst_idx + 1;
            let topic = format!("test/n{src_node}to{dst_node}");
            let msg = format!("msg_{src_node}_{dst_node}");

            let result = run_pubsub_test(src_port, dst_port, &topic, &msg);

            if result {
                println!("  Node {src_node} → Node {dst_node}: ✓");
                passed += 1;
            } else {
                println!("  Node {src_node} → Node {dst_node}: ✗");
                failed += 1;
            }

            std::thread::sleep(std::time::Duration::from_millis(300));
        }
    }

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_test_db(nodes: u8, ports: &[u16]) {
    println!("=== Cross-Node DB CRUD Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0;
    let mut failed = 0;

    for (idx, &port) in ports.iter().enumerate() {
        let node = idx + 1;

        let create_output = Command::new(&exe)
            .args([
                "create",
                "test_users",
                "-d",
                &format!(r#"{{"name": "User{node}", "node": {node}}}"#),
                "--broker",
                &format!("127.0.0.1:{port}"),
            ])
            .output();

        let created = create_output.map(|o| o.status.success()).unwrap_or(false);

        if created {
            println!("  Create via Node {node}: ✓");
            passed += 1;
        } else {
            println!("  Create via Node {node}: ✗");
            failed += 1;
        }

        let read_port = ports[(idx + 1) % ports.len()];
        let read_node = ((idx + 1) % ports.len()) + 1;

        let list_output = Command::new(&exe)
            .args([
                "list",
                "test_users",
                "-f",
                &format!("node={node}"),
                "--broker",
                &format!("127.0.0.1:{read_port}"),
            ])
            .output();

        let can_read = list_output
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains(&format!("User{node}"))
            })
            .unwrap_or(false);

        if can_read {
            println!("  Read from Node {read_node} (created on {node}): ✓");
            passed += 1;
        } else {
            println!("  Read from Node {read_node} (created on {node}): ✗");
            failed += 1;
        }

        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_test_constraints(nodes: u8, ports: &[u16]) {
    println!("=== Unique Constraint Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0;
    let mut failed = 0;

    let ts = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let entity = format!("test_products_{ts}");
    let constraint_name = format!("unique_{entity}_sku");

    let add_output = Command::new(&exe)
        .args([
            "constraint",
            "add",
            &entity,
            "--unique",
            "sku",
            "--name",
            &constraint_name,
            "--broker",
            &format!("127.0.0.1:{}", ports[0]),
            "--user",
            "admin",
        ])
        .output();

    let constraint_added = add_output
        .as_ref()
        .map(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.contains("constraint added") || stdout.contains("\"status\":\"ok\"")
        })
        .unwrap_or(false);

    if constraint_added {
        println!("  Add unique constraint via Node 1: ✓");
        passed += 1;
    } else {
        println!("  Add unique constraint via Node 1: ✗");
        if let Ok(o) = &add_output {
            eprintln!("    stdout: {}", String::from_utf8_lossy(&o.stdout));
            eprintln!("    stderr: {}", String::from_utf8_lossy(&o.stderr));
        }
        failed += 1;
        println!("\nResults: {passed} passed, {failed} failed\n");
        return;
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    let create1 = Command::new(&exe)
        .args([
            "create",
            &entity,
            "-d",
            r#"{"name": "Widget A", "sku": "SKU-001"}"#,
            "--broker",
            &format!("127.0.0.1:{}", ports[0]),
            "--user",
            "admin",
        ])
        .output();

    let first_created = create1.map(|o| o.status.success()).unwrap_or(false);

    if first_created {
        println!("  Create first product (SKU-001) via Node 1: ✓");
        passed += 1;
    } else {
        println!("  Create first product (SKU-001) via Node 1: ✗");
        failed += 1;
    }

    std::thread::sleep(std::time::Duration::from_millis(300));

    let create_dup = Command::new(&exe)
        .args([
            "create",
            &entity,
            "-d",
            r#"{"name": "Duplicate Widget", "sku": "SKU-001"}"#,
            "--broker",
            &format!("127.0.0.1:{}", ports[0]),
            "--user",
            "admin",
        ])
        .output();

    let dup_rejected = create_dup
        .map(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let stderr = String::from_utf8_lossy(&o.stderr);
            let combined = format!("{stdout}{stderr}").to_lowercase();
            !o.status.success()
                || combined.contains("unique")
                || combined.contains("conflict")
                || combined.contains("duplicate")
                || combined.contains("constraint")
        })
        .unwrap_or(false);

    if dup_rejected {
        println!("  Duplicate SKU-001 rejected: ✓");
        passed += 1;
    } else {
        println!("  Duplicate SKU-001 rejected: ✗");
        failed += 1;
    }

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_test_wildcards(nodes: u8, ports: &[u16]) {
    println!("=== Cross-Node Wildcard Subscription Test ({nodes} nodes) ===\n");

    let mut passed = 0;
    let mut failed = 0;

    let sub_port = ports[0];
    let pub_port = ports[ports.len() - 1];
    let src_node = ports.len();
    let dst_node = 1;

    let ts = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let sub_id = format!("wild_sub_{ts}");
    let pub_id = format!("wild_pub_{ts}");

    let single_level = Command::new("timeout")
        .args([
            "5",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_id}_single' -h 127.0.0.1 -p {sub_port} -t 'sensors/+/temp' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_id}_single' -h 127.0.0.1 -p {pub_port} -t 'sensors/room1/temp' -m 'single_level_ok'; wait"
            ),
        ])
        .output();

    if single_level
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("single_level_ok"))
        .unwrap_or(false)
    {
        println!("  Single-level (+) Node {src_node} → Node {dst_node}: ✓");
        passed += 1;
    } else {
        println!("  Single-level (+) Node {src_node} → Node {dst_node}: ✗");
        failed += 1;
    }

    std::thread::sleep(std::time::Duration::from_millis(300));

    let multi_level = Command::new("timeout")
        .args([
            "5",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_id}_multi' -h 127.0.0.1 -p {sub_port} -t 'home/#' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_id}_multi' -h 127.0.0.1 -p {pub_port} -t 'home/kitchen/oven' -m 'multi_level_ok'; wait"
            ),
        ])
        .output();

    if multi_level
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("multi_level_ok"))
        .unwrap_or(false)
    {
        println!("  Multi-level (#) Node {src_node} → Node {dst_node}: ✓");
        passed += 1;
    } else {
        println!("  Multi-level (#) Node {src_node} → Node {dst_node}: ✗");
        failed += 1;
    }

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_test_retained(nodes: u8, ports: &[u16]) {
    println!("=== Cross-Node Retained Message Test ({nodes} nodes) ===\n");

    let mut passed = 0;
    let mut failed = 0;

    let ts = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let topic = format!("retained/test/{ts}");
    let msg = format!("retained_msg_{ts}");

    let pub_port = ports[0];
    let sub_port = ports[ports.len() - 1];

    let _ = Command::new("mosquitto_pub")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &pub_port.to_string(),
            "-t",
            &topic,
            "-m",
            &msg,
            "-r",
            "-q",
            "1",
        ])
        .output();

    std::thread::sleep(std::time::Duration::from_millis(1000));

    let sub_output = Command::new("timeout")
        .args([
            "3",
            "mosquitto_sub",
            "-h",
            "127.0.0.1",
            "-p",
            &sub_port.to_string(),
            "-t",
            &topic,
            "-C",
            "1",
        ])
        .output();

    let received = sub_output
        .map(|o| String::from_utf8_lossy(&o.stdout).contains(&msg))
        .unwrap_or(false);

    if received {
        println!(
            "  Retained from Node 1, received on Node {}: ✓",
            ports.len()
        );
        passed += 1;
    } else {
        println!(
            "  Retained from Node 1, received on Node {}: ✗",
            ports.len()
        );
        failed += 1;
    }

    let _ = Command::new("mosquitto_pub")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &pub_port.to_string(),
            "-t",
            &topic,
            "-m",
            "",
            "-r",
        ])
        .output();

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_test_lwt(nodes: u8, ports: &[u16]) {
    println!("=== Cross-Node Last Will & Testament Test ({nodes} nodes) ===\n");

    let mut passed = 0;
    let mut failed = 0;

    let ts = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let will_topic = format!("lwt/status/{ts}");
    let will_msg = format!("client_offline_{ts}");

    let connect_port = ports[0];
    let sub_port = ports[ports.len() - 1];

    let sub_id = format!("lwt_sub_{ts}");
    let client_id = format!("lwt_client_{ts}");

    let lwt_output = Command::new("timeout")
        .args([
            "12",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_id}' -h 127.0.0.1 -p {sub_port} -t '{will_topic}' -C 1 & SUB_PID=$!; sleep 2; mosquitto_sub -i '{client_id}' -h 127.0.0.1 -p {connect_port} -t 'dummy/topic' --will-topic '{will_topic}' --will-payload '{will_msg}' --will-qos 1 & sleep 2; kill -9 $!; wait $SUB_PID"
            ),
        ])
        .output();

    let received = lwt_output
        .map(|o| String::from_utf8_lossy(&o.stdout).contains(&will_msg))
        .unwrap_or(false);

    if received {
        println!("  LWT from Node 1 received on Node {}: ✓", ports.len());
        passed += 1;
    } else {
        println!("  LWT from Node 1 received on Node {}: ✗", ports.len());
        failed += 1;
    }

    println!("\nResults: {passed} passed, {failed} failed\n");
}

fn run_pubsub_test(pub_port: u16, sub_port: u16, topic: &str, msg: &str) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let sub_client_id = format!("sub-{sub_port}-{ts}");
    let pub_client_id = format!("pub-{pub_port}-{ts}");
    let output = Command::new("timeout")
        .args([
            "5",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_client_id}' -h 127.0.0.1 -p {sub_port} -t '{topic}' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_client_id}' -h 127.0.0.1 -p {pub_port} -t '{topic}' -m '{msg}'; wait"
            ),
        ])
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.trim() == msg
        }
        Err(_) => false,
    }
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn cmd_dev_start_cluster(
    nodes: u8,
    clean: bool,
    quic_cert: &std::path::Path,
    quic_key: &std::path::Path,
    quic_ca: &std::path::Path,
    no_quic: bool,
    db_prefix: &str,
    bind_host: &str,
    topology: Option<&str>,
    bridge_out: bool,
    no_bridge_out: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if clean {
        println!("Cleaning existing databases...");
        let _ = cmd_dev_clean(db_prefix);
    }

    let exe = std::env::current_exe()?;

    let topology_name = topology.unwrap_or("partial");
    println!("Using {topology_name} mesh topology (bind: {bind_host})");

    for node_id in 1..=nodes {
        let port = 1882 + u16::from(node_id);
        let db_path = format!("{db_prefix}-{node_id}");

        let mut cmd = Command::new(&exe);
        cmd.args([
            "cluster",
            "start",
            "--node-id",
            &node_id.to_string(),
            "--bind",
            &format!("{bind_host}:{port}"),
            "--db",
            &db_path,
            "--admin-users",
            "admin",
        ]);

        let peers: Vec<String> = match topology_name {
            "full" => (1..=nodes)
                .filter(|&n| n != node_id)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
            "upper" => ((node_id + 1)..=nodes)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
            _ => (1..node_id)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
        };
        if !peers.is_empty() {
            cmd.args(["--peers", &peers.join(",")]);
        }

        if !no_bridge_out && (bridge_out || topology_name == "full") {
            cmd.arg("--bridge-out");
        }

        if !no_quic && quic_cert.exists() && quic_key.exists() {
            cmd.args([
                "--quic-cert",
                quic_cert.to_str().unwrap_or(""),
                "--quic-key",
                quic_key.to_str().unwrap_or(""),
            ]);
            if quic_ca.exists() {
                cmd.args(["--quic-ca", quic_ca.to_str().unwrap_or("")]);
            }
            #[cfg(feature = "dev-insecure")]
            cmd.arg("--quic-insecure");
        }

        let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
        cmd.env("RUST_LOG", &rust_log);

        std::fs::create_dir_all(&db_path)?;
        let log_file = std::fs::File::create(format!("{db_path}/mqdb.log"))?;
        cmd.stdout(log_file.try_clone()?);
        cmd.stderr(log_file);

        let transport_mode = if no_quic { " (TCP)" } else { " (QUIC)" };
        println!("Starting node {node_id} on port {port}{transport_mode}...");

        cmd.spawn()?;

        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    println!("\nCluster started with {nodes} nodes");
    println!("Use 'mqdb dev ps' to check status");
    println!("Use 'mqdb dev kill' to stop");

    Ok(())
}

async fn wait_for_broker_ready(
    conn: &ConnectionArgs,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    print!("Waiting for broker...");
    std::io::Write::flush(&mut std::io::stdout())?;

    loop {
        if start.elapsed() > timeout {
            println!(" timeout!");
            return Err(format!("Broker not ready after {timeout_secs}s").into());
        }

        let client_id = format!("bench-ready-{}", uuid::Uuid::new_v4());
        let client = MqttClient::new(&client_id);
        if conn.insecure {
            client.set_insecure_tls(true).await;
        }
        let connected = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
            let opts = ConnectOptions::new(&client_id).with_credentials(user.clone(), pass.clone());
            Box::pin(client.connect_with_options(&conn.broker, opts))
                .await
                .is_ok()
        } else {
            client.connect(&conn.broker).await.is_ok()
        };

        if connected {
            let response_topic = format!("bench-ready/{}", uuid::Uuid::new_v4());
            let (payload_tx, payload_rx) = flume::bounded::<Vec<u8>>(1);

            let sub_ok = client
                .subscribe(&response_topic, move |msg| {
                    let _ = payload_tx.try_send(msg.payload.clone());
                })
                .await
                .is_ok();

            if sub_ok {
                let opts = PublishOptions {
                    properties: PublishProperties {
                        response_topic: Some(response_topic.clone()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                let _ = client
                    .publish_with_options("$DB/_health", b"{}".to_vec(), opts)
                    .await;

                if let Ok(Ok(payload)) =
                    tokio::time::timeout(Duration::from_secs(2), payload_rx.recv_async()).await
                    && let Ok(json) = serde_json::from_slice::<serde_json::Value>(&payload)
                    && json
                        .get("data")
                        .and_then(|d| d.get("ready"))
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                {
                    let _ = client.disconnect().await;
                    println!(" ready!");
                    return Ok(());
                }
            }
            let _ = client.disconnect().await;
        }

        print!(".");
        std::io::Write::flush(&mut std::io::stdout())?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

struct BenchPubsubArgs {
    publishers: usize,
    subscribers: usize,
    duration: u64,
    size: usize,
    qos: u8,
    topic: String,
    topics: usize,
    wildcard: bool,
    warmup: u64,
    conn: ConnectionArgs,
    pub_broker: Option<String>,
    sub_broker: Option<String>,
    format: OutputFormat,
}

#[derive(Default)]
struct BenchMetrics {
    messages_sent: std::sync::atomic::AtomicU64,
    messages_received: std::sync::atomic::AtomicU64,
    latencies_ns: std::sync::Mutex<Vec<u64>>,
    errors: std::sync::atomic::AtomicU64,
}

impl BenchMetrics {
    fn record_latency(&self, ns: u64) {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            latencies.push(ns);
        }
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn calculate_percentile(&self, p: f64) -> u64 {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            if latencies.is_empty() {
                return 0;
            }
            latencies.sort_unstable();
            let idx = ((latencies.len() as f64) * p / 100.0) as usize;
            latencies[idx.min(latencies.len() - 1)]
        } else {
            0
        }
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
async fn cmd_bench_pubsub(args: BenchPubsubArgs) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let sub_broker = args.sub_broker.as_ref().unwrap_or(&args.conn.broker);
    let pub_broker = args.pub_broker.as_ref().unwrap_or(&args.conn.broker);
    let is_cross_node = sub_broker != pub_broker;

    let sub_conn = ConnectionArgs {
        broker: sub_broker.clone(),
        user: args.conn.user.clone(),
        pass: args.conn.pass.clone(),
        timeout: args.conn.timeout,
        insecure: args.conn.insecure,
    };
    wait_for_broker_ready(&sub_conn, 30).await?;

    if is_cross_node {
        let pub_conn = ConnectionArgs {
            broker: pub_broker.clone(),
            user: args.conn.user.clone(),
            pass: args.conn.pass.clone(),
            timeout: args.conn.timeout,
            insecure: args.conn.insecure,
        };
        wait_for_broker_ready(&pub_conn, 30).await?;
    }

    let metrics = Arc::new(BenchMetrics::default());
    let running = Arc::new(AtomicBool::new(true));

    let topics_str = if args.topics > 1 {
        let sub_mode = if args.wildcard {
            "wildcard"
        } else {
            "individual"
        };
        format!(", {} topics ({})", args.topics, sub_mode)
    } else {
        String::new()
    };

    if is_cross_node {
        println!(
            "Benchmark (cross-node): {} publishers @ {}, {} subscribers @ {}, {}s duration ({}s warmup), {} byte payload, QoS {}{}",
            args.publishers,
            pub_broker,
            args.subscribers,
            sub_broker,
            args.duration,
            args.warmup,
            args.size,
            args.qos,
            topics_str
        );
    } else {
        println!(
            "Benchmark: {} publishers, {} subscribers, {}s duration ({}s warmup), {} byte payload, QoS {}{}",
            args.publishers,
            args.subscribers,
            args.duration,
            args.warmup,
            args.size,
            args.qos,
            topics_str
        );
    }

    let mut sub_handles = Vec::new();
    let mut pub_handles = Vec::new();

    let use_wildcard = args.wildcard;
    let topic_count = args.topics;
    let topic_base = args.topic.clone();

    for sub_id in 0..args.subscribers {
        let broker = sub_broker.clone();
        let conn = args.conn.clone();
        let topic_base = topic_base.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-sub-{sub_id}");
            let client = MqttClient::new(&client_id);

            let opts = ConnectOptions::new(&client_id)
                .with_clean_start(true)
                .with_keep_alive(Duration::from_secs(30));
            let opts = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                opts.with_credentials(user.clone(), pass.clone())
            } else {
                opts
            };

            if let Err(e) = Box::pin(client.connect_with_options(&broker, opts)).await {
                eprintln!("Subscriber {sub_id} connect failed: {e}");
                return;
            }

            if use_wildcard && topic_count > 1 {
                let wildcard_topic = format!("{topic_base}/#");
                let metrics_clone = Arc::clone(&metrics);
                if let Err(e) = client
                    .subscribe(&wildcard_topic, move |_| {
                        metrics_clone
                            .messages_received
                            .fetch_add(1, Ordering::Relaxed);
                    })
                    .await
                {
                    eprintln!("Subscriber {sub_id} wildcard subscribe failed: {e}");
                    return;
                }
            } else if topic_count > 1 {
                for i in 0..topic_count {
                    let topic = format!("{topic_base}/{i}");
                    let metrics_clone = Arc::clone(&metrics);
                    if let Err(e) = client
                        .subscribe(&topic, move |_| {
                            metrics_clone
                                .messages_received
                                .fetch_add(1, Ordering::Relaxed);
                        })
                        .await
                    {
                        eprintln!("Subscriber {sub_id} subscribe to {topic} failed: {e}");
                        return;
                    }
                }
            } else {
                let metrics_clone = Arc::clone(&metrics);
                if let Err(e) = client
                    .subscribe(&topic_base, move |_| {
                        metrics_clone
                            .messages_received
                            .fetch_add(1, Ordering::Relaxed);
                    })
                    .await
                {
                    eprintln!("Subscriber {sub_id} subscribe failed: {e}");
                    return;
                }
            }

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let _ = client.disconnect().await;
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload: Arc<[u8]> = vec![0u8; args.size].into();
    let warmup_duration = Duration::from_secs(args.warmup);
    let measure_duration = Duration::from_secs(args.duration);

    for pub_id in 0..args.publishers {
        let broker = pub_broker.clone();
        let conn = args.conn.clone();
        let topic_base = topic_base.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);
        let payload = Arc::clone(&payload);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-pub-{pub_id}");
            let client = MqttClient::new(&client_id);

            let opts = ConnectOptions::new(&client_id)
                .with_clean_start(true)
                .with_keep_alive(Duration::from_secs(30));
            let opts = if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                opts.with_credentials(user.clone(), pass.clone())
            } else {
                opts
            };

            if let Err(e) = Box::pin(client.connect_with_options(&broker, opts)).await {
                eprintln!("Publisher {pub_id} connect failed: {e}");
                return;
            }

            let mut topic_idx: usize = 0;
            while running.load(Ordering::Relaxed) {
                let topic = if topic_count > 1 {
                    format!("{}/{}", topic_base, topic_idx % topic_count)
                } else {
                    topic_base.clone()
                };
                if client.publish(&topic, payload.to_vec()).await.is_ok() {
                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                }
                topic_idx = topic_idx.wrapping_add(1);
            }

            let _ = client.disconnect().await;
        });
        pub_handles.push(handle);
    }

    if args.warmup > 0 {
        println!("Warming up for {}s...", args.warmup);
        tokio::time::sleep(warmup_duration).await;
    }

    metrics.messages_sent.store(0, Ordering::SeqCst);
    metrics.messages_received.store(0, Ordering::SeqCst);

    println!("Measuring for {}s...", args.duration);
    let start_time = Instant::now();

    tokio::time::sleep(measure_duration).await;

    let elapsed = start_time.elapsed();
    running.store(false, Ordering::Release);

    tokio::time::sleep(Duration::from_millis(200)).await;

    for handle in pub_handles {
        handle.abort();
    }
    for handle in sub_handles {
        handle.abort();
    }

    let elapsed_secs = elapsed.as_secs_f64();

    let sent = metrics.messages_sent.load(Ordering::Relaxed);
    let received = metrics.messages_received.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);

    let p50 = metrics.calculate_percentile(50.0);
    let p95 = metrics.calculate_percentile(95.0);
    let p99 = metrics.calculate_percentile(99.0);

    let throughput = received as f64 / elapsed_secs;
    let bytes_received = received * args.size as u64;
    let bandwidth_mbps = (bytes_received as f64 * 8.0) / (elapsed_secs * 1_000_000.0);

    if let OutputFormat::Json = args.format {
        let result = json!({
            "messages_sent": sent,
            "messages_received": received,
            "errors": errors,
            "duration_secs": elapsed_secs,
            "throughput_msg_sec": throughput,
            "bandwidth_mbps": bandwidth_mbps,
            "latency_p50_us": p50 / 1000,
            "latency_p95_us": p95 / 1000,
            "latency_p99_us": p99 / 1000,
            "config": {
                "publishers": args.publishers,
                "subscribers": args.subscribers,
                "duration_secs": args.duration,
                "warmup_secs": args.warmup,
                "payload_size": args.size,
                "qos": args.qos
            }
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let p50_us = p50 / 1000;
        let p95_us = p95 / 1000;
        let p99_us = p99 / 1000;
        println!("\n┌─────────────────────────────────────────┐");
        println!("│           BENCHMARK RESULTS             │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Messages sent:     {sent:>19} │");
        println!("│ Messages received: {received:>19} │");
        println!("│ Errors:            {errors:>19} │");
        println!("│ Duration:          {elapsed_secs:>16.2} s │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Throughput:     {throughput:>16.0} msg/s │");
        println!("│ Bandwidth:      {bandwidth_mbps:>16.2} Mbps │");
        println!("├─────────────────────────────────────────┤");
        println!("│ Latency p50:      {p50_us:>17} µs │");
        println!("│ Latency p95:      {p95_us:>17} µs │");
        println!("│ Latency p99:      {p99_us:>17} µs │");
        println!("└─────────────────────────────────────────┘");
    }

    Ok(())
}

struct BenchDbArgs {
    operations: u64,
    entity: String,
    op: String,
    concurrency: usize,
    fields: usize,
    field_size: usize,
    warmup: u64,
    cleanup: bool,
    seed: u64,
    no_latency: bool,
    async_mode: bool,
    qos: u8,
    duration: Option<u64>,
    conn: ConnectionArgs,
    format: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbOp {
    Insert,
    Get,
    Update,
    Delete,
    List,
    Mixed,
}

impl DbOp {
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

#[derive(Default)]
struct DbBenchMetrics {
    inserts: std::sync::atomic::AtomicU64,
    gets: std::sync::atomic::AtomicU64,
    updates: std::sync::atomic::AtomicU64,
    deletes: std::sync::atomic::AtomicU64,
    lists: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    latencies_ns: std::sync::Mutex<Vec<u64>>,
}

impl DbBenchMetrics {
    fn record_latency(&self, ns: u64) {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            latencies.push(ns);
        }
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn calculate_percentile(&self, p: f64) -> u64 {
        if let Ok(mut latencies) = self.latencies_ns.lock() {
            if latencies.is_empty() {
                return 0;
            }
            latencies.sort_unstable();
            let idx = ((latencies.len() as f64) * p / 100.0) as usize;
            latencies[idx.min(latencies.len() - 1)]
        } else {
            0
        }
    }

    fn total_ops(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.inserts.load(Ordering::Relaxed)
            + self.gets.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
            + self.lists.load(Ordering::Relaxed)
    }
}

fn generate_record(fields: usize, field_size: usize, id: u64) -> Value {
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

const RAMP_INITIAL_RATE: u64 = 500;
const RAMP_INITIAL_STEP_PCT: f64 = 0.50;
const RAMP_MIN_STEP_PCT: f64 = 0.05;
const RAMP_INTERVAL_MS: u64 = 2000;
const RAMP_THROUGHPUT_THRESHOLD: f64 = 0.90;
const RAMP_LATENCY_SLOWDOWN_FACTOR: f64 = 1.5;
const OVERLOAD_STEPS: u32 = 2;
const STABILIZATION_SECS: u64 = 3;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
async fn cmd_bench_db_async(
    args: &BenchDbArgs,
    op: DbOp,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;

    if op == DbOp::Mixed {
        return Err("Async mode does not support mixed operations".into());
    }

    let record_size_approx = args.fields * (args.field_size + 20);
    let duration_secs = args.duration.unwrap_or(30);

    let qos_str = match args.qos {
        0 => "QoS 0",
        1 => "QoS 1",
        _ => "QoS 2",
    };
    println!(
        "DB Benchmark (ASYNC): {duration_secs}s duration, op={op:?}, ~{record_size_approx} bytes/record, {qos_str}"
    );

    let client_id = format!("bench-db-async-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(client_id.clone());
    if args.conn.insecure {
        client.set_insecure_tls(true).await;
    }
    client.connect(&args.conn.broker).await?;

    let seeded_ids: Arc<Vec<String>> = if matches!(op, DbOp::Get | DbOp::Update | DbOp::Delete) {
        let seed_count = args.seed.max(1000);
        println!("Seeding {seed_count} records for {op:?} benchmark...");
        let mut ids = Vec::with_capacity(seed_count as usize);
        for i in 0..seed_count {
            let record = generate_record(args.fields, args.field_size, i);
            let topic = format!("$DB/{}/create", args.entity);
            let response_topic = format!("{client_id}/resp/{i}");

            let (tx, rx) = flume::bounded::<Option<String>>(1);
            let tx_clone = tx.clone();
            client
                .subscribe(&response_topic, move |msg| {
                    let actual_id =
                        serde_json::from_slice::<Value>(&msg.payload)
                            .ok()
                            .and_then(|v| {
                                v.get("id")
                                    .and_then(|id| id.as_str().map(String::from))
                                    .or_else(|| {
                                        v.get("data")?.get("id")?.as_str().map(String::from)
                                    })
                            });
                    let _ = tx_clone.try_send(actual_id);
                })
                .await?;

            let opts = PublishOptions {
                properties: PublishProperties {
                    response_topic: Some(response_topic.clone()),
                    ..Default::default()
                },
                ..Default::default()
            };
            client
                .publish_with_options(&topic, serde_json::to_vec(&record).unwrap(), opts)
                .await?;

            if let Some(actual_id) = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
            {
                ids.push(actual_id);
            }
            let _ = client.unsubscribe(&response_topic).await;

            if (i + 1) % 500 == 0 {
                println!("  Seeded {} records...", i + 1);
            }
        }
        println!("Seeding complete. {} records ready.", ids.len());
        Arc::new(ids)
    } else {
        Arc::new(Vec::new())
    };

    let response_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let response_count_clone = Arc::clone(&response_count);
    let error_count_clone = Arc::clone(&error_count);

    let pending_times: Arc<std::sync::Mutex<HashMap<u64, Instant>>> =
        Arc::new(std::sync::Mutex::new(HashMap::with_capacity(10000)));
    let latencies: Arc<std::sync::Mutex<Vec<f64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(100_000)));
    let pending_times_resp = Arc::clone(&pending_times);
    let latencies_resp = Arc::clone(&latencies);

    let response_topic = format!("{client_id}/responses/#");
    client
        .subscribe(&response_topic, move |msg| {
            let corr_id = msg
                .topic
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u64>().ok());

            if let Some(id) = corr_id
                && let Ok(mut pending) = pending_times_resp.lock()
                && let Some(sent_at) = pending.remove(&id)
            {
                let latency_ms = sent_at.elapsed().as_secs_f64() * 1000.0;
                if let Ok(mut lats) = latencies_resp.lock() {
                    lats.push(latency_ms);
                }
            }

            if let Ok(v) = serde_json::from_slice::<Value>(&msg.payload) {
                if v.get("status").and_then(Value::as_str) == Some("ok") {
                    response_count_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    error_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                error_count_clone.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await?;

    println!("Ramping up from {RAMP_INITIAL_RATE} ops/s...");

    let stop_flag = Arc::new(AtomicBool::new(false));
    let published_count = Arc::new(AtomicU64::new(0));
    let target_rate = Arc::new(AtomicU64::new(RAMP_INITIAL_RATE));
    let delete_index = Arc::new(AtomicU64::new(0));

    let stop_clone = Arc::clone(&stop_flag);
    let published_clone = Arc::clone(&published_count);
    let target_rate_clone = Arc::clone(&target_rate);
    let delete_index_clone = Arc::clone(&delete_index);
    let pending_times_pub = Arc::clone(&pending_times);
    let client_clone = client.clone();
    let entity = args.entity.clone();
    let fields = args.fields;
    let field_size = args.field_size;
    let client_id_clone = client_id.clone();
    let qos = args.qos;
    let seeded_ids_clone = Arc::clone(&seeded_ids);

    let publisher_handle = tokio::spawn(async move {
        let mut i = 0u64;
        let mut interval_start = Instant::now();
        let mut ops_this_interval = 0u64;

        while !stop_clone.load(Ordering::Relaxed) {
            let current_rate = target_rate_clone.load(Ordering::Relaxed);
            let interval_elapsed = interval_start.elapsed().as_secs_f64();
            let expected_ops = (current_rate as f64 * interval_elapsed) as u64;

            if ops_this_interval < expected_ops {
                let (topic, payload) = match op {
                    DbOp::Insert => {
                        let record = generate_record(fields, field_size, i);
                        (
                            format!("$DB/{entity}/create"),
                            serde_json::to_vec(&record).unwrap(),
                        )
                    }
                    DbOp::Get => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let id = &seeded_ids_clone[i as usize % seeded_ids_clone.len()];
                        (format!("$DB/{entity}/{id}"), b"{}".to_vec())
                    }
                    DbOp::Update => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let id = &seeded_ids_clone[i as usize % seeded_ids_clone.len()];
                        let record = generate_record(fields, field_size, i);
                        (
                            format!("$DB/{entity}/{id}/update"),
                            serde_json::to_vec(&record).unwrap(),
                        )
                    }
                    DbOp::Delete => {
                        if seeded_ids_clone.is_empty() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        let idx = delete_index_clone.fetch_add(1, Ordering::Relaxed) as usize;
                        if idx >= seeded_ids_clone.len() {
                            break;
                        }
                        let id = &seeded_ids_clone[idx];
                        (format!("$DB/{entity}/{id}/delete"), b"{}".to_vec())
                    }
                    DbOp::List => (
                        format!("$DB/{entity}/list"),
                        serde_json::to_vec(&json!({"limit": 10})).unwrap(),
                    ),
                    DbOp::Mixed => unreachable!(),
                };

                let correlation_id = format!("{i}");
                let individual_response = format!("{client_id_clone}/responses/{correlation_id}");

                let qos_level = match qos {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    _ => QoS::ExactlyOnce,
                };
                let opts = PublishOptions {
                    qos: qos_level,
                    properties: PublishProperties {
                        response_topic: Some(individual_response),
                        correlation_data: Some(correlation_id.into_bytes()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                if let Ok(mut pending) = pending_times_pub.lock() {
                    pending.insert(i, Instant::now());
                }

                if client_clone
                    .publish_with_options(&topic, payload, opts)
                    .await
                    .is_ok()
                {
                    published_clone.fetch_add(1, Ordering::Relaxed);
                    ops_this_interval += 1;
                }
                i += 1;
            } else {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }

            if interval_elapsed >= 1.0 {
                interval_start = Instant::now();
                ops_this_interval = 0;
            }
        }
    });

    let start_time = Instant::now();
    let mut last_response_count = 0u64;
    let mut last_latency_count = 0usize;
    let mut current_rate = RAMP_INITIAL_RATE;
    let mut current_step_pct = RAMP_INITIAL_STEP_PCT;
    let mut saturation_rate: Option<f64> = None;
    let mut overload_remaining = 0u32;
    let mut peak_rate = 0u64;
    let mut baseline_latency: Option<f64> = None;
    let backlog_threshold_secs = 2.0;

    while start_time.elapsed().as_secs() < duration_secs {
        tokio::time::sleep(Duration::from_millis(RAMP_INTERVAL_MS)).await;

        let current_published = published_count.load(Ordering::Relaxed);
        let current_responses = response_count.load(Ordering::Relaxed);
        let backlog = current_published.saturating_sub(current_responses);
        let interval_responses = current_responses.saturating_sub(last_response_count);
        let interval_throughput = interval_responses as f64 / (RAMP_INTERVAL_MS as f64 / 1000.0);
        let backlog_limit = (current_rate as f64 * backlog_threshold_secs) as u64;

        let (interval_p50, interval_p95) = if let Ok(lats) = latencies.lock() {
            let new_count = lats.len();
            if new_count > last_latency_count {
                let mut interval_lats: Vec<f64> = lats[last_latency_count..new_count].to_vec();
                interval_lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let p50_idx = interval_lats.len() / 2;
                let p95_idx = (interval_lats.len() as f64 * 0.95) as usize;
                let p50 = interval_lats.get(p50_idx).copied().unwrap_or(0.0);
                let p95 = interval_lats
                    .get(p95_idx.min(interval_lats.len().saturating_sub(1)))
                    .copied()
                    .unwrap_or(0.0);
                last_latency_count = new_count;
                (p50, p95)
            } else {
                (0.0, 0.0)
            }
        } else {
            (0.0, 0.0)
        };

        if baseline_latency.is_none() && interval_p50 > 0.0 {
            baseline_latency = Some(interval_p50);
        }

        let phase = if saturation_rate.is_none() {
            "ramp"
        } else if overload_remaining > 0 {
            "overload"
        } else {
            "stable"
        };
        println!(
            "  [{phase}] Rate: {current_rate} target, {interval_throughput:.0} actual, backlog: {backlog}, p50: {interval_p50:.1}ms, p95: {interval_p95:.1}ms"
        );

        if saturation_rate.is_none() {
            let throughput_ok =
                interval_throughput >= current_rate as f64 * RAMP_THROUGHPUT_THRESHOLD;
            let backlog_ok = backlog < backlog_limit;
            let latency_ok = baseline_latency
                .is_none_or(|base| interval_p50 < base * RAMP_LATENCY_SLOWDOWN_FACTOR);

            if throughput_ok && backlog_ok && latency_ok {
                let step = (current_rate as f64 * current_step_pct) as u64;
                let step = step.max(50);
                current_rate += step;
                target_rate.store(current_rate, Ordering::Relaxed);
            } else if !latency_ok && throughput_ok && backlog_ok {
                current_step_pct = (current_step_pct / 2.0).max(RAMP_MIN_STEP_PCT);
                let step = (current_rate as f64 * current_step_pct) as u64;
                let step = step.max(50);
                current_rate += step;
                target_rate.store(current_rate, Ordering::Relaxed);
                println!(
                    "  Latency increasing, reducing step to {:.0}%",
                    current_step_pct * 100.0
                );
            } else {
                let reason = if throughput_ok {
                    "backlog growth"
                } else {
                    "throughput drop"
                };
                saturation_rate = Some(interval_throughput);
                println!("Saturation detected ({reason}) at {interval_throughput:.0} ops/s");
                overload_remaining = OVERLOAD_STEPS;
                let step = (current_rate as f64 * RAMP_MIN_STEP_PCT) as u64;
                current_rate += step.max(50);
                target_rate.store(current_rate, Ordering::Relaxed);
                println!("Entering overload phase at {current_rate} ops/s...");
            }
        } else if overload_remaining > 0 {
            overload_remaining -= 1;
            if overload_remaining > 0 {
                let step = (current_rate as f64 * RAMP_MIN_STEP_PCT) as u64;
                current_rate += step.max(50);
                target_rate.store(current_rate, Ordering::Relaxed);
            } else {
                peak_rate = current_rate;
                let stable_rate = saturation_rate.unwrap_or(interval_throughput) as u64;
                println!(
                    "Overload complete (peak {peak_rate}), stabilizing at {stable_rate} ops/s..."
                );
                target_rate.store(stable_rate, Ordering::Relaxed);
            }
        }

        last_response_count = current_responses;

        if saturation_rate.is_some() && overload_remaining == 0 {
            tokio::time::sleep(Duration::from_secs(STABILIZATION_SECS)).await;
            break;
        }
    }

    stop_flag.store(true, Ordering::Relaxed);
    let _ = publisher_handle.await;

    let published = published_count.load(Ordering::Relaxed);
    println!("Published {published} operations, waiting for responses...");

    let wait_timeout = Duration::from_secs(10);
    let wait_start = Instant::now();
    loop {
        let received = response_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);
        let total = received + errors;

        if total >= published {
            break;
        }

        if wait_start.elapsed() > wait_timeout {
            println!("Timeout: {received}/{published} responses received");
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let total_elapsed = start_time.elapsed();
    let received = response_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    let _ = client.unsubscribe(&response_topic).await;
    let _ = client.disconnect().await;

    let throughput = received as f64 / total_elapsed.as_secs_f64();
    let saturation_display = saturation_rate.unwrap_or(throughput);
    let duration = total_elapsed.as_secs_f64();

    let (lat_min, lat_p50, lat_p95, lat_p99, lat_max) = if let Ok(mut lats) = latencies.lock() {
        if lats.is_empty() {
            (0.0, 0.0, 0.0, 0.0, 0.0)
        } else {
            lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let len = lats.len();
            let p50 = lats[len / 2];
            let p95 = lats[(len as f64 * 0.95) as usize];
            let p99 = lats[(len as f64 * 0.99) as usize];
            (lats[0], p50, p95, p99, lats[len - 1])
        }
    } else {
        (0.0, 0.0, 0.0, 0.0, 0.0)
    };

    println!("\n┌───────────────────────────────────────────┐");
    println!("│           DB Benchmark Results            │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Mode:                              ASYNC  │");
    println!("│ Duration:              {duration:>10.2} s      │");
    println!("│ Published:             {published:>10}         │");
    println!("│ Successful:            {received:>10}         │");
    println!("│ Errors:                {errors:>10}         │");
    println!("│ Saturation Point:      {saturation_display:>10.0} ops/s  │");
    if peak_rate > 0 {
        println!("│ Peak Rate Tested:      {peak_rate:>10} ops/s  │");
    }
    println!("│ Throughput:            {throughput:>10.0} ops/s  │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Latency (ms):                             │");
    println!("│   Min:                 {lat_min:>10.2} ms      │");
    println!("│   p50:                 {lat_p50:>10.2} ms      │");
    println!("│   p95:                 {lat_p95:>10.2} ms      │");
    println!("│   p99:                 {lat_p99:>10.2} ms      │");
    println!("│   Max:                 {lat_max:>10.2} ms      │");
    println!("└───────────────────────────────────────────┘");

    Ok(())
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
async fn cmd_bench_db(args: BenchDbArgs) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    wait_for_broker_ready(&args.conn, 30).await?;

    let op = DbOp::from_str(&args.op).ok_or_else(|| {
        format!(
            "Invalid operation '{}'. Use: insert, get, update, delete, list, mixed",
            args.op
        )
    })?;

    if args.async_mode {
        return cmd_bench_db_async(&args, op).await;
    }

    let record_size_approx = args.fields * (args.field_size + 20);
    let latency_mode = if args.no_latency {
        " (throughput only)"
    } else {
        ""
    };
    println!(
        "DB Benchmark: {} ops, {} concurrent, op={:?}, ~{} bytes/record{}",
        args.operations, args.concurrency, op, record_size_approx, latency_mode
    );

    let metrics = Arc::new(DbBenchMetrics::default());
    let inserted_ids: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let id_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    if args.seed > 0 {
        println!("Seeding {} records...", args.seed);
        let client = MqttClient::new("bench-db-seeder".to_string());
        client.connect(&args.conn.broker).await?;

        for i in 0..args.seed {
            let id = id_counter.fetch_add(1, Ordering::Relaxed);
            let record = generate_record(args.fields, args.field_size, id);
            let topic = format!("$DB/{}/create", args.entity);
            let response_topic = format!("bench-db-seeder/resp/{}", uuid::Uuid::new_v4());

            let (tx, rx) = flume::bounded::<Option<String>>(1);
            let tx_clone = tx.clone();
            client
                .subscribe(&response_topic, move |msg| {
                    let actual_id =
                        serde_json::from_slice::<Value>(&msg.payload)
                            .ok()
                            .and_then(|v| {
                                v.get("id")
                                    .and_then(|id| id.as_str().map(String::from))
                                    .or_else(|| {
                                        v.get("data")?.get("id")?.as_str().map(String::from)
                                    })
                            });
                    let _ = tx_clone.try_send(actual_id);
                })
                .await?;

            let opts = PublishOptions {
                properties: PublishProperties {
                    response_topic: Some(response_topic.clone()),
                    ..Default::default()
                },
                ..Default::default()
            };
            client
                .publish_with_options(&topic, serde_json::to_vec(&record).unwrap(), opts)
                .await?;

            if let Some(actual_id) = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                && let Ok(mut ids) = inserted_ids.lock()
            {
                ids.push(actual_id);
            }
            let _ = client.unsubscribe(&response_topic).await;

            if (i + 1) % 1000 == 0 {
                println!("  Seeded {} records...", i + 1);
            }
        }

        let _ = client.disconnect().await;
        println!("Seeding complete. {} records ready.", args.seed);
    }

    let ops_per_client = args.operations / args.concurrency.max(1) as u64;
    let warmup_per_client = args.warmup / args.concurrency.max(1) as u64;

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for client_id in 0..args.concurrency {
        let conn = args.conn.clone();
        let entity = args.entity.clone();
        let metrics = Arc::clone(&metrics);
        let inserted_ids = Arc::clone(&inserted_ids);
        let id_counter = Arc::clone(&id_counter);
        let fields = args.fields;
        let field_size = args.field_size;
        let no_latency = args.no_latency;

        let handle = tokio::spawn(async move {
            let client_name = format!("bench-db-{client_id}");
            let client = MqttClient::new(client_name.clone());

            if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                let opts =
                    ConnectOptions::new(client_name).with_credentials(user.clone(), pass.clone());
                if let Err(e) = Box::pin(client.connect_with_options(&conn.broker, opts)).await {
                    eprintln!("Client {client_id} connect failed: {e}");
                    return;
                }
            } else if let Err(e) = client.connect(&conn.broker).await {
                eprintln!("Client {client_id} connect failed: {e}");
                return;
            }

            for i in 0..(warmup_per_client + ops_per_client) {
                let is_warmup = i < warmup_per_client;
                let current_op = if op == DbOp::Mixed {
                    match i % 5 {
                        1 => DbOp::Get,
                        2 => DbOp::Update,
                        3 => DbOp::List,
                        4 => DbOp::Delete,
                        _ => DbOp::Insert,
                    }
                } else {
                    op
                };

                let op_start = if no_latency {
                    None
                } else {
                    Some(Instant::now())
                };
                let success = match current_op {
                    DbOp::Insert => {
                        let id = id_counter.fetch_add(1, Ordering::Relaxed);
                        let record = generate_record(fields, field_size, id);
                        let topic = format!("$DB/{entity}/create");
                        let response_topic =
                            format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                        let (tx, rx) = flume::bounded::<Option<String>>(1);
                        let tx_clone = tx.clone();
                        if client
                            .subscribe(&response_topic, move |msg| {
                                let actual_id = serde_json::from_slice::<Value>(&msg.payload)
                                    .ok()
                                    .and_then(|v| {
                                        v.get("id")
                                            .and_then(|id| id.as_str().map(String::from))
                                            .or_else(|| {
                                                v.get("data")?.get("id")?.as_str().map(String::from)
                                            })
                                    });
                                let _ = tx_clone.try_send(actual_id);
                            })
                            .await
                            .is_err()
                        {
                            false
                        } else {
                            let opts = PublishOptions {
                                properties: PublishProperties {
                                    response_topic: Some(response_topic.clone()),
                                    ..Default::default()
                                },
                                ..Default::default()
                            };
                            if client
                                .publish_with_options(
                                    &topic,
                                    serde_json::to_vec(&record).unwrap(),
                                    opts,
                                )
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let actual_id =
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                                        .await
                                        .ok()
                                        .and_then(Result::ok)
                                        .flatten();
                                let success = actual_id.is_some();
                                if let Some(actual_id) = actual_id
                                    && let Ok(mut ids) = inserted_ids.lock()
                                {
                                    ids.push(actual_id);
                                }
                                let _ = client.unsubscribe(&response_topic).await;
                                success
                            }
                        }
                    }
                    DbOp::Get => {
                        let id = {
                            let ids = inserted_ids.lock().ok();
                            ids.and_then(|ids| {
                                if ids.is_empty() {
                                    None
                                } else {
                                    Some(ids[i as usize % ids.len()].clone())
                                }
                            })
                        };
                        if let Some(id) = id {
                            let topic = format!("$DB/{entity}/{id}");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(&topic, b"{}".to_vec(), opts)
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::Update => {
                        let id = {
                            let ids = inserted_ids.lock().ok();
                            ids.and_then(|ids| {
                                if ids.is_empty() {
                                    None
                                } else {
                                    Some(ids[i as usize % ids.len()].clone())
                                }
                            })
                        };
                        if let Some(id) = id {
                            let record = generate_record(fields, field_size, i);
                            let topic = format!("$DB/{entity}/{id}/update");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(
                                        &topic,
                                        serde_json::to_vec(&record).unwrap(),
                                        opts,
                                    )
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::Delete => {
                        let id = {
                            let mut ids = inserted_ids.lock().ok();
                            ids.as_mut().and_then(|ids| ids.pop())
                        };
                        if let Some(id) = id {
                            let topic = format!("$DB/{entity}/{id}/delete");
                            let response_topic =
                                format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                            let (tx, rx) = flume::bounded::<bool>(1);
                            let tx_clone = tx.clone();
                            if client
                                .subscribe(&response_topic, move |_msg| {
                                    let _ = tx_clone.try_send(true);
                                })
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let opts = PublishOptions {
                                    properties: PublishProperties {
                                        response_topic: Some(response_topic.clone()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                };
                                if client
                                    .publish_with_options(&topic, b"{}".to_vec(), opts)
                                    .await
                                    .is_err()
                                {
                                    false
                                } else {
                                    let result = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        rx.recv_async(),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .unwrap_or(false);
                                    let _ = client.unsubscribe(&response_topic).await;
                                    result
                                }
                            }
                        } else {
                            true
                        }
                    }
                    DbOp::List => {
                        let topic = format!("$DB/{entity}/list");
                        let response_topic =
                            format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                        let (tx, rx) = flume::bounded::<bool>(1);
                        let tx_clone = tx.clone();
                        if client
                            .subscribe(&response_topic, move |_msg| {
                                let _ = tx_clone.try_send(true);
                            })
                            .await
                            .is_err()
                        {
                            false
                        } else {
                            let opts = PublishOptions {
                                properties: PublishProperties {
                                    response_topic: Some(response_topic.clone()),
                                    ..Default::default()
                                },
                                ..Default::default()
                            };
                            let payload = json!({"limit": 10});
                            if client
                                .publish_with_options(
                                    &topic,
                                    serde_json::to_vec(&payload).unwrap(),
                                    opts,
                                )
                                .await
                                .is_err()
                            {
                                false
                            } else {
                                let result =
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                                        .await
                                        .ok()
                                        .and_then(Result::ok)
                                        .unwrap_or(false);
                                let _ = client.unsubscribe(&response_topic).await;
                                result
                            }
                        }
                    }
                    DbOp::Mixed => unreachable!(),
                };

                if !is_warmup {
                    if success {
                        if let Some(start) = op_start {
                            let latency_ns = start.elapsed().as_nanos() as u64;
                            metrics.record_latency(latency_ns);
                        }
                        match current_op {
                            DbOp::Insert => metrics.inserts.fetch_add(1, Ordering::Relaxed),
                            DbOp::Get => metrics.gets.fetch_add(1, Ordering::Relaxed),
                            DbOp::Update => metrics.updates.fetch_add(1, Ordering::Relaxed),
                            DbOp::Delete => metrics.deletes.fetch_add(1, Ordering::Relaxed),
                            DbOp::List => metrics.lists.fetch_add(1, Ordering::Relaxed),
                            DbOp::Mixed => 0,
                        };
                    } else {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            let _ = client.disconnect().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    let total = metrics.total_ops();
    let inserts = metrics.inserts.load(Ordering::Relaxed);
    let gets = metrics.gets.load(Ordering::Relaxed);
    let updates = metrics.updates.load(Ordering::Relaxed);
    let deletes = metrics.deletes.load(Ordering::Relaxed);
    let lists = metrics.lists.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);

    let p50 = metrics.calculate_percentile(50.0);
    let p95 = metrics.calculate_percentile(95.0);
    let p99 = metrics.calculate_percentile(99.0);

    let throughput = total as f64 / elapsed_secs;

    if args.cleanup {
        println!("Cleaning up test entity...");
        let ids_to_cleanup: Vec<String> = inserted_ids
            .lock()
            .map(|ids| ids.clone())
            .unwrap_or_default();
        if !ids_to_cleanup.is_empty() {
            let client = MqttClient::new("bench-cleanup");
            if client.connect(&args.conn.broker).await.is_ok() {
                for id in &ids_to_cleanup {
                    let topic = format!("$DB/{}/{id}/delete", args.entity);
                    let _ = client.publish(&topic, b"{}".to_vec()).await;
                }
                let _ = client.disconnect().await;
            }
        }
    }

    if let OutputFormat::Json = args.format {
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
            "latency_p50_us": p50 / 1000,
            "latency_p95_us": p95 / 1000,
            "latency_p99_us": p99 / 1000,
            "config": {
                "operations": args.operations,
                "concurrency": args.concurrency,
                "op": args.op,
                "fields": args.fields,
                "field_size": args.field_size,
                "record_size_approx": record_size_approx
            }
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let p50_us = p50 / 1000;
        let p95_us = p95 / 1000;
        let p99_us = p99 / 1000;
        println!("\n┌───────────────────────────────────────────┐");
        println!("│           DB BENCHMARK RESULTS            │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Total operations:   {total:>20} │");
        println!("│   Inserts:          {inserts:>20} │");
        println!("│   Gets:             {gets:>20} │");
        println!("│   Updates:          {updates:>20} │");
        println!("│   Deletes:          {deletes:>20} │");
        println!("│   Lists:            {lists:>20} │");
        println!("│ Errors:             {errors:>20} │");
        println!("│ Duration:           {elapsed_secs:>17.2} s │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Throughput:       {throughput:>16.0} ops/s │");
        println!("├───────────────────────────────────────────┤");
        println!("│ Latency p50:        {p50_us:>17} µs │");
        println!("│ Latency p95:        {p95_us:>17} µs │");
        println!("│ Latency p99:        {p99_us:>17} µs │");
        println!("└───────────────────────────────────────────┘");
    }

    Ok(())
}

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

async fn cmd_dev_bench(
    scenario: DevBenchScenario,
    output: Option<PathBuf>,
    baseline: Option<PathBuf>,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let conn = ConnectionArgs {
        broker: "127.0.0.1:1883".to_string(),
        user: None,
        pass: None,
        timeout: 30,
        insecure: false,
    };

    if !is_agent_running() {
        println!("Starting agent for benchmark...");
        start_agent_for_bench(&exe, db)?;
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

fn is_agent_running() -> bool {
    Command::new("pgrep")
        .args(["-f", "mqdb agent"])
        .output()
        .is_ok_and(|o| !o.stdout.is_empty())
}

fn start_agent_for_bench(
    exe: &std::path::Path,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(db)?;
    let log_file = std::fs::File::create(format!("{db}/mqdb.log"))?;

    #[cfg(feature = "dev-insecure")]
    let args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    Command::new(exe)
        .args(&args)
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()?;

    Ok(())
}

#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn run_pubsub_benchmark(
    conn: &ConnectionArgs,
    publishers: usize,
    subscribers: usize,
    duration: u64,
    size: usize,
    _qos: u8,
) -> Result<DevBenchResult, Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let metrics = Arc::new(BenchMetrics::default());
    let running = Arc::new(AtomicBool::new(true));

    println!(
        "\nRunning pub/sub benchmark: {publishers} pubs, {subscribers} subs, {duration}s, {size} bytes"
    );

    let mut sub_handles = Vec::new();
    for sub_id in 0..subscribers {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-sub-{sub_id}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            let metrics_clone = Arc::clone(&metrics);
            let _ = client
                .subscribe("bench/test", move |msg| {
                    let payload = &msg.payload;
                    if payload.len() >= 8
                        && let Ok(bytes) = payload[0..8].try_into()
                    {
                        let sent_ns = u64::from_be_bytes(bytes);
                        let recv_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64;
                        if recv_time > sent_ns {
                            metrics_clone.record_latency(recv_time - sent_ns);
                        }
                    }
                    metrics_clone
                        .messages_received
                        .fetch_add(1, Ordering::Relaxed);
                })
                .await;

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = client.disconnect().await;
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let start_time = Instant::now();
    let measure_duration = Duration::from_secs(duration);

    let mut pub_handles = Vec::new();
    for pub_id in 0..publishers {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-pub-{pub_id}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            while running.load(Ordering::Relaxed) {
                let mut payload = vec![0u8; size];
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                payload[0..8].copy_from_slice(&now.to_be_bytes());

                let _ = client.publish("bench/test", payload).await;
                metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
            }

            let _ = client.disconnect().await;
        });
        pub_handles.push(handle);
    }

    tokio::time::sleep(measure_duration).await;

    running.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;

    for handle in pub_handles {
        handle.abort();
    }
    for handle in sub_handles {
        handle.abort();
    }

    let elapsed = start_time.elapsed();
    let sent = metrics.messages_sent.load(Ordering::Relaxed);
    let throughput = sent as f64 / elapsed.as_secs_f64();
    let p50 = metrics.calculate_percentile(50.0) / 1000;
    let p95 = metrics.calculate_percentile(95.0) / 1000;
    let p99 = metrics.calculate_percentile(99.0) / 1000;

    Ok(DevBenchResult {
        scenario: "pubsub".to_string(),
        timestamp: chrono_timestamp(),
        throughput,
        latency_p50_us: p50,
        latency_p95_us: p95,
        latency_p99_us: p99,
        config: serde_json::json!({
            "publishers": publishers,
            "subscribers": subscribers,
            "duration_secs": duration,
            "size": size
        }),
    })
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
async fn run_db_benchmark(
    conn: &ConnectionArgs,
    operations: u64,
    concurrency: usize,
    op: &str,
) -> Result<DevBenchResult, Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    println!("\nRunning DB benchmark: {operations} ops, {concurrency} concurrency, op={op}");

    let metrics = Arc::new(BenchMetrics::default());
    let ops_per_client = operations / concurrency.max(1) as u64;

    let start_time = Instant::now();

    let mut handles = Vec::new();
    for client_id_num in 0..concurrency {
        let conn = conn.clone();
        let metrics = Arc::clone(&metrics);
        let op = op.to_string();

        let handle = tokio::spawn(async move {
            let client_id = format!("dev-bench-db-{client_id_num}");
            let client = MqttClient::new(&client_id);
            if client.connect(&conn.broker).await.is_err() {
                return;
            }

            for i in 0..ops_per_client {
                let op_start = Instant::now();
                let entity = "dev_bench_entity";
                let id = format!("{client_id_num}-{i}");

                let result = match op.as_str() {
                    "insert" | "mixed" => {
                        let data = serde_json::json!({"field": "value", "num": i});
                        db_create(&client, entity, &data).await
                    }
                    "get" => db_read(&client, entity, &id).await,
                    _ => Ok(serde_json::Value::Null),
                };

                if result.is_ok() {
                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                    metrics.record_latency(op_start.elapsed().as_nanos() as u64);
                } else {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
            }

            let _ = client.disconnect().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    let completed = metrics.messages_sent.load(Ordering::Relaxed);
    let throughput = completed as f64 / elapsed.as_secs_f64();
    let p50 = metrics.calculate_percentile(50.0) / 1000;
    let p95 = metrics.calculate_percentile(95.0) / 1000;
    let p99 = metrics.calculate_percentile(99.0) / 1000;

    Ok(DevBenchResult {
        scenario: "db".to_string(),
        timestamp: chrono_timestamp(),
        throughput,
        latency_p50_us: p50,
        latency_p95_us: p95,
        latency_p99_us: p99,
        config: serde_json::json!({
            "operations": operations,
            "concurrency": concurrency,
            "op": op
        }),
    })
}

async fn db_create(
    client: &MqttClient,
    entity: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let topic = format!("$DB/{entity}");
    let payload = serde_json::to_vec(data)?;
    client.publish_with_options(&topic, payload, opts).await?;

    match tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await {
        Ok(Ok(payload)) => Ok(serde_json::from_slice(&payload)?),
        _ => Err("timeout".into()),
    }
}

async fn db_read(
    client: &MqttClient,
    entity: &str,
    id: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let topic = format!("$DB/{entity}/{id}");
    client
        .publish_with_options(&topic, Vec::new(), opts)
        .await?;

    match tokio::time::timeout(Duration::from_secs(5), rx.recv_async()).await {
        Ok(Ok(payload)) => Ok(serde_json::from_slice(&payload)?),
        _ => Err("timeout".into()),
    }
}

fn chrono_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{now}")
}

fn print_bench_result(result: &DevBenchResult) {
    println!("\n┌───────────────────────────────────────────┐");
    println!(
        "│        DEV BENCH RESULTS ({:>6})        │",
        result.scenario.to_uppercase()
    );
    println!("├───────────────────────────────────────────┤");
    println!("│ Throughput:       {:>16.0} ops/s │", result.throughput);
    println!("├───────────────────────────────────────────┤");
    println!("│ Latency p50:        {:>17} µs │", result.latency_p50_us);
    println!("│ Latency p95:        {:>17} µs │", result.latency_p95_us);
    println!("│ Latency p99:        {:>17} µs │", result.latency_p99_us);
    println!("└───────────────────────────────────────────┘");
}

#[allow(clippy::cast_possible_wrap)]
fn compare_with_baseline(
    current: &DevBenchResult,
    baseline_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline_json = std::fs::read_to_string(baseline_path)?;
    let baseline: DevBenchResult = serde_json::from_str(&baseline_json)?;

    let throughput_delta = (current.throughput - baseline.throughput) / baseline.throughput * 100.0;
    let p50_delta = current.latency_p50_us as i64 - baseline.latency_p50_us as i64;
    let p95_delta = current.latency_p95_us as i64 - baseline.latency_p95_us as i64;
    let p99_delta = current.latency_p99_us as i64 - baseline.latency_p99_us as i64;

    println!("\n┌───────────────────────────────────────────┐");
    println!("│           COMPARISON TO BASELINE          │");
    println!("├───────────────────────────────────────────┤");
    println!("│ Throughput:          {throughput_delta:>+16.1}%  │");
    println!("│ Latency p50:         {p50_delta:>+16} µs │");
    println!("│ Latency p95:         {p95_delta:>+16} µs │");
    println!("│ Latency p99:         {p99_delta:>+16} µs │");
    println!("└───────────────────────────────────────────┘");

    Ok(())
}

fn build_bench_args(scenario: &DevBenchScenario) -> Vec<String> {
    match scenario {
        DevBenchScenario::Pubsub {
            publishers,
            subscribers,
            duration,
            size,
            qos,
        } => vec![
            "bench".to_string(),
            "pubsub".to_string(),
            "--publishers".to_string(),
            publishers.to_string(),
            "--subscribers".to_string(),
            subscribers.to_string(),
            "--duration".to_string(),
            duration.to_string(),
            "--size".to_string(),
            size.to_string(),
            "--qos".to_string(),
            qos.to_string(),
        ],
        DevBenchScenario::Db {
            operations,
            concurrency,
            op,
        } => vec![
            "bench".to_string(),
            "db".to_string(),
            "--operations".to_string(),
            operations.to_string(),
            "--concurrency".to_string(),
            concurrency.to_string(),
            "--op".to_string(),
            op.clone(),
        ],
    }
}

fn profile_with_samply(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");
    let symbol_dir = cwd.join("target/profiling");
    println!("Starting agent under samply profiler...");

    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut samply = Command::new("samply")
        .args([
            "record",
            "--save-only",
            "--unstable-presymbolicate",
            "--symbol-dir",
            symbol_dir.to_str().unwrap_or("."),
            "-o",
            output_path.to_str().unwrap_or("profile"),
            "--",
        ])
        .arg(&profiling_exe)
        .args(&agent_args)
        .spawn()?;

    std::thread::sleep(Duration::from_secs(3));

    println!("\nRunning benchmark...");
    let bench_status = Command::new(&profiling_exe).args(bench_args).status()?;
    if !bench_status.success() {
        println!("Benchmark failed with status: {bench_status}");
    }

    println!("\nStopping agent to save profile...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match samply.wait() {
        Ok(status) => println!("Samply exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for samply: {e}"),
    }

    println!("\nProfile saved: {}", output_path.display());
    println!("Load with: samply load {}", output_path.display());
    Ok(())
}

fn profile_with_flamegraph(
    exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    println!("Starting agent under flamegraph profiler...\n");

    let output_str = output_path.to_str().unwrap_or("flamegraph.svg");
    #[cfg(feature = "dev-insecure")]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let flamegraph_args = vec![
        "flamegraph",
        "--profile=profiling",
        "-o",
        output_str,
        "--",
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
    ];

    let mut flamegraph = Command::new("cargo").args(&flamegraph_args).spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    println!("Running benchmark...");
    let _ = Command::new(exe).args(bench_args).status();

    println!("\nStopping agent to save flamegraph...");
    let _ = Command::new("pkill")
        .args(["-INT", "-f", "mqdb agent start"])
        .status();

    match flamegraph.wait() {
        Ok(status) => println!("Flamegraph exited with: {status}"),
        Err(e) => println!("Warning: Failed to wait for flamegraph: {e}"),
    }

    println!("\nFlamegraph saved: {}", output_path.display());
    Ok(())
}

fn profile_with_sample(
    _exe: &std::path::Path,
    db: &str,
    output_path: &std::path::Path,
    bench_args: &[String],
    duration: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nBuilding with profiling profile (full DWARF symbols)...");
    let build_status = Command::new("cargo")
        .args(["build", "--profile=profiling"])
        .status()?;
    if !build_status.success() {
        return Err("Failed to build with profiling profile".into());
    }

    let cwd = std::env::current_dir()?;
    let profiling_exe = cwd.join("target/profiling/mqdb");

    println!("Starting agent...");
    #[cfg(feature = "dev-insecure")]
    let agent_args = vec![
        "agent",
        "start",
        "--db",
        db,
        "--bind",
        "127.0.0.1:1883",
        "--anonymous",
    ];
    #[cfg(not(feature = "dev-insecure"))]
    let agent_args = vec!["agent", "start", "--db", db, "--bind", "127.0.0.1:1883"];

    let mut agent = Command::new(&profiling_exe)
        .args(&agent_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_secs(2));

    let agent_pid = agent.id();
    println!("Agent started with PID: {agent_pid}");

    println!("\nRunning benchmark in background...");
    let mut bench = Command::new(&profiling_exe)
        .args(bench_args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::sleep(Duration::from_millis(500));

    println!("Sampling for {duration} seconds...");
    let sample_output = output_path.with_extension("txt");
    let sample_status = Command::new("sample")
        .args([
            &agent_pid.to_string(),
            &duration.to_string(),
            "-f",
            sample_output.to_str().unwrap_or("profile.txt"),
        ])
        .status()?;

    if !sample_status.success() {
        println!("Warning: sample command failed with status: {sample_status}");
    }

    println!("\nStopping benchmark and agent...");
    let _ = bench.kill();
    let _ = agent.kill();
    let _ = bench.wait();
    let _ = agent.wait();

    println!("\nProfile saved: {}", sample_output.display());

    if sample_output.exists() {
        println!("\nGenerating analysis report...\n");
        analyze_sample_output(&sample_output)?;
    }

    Ok(())
}

mod profile_analysis {
    use std::collections::HashMap;

    type CategoryFilter = fn(&str) -> bool;

    fn is_waiting(f: &str) -> bool {
        f.contains("__psynch_cvwait") || f.contains("__psynch_mutexwait")
    }
    fn is_io_wait(f: &str) -> bool {
        f.contains("kevent")
    }
    fn is_network_io(f: &str) -> bool {
        f.contains("__sendto") || f.contains("__recvfrom")
    }
    fn is_mqdb(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqdb::")
    }
    fn is_mqtt5(f: &str) -> bool {
        f.contains("[mqdb]") && f.contains("mqtt5::")
    }
    fn is_allocation(f: &str) -> bool {
        f.contains("malloc") || f.contains("free") || f.contains("_xzm_")
    }
    fn is_sync(f: &str) -> bool {
        f.contains("pthread_mutex") || f.contains("pthread_cond") || f.contains("parking_lot::")
    }
    fn is_channel(f: &str) -> bool {
        f.contains("flume::")
    }

    const CATEGORIES: &[(&str, CategoryFilter)] = &[
        ("WAITING", is_waiting),
        ("IO_WAIT", is_io_wait),
        ("NETWORK_IO", is_network_io),
        ("MQDB", is_mqdb),
        ("MQTT5", is_mqtt5),
        ("ALLOCATION", is_allocation),
        ("SYNC", is_sync),
        ("CHANNEL", is_channel),
    ];

    fn pct(num: u64, denom: u64) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let result = (num as f64 / denom as f64) * 100.0;
        result
    }

    fn compute_exclusive(call_graph: &[(usize, u64, String)]) -> HashMap<String, u64> {
        let mut exclusive: HashMap<String, u64> = HashMap::new();
        for (i, (depth, count, key)) in call_graph.iter().enumerate() {
            let is_leaf = call_graph
                .iter()
                .skip(i + 1)
                .take_while(|(d, _, _)| *d > *depth)
                .next()
                .is_none();
            if is_leaf {
                *exclusive.entry(key.clone()).or_insert(0) += count;
            }
        }
        exclusive
    }

    fn print_category_breakdown(exclusive: &HashMap<String, u64>, total_excl: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("EXCLUSIVE TIME BY CATEGORY");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        for (cat_name, filter) in CATEGORIES {
            let cat_total: u64 = exclusive
                .iter()
                .filter(|(k, _)| filter(k))
                .map(|(_, v)| v)
                .sum();
            if cat_total > 0 {
                println!(
                    "{cat_name:15} {cat_total:8} samples ({:5.1}%)",
                    pct(cat_total, total_excl)
                );
            }
        }
    }

    fn print_top_functions(inclusive: &HashMap<String, u64>, total_samples: u64) {
        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("TOP MQTT5/MQDB FUNCTIONS (by inclusive time)");
        println!(
            "--------------------------------------------------------------------------------\n"
        );

        let mut mqtt_funcs: Vec<_> = inclusive
            .iter()
            .filter(|(k, v)| {
                (k.contains("mqtt5::") || k.contains("mqdb::"))
                    && !k.to_lowercase().contains("main")
                    && **v > total_samples / 100
            })
            .collect();
        mqtt_funcs.sort_by(|a, b| b.1.cmp(a.1));

        for (func, count) in mqtt_funcs.iter().take(12) {
            let short = if func.len() > 70 { &func[..70] } else { func };
            println!("{count:6} ({:5.2}%) {short}", pct(**count, total_samples));
        }
    }

    pub fn analyze(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::BufRead;

        let file = std::fs::File::open(filepath)?;
        let reader = std::io::BufReader::new(file);

        let mut inclusive: HashMap<String, u64> = HashMap::new();
        let mut thread_totals: Vec<u64> = Vec::new();
        let mut call_graph_lines: Vec<(usize, u64, String)> = Vec::new();
        let mut in_call_graph = false;

        let thread_re = regex::Regex::new(r"^\s*(\d+)\s+Thread_\d+")?;
        let call_re = regex::Regex::new(r"^(\s*[+!|:\s]*)(\d+)\s+(.+?)\s+\(in\s+([^)]+)\)")?;

        for line in reader.lines() {
            let line = line?;

            if line.contains("Call graph:") {
                in_call_graph = true;
                continue;
            }
            if in_call_graph && line.trim().starts_with("Total number") {
                break;
            }
            if let Some(caps) = thread_re.captures(&line) {
                if let Ok(count) = caps[1].parse::<u64>() {
                    thread_totals.push(count);
                }
                continue;
            }
            if !in_call_graph {
                continue;
            }
            if let Some(caps) = call_re.captures(&line) {
                let prefix = &caps[1];
                let count: u64 = caps[2].parse().unwrap_or(0);
                let func_name = super::demangle_rust_symbol(&caps[3]);
                let library = &caps[4];

                let key = format!("{func_name} [{library}]");
                let depth = prefix.replace(' ', "").len();

                call_graph_lines.push((depth, count, key.clone()));
                let entry = inclusive.entry(key).or_insert(0);
                *entry = (*entry).max(count);
            }
        }

        let exclusive = compute_exclusive(&call_graph_lines);
        let total_samples = thread_totals.iter().max().copied().unwrap_or(1);
        let total_excl: u64 = exclusive.values().sum();

        println!(
            "================================================================================"
        );
        println!("MQDB PROFILE ANALYSIS REPORT");
        println!(
            "================================================================================"
        );
        println!("\nTotal samples (per thread): {total_samples}");
        println!("Total exclusive samples: {total_excl}");

        print_category_breakdown(&exclusive, total_excl);
        print_top_functions(&inclusive, total_samples);

        let waiting: u64 = exclusive
            .iter()
            .filter(|(k, _)| {
                k.contains("__psynch_cvwait")
                    || k.contains("__psynch_mutexwait")
                    || k.contains("kevent")
            })
            .map(|(_, v)| v)
            .sum();
        let active = total_excl.saturating_sub(waiting);

        println!(
            "\n--------------------------------------------------------------------------------"
        );
        println!("SUMMARY");
        println!(
            "--------------------------------------------------------------------------------"
        );
        println!(
            "\nActive work: {active} samples ({:.1}%)",
            pct(active, total_excl)
        );
        println!(
            "Waiting/idle: {waiting} samples ({:.1}%)",
            pct(waiting, total_excl)
        );

        if active < total_excl / 10 {
            println!("\n[INFO] System is mostly idle - try longer benchmark or more clients");
        }

        println!(
            "\n================================================================================"
        );
        Ok(())
    }
}

fn analyze_sample_output(filepath: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    profile_analysis::analyze(filepath)
}

fn demangle_rust_symbol(name: &str) -> String {
    let mut s = name.to_string();
    s = s.replace("::$u7b$$u7b$closure$u7d$$u7d$", "::{{closure}}");
    s = s.replace("$LT$", "<");
    s = s.replace("$GT$", ">");
    s = s.replace("$u20$", " ");
    if let Some(pos) = s.rfind("::h")
        && s.len() - pos == 19
        && s[pos + 3..].chars().all(|c| c.is_ascii_hexdigit())
    {
        s.truncate(pos);
    }
    s
}

fn cmd_dev_profile(
    scenario: &DevBenchScenario,
    tool: &str,
    duration: u64,
    output: Option<PathBuf>,
    db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;

    if is_agent_running() {
        println!("Killing existing agent for profiling...");
        let _ = Command::new("pkill").args(["-f", "mqdb agent"]).status();
        std::thread::sleep(Duration::from_secs(1));
    }

    std::fs::create_dir_all(db)?;

    let timestamp = chrono_timestamp();
    let output_path = output.unwrap_or_else(|| {
        let dir = PathBuf::from(".claude/profiles");
        let _ = std::fs::create_dir_all(&dir);
        match tool {
            "flamegraph" => dir.join(format!("profile_{timestamp}.svg")),
            _ => dir.join(format!("profile_{timestamp}")),
        }
    });

    println!("Profiling with {tool}...");
    println!("Output will be saved to: {}", output_path.display());

    let bench_args = build_bench_args(scenario);

    match tool {
        "samply" => profile_with_samply(&exe, db, &output_path, &bench_args)?,
        "flamegraph" => profile_with_flamegraph(&exe, db, &output_path, &bench_args)?,
        "sample" => profile_with_sample(&exe, db, &output_path, &bench_args, duration)?,
        _ => {
            return Err(format!(
                "Unknown profiling tool: {tool}. Use 'sample', 'samply', or 'flamegraph'"
            )
            .into());
        }
    }

    Ok(())
}

fn cmd_dev_baseline(action: DevBaselineAction) -> Result<(), Box<dyn std::error::Error>> {
    let baselines_dir = PathBuf::from(".claude/benchmarks/baselines");

    match action {
        DevBaselineAction::Save { name, scenario } => {
            std::fs::create_dir_all(&baselines_dir)?;

            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            println!("To save baseline, first run benchmark with --output:");
            println!(
                "  mqdb dev bench {scenario_name} --output {}",
                path.display()
            );
        }
        DevBaselineAction::List => {
            if !baselines_dir.exists() {
                println!("No baselines saved yet.");
                println!("Create with: mqdb dev baseline save <name> pubsub|db");
                return Ok(());
            }

            println!("Saved baselines:");
            for entry in std::fs::read_dir(&baselines_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "json")
                    && let Some(name) = path.file_stem()
                {
                    println!("  {}", name.to_string_lossy());
                }
            }
        }
        DevBaselineAction::Compare { name, scenario } => {
            let scenario_name = match &scenario {
                DevBenchScenario::Pubsub { .. } => "pubsub",
                DevBenchScenario::Db { .. } => "db",
            };

            let filename = format!("{scenario_name}_{name}.json");
            let path = baselines_dir.join(&filename);

            if !path.exists() {
                return Err(format!("Baseline not found: {}", path.display()).into());
            }

            println!("To compare against baseline, run benchmark with --baseline:");
            println!(
                "  mqdb dev bench {scenario_name} --baseline {}",
                path.display()
            );
        }
    }

    Ok(())
}
