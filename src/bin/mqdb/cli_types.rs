use clap::{Args, Parser, Subcommand, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mqdb")]
#[command(about = "MQDB command-line interface")]
#[command(version)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
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
pub(crate) enum DbAction {
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
pub(crate) enum BackupAction {
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

#[derive(Subcommand)]
pub(crate) enum ConsumerGroupAction {
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
pub(crate) enum DurabilityArg {
    Immediate,
    #[default]
    Periodic,
    None,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum JwtAlgorithmArg {
    Hs256,
    Rs256,
    Es256,
}

#[derive(Args)]
pub(crate) struct AuthArgs {
    #[arg(long, help = "Path to password file")]
    pub(crate) passwd: Option<PathBuf>,
    #[arg(long, help = "Path to ACL file")]
    pub(crate) acl: Option<PathBuf>,
    #[cfg(feature = "dev-insecure")]
    #[arg(long, help = "Allow anonymous connections (dev only)")]
    pub(crate) anonymous: bool,
    #[arg(long, help = "Path to SCRAM-SHA-256 credentials file")]
    pub(crate) scram_file: Option<PathBuf>,
    #[arg(long, help = "JWT algorithm: hs256, rs256, es256")]
    pub(crate) jwt_algorithm: Option<JwtAlgorithmArg>,
    #[arg(long, requires = "jwt_algorithm", help = "Path to JWT secret/key file")]
    pub(crate) jwt_key: Option<PathBuf>,
    #[arg(long, help = "JWT issuer claim")]
    pub(crate) jwt_issuer: Option<String>,
    #[arg(long, help = "JWT audience claim")]
    pub(crate) jwt_audience: Option<String>,
    #[arg(
        long,
        default_value = "60",
        help = "JWT clock skew tolerance in seconds"
    )]
    pub(crate) jwt_clock_skew: u64,
    #[arg(long, conflicts_with_all = ["jwt_algorithm"], help = "Path to federated JWT config JSON")]
    pub(crate) federated_jwt_config: Option<PathBuf>,
    #[arg(long, help = "Path to certificate auth file")]
    pub(crate) cert_auth_file: Option<PathBuf>,
    #[arg(long, help = "Disable authentication rate limiting")]
    pub(crate) no_rate_limit: bool,
    #[arg(long, default_value = "5", help = "Rate limit max failed attempts")]
    pub(crate) rate_limit_max_attempts: u32,
    #[arg(long, default_value = "60", help = "Rate limit window in seconds")]
    pub(crate) rate_limit_window_secs: u64,
    #[arg(
        long,
        default_value = "300",
        help = "Rate limit lockout duration in seconds"
    )]
    pub(crate) rate_limit_lockout_secs: u64,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Comma-separated list of admin usernames"
    )]
    pub(crate) admin_users: Vec<String>,
}

#[derive(Args, Clone)]
pub(crate) struct OAuthArgs {
    #[arg(long, help = "HTTP server bind address for OAuth (e.g. 0.0.0.0:8081)")]
    pub(crate) http_bind: Option<SocketAddr>,
    #[arg(long, help = "Path to file containing Google OAuth client secret")]
    pub(crate) oauth_client_secret: Option<PathBuf>,
    #[arg(
        long,
        help = "OAuth redirect URI (default: http://localhost:{http_port}/oauth/callback)"
    )]
    pub(crate) oauth_redirect_uri: Option<String>,
    #[arg(long, help = "URI to redirect browser after OAuth completes")]
    pub(crate) oauth_frontend_redirect: Option<String>,
    #[arg(
        long,
        default_value = "30",
        help = "Ticket JWT expiry in seconds (default: 30)"
    )]
    pub(crate) ticket_expiry_secs: u64,
    #[arg(long, help = "Set Secure flag on session cookies (requires HTTPS)")]
    pub(crate) cookie_secure: bool,
    #[arg(
        long,
        help = "CORS allowed origin for auth endpoints (e.g. http://localhost:8000)"
    )]
    pub(crate) cors_origin: Option<String>,
    #[arg(
        long,
        default_value = "10",
        help = "Max ticket requests per minute per session"
    )]
    pub(crate) ticket_rate_limit: u32,
}

#[derive(Subcommand)]
pub(crate) enum AclAction {
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
pub(crate) enum AgentAction {
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
pub(crate) enum ClusterAction {
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
pub(crate) enum SchemaAction {
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
pub(crate) enum ConstraintAction {
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
pub(crate) struct ConnectionArgs {
    #[arg(long, env = "MQDB_BROKER", default_value = "127.0.0.1:1883")]
    pub(crate) broker: String,
    #[arg(long, env = "MQDB_USER")]
    pub(crate) user: Option<String>,
    #[arg(long, env = "MQDB_PASS")]
    pub(crate) pass: Option<String>,
    #[arg(long, default_value = "30")]
    pub(crate) timeout: u64,
    #[arg(
        long,
        help = "Skip TLS certificate verification (for self-signed certs)"
    )]
    pub(crate) insecure: bool,
}

#[derive(Clone, ValueEnum)]
pub(crate) enum OutputFormat {
    Json,
    Table,
    Csv,
}

#[derive(Clone, ValueEnum)]
pub(crate) enum SubscriptionModeArg {
    Broadcast,
    LoadBalanced,
    Ordered,
}
