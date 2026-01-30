use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use super::agent::{AgentAction, ClusterAction};
use super::auth::AclAction;
use super::bench::BenchAction;
use super::db::{BackupAction, ConstraintAction, ConsumerGroupAction, DbAction, SchemaAction};
use super::dev::DevAction;

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
