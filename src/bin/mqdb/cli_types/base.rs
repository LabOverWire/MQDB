// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

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
    #[command(about = "Manage the standalone MQTT broker agent")]
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },
    #[command(about = "Manage distributed cluster nodes")]
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    #[command(about = "Manage password-file credentials")]
    Passwd {
        #[arg(help = "Username to add, update, or delete")]
        username: String,
        #[arg(short, long, help = "Batch mode - password on command line")]
        batch: Option<String>,
        #[arg(short = 'D', long, help = "Delete the specified user")]
        delete: bool,
        #[arg(short = 'n', long, help = "Output credentials to stdout")]
        stdout: bool,
        #[arg(short, long, help = "Password file path")]
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
    #[command(about = "Create a new record in an entity")]
    Create {
        #[arg(help = "Entity name (e.g. users, orders)")]
        entity: String,
        #[arg(short, long, help = "JSON data for the new record")]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Read a record by ID")]
    Read {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(help = "Record ID")]
        id: String,
        #[arg(long, help = "Comma-separated list of fields to return")]
        projection: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Update a record by ID")]
    Update {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(help = "Record ID")]
        id: String,
        #[arg(short, long, help = "JSON data with fields to update")]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Delete a record by ID")]
    Delete {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(help = "Record ID")]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "List records with optional filtering and sorting")]
    List {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(
            short,
            long,
            help = "Filter expressions: field=value, field>value, field~pattern*"
        )]
        filter: Vec<String>,
        #[arg(short, long, help = "Sort: field:asc or field:desc (comma-separated)")]
        sort: Option<String>,
        #[arg(short, long, help = "Maximum number of records to return")]
        limit: Option<usize>,
        #[arg(short, long, help = "Number of records to skip")]
        offset: Option<usize>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Watch an entity for real-time change events")]
    Watch {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "Filter expressions for events")]
        filter: Vec<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Manage entity schemas")]
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    #[command(about = "Manage entity constraints (unique, foreign key, not-null)")]
    Constraint {
        #[command(subcommand)]
        action: ConstraintAction,
    },
    #[command(about = "Create and manage database backups")]
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    #[command(about = "Restore a database from a named backup")]
    Restore {
        #[arg(short, long, help = "Name of the backup to restore")]
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    #[command(about = "Subscribe to entity change events")]
    Subscribe {
        #[arg(help = "Topic pattern to subscribe to")]
        pattern: String,
        #[arg(long, help = "Filter events to a specific entity")]
        entity: Option<String>,
        #[arg(long, help = "Consumer group name for load-balanced delivery")]
        group: Option<String>,
        #[arg(
            long,
            default_value = "broadcast",
            help = "Delivery mode: broadcast, load-balanced, ordered"
        )]
        mode: SubscriptionModeArg,
        #[arg(long, default_value = "10", help = "Heartbeat interval in seconds")]
        heartbeat_interval: u64,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Manage consumer groups")]
    ConsumerGroup {
        #[command(subcommand)]
        action: ConsumerGroupAction,
    },
    #[command(about = "Low-level database management commands")]
    Db {
        #[command(subcommand)]
        action: DbAction,
    },
    #[command(about = "Development and testing utilities")]
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    #[command(about = "Run performance benchmarks")]
    Bench {
        #[command(subcommand)]
        action: BenchAction,
    },
}

#[derive(Clone, clap::Args)]
pub(crate) struct ConnectionArgs {
    #[arg(long, env = "MQDB_BROKER", default_value = "127.0.0.1:1883")]
    pub(crate) broker: String,
    #[arg(long, env = "MQDB_USER", requires = "pass")]
    pub(crate) user: Option<String>,
    #[arg(long, env = "MQDB_PASS", requires = "user")]
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
