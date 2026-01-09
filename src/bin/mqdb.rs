use bebytes::BeBytes;
use clap::{Parser, Subcommand, ValueEnum};
use mqdb::cluster::db::DbEntity;
use mqdb::cluster::db_protocol::{DbReadRequest, DbResponse, DbStatus, DbWriteRequest};
use mqdb::{ClusterConfig, ClusteredAgent, Database, MqdbAgent, PeerConfig};
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
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
        #[arg(long)]
        no_quic: bool,
        #[arg(long, default_value = "/tmp/mqdb-test")]
        db_prefix: String,
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
        #[arg(long, default_value = "10000", help = "Total messages to send")]
        messages: u64,
        #[arg(
            long,
            default_value = "0",
            help = "Messages per second (0 = unlimited)"
        )]
        rate: u64,
        #[arg(long, default_value = "64", help = "Payload size in bytes")]
        size: usize,
        #[arg(long, default_value = "0", help = "MQTT QoS level (0, 1, or 2)")]
        qos: u8,
        #[arg(long, default_value = "bench/test", help = "Topic pattern")]
        topic: String,
        #[arg(long, help = "Warmup messages before measuring")]
        warmup: Option<u64>,
        #[command(flatten)]
        conn: ConnectionArgs,
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

#[derive(Subcommand)]
enum AgentAction {
    Start {
        #[arg(long, default_value = "127.0.0.1:1883")]
        bind: SocketAddr,
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        passwd: Option<PathBuf>,
        #[arg(long)]
        acl: Option<PathBuf>,
        #[arg(long)]
        anonymous: bool,
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
        #[arg(long, help = "Path to password file")]
        passwd: Option<PathBuf>,
        #[arg(long, help = "Path to ACL file")]
        acl: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC TLS certificate")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC TLS private key")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "Disable QUIC transport")]
        no_quic: bool,
        #[arg(
            long,
            help = "Disable store persistence (data will not survive restarts)"
        )]
        no_persist_stores: bool,
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
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Agent { action } => match action {
            AgentAction::Start {
                bind,
                db,
                passwd,
                acl,
                anonymous,
            } => {
                cmd_agent_start(bind, db, passwd, acl, anonymous).await?;
            }
            AgentAction::Status { conn } => {
                Box::pin(cmd_agent_status(conn)).await?;
            }
        },
        Commands::Cluster { action } => match action {
            ClusterAction::Start {
                node_id,
                node_name,
                bind,
                db,
                peers,
                passwd,
                acl,
                quic_cert,
                quic_key,
                no_quic,
                no_persist_stores,
            } => {
                Box::pin(cmd_cluster_start(ClusterStartArgs {
                    node_id,
                    node_name,
                    bind,
                    db_path: db,
                    peers,
                    passwd,
                    acl,
                    quic_cert,
                    quic_key,
                    no_quic,
                    no_persist_stores,
                }))
                .await?;
            }
            ClusterAction::Rebalance { conn } => {
                Box::pin(cmd_cluster_rebalance(conn)).await?;
            }
            ClusterAction::Status { conn } => {
                Box::pin(cmd_cluster_status(conn)).await?;
            }
        },
        Commands::Passwd {
            username,
            batch,
            delete,
            stdout: _,
        } => {
            cmd_passwd(&username, batch, delete)?;
        }
        Commands::Create {
            entity,
            data,
            conn,
            format,
        } => {
            Box::pin(cmd_create(entity, data, conn, format)).await?;
        }
        Commands::Read {
            entity,
            id,
            projection,
            conn,
            format,
        } => {
            Box::pin(cmd_read(entity, id, projection, conn, format)).await?;
        }
        Commands::Update {
            entity,
            id,
            data,
            conn,
            format,
        } => {
            Box::pin(cmd_update(entity, id, data, conn, format)).await?;
        }
        Commands::Delete {
            entity,
            id,
            conn,
            format,
        } => {
            Box::pin(cmd_delete(entity, id, conn, format)).await?;
        }
        Commands::List {
            entity,
            filter,
            sort,
            limit,
            offset,
            conn,
            format,
        } => {
            Box::pin(cmd_list(entity, filter, sort, limit, offset, conn, format)).await?;
        }
        Commands::Watch {
            entity,
            filter,
            conn,
            format,
        } => {
            Box::pin(cmd_watch(entity, filter, conn, format)).await?;
        }
        Commands::Schema { action } => match action {
            SchemaAction::Set { entity, file, conn } => {
                Box::pin(cmd_schema_set(entity, file, conn)).await?;
            }
            SchemaAction::Get {
                entity,
                conn,
                format,
            } => {
                Box::pin(cmd_schema_get(entity, conn, format)).await?;
            }
        },
        Commands::Constraint { action } => match action {
            ConstraintAction::Add {
                entity,
                name,
                unique,
                fk,
                not_null,
                conn,
            } => {
                Box::pin(cmd_constraint_add(entity, name, unique, fk, not_null, conn)).await?;
            }
            ConstraintAction::List {
                entity,
                conn,
                format,
            } => {
                Box::pin(cmd_constraint_list(entity, conn, format)).await?;
            }
        },
        Commands::Backup { action } => match action {
            BackupAction::Create { name, conn } => {
                Box::pin(cmd_backup_create(&name, &conn)).await?;
            }
            BackupAction::List { conn } => {
                Box::pin(cmd_backup_list(&conn)).await?;
            }
        },
        Commands::Restore { name, conn } => {
            Box::pin(cmd_restore(&name, &conn)).await?;
        }
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
        Commands::ConsumerGroup { action } => match action {
            ConsumerGroupAction::List { conn, format } => {
                Box::pin(cmd_consumer_group_list(conn, format)).await?;
            }
            ConsumerGroupAction::Show { name, conn, format } => {
                Box::pin(cmd_consumer_group_show(name, conn, format)).await?;
            }
        },
        Commands::Db { action } => match action {
            DbAction::Create {
                partition,
                entity,
                data,
                conn,
            } => {
                Box::pin(cmd_db_create(partition, entity, data, conn)).await?;
            }
            DbAction::Read {
                partition,
                entity,
                id,
                conn,
            } => {
                Box::pin(cmd_db_read(partition, entity, id, conn)).await?;
            }
            DbAction::Update {
                partition,
                entity,
                id,
                data,
                conn,
            } => {
                Box::pin(cmd_db_update(partition, entity, id, data, conn)).await?;
            }
            DbAction::Delete {
                partition,
                entity,
                id,
                conn,
            } => {
                Box::pin(cmd_db_delete(partition, entity, id, conn)).await?;
            }
        },
        Commands::Dev { action } => match action {
            DevAction::Ps => {
                cmd_dev_ps()?;
            }
            DevAction::Kill { all, node, agent } => {
                cmd_dev_kill(all, node, agent);
            }
            DevAction::Clean { db_prefix } => {
                cmd_dev_clean(&db_prefix)?;
            }
            DevAction::Logs {
                node,
                pattern,
                follow,
                last,
                db_prefix,
            } => {
                cmd_dev_logs(node, pattern.as_deref(), follow, last, &db_prefix)?;
            }
            DevAction::Test {
                pubsub,
                db,
                constraints,
                wildcards,
                retained,
                lwt,
                all,
                nodes,
            } => {
                cmd_dev_test(
                    pubsub,
                    db,
                    constraints,
                    wildcards,
                    retained,
                    lwt,
                    all,
                    nodes,
                );
            }
            DevAction::StartCluster {
                nodes,
                clean,
                quic_cert,
                quic_key,
                no_quic,
                db_prefix,
            } => {
                cmd_dev_start_cluster(nodes, clean, &quic_cert, &quic_key, no_quic, &db_prefix)?;
            }
        },
        Commands::Bench { action } => match action {
            BenchAction::Pubsub {
                publishers,
                subscribers,
                messages,
                rate,
                size,
                qos,
                topic,
                warmup,
                conn,
                format,
            } => {
                Box::pin(cmd_bench_pubsub(BenchPubsubArgs {
                    publishers,
                    subscribers,
                    messages,
                    rate,
                    size,
                    qos,
                    topic,
                    warmup: warmup.unwrap_or(0),
                    conn,
                    format,
                }))
                .await?;
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
                    conn,
                    format,
                }))
                .await?;
            }
        },
    }

    Ok(())
}

async fn cmd_agent_start(
    bind: SocketAddr,
    db_path: PathBuf,
    passwd: Option<PathBuf>,
    acl: Option<PathBuf>,
    anonymous: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(&db_path).await?;

    let mut agent = MqdbAgent::new(db).with_bind_address(bind);

    if let Some(passwd_file) = passwd {
        agent = agent.with_password_file(passwd_file);
    }
    if let Some(acl_file) = acl {
        agent = agent.with_acl_file(acl_file);
    }
    if !anonymous {
        agent = agent.with_anonymous(false);
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

struct ClusterStartArgs {
    node_id: u16,
    node_name: Option<String>,
    bind: SocketAddr,
    db_path: PathBuf,
    peers: Vec<String>,
    passwd: Option<PathBuf>,
    acl: Option<PathBuf>,
    quic_cert: Option<PathBuf>,
    quic_key: Option<PathBuf>,
    no_quic: bool,
    no_persist_stores: bool,
}

async fn cmd_cluster_start(args: ClusterStartArgs) -> Result<(), Box<dyn std::error::Error>> {
    let peer_configs = parse_peer_configs(&args.peers)?;

    let mut config = ClusterConfig::new(args.node_id, args.db_path, peer_configs);
    config = config.with_bind_address(args.bind);

    if let Some(name) = args.node_name {
        config = config.with_node_name(name);
    }
    if let Some(passwd_file) = args.passwd {
        config = config.with_password_file(passwd_file);
    }
    if let Some(acl_file) = args.acl {
        config = config.with_acl_file(acl_file);
    }
    if let (Some(cert), Some(key)) = (args.quic_cert, args.quic_key) {
        config = config.with_quic_certs(cert, key);
    }
    if args.no_quic {
        config = config.with_quic(false);
    }
    if args.no_persist_stores {
        config = config.with_persist_stores(false);
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
) -> Result<(), Box<dyn std::error::Error>> {
    if delete {
        eprintln!("Delete operation not yet implemented");
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
    println!("{username}:{hash}");

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
    if !filters.is_empty() {
        eprintln!("Note: Client-side filtering not yet implemented, showing all events");
    }

    let client = Box::pin(connect_client(&conn)).await?;
    let topic = format!("$DB/{entity}/events/#");

    let (tx, mut rx) = mpsc::channel::<Value>(100);

    client
        .subscribe(&topic, move |msg| {
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send(value);
            }
        })
        .await?;

    eprintln!("Watching {entity} events (Ctrl+C to stop)...");

    while let Some(event) = rx.recv().await {
        output_response(&event, &format);
    }

    Ok(())
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
    let (tx, mut rx) = mpsc::channel::<Value>(1);

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
    let response = tokio::time::timeout(timeout, rx.recv())
        .await
        .map_err(|_| "Request timed out")?
        .ok_or("No response received")?;

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
        let (tx, mut rx) = mpsc::channel::<Value>(100);

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
                event = rx.recv() => {
                    if let Some(event) = event {
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
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);

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
    let response_bytes = tokio::time::timeout(timeout, rx.recv())
        .await
        .map_err(|_| "Request timed out")?
        .ok_or("No response received")?;

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

    std::thread::sleep(std::time::Duration::from_millis(500));

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

fn cmd_dev_start_cluster(
    nodes: u8,
    clean: bool,
    quic_cert: &std::path::Path,
    quic_key: &std::path::Path,
    no_quic: bool,
    db_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if clean {
        println!("Cleaning existing databases...");
        let _ = cmd_dev_clean(db_prefix);
    }

    let exe = std::env::current_exe()?;

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
            &format!("127.0.0.1:{port}"),
            "--db",
            &db_path,
        ]);

        if node_id > 1 {
            cmd.args(["--peers", "1@127.0.0.1:1883"]);
        }

        if !no_quic && quic_cert.exists() && quic_key.exists() {
            cmd.args([
                "--quic-cert",
                quic_cert.to_str().unwrap_or(""),
                "--quic-key",
                quic_key.to_str().unwrap_or(""),
            ]);
        }

        let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
        cmd.env("RUST_LOG", &rust_log);

        std::fs::create_dir_all(&db_path)?;
        let log_file = std::fs::File::create(format!("{db_path}/mqdb.log"))?;
        cmd.stdout(log_file.try_clone()?);
        cmd.stderr(log_file);

        println!(
            "Starting node {node_id} on port {port}{}...",
            if no_quic { " (TCP)" } else { " (QUIC)" }
        );

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
            let (payload_tx, mut payload_rx) = mpsc::channel::<Vec<u8>>(1);

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

                if let Ok(Some(payload)) =
                    tokio::time::timeout(Duration::from_secs(2), payload_rx.recv()).await
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
    messages: u64,
    rate: u64,
    size: usize,
    qos: u8,
    topic: String,
    warmup: u64,
    conn: ConnectionArgs,
    format: OutputFormat,
}

#[derive(Default)]
struct BenchMetrics {
    messages_sent: std::sync::atomic::AtomicU64,
    messages_received: std::sync::atomic::AtomicU64,
    bytes_sent: std::sync::atomic::AtomicU64,
    bytes_received: std::sync::atomic::AtomicU64,
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

    wait_for_broker_ready(&args.conn, 30).await?;

    let metrics = Arc::new(BenchMetrics::default());
    let running = Arc::new(AtomicBool::new(true));

    println!(
        "Benchmark: {} publishers, {} subscribers, {} messages, {} byte payload, QoS {}",
        args.publishers, args.subscribers, args.messages, args.size, args.qos
    );

    let mut sub_handles = Vec::new();
    let mut pub_handles = Vec::new();

    for sub_id in 0..args.subscribers {
        let conn = args.conn.clone();
        let topic = args.topic.clone();
        let metrics = Arc::clone(&metrics);
        let running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-sub-{sub_id}");
            let client = MqttClient::new(&client_id);

            if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                let opts =
                    ConnectOptions::new(&client_id).with_credentials(user.clone(), pass.clone());
                if let Err(e) = Box::pin(client.connect_with_options(&conn.broker, opts)).await {
                    eprintln!("Subscriber {sub_id} connect failed: {e}");
                    return;
                }
            } else if let Err(e) = client.connect(&conn.broker).await {
                eprintln!("Subscriber {sub_id} connect failed: {e}");
                return;
            }

            let metrics_clone = Arc::clone(&metrics);
            if let Err(e) = client
                .subscribe(&topic, move |msg| {
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
                    metrics_clone
                        .bytes_received
                        .fetch_add(payload.len() as u64, Ordering::Relaxed);
                })
                .await
            {
                eprintln!("Subscriber {sub_id} subscribe failed: {e}");
                return;
            }

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let _ = client.disconnect().await;
        });
        sub_handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload_template: Vec<u8> = vec![0u8; args.size];
    let total_messages = args.messages;
    let messages_per_pub = total_messages / args.publishers.max(1) as u64;
    let rate_per_pub = if args.rate > 0 {
        args.rate / args.publishers.max(1) as u64
    } else {
        0
    };

    let start_time = Instant::now();

    for pub_id in 0..args.publishers {
        let conn = args.conn.clone();
        let topic = args.topic.clone();
        let metrics = Arc::clone(&metrics);
        let payload_template = payload_template.clone();
        let warmup = args.warmup;

        let handle = tokio::spawn(async move {
            let client_id = format!("bench-pub-{pub_id}");
            let client = MqttClient::new(&client_id);

            if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
                let opts =
                    ConnectOptions::new(&client_id).with_credentials(user.clone(), pass.clone());
                if let Err(e) = Box::pin(client.connect_with_options(&conn.broker, opts)).await {
                    eprintln!("Publisher {pub_id} connect failed: {e}");
                    return;
                }
            } else if let Err(e) = client.connect(&conn.broker).await {
                eprintln!("Publisher {pub_id} connect failed: {e}");
                return;
            }

            let interval = if rate_per_pub > 0 {
                Some(Duration::from_nanos(1_000_000_000 / rate_per_pub))
            } else {
                None
            };

            for i in 0..(warmup + messages_per_pub) {
                let mut payload = payload_template.clone();
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                payload[0..8].copy_from_slice(&now_ns.to_be_bytes());

                if let Err(e) = client.publish(&topic, payload.clone()).await {
                    eprintln!("Publish error: {e}");
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                } else if i >= warmup {
                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .bytes_sent
                        .fetch_add(payload.len() as u64, Ordering::Relaxed);
                }

                if let Some(interval) = interval {
                    tokio::time::sleep(interval).await;
                }
            }

            let _ = client.disconnect().await;
        });
        pub_handles.push(handle);
    }

    for handle in pub_handles {
        let _ = handle.await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    running.store(false, Ordering::Release);

    for handle in sub_handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    let sent = metrics.messages_sent.load(Ordering::Relaxed);
    let received = metrics.messages_received.load(Ordering::Relaxed);
    let bytes_sent = metrics.bytes_sent.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);

    let p50 = metrics.calculate_percentile(50.0);
    let p95 = metrics.calculate_percentile(95.0);
    let p99 = metrics.calculate_percentile(99.0);

    let throughput = sent as f64 / elapsed_secs;
    let bandwidth_mbps = (bytes_sent as f64 * 8.0) / (elapsed_secs * 1_000_000.0);

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
                "messages": args.messages,
                "payload_size": args.size,
                "qos": args.qos,
                "rate_limit": args.rate
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

    let record_size_approx = args.fields * (args.field_size + 20);
    println!(
        "DB Benchmark: {} ops, {} concurrent, op={:?}, ~{} bytes/record",
        args.operations, args.concurrency, op, record_size_approx
    );

    let metrics = Arc::new(DbBenchMetrics::default());
    let inserted_ids: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let id_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

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

                let op_start = Instant::now();
                let success = match current_op {
                    DbOp::Insert => {
                        let id = id_counter.fetch_add(1, Ordering::Relaxed);
                        let record = generate_record(fields, field_size, id);
                        let topic = format!("$DB/{entity}/create");
                        let response_topic =
                            format!("bench-db-{client_id}/resp/{}", uuid::Uuid::new_v4());

                        let (tx, mut rx) = mpsc::channel::<bool>(1);
                        let tx_clone = tx.clone();
                        if client
                            .subscribe(&response_topic, move |msg| {
                                let success = msg.payload.iter().any(|&b| b == b'"' || b == b'{');
                                let _ = tx_clone.try_send(success);
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
                                let result =
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv())
                                        .await
                                        .ok()
                                        .flatten()
                                        .unwrap_or(false);
                                if result && let Ok(mut ids) = inserted_ids.lock() {
                                    ids.push(format!("{id}"));
                                }
                                let _ = client.unsubscribe(&response_topic).await;
                                result
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

                            let (tx, mut rx) = mpsc::channel::<bool>(1);
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
                                    let result =
                                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                                            .await
                                            .ok()
                                            .flatten()
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

                            let (tx, mut rx) = mpsc::channel::<bool>(1);
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
                                    let result =
                                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                                            .await
                                            .ok()
                                            .flatten()
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

                            let (tx, mut rx) = mpsc::channel::<bool>(1);
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
                                    let result =
                                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                                            .await
                                            .ok()
                                            .flatten()
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

                        let (tx, mut rx) = mpsc::channel::<bool>(1);
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
                                    tokio::time::timeout(Duration::from_secs(5), rx.recv())
                                        .await
                                        .ok()
                                        .flatten()
                                        .unwrap_or(false);
                                let _ = client.unsubscribe(&response_topic).await;
                                result
                            }
                        }
                    }
                    DbOp::Mixed => unreachable!(),
                };

                let latency_ns = op_start.elapsed().as_nanos() as u64;

                if !is_warmup {
                    if success {
                        metrics.record_latency(latency_ns);
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
