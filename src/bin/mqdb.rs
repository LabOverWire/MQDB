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
    Start {
        #[arg(long)]
        node_id: u16,
        #[arg(long)]
        node_name: Option<String>,
        #[arg(long, default_value = "0.0.0.0:1883")]
        bind: SocketAddr,
        #[arg(long)]
        db: PathBuf,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        #[arg(long)]
        passwd: Option<PathBuf>,
        #[arg(long)]
        acl: Option<PathBuf>,
        #[arg(long)]
        quic_cert: Option<PathBuf>,
        #[arg(long)]
        quic_key: Option<PathBuf>,
        #[arg(long)]
        no_quic: bool,
    },
    Rebalance {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
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
                unique,
                fk,
                not_null,
                conn,
            } => {
                Box::pin(cmd_constraint_add(entity, unique, fk, not_null, conn)).await?;
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

    let mut agent = ClusteredAgent::new(config).map_err(|e| e.clone())?;
    Box::pin(agent.run()).await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn cmd_cluster_rebalance(conn: ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$SYS/mqdb/cluster/rebalance";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(data) = response.get("data") {
            if let Some(id) = data.get("rebalance_id").and_then(serde_json::Value::as_str) {
                println!("Rebalance initiated: {id}");
            } else {
                println!("Rebalance initiated");
            }
        } else {
            println!("Rebalance initiated");
        }
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_cluster_status(conn: ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$SYS/mqdb/cluster/status";
    let response = Box::pin(execute_request(&conn, topic, json!({}))).await?;

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(data) = response.get("data") {
            println!("{}", serde_json::to_string_pretty(data)?);
        }
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    } else {
        output_response(&response, &OutputFormat::Json);
    }

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
    unique: Option<String>,
    fk: Option<String>,
    not_null: Option<String>,
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("$DB/_admin/constraint/{entity}/add");

    let payload = if let Some(field) = unique {
        json!({ "type": "unique", "fields": [field] })
    } else if let Some(fk_spec) = fk {
        let parts: Vec<&str> = fk_spec.split(':').collect();
        if parts.len() < 3 {
            return Err("FK format: field:target_entity:target_field[:action]".into());
        }
        json!({
            "type": "foreign_key",
            "field": parts[0],
            "target_entity": parts[1],
            "target_field": parts[2],
            "on_delete": parts.get(3).unwrap_or(&"restrict")
        })
    } else if let Some(field) = not_null {
        json!({ "type": "not_null", "field": field })
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

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(data) = response.get("data")
            && let Some(msg) = data.get("message").and_then(serde_json::Value::as_str)
        {
            println!("{msg}");
        }
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_backup_list(conn: &ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/backup/list";
    let payload = json!({});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
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
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        eprintln!("Error: {error}");
    }

    Ok(())
}

async fn cmd_restore(name: &str, conn: &ConnectionArgs) -> Result<(), Box<dyn std::error::Error>> {
    let topic = "$DB/_admin/restore";
    let payload = json!({"name": name});
    let response = Box::pin(execute_request(conn, topic, payload)).await?;

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        println!("Restore initiated");
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
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

            let ops = ["!=", ">=", "<=", "!?", ">", "<", "=", "~", "?"];

            for op in ops {
                if let Some(pos) = part.find(op) {
                    let field = part[..pos].trim();
                    let value_str = part[pos + op.len()..].trim();

                    let filter_op = match op {
                        "!=" => "ne",
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

    if response
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
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
    } else if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
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
