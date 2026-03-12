// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod cli_types;
mod commands;
mod common;

use cli_types::{
    AclAction, AgentAction, BackupAction, BenchAction, Cli, ClusterAction, Commands,
    ConstraintAction, ConsumerGroupAction, DbAction, DevAction, IndexAction, SchemaAction,
};
use commands::agent::{AgentStartArgs, cmd_agent_start, cmd_agent_status};
use commands::bench::{BenchDbArgs, BenchPubsubArgs, cmd_bench_db, cmd_bench_pubsub};
use commands::cluster::{
    ClusterStartArgs, cmd_cluster_rebalance, cmd_cluster_start, cmd_cluster_status,
};
use commands::crud::{
    ListParams, cmd_backup_create, cmd_backup_list, cmd_constraint_add, cmd_constraint_list,
    cmd_create, cmd_delete, cmd_index_add, cmd_list, cmd_read, cmd_restore, cmd_schema_get,
    cmd_schema_set, cmd_subscribe, cmd_update, cmd_watch,
};

use clap::Parser;
use tracing_subscriber::EnvFilter;

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
        } => commands::auth::cmd_passwd(&username, batch, delete, file)?,
        Commands::Scram {
            username,
            batch,
            delete,
            stdout,
            file,
            iterations,
        } => commands::auth::cmd_scram(&username, batch, delete, stdout, file, iterations)?,
        Commands::Acl { action } => Box::pin(commands::acl::cmd_acl(action)).await?,
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
            entity, filter, sort, limit, offset, projection, conn, format,
        } => {
            let params = ListParams { entity, filters: filter, sort, limit, offset, projection };
            Box::pin(cmd_list(params, conn, format)).await?;
        }
        Commands::Watch {
            entity,
            filter,
            conn,
            format,
        } => Box::pin(cmd_watch(entity, filter, conn, format)).await?,
        Commands::Schema { action } => dispatch_schema(action).await?,
        Commands::Constraint { action } => dispatch_constraint(action).await?,
        Commands::Index { action } => dispatch_index(action).await?,
        Commands::Backup { action } => dispatch_backup(action).await?,
        Commands::Restore { name, conn } => Box::pin(cmd_restore(&name, &conn)).await?,
        Commands::Subscribe {
            pattern, entity, group, mode, heartbeat_interval, conn, format,
        } => {
            Box::pin(cmd_subscribe(pattern, entity, group, mode, heartbeat_interval, conn, format))
                .await?;
        }
        Commands::ConsumerGroup { action } => dispatch_consumer_group(action).await?,
        Commands::Db { action } => dispatch_db(action).await?,
        Commands::Dev { action } => dispatch_dev(action).await?,
        Commands::Bench { action } => dispatch_bench(action).await?,
    }
    Ok(())
}

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
            ownership,
            event_scope,
            passphrase_file,
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
                ownership,
                event_scope,
                passphrase_file,
            })
            .await
        }
        AgentAction::Status { conn } => Box::pin(cmd_agent_status(conn)).await,
    }
}

async fn dispatch_cluster(action: ClusterAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ClusterAction::Start(fields) => {
            Box::pin(cmd_cluster_start(ClusterStartArgs {
                node_id: fields.node_id,
                node_name: fields.node_name,
                bind: fields.bind,
                db_path: fields.db,
                peers: fields.peers,
                auth: *fields.auth,
                quic_cert: fields.quic_cert,
                quic_key: fields.quic_key,
                quic_ca: fields.quic_ca,
                no_quic: fields.no_quic,
                no_persist_stores: fields.no_persist_stores,
                durability: fields.durability,
                durability_ms: fields.durability_ms,
                bridge_out: fields.bridge_out,
                cluster_port_offset: fields.cluster_port_offset,
                #[cfg(feature = "dev-insecure")]
                quic_insecure: fields.quic_insecure,
                ws_bind: fields.ws_bind,
                oauth: *fields.oauth,
                ownership: fields.ownership,
                event_scope: fields.event_scope,
                passphrase_file: fields.passphrase_file,
            }))
            .await
        }
        ClusterAction::Rebalance { conn } => Box::pin(cmd_cluster_rebalance(conn)).await,
        ClusterAction::Status { conn } => Box::pin(cmd_cluster_status(conn)).await,
    }
}

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

async fn dispatch_index(action: IndexAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        IndexAction::Add {
            entity,
            fields,
            conn,
        } => Box::pin(cmd_index_add(entity, fields, conn)).await,
    }
}

async fn dispatch_backup(action: BackupAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        BackupAction::Create { name, conn } => Box::pin(cmd_backup_create(&name, &conn)).await,
        BackupAction::List { conn } => Box::pin(cmd_backup_list(&conn)).await,
    }
}

async fn dispatch_consumer_group(
    action: ConsumerGroupAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ConsumerGroupAction::List { conn, format } => {
            Box::pin(commands::consumer::cmd_consumer_group_list(conn, format)).await
        }
        ConsumerGroupAction::Show { name, conn, format } => {
            Box::pin(commands::consumer::cmd_consumer_group_show(
                name, conn, format,
            ))
            .await
        }
    }
}

async fn dispatch_db(action: DbAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        DbAction::Create {
            partition,
            entity,
            data,
            conn,
        } => {
            Box::pin(commands::consumer::cmd_db_create(
                partition, entity, data, conn,
            ))
            .await
        }
        DbAction::Read {
            partition,
            entity,
            id,
            conn,
        } => Box::pin(commands::consumer::cmd_db_read(partition, entity, id, conn)).await,
        DbAction::Update {
            partition,
            entity,
            id,
            data,
            conn,
        } => {
            Box::pin(commands::consumer::cmd_db_update(
                partition, entity, id, data, conn,
            ))
            .await
        }
        DbAction::Delete {
            partition,
            entity,
            id,
            conn,
        } => {
            Box::pin(commands::consumer::cmd_db_delete(
                partition, entity, id, conn,
            ))
            .await
        }
    }
}

async fn dispatch_dev(action: DevAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        DevAction::Ps => commands::dev::cmd_dev_ps()?,
        DevAction::Kill { all, node, agent } => commands::dev::cmd_dev_kill(all, node, agent),
        DevAction::Clean { db_prefix } => commands::dev::cmd_dev_clean(&db_prefix)?,
        DevAction::Logs {
            node,
            pattern,
            follow,
            last,
            db_prefix,
        } => commands::dev::cmd_dev_logs(node, pattern.as_deref(), follow, last, &db_prefix)?,
        DevAction::Test {
            pubsub,
            db,
            constraints,
            wildcards,
            retained,
            lwt,
            ownership,
            stress_constraints,
            all,
            nodes,
        } => commands::dev::cmd_dev_test(
            pubsub,
            db,
            constraints,
            wildcards,
            retained,
            lwt,
            ownership,
            stress_constraints,
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
            passwd,
            ownership,
        } => commands::dev::cmd_dev_start_cluster(
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
            passwd.as_deref(),
            ownership.as_deref(),
        )?,
        DevAction::Bench {
            scenario,
            output,
            baseline,
            db,
        } => {
            Box::pin(commands::dev_bench::cmd_dev_bench(
                scenario, output, baseline, &db,
            ))
            .await?;
        }
        DevAction::Profile {
            scenario,
            tool,
            duration,
            output,
            db,
        } => commands::dev_bench::cmd_dev_profile(&scenario, &tool, duration, output, &db)?,
        DevAction::Baseline { action } => commands::dev_bench::cmd_dev_baseline(action)?,
    }
    Ok(())
}

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
