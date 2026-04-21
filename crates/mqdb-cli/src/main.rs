// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod cli_types;
mod commands;
mod common;
mod license;

use cli_types::{
    AclAction, AgentAction, BackupAction, BenchAction, Cli, Commands, ConstraintAction,
    ConsumerGroupAction, IndexAction, LicenseAction, SchemaAction,
};
#[cfg(feature = "cluster")]
use cli_types::{ClusterAction, DbAction, DevAction};
use commands::agent::{AgentStartArgs, cmd_agent_start, cmd_agent_status};
use commands::bench::{
    BenchDbArgs, BenchPubsubArgs, cmd_bench_db, cmd_bench_db_cascade, cmd_bench_db_changefeed,
    cmd_bench_db_unique, cmd_bench_pubsub,
};
#[cfg(feature = "cluster")]
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
    #[cfg(feature = "cluster")]
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let cli = Cli::parse();
    dispatch_command(cli.command).await
}

fn init_default_tracing() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

async fn dispatch_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if !matches!(
        &command,
        Commands::Agent {
            action: AgentAction::Start(_)
        }
    ) {
        init_default_tracing();
    }

    match command {
        Commands::Agent { action } => dispatch_agent(action).await?,
        #[cfg(feature = "cluster")]
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
        Commands::Schema { action } => dispatch_schema(action).await?,
        Commands::Constraint { action } => dispatch_constraint(action).await?,
        Commands::Index { action } => dispatch_index(action).await?,
        Commands::Backup { action } => dispatch_backup(action).await?,
        Commands::Restore { name, conn } => Box::pin(cmd_restore(&name, &conn)).await?,
        Commands::ConsumerGroup { action } => dispatch_consumer_group(action).await?,
        #[cfg(feature = "cluster")]
        Commands::Db { action } => dispatch_db(action).await?,
        #[cfg(feature = "cluster")]
        Commands::Dev { action } => dispatch_dev(action).await?,
        Commands::Bench { action } => dispatch_bench(action).await?,
        Commands::License { action } => dispatch_license(action)?,
        crud @ (Commands::Create { .. }
        | Commands::Read { .. }
        | Commands::Update { .. }
        | Commands::Delete { .. }
        | Commands::List { .. }
        | Commands::Watch { .. }
        | Commands::Subscribe { .. }) => dispatch_crud(crud).await?,
    }
    Ok(())
}

async fn dispatch_crud(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
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
            projection,
            conn,
            format,
        } => {
            let params = ListParams {
                entity,
                filters: filter,
                sort,
                limit,
                offset,
                projection,
            };
            Box::pin(cmd_list(params, conn, format)).await?;
        }
        Commands::Watch {
            entity,
            filter,
            conn,
            format,
        } => Box::pin(cmd_watch(entity, filter, conn, format)).await?,
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
        _ => unreachable!("caller ensures only CRUD variants reach dispatch_crud"),
    }
    Ok(())
}

async fn dispatch_agent(action: AgentAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        AgentAction::Start(fields) => {
            cmd_agent_start(AgentStartArgs {
                bind: fields.bind,
                db_path: fields.db,
                memory_backend: fields.memory_backend,
                auth: *fields.auth,
                durability: fields.durability,
                durability_ms: fields.durability_ms,
                quic_cert: fields.quic_cert,
                quic_key: fields.quic_key,
                quic_cert_data: fields.quic_cert_data,
                quic_key_data: fields.quic_key_data,
                ws_bind: fields.ws_bind,
                oauth: *fields.oauth,
                ownership: fields.ownership,
                event_scope: fields.event_scope,
                passphrase_file: fields.passphrase_file,
                passphrase_data: fields.passphrase_data,
                license: fields.license,
                license_data: fields.license_data,
                vault_min_passphrase_length: fields.vault_min_passphrase_length,
                otlp_endpoint: fields.otlp_endpoint,
                otel_service_name: fields.otel_service_name,
                otel_sampling_ratio: fields.otel_sampling_ratio,
            })
            .await
        }
        AgentAction::Status { conn } => Box::pin(cmd_agent_status(conn)).await,
    }
}

#[cfg(feature = "cluster")]
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
                quic_cert_data: fields.quic_cert_data,
                quic_key_data: fields.quic_key_data,
                quic_ca_data: fields.quic_ca_data,
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
                passphrase_data: fields.passphrase_data,
                license: fields.license,
                license_data: fields.license_data,
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

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
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
            license,
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
            license.as_deref(),
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
            license,
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
            license.as_deref(),
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

fn dispatch_license(action: LicenseAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LicenseAction::Verify { license: path } => {
            let info = license::verify_license_file(&path)
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            println!("Customer:   {}", info.customer);
            println!("Tier:       {}", info.tier);
            println!("Features:   {}", info.features);
            println!("Trial:      {}", info.trial);
            println!("Expires at: {} (unix)", info.expires_at);
            println!("Days left:  {}", info.days_remaining());
            println!("Status:     valid");
        }
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
            write_rate,
            attempts_per_client,
            children,
            runs,
            conn,
            format,
        } => {
            let args = BenchDbArgs {
                operations,
                entity,
                op: op.clone(),
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
            };
            match op.to_lowercase().as_str() {
                "changefeed" => {
                    let duration_secs = duration.unwrap_or(30);
                    Box::pin(cmd_bench_db_changefeed(args, write_rate, duration_secs)).await
                }
                "unique" => Box::pin(cmd_bench_db_unique(args, attempts_per_client)).await,
                "cascade" => Box::pin(cmd_bench_db_cascade(args, children, runs)).await,
                _ => Box::pin(cmd_bench_db(args)).await,
            }
        }
    }
}
