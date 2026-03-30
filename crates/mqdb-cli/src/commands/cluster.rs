// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use mqdb_cluster::{ClusterConfig, ClusteredAgent, PeerConfig};
use serde_json::json;

use super::agent::{build_auth_setup_config, build_http_config};
use crate::cli_types::{AuthArgs, ConnectionArgs, DurabilityArg, OAuthArgs, OutputFormat};
use crate::commands::env_secret::{
    resolve_federated_jwt_content, resolve_passphrase, resolve_path_or_data,
};
use crate::common::{execute_request, output_response};

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ClusterStartArgs {
    pub(crate) node_id: u16,
    pub(crate) node_name: Option<String>,
    pub(crate) bind: SocketAddr,
    pub(crate) db_path: PathBuf,
    pub(crate) peers: Vec<String>,
    pub(crate) auth: AuthArgs,
    pub(crate) quic_cert: Option<PathBuf>,
    pub(crate) quic_key: Option<PathBuf>,
    pub(crate) quic_ca: Option<PathBuf>,
    pub(crate) quic_cert_data: Option<String>,
    pub(crate) quic_key_data: Option<String>,
    pub(crate) quic_ca_data: Option<String>,
    pub(crate) no_quic: bool,
    pub(crate) no_persist_stores: bool,
    pub(crate) durability: DurabilityArg,
    pub(crate) durability_ms: u64,
    pub(crate) bridge_out: bool,
    pub(crate) cluster_port_offset: u16,
    #[cfg(feature = "dev-insecure")]
    pub(crate) quic_insecure: bool,
    pub(crate) ws_bind: Option<SocketAddr>,
    pub(crate) oauth: OAuthArgs,
    pub(crate) ownership: Option<String>,
    pub(crate) event_scope: Option<String>,
    pub(crate) passphrase_file: Option<PathBuf>,
    pub(crate) passphrase_data: Option<String>,
    pub(crate) license: Option<PathBuf>,
    pub(crate) license_data: Option<String>,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn cmd_cluster_start(
    args: ClusterStartArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqdb_core::config::DurabilityMode;

    let license_info = if args.license_data.is_some() || args.license.is_some() {
        let result = if let Some(ref content) = args.license_data {
            crate::license::verify_license_token(content.trim())
        } else {
            crate::license::verify_license_file(args.license.as_ref().unwrap())
        };
        match result {
            Ok(info) => {
                tracing::info!(
                    customer = %info.customer,
                    tier = %info.tier,
                    trial = info.trial,
                    days_remaining = info.days_remaining(),
                    "license validated"
                );
                Some(info)
            }
            Err(e) => {
                return Err(format!("license validation failed: {e}").into());
            }
        }
    } else {
        None
    };

    let passphrase = resolve_passphrase(
        args.passphrase_file.as_ref(),
        args.passphrase_data.as_deref(),
    )?;

    let needs_vault = passphrase.is_some();
    crate::license::enforce_license(license_info.as_ref(), needs_vault, true)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let peer_configs = parse_peer_configs(&args.peers)?;

    let stores_durability = match args.durability {
        DurabilityArg::Immediate => DurabilityMode::Immediate,
        DurabilityArg::Periodic => DurabilityMode::PeriodicMs(args.durability_ms),
        DurabilityArg::None => DurabilityMode::None,
    };

    let passwd_file = resolve_path_or_data(
        args.auth.passwd.clone(),
        args.auth.passwd_data.as_deref(),
        "cluster_passwd",
    )?;
    let acl_file = resolve_path_or_data(
        args.auth.acl.clone(),
        args.auth.acl_data.as_deref(),
        "cluster_acl",
    )?;
    let federated_content = resolve_federated_jwt_content(
        args.auth.federated_jwt_config_data.as_deref(),
        args.auth.federated_jwt_config.as_ref(),
    );
    let auth_setup = build_auth_setup_config(&args.auth, federated_content.as_deref())?;

    let db_path = args.db_path;
    let mut config = ClusterConfig::new(args.node_id, db_path.clone(), peer_configs);
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

    let quic_cert = resolve_path_or_data(
        args.quic_cert,
        args.quic_cert_data.as_deref(),
        "cluster_quic_cert.pem",
    )?;
    let quic_key = resolve_path_or_data(
        args.quic_key,
        args.quic_key_data.as_deref(),
        "cluster_quic_key.pem",
    )?;
    let quic_ca = resolve_path_or_data(
        args.quic_ca,
        args.quic_ca_data.as_deref(),
        "cluster_quic_ca.pem",
    )?;

    if let (Some(cert), Some(key)) = (quic_cert, quic_key) {
        config = config.with_quic_certs(cert, key);
    }
    if let Some(ca) = quic_ca {
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
    let ownership_arc = if let Some(ref ownership_spec) = args.ownership {
        let admin_set = args.auth.admin_users.iter().cloned().collect();
        let ownership = mqdb_core::types::OwnershipConfig::parse(ownership_spec)
            .map_err(|e| format!("invalid --ownership: {e}"))?
            .with_admin_users(admin_set);
        std::sync::Arc::new(ownership)
    } else {
        std::sync::Arc::new(mqdb_core::types::OwnershipConfig::default())
    };
    if let Some(http_bind) = args.oauth.http_bind {
        let http_config = build_http_config(
            http_bind,
            &args.auth,
            &args.oauth,
            federated_content.as_deref(),
            ownership_arc.clone(),
            &db_path,
        )?;
        config = config.with_http_config(http_config);
    }
    if args.ownership.is_some() {
        config = config.with_ownership((*ownership_arc).clone());
    }
    if let Some(event_scope_spec) = args.event_scope {
        let scope_config = mqdb_core::types::ScopeConfig::parse(&event_scope_spec)
            .map_err(|e| format!("invalid --event-scope: {e}"))?;
        config = config.with_scope_config(scope_config);
    }
    if let Some(ref info) = license_info {
        config = config.with_license_expiry(info.expires_at);
    }
    if let Some(ref pp) = passphrase {
        config = config.with_passphrase(pp.clone());
    }

    let mut agent = ClusteredAgent::new(config)?;
    Box::pin(agent.run()).await.map_err(|e| e.to_string())?;

    Ok(())
}

pub(crate) async fn cmd_cluster_rebalance(
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
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

pub(crate) async fn cmd_cluster_status(
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
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
        let mut primary_counts: HashMap<u64, usize> = HashMap::new();
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

pub(crate) fn parse_peer_configs(
    peers: &[String],
) -> Result<Vec<PeerConfig>, Box<dyn std::error::Error>> {
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
