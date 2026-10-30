// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::process::Command;

use super::helpers::{NODE_PID_FILE, absolute_prefix, cmd_dev_clean};

fn ensure_dev_passwd(exe: &Path, db_prefix: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = format!("{db_prefix}-passwd");
    let _ = std::fs::remove_file(&path);
    let output = Command::new(exe)
        .args(["passwd", "admin", "-b", "admin", "-f", &path])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to create dev passwd file: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(PathBuf::from(path))
}

fn check_port_block(base_port: u16, nodes: u8) -> Result<(), String> {
    let block_end = u32::from(base_port) + 2 * u32::from(nodes);
    if block_end > u32::from(u16::MAX) + 1 {
        return Err(format!(
            "--base-port {base_port} with {nodes} nodes needs ports up to {}, past {}",
            block_end - 1,
            u16::MAX
        ));
    }
    Ok(())
}

fn node_port(base_port: u16, node_id: u8) -> u16 {
    base_port + u16::from(node_id) - 1
}

fn node_bind_args(base_port: u16, nodes: u8, node_id: u8, bind_host: &str) -> [String; 4] {
    [
        "--bind".to_string(),
        format!("{bind_host}:{}", node_port(base_port, node_id)),
        "--cluster-port-offset".to_string(),
        nodes.to_string(),
    ]
}

fn node_peers(base_port: u16, nodes: u8, node_id: u8, topology: &str) -> Option<String> {
    let is_peer = |n: u8| match topology {
        "full" => n != node_id,
        "upper" => n > node_id,
        _ => n < node_id,
    };
    let peers = (1..=nodes)
        .filter(|&n| is_peer(n))
        .map(|n| format!("{n}@127.0.0.1:{}", node_port(base_port, n)))
        .fold(String::new(), |list, peer| {
            if list.is_empty() {
                peer
            } else {
                format!("{list},{peer}")
            }
        });
    (!peers.is_empty()).then_some(peers)
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
pub(crate) fn cmd_dev_start_cluster(
    nodes: u8,
    clean: bool,
    quic_cert: &Path,
    quic_key: &Path,
    quic_ca: &Path,
    no_quic: bool,
    db_prefix: &str,
    base_port: u16,
    bind_host: &str,
    topology: Option<&str>,
    bridge_out: bool,
    no_bridge_out: bool,
    passwd: Option<&Path>,
    ownership: Option<&str>,
    license: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    check_port_block(base_port, nodes)?;
    let prefix = absolute_prefix(db_prefix)?;
    let db_prefix = prefix.as_str();

    if clean {
        println!("Cleaning existing databases...");
        cmd_dev_clean(db_prefix)?;
    }

    let exe = std::env::current_exe()?;

    let passwd_path = match passwd {
        Some(path) => path.to_path_buf(),
        None => ensure_dev_passwd(&exe, db_prefix)?,
    };
    let passwd_str = passwd_path
        .to_str()
        .ok_or("passwd path is not valid UTF-8")?;

    let topology_name = topology.unwrap_or(if no_quic { "partial" } else { "full" });
    println!("Using {topology_name} mesh topology (bind: {bind_host})");

    for node_id in 1..=nodes {
        let port = node_port(base_port, node_id);
        let db_path = format!("{db_prefix}-{node_id}");

        let mut cmd = Command::new(&exe);
        cmd.args([
            "cluster",
            "start",
            "--node-id",
            &node_id.to_string(),
            "--db",
            &db_path,
            "--admin-users",
            "admin",
            "--passwd",
            passwd_str,
        ]);

        if let Some(ownership_spec) = ownership {
            cmd.args(["--ownership", ownership_spec]);
        }

        if let Some(lic_path) = license {
            let lic_str = lic_path.to_str().ok_or("license path is not valid UTF-8")?;
            cmd.args(["--license", lic_str]);
        }

        cmd.args(node_bind_args(base_port, nodes, node_id, bind_host));
        if let Some(peers) = node_peers(base_port, nodes, node_id, topology_name) {
            cmd.args(["--peers", &peers]);
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

        let child = cmd.spawn()?;
        std::fs::write(format!("{db_path}/{NODE_PID_FILE}"), child.id().to_string())?;

        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    println!("\nCluster started with {nodes} nodes");
    println!("Use 'mqdb dev ps' to check status");
    println!("Use 'mqdb dev kill --db-prefix {db_prefix}' to stop");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_node_and_peer_port_follows_the_base_port() {
        assert_eq!(
            node_bind_args(32000, 3, 2, "127.0.0.1"),
            ["--bind", "127.0.0.1:32001", "--cluster-port-offset", "3"]
        );
        assert_eq!(
            node_bind_args(32000, 3, 1, "0.0.0.0"),
            ["--bind", "0.0.0.0:32000", "--cluster-port-offset", "3"]
        );
        assert_eq!(
            node_peers(32000, 3, 2, "full").as_deref(),
            Some("1@127.0.0.1:32000,3@127.0.0.1:32002")
        );
        assert_eq!(
            node_peers(32000, 3, 1, "upper").as_deref(),
            Some("2@127.0.0.1:32001,3@127.0.0.1:32002")
        );
        assert_eq!(
            node_peers(32000, 3, 3, "partial").as_deref(),
            Some("1@127.0.0.1:32000,2@127.0.0.1:32001")
        );
        assert_eq!(node_peers(32000, 3, 1, "partial"), None);
        assert_eq!(node_peers(32000, 3, 3, "upper"), None);
    }

    #[test]
    fn the_port_block_must_fit_below_65536() {
        assert!(check_port_block(65530, 3).is_ok());
        assert!(check_port_block(65531, 3).is_err());
        assert!(check_port_block(u16::MAX, 1).is_err());
    }
}
