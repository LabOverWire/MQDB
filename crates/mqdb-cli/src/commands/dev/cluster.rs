// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::process::Command;

use super::helpers::{NODE_PID_FILE, absolute_prefix, cmd_dev_clean, refuse_if_in_use};

fn ensure_dev_passwd(exe: &Path, db_prefix: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = format!("{db_prefix}-passwd");
    match std::fs::remove_file(&path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(format!("remove stale {path}: {e}").into());
        }
        Ok(()) | Err(_) => {}
    }
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

pub(crate) const TRANSPORT_PORT_OFFSET: u8 = 50;

fn check_port_block(base_port: u16, nodes: u8) -> Result<(), String> {
    if nodes == 0 || nodes > TRANSPORT_PORT_OFFSET {
        return Err(format!(
            "--nodes must be between 1 and {TRANSPORT_PORT_OFFSET}, got {nodes}"
        ));
    }
    let last_port = u32::from(base_port) + u32::from(TRANSPORT_PORT_OFFSET) + u32::from(nodes) - 1;
    if last_port > u32::from(u16::MAX) {
        return Err(format!(
            "--base-port {base_port} with {nodes} nodes needs ports up to {last_port}, past {}",
            u16::MAX
        ));
    }
    Ok(())
}

fn node_port(base_port: u16, node_id: u8) -> u16 {
    base_port + u16::from(node_id) - 1
}

fn node_bind_args(base_port: u16, node_id: u8, bind_host: &str) -> [String; 4] {
    [
        "--bind".to_string(),
        format!("{bind_host}:{}", node_port(base_port, node_id)),
        "--cluster-port-offset".to_string(),
        TRANSPORT_PORT_OFFSET.to_string(),
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

pub(crate) enum BridgeDirection {
    TopologyDefault,
    Out,
    Both,
}

pub(crate) struct StartClusterOptions {
    pub(crate) nodes: u8,
    pub(crate) clean: bool,
    pub(crate) quic_cert: PathBuf,
    pub(crate) quic_key: PathBuf,
    pub(crate) quic_ca: PathBuf,
    pub(crate) no_quic: bool,
    pub(crate) db_prefix: String,
    pub(crate) base_port: u16,
    pub(crate) bind_host: String,
    pub(crate) topology: Option<String>,
    pub(crate) bridge: BridgeDirection,
    pub(crate) passwd: Option<PathBuf>,
    pub(crate) ownership: Option<String>,
    pub(crate) license: Option<PathBuf>,
}

impl StartClusterOptions {
    fn topology_name(&self) -> &str {
        self.topology
            .as_deref()
            .unwrap_or(if self.no_quic { "partial" } else { "full" })
    }

    fn bridge_out(&self) -> bool {
        match self.bridge {
            BridgeDirection::Out => true,
            BridgeDirection::Both => false,
            BridgeDirection::TopologyDefault => self.topology_name() == "full",
        }
    }
}

fn utf8_path<'a>(path: &'a Path, what: &str) -> Result<&'a str, String> {
    path.to_str()
        .ok_or_else(|| format!("{what} path {} is not valid UTF-8", path.display()))
}

fn node_command(
    exe: &Path,
    options: &StartClusterOptions,
    node_id: u8,
    db_path: &str,
    passwd: &str,
) -> Result<Command, Box<dyn std::error::Error>> {
    let mut cmd = Command::new(exe);
    cmd.args([
        "cluster",
        "start",
        "--node-id",
        &node_id.to_string(),
        "--db",
        db_path,
        "--admin-users",
        "admin",
        "--passwd",
        passwd,
    ]);
    if let Some(ownership_spec) = &options.ownership {
        cmd.args(["--ownership", ownership_spec]);
    }
    if let Some(license) = &options.license {
        cmd.args(["--license", utf8_path(license, "license")?]);
    }
    cmd.args(node_bind_args(
        options.base_port,
        node_id,
        &options.bind_host,
    ));
    if let Some(peers) = node_peers(
        options.base_port,
        options.nodes,
        node_id,
        options.topology_name(),
    ) {
        cmd.args(["--peers", &peers]);
    }
    if options.bridge_out() {
        cmd.arg("--bridge-out");
    }
    if !options.no_quic && options.quic_cert.exists() && options.quic_key.exists() {
        cmd.args([
            "--quic-cert",
            utf8_path(&options.quic_cert, "--quic-cert")?,
            "--quic-key",
            utf8_path(&options.quic_key, "--quic-key")?,
        ]);
        if options.quic_ca.exists() {
            cmd.args(["--quic-ca", utf8_path(&options.quic_ca, "--quic-ca")?]);
        }
        #[cfg(feature = "dev-insecure")]
        cmd.arg("--quic-insecure");
    }
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    cmd.env("RUST_LOG", &rust_log);
    Ok(cmd)
}

pub(crate) fn cmd_dev_start_cluster(
    options: &StartClusterOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let nodes = options.nodes;
    check_port_block(options.base_port, nodes)?;
    let db_prefix = absolute_prefix(&options.db_prefix)?;

    if options.clean {
        println!("Cleaning existing databases...");
        cmd_dev_clean(&db_prefix)?;
    } else {
        let generated_passwd = PathBuf::from(format!("{db_prefix}-passwd"));
        refuse_if_in_use(
            &db_prefix,
            options
                .passwd
                .is_none()
                .then_some(generated_passwd.as_path()),
        )?;
    }

    let exe = std::env::current_exe()?;
    let passwd_path = match &options.passwd {
        Some(path) => path.clone(),
        None => ensure_dev_passwd(&exe, &db_prefix)?,
    };
    let passwd = utf8_path(&passwd_path, "passwd")?;

    println!(
        "Using {} mesh topology (bind: {})",
        options.topology_name(),
        options.bind_host
    );

    for node_id in 1..=nodes {
        let port = node_port(options.base_port, node_id);
        let db_path = format!("{db_prefix}-{node_id}");
        let mut cmd = node_command(&exe, options, node_id, &db_path, passwd)?;

        std::fs::create_dir_all(&db_path)?;
        let pid_path = format!("{db_path}/{NODE_PID_FILE}");
        let mut pid_file = std::fs::File::create(&pid_path)
            .map_err(|e| format!("record node {node_id} in {pid_path}: {e}"))?;
        let log_file = std::fs::File::create(format!("{db_path}/mqdb.log"))?;
        cmd.stdout(log_file.try_clone()?);
        cmd.stderr(log_file);

        let transport_mode = if options.no_quic { " (TCP)" } else { " (QUIC)" };
        println!("Starting node {node_id} on port {port}{transport_mode}...");

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                drop(pid_file);
                std::fs::remove_file(&pid_path).map_err(|remove| {
                    format!("start node {node_id}: {e}; remove {pid_path}: {remove}")
                })?;
                return Err(format!("start node {node_id}: {e}").into());
            }
        };
        if let Err(e) = std::io::Write::write_all(&mut pid_file, child.id().to_string().as_bytes())
        {
            let stopped = child.kill().and_then(|()| child.wait());
            return Err(match stopped {
                Ok(_) => format!("record node {node_id} in {pid_path}: {e}; stopped it"),
                Err(stop) => format!(
                    "record node {node_id} in {pid_path}: {e}; could not stop pid {}: {stop}",
                    child.id()
                ),
            }
            .into());
        }

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
            node_bind_args(32000, 2, "127.0.0.1"),
            ["--bind", "127.0.0.1:32001", "--cluster-port-offset", "50"]
        );
        assert_eq!(
            node_bind_args(32000, 1, "0.0.0.0"),
            ["--bind", "0.0.0.0:32000", "--cluster-port-offset", "50"]
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
    fn no_accepted_cluster_size_puts_a_node_on_a_transport_port() {
        let ports = |node_id: u8| {
            let [_, bind, _, offset] = node_bind_args(32000, node_id, "127.0.0.1");
            let bind_port: u16 = bind
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse().ok())
                .expect("bind port");
            let offset: u16 = offset.parse().expect("offset");
            (bind_port, bind_port + offset)
        };
        for nodes in (1..=u8::MAX).filter(|&nodes| check_port_block(32000, nodes).is_ok()) {
            for added in 1..=nodes {
                for existing in 1..=nodes {
                    assert_ne!(
                        ports(added).0,
                        ports(existing).1,
                        "with {nodes} nodes, node {added} binds node {existing}'s transport port"
                    );
                }
            }
        }
    }

    #[test]
    fn the_port_block_must_fit_below_65536() {
        assert!(check_port_block(65485, 1).is_ok());
        assert!(check_port_block(65485, 2).is_err());
        assert!(check_port_block(32000, 50).is_ok());
        assert!(check_port_block(32000, 51).is_err());
        assert!(check_port_block(32000, 0).is_err());
    }
}
