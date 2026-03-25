// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::process::Command;

use super::helpers::cmd_dev_clean;

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

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
pub(crate) fn cmd_dev_start_cluster(
    nodes: u8,
    clean: bool,
    quic_cert: &Path,
    quic_key: &Path,
    quic_ca: &Path,
    no_quic: bool,
    db_prefix: &str,
    bind_host: &str,
    topology: Option<&str>,
    bridge_out: bool,
    no_bridge_out: bool,
    passwd: Option<&Path>,
    ownership: Option<&str>,
    license: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if clean {
        println!("Cleaning existing databases...");
        let _ = cmd_dev_clean(db_prefix);
    }

    let exe = std::env::current_exe()?;

    let generated_passwd = if passwd.is_none() {
        Some(ensure_dev_passwd(&exe, db_prefix)?)
    } else {
        None
    };
    let passwd_path = passwd.unwrap_or_else(|| generated_passwd.as_deref().expect("just created"));

    let topology_name = topology.unwrap_or("partial");
    println!("Using {topology_name} mesh topology (bind: {bind_host})");

    for node_id in 1..=nodes {
        let port = 1882 + u16::from(node_id);
        let db_path = format!("{db_prefix}-{node_id}");

        let passwd_str = passwd_path.to_str().expect("db_prefix is valid UTF-8");

        let mut cmd = Command::new(&exe);
        cmd.args([
            "cluster",
            "start",
            "--node-id",
            &node_id.to_string(),
            "--bind",
            &format!("{bind_host}:{port}"),
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
            cmd.args(["--license", lic_path.to_str().unwrap_or("")]);
        }

        let peers: Vec<String> = match topology_name {
            "full" => (1..=nodes)
                .filter(|&n| n != node_id)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
            "upper" => ((node_id + 1)..=nodes)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
            _ => (1..node_id)
                .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
                .collect(),
        };
        if !peers.is_empty() {
            cmd.args(["--peers", &peers.join(",")]);
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

        cmd.spawn()?;

        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    println!("\nCluster started with {nodes} nodes");
    println!("Use 'mqdb dev ps' to check status");
    println!("Use 'mqdb dev kill' to stop");

    Ok(())
}
