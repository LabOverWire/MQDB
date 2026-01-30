use std::process::Command;

use super::helpers::cmd_dev_clean;

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
pub(crate) fn cmd_dev_start_cluster(
    nodes: u8,
    clean: bool,
    quic_cert: &std::path::Path,
    quic_key: &std::path::Path,
    quic_ca: &std::path::Path,
    no_quic: bool,
    db_prefix: &str,
    bind_host: &str,
    topology: Option<&str>,
    bridge_out: bool,
    no_bridge_out: bool,
    passwd: Option<&std::path::Path>,
    ownership: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if clean {
        println!("Cleaning existing databases...");
        let _ = cmd_dev_clean(db_prefix);
    }

    let exe = std::env::current_exe()?;

    let topology_name = topology.unwrap_or("partial");
    println!("Using {topology_name} mesh topology (bind: {bind_host})");

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
            &format!("{bind_host}:{port}"),
            "--db",
            &db_path,
            "--admin-users",
            "admin",
        ]);

        if let Some(passwd_path) = passwd {
            if let Some(s) = passwd_path.to_str() {
                cmd.args(["--passwd", s]);
            }
        }

        if let Some(ownership_spec) = ownership {
            cmd.args(["--ownership", ownership_spec]);
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
