use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

fn wait_for_cluster_ready(nodes: u8, timeout_secs: u64) -> bool {
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
pub(crate) fn cmd_dev_test(
    pubsub: bool,
    db: bool,
    constraints: bool,
    wildcards: bool,
    retained: bool,
    lwt: bool,
    ownership: bool,
    all: bool,
    nodes: u8,
) {
    let run_all =
        all || (!pubsub && !db && !constraints && !wildcards && !retained && !lwt && !ownership);

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

    if ownership {
        run_test_ownership(nodes, &ports);
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
            "--user",
            "admin",
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
            "--user",
            "admin",
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
            "--user",
            "admin",
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

    std::thread::sleep(std::time::Duration::from_millis(1000));

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

fn wait_for_auth_cluster(nodes: u8, timeout_secs: u64) -> bool {
    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

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
                    "-u",
                    "admin",
                    "-P",
                    "admin",
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
            return true;
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    false
}

#[allow(clippy::too_many_lines)]
fn run_test_ownership(nodes: u8, _ports: &[u16]) {
    println!("=== Ownership Enforcement Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0;
    let mut failed = 0;

    let passwd_path = "/tmp/mqdb-test-ownership-passwd";
    let db_prefix = "/tmp/mqdb-test-own";

    println!("  Setting up authenticated cluster with ownership...");

    let _ = std::fs::remove_file(passwd_path);
    for (user, pass) in [("alice", "alice"), ("bob", "bob"), ("admin", "admin")] {
        let _ = Command::new(&exe)
            .args(["passwd", user, "-b", pass, "-f", passwd_path])
            .output();
    }

    let ts = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let entity = format!("test_owned_{ts}");
    let ownership_spec = format!("{entity}=userId");

    let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
    std::thread::sleep(Duration::from_secs(1));

    for i in 1..=nodes {
        let _ = std::fs::remove_dir_all(format!("{db_prefix}-{i}"));
    }

    let quic_cert = PathBuf::from("test_certs/server.pem");
    let quic_key = PathBuf::from("test_certs/server.key");

    for node_id in 1..=nodes {
        let port = 1882 + u16::from(node_id);
        let db_path = format!("{db_prefix}-{node_id}");
        let _ = std::fs::create_dir_all(&db_path);

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
            "--admin-users",
            "admin",
            "--passwd",
            passwd_path,
            "--ownership",
            &ownership_spec,
        ]);

        let peers: Vec<String> = (1..node_id)
            .map(|n| format!("{}@127.0.0.1:{}", n, 1882 + u16::from(n)))
            .collect();
        if !peers.is_empty() {
            cmd.args(["--peers", &peers.join(",")]);
        }

        if quic_cert.exists() && quic_key.exists() {
            cmd.args([
                "--quic-cert",
                quic_cert.to_str().unwrap_or(""),
                "--quic-key",
                quic_key.to_str().unwrap_or(""),
            ]);
            #[cfg(feature = "dev-insecure")]
            cmd.arg("--quic-insecure");
        }

        cmd.env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        );

        if let Ok(log_file) = std::fs::File::create(format!("{db_path}/mqdb.log")) {
            if let Ok(log_clone) = log_file.try_clone() {
                cmd.stdout(log_clone);
            }
            cmd.stderr(log_file);
        }

        println!("  Starting node {node_id} on port {port} (auth + ownership)...");
        let _ = cmd.spawn();
        std::thread::sleep(Duration::from_millis(500));
    }

    println!("  Waiting for authenticated cluster...");
    if !wait_for_auth_cluster(nodes, 15) {
        println!("  Cluster failed to become ready");
        let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
        println!("\nResults: {passed} passed, {failed} failed\n");
        return;
    }
    println!("  Authenticated cluster ready!\n");

    let broker = "127.0.0.1:1883";

    let create_output = Command::new(&exe)
        .args([
            "create",
            &entity,
            "-d",
            r#"{"userId": "alice", "title": "Alice Doc"}"#,
            "--broker",
            broker,
            "--user",
            "alice",
            "--pass",
            "alice",
            "--format",
            "json",
        ])
        .output();

    let id = create_output
        .as_ref()
        .ok()
        .and_then(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            serde_json::from_str::<serde_json::Value>(&stdout).ok()
        })
        .and_then(|v| v["id"].as_str().map(String::from));

    if id.is_some() {
        println!("  Create as alice: ✓");
        passed += 1;
    } else {
        println!("  Create as alice: ✗");
        if let Ok(o) = &create_output {
            eprintln!("    stdout: {}", String::from_utf8_lossy(&o.stdout));
            eprintln!("    stderr: {}", String::from_utf8_lossy(&o.stderr));
        }
        failed += 1;
        let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
        println!("\nResults: {passed} passed, {failed} failed\n");
        return;
    }

    let id = id.unwrap();
    std::thread::sleep(Duration::from_millis(500));

    let list_alice = Command::new(&exe)
        .args([
            "list", &entity, "--broker", broker, "--user", "alice", "--pass", "alice", "--format",
            "json",
        ])
        .output();

    let alice_count = list_alice
        .as_ref()
        .ok()
        .and_then(|o| {
            serde_json::from_str::<serde_json::Value>(&String::from_utf8_lossy(&o.stdout)).ok()
        })
        .and_then(|v| v["data"].as_array().map(Vec::len))
        .unwrap_or(0);

    if alice_count == 1 {
        println!("  List as alice (sees own doc): ✓");
        passed += 1;
    } else {
        println!("  List as alice (sees own doc): ✗ (got {alice_count})");
        failed += 1;
    }

    let list_bob = Command::new(&exe)
        .args([
            "list", &entity, "--broker", broker, "--user", "bob", "--pass", "bob", "--format",
            "json",
        ])
        .output();

    let bob_count = list_bob
        .as_ref()
        .ok()
        .and_then(|o| {
            serde_json::from_str::<serde_json::Value>(&String::from_utf8_lossy(&o.stdout)).ok()
        })
        .and_then(|v| v["data"].as_array().map(Vec::len))
        .unwrap_or(0);

    if bob_count == 0 {
        println!("  List as bob (sees nothing): ✓");
        passed += 1;
    } else {
        println!("  List as bob (sees nothing): ✗ (got {bob_count})");
        failed += 1;
    }

    let update_bob = Command::new(&exe)
        .args([
            "update",
            &entity,
            &id,
            "-d",
            r#"{"title": "Stolen"}"#,
            "--broker",
            broker,
            "--user",
            "bob",
            "--pass",
            "bob",
            "--format",
            "json",
        ])
        .output();

    let bob_update_blocked = update_bob.as_ref().ok().is_some_and(|o| {
        let stdout = String::from_utf8_lossy(&o.stdout).to_lowercase();
        !o.status.success() || stdout.contains("forbidden") || stdout.contains("error")
    });

    if bob_update_blocked {
        println!("  Update as bob (forbidden): ✓");
        passed += 1;
    } else {
        println!("  Update as bob (forbidden): ✗");
        if let Ok(o) = &update_bob {
            eprintln!("    stdout: {}", String::from_utf8_lossy(&o.stdout));
        }
        failed += 1;
    }

    let delete_bob = Command::new(&exe)
        .args([
            "delete", &entity, &id, "--broker", broker, "--user", "bob", "--pass", "bob",
            "--format", "json",
        ])
        .output();

    let bob_delete_blocked = delete_bob.as_ref().ok().is_some_and(|o| {
        let stdout = String::from_utf8_lossy(&o.stdout).to_lowercase();
        !o.status.success() || stdout.contains("forbidden") || stdout.contains("error")
    });

    if bob_delete_blocked {
        println!("  Delete as bob (forbidden): ✓");
        passed += 1;
    } else {
        println!("  Delete as bob (forbidden): ✗");
        failed += 1;
    }

    let update_alice = Command::new(&exe)
        .args([
            "update",
            &entity,
            &id,
            "-d",
            r#"{"title": "Updated by Alice"}"#,
            "--broker",
            broker,
            "--user",
            "alice",
            "--pass",
            "alice",
            "--format",
            "json",
        ])
        .output();

    let alice_update_ok = update_alice
        .as_ref()
        .ok()
        .is_some_and(|o| o.status.success());

    if alice_update_ok {
        println!("  Update as alice (owner): ✓");
        passed += 1;
    } else {
        println!("  Update as alice (owner): ✗");
        if let Ok(o) = &update_alice {
            eprintln!("    stdout: {}", String::from_utf8_lossy(&o.stdout));
            eprintln!("    stderr: {}", String::from_utf8_lossy(&o.stderr));
        }
        failed += 1;
    }

    let delete_alice = Command::new(&exe)
        .args([
            "delete", &entity, &id, "--broker", broker, "--user", "alice", "--pass", "alice",
            "--format", "json",
        ])
        .output();

    let alice_delete_ok = delete_alice
        .as_ref()
        .ok()
        .is_some_and(|o| o.status.success());

    if alice_delete_ok {
        println!("  Delete as alice (owner): ✓");
        passed += 1;
    } else {
        println!("  Delete as alice (owner): ✗");
        failed += 1;
    }

    let _ = Command::new("pkill").args(["-f", "mqdb cluster"]).status();
    let _ = std::fs::remove_file(passwd_path);

    println!("\nResults: {passed} passed, {failed} failed\n");
}
