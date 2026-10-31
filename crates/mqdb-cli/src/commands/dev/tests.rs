// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use super::cluster::{
    BridgeDirection, StartClusterOptions, TRANSPORT_PORT_OFFSET, cmd_dev_start_cluster,
};
use super::helpers::{StartedNode, absolute_prefix, cmd_dev_clean, stop_started_nodes};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Suite {
    Pubsub,
    Db,
    Constraints,
    Wildcards,
    Retained,
    Lwt,
    Events,
    Ownership,
    Sharing,
    Presence,
    StressConstraints,
}

impl Suite {
    fn runs_by_default(self) -> bool {
        matches!(
            self,
            Self::Pubsub
                | Self::Db
                | Self::Constraints
                | Self::Wildcards
                | Self::Retained
                | Self::Lwt
                | Self::Events
        )
    }

    fn starts_own_cluster(self) -> bool {
        matches!(self, Self::Ownership | Self::Sharing | Self::Presence)
    }
}

pub(crate) struct DevTestOptions {
    pub(crate) nodes: u8,
    pub(crate) base_port: u16,
    pub(crate) db_prefix: String,
    pub(crate) license: Option<PathBuf>,
}

fn node_ports(base_port: u16, nodes: u8) -> Result<Vec<u16>, String> {
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
    Ok((0..u16::from(nodes)).map(|i| base_port + i).collect())
}

fn node_accepts_admin(port: u16) -> bool {
    Command::new("timeout")
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
        .output()
        .is_ok_and(|output| output.status.success())
}

fn wait_for_cluster_ready(ports: &[u16], timeout_secs: u64) -> bool {
    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    println!("Waiting for cluster to be ready ({} nodes)...", ports.len());

    while start.elapsed() < timeout {
        if ports.iter().all(|&port| node_accepts_admin(port)) {
            let settled = wait_for_partitions_settled(ports, timeout_secs);
            if settled {
                println!("Cluster ready!");
            } else {
                println!("Warning: running tests while partitions may still be moving");
            }
            return settled;
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    println!("Warning: cluster readiness check timed out");
    false
}

pub(crate) fn cmd_dev_test(
    requested: &[(Suite, bool)],
    all: bool,
    options: &DevTestOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let ports = node_ports(options.base_port, options.nodes)?;
    let run_defaults = all || requested.iter().all(|&(_, wanted)| !wanted);
    let selected = requested
        .iter()
        .filter(|&&(suite, wanted)| wanted || (run_defaults && suite.runs_by_default()))
        .map(|&(suite, _)| suite);

    let tests_running_cluster = selected.clone().any(|suite| !suite.starts_own_cluster());
    if tests_running_cluster && selected.clone().any(Suite::starts_own_cluster) {
        return Err("--ownership, --sharing and --presence start their own cluster on the --base-port block, so they cannot run together with suites that test a running cluster there; run them in a separate dev test".into());
    }
    if tests_running_cluster {
        wait_for_cluster_ready(&ports, 10);
    }

    let nodes = options.nodes;
    let failed: u32 = selected
        .map(|suite| match suite {
            Suite::Pubsub => run_test_pubsub(nodes, &ports),
            Suite::Db => run_test_db(nodes, &ports),
            Suite::Constraints => run_test_constraints(nodes, &ports),
            Suite::Wildcards => run_test_wildcards(nodes, &ports),
            Suite::Retained => run_test_retained(nodes, &ports),
            Suite::Lwt => run_test_lwt(nodes, &ports),
            Suite::Events => run_test_events(nodes, &ports),
            Suite::Ownership => run_test_ownership(options, &ports),
            Suite::Sharing => run_test_sharing(options, &ports),
            Suite::Presence => run_test_presence(options, &ports),
            Suite::StressConstraints => run_test_stress_constraints(nodes, &ports),
        })
        .sum();
    if failed == 0 {
        Ok(())
    } else {
        Err(format!("{failed} dev test check(s) failed").into())
    }
}

fn run_test_pubsub(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node Pub/Sub Matrix Test ({nodes} nodes) ===\n");

    let mut passed = 0u32;
    let mut failed = 0u32;

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

    report(passed, failed)
}

fn run_test_db(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node DB CRUD Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;

    for (idx, &port) in ports.iter().enumerate() {
        let node = idx + 1;

        let data = format!(r#"{{"name": "User{node}", "node": {node}}}"#);
        let create_output = mqdb_cmd(&exe, &["create", "test_users", "-d", &data], port);

        let created = create_output.is_ok_and(|o| o.status.success());

        if created {
            println!("  Create via Node {node}: ✓");
            passed += 1;
        } else {
            println!("  Create via Node {node}: ✗");
            failed += 1;
        }

        let read_port = ports[(idx + 1) % ports.len()];
        let read_node = ((idx + 1) % ports.len()) + 1;

        let filter = format!("node={node}");
        let list_output = mqdb_cmd(&exe, &["list", "test_users", "-f", &filter], read_port);

        let can_read = list_output.is_ok_and(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.contains(&format!("User{node}"))
        });

        if can_read {
            println!("  Read from Node {read_node} (created on {node}): ✓");
            passed += 1;
        } else {
            println!("  Read from Node {read_node} (created on {node}): ✗");
            failed += 1;
        }

        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    report(passed, failed)
}

fn mqdb_cmd(
    exe: &std::path::Path,
    args: &[&str],
    port: u16,
) -> std::io::Result<std::process::Output> {
    let broker = format!("127.0.0.1:{port}");
    let mut full_args: Vec<&str> = args.to_vec();
    full_args.extend_from_slice(&["--broker", &broker, "--user", "admin", "--pass", "admin"]);
    Command::new(exe).args(&full_args).output()
}

fn run_test_constraints(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Unique Constraint Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    let entity = format!("test_products_{ts}");
    let constraint_name = format!("unique_{entity}_sku");

    let add_output = mqdb_cmd(
        &exe,
        &[
            "constraint",
            "add",
            &entity,
            "--unique",
            "sku",
            "--name",
            &constraint_name,
        ],
        ports[0],
    );

    let constraint_added = add_output.as_ref().is_ok_and(|o| {
        let stdout = String::from_utf8_lossy(&o.stdout);
        stdout.contains("constraint added") || stdout.contains("\"status\":\"ok\"")
    });

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
        return report(passed, failed);
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    let create1 = mqdb_cmd(
        &exe,
        &[
            "create",
            &entity,
            "-d",
            r#"{"name": "Widget A", "sku": "SKU-001"}"#,
        ],
        ports[0],
    );

    let first_created = create1.is_ok_and(|o| o.status.success());

    if first_created {
        println!("  Create first product (SKU-001) via Node 1: ✓");
        passed += 1;
    } else {
        println!("  Create first product (SKU-001) via Node 1: ✗");
        failed += 1;
    }

    std::thread::sleep(std::time::Duration::from_millis(300));

    let create_dup = mqdb_cmd(
        &exe,
        &[
            "create",
            &entity,
            "-d",
            r#"{"name": "Duplicate Widget", "sku": "SKU-001"}"#,
        ],
        ports[0],
    );

    let dup_rejected = create_dup.is_ok_and(|o| {
        let stdout = String::from_utf8_lossy(&o.stdout);
        let stderr = String::from_utf8_lossy(&o.stderr);
        let combined = format!("{stdout}{stderr}").to_lowercase();
        !o.status.success()
            || combined.contains("unique")
            || combined.contains("conflict")
            || combined.contains("duplicate")
            || combined.contains("constraint")
    });

    if dup_rejected {
        println!("  Duplicate SKU-001 rejected: ✓");
        passed += 1;
    } else {
        println!("  Duplicate SKU-001 rejected: ✗");
        failed += 1;
    }

    report(passed, failed)
}

fn run_test_wildcards(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node Wildcard Subscription Test ({nodes} nodes) ===\n");

    let mut passed = 0u32;
    let mut failed = 0u32;

    let sub_port = ports[0];
    let pub_port = ports[ports.len() - 1];
    let src_node = ports.len();
    let dst_node = 1;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    let sub_id = format!("wild_sub_{ts}");
    let pub_id = format!("wild_pub_{ts}");

    let single_level = Command::new("timeout")
        .args([
            "5",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_id}_single' -h 127.0.0.1 -p {sub_port} -u admin -P admin -t 'sensors/+/temp' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_id}_single' -h 127.0.0.1 -p {pub_port} -u admin -P admin -t 'sensors/room1/temp' -m 'single_level_ok'; wait"
            ),
        ])
        .output();

    if single_level.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains("single_level_ok")) {
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
                "mosquitto_sub -i '{sub_id}_multi' -h 127.0.0.1 -p {sub_port} -u admin -P admin -t 'home/#' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_id}_multi' -h 127.0.0.1 -p {pub_port} -u admin -P admin -t 'home/kitchen/oven' -m 'multi_level_ok'; wait"
            ),
        ])
        .output();

    if multi_level.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains("multi_level_ok")) {
        println!("  Multi-level (#) Node {src_node} → Node {dst_node}: ✓");
        passed += 1;
    } else {
        println!("  Multi-level (#) Node {src_node} → Node {dst_node}: ✗");
        failed += 1;
    }

    report(passed, failed)
}

fn run_test_retained(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node Retained Message Test ({nodes} nodes) ===\n");

    let mut passed = 0u32;
    let mut failed = 0u32;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
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
            "-u",
            "admin",
            "-P",
            "admin",
            "-t",
            &topic,
            "-m",
            &msg,
            "-r",
            "-q",
            "1",
        ])
        .output();

    std::thread::sleep(std::time::Duration::from_secs(1));

    let sub_output = Command::new("timeout")
        .args([
            "3",
            "mosquitto_sub",
            "-h",
            "127.0.0.1",
            "-p",
            &sub_port.to_string(),
            "-u",
            "admin",
            "-P",
            "admin",
            "-t",
            &topic,
            "-C",
            "1",
        ])
        .output();

    let received = sub_output.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains(&msg));

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
            "-u",
            "admin",
            "-P",
            "admin",
            "-t",
            &topic,
            "-m",
            "",
            "-r",
        ])
        .output();

    report(passed, failed)
}

fn run_test_lwt(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node Last Will & Testament Test ({nodes} nodes) ===\n");

    let mut passed = 0u32;
    let mut failed = 0u32;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
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
                "mosquitto_sub -i '{sub_id}' -h 127.0.0.1 -p {sub_port} -u admin -P admin -t '{will_topic}' -C 1 & SUB_PID=$!; sleep 2; mosquitto_sub -i '{client_id}' -h 127.0.0.1 -p {connect_port} -u admin -P admin -t 'dummy/topic' --will-topic '{will_topic}' --will-payload '{will_msg}' --will-qos 1 & sleep 2; kill -9 $!; wait $SUB_PID"
            ),
        ])
        .output();

    let received = lwt_output.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains(&will_msg));

    if received {
        println!("  LWT from Node 1 received on Node {}: ✓", ports.len());
        passed += 1;
    } else {
        println!("  LWT from Node 1 received on Node {}: ✗", ports.len());
        failed += 1;
    }

    report(passed, failed)
}

fn run_test_events(nodes: u8, ports: &[u16]) -> u32 {
    println!("=== Cross-Node Change Event Test ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    let entity = format!("events_test_{ts}");
    let expected = ports.len() * 3;

    let watchers: Vec<Option<std::process::Child>> = ports
        .iter()
        .map(|&port| {
            spawn_event_watcher(port, &entity, expected, &format!("{entity}-watcher-{port}"))
        })
        .collect();
    std::thread::sleep(Duration::from_secs(2));

    for (idx, &port) in ports.iter().enumerate() {
        let node = idx + 1;
        let id = format!("n{node}");
        let succeeded =
            |args: &[&str]| mqdb_cmd(&exe, args, port).is_ok_and(|o| o.status.success());
        let created = succeeded(&[
            "create",
            &entity,
            "-d",
            &format!(r#"{{"id":"{id}","n":0}}"#),
        ]);
        let updated = succeeded(&["update", &entity, &id, "-d", r#"{"n":1}"#]);
        let deleted = succeeded(&["delete", &entity, &id]);
        record(
            &mut passed,
            &mut failed,
            &format!("Create, update and delete via Node {node}"),
            created && updated && deleted,
        );
    }

    for (idx, watcher) in watchers.into_iter().enumerate() {
        let node = idx + 1;
        let events = watcher
            .and_then(|child| child.wait_with_output().ok())
            .map(|output| record_operations(&String::from_utf8_lossy(&output.stdout)))
            .unwrap_or_default();
        let in_order = (1..=ports.len()).all(|writer| {
            let id = format!("n{writer}");
            events
                .iter()
                .filter(|(event_id, _)| *event_id == id)
                .map(|(_, operation)| operation.as_str())
                .eq(["Create", "Update", "Delete"])
        });
        record(
            &mut passed,
            &mut failed,
            &format!(
                "Watcher on Node {node} got {}/{expected} events, each record in order",
                events.len()
            ),
            events.len() == expected && in_order,
        );
    }

    report(passed, failed)
}

fn spawn_event_watcher(
    port: u16,
    entity: &str,
    expected: usize,
    client_id: &str,
) -> Option<std::process::Child> {
    Command::new("mosquitto_sub")
        .args([
            "-V",
            "mqttv5",
            "-i",
            client_id,
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-u",
            "admin",
            "-P",
            "admin",
            "-q",
            "1",
            "-t",
            &format!("$DB/{entity}/events/#"),
            "-C",
            &expected.to_string(),
            "-W",
            "20",
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .ok()
}

fn record_operations(output: &str) -> Vec<(String, String)> {
    output
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|event| {
            Some((
                event.get("id")?.as_str()?.to_string(),
                event.get("operation")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

fn run_pubsub_test(pub_port: u16, sub_port: u16, topic: &str, msg: &str) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let sub_client_id = format!("sub-{sub_port}-{ts}");
    let pub_client_id = format!("pub-{pub_port}-{ts}");
    let output = Command::new("timeout")
        .args([
            "5",
            "sh",
            "-c",
            &format!(
                "mosquitto_sub -i '{sub_client_id}' -h 127.0.0.1 -p {sub_port} -u admin -P admin -t '{topic}' -C 1 & sleep 1.5; mosquitto_pub -i '{pub_client_id}' -h 127.0.0.1 -p {pub_port} -u admin -P admin -t '{topic}' -m '{msg}'; wait"
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

type PartitionAssignment = (u64, Option<u64>, Vec<u64>);

fn partition_map_on(port: u16) -> Option<Vec<PartitionAssignment>> {
    let output = Command::new("timeout")
        .args([
            "3",
            "mosquitto_rr",
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-u",
            "admin",
            "-P",
            "admin",
            "-t",
            "$SYS/mqdb/cluster/status",
            "-e",
            &format!("resp/dev-settle-{port}"),
            "-m",
            "{}",
            "-W",
            "2",
        ])
        .output()
        .ok()?;
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let data = &status["data"];
    let fully_linked = data["unlinked_nodes"].as_array().is_some_and(Vec::is_empty);
    if !fully_linked {
        return None;
    }
    let mut assignments: Vec<PartitionAssignment> = data["partitions"]
        .as_array()?
        .iter()
        .map(|p| {
            let replicas = p["replicas"]
                .as_array()
                .map(|r| r.iter().filter_map(serde_json::Value::as_u64).collect())
                .unwrap_or_default();
            (
                p["id"].as_u64().unwrap_or(0),
                p["primary"].as_u64(),
                replicas,
            )
        })
        .collect();
    assignments.sort_unstable();
    Some(assignments)
}

fn partition_map_complete(map: &[PartitionAssignment], nodes: u8) -> bool {
    map.len() == usize::from(mqdb_core::NUM_PARTITIONS)
        && map
            .iter()
            .all(|(_, primary, replicas)| primary.is_some() && (nodes < 2 || !replicas.is_empty()))
        && (1..=u64::from(nodes))
            .all(|node| map.iter().any(|(_, primary, _)| *primary == Some(node)))
}

fn wait_for_partitions_settled(ports: &[u16], timeout_secs: u64) -> bool {
    const STABLE_POLLS: u32 = 3;
    let nodes = u8::try_from(ports.len()).unwrap_or(u8::MAX);
    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs.max(30));
    let mut previous: Option<Vec<PartitionAssignment>> = None;
    let mut stable_polls = 0;

    while start.elapsed() < timeout {
        let maps: Vec<Option<Vec<PartitionAssignment>>> =
            ports.iter().map(|&port| partition_map_on(port)).collect();
        let agreed = match maps.first() {
            Some(Some(first)) if partition_map_complete(first, nodes) => maps
                .iter()
                .all(|m| m.as_ref() == Some(first))
                .then(|| first.clone()),
            _ => None,
        };
        match agreed {
            Some(map) if previous.as_ref() == Some(&map) => stable_polls += 1,
            Some(map) => {
                previous = Some(map);
                stable_polls = 1;
            }
            None => {
                previous = None;
                stable_polls = 0;
            }
        }
        if stable_polls >= STABLE_POLLS {
            println!(
                "Partition map settled on all {nodes} nodes after {:.1}s",
                start.elapsed().as_secs_f64()
            );
            return true;
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    println!(
        "Warning: partition map did not settle within {}s",
        timeout.as_secs()
    );
    false
}

fn port_in_use(port: u16) -> bool {
    let local = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    TcpStream::connect_timeout(&local, Duration::from_millis(300)).is_ok()
        || UdpSocket::bind(local).is_err()
}

fn busy_ports(ports: &[u16]) -> Vec<u16> {
    ports
        .iter()
        .flat_map(|&port| [port, port + u16::from(TRANSPORT_PORT_OFFSET)])
        .filter(|&port| port_in_use(port))
        .collect()
}

struct SuiteSetup<'a> {
    dir: &'a str,
    users: &'a [&'a str],
    ownership: Option<String>,
    presence: bool,
    label: &'a str,
}

struct SuiteCluster {
    nodes: Vec<StartedNode>,
    passwd: Option<PathBuf>,
}

impl SuiteCluster {
    fn stop(&mut self) -> Result<(), String> {
        let mut nodes = std::mem::take(&mut self.nodes);
        if let Err(e) = stop_started_nodes(&mut nodes) {
            if let Some(passwd) = self.passwd.take() {
                return Err(format!("{e}; kept {} for them", passwd.display()));
            }
            return Err(e);
        }
        match self.passwd.take().map(std::fs::remove_file) {
            Some(Err(e)) if e.kind() != ErrorKind::NotFound => {
                Err(format!("could not remove the suite passwd file: {e}"))
            }
            Some(Ok(()) | Err(_)) | None => Ok(()),
        }
    }

    fn finish(mut self) -> u32 {
        match self.stop() {
            Ok(()) => 0,
            Err(e) => {
                println!("  could not stop the suite cluster cleanly: {e}");
                1
            }
        }
    }
}

impl Drop for SuiteCluster {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            eprintln!("  could not stop the suite cluster cleanly: {e}");
        }
    }
}

fn create_suite_passwd(exe: &Path, passwd: &str, users: &[&str]) -> Result<(), String> {
    for user in users {
        let output = Command::new(exe)
            .args(["passwd", user, "-b", user, "-f", passwd])
            .output()
            .map_err(|e| format!("run mqdb passwd: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "mqdb passwd {user}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
    }
    Ok(())
}

fn start_suite_cluster(
    exe: &Path,
    options: &DevTestOptions,
    ports: &[u16],
    setup: SuiteSetup,
) -> Result<SuiteCluster, String> {
    let busy = busy_ports(ports);
    if !busy.is_empty() {
        return Err(format!(
            "ports {busy:?} are already in use; this suite starts its own cluster, so pick a free block with --base-port"
        ));
    }

    let prefix = absolute_prefix(&format!(
        "{}-{}-{}",
        options.db_prefix, setup.dir, options.base_port
    ))
    .map_err(|e| e.to_string())?;
    cmd_dev_clean(&prefix).map_err(|e| e.to_string())?;
    let passwd = format!("{prefix}-passwd");
    let mut cluster = SuiteCluster {
        nodes: Vec::new(),
        passwd: Some(PathBuf::from(&passwd)),
    };
    create_suite_passwd(exe, &passwd, setup.users)?;

    println!("  Starting {} nodes ({})...", ports.len(), setup.label);
    cluster.nodes = cmd_dev_start_cluster(&StartClusterOptions {
        nodes: options.nodes,
        clean: false,
        quic_cert: PathBuf::from("test_certs/server.pem"),
        quic_key: PathBuf::from("test_certs/server.key"),
        quic_ca: PathBuf::from("test_certs/ca.pem"),
        no_quic: false,
        db_prefix: prefix,
        base_port: options.base_port,
        bind_host: "127.0.0.1".to_string(),
        topology: Some("full".to_string()),
        bridge: BridgeDirection::TopologyDefault,
        passwd: Some(PathBuf::from(&passwd)),
        ownership: setup.ownership,
        presence: setup.presence,
        license: options.license.clone(),
    })
    .map_err(|e| e.to_string())?;

    println!("  Waiting for authenticated cluster...");
    wait_for_suite_cluster(&mut cluster.nodes, ports, 30)?;
    Ok(cluster)
}

fn exited_node(nodes: &mut [StartedNode]) -> Option<String> {
    nodes.iter_mut().find_map(|node| match node.exit_status() {
        Ok(Some(status)) => Some(format!(
            "node {} exited ({status}); see {}",
            node.node_id,
            node.dir.join("mqdb.log").display()
        )),
        Ok(None) => None,
        Err(e) => Some(e),
    })
}

fn wait_for_suite_cluster(
    nodes: &mut [StartedNode],
    ports: &[u16],
    timeout_secs: u64,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if let Some(exited) = exited_node(nodes) {
            return Err(exited);
        }
        if ports.iter().all(|&port| node_accepts_admin(port)) {
            return if wait_for_partitions_settled(ports, timeout_secs * 2) {
                Ok(())
            } else {
                Err("partition map did not settle".to_string())
            };
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!(
        "nodes did not accept connections within {timeout_secs}s; see their mqdb.log"
    ))
}

fn report(passed: u32, failed: u32) -> u32 {
    println!("\nResults: {passed} passed, {failed} failed\n");
    failed
}

fn record(passed: &mut u32, failed: &mut u32, label: &str, ok: bool) {
    if ok {
        println!("  {label}: ✓");
        *passed += 1;
    } else {
        println!("  {label}: ✗");
        *failed += 1;
    }
}

fn run_as(exe: &Path, broker: &str, user: &str, args: &[&str]) -> (Option<bool>, String) {
    match Command::new(exe)
        .args(args)
        .args([
            "--broker", broker, "--user", user, "--pass", user, "--format", "json",
        ])
        .output()
    {
        Ok(o) => (
            Some(o.status.success()),
            String::from_utf8_lossy(&o.stdout).to_string(),
        ),
        Err(e) => {
            eprintln!("    could not run {}: {e}", exe.display());
            (None, String::new())
        }
    }
}

fn data_len(out: &str) -> usize {
    serde_json::from_str::<serde_json::Value>(out)
        .ok()
        .and_then(|v| v["data"].as_array().map(Vec::len))
        .unwrap_or(0)
}

fn denied((ok, out): &(Option<bool>, String)) -> bool {
    let out = out.to_lowercase();
    match ok {
        Some(false) => true,
        Some(true) => out.contains("forbidden") || out.contains("error"),
        None => false,
    }
}

fn suite_entity(kind: &str) -> String {
    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    format!("test_{kind}_{ts}")
}

fn run_test_ownership(options: &DevTestOptions, ports: &[u16]) -> u32 {
    println!(
        "=== Ownership Enforcement Test ({} nodes) ===\n",
        options.nodes
    );

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;
    let entity = suite_entity("owned");
    let ownership_spec = format!("{entity}=userId");

    println!("  Setting up authenticated cluster with ownership...");
    let setup = SuiteSetup {
        dir: "own",
        users: &["alice", "bob", "admin"],
        ownership: Some(ownership_spec),
        presence: false,
        label: "auth + ownership",
    };
    let cluster = match start_suite_cluster(&exe, options, ports, setup) {
        Ok(cluster) => cluster,
        Err(e) => {
            println!("  Cluster failed to become ready: {e}");
            return report(passed, 1);
        }
    };
    println!("  Authenticated cluster ready!\n");

    let broker = format!("127.0.0.1:{}", ports[0]);
    let run = |user: &str, args: &[&str]| run_as(&exe, &broker, user, args);

    let (_, create_out) = run(
        "alice",
        &[
            "create",
            &entity,
            "-d",
            r#"{"userId": "alice", "title": "Alice Doc"}"#,
        ],
    );
    let id = serde_json::from_str::<serde_json::Value>(&create_out)
        .ok()
        .and_then(|v| v["data"]["id"].as_str().map(String::from));
    record(&mut passed, &mut failed, "Create as alice", id.is_some());
    let Some(id) = id else {
        eprintln!("    stdout: {create_out}");
        let stop_failures = cluster.finish();
        return report(passed, failed + stop_failures);
    };
    std::thread::sleep(Duration::from_millis(500));

    let alice_count = data_len(&run("alice", &["list", &entity]).1);
    record(
        &mut passed,
        &mut failed,
        &format!("List as alice (sees own doc, got {alice_count})"),
        alice_count == 1,
    );
    let bob_count = data_len(&run("bob", &["list", &entity]).1);
    record(
        &mut passed,
        &mut failed,
        &format!("List as bob (sees nothing, got {bob_count})"),
        bob_count == 0,
    );
    let bob_update = run(
        "bob",
        &["update", &entity, &id, "-d", r#"{"title": "Stolen"}"#],
    );
    record(
        &mut passed,
        &mut failed,
        "Update as bob (forbidden)",
        denied(&bob_update),
    );
    let bob_delete = run("bob", &["delete", &entity, &id]);
    record(
        &mut passed,
        &mut failed,
        "Delete as bob (forbidden)",
        denied(&bob_delete),
    );
    let admin_count = data_len(&run("admin", &["list", &entity]).1);
    record(
        &mut passed,
        &mut failed,
        &format!("List as admin (sees all, got {admin_count})"),
        admin_count == 1,
    );
    let (admin_update, _) = run(
        "admin",
        &[
            "update",
            &entity,
            &id,
            "-d",
            r#"{"title": "Admin Override"}"#,
        ],
    );
    record(
        &mut passed,
        &mut failed,
        "Update as admin (bypass)",
        admin_update == Some(true),
    );
    let (alice_update, _) = run(
        "alice",
        &[
            "update",
            &entity,
            &id,
            "-d",
            r#"{"title": "Updated by Alice"}"#,
        ],
    );
    record(
        &mut passed,
        &mut failed,
        "Update as alice (owner)",
        alice_update == Some(true),
    );
    let (alice_delete, _) = run("alice", &["delete", &entity, &id]);
    record(
        &mut passed,
        &mut failed,
        "Delete as alice (owner)",
        alice_delete == Some(true),
    );

    let stop_failures = cluster.finish();
    report(passed, failed + stop_failures)
}

fn sharing_checks(exe: &Path, entity: &str, ports: &[u16], passed: &mut u32, failed: &mut u32) {
    let broker = |idx: usize| format!("127.0.0.1:{}", ports[idx.min(ports.len() - 1)]);
    let (n1, n2, n3) = (broker(0), broker(1), broker(2));
    let data_array_has = |out: &str, pred: &dyn Fn(&serde_json::Value) -> bool| -> bool {
        serde_json::from_str::<serde_json::Value>(out)
            .ok()
            .and_then(|v| v["data"].as_array().map(|a| a.iter().any(pred)))
            .unwrap_or(false)
    };

    let (created, create_out) = run_as(
        exe,
        &n1,
        "alice",
        &["create", entity, "-d", r#"{"userId":"alice","title":"D1"}"#],
    );
    let id = serde_json::from_str::<serde_json::Value>(&create_out)
        .ok()
        .and_then(|v| v["data"]["id"].as_str().map(String::from));
    record(
        passed,
        failed,
        "create as alice",
        created == Some(true) && id.is_some(),
    );
    let Some(id) = id else {
        return;
    };
    let id = id.as_str();
    std::thread::sleep(Duration::from_millis(300));

    let (bob_before, _) = run_as(exe, &n2, "bob", &["read", entity, id]);
    record(
        passed,
        failed,
        "bob read before share denied (cross-node)",
        bob_before == Some(false),
    );

    let (share_view, share_out) = run_as(
        exe,
        &n1,
        "alice",
        &["share", entity, id, "bob", "--permission", "view"],
    );
    record(
        passed,
        failed,
        "alice shares view with bob",
        share_view == Some(true) && share_out.contains("shared"),
    );

    let (bob_view_read, bob_read_out) = run_as(exe, &n2, "bob", &["read", entity, id]);
    record(
        passed,
        failed,
        "bob read after view grant (cross-node)",
        bob_view_read == Some(true) && bob_read_out.contains("D1"),
    );

    let (bob_view_update, _) = run_as(
        exe,
        &n2,
        "bob",
        &["update", entity, id, "-d", r#"{"title":"X"}"#],
    );
    record(
        passed,
        failed,
        "bob update with view-only denied (cross-node)",
        bob_view_update == Some(false),
    );

    let (share_edit, _) = run_as(
        exe,
        &n1,
        "alice",
        &["share", entity, id, "bob", "--permission", "edit"],
    );
    record(
        passed,
        failed,
        "alice upgrades bob to edit",
        share_edit == Some(true),
    );

    let (bob_edit_update, _) = run_as(
        exe,
        &n3,
        "bob",
        &["update", entity, id, "-d", r#"{"title":"EditedByBob"}"#],
    );
    record(
        passed,
        failed,
        "bob update with edit grant (cross-node)",
        bob_edit_update == Some(true),
    );

    let (_, shares_out) = run_as(exe, &n1, "alice", &["shares", entity, id]);
    record(
        passed,
        failed,
        "alice shares lists bob's grant",
        data_array_has(&shares_out, &|g| g["grantee"] == "bob"),
    );

    let (_, shared_out) = run_as(exe, &n3, "bob", &["shared", entity]);
    record(
        passed,
        failed,
        "bob shared lists hydrated resource (cross-node scatter)",
        data_array_has(&shared_out, &|r| {
            r["title"] == "EditedByBob" && r.get("resource_id").is_none()
        }),
    );

    let (unshared, _) = run_as(exe, &n2, "alice", &["unshare", entity, id, "bob"]);
    record(
        passed,
        failed,
        "alice unshares bob (cross-node)",
        unshared == Some(true),
    );
    std::thread::sleep(Duration::from_millis(300));

    let (bob_after, _) = run_as(exe, &n3, "bob", &["read", entity, id]);
    record(
        passed,
        failed,
        "bob read after unshare denied (cross-node)",
        bob_after == Some(false),
    );
}

fn run_test_sharing(options: &DevTestOptions, ports: &[u16]) -> u32 {
    println!(
        "=== Cluster Diagram Sharing E2E ({} nodes) ===\n",
        options.nodes
    );

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;
    let entity = suite_entity("shared");
    let ownership_spec = format!("{entity}=userId");

    println!("  Setting up authenticated cluster with ownership...");
    let setup = SuiteSetup {
        dir: "share",
        users: &["alice", "bob", "admin"],
        ownership: Some(ownership_spec),
        presence: false,
        label: "auth + ownership",
    };
    let cluster = match start_suite_cluster(&exe, options, ports, setup) {
        Ok(cluster) => cluster,
        Err(e) => {
            println!("  Cluster failed to become ready: {e}");
            return report(passed, 1);
        }
    };
    println!("  Authenticated cluster ready!\n");

    sharing_checks(&exe, &entity, ports, &mut passed, &mut failed);

    let stop_failures = cluster.finish();
    report(passed, failed + stop_failures)
}

fn add_stress_constraint(exe: &Path, entity: &str, port: u16) -> bool {
    let constraint_name = format!("unique_{entity}_sku");
    let add_output = mqdb_cmd(
        exe,
        &[
            "constraint",
            "add",
            entity,
            "--unique",
            "sku",
            "--name",
            &constraint_name,
        ],
        port,
    );
    let added = add_output.as_ref().is_ok_and(|o| {
        let stdout = String::from_utf8_lossy(&o.stdout);
        stdout.contains("constraint added") || stdout.contains("\"status\":\"ok\"")
    });
    if !added && let Ok(o) = &add_output {
        eprintln!("    stdout: {}", String::from_utf8_lossy(&o.stdout));
        eprintln!("    stderr: {}", String::from_utf8_lossy(&o.stderr));
    }
    added
}

fn stress_unique_creates(exe: &Path, entity: &str, ports: &[u16]) -> (u32, u32) {
    let thread_count: u32 = 20;
    let creates_per_thread: u32 = 10;
    let total = thread_count * creates_per_thread;
    println!(
        "Phase 1: {total} concurrent creates with unique SKUs across {} nodes...",
        ports.len()
    );

    let handles: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let exe = exe.to_path_buf();
            let entity = entity.to_string();
            let port = ports[thread_id as usize % ports.len()];
            std::thread::spawn(move || {
                (0..creates_per_thread)
                    .map(|seq| {
                        let sku = format!("STRESS-{thread_id:03}-{seq:03}");
                        let data = format!(r#"{{"name": "Item {sku}", "sku": "{sku}"}}"#);
                        mqdb_cmd(&exe, &["create", &entity, "-d", &data], port)
                            .is_ok_and(|o| o.status.success())
                    })
                    .fold((0u32, 0u32), |(ok, err), created| {
                        if created {
                            (ok + 1, err)
                        } else {
                            (ok, err + 1)
                        }
                    })
            })
        })
        .collect();

    handles
        .into_iter()
        .filter_map(|handle| handle.join().ok())
        .fold((0, 0), |(ok, err), (s, f)| (ok + s, err + f))
}

fn stress_conflicting_creates(exe: &Path, entity: &str, ports: &[u16], sku: &str) -> (u32, u32) {
    let conflict_threads: u32 = 10;
    println!(
        "\nPhase 2: {conflict_threads} concurrent creates with same SKU across {} nodes...",
        ports.len()
    );

    let handles: Vec<_> = (0..conflict_threads)
        .map(|i| {
            let exe = exe.to_path_buf();
            let entity = entity.to_string();
            let sku = sku.to_string();
            let port = ports[i as usize % ports.len()];
            std::thread::spawn(move || {
                let data = format!(r#"{{"name": "Conflict Item {i}", "sku": "{sku}"}}"#);
                mqdb_cmd(&exe, &["create", &entity, "-d", &data], port)
                    .is_ok_and(|o| o.status.success())
            })
        })
        .collect();

    handles
        .into_iter()
        .filter_map(|handle| handle.join().ok())
        .fold((0, 0), |(ok, rejected), succeeded| {
            if succeeded {
                (ok + 1, rejected)
            } else {
                (ok, rejected + 1)
            }
        })
}

fn run_test_stress_constraints(nodes: u8, ports: &[u16]) -> u32 {
    if nodes < 2 {
        println!("=== Stress Test: Cross-Node Unique Constraints (skipped, need >= 2 nodes) ===\n");
        return 0;
    }

    println!("=== Stress Test: Cross-Node Unique Constraints ({nodes} nodes) ===\n");

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;

    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    let entity = format!("stress_products_{ts}");

    if !add_stress_constraint(&exe, &entity, ports[0]) {
        println!("  Add unique constraint: ✗ (aborting stress test)");
        return report(0, 1);
    }

    std::thread::sleep(Duration::from_millis(500));

    let (successes, failures) = stress_unique_creates(&exe, &entity, ports);
    let mark = if failures == 0 { "✓" } else { "✗" };
    println!("  Results: {successes} succeeded, {failures} false conflicts {mark}");
    record(&mut passed, &mut failed, "Unique creates", failures == 0);

    std::thread::sleep(Duration::from_millis(500));

    let conflict_sku = format!("CONFLICT-{ts}");
    let (successes, rejections) = stress_conflicting_creates(&exe, &entity, ports, &conflict_sku);
    let mark = if successes <= 1 { "✓" } else { "✗" };
    println!(
        "  Results: {successes} succeeded (expected ≤1), {rejections} correctly rejected {mark}"
    );
    record(&mut passed, &mut failed, "Same-SKU creates", successes <= 1);

    report(passed, failed)
}

fn presence_line_has(out: &str, client_id: &str, event: &str) -> bool {
    out.lines()
        .any(|line| line.contains(client_id) && line.contains(event))
}

fn shell_output(timeout_secs: &str, script: &str) -> String {
    Command::new("timeout")
        .args([timeout_secs, "sh", "-c", script])
        .output()
        .map_or_else(
            |_| String::new(),
            |o| String::from_utf8_lossy(&o.stdout).to_string(),
        )
}

fn capture_presence(sub_port: u16, holder_id: &str, holder_port: u16) -> String {
    shell_output(
        "12",
        &format!(
            "mosquitto_sub -i 'janitor-{sub_port}' -h 127.0.0.1 -p {sub_port} -u alice -P alice -t '$DB/_presence/#' -v -W 8 & \
             sleep 2; \
             mosquitto_pub -i '{holder_id}' -h 127.0.0.1 -p {holder_port} -u alice -P alice -t 'presence/noop' -m 'x'; \
             wait"
        ),
    )
}

fn presence_checks(ports: &[u16], check: &mut dyn FnMut(&str, bool)) {
    let ts = std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_millis());
    let first = ports[0];

    let same_id = format!("seat-holder-same-{ts}");
    let out = capture_presence(first, &same_id, first);
    check(
        "same-node connect presence",
        presence_line_has(&out, &same_id, "\"event\":\"connect\""),
    );
    check(
        "same-node disconnect presence",
        presence_line_has(&out, &same_id, "\"event\":\"disconnect\""),
    );

    if let Some(&second) = ports.get(1) {
        let cross_id = format!("seat-holder-cross-{ts}");
        let out = capture_presence(first, &cross_id, second);
        check(
            "cross-node connect presence (janitor n1, holder n2)",
            presence_line_has(&out, &cross_id, "\"event\":\"connect\""),
        );
        check(
            "cross-node disconnect presence (janitor n1, holder n2)",
            presence_line_has(&out, &cross_id, "\"event\":\"disconnect\""),
        );
    }

    if let (Some(&second), Some(&third)) = (ports.get(1), ports.get(2)) {
        let far_id = format!("seat-holder-far-{ts}");
        let out = capture_presence(third, &far_id, second);
        check(
            "cross-node presence (janitor n3, holder n2)",
            out.contains(&far_id),
        );
    }

    let holder = ports.get(1).copied().unwrap_or(first);
    let retained_id = format!("seat-holder-retained-{ts}");
    shell_output(
        "5",
        &format!(
            "mosquitto_pub -i '{retained_id}' -h 127.0.0.1 -p {holder} -u alice -P alice -t 'presence/noop' -m 'x'"
        ),
    );
    std::thread::sleep(Duration::from_secs(2));
    let late = shell_output(
        "5",
        &format!(
            "mosquitto_sub -i 'janitor-late' -h 127.0.0.1 -p {first} -u alice -P alice -t '$DB/_presence/#' -v -W 3"
        ),
    );
    check(
        "retained presence reaches a late janitor on another node",
        late.contains(&retained_id),
    );

    shell_output(
        "5",
        &format!(
            "mosquitto_pub -i 'attacker' -h 127.0.0.1 -p {first} -u alice -P alice -t '$DB/_presence/victim' -m '{{\"forged\":true}}'"
        ),
    );
    let victim = shell_output(
        "5",
        &format!(
            "mosquitto_sub -i 'janitor-forge' -h 127.0.0.1 -p {first} -u alice -P alice -t '$DB/_presence/victim' -v -W 3"
        ),
    );
    check("a client cannot forge presence", !victim.contains("forged"));
}

fn run_test_presence(options: &DevTestOptions, ports: &[u16]) -> u32 {
    println!(
        "=== Cluster Presence Feed E2E ({} nodes) ===\n",
        options.nodes
    );

    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mqdb"));
    let mut passed = 0u32;
    let mut failed = 0u32;

    let setup = SuiteSetup {
        dir: "presence",
        users: &["alice", "admin"],
        ownership: None,
        presence: true,
        label: "auth + presence",
    };
    let cluster = match start_suite_cluster(&exe, options, ports, setup) {
        Ok(cluster) => cluster,
        Err(e) => {
            println!("  Cluster failed to become ready: {e}");
            return report(0, 1);
        }
    };
    println!("  Authenticated cluster ready!\n");

    presence_checks(ports, &mut |name, ok| {
        record(&mut passed, &mut failed, name, ok);
    });

    let stop_failures = cluster.finish();
    report(passed, failed + stop_failures)
}

#[cfg(test)]
mod port_tests {
    use super::*;

    #[test]
    fn node_ports_follow_the_base_port() {
        assert_eq!(node_ports(33601, 3), Ok(vec![33601, 33602, 33603]));
        assert_eq!(node_ports(1883, 1), Ok(vec![1883]));
    }

    #[test]
    fn node_ports_refuse_blocks_that_overflow_or_overlap_transport_ports() {
        assert!(node_ports(1883, 0).is_err());
        assert!(node_ports(1883, 50).is_ok());
        assert!(node_ports(1883, 51).is_err());
        assert!(node_ports(65485, 1).is_ok());
        assert!(node_ports(65485, 2).is_err());
    }

    #[test]
    fn a_command_that_never_ran_is_not_counted_as_denied() {
        let never_ran = run_as(
            Path::new("/nonexistent/mqdb"),
            "127.0.0.1:1",
            "bob",
            &["delete", "x", "y"],
        );
        assert_eq!(never_ran.0, None);
        assert!(!denied(&never_ran));
        assert!(denied(&(Some(false), String::new())));
        assert!(denied(&(
            Some(true),
            r#"{"error":"forbidden"}"#.to_string()
        )));
        assert!(!denied(&(Some(true), r#"{"status":"ok"}"#.to_string())));
    }

    #[test]
    fn a_listening_port_is_reported_busy() {
        let listener =
            std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();
        assert!(port_in_use(port));
        drop(listener);
    }

    #[test]
    fn a_bound_udp_port_is_reported_busy() {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind udp");
        let port = socket.local_addr().expect("local addr").port();
        assert!(port_in_use(port));
        drop(socket);
    }
}
