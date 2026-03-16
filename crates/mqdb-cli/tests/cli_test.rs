// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Command;

async fn start_agent_background(port: u16) -> (TempDir, tokio::task::JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true);

    let handle = tokio::spawn(async move {
        let _ = agent.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    (tmp, handle)
}

fn mqdb_bin() -> String {
    env!("CARGO_BIN_EXE_mqdb").to_string()
}

async fn run_mqdb(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(mqdb_bin())
        .args(args)
        .output()
        .await
        .expect("failed to execute mqdb");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    (output.status.success(), stdout, stderr)
}

#[tokio::test]
async fn test_cli_create_and_read() {
    let port = next_test_port();
    let (_tmp, handle) = start_agent_background(port).await;

    let (success, stdout, stderr) = run_mqdb(&[
        "create",
        "users",
        "-d",
        r#"{"name":"Alice","email":"alice@example.com"}"#,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;

    assert!(
        success,
        "create should succeed: stderr={stderr}, stdout={stdout}"
    );

    let created: Value = serde_json::from_str(&stdout).expect("should parse JSON output");
    let id = created
        .get("data")
        .and_then(|d| d.get("id"))
        .and_then(|v| v.as_str())
        .expect("should have data.id");

    let (success, stdout, _stderr) = run_mqdb(&[
        "read",
        "users",
        id,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;

    assert!(success, "read should succeed");

    let read: Value = serde_json::from_str(&stdout).expect("should parse JSON output");
    let data = read.get("data").expect("should have data");
    assert_eq!(data.get("name").and_then(|v| v.as_str()), Some("Alice"));

    handle.abort();
}

#[tokio::test]
async fn test_cli_list() {
    let port = next_test_port();
    let (_tmp, handle) = start_agent_background(port).await;

    for i in 1..=3 {
        let data = format!(r#"{{"name":"User{i}","status":"active"}}"#);
        let (success, _, stderr) = run_mqdb(&[
            "create",
            "users",
            "-d",
            &data,
            "--broker",
            &format!("127.0.0.1:{port}"),
        ])
        .await;
        assert!(success, "create should succeed: {stderr}");
    }

    let (success, stdout, _stderr) = run_mqdb(&[
        "list",
        "users",
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;

    assert!(success, "list should succeed");

    let list: Value = serde_json::from_str(&stdout).expect("should parse JSON output");
    let data = list.get("data").expect("should have data");
    let items = data.as_array().expect("data should be array");
    assert_eq!(items.len(), 3);

    handle.abort();
}

#[tokio::test]
async fn test_cli_update_and_delete() {
    let port = next_test_port();
    let (_tmp, handle) = start_agent_background(port).await;

    let (success, stdout, _stderr) = run_mqdb(&[
        "create",
        "users",
        "-d",
        r#"{"name":"Bob"}"#,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;
    assert!(success, "create should succeed");

    let created: Value = serde_json::from_str(&stdout).expect("should parse JSON");
    let id = created
        .get("data")
        .and_then(|d| d.get("id"))
        .and_then(|v| v.as_str())
        .unwrap();

    let (success, stdout, _stderr) = run_mqdb(&[
        "update",
        "users",
        id,
        "-d",
        r#"{"name":"Bob Smith"}"#,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;
    assert!(success, "update should succeed");

    let updated: Value = serde_json::from_str(&stdout).expect("should parse JSON");
    let data = updated.get("data").expect("should have data");
    assert_eq!(data.get("name").and_then(|v| v.as_str()), Some("Bob Smith"));

    let (success, _, _) = run_mqdb(&[
        "delete",
        "users",
        id,
        "--broker",
        &format!("127.0.0.1:{port}"),
    ])
    .await;
    assert!(success, "delete should succeed");

    let (_success, stdout, _stderr) = run_mqdb(&[
        "read",
        "users",
        id,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;
    let response: Value = serde_json::from_str(&stdout).expect("should parse JSON");
    assert_eq!(
        response.get("status").and_then(|v| v.as_str()),
        Some("error"),
        "read after delete should return error status"
    );

    handle.abort();
}

#[tokio::test]
async fn test_cli_bench_db() {
    let port = next_test_port();
    let (_tmp, handle) = start_agent_background(port).await;

    let (success, stdout, stderr) = run_mqdb(&[
        "bench",
        "db",
        "--operations",
        "10",
        "--broker",
        &format!("127.0.0.1:{port}"),
    ])
    .await;

    assert!(
        success,
        "bench db should succeed: stdout={stdout}, stderr={stderr}"
    );
    assert!(
        stdout.contains("ops/s") || stdout.contains("operations"),
        "output should contain benchmark stats"
    );

    handle.abort();
}
