// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod common;
#[path = "../../mqdb-agent/tests/running_agent/mod.rs"]
mod running_agent;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use running_agent::RunningAgent;
use serde_json::Value;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::process::Command;

async fn open_db() -> (TempDir, Database) {
    let dir = TempDir::new().expect("agent database dir");
    let db = Database::open(dir.path())
        .await
        .expect("open agent database");
    (dir, db)
}

async fn start_agent_background(port: u16) -> RunningAgent {
    let (dir, db) = open_db().await;
    RunningAgent::start(MqdbAgent::new(db).with_anonymous(true), port, dir).await
}

async fn start_agent_with_ownership(port: u16, passwd: &std::path::Path) -> RunningAgent {
    let (dir, db) = open_db().await;
    let ownership = mqdb_core::types::OwnershipConfig::parse("diagrams=userId")
        .expect("valid ownership config");
    let agent = MqdbAgent::new(db)
        .with_password_file(passwd.to_path_buf())
        .with_ownership_config(ownership);
    RunningAgent::start(agent, port, dir).await
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

fn parse_json(stdout: &str, stderr: &str) -> Value {
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "JSON parse failed: {e}\nstdout ({} bytes): {stdout:?}\nstderr ({} bytes): {stderr:?}",
            stdout.len(),
            stderr.len(),
        )
    })
}

#[tokio::test]
async fn test_cli_share_lifecycle() {
    let port = next_test_port();
    let pw_dir = TempDir::new().expect("password file dir");
    let pw_path = pw_dir.path().join("passwd");
    let pw_str = pw_path.to_str().expect("utf-8 password file path");
    for (user, pass) in [("alice", "alice"), ("bob", "bob")] {
        let (added, out, err) = run_mqdb(&["passwd", user, "-b", pass, "-f", pw_str]).await;
        assert!(added, "passwd {user} should succeed: {out}{err}");
    }
    let agent = start_agent_with_ownership(port, &pw_path).await;
    let broker = format!("127.0.0.1:{port}");

    let (created, create_out, create_err) = run_mqdb(&[
        "create",
        "diagrams",
        "-d",
        r#"{"userId":"alice","title":"D1"}"#,
        "--broker",
        &broker,
        "--user",
        "alice",
        "--pass",
        "alice",
        "--format",
        "json",
    ])
    .await;
    assert!(created, "create should succeed: {create_out}{create_err}");
    let id = parse_json(&create_out, &create_err)["data"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    let (before, _, _) = run_mqdb(&[
        "read", "diagrams", &id, "--broker", &broker, "--user", "bob", "--pass", "bob", "--format",
        "json",
    ])
    .await;
    assert!(!before, "bob must not read before a grant");

    let (shared_ok, share_out, share_err) = run_mqdb(&[
        "share",
        "diagrams",
        &id,
        "bob",
        "--permission",
        "view",
        "--broker",
        &broker,
        "--user",
        "alice",
        "--pass",
        "alice",
        "--format",
        "json",
    ])
    .await;
    assert!(shared_ok, "share should succeed: {share_out}{share_err}");
    assert_eq!(parse_json(&share_out, &share_err)["status"], "ok");

    let (after, read_out, _) = run_mqdb(&[
        "read", "diagrams", &id, "--broker", &broker, "--user", "bob", "--pass", "bob", "--format",
        "json",
    ])
    .await;
    assert!(after, "bob must read after a view grant");
    assert_eq!(parse_json(&read_out, "")["data"]["title"], "D1");

    let (_, shares_out, _) = run_mqdb(&[
        "shares", "diagrams", &id, "--broker", &broker, "--user", "alice", "--pass", "alice",
        "--format", "json",
    ])
    .await;
    let grants = parse_json(&shares_out, "")["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(grants.len(), 1, "one grant listed: {shares_out}");
    assert_eq!(grants[0]["grantee"], "bob");
    assert_eq!(grants[0]["permission"], "view");

    let (_, shared_list_out, _) = run_mqdb(&[
        "shared", "diagrams", "--broker", &broker, "--user", "bob", "--pass", "bob", "--format",
        "json",
    ])
    .await;
    let resources = parse_json(&shared_list_out, "")["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        resources.len(),
        1,
        "bob sees one shared resource: {shared_list_out}"
    );
    assert_eq!(
        resources[0]["title"], "D1",
        "shared returns the hydrated resource, not the grant row"
    );

    let (revoked, _, _) = run_mqdb(&[
        "unshare", "diagrams", &id, "bob", "--broker", &broker, "--user", "alice", "--pass",
        "alice", "--format", "json",
    ])
    .await;
    assert!(revoked, "unshare should succeed");

    let (after_revoke, _, _) = run_mqdb(&[
        "read", "diagrams", &id, "--broker", &broker, "--user", "bob", "--pass", "bob", "--format",
        "json",
    ])
    .await;
    assert!(!after_revoke, "bob must not read after revoke");

    agent.stop().await;
}

#[tokio::test]
async fn test_cli_create_and_read() {
    let port = next_test_port();
    let agent = start_agent_background(port).await;

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

    let created = parse_json(&stdout, &stderr);
    let id = created
        .get("data")
        .and_then(|d| d.get("id"))
        .and_then(|v| v.as_str())
        .expect("should have data.id");

    let (success, stdout, stderr) = run_mqdb(&[
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

    let read = parse_json(&stdout, &stderr);
    let data = read.get("data").expect("should have data");
    assert_eq!(data.get("name").and_then(|v| v.as_str()), Some("Alice"));

    agent.stop().await;
}

#[tokio::test]
async fn test_cli_list() {
    let port = next_test_port();
    let agent = start_agent_background(port).await;

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

    let (success, stdout, stderr) = run_mqdb(&[
        "list",
        "users",
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;

    assert!(success, "list should succeed");

    let list = parse_json(&stdout, &stderr);
    let data = list.get("data").expect("should have data");
    let items = data.as_array().expect("data should be array");
    assert_eq!(items.len(), 3);

    agent.stop().await;
}

#[tokio::test]
async fn test_cli_update_and_delete() {
    let port = next_test_port();
    let agent = start_agent_background(port).await;

    let (success, stdout, stderr) = run_mqdb(&[
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

    let created = parse_json(&stdout, &stderr);
    let id = created
        .get("data")
        .and_then(|d| d.get("id"))
        .and_then(|v| v.as_str())
        .expect("should have data.id");

    let (success, stdout, stderr) = run_mqdb(&[
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

    let updated = parse_json(&stdout, &stderr);
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

    let (found, stdout, stderr) = run_mqdb(&[
        "read",
        "users",
        id,
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--format",
        "json",
    ])
    .await;
    assert!(!found, "read after delete should fail");
    let response = parse_json(&stdout, &stderr);
    assert_eq!(
        response.get("status").and_then(|v| v.as_str()),
        Some("error"),
        "read after delete should return error status"
    );

    agent.stop().await;
}

#[tokio::test]
async fn test_cli_connect_timeout_against_silent_listener() {
    use std::net::TcpListener;
    use std::sync::mpsc;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent listener");
    let port = listener
        .local_addr()
        .expect("silent listener address")
        .port();

    let silent_hold = Duration::from_secs(30);
    let (accepted_tx, accepted_rx) = mpsc::channel();
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            accepted_tx.send(Instant::now()).ok();
            std::thread::sleep(silent_hold);
            drop(stream);
        }
    });
    let min_wall = Duration::from_secs(1);
    let max_wall = Duration::from_secs(4);

    let (success, stdout, stderr) = run_mqdb(&[
        "list",
        "users",
        "--broker",
        &format!("127.0.0.1:{port}"),
        "--user",
        "x",
        "--pass",
        "y",
        "--timeout",
        "2",
    ])
    .await;
    let elapsed = accepted_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|err| {
            panic!(
                "the CLI must connect to the silent listener ({err}): stdout={stdout}, \
                 stderr={stderr}"
            )
        })
        .elapsed();

    assert!(
        !success,
        "command must fail when broker never sends CONNACK: stdout={stdout}, stderr={stderr}",
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("timed out"),
        "error message must mention timeout: stdout={stdout}, stderr={stderr}",
    );
    assert!(
        elapsed >= min_wall,
        "the 2s connect timeout must not abort within {min_wall:?} of the connection being \
         accepted; {elapsed:?} means --timeout fired early",
    );
    assert!(
        elapsed < max_wall,
        "the 2s connect timeout must abort within {max_wall:?} of the connection being \
         accepted (far below the {silent_hold:?} silent hold); {elapsed:?} means --timeout \
         was ignored or regressed",
    );
}

#[tokio::test]
async fn test_cli_bench_db() {
    let port = next_test_port();
    let agent = start_agent_background(port).await;

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

    agent.stop().await;
}
