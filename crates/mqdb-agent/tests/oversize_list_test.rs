// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::broker_defaults::BROKER_MAX_PACKET_SIZE;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use running_agent::RunningAgent;
use serde_json::{Value, json};
use std::time::Duration;
use tempfile::TempDir;

const RECORD_PAD: usize = 50 * 1024;

async fn start_agent(password: &str) -> RunningAgent {
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let hash = PasswordAuthProvider::hash_password(password).unwrap();
    std::fs::write(&password_file, format!("alice:{hash}\n")).unwrap();
    let db = Database::open_without_background_tasks(tmp.path().join("agent"))
        .await
        .unwrap();
    let agent = MqdbAgent::new(db).with_password_file(password_file);
    RunningAgent::start(agent, next_test_port(), tmp).await
}

async fn connect(port: u16, password: &str) -> MqttClient {
    let client_id = format!("alice-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(client_id.clone());
    let options = ConnectOptions::new(client_id).with_credentials("alice", password);
    Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
        .await
        .unwrap();
    client
}

async fn request(client: &MqttClient, topic: &str, payload: &Value) -> Option<Value> {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .unwrap();
    let options = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic),
            ..Default::default()
        },
        ..Default::default()
    };
    client
        .publish_with_options(topic, serde_json::to_vec(payload).unwrap(), options)
        .await
        .unwrap();
    let payload = tokio::time::timeout(Duration::from_secs(10), rx.recv_async())
        .await
        .ok()?
        .ok()?;
    Some(serde_json::from_slice(&payload).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_too_large_for_one_packet_is_answered_with_a_size_refusal() {
    let password = uuid::Uuid::new_v4().to_string();
    let agent = start_agent(&password).await;
    let client = connect(agent.port, &password).await;

    let records = BROKER_MAX_PACKET_SIZE / RECORD_PAD + 2;
    for index in 0..records {
        let created = request(
            &client,
            "$DB/bigdocs/create",
            &json!({"id": format!("r{index:05}"), "v": index, "pad": "x".repeat(RECORD_PAD)}),
        )
        .await
        .expect("no create response");
        assert_eq!(created["status"], "ok", "{created}");
    }

    let listed = request(&client, "$DB/bigdocs/list", &json!({}))
        .await
        .expect("the oversized list got no answer");
    assert_eq!(listed["code"], 413, "{}", listed["message"]);

    let projected = request(&client, "$DB/bigdocs/list", &json!({"projection": ["v"]}))
        .await
        .expect("the projected list got no answer");
    assert_eq!(
        projected["data"].as_array().map(Vec::len),
        Some(records),
        "{}",
        projected["status"]
    );

    agent.stop().await;
}

fn answered(reply: Option<Value>, topic: &str) -> Value {
    reply.unwrap_or_else(|| panic!("no answer on {topic}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r385_an_applied_update_is_not_reported_as_an_error() {
    let password = uuid::Uuid::new_v4().to_string();
    let agent = start_agent(&password).await;
    let client = connect(agent.port, &password).await;
    let part = 3 * 1024 * 1024;

    let created = request(
        &client,
        "$DB/grow/create",
        &json!({"id": "k", "a": "x".repeat(part)}),
    )
    .await;
    let mut updated = None;
    for field in ["b", "c", "d"] {
        updated = request(
            &client,
            "$DB/grow/k/update",
            &json!({field: "y".repeat(part)}),
        )
        .await;
    }
    let has_b = request(
        &client,
        "$DB/grow/list",
        &json!({"filters": [{"field": "d", "op": "is_not_null"}], "projection": ["id"]}),
    )
    .await;
    agent.stop().await;

    let created = answered(created, "$DB/grow/create");
    let updated = answered(updated, "$DB/grow/k/update");
    let has_b = answered(has_b, "$DB/grow/list");
    assert_eq!(created["status"], "ok", "create: {}", created["message"]);
    let applied = has_b["data"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty());
    assert!(
        !(updated["status"] == "error" && applied),
        "the update was applied (field d stored) but the client was told status=error code={}: {}",
        updated["code"],
        updated["message"]
    );
    assert!(applied, "{has_b}");
    assert_eq!(updated["data"]["id"], "k", "{updated}");
}
