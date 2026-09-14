// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

async fn start_agent_with_presence(
    port: u16,
    presence: bool,
) -> (TempDir, tokio::task::JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true)
        .with_presence(presence);
    let (handle, mut ready_rx, _shutdown) = agent.start().await.unwrap();
    let _ = ready_rx.changed().await;
    (tmp, handle)
}

async fn subscribe_to_presence(port: u16) -> (MqttClient, flume::Receiver<(String, Value)>) {
    let janitor = MqttClient::new("janitor");
    janitor
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .expect("a non-admin janitor must be able to connect");

    let (tx, rx) = flume::bounded(64);
    janitor
        .subscribe("$DB/_presence/#", move |msg| {
            if msg.topic.ends_with("/janitor") {
                return;
            }
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send((msg.topic.to_string(), value));
            }
        })
        .await
        .expect("a non-admin janitor must be allowed to subscribe to presence");

    tokio::time::sleep(Duration::from_millis(150)).await;
    (janitor, rx)
}

async fn next_presence(rx: &flume::Receiver<(String, Value)>) -> (String, Value) {
    tokio::time::timeout(Duration::from_secs(3), rx.recv_async())
        .await
        .expect("a presence message should arrive")
        .expect("presence channel stays open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_reports_connect_and_clean_disconnect() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_presence(port, true).await;
    let (janitor, rx) = subscribe_to_presence(port).await;

    let holder = MqttClient::new("seat-holder-7");
    holder
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let (topic, connect_event) = next_presence(&rx).await;
    assert_eq!(topic, "$DB/_presence/seat-holder-7");
    assert_eq!(connect_event["client_id"], "seat-holder-7");
    assert_eq!(connect_event["event"], "connect");
    assert!(connect_event["ts"].as_u64().is_some_and(|ts| ts > 0));

    holder.disconnect().await.unwrap();

    let (topic, disconnect_event) = next_presence(&rx).await;
    assert_eq!(topic, "$DB/_presence/seat-holder-7");
    assert_eq!(
        disconnect_event["event"], "disconnect",
        "a clean disconnect must still emit presence, unlike LWT which is unexpected-only"
    );
    assert_eq!(disconnect_event["unexpected"], false);

    janitor.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_is_retained_for_late_subscribers() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_presence(port, true).await;

    let holder = MqttClient::new("seat-holder-late");
    holder
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let (janitor, rx) = subscribe_to_presence(port).await;
    let (topic, retained) = next_presence(&rx).await;
    assert_eq!(topic, "$DB/_presence/seat-holder-late");
    assert_eq!(
        retained["event"], "connect",
        "a janitor subscribing after the fact must still learn the current state"
    );

    janitor.disconnect().await.unwrap();
    holder.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_clients_cannot_forge_presence() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_presence(port, true).await;
    let (janitor, rx) = subscribe_to_presence(port).await;

    let attacker = MqttClient::new("attacker");
    attacker
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let forged = br#"{"client_id":"seat-holder-7","event":"disconnect","unexpected":true,"ts":1}"#;
    let _ = attacker
        .publish("$DB/_presence/seat-holder-7", forged.to_vec())
        .await;

    tokio::time::sleep(Duration::from_millis(400)).await;

    let forged_delivered = rx
        .try_iter()
        .any(|(_, value)| value["ts"].as_u64() == Some(1));
    assert!(
        !forged_delivered,
        "the presence topic is ReadOnly, so a client must never be able to forge a presence message"
    );

    attacker.disconnect().await.unwrap();
    janitor.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_is_off_by_default() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_presence(port, false).await;
    let (janitor, rx) = subscribe_to_presence(port).await;

    let holder = MqttClient::new("seat-holder-quiet");
    holder
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        rx.is_empty(),
        "presence must stay silent unless it is explicitly enabled"
    );

    holder.disconnect().await.unwrap();
    janitor.disconnect().await.unwrap();
    agent_handle.abort();
}
