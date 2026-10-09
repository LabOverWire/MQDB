// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::types::OwnershipConfig;
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

struct User {
    name: &'static str,
    password: String,
}

impl User {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            password: uuid::Uuid::new_v4().to_string(),
        }
    }

    async fn connect(&self, port: u16) -> MqttClient {
        let client_id = format!("{}-{}", self.name, uuid::Uuid::new_v4());
        let client = MqttClient::new(client_id.clone());
        let options = ConnectOptions::new(client_id).with_credentials(self.name, &self.password);
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
            .await
            .unwrap();
        client
    }
}

async fn publish_with_response_topic(
    client: &MqttClient,
    topic: &str,
    payload: &Value,
    response_topic: &str,
) {
    let options = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    client
        .publish_with_options(topic, serde_json::to_vec(payload).unwrap(), options)
        .await
        .unwrap();
}

async fn request(client: &MqttClient, topic: &str, payload: &Value) -> Value {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .unwrap();
    publish_with_response_topic(client, topic, payload, &response_topic).await;
    let payload = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("no response")
        .unwrap();
    serde_json::from_slice(&payload).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn responses_published_to_request_topics_are_not_executed() {
    let tmp = TempDir::new().unwrap();
    let alice = User::new("alice");
    let bob = User::new("bob");
    let password_file = tmp.path().join("passwd");
    let lines: String = [&alice, &bob]
        .iter()
        .map(|user| {
            let hash = PasswordAuthProvider::hash_password(&user.password).unwrap();
            format!("{}:{hash}\n", user.name)
        })
        .collect();
    std::fs::write(&password_file, lines).unwrap();

    let db = Database::open_without_background_tasks(tmp.path().join("agent"))
        .await
        .unwrap();
    let port = next_test_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_password_file(password_file)
        .with_ownership_config(OwnershipConfig::parse("diagrams=userId").unwrap());
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !*ready_rx.borrow() {
            ready_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("agent did not become ready");

    let bob_client = bob.connect(port).await;
    let created = request(
        &bob_client,
        "$DB/diagrams/create",
        &json!({"id": "bobs-diagram", "title": "plan", "userId": "bob"}),
    )
    .await;
    assert_eq!(created["status"], "ok", "{created}");

    let alice_client = alice.connect(port).await;
    let denied = request(
        &alice_client,
        "$DB/diagrams/bobs-diagram/delete",
        &json!({}),
    )
    .await;
    assert_eq!(denied["status"], "error", "{denied}");

    publish_with_response_topic(
        &alice_client,
        "$DB/notes/list",
        &json!({}),
        "$DB/diagrams/bobs-diagram/delete",
    )
    .await;
    publish_with_response_topic(
        &alice_client,
        "$DB/notes/list",
        &json!({}),
        "$DB/smuggled/create",
    )
    .await;
    request(&alice_client, "$DB/notes/list", &json!({})).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let diagram = request(&bob_client, "$DB/diagrams/bobs-diagram", &json!({})).await;
    assert_eq!(
        diagram["status"], "ok",
        "a response published to a delete topic deleted another user's record: {diagram}"
    );
    let smuggled = request(&bob_client, "$DB/smuggled/list", &json!({})).await;
    assert_eq!(
        smuggled["data"],
        json!([]),
        "a response published to a create topic created a record"
    );

    alice_client.disconnect().await.unwrap();
    bob_client.disconnect().await.unwrap();
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("agent did not stop")
        .unwrap();
    tmp.close().unwrap();
}
