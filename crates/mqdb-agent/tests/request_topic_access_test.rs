// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::types::OwnershipConfig;
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use running_agent::RunningAgent;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;
use tempfile::TempDir;

const QUIET_PERIOD: Duration = Duration::from_millis(500);

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

async fn start_agent(users: &[&User], ownership: &str) -> RunningAgent {
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let lines: String = users
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
    let admins = HashSet::from(["root".to_string()]);
    let agent = MqdbAgent::new(db)
        .with_password_file(password_file)
        .with_admin_users(admins.clone())
        .with_ownership_config(
            OwnershipConfig::parse(ownership)
                .unwrap()
                .with_admin_users(admins),
        );
    RunningAgent::start(agent, next_test_port(), tmp).await
}

async fn request_on(client: &MqttClient, topic: &str, payload: &Value, response_topic: &str) {
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

async fn watch(
    client: &MqttClient,
    filter: &str,
) -> Result<flume::Receiver<(String, Vec<u8>)>, mqtt5::error::MqttError> {
    let (tx, rx) = flume::unbounded();
    client
        .subscribe(filter, move |msg| {
            let _ = tx.try_send((msg.topic.to_string(), msg.payload.clone()));
        })
        .await?;
    Ok(rx)
}

async fn next_message(messages: &flume::Receiver<(String, Vec<u8>)>) -> (String, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(5), messages.recv_async())
        .await
        .expect("no message was delivered")
        .unwrap()
}

async fn request(client: &MqttClient, topic: &str, payload: &Value) -> Value {
    let response_topic = format!("resp/{}", uuid::Uuid::new_v4());
    let responses = watch(client, &response_topic).await.unwrap();
    request_on(client, topic, payload, &response_topic).await;
    serde_json::from_slice(&next_message(&responses).await.1).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_admin_cannot_subscribe_to_requests_and_nothing_else_changes() {
    let alice = User::new("alice");
    let mallory = User::new("mallory");
    let root = User::new("root");
    let agent = start_agent(&[&alice, &mallory, &root], "notes=ownerId").await;
    let alice_client = alice.connect(agent.port).await;
    let mallory_client = mallory.connect(agent.port).await;
    let root_client = root.connect(agent.port).await;

    for filter in [
        "$DB/notes/create",
        "$DB/notes/list",
        "$DB/notes/n1",
        "$DB/notes/n1/update",
        "$DB/notes/n1/delete",
        "$DB/notes/n1/share",
        "$DB/+/create",
        "$DB/notes/+",
        "$DB/notes/#",
        "$DB/+/#",
        "$DB/#",
        "$share/g/$DB/notes/create",
        "$share/g/$DB/+/create",
        "$share/g/$DB/#",
    ] {
        assert!(
            watch(&mallory_client, filter).await.is_err(),
            "mallory must not subscribe to {filter}"
        );
    }

    let events = watch(&mallory_client, "$DB/notes/events/#").await.unwrap();
    let admin_requests = watch(&root_client, "$DB/notes/create").await.unwrap();
    let created = request(
        &alice_client,
        "$DB/notes/create",
        &json!({"id": "n1", "ownerId": "alice", "secret": "alice-only"}),
    )
    .await;
    assert_eq!(created["status"], "ok", "{created}");

    let (event_topic, _) = next_message(&events).await;
    assert!(
        event_topic.starts_with("$DB/notes/events/"),
        "{event_topic}"
    );
    let (request_topic, request_payload) = next_message(&admin_requests).await;
    assert_eq!(request_topic, "$DB/notes/create");
    assert!(String::from_utf8_lossy(&request_payload).contains("alice-only"));

    alice_client.disconnect().await.unwrap();
    mallory_client.disconnect().await.unwrap();
    root_client.disconnect().await.unwrap();
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_response_topic_under_db_works_unless_it_is_a_request_of_an_owned_entity() {
    let mallory = User::new("mallory");
    let agent = start_agent(&[&mallory], "notes=ownerId,events2=ownerId").await;
    let client = mallory.connect(agent.port).await;

    for (filter, response_topic) in [
        ("$DB/clients/c1/#", "$DB/clients/c1/response"),
        ("$DB/clients/c1/replies/+", "$DB/clients/c1/replies/1"),
        ("$DB/notes/n1/replies/#", "$DB/notes/n1/replies/1"),
    ] {
        let responses = watch(&client, filter).await.unwrap();
        request_on(&client, "$DB/notes/list", &json!({}), response_topic).await;
        let (topic, payload) = next_message(&responses).await;
        assert_eq!(topic, response_topic);
        let reply: Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(reply["status"], "ok", "{reply}");
    }

    assert!(
        watch(&client, "$DB/events2/create").await.is_err(),
        "a response topic that is also a request of an owned entity must be refused"
    );

    client.disconnect().await.unwrap();
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_of_an_entity_with_no_owner_stays_subscribable() {
    let alice = User::new("alice");
    let mallory = User::new("mallory");
    let agent = start_agent(&[&alice, &mallory], "notes=ownerId").await;
    let alice_client = alice.connect(agent.port).await;
    let mallory_client = mallory.connect(agent.port).await;

    let requests = watch(&mallory_client, "$DB/public/create").await.unwrap();
    let created = request(&alice_client, "$DB/public/create", &json!({"id": "p1"})).await;
    assert_eq!(created["status"], "ok", "{created}");
    assert_eq!(next_message(&requests).await.0, "$DB/public/create");

    let refused = watch(&mallory_client, "$DB/notes/create").await;
    assert!(refused.is_err());
    let note = request(
        &alice_client,
        "$DB/notes/create",
        &json!({"id": "n1", "ownerId": "alice"}),
    )
    .await;
    assert_eq!(note["status"], "ok", "{note}");
    tokio::time::sleep(QUIET_PERIOD).await;
    assert!(requests.try_recv().is_err());

    alice_client.disconnect().await.unwrap();
    mallory_client.disconnect().await.unwrap();
    agent.stop().await;
}
