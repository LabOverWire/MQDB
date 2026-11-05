// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use running_agent::RunningAgent;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;
use tempfile::TempDir;

const QUIET_PERIOD: Duration = Duration::from_millis(700);

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
        let client = MqttClient::new(&client_id);
        let options = ConnectOptions::new(&client_id).with_credentials(self.name, &self.password);
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
            .await
            .unwrap();
        client
    }
}

async fn start_agent(users: &[&User]) -> RunningAgent {
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
    let ownership = OwnershipConfig::parse("collars=ownerId")
        .unwrap()
        .with_derivations("fixes=collarId>collars")
        .unwrap()
        .with_admin_users(admins.clone());
    let agent = MqdbAgent::new(db)
        .with_password_file(password_file)
        .with_admin_users(admins)
        .with_ownership_config(ownership)
        .with_scope_config(ScopeConfig::parse("collars=collarId").unwrap());
    RunningAgent::start(agent, next_test_port(), tmp).await
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
    let payload = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("the agent must answer")
        .unwrap();
    serde_json::from_slice(&payload).unwrap()
}

async fn watch(
    client: &MqttClient,
    filter: &str,
) -> Result<flume::Receiver<String>, mqtt5::error::MqttError> {
    let (tx, rx) = flume::unbounded();
    client
        .subscribe(filter, move |msg| {
            let _ = tx.try_send(msg.topic.to_string());
        })
        .await?;
    Ok(rx)
}

async fn next_topic(events: &flume::Receiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(5), events.recv_async())
        .await
        .expect("the subscriber must receive the change event")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_events_reach_only_users_who_can_view_the_scope() {
    let alice = User::new("alice");
    let mallory = User::new("mallory");
    let vera = User::new("vera");
    let root = User::new("root");
    let agent = start_agent(&[&alice, &mallory, &vera, &root]).await;

    let alice_client = alice.connect(agent.port).await;
    let mallory_client = mallory.connect(agent.port).await;
    let vera_client = vera.connect(agent.port).await;
    let root_client = root.connect(agent.port).await;

    let created = request(
        &alice_client,
        "$DB/collars/create",
        &json!({"id": "c2", "ownerId": "alice"}),
    )
    .await;
    assert_eq!(created["status"], "ok", "{created}");
    let shared = request(
        &alice_client,
        "$DB/collars/c2/share",
        &json!({"grantee": "vera", "permission": "view"}),
    )
    .await;
    assert_eq!(shared["status"], "ok", "{shared}");

    for filter in [
        "$DB/collars/c2/#",
        "$DB/collars/+/#",
        "$DB/+/c2/#",
        "$DB/#",
        "$share/g/$DB/collars/c2/#",
    ] {
        assert!(
            watch(&mallory_client, filter).await.is_err(),
            "mallory must not subscribe to {filter}"
        );
    }

    let owner_events = watch(&alice_client, "$DB/collars/c2/#").await.unwrap();
    let viewer_events = watch(&vera_client, "$DB/collars/c2/#").await.unwrap();
    let admin_events = watch(&root_client, "$DB/collars/+/#").await.unwrap();
    let broadcast = watch(&mallory_client, "$DB/fixes/events/#").await.unwrap();

    let fix = request(
        &alice_client,
        "$DB/fixes/create",
        &json!({"id": "f1", "collarId": "c2"}),
    )
    .await;
    assert_eq!(fix["status"], "ok", "{fix}");

    let expected = "$DB/collars/c2/fixes/events/created";
    assert_eq!(next_topic(&owner_events).await, expected);
    assert_eq!(next_topic(&viewer_events).await, expected);
    assert_eq!(next_topic(&admin_events).await, expected);

    let notes = watch(&mallory_client, "$DB/notes/events/#").await.unwrap();
    let admin_everything = watch(&root_client, "$DB/#").await.unwrap();
    let unscoped = request(&alice_client, "$DB/notes/create", &json!({"id": "n1"})).await;
    assert_eq!(unscoped["status"], "ok", "{unscoped}");
    let broadcast_topic = next_topic(&notes).await;
    assert!(
        broadcast_topic.starts_with("$DB/notes/events/") && broadcast_topic.ends_with("/n1"),
        "{broadcast_topic}"
    );
    assert!(!next_topic(&admin_everything).await.is_empty());
    tokio::time::sleep(QUIET_PERIOD).await;
    assert!(
        broadcast.try_recv().is_err(),
        "a scoped record must not also go to the entity broadcast"
    );

    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slash_in_a_scope_value_cannot_be_forged_or_read_through_a_prefix() {
    let alice = User::new("alice");
    let mallory = User::new("mallory");
    let root = User::new("root");
    let agent = start_agent(&[&alice, &mallory, &root]).await;

    let alice_client = alice.connect(agent.port).await;
    let mallory_client = mallory.connect(agent.port).await;
    let root_client = root.connect(agent.port).await;

    for (client, id, owner) in [
        (&alice_client, "a/b", "alice"),
        (&mallory_client, "a", "mallory"),
    ] {
        let created = request(
            client,
            "$DB/collars/create",
            &json!({"id": id, "ownerId": owner}),
        )
        .await;
        assert_eq!(created["status"], "ok", "{created}");
    }

    let prefix_events = watch(&mallory_client, "$DB/collars/a/#").await.unwrap();
    assert!(
        watch(&mallory_client, "$DB/collars/a%2Fb/#").await.is_err(),
        "mallory must not subscribe to the scope she cannot view"
    );
    let owner_events = watch(&alice_client, "$DB/collars/a%2Fb/#").await.unwrap();
    let admin_events = watch(&root_client, "$DB/collars/#").await.unwrap();

    for forged in [
        "$DB/collars/a/b/fixes/events/FORGED",
        "$DB/collars/a/b/events/FORGED",
        "$DB/collars/a%2Fb/fixes/events/FORGED",
    ] {
        let refused = mallory_client
            .publish_qos1(forged, b"{\"forged\":true}".to_vec())
            .await;
        assert!(refused.is_err(), "{forged}: {refused:?}");
    }

    let fix = request(
        &alice_client,
        "$DB/fixes/create",
        &json!({"id": "f1", "collarId": "a/b"}),
    )
    .await;
    assert_eq!(fix["status"], "ok", "{fix}");

    let expected = "$DB/collars/a%2Fb/fixes/events/created";
    assert_eq!(next_topic(&owner_events).await, expected);
    loop {
        let topic = next_topic(&admin_events).await;
        assert!(!topic.contains("FORGED"), "{topic}");
        if topic == expected {
            break;
        }
    }
    tokio::time::sleep(QUIET_PERIOD).await;
    assert!(
        prefix_events.try_recv().is_err(),
        "owning a prefix of a scope value must not deliver its events"
    );

    agent.stop().await;
}
