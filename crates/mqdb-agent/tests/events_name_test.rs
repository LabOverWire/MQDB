// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::config::DatabaseConfig;
use mqdb_core::schema::{FieldDefinition, FieldType, Schema};
use mqtt5::QoS;
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use running_agent::RunningAgent;
use serde_json::{Value, json};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

const REPLY_TIMEOUT: Duration = Duration::from_secs(3);

async fn start_agent() -> (RunningAgent, Database) {
    start_agent_with_event_partitions(8).await
}

async fn start_agent_with_event_partitions(partitions: u8) -> (RunningAgent, Database) {
    let tmp = TempDir::new().unwrap();
    let mut config = DatabaseConfig::new(tmp.path().join("agent")).without_background_tasks();
    config.shared_subscription.num_partitions = partitions;
    let db = Database::open_with_config(config).await.unwrap();
    let agent = MqdbAgent::new(db.clone()).with_anonymous(true);
    (RunningAgent::start(agent, next_test_port(), tmp).await, db)
}

async fn connect(port: u16, name: &str) -> MqttClient {
    let client = MqttClient::new(name);
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    client
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
        qos: QoS::AtLeastOnce,
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
    let reply = tokio::time::timeout(REPLY_TIMEOUT, rx.recv_async())
        .await
        .unwrap_or_else(|_| panic!("no reply to {topic}"))
        .unwrap();
    serde_json::from_slice(&reply).unwrap()
}

async fn watch(client: &MqttClient, filter: &str) -> flume::Receiver<String> {
    let (tx, rx) = flume::unbounded();
    client
        .subscribe(filter, move |msg| {
            let _ = tx.try_send(msg.topic.to_string());
        })
        .await
        .unwrap();
    rx
}

async fn next_topic(events: &flume::Receiver<String>) -> String {
    tokio::time::timeout(REPLY_TIMEOUT, events.recv_async())
        .await
        .expect("no change event")
        .unwrap()
}

async fn restore_record(db: &Database, dir: &Path, entity: &str, id: &str) {
    let backup = dir.join(format!("{entity}-{id}.jsonl"));
    let line = json!({"_entity": entity, "_data": {"id": id, "v": 1}}).to_string();
    std::fs::write(&backup, line).unwrap();
    assert_eq!(db.restore_logical(&backup).await.unwrap(), 1);
}

async fn crud_round_trip(client: &MqttClient, entity: &str, id: &str) {
    let label = format!("{entity}/{id}");
    let created = request(
        client,
        &format!("$DB/{entity}/create"),
        &json!({"id": id, "v": 1}),
    )
    .await;
    assert_eq!(created["status"], "ok", "create {label}: {created}");
    let read = request(client, &format!("$DB/{entity}/{id}"), &json!({})).await;
    assert_eq!(read["data"]["v"], 1, "read {label}: {read}");
    let updated = request(
        client,
        &format!("$DB/{entity}/{id}/update"),
        &json!({"v": 2}),
    )
    .await;
    assert_eq!(updated["data"]["v"], 2, "update {label}: {updated}");
    let listed = request(client, &format!("$DB/{entity}/list"), &json!({})).await;
    assert_eq!(listed["data"][0]["id"], id, "list {label}: {listed}");
    let deleted = request(client, &format!("$DB/{entity}/{id}/delete"), &json!({})).await;
    assert_eq!(deleted["status"], "ok", "delete {label}: {deleted}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entities_and_ids_that_look_like_events_are_served() {
    let (agent, db) = start_agent().await;
    let client = connect(agent.port, "looks-like-events").await;

    for (entity, id) in [
        ("events", "e1"),
        ("events2", "e1"),
        ("my_events", "e1"),
        ("ok1", "events-1"),
        ("ok2", "eventsabc"),
        ("ok3", "myevents"),
        ("events3", "events-1"),
    ] {
        crud_round_trip(&client, entity, id).await;
    }

    client.disconnect().await.unwrap();
    drop(db);
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_requests_naming_events_are_served() {
    let (agent, db) = start_agent().await;
    let client = connect(agent.port, "admin-events").await;

    let schema = request(
        &client,
        "$DB/_admin/schema/events/set",
        &json!({"a": {"type": "string"}}),
    )
    .await;
    assert_eq!(schema["status"], "ok", "{schema}");
    let schema = request(&client, "$DB/_admin/schema/events/get", &json!({})).await;
    assert_eq!(schema["status"], "ok", "{schema}");
    let constraint = request(
        &client,
        "$DB/_admin/constraint/events/add",
        &json!({"type": "unique", "fields": ["a"]}),
    )
    .await;
    assert_eq!(constraint["status"], "ok", "{constraint}");
    let constraints = request(&client, "$DB/_admin/constraint/events/list", &json!({})).await;
    assert_eq!(constraints["status"], "ok", "{constraints}");
    let index = request(
        &client,
        "$DB/_admin/index/events/add",
        &json!({"fields": ["a"]}),
    )
    .await;
    assert_eq!(index["status"], "ok", "{index}");
    let group = request(&client, "$DB/_admin/consumer-groups/events", &json!({})).await;
    assert!(group.get("status").is_some(), "{group}");

    client.disconnect().await.unwrap();
    drop(db);
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_with_the_id_events_is_refused_with_an_error_reply() {
    let (agent, db) = start_agent().await;
    let client = connect(agent.port, "refused-create").await;

    for entity in ["ok", "events"] {
        let refused = request(
            &client,
            &format!("$DB/{entity}/create"),
            &json!({"id": "events", "v": 1}),
        )
        .await;
        assert_eq!(refused["status"], "error", "{entity}: {refused}");
        assert_eq!(refused["code"], 400, "{entity}: {refused}");
        assert!(
            refused["message"].as_str().unwrap().contains("reserved"),
            "{entity}: {refused}"
        );
        let listed = request(&client, &format!("$DB/{entity}/list"), &json!({})).await;
        assert_eq!(listed["data"], json!([]), "{entity}: {listed}");
    }

    let other_case = request(&client, "$DB/ok/create", &json!({"id": "Events"})).await;
    assert_eq!(other_case["status"], "ok", "{other_case}");

    client.disconnect().await.unwrap();
    drop(db);
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_default_cannot_assign_the_id_events() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(tmp.path().join("db"))
        .await
        .unwrap();
    db.add_schema(
        Schema::new("defaulted")
            .add_field(FieldDefinition::new("id", FieldType::String).with_default(json!("events"))),
    )
    .await
    .unwrap();

    let response = db
        .execute(mqdb_core::transport::Request::Create {
            entity: "defaulted".to_string(),
            data: json!({"v": 1}),
        })
        .await;

    let reply = serde_json::to_value(&response).unwrap();
    assert_eq!(reply["status"], "error", "{reply}");
    assert_eq!(reply["code"], 400, "{reply}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_event_is_never_handled_as_a_request() {
    for partitions in [0, 8] {
        change_events_leave_the_record_named_events_alone(partitions).await;
    }
}

async fn change_events_leave_the_record_named_events_alone(partitions: u8) {
    let (agent, db) = start_agent_with_event_partitions(partitions).await;
    let client = connect(agent.port, "own-events").await;
    let backups = TempDir::new().unwrap();

    for entity in ["ok", "events"] {
        restore_record(&db, backups.path(), entity, "events").await;
        let events = watch(&client, &format!("$DB/{entity}/events/#")).await;

        for id in ["update", "delete", "share", "unshare", "shares"] {
            let created = request(
                &client,
                &format!("$DB/{entity}/create"),
                &json!({"id": id, "v": 99}),
            )
            .await;
            assert_eq!(created["status"], "ok", "{entity}/{id}: {created}");
            let event = next_topic(&events).await;
            assert!(event.ends_with(&format!("/{id}")), "{event}");
            assert_eq!(
                event == format!("$DB/{entity}/events/{id}"),
                partitions == 0,
                "{event}"
            );
        }
        let barrier = request(&client, &format!("$DB/{entity}/list"), &json!({})).await;
        assert_eq!(barrier["status"], "ok", "{barrier}");

        let untouched = db
            .read(entity.to_string(), "events".to_string(), vec![], None)
            .await
            .unwrap();
        assert_eq!(untouched["v"], 1, "{entity}: {untouched}");
        assert_eq!(untouched["_version"], 1, "{entity}: {untouched}");
    }

    client.disconnect().await.unwrap();
    drop(db);
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_cannot_publish_a_change_event() {
    let (agent, db) = start_agent().await;
    let client = connect(agent.port, "forger").await;

    for entity in ["ok", "events"] {
        let events = watch(&client, &format!("$DB/{entity}/events/#")).await;
        for level in ["events", "events/fake", "events/p3/fake", "events/update"] {
            let topic = format!("$DB/{entity}/{level}");
            let options = PublishOptions {
                qos: QoS::AtLeastOnce,
                ..Default::default()
            };
            let forged = client
                .publish_with_options(&topic, b"{}".to_vec(), options)
                .await;
            assert!(forged.is_err(), "{topic} was accepted");
        }

        let real = format!("real-{entity}");
        let created = request(
            &client,
            &format!("$DB/{entity}/create"),
            &json!({"id": real}),
        )
        .await;
        assert_eq!(created["status"], "ok", "{created}");
        let event = next_topic(&events).await;
        assert!(
            event.ends_with(&format!("/{real}")),
            "a forged event was delivered ahead of the real one: {event}"
        );
    }

    client.disconnect().await.unwrap();
    drop(db);
    agent.stop().await;
}
