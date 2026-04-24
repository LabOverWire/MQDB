// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
async fn mqtt_request_response(
    client: &MqttClient,
    topic: &str,
    payload: &[u8],
    timeout_ms: u64,
) -> Option<Value> {
    let response_topic = format!("test-response/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);

    client
        .subscribe(&response_topic, move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .ok()?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic),
            ..Default::default()
        },
        ..Default::default()
    };

    client
        .publish_with_options(topic, payload.to_vec(), opts)
        .await
        .ok()?;

    match tokio::time::timeout(Duration::from_millis(timeout_ms), rx.recv_async()).await {
        Ok(Ok(data)) => serde_json::from_slice(&data).ok(),
        _ => None,
    }
}

fn assert_response_ok(response: &Value) {
    assert_eq!(
        response.get("status").and_then(Value::as_str),
        Some("ok"),
        "expected ok response, got: {response}"
    );
}

fn get_response_data(response: &Value) -> Option<&Value> {
    response.get("data")
}

async fn start_agent(port: u16) -> (TempDir, tokio::task::JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true);
    let (handle, mut ready_rx, _shutdown) = agent.start().await.unwrap();
    let _ = ready_rx.changed().await;
    (tmp, handle)
}

async fn start_agent_with_admin(
    port: u16,
    admin_user: &str,
) -> (TempDir, tokio::task::JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut admin_users = std::collections::HashSet::new();
    admin_users.insert(admin_user.to_string());
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true)
        .with_admin_users(admin_users);
    let (handle, mut ready_rx, _shutdown) = agent.start().await.unwrap();
    let _ = ready_rx.changed().await;
    (tmp, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_health_endpoint_agent_mode() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port).await;

    let client = MqttClient::new("test-health-agent");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = mqtt_request_response(&client, "$DB/_health", b"{}", 2000)
        .await
        .expect("should receive health response");

    assert_response_ok(&response);

    let data = get_response_data(&response).expect("should have data field");
    assert_eq!(data.get("mode").and_then(|v| v.as_str()), Some("agent"));
    assert_eq!(
        data.get("ready").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(data.get("status").and_then(|v| v.as_str()), Some("healthy"));

    client.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_admin_schema_set_and_get() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_admin(port, "admin").await;

    let client = MqttClient::new("test-schema-ops");
    let options = ConnectOptions::new("test-schema-ops").with_credentials("admin", "");
    Box::pin(client.connect_with_options(&format!("mqtt://127.0.0.1:{port}"), options))
        .await
        .unwrap();

    let schema = json!({
        "name": {"type": "string", "required": true},
        "email": {"type": "string", "required": false}
    });

    let set_response = mqtt_request_response(
        &client,
        "$DB/_admin/schema/test_entity/set",
        schema.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive schema set response");

    assert_response_ok(&set_response);

    let get_response =
        mqtt_request_response(&client, "$DB/_admin/schema/test_entity/get", b"{}", 2000)
            .await
            .expect("should receive schema get response");

    assert_response_ok(&get_response);
    let data = get_response_data(&get_response).expect("should have data");
    assert!(data.get("fields").is_some(), "schema should have fields");

    client.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_admin_constraint_add_and_list() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent_with_admin(port, "admin").await;

    let client = MqttClient::new("test-constraint-ops");
    let options = ConnectOptions::new("test-constraint-ops").with_credentials("admin", "");
    Box::pin(client.connect_with_options(&format!("mqtt://127.0.0.1:{port}"), options))
        .await
        .unwrap();

    let constraint = json!({
        "type": "unique",
        "fields": ["email"]
    });

    let add_response = mqtt_request_response(
        &client,
        "$DB/_admin/constraint/users/add",
        constraint.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive constraint add response");

    assert_response_ok(&add_response);

    let list_response =
        mqtt_request_response(&client, "$DB/_admin/constraint/users/list", b"{}", 2000)
            .await
            .expect("should receive constraint list response");

    assert_response_ok(&list_response);

    client.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_crud_via_mqtt() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port).await;

    let client = MqttClient::new("test-crud-mqtt");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let user = json!({
        "name": "Alice",
        "email": "alice@example.com"
    });

    let create_response = mqtt_request_response(
        &client,
        "$DB/users/create",
        user.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive create response");

    assert_response_ok(&create_response);
    let created = get_response_data(&create_response).expect("should have data");
    let id = created
        .get("id")
        .and_then(|v| v.as_str())
        .expect("should have id");

    let read_response = mqtt_request_response(&client, &format!("$DB/users/{id}"), b"{}", 2000)
        .await
        .expect("should receive read response");

    assert_response_ok(&read_response);
    let read_data = get_response_data(&read_response).expect("should have data");
    assert_eq!(
        read_data.get("name").and_then(|v| v.as_str()),
        Some("Alice")
    );

    let update = json!({"name": "Alice Smith"});
    let update_response = mqtt_request_response(
        &client,
        &format!("$DB/users/{id}/update"),
        update.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive update response");

    assert_response_ok(&update_response);

    let delete_response =
        mqtt_request_response(&client, &format!("$DB/users/{id}/delete"), b"{}", 2000)
            .await
            .expect("should receive delete response");

    assert_response_ok(&delete_response);

    client.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test]
async fn test_list_query_complexity_limits_via_mqtt() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port).await;

    let client = MqttClient::new("test-query-limits");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let too_many_filters: Vec<Value> = (0..17)
        .map(|i| json!({"field": format!("f{i}"), "op": "eq", "value": "x"}))
        .collect();
    let payload = json!({"filters": too_many_filters});
    let resp = mqtt_request_response(
        &client,
        "$DB/items/list",
        payload.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive response for too many filters");
    assert_eq!(
        resp.get("status").and_then(Value::as_str),
        Some("error"),
        "too many filters should be rejected: {resp}"
    );
    assert!(
        resp.get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("too many filters"),
        "error should mention 'too many filters': {resp}"
    );

    let too_many_sorts: Vec<Value> = (0..5)
        .map(|i| json!({"field": format!("s{i}"), "direction": "asc"}))
        .collect();
    let payload = json!({"sort": too_many_sorts});
    let resp = mqtt_request_response(
        &client,
        "$DB/items/list",
        payload.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive response for too many sorts");
    assert_eq!(
        resp.get("status").and_then(Value::as_str),
        Some("error"),
        "too many sort fields should be rejected: {resp}"
    );
    assert!(
        resp.get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("too many sort fields"),
        "error should mention 'too many sort fields': {resp}"
    );

    let valid_filters: Vec<Value> = (0..16)
        .map(|i| json!({"field": format!("f{i}"), "op": "eq", "value": "x"}))
        .collect();
    let payload = json!({"filters": valid_filters});
    let resp = mqtt_request_response(
        &client,
        "$DB/items/list",
        payload.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive response for 16 filters");
    assert_eq!(
        resp.get("status").and_then(Value::as_str),
        Some("ok"),
        "16 filters should be accepted: {resp}"
    );

    let valid_sorts: Vec<Value> = (0..4)
        .map(|i| json!({"field": format!("s{i}"), "direction": "asc"}))
        .collect();
    let payload = json!({"sort": valid_sorts});
    let resp = mqtt_request_response(
        &client,
        "$DB/items/list",
        payload.to_string().as_bytes(),
        2000,
    )
    .await
    .expect("should receive response for 4 sorts");
    assert_eq!(
        resp.get("status").and_then(Value::as_str),
        Some("ok"),
        "4 sort fields should be accepted: {resp}"
    );

    client.disconnect().await.unwrap();
    agent_handle.abort();
}
