use mqtt5::broker::config::{BrokerConfig, StorageBackend, StorageConfig};
use mqtt5::broker::MqttBroker;
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(21000);

pub fn next_test_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub async fn start_test_broker(port: u16) -> TempDir {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let storage_config = StorageConfig {
        backend: StorageBackend::Memory,
        enable_persistence: true,
        ..Default::default()
    };
    let config = BrokerConfig::default()
        .with_bind_address(addr)
        .with_max_clients(100)
        .with_storage(storage_config);

    let mut broker = MqttBroker::with_config(config).await.unwrap();
    let mut ready_rx = broker.ready_receiver();

    tokio::spawn(async move {
        let _ = broker.run().await;
    });

    let _ = ready_rx.changed().await;

    TempDir::new().unwrap()
}

pub async fn mqtt_request_response(
    client: &MqttClient,
    topic: &str,
    payload: &[u8],
    timeout_ms: u64,
) -> Option<Value> {
    let response_topic = format!("test-response/{}", uuid::Uuid::new_v4());
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);

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

    match tokio::time::timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
        Ok(Some(data)) => serde_json::from_slice(&data).ok(),
        _ => None,
    }
}

pub fn assert_response_ok(response: &Value) {
    assert_eq!(
        response.get("status").and_then(Value::as_str),
        Some("ok"),
        "expected ok response, got: {response}"
    );
}

pub fn assert_response_error(response: &Value, expected_code: u16) {
    assert_eq!(
        response.get("status").and_then(Value::as_str),
        Some("error"),
        "expected error response, got: {response}"
    );
    assert_eq!(
        response.get("code").and_then(Value::as_u64),
        Some(u64::from(expected_code)),
        "expected error code {expected_code}, got: {response}"
    );
}

pub fn get_response_data(response: &Value) -> Option<&Value> {
    response.get("data")
}
