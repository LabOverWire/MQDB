mod common;

use common::next_test_port;
use mqtt5::QoS;
use mqtt5::broker::MqttBroker;
use mqtt5::broker::config::{BrokerConfig, StorageBackend, StorageConfig};
use mqtt5::client::MqttClient;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;

async fn start_test_broker(port: u16) -> TempDir {
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

#[tokio::test]
async fn test_subscribe_unsubscribe_flow() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let client = MqttClient::new("test-sub-unsub");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel::<String>(10);

    client
        .subscribe("test/topic", move |msg| {
            let payload = String::from_utf8_lossy(&msg.payload).to_string();
            let _ = tx.try_send(payload);
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish("test/topic", b"message1".to_vec())
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("should receive within timeout")
        .expect("should have message");

    assert_eq!(received, "message1");

    client.unsubscribe("test/topic").await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    client
        .publish("test/topic", b"message2".to_vec())
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
    let no_message = match result {
        Err(_) | Ok(None) => true,
        Ok(Some(_)) => false,
    };
    assert!(no_message, "should not receive after unsubscribe");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_qos1_delivery() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let publisher = MqttClient::new("test-qos1-pub");
    publisher
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let subscriber = MqttClient::new("test-qos1-sub");
    subscriber
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(10);

    subscriber
        .subscribe("test/qos1", move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .publish_qos("test/qos1", b"qos1-message".to_vec(), QoS::AtLeastOnce)
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("should receive within timeout")
        .expect("should have message");

    assert_eq!(received, b"qos1-message");

    publisher.disconnect().await.unwrap();
    subscriber.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_retained_message() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let publisher = MqttClient::new("test-retain-pub");
    publisher
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    publisher
        .publish_retain("test/retained", b"retained-msg".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher.disconnect().await.unwrap();

    let subscriber = MqttClient::new("test-retain-sub");
    subscriber
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(10);

    subscriber
        .subscribe("test/retained", move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("new subscriber should receive retained message")
        .expect("should have message");

    assert_eq!(received, b"retained-msg");

    subscriber.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_wildcard_plus_subscription() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let client = MqttClient::new("test-wildcard-plus");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    client
        .subscribe("sensor/+/temperature", move |_msg| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish("sensor/room1/temperature", b"22.5".to_vec())
        .await
        .unwrap();
    client
        .publish("sensor/room2/temperature", b"23.0".to_vec())
        .await
        .unwrap();
    client
        .publish("sensor/room1/humidity", b"45".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "should match 2 temperature topics"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_wildcard_hash_subscription() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let client = MqttClient::new("test-wildcard-hash");
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    client
        .subscribe("home/#", move |_msg| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish("home/kitchen/light", b"on".to_vec())
        .await
        .unwrap();
    client
        .publish("home/bedroom/fan", b"off".to_vec())
        .await
        .unwrap();
    client.publish("home/status", b"ok".to_vec()).await.unwrap();
    client
        .publish("office/printer", b"idle".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        3,
        "should match 3 home/# topics"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_multiple_subscribers_same_topic() {
    let port = next_test_port();
    let _tmp = start_test_broker(port).await;

    let sub1 = MqttClient::new("test-multi-sub1");
    sub1.connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let sub2 = MqttClient::new("test-multi-sub2");
    sub2.connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let publisher = MqttClient::new("test-multi-pub");
    publisher
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();

    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));

    let c1 = counter1.clone();
    sub1.subscribe("broadcast/topic", move |_msg| {
        c1.fetch_add(1, Ordering::SeqCst);
    })
    .await
    .unwrap();

    let c2 = counter2.clone();
    sub2.subscribe("broadcast/topic", move |_msg| {
        c2.fetch_add(1, Ordering::SeqCst);
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .publish("broadcast/topic", b"hello".to_vec())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        counter1.load(Ordering::SeqCst),
        1,
        "sub1 should receive message"
    );
    assert_eq!(
        counter2.load(Ordering::SeqCst),
        1,
        "sub2 should receive message"
    );

    sub1.disconnect().await.unwrap();
    sub2.disconnect().await.unwrap();
    publisher.disconnect().await.unwrap();
}
