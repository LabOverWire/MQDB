// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use mqtt5::types::PublishOptions;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

const NODE_BIRTH_TOPIC: &str = "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1";
const DEVICE_BIRTH_TOPIC: &str = "$sparkplug/certificates/spBv1.0/plant/DBIRTH/edge-1/press-4";

async fn start_agent(port: u16, sparkplug_aware: bool) -> (TempDir, tokio::task::JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true)
        .with_sparkplug_aware(sparkplug_aware);
    let (handle, mut ready_rx, _shutdown) = agent.start().await.unwrap();
    let _ = ready_rx.changed().await;
    (tmp, handle)
}

async fn connect(port: u16, client_id: &str) -> MqttClient {
    let client = MqttClient::new(client_id);
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    client
}

async fn publish_birth(client: &MqttClient, topic: &str, payload: &[u8]) {
    let options = PublishOptions {
        qos: mqtt5::QoS::AtMostOnce,
        retain: false,
        ..Default::default()
    };
    client
        .publish_with_options(topic, payload.to_vec(), options)
        .await
        .unwrap();
}

async fn watch_certificates(
    port: u16,
    client_id: &str,
) -> (MqttClient, flume::Receiver<(String, Vec<u8>, bool)>) {
    let host = connect(port, client_id).await;
    let (tx, rx) = flume::unbounded();
    host.subscribe("$sparkplug/certificates/#", move |msg| {
        let _ = tx.try_send((msg.topic.to_string(), msg.payload.to_vec(), msg.retain));
    })
    .await
    .expect("a host application must be allowed to subscribe to certificates");
    (host, rx)
}

async fn next_certificate(
    rx: &flume::Receiver<(String, Vec<u8>, bool)>,
) -> (String, Vec<u8>, bool) {
    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("a certificate should arrive")
        .unwrap()
}

async fn late_certificates(
    port: u16,
    client_id: &str,
    topics: &[&str],
) -> (MqttClient, Vec<(String, Vec<u8>, bool)>) {
    let (host, rx) = watch_certificates(port, client_id).await;
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !topics.iter().all(|topic| {
            received
                .iter()
                .any(|(got, _, retained): &(String, Vec<u8>, bool)| got == topic && *retained)
        }) {
            received.push(rx.recv_async().await.unwrap());
        }
    })
    .await
    .expect("a late subscriber must get every certificate as a retained message");
    tokio::time::sleep(Duration::from_millis(300)).await;
    received.extend(rx.try_iter());
    (host, received)
}

async fn wait_for_certificate(port: u16, topic: &str, payload: &[u8]) {
    let (host, rx) = watch_certificates(port, "certificate-probe").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (got_topic, got_payload, _) = rx.recv_async().await.unwrap();
            if got_topic == topic && got_payload == payload {
                return;
            }
        }
    })
    .await
    .expect("the certificate should be stored");
    host.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn births_are_retained_on_certificate_topics() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;

    let edge = connect(port, "edge-1").await;
    let node_birth = [0x08, 0x00, 0xff, 0x12, 0x03];
    let device_birth = [0x08, 0x01, 0x80, 0x7f];
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", &node_birth).await;
    publish_birth(&edge, "spBv1.0/plant/DBIRTH/edge-1/press-4", &device_birth).await;
    wait_for_certificate(port, DEVICE_BIRTH_TOPIC, &device_birth).await;

    let (host, received) =
        late_certificates(port, "scada-host", &[NODE_BIRTH_TOPIC, DEVICE_BIRTH_TOPIC]).await;
    for (topic, payload, _) in &received {
        let expected = if topic == NODE_BIRTH_TOPIC {
            node_birth.as_slice()
        } else {
            device_birth.as_slice()
        };
        assert_eq!(
            payload.as_slice(),
            expected,
            "a host subscribing late must get each birth byte for byte: {topic}"
        );
    }

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_latest_birth_replaces_the_stored_certificate() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;

    let edge = connect(port, "edge-1").await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"bdseq-0").await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"bdseq-1").await;
    wait_for_certificate(port, NODE_BIRTH_TOPIC, b"bdseq-1").await;

    let (host, received) = late_certificates(port, "scada-host", &[NODE_BIRTH_TOPIC]).await;
    assert!(
        received
            .iter()
            .all(|(topic, payload, _)| topic == NODE_BIRTH_TOPIC && payload == b"bdseq-1"),
        "only the most recent NBIRTH may be stored: {received:?}"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_and_death_messages_are_not_stored() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;
    let (host, rx) = watch_certificates(port, "scada-host").await;

    let edge = connect(port, "edge-1").await;
    for topic in [
        "spBv1.0/plant/NDATA/edge-1",
        "spBv1.0/plant/DDATA/edge-1/press-4",
        "spBv1.0/plant/NDEATH/edge-1",
        "spBv1.0/plant/DDEATH/edge-1/press-4",
        "spBv1.0/STATE/scada-host",
    ] {
        publish_birth(&edge, topic, b"not-a-birth").await;
    }
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"birth").await;

    let (topic, payload, _) = next_certificate(&rx).await;
    assert_eq!(
        (topic.as_str(), payload.as_slice()),
        (NODE_BIRTH_TOPIC, b"birth".as_slice()),
        "certificates are published in order, so anything stored for the earlier messages would have arrived first"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_cannot_forge_certificates() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;

    let attacker = connect(port, "attacker").await;
    let options = PublishOptions {
        qos: mqtt5::QoS::AtLeastOnce,
        retain: true,
        ..Default::default()
    };
    let _ = attacker
        .publish_with_options(NODE_BIRTH_TOPIC, b"forged".to_vec(), options)
        .await;

    let edge = connect(port, "edge-2").await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-2", b"real").await;
    wait_for_certificate(
        port,
        "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-2",
        b"real",
    )
    .await;

    let real_topic = "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-2";
    let (host, received) = late_certificates(port, "scada-host", &[real_topic]).await;
    assert!(
        received
            .iter()
            .all(|(topic, payload, _)| topic == real_topic && payload == b"real"),
        "the forged certificate must not be stored; only the real one may be retained: {received:?}"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    attacker.disconnect().await.unwrap();
    agent_handle.abort();
}
