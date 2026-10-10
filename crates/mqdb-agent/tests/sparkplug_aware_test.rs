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
    let (handle, ready_rx, _shutdown) = agent.start().await.unwrap();
    wait_until_ready(ready_rx).await;
    (tmp, handle)
}

async fn wait_until_ready(mut ready_rx: tokio::sync::watch::Receiver<bool>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !*ready_rx.borrow() {
            ready_rx
                .changed()
                .await
                .expect("the agent stopped before it became ready");
        }
    })
    .await
    .expect("agent did not become ready");
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

async fn watch_stored_births(
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

async fn next_stored_birth(
    rx: &flume::Receiver<(String, Vec<u8>, bool)>,
) -> (String, Vec<u8>, bool) {
    tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("a certificate should arrive")
        .unwrap()
}

async fn late_stored_births(
    port: u16,
    client_id: &str,
    topics: &[&str],
) -> (MqttClient, Vec<(String, Vec<u8>, bool)>) {
    let (host, rx) = watch_stored_births(port, client_id).await;
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

async fn wait_until_stored(port: u16, topic: &str, payload: &[u8]) {
    let (host, rx) = watch_stored_births(port, "certificate-probe").await;
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
    wait_until_stored(port, DEVICE_BIRTH_TOPIC, &device_birth).await;

    let (host, received) =
        late_stored_births(port, "scada-host", &[NODE_BIRTH_TOPIC, DEVICE_BIRTH_TOPIC]).await;
    for (topic, payload, _) in &received {
        let expected = if topic == NODE_BIRTH_TOPIC {
            node_birth.as_slice()
        } else {
            device_birth.as_slice()
        };
        assert_eq!(
            payload.as_slice(),
            expected,
            "a host subscribing late must get each birth byte for byte"
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
    wait_until_stored(port, NODE_BIRTH_TOPIC, b"bdseq-1").await;

    let (host, received) = late_stored_births(port, "scada-host", &[NODE_BIRTH_TOPIC]).await;
    assert!(
        received
            .iter()
            .all(|(topic, payload, _)| topic == NODE_BIRTH_TOPIC && payload == b"bdseq-1"),
        "only the most recent NBIRTH may be stored"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_and_death_messages_are_not_stored() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;
    let (host, rx) = watch_stored_births(port, "scada-host").await;

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

    let (topic, payload, _) = next_stored_birth(&rx).await;
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
    let forged = attacker
        .publish_with_options(NODE_BIRTH_TOPIC, b"forged".to_vec(), options)
        .await;
    assert!(
        forged.is_err(),
        "the broker must refuse a client publish to the certificate namespace"
    );

    let edge = connect(port, "edge-2").await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-2", b"real").await;
    wait_until_stored(
        port,
        "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-2",
        b"real",
    )
    .await;

    let real_topic = "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-2";
    let (host, received) = late_stored_births(port, "scada-host", &[real_topic]).await;
    assert!(
        received
            .iter()
            .all(|(topic, payload, _)| topic == real_topic && payload == b"real"),
        "the forged certificate must not be stored; only the real one may be retained"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    attacker.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_rebirth_clears_certificates_of_devices_it_no_longer_has() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, true).await;
    let removed_device = "$sparkplug/certificates/spBv1.0/plant/DBIRTH/edge-1/press-9";

    let edge = connect(port, "edge-1").await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"bdseq-0").await;
    publish_birth(&edge, "spBv1.0/plant/DBIRTH/edge-1/press-4", b"press-4-v0").await;
    publish_birth(&edge, "spBv1.0/plant/DBIRTH/edge-1/press-9", b"press-9-v0").await;
    wait_until_stored(port, removed_device, b"press-9-v0").await;

    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"bdseq-1").await;
    publish_birth(&edge, "spBv1.0/plant/DBIRTH/edge-1/press-4", b"press-4-v1").await;
    wait_until_stored(port, DEVICE_BIRTH_TOPIC, b"press-4-v1").await;

    let (host, received) =
        late_stored_births(port, "scada-host", &[NODE_BIRTH_TOPIC, DEVICE_BIRTH_TOPIC]).await;
    assert!(
        received.iter().all(|(topic, _, _)| topic != removed_device),
        "a device missing from the latest birth sequence must not keep its certificate"
    );
    assert!(
        received.iter().all(|(topic, payload, _)| {
            (topic == NODE_BIRTH_TOPIC && payload == b"bdseq-1")
                || (topic == DEVICE_BIRTH_TOPIC && payload == b"press-4-v1")
        }),
        "only the certificates of the latest birth sequence may be stored"
    );

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_certificate_namespace_stays_open_when_the_feature_is_off() {
    let port = next_test_port();
    let (_tmp, agent_handle) = start_agent(port, false).await;

    let bridge = connect(port, "external-aware-bridge").await;
    publish_birth(&bridge, "spBv1.0/plant/NBIRTH/edge-2", b"unstored-birth").await;
    let options = PublishOptions {
        qos: mqtt5::QoS::AtLeastOnce,
        retain: true,
        ..Default::default()
    };
    bridge
        .publish_with_options(NODE_BIRTH_TOPIC, b"from-a-bridge".to_vec(), options)
        .await
        .unwrap();

    let (host, received) = late_stored_births(port, "scada-host", &[NODE_BIRTH_TOPIC]).await;
    assert!(
        received
            .iter()
            .any(|(topic, payload, _)| topic == NODE_BIRTH_TOPIC && payload == b"from-a-bridge"),
        "without --sparkplug-aware, $sparkplug/ must behave like any other topic"
    );
    assert!(
        received
            .iter()
            .all(|(topic, _, _)| topic == NODE_BIRTH_TOPIC),
        "without --sparkplug-aware, births must not be stored; the birth was published first on the same connection"
    );

    host.disconnect().await.unwrap();
    bridge.disconnect().await.unwrap();
    agent_handle.abort();
}

struct Account {
    name: &'static str,
    password: String,
}

impl Account {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            password: uuid::Uuid::new_v4().to_string(),
        }
    }

    async fn connect(&self, port: u16) -> MqttClient {
        let client_id = format!("{}-{}", self.name, uuid::Uuid::new_v4());
        let client = MqttClient::new(client_id.clone());
        let options = mqtt5::types::ConnectOptions::new(client_id)
            .with_credentials(self.name, &self.password);
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
            .await
            .unwrap();
        client
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restrictive_acl_does_not_stop_certificates() {
    let edge_account = Account::new("edge");
    let host_account = Account::new("host");
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let lines: String = [&edge_account, &host_account]
        .iter()
        .map(|account| {
            let hash =
                mqtt5::broker::PasswordAuthProvider::hash_password(&account.password).unwrap();
            format!("{}:{hash}\n", account.name)
        })
        .collect();
    std::fs::write(&password_file, lines).unwrap();
    let acl_file = tmp.path().join("acl");
    std::fs::write(
        &acl_file,
        "user * topic $DB/# permission readwrite\n\
         user edge topic spBv1.0/# permission readwrite\n\
         user host topic $sparkplug/# permission read\n",
    )
    .unwrap();

    let port = next_test_port();
    let db = Database::open(tmp.path().join("db")).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_password_file(password_file)
        .with_acl_file(acl_file)
        .with_sparkplug_aware(true);
    let (agent_handle, ready_rx, _shutdown) = agent.start().await.unwrap();
    wait_until_ready(ready_rx).await;

    let edge = edge_account.connect(port).await;
    publish_birth(&edge, "spBv1.0/plant/NBIRTH/edge-1", b"acl-birth").await;

    let host = host_account.connect(port).await;
    let (tx, rx) = flume::unbounded();
    host.subscribe("$sparkplug/certificates/#", move |msg| {
        let _ = tx.try_send((msg.topic.to_string(), msg.payload.to_vec(), msg.retain));
    })
    .await
    .unwrap();
    let (topic, payload, retained) = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("an ACL that grants nobody $sparkplug/# publish rights must not stop certificates")
        .unwrap();
    assert_eq!(topic, NODE_BIRTH_TOPIC);
    assert_eq!(payload, b"acl-birth");
    assert!(retained);

    host.disconnect().await.unwrap();
    edge.disconnect().await.unwrap();
    agent_handle.abort();
}
