// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::broker_defaults::BROKER_SYS_RETAINED_TOPICS;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use mqtt5::protocol::v5::reason_codes::ReasonCode;
use mqtt5::{MqttError, PublishOptions, QoS};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

struct Limits {
    messages: usize,
    message_size: usize,
    presence: bool,
    sparkplug_aware: bool,
}

struct TestAgent {
    dir: TempDir,
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl TestAgent {
    async fn stop(self) {
        self.shutdown
            .send(())
            .expect("the agent listens for shutdown until it stops");
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("the agent should stop after shutdown")
            .expect("the agent task should not panic");
        drop(self.dir);
    }
}

async fn start_agent(port: u16, limits: Limits) -> TestAgent {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_anonymous(true)
        .with_presence(limits.presence)
        .with_sparkplug_aware(limits.sparkplug_aware)
        .with_max_retained_messages(limits.messages)
        .with_max_retained_message_size(limits.message_size);
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), ready_rx.wait_for(|ready| *ready))
        .await
        .expect("agent did not become ready")
        .expect("the agent stopped before it became ready");
    TestAgent {
        dir: tmp,
        handle,
        shutdown,
    }
}

async fn connect(port: u16, client_id: &str) -> MqttClient {
    let client = MqttClient::new(client_id);
    client
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    client
}

async fn publish_retained(
    client: &MqttClient,
    topic: &str,
    payload: &[u8],
) -> Result<(), MqttError> {
    let options = PublishOptions {
        qos: QoS::AtLeastOnce,
        retain: true,
        ..Default::default()
    };
    client
        .publish_with_options(topic, payload.to_vec(), options)
        .await
        .map(|_| ())
}

fn assert_refused(result: Result<(), MqttError>, what: &str) {
    assert!(
        matches!(
            result,
            Err(MqttError::PublishFailed(ReasonCode::QuotaExceeded))
        ),
        "{what} must be refused with quota exceeded, got {result:?}"
    );
}

async fn stored_retained(port: u16, client_id: &str, filter: &str) -> Vec<(String, Vec<u8>)> {
    let client = connect(port, client_id).await;
    let (tx, rx) = flume::unbounded();
    client
        .subscribe(filter, move |msg| {
            if msg.retain {
                tx.try_send((msg.topic.to_string(), msg.payload.to_vec()))
                    .expect("the receiver lives until the subscriber disconnects");
            }
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.disconnect().await.unwrap();
    let mut stored: Vec<_> = rx.try_iter().collect();
    stored.sort();
    stored
}

async fn wait_for_sys_topics(port: u16) {
    let stored = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let stored = stored_retained(port, "sys-probe", "$SYS/#").await;
            if stored
                .iter()
                .any(|(topic, _)| topic == "$SYS/broker/subscriptions/count")
            {
                return stored;
            }
        }
    })
    .await
    .expect("the broker should publish its retained $SYS topics");
    assert_eq!(
        stored.len(),
        BROKER_SYS_RETAINED_TOPICS,
        "the broker's retained $SYS topics changed, update BROKER_SYS_RETAINED_TOPICS: {:?}",
        stored.iter().map(|(topic, _)| topic).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_publish_over_the_size_limit_is_refused_and_not_stored() {
    let port = next_test_port();
    let agent = start_agent(
        port,
        Limits {
            messages: 0,
            message_size: 16,
            presence: false,
            sparkplug_aware: false,
        },
    )
    .await;

    let client = connect(port, "publisher").await;
    assert_refused(
        publish_retained(&client, "limits/large", &[7; 17]).await,
        "a retained payload one byte over the limit",
    );
    publish_retained(&client, "limits/small", &[7; 16])
        .await
        .expect("a retained payload at the limit must be accepted");

    assert_eq!(
        stored_retained(port, "late-subscriber", "limits/#").await,
        vec![("limits/small".to_string(), vec![7; 16])],
        "only the payload within the limit may be stored"
    );

    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_limit_refuses_only_new_retained_topics() {
    let port = next_test_port();
    let agent = start_agent(
        port,
        Limits {
            messages: BROKER_SYS_RETAINED_TOPICS + 2,
            message_size: 0,
            presence: false,
            sparkplug_aware: false,
        },
    )
    .await;
    wait_for_sys_topics(port).await;

    let client = connect(port, "publisher").await;
    publish_retained(&client, "limits/a", b"a1").await.unwrap();
    publish_retained(&client, "limits/b", b"b1").await.unwrap();
    assert_refused(
        publish_retained(&client, "limits/c", b"c1").await,
        "a third retained topic over a limit of two",
    );
    publish_retained(&client, "limits/a", b"a2")
        .await
        .expect("replacing an existing retained topic must be accepted at the limit");
    publish_retained(&client, "limits/b", b"")
        .await
        .expect("clearing a retained topic must be accepted at the limit");
    publish_retained(&client, "limits/c", b"c2")
        .await
        .expect("a cleared topic must free room for a new one");

    assert_eq!(
        stored_retained(port, "late-subscriber", "limits/#").await,
        vec![
            ("limits/a".to_string(), b"a2".to_vec()),
            ("limits/c".to_string(), b"c2".to_vec()),
        ]
    );

    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_and_certificates_are_stored_after_the_count_limit_is_reached() {
    let port = next_test_port();
    let agent = start_agent(
        port,
        Limits {
            messages: BROKER_SYS_RETAINED_TOPICS + 1,
            message_size: 0,
            presence: true,
            sparkplug_aware: true,
        },
    )
    .await;
    wait_for_sys_topics(port).await;

    let client = connect(port, "publisher").await;
    assert_refused(
        publish_retained(&client, "limits/a", b"a1").await,
        "a client retained topic once presence has filled the limit",
    );

    let edge = connect(port, "edge-1").await;
    edge.publish_qos1("spBv1.0/plant/NBIRTH/edge-1", b"nbirth".to_vec())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while stored_retained(port, "janitor", "$DB/_presence/edge-1")
            .await
            .is_empty()
        {}
    })
    .await
    .expect("presence for a client connecting after the limit is reached must still be retained");
    assert_eq!(
        stored_retained(port, "scada-host", "$sparkplug/certificates/#").await,
        vec![(
            "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1".to_string(),
            b"nbirth".to_vec(),
        )],
        "a birth certificate must still be stored after the limit is reached"
    );

    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn birth_over_the_size_limit_clears_its_certificate() {
    let port = next_test_port();
    let agent = start_agent(
        port,
        Limits {
            messages: 0,
            message_size: 16,
            presence: false,
            sparkplug_aware: true,
        },
    )
    .await;

    let edge = connect(port, "edge-1").await;
    edge.publish_qos1("spBv1.0/plant/NBIRTH/edge-1", vec![1; 16])
        .await
        .unwrap();
    assert_eq!(
        stored_retained(port, "scada-host", "$sparkplug/certificates/#").await,
        vec![(
            "$sparkplug/certificates/spBv1.0/plant/NBIRTH/edge-1".to_string(),
            vec![1; 16],
        )],
        "a birth within the limit must be stored as a certificate"
    );

    edge.publish_qos1("spBv1.0/plant/NBIRTH/edge-1", vec![2; 17])
        .await
        .unwrap();
    assert_eq!(
        stored_retained(port, "scada-host", "$sparkplug/certificates/#").await,
        Vec::<(String, Vec<u8>)>::new(),
        "a birth over the limit must not be stored and must not leave the previous certificate behind"
    );

    agent.stop().await;
}
