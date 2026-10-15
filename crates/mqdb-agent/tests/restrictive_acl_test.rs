// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
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
        self.connect_as(port, &client_id).await
    }

    async fn connect_as(&self, port: u16, client_id: &str) -> MqttClient {
        let client = MqttClient::new(client_id);
        let options = ConnectOptions::new(client_id).with_credentials(self.name, &self.password);
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
            .await
            .unwrap();
        client
    }
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
        .expect("the agent must answer a user the ACL grants $DB/#")
        .unwrap();
    serde_json::from_slice(&payload).unwrap()
}

struct TestAgent {
    port: u16,
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    tmp: TempDir,
}

impl TestAgent {
    async fn start(users: &[&User], acl: &str, presence: bool) -> Self {
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
        let acl_file = tmp.path().join("acl");
        std::fs::write(&acl_file, acl).unwrap();

        let db = Database::open_without_background_tasks(tmp.path().join("agent"))
            .await
            .unwrap();
        let port = next_test_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let agent = MqdbAgent::new(db)
            .with_bind_address(addr)
            .with_password_file(password_file)
            .with_acl_file(acl_file)
            .with_presence(presence);
        let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !*ready_rx.borrow() {
                ready_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("an ACL that names only application users must not stop the agent");
        Self {
            port,
            handle,
            shutdown,
            tmp,
        }
    }

    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("agent did not stop")
            .unwrap();
        self.tmp.close().unwrap();
    }
}

const APP_ONLY_ACL: &str = "user app topic $DB/# permission readwrite\n\
                            user app topic resp/# permission readwrite\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acl_naming_only_application_users_still_serves_requests() {
    let app = User::new("app");
    let outsider = User::new("outsider");
    let agent = TestAgent::start(&[&app, &outsider], APP_ONLY_ACL, false).await;

    let app_client = app.connect(agent.port).await;
    let created = request(
        &app_client,
        "$DB/notes/create",
        &json!({"id": "n1", "title": "plan"}),
    )
    .await;
    assert_eq!(created["status"], "ok", "{created}");
    let read = request(&app_client, "$DB/notes/n1", &json!({})).await;
    assert_eq!(read["status"], "ok", "{read}");

    let outsider_client = outsider.connect(agent.port).await;
    assert!(
        outsider_client.subscribe("resp/#", |_| {}).await.is_err(),
        "a user the ACL grants nothing must not subscribe"
    );
    outsider_client
        .publish(
            "$DB/notes/create",
            serde_json::to_vec(&json!({"id": "n2", "title": "smuggled"})).unwrap(),
        )
        .await
        .unwrap();
    let missing = request(&app_client, "$DB/notes/n2", &json!({})).await;
    assert_eq!(
        missing["status"], "error",
        "a user the ACL grants nothing must not write: {missing}"
    );

    outsider_client.disconnect().await.unwrap();
    app_client.disconnect().await.unwrap();
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acl_naming_only_application_users_still_publishes_presence() {
    let app = User::new("app");
    let agent = TestAgent::start(&[&app], APP_ONLY_ACL, true).await;

    let watcher = app.connect(agent.port).await;
    let (tx, rx) = flume::unbounded::<Value>();
    watcher
        .subscribe("$DB/_presence/peer-1", move |msg| {
            if let Ok(event) = serde_json::from_slice(&msg.payload) {
                let _ = tx.try_send(event);
            }
        })
        .await
        .unwrap();

    let peer = app.connect_as(agent.port, "peer-1").await;
    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = rx.recv_async().await.unwrap();
            if event["event"] == "connect" {
                return event;
            }
        }
    })
    .await
    .expect("presence must be published under an ACL that names only application users");
    assert_eq!(event["client_id"], "peer-1", "{event}");
    assert_eq!(event["user_id"], "app", "{event}");

    peer.disconnect().await.unwrap();
    watcher.disconnect().await.unwrap();
    agent.stop().await;
}
