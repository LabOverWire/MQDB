// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::ConnectOptions;
use running_agent::RunningAgent;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

async fn password_agent(password_file: &Path) -> (MqdbAgent, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(dir.path())
        .await
        .unwrap();
    let agent = MqdbAgent::new(db).with_password_file(password_file.to_path_buf());
    (agent, dir)
}

async fn admin_login(port: u16, password: &str) -> bool {
    let client_id = format!("admin-check-{port}");
    let client = MqttClient::new(client_id.clone());
    let options = ConnectOptions::new(client_id).with_credentials("admin", password);
    let connected = Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
        .await
        .is_ok();
    if connected {
        client.disconnect().await.unwrap();
    }
    connected
}

async fn start_agent(password_file: &Path) -> RunningAgent {
    let (agent, dir) = password_agent(password_file).await;
    RunningAgent::start(agent, next_test_port(), dir).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agents_sharing_a_password_file_leave_it_unchanged() {
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let admin_password = uuid::Uuid::new_v4().to_string();
    let hash = PasswordAuthProvider::hash_password(&admin_password).unwrap();
    let original = format!("admin:{hash}\n");
    std::fs::write(&password_file, &original).unwrap();

    let (first, second) = tokio::join!(start_agent(&password_file), start_agent(&password_file));

    assert_eq!(std::fs::read_to_string(&password_file).unwrap(), original);
    for port in [first.port, second.port] {
        assert!(
            admin_login(port, &admin_password).await,
            "admin must still be able to log in"
        );
    }
    first.stop().await;
    second.stop().await;
    tmp.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_service_credentials_keep_the_file_password() {
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let admin_password = uuid::Uuid::new_v4().to_string();
    let hash = PasswordAuthProvider::hash_password(&admin_password).unwrap();
    std::fs::write(&password_file, format!("admin:{hash}\n")).unwrap();

    let (agent, dir) = password_agent(&password_file).await;
    let agent =
        agent.with_service_credentials("admin".to_string(), uuid::Uuid::new_v4().to_string());
    let running = RunningAgent::launch(agent, next_test_port(), dir).await;

    let file_password_accepted = tokio::time::timeout(Duration::from_secs(10), async {
        while !admin_login(running.port, &admin_password).await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        file_password_accepted,
        "the password file's admin entry must not be replaced by explicit service credentials"
    );
    running.stop().await;
    tmp.close().unwrap();
}
