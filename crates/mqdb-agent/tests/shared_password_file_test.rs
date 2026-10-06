// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::types::ConnectOptions;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

async fn start_agent(
    data_dir: &Path,
    password_file: &Path,
) -> (u16, tokio::sync::broadcast::Sender<()>) {
    let db = Database::open_without_background_tasks(data_dir)
        .await
        .unwrap();
    let port = next_test_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(addr)
        .with_password_file(password_file.to_path_buf());
    let (_handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !*ready_rx.borrow() {
            ready_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("agent did not become ready");
    (port, shutdown)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agents_sharing_a_password_file_leave_it_unchanged() {
    let tmp = TempDir::new().unwrap();
    let password_file = tmp.path().join("passwd");
    let hash = PasswordAuthProvider::hash_password("secret").unwrap();
    let original = format!("admin:{hash}\n");
    std::fs::write(&password_file, &original).unwrap();

    let dir_a = tmp.path().join("agent-a");
    let dir_b = tmp.path().join("agent-b");
    let (first, second) = tokio::join!(
        start_agent(&dir_a, &password_file),
        start_agent(&dir_b, &password_file),
    );

    assert_eq!(std::fs::read_to_string(&password_file).unwrap(), original);
    for (port, _) in [&first, &second] {
        let client = MqttClient::new(format!("admin-check-{port}"));
        let options =
            ConnectOptions::new(format!("admin-check-{port}")).with_credentials("admin", "secret");
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{port}"), options))
            .await
            .expect("admin must still be able to log in");
        let _ = client.disconnect().await;
    }
    let _ = first.1.send(());
    let _ = second.1.send(());
    tmp.close().unwrap();
}
