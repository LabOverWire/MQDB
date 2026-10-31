// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

const NAMED_CLIENTS: usize = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assigned_client_id_never_matches_a_connected_client() {
    let dir = TempDir::new().expect("agent dir");
    let db = Database::open_without_background_tasks(dir.path().join("db"))
        .await
        .expect("open database");
    let port = next_test_port();
    let agent = MqdbAgent::new(db)
        .with_bind_address(SocketAddr::from(([127, 0, 0, 1], port)))
        .with_anonymous(true);
    let (handle, mut ready_rx, shutdown) = agent.start().await.expect("the agent starts");
    tokio::time::timeout(Duration::from_secs(10), ready_rx.wait_for(|ready| *ready))
        .await
        .expect("the agent becomes ready in time")
        .expect("the agent signals readiness");
    let url = format!("mqtt://127.0.0.1:{port}");

    let named: [MqttClient; NAMED_CLIENTS] =
        std::array::from_fn(|index| MqttClient::new(format!("auto-{index}")));
    for client in &named {
        client.connect(&url).await.expect("a named client connects");
    }

    let unnamed = MqttClient::new("");
    unnamed
        .connect(&url)
        .await
        .expect("a client with an empty id connects");
    let assigned = unnamed.client_id().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let held = (0..NAMED_CLIENTS).find(|index| assigned == format!("auto-{index}"));
    assert!(
        !assigned.is_empty() && held.is_none(),
        "the broker assigned {assigned:?}, an id a connected client already holds"
    );
    for (index, client) in named.iter().enumerate() {
        assert!(
            client.is_connected().await,
            "auto-{index} was disconnected by the client assigned {assigned:?}"
        );
    }

    unnamed.disconnect().await.expect("disconnect unnamed");
    for client in &named {
        client.disconnect().await.expect("disconnect named");
    }
    shutdown
        .send(())
        .expect("the agent listens for shutdown until it stops");
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the agent should stop after shutdown")
        .expect("the agent task should not panic");
}
