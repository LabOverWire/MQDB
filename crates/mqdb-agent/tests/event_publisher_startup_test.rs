// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::config::DatabaseConfig;
use mqdb_core::types::ScopeConfig;
use mqtt5::client::MqttClient;
use serde_json::json;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

struct RunningAgent {
    db: Database,
    port: u16,
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl RunningAgent {
    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("agent did not stop")
            .unwrap();
    }
}

async fn start_agent(
    config: impl FnOnce(DatabaseConfig) -> DatabaseConfig,
) -> (RunningAgent, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open_with_config(config(
        DatabaseConfig::new(tmp.path().join("agent")).without_background_tasks(),
    ))
    .await
    .unwrap();
    let port = next_test_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let agent = MqdbAgent::new(db.clone())
        .with_bind_address(addr)
        .with_anonymous(true);
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    while !*ready_rx.borrow() {
        ready_rx.changed().await.unwrap();
    }
    (
        RunningAgent {
            db,
            port,
            handle,
            shutdown,
        },
        tmp,
    )
}

async fn watch_events(
    port: u16,
    client_id: &str,
    entity: &str,
) -> (MqttClient, flume::Receiver<String>) {
    let subscriber = MqttClient::new(client_id);
    subscriber
        .connect(&format!("mqtt://127.0.0.1:{port}"))
        .await
        .unwrap();
    let (tx, rx) = flume::unbounded();
    subscriber
        .subscribe(&format!("$DB/{entity}/events/#"), move |msg| {
            let _ = tx.try_send(msg.topic.to_string());
        })
        .await
        .unwrap();
    (subscriber, rx)
}

async fn create(db: &Database, entity: &str, id: &str) {
    db.create(
        entity.to_string(),
        json!({"id": id}),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_right_after_ready_publishes_its_change_event() {
    for trial in 0..5 {
        let (agent, tmp) = start_agent(|config| config).await;
        let (subscriber, events) =
            watch_events(agent.port, &format!("watcher-{trial}"), "widget").await;

        create(&agent.db, "widget", &format!("w-{trial}")).await;

        let event = tokio::time::timeout(Duration::from_secs(2), events.recv_async()).await;
        assert!(
            event.is_ok(),
            "trial {trial}: no change event for a write made right after ready"
        );
        subscriber.disconnect().await.unwrap();
        agent.stop().await;
        tmp.close().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_keeps_running_after_falling_behind() {
    let (agent, tmp) = start_agent(|config| config.with_event_capacity(1)).await;
    let (subscriber, events) = watch_events(agent.port, "watcher-lag", "burst").await;

    let burst: Vec<_> = (0..2000)
        .map(|i| {
            let db = agent.db.clone();
            tokio::spawn(async move { create(&db, "burst", &format!("b-{i}")).await })
        })
        .collect();
    for task in burst {
        task.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    while events.try_recv().is_ok() {}

    create(&agent.db, "burst", "after-burst").await;
    let topic = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let topic = events.recv_async().await.unwrap();
            if topic.contains("after-burst") {
                return topic;
            }
        }
    })
    .await;
    assert!(
        topic.is_ok(),
        "no change event after the publisher fell behind"
    );
    subscriber.disconnect().await.unwrap();
    agent.stop().await;
    tmp.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_right_after_start_stops_the_agent() {
    for _ in 0..5 {
        let tmp = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path().join("agent"))
            .await
            .unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{}", next_test_port()).parse().unwrap();
        let agent = MqdbAgent::new(db)
            .with_bind_address(addr)
            .with_anonymous(true);
        let (handle, ready_rx, shutdown) = agent.start().await.unwrap();
        shutdown.send(()).unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            stopped.is_ok(),
            "agent did not stop after an early shutdown"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !*ready_rx.borrow(),
            "agent reported ready after it was shut down before becoming ready"
        );
        tmp.close().unwrap();
    }
}
