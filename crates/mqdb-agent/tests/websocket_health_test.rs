// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct RunningAgent {
    ws_port: u16,
    shutdown: tokio::sync::broadcast::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
    db_dir: TempDir,
}

impl RunningAgent {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.await;
        drop(self.db_dir);
    }
}

async fn start_agent_with_ws() -> RunningAgent {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let mqtt_addr: SocketAddr = format!("127.0.0.1:{}", next_test_port()).parse().unwrap();
    let ws_port = next_test_port();
    let ws_addr: SocketAddr = format!("127.0.0.1:{ws_port}").parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(mqtt_addr)
        .with_ws_bind_address(ws_addr)
        .with_anonymous(true);
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    RunningAgent {
        ws_port,
        shutdown,
        handle,
        db_dir: tmp,
    }
}

async fn plain_http_get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUser-Agent: ELB-HealthChecker/2.0\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("the WS port must answer a plain HTTP request")
        .unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_get_health_on_ws_port_returns_200() {
    let agent = start_agent_with_ws().await;

    let response = plain_http_get(agent.ws_port, "/health").await;

    assert!(
        response.starts_with("HTTP/1.1 200 OK\r\n"),
        "unexpected response: {response:?}"
    );
    assert!(response.ends_with("\r\n\r\n{\"status\":\"ok\"}"));

    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_over_websocket_still_connects_on_mqtt_path() {
    let agent = start_agent_with_ws().await;
    let url = format!("ws://127.0.0.1:{}/mqtt", agent.ws_port);

    let subscriber = MqttClient::new("ws-health-sub");
    subscriber.connect(&url).await.unwrap();
    let (tx, rx) = flume::bounded(1);
    subscriber
        .subscribe("ws/health/check", move |msg| {
            let _ = tx.try_send(msg.payload.to_vec());
        })
        .await
        .unwrap();

    let publisher = MqttClient::new("ws-health-pub");
    publisher.connect(&url).await.unwrap();
    publisher
        .publish_qos1("ws/health/check", b"over-ws".to_vec())
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("a message published over WebSocket should arrive")
        .unwrap();
    assert_eq!(payload, b"over-ws");

    publisher.disconnect().await.unwrap();
    subscriber.disconnect().await.unwrap();
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_connection_does_not_block_other_ws_clients() {
    let agent = start_agent_with_ws().await;
    let idle = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();

    let client = MqttClient::new("ws-behind-idle");
    tokio::time::timeout(
        Duration::from_secs(3),
        client.connect(&format!("ws://127.0.0.1:{}/mqtt", agent.ws_port)),
    )
    .await
    .expect("an idle socket must not stall WebSocket handshakes")
    .unwrap();

    let health = tokio::time::timeout(
        Duration::from_secs(3),
        plain_http_get(agent.ws_port, "/health"),
    )
    .await
    .expect("an idle socket must not stall the WS accept loop");
    assert!(health.starts_with("HTTP/1.1 200 OK\r\n"));

    client.disconnect().await.unwrap();
    drop(idle);
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_get_on_other_paths_is_not_healthy() {
    let agent = start_agent_with_ws().await;

    let response = plain_http_get(agent.ws_port, "/mqtt").await;

    assert!(
        !response.starts_with("HTTP/1.1 200"),
        "a non-upgrade GET /mqtt must not report healthy: {response:?}"
    );

    agent.stop().await;
}
