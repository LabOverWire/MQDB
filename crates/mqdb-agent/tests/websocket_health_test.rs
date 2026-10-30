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

async fn agent_with_ws(ws_addr: SocketAddr) -> (MqdbAgent, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let mqtt_addr: SocketAddr = format!("127.0.0.1:{}", next_test_port()).parse().unwrap();
    let agent = MqdbAgent::new(db)
        .with_bind_address(mqtt_addr)
        .with_ws_bind_address(ws_addr)
        .with_anonymous(true);
    (agent, tmp)
}

fn local_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

async fn start_agent_with_ws() -> RunningAgent {
    let ws_port = next_test_port();
    let (agent, tmp) = agent_with_ws(local_addr(ws_port)).await;
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    RunningAgent {
        ws_port,
        shutdown,
        handle,
        db_dir: tmp,
    }
}

const UPGRADE_HEADERS: &str = "Host: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n";

const MQTT_CONNECT_FRAME: [u8; 22] = [
    0x82, 0x90, 0, 0, 0, 0, 0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 0x02, 0, 60, 0, 0, 1, b'x',
];

async fn read_with_deadline(stream: &mut TcpStream, deadline: Duration) -> Vec<u8> {
    let mut received = Vec::new();
    let _ = tokio::time::timeout(deadline, stream.read_to_end(&mut received)).await;
    received
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
async fn upgrade_on_unknown_path_is_rejected_by_the_handshake() {
    let agent = start_agent_with_ws().await;
    let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    let request = format!("GET /other HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let response = read_with_deadline(&mut stream, Duration::from_secs(5)).await;

    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 404"),
        "the WebSocket handshake must reject an unknown path with 404: {response:?}"
    );
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_request_head_is_closed_by_the_deadline() {
    let agent = start_agent_with_ws().await;
    let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    let padding = "a".repeat(9000);
    let request = format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}X-Pad: {padding}");
    stream.write_all(request.as_bytes()).await.unwrap();

    assert!(
        closes_within(&mut stream, Duration::from_secs(15)).await,
        "a request head that never ends must not hold the socket open"
    );
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_mid_handshake_at_shutdown_gets_no_session() {
    let agent = start_agent_with_ws().await;
    let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    stream.write_all(b"GET /mqtt HTTP/1.1\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    agent.stop().await;

    let rest = format!("{UPGRADE_HEADERS}\r\n");
    if stream.write_all(rest.as_bytes()).await.is_ok()
        && read_until_head_end(&mut stream)
            .await
            .starts_with("HTTP/1.1 101")
    {
        let _ = stream.write_all(&MQTT_CONNECT_FRAME).await;
    }
    assert!(
        closes_within(&mut stream, Duration::from_secs(3)).await,
        "a connection still in its handshake at shutdown must be closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_session_is_closed_by_agent_stop() {
    let agent = start_agent_with_ws().await;
    let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    stream
        .write_all(format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();
    assert!(
        read_until_head_end(&mut stream)
            .await
            .starts_with("HTTP/1.1 101")
    );
    stream.write_all(&MQTT_CONNECT_FRAME).await.unwrap();

    agent.stop().await;

    assert!(
        closes_within(&mut stream, Duration::from_secs(3)).await,
        "an MQTT session over WebSocket must be closed by agent stop"
    );
}

async fn closes_within(stream: &mut TcpStream, deadline: Duration) -> bool {
    let mut chunk = [0u8; 1024];
    let draining = async {
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    };
    tokio::time::timeout(deadline, draining).await.is_ok()
}

async fn read_until_head_end(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    let reading = async {
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(1) => head.push(byte[0]),
                _ => break,
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(3), reading).await;
    String::from_utf8_lossy(&head).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_is_not_served_before_the_agent_is_ready() {
    let ws_port = next_test_port();
    let (agent, tmp) = agent_with_ws(local_addr(ws_port)).await;
    let (handle, ready_rx, shutdown) = agent.start().await.unwrap();

    let response = plain_http_get(ws_port, "/health").await;
    let ready_when_answered = *ready_rx.borrow();

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(
        ready_when_answered,
        "/health answered 200 before the agent reported ready"
    );
    let _ = shutdown.send(());
    let _ = handle.await;
    drop(tmp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_bind_failure_names_the_websocket_listener() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_addr = taken.local_addr().unwrap();
    let (agent, tmp) = agent_with_ws(ws_addr).await;

    let error = match agent.start().await {
        Ok(_) => panic!("the agent must not start when the WebSocket port is taken"),
        Err(e) => e.to_string(),
    };

    assert!(
        error.contains("WebSocket") && error.contains(&ws_addr.to_string()),
        "the bind error must name the WebSocket listener and {ws_addr}: {error:?}"
    );
    drop(taken);
    drop(tmp);
}
