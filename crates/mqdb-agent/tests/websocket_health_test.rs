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
    let mut received = Vec::new();
    if stream.write_all(rest.as_bytes()).await.is_ok() {
        let head = read_until_head_end(&mut stream).await;
        if head.starts_with("HTTP/1.1 101") {
            let _ = stream.write_all(&MQTT_CONNECT_FRAME).await;
        }
        received.extend_from_slice(head.as_bytes());
    }
    let tail = read_until_closed(&mut stream, Duration::from_secs(3)).await;
    assert!(
        tail.is_some(),
        "a connection still in its handshake at shutdown must be closed"
    );
    received.extend(tail.unwrap_or_default());

    assert!(
        !String::from_utf8_lossy(&received).contains("101 Switching Protocols"),
        "no WebSocket upgrade may complete after the agent stopped: {received:?}"
    );
    assert!(
        !contains_connack_frame(&received),
        "no MQTT session may be accepted after the agent stopped: {received:?}"
    );
}

async fn read_until_closed(stream: &mut TcpStream, deadline: Duration) -> Option<Vec<u8>> {
    let mut received = Vec::new();
    let mut chunk = [0u8; 1024];
    let reading = async {
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => received.extend_from_slice(&chunk[..n]),
            }
        }
    };
    tokio::time::timeout(deadline, reading).await.ok()?;
    Some(received)
}

fn contains_connack_frame(bytes: &[u8]) -> bool {
    bytes
        .windows(3)
        .any(|window| window[0] == 0x82 && window[2] == 0x20)
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
    let packet_type = tokio::time::timeout(Duration::from_secs(3), first_packet_type(&mut stream))
        .await
        .expect("the broker must answer CONNECT");
    assert_eq!(
        packet_type, 0x20,
        "the broker must answer CONNECT with CONNACK"
    );
    let mut connack_rest = [0u8; 64];
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut connack_rest)).await;

    agent.stop().await;

    let after_stop = read_until_closed(&mut stream, Duration::from_secs(3)).await;
    assert!(
        after_stop.is_some(),
        "an MQTT session over WebSocket must be closed by agent stop"
    );
    let after_stop = after_stop.unwrap_or_default();
    assert!(
        after_stop
            .windows(5)
            .any(|frame| frame[0] == 0x82 && frame[2] == 0xE0 && frame[4] == 0x8B),
        "a connected MQTT 5 session must get the shutdown DISCONNECT: {after_stop:?}"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_a_connection_flood_stops_promptly() {
    for attempt in 0..15 {
        let agent = start_agent_with_ws().await;
        let ws_port = agent.ws_port;
        let (halfway_tx, halfway_rx) = std::sync::mpsc::channel();
        let flood = std::thread::spawn(move || {
            let held: [Option<std::net::TcpStream>; 60] = std::array::from_fn(|index| {
                let stream = std::net::TcpStream::connect(("127.0.0.1", ws_port)).ok();
                if index == 30 {
                    let _ = halfway_tx.send(());
                }
                stream
            });
            held
        });
        tokio::task::spawn_blocking(move || halfway_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let _ = agent.shutdown.send(());
        let started = std::time::Instant::now();
        let stopped = tokio::time::timeout(Duration::from_secs(8), agent.handle).await;
        let held = flood.join().unwrap();
        assert!(
            stopped.is_ok() && started.elapsed() < Duration::from_secs(3),
            "attempt {attempt}: agent stop took {:?} behind connections accepted as shutdown was sent",
            started.elapsed()
        );
        drop(held);
        drop(agent.db_dir);
    }
}

const STALLED_CONNECT_FRAME: [u8; 22] = [
    0x82, 0x90, 0, 0, 0, 0, 0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 0x02, 0, 0, 0, 0, 1, b's',
];

const STALLED_SUBSCRIBE_FRAME: [u8; 21] = [
    0x82, 0x8f, 0, 0, 0, 0, 0x82, 13, 0, 1, 0, 0, 7, b's', b't', b'a', b'l', b'l', b'/', b't', 0,
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_is_not_held_by_a_client_that_stopped_reading() {
    let agent = start_agent_with_ws().await;
    let mut stalled = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    stalled
        .write_all(format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();
    assert!(
        read_until_head_end(&mut stalled)
            .await
            .starts_with("HTTP/1.1 101")
    );
    stalled.write_all(&STALLED_CONNECT_FRAME).await.unwrap();
    stalled.write_all(&STALLED_SUBSCRIBE_FRAME).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = MqttClient::new("stall-publisher");
    publisher
        .connect(&format!("ws://127.0.0.1:{}/mqtt", agent.ws_port))
        .await
        .unwrap();
    let payload = vec![b'p'; 16 * 1024];
    for _ in 0..2000 {
        publisher
            .publish_qos1("stall/t", payload.clone())
            .await
            .unwrap();
    }

    let started = std::time::Instant::now();
    let _ = agent.shutdown.send(());
    let stopped = tokio::time::timeout(Duration::from_secs(20), agent.handle).await;
    let elapsed = started.elapsed();
    assert!(
        stopped.is_ok() && elapsed < Duration::from_secs(8),
        "agent stop took {elapsed:?} behind a WebSocket client that stopped reading"
    );
    assert!(
        peer_closed(&mut stalled).await,
        "the connection of a client that stopped reading must not outlive agent stop"
    );
    drop(agent.db_dir);
}

const PINGREQ_FRAME: [u8; 8] = [0x82, 0x82, 0, 0, 0, 0, 0xC0, 0x00];

async fn peer_closed(stream: &mut TcpStream) -> bool {
    for _ in 0..10 {
        if stream.write_all(&PINGREQ_FRAME).await.is_err() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn upgrade_only(port: u16) -> (TcpStream, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();
    let head = read_until_head_end(&mut stream).await;
    (stream, head)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgraded_connections_without_connect_keep_their_handshake_slot() {
    let agent = start_agent_with_ws().await;
    let mut silent: [Option<TcpStream>; 128] = std::array::from_fn(|_| None);
    for slot in &mut silent {
        let (stream, head) = upgrade_only(agent.ws_port).await;
        assert!(head.starts_with("HTTP/1.1 101"));
        *slot = Some(stream);
    }

    let mut next = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    next.write_all(format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();
    let mut first = [0u8; 1];
    let early = tokio::time::timeout(Duration::from_secs(2), next.read(&mut first)).await;
    assert!(
        early.is_err(),
        "a connection must wait while 128 upgraded connections have not sent CONNECT"
    );

    drop(silent);
    let head = read_until_head_end(&mut next).await;
    assert!(
        head.starts_with("HTTP/1.1 101"),
        "the waiting connection must upgrade once slots free: {head:?}"
    );
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_sessions_release_their_handshake_slot() {
    let agent = start_agent_with_ws().await;
    let url = format!("ws://127.0.0.1:{}/mqtt", agent.ws_port);
    let clients: [MqttClient; 130] =
        std::array::from_fn(|index| MqttClient::new(format!("slot-release-{index}")));
    for client in &clients {
        tokio::time::timeout(Duration::from_secs(3), client.connect(&url))
            .await
            .expect("an MQTT session must not keep a handshake slot after its CONNACK")
            .unwrap();
    }
    for client in &clients {
        client.disconnect().await.unwrap();
    }
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgraded_connection_without_connect_does_not_delay_stop() {
    let agent = start_agent_with_ws().await;
    let (mut stream, head) = upgrade_only(agent.ws_port).await;
    assert!(head.starts_with("HTTP/1.1 101"));

    let started = std::time::Instant::now();
    let _ = agent.shutdown.send(());
    let stopped = tokio::time::timeout(Duration::from_secs(8), agent.handle).await;
    let elapsed = started.elapsed();
    assert!(
        stopped.is_ok() && elapsed < Duration::from_secs(3),
        "agent stop took {elapsed:?} behind a connection that upgraded but sent no CONNECT"
    );

    let _ = stream.write_all(&MQTT_CONNECT_FRAME).await;
    let reply = read_until_closed(&mut stream, Duration::from_secs(3))
        .await
        .unwrap_or_default();
    assert!(
        !contains_connack_frame(&reply),
        "a CONNECT sent after agent stop must not be accepted: {reply:?}"
    );
    drop(agent.db_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_request_on_health_path_is_answered_as_health() {
    let agent = start_agent_with_ws().await;
    let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
        .await
        .unwrap();
    stream
        .write_all(format!("GET /health HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();

    let response = read_with_deadline(&mut stream, Duration::from_secs(5)).await;

    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK\r\n"),
        "GET /health is a health probe with or without upgrade headers: {response:?}"
    );
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reply_survives_unread_request_bytes() {
    let agent = start_agent_with_ws().await;
    let body = [b'b'; 65536];
    for attempt in 0..10 {
        let mut stream = TcpStream::connect(("127.0.0.1", agent.ws_port))
            .await
            .unwrap();
        let head = b"GET /health HTTP/1.1\r\nHost: x\r\nContent-Length: 65536\r\n\r\n";
        let _ = stream.write_all(head).await;
        let _ = stream.write_all(&body).await;
        let response = read_with_deadline(&mut stream, Duration::from_secs(5)).await;
        assert!(
            String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK\r\n"),
            "attempt {attempt}: the health reply was lost: {} bytes received",
            response.len()
        );
    }
    agent.stop().await;
}

fn scram_connect_frame(index: u8) -> [u8; 64] {
    let client_id = [b'a' + index / 26, b'a' + index % 26];
    let parts: [&[u8]; 8] = [
        &[0x82, 0x80 | 58, 0, 0, 0, 0, 0x10, 56, 0, 4],
        b"MQTT",
        &[5, 0x02, 0, 60, 41, 0x15, 0, 13],
        b"SCRAM-SHA-256",
        &[0x16, 0, 22],
        b"n,,n=tester,r=abcdefgh",
        &[0, 2],
        &client_id,
    ];
    let mut frame = [0u8; 64];
    let mut at = 0;
    for part in parts {
        frame[at..at + part.len()].copy_from_slice(part);
        at += part.len();
    }
    assert_eq!(at, frame.len());
    frame
}

async fn first_packet_type(stream: &mut TcpStream) -> u8 {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).await.unwrap();
    let extended = match header[1] & 0x7f {
        126 => 2,
        127 => 8,
        _ => 0,
    };
    let mut length = [0u8; 8];
    stream.read_exact(&mut length[..extended]).await.unwrap();
    let mut packet_type = [0u8; 1];
    stream.read_exact(&mut packet_type).await.unwrap();
    packet_type[0]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enhanced_auth_exchanges_keep_their_handshake_slot() {
    let tmp = TempDir::new().unwrap();
    let scram_file = tmp.path().join("scram");
    let line = mqtt5::broker::auth_mechanisms::generate_scram_credential_line_with_iterations(
        "tester", "secret", 10_000,
    )
    .unwrap();
    std::fs::write(&scram_file, format!("{line}\n")).unwrap();
    let db = Database::open(tmp.path().join("db")).await.unwrap();
    let ws_port = next_test_port();
    let agent = MqdbAgent::new(db)
        .with_bind_address(local_addr(next_test_port()))
        .with_ws_bind_address(local_addr(ws_port))
        .with_scram_file(scram_file);
    let (handle, mut ready_rx, shutdown) = agent.start().await.unwrap();
    ready_rx.wait_for(|ready| *ready).await.unwrap();

    let mut pending: [Option<TcpStream>; 128] = std::array::from_fn(|_| None);
    for (index, slot) in (0u8..).zip(pending.iter_mut()) {
        let (mut stream, head) = upgrade_only(ws_port).await;
        assert!(head.starts_with("HTTP/1.1 101"));
        stream.write_all(&scram_connect_frame(index)).await.unwrap();
        let packet_type =
            tokio::time::timeout(Duration::from_secs(3), first_packet_type(&mut stream))
                .await
                .expect("the broker must answer a SCRAM CONNECT");
        assert_eq!(packet_type, 0xF0, "the broker must answer with AUTH");
        *slot = Some(stream);
    }

    let mut next = TcpStream::connect(("127.0.0.1", ws_port)).await.unwrap();
    next.write_all(format!("GET /mqtt HTTP/1.1\r\n{UPGRADE_HEADERS}\r\n").as_bytes())
        .await
        .unwrap();
    let mut first = [0u8; 1];
    let early = tokio::time::timeout(Duration::from_secs(2), next.read(&mut first)).await;
    assert!(
        early.is_err(),
        "a connection must wait while 128 connections are mid enhanced-auth without a CONNACK"
    );

    drop(pending);
    drop(next);
    let _ = shutdown.send(());
    let _ = handle.await;
    drop(tmp);
}
