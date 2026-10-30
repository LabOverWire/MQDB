// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqtt5::broker::BrokerConfig;
use mqtt5::broker::MqttBroker;
use mqtt5::broker::ResourceMonitor;
use mqtt5::broker::auth::AuthProvider;
use mqtt5::broker::client_handler::ClientHandler;
use mqtt5::broker::router::MessageRouter;
use mqtt5::broker::storage::DynamicStorage;
use mqtt5::broker::sys_topics::BrokerStats;
use mqtt5::broker::transport::BrokerTransport;
use mqtt5::broker::websocket_server::{WebSocketServerConfig, accept_websocket_connection};
use mqtt5::time::Duration;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

const MAX_REQUEST_HEAD: usize = 8192;
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_PATH: &str = "/health";
const HEALTH_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}";

pub(super) struct WebSocketListener {
    listener: TcpListener,
    config: BrokerConfig,
}

#[derive(Clone)]
struct WebSocketBroker {
    config: Arc<BrokerConfig>,
    router: Arc<MessageRouter>,
    auth_provider: Arc<dyn AuthProvider>,
    storage: Option<Arc<DynamicStorage>>,
    stats: Arc<BrokerStats>,
    resource_monitor: Arc<ResourceMonitor>,
    shutdown_tx: broadcast::Sender<()>,
}

impl WebSocketListener {
    pub(super) async fn bind(addr: SocketAddr, config: &BrokerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!(ws_bind = %addr, "WebSocket listener configured");
        Ok(Self {
            listener,
            config: config.clone(),
        })
    }

    pub(super) fn spawn(
        self,
        broker: &MqttBroker,
        shutdown_tx: broadcast::Sender<()>,
    ) -> tokio::task::JoinHandle<()> {
        let Self { listener, config } = self;
        let mut broker_ready_rx = broker.ready_receiver();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let ws_broker = WebSocketBroker {
            config: Arc::new(config),
            router: broker.router(),
            auth_provider: broker.auth_provider(),
            storage: broker.storage(),
            stats: broker.stats(),
            resource_monitor: broker.resource_monitor(),
            shutdown_tx,
        };
        tokio::spawn(async move {
            tokio::select! {
                ready = broker_ready_rx.wait_for(|ready| *ready) => {
                    if ready.is_err() {
                        error!("broker stopped before the WebSocket listener could start");
                        return;
                    }
                }
                _ = shutdown_rx.recv() => return,
            }
            info!(health_path = HEALTH_PATH, "WebSocket listener accepting");
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((stream, addr)) => ws_broker.admit(stream, addr).await,
                        Err(e) => error!("WebSocket accept error: {e}"),
                    },
                    _ = shutdown_rx.recv() => {
                        debug!("WebSocket listener shutting down");
                        break;
                    }
                }
            }
        })
    }
}

impl WebSocketBroker {
    async fn admit(&self, stream: TcpStream, addr: SocketAddr) {
        if !self.resource_monitor.can_accept_connection(addr.ip()).await {
            warn!("WebSocket connection rejected from {addr}: resource limits exceeded");
            return;
        }
        if let Err(e) = stream.set_nodelay(true) {
            warn!(addr = %addr, "failed to set TCP_NODELAY on accepted socket: {e}");
        }
        let broker = self.clone();
        tokio::spawn(async move { broker.serve(stream, addr).await });
    }

    async fn serve(self, mut stream: TcpStream, addr: SocketAddr) {
        let head = match tokio::time::timeout(REQUEST_HEAD_TIMEOUT, read_request_head(&mut stream))
            .await
        {
            Ok(Ok(head)) => head,
            Ok(Err(e)) => {
                debug!("WebSocket request from {addr} failed: {e}");
                return;
            }
            Err(_) => {
                debug!("WebSocket request from {addr} timed out");
                return;
            }
        };

        if is_health_request(&head) {
            if let Err(e) = respond_healthy(&mut stream).await {
                debug!("health response to {addr} failed: {e}");
            }
            return;
        }

        let (read_half, write_half) = stream.into_split();
        let replayed = tokio::io::join(Cursor::new(head).chain(read_half), write_half);
        let ws_stream = match accept_websocket_connection(
            replayed,
            &WebSocketServerConfig::new(),
            addr,
        )
        .await
        {
            Ok(ws_stream) => ws_stream,
            Err(e) => {
                error!("WebSocket handshake failed: {e}");
                return;
            }
        };

        let handler = ClientHandler::new(
            BrokerTransport::websocket(ws_stream),
            addr,
            self.config,
            self.router,
            self.auth_provider,
            self.storage,
            self.stats,
            self.resource_monitor,
            self.shutdown_tx.subscribe(),
        );
        match handler.run().await {
            Err(e) if e.is_normal_disconnect() => debug!("Client handler finished"),
            Err(e) => warn!("Client handler error: {e}"),
            Ok(()) => {}
        }
    }
}

async fn read_request_head(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut head = [0u8; MAX_REQUEST_HEAD];
    let mut len = 0;
    while len < MAX_REQUEST_HEAD && !head[..len].windows(4).any(|w| w == b"\r\n\r\n") {
        let read = stream.read(&mut head[len..]).await?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        len += read;
    }
    Ok(head[..len].to_vec())
}

fn is_health_request(head: &[u8]) -> bool {
    let request_line = head.split(|&b| b == b'\r').next().unwrap_or_default();
    let mut parts = request_line.split(|&b| b == b' ');
    let method = parts.next();
    let path = parts
        .next()
        .and_then(|target| target.split(|&b| b == b'?').next());
    method == Some(b"GET") && path == Some(HEALTH_PATH.as_bytes())
}

async fn respond_healthy(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(HEALTH_RESPONSE).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::is_health_request;

    #[test]
    fn health_request_matches_get_health_only() {
        assert!(is_health_request(
            b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"
        ));
        assert!(is_health_request(b"GET /health?probe=alb HTTP/1.1\r\n\r\n"));
        assert!(!is_health_request(b"GET /mqtt HTTP/1.1\r\n\r\n"));
        assert!(!is_health_request(b"POST /health HTTP/1.1\r\n\r\n"));
        assert!(!is_health_request(b"GET /healthz HTTP/1.1\r\n\r\n"));
        assert!(!is_health_request(b""));
    }
}
