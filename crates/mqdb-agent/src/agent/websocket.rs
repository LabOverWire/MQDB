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
use mqtt5::broker::websocket_server::{
    WebSocketServerConfig, WebSocketStreamWrapper, accept_websocket_connection,
};
use mqtt5::time::Duration;
use std::io::Cursor;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Chain, Join, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch};
use tracing::{debug, error, info, instrument, warn};

const MAX_REQUEST_HEAD: usize = 8192;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const MAX_PENDING_HANDSHAKES: usize = 128;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const BINARY_FRAME: u8 = 0x82;
const CONNACK_PACKET: u8 = 0x20;
const HEAD_TERMINATOR: &[u8] = b"\r\n\r\n";
const HEALTH_PATH: &[u8] = b"/health";
const HEALTH_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}";

type ReplayedStream = ConnectGate<Join<Chain<Cursor<Box<[u8]>>, OwnedReadHalf>, OwnedWriteHalf>>;
type PermitSlot = Arc<Mutex<Option<OwnedSemaphorePermit>>>;

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
}

struct ConnectGate<S> {
    inner: S,
    permit: PermitSlot,
    frames: FrameTracker,
}

#[derive(Default)]
struct FrameTracker {
    header: [u8; 10],
    header_len: usize,
    payload_left: u64,
    binary_payload_start: bool,
    connack_seen: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListenerPhase {
    Accepting,
    Draining,
    Closing,
}

struct ConnectionSignals {
    handler_shutdown: broadcast::Receiver<()>,
    phase: watch::Receiver<ListenerPhase>,
}

struct RequestHead {
    bytes: Box<[u8; MAX_REQUEST_HEAD]>,
    len: usize,
}

impl WebSocketListener {
    #[instrument(skip(config), err)]
    pub(super) async fn bind(addr: SocketAddr, config: &BrokerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("WebSocket listener failed to bind {addr}: {e}"),
            )
        })?;
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
        mut agent_ready_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let Self { listener, config } = self;
        let mut shutdown_rx = shutdown_tx.subscribe();
        let ws_broker = WebSocketBroker {
            config: Arc::new(config),
            router: broker.router(),
            auth_provider: broker.auth_provider(),
            storage: broker.storage(),
            stats: broker.stats(),
            resource_monitor: broker.resource_monitor(),
        };
        tokio::spawn(async move {
            tokio::select! {
                ready = agent_ready_rx.wait_for(|ready| *ready) => {
                    if ready.is_err() {
                        error!("agent stopped before the WebSocket listener could start");
                        return;
                    }
                }
                _ = shutdown_rx.recv() => return,
            }
            info!(health_path = "/health", "WebSocket listener accepting");
            let (open_tx, mut closed_rx) = mpsc::channel::<()>(1);
            let (connection_shutdown_tx, _) = broadcast::channel::<()>(1);
            let (phase_tx, phase_rx) = watch::channel(ListenerPhase::Accepting);
            let pending = Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES));
            loop {
                let permit = tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    permit = Arc::clone(&pending).acquire_owned() => match permit {
                        Ok(permit) => permit,
                        Err(_) => break,
                    },
                };
                let accepted = tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, addr)) => {
                        let connection = ws_broker.clone();
                        let signals = ConnectionSignals {
                            handler_shutdown: connection_shutdown_tx.subscribe(),
                            phase: phase_rx.clone(),
                        };
                        let open = open_tx.clone();
                        tokio::spawn(async move {
                            connection.serve(stream, addr, permit, signals).await;
                            drop(open);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "WebSocket accept error");
                        drop(permit);
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                    }
                }
            }
            debug!("WebSocket listener shutting down");
            drop(listener);
            drop(phase_rx);
            let _ = phase_tx.send(ListenerPhase::Draining);
            let _ = connection_shutdown_tx.send(());
            drop(open_tx);
            if tokio::time::timeout(DRAIN_TIMEOUT, closed_rx.recv())
                .await
                .is_err()
            {
                warn!("WebSocket connections did not close within the drain timeout, closing them");
                let _ = phase_tx.send(ListenerPhase::Closing);
                closed_rx.recv().await;
            }
        })
    }
}

impl WebSocketBroker {
    #[instrument(skip_all, fields(%addr))]
    async fn serve(
        self,
        stream: TcpStream,
        addr: SocketAddr,
        pending_permit: OwnedSemaphorePermit,
        signals: ConnectionSignals,
    ) {
        let ConnectionSignals {
            handler_shutdown,
            mut phase,
        } = signals;
        if let Err(e) = stream.set_nodelay(true) {
            warn!(error = %e, "failed to set TCP_NODELAY on accepted socket");
        }
        let permit_slot = PermitSlot::default();
        let upgraded = tokio::select! {
            upgraded = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                self.upgrade(stream, addr, Arc::clone(&permit_slot)),
            ) => upgraded,
            _ = phase.wait_for(|phase| *phase != ListenerPhase::Accepting) => {
                debug!("WebSocket connection closed by shutdown before its handshake finished");
                return;
            }
        };
        let ws_stream = match upgraded {
            Ok(Some(ws_stream)) => ws_stream,
            Ok(None) => return,
            Err(_) => {
                debug!("WebSocket handshake timed out");
                return;
            }
        };
        if let Ok(mut slot) = permit_slot.lock() {
            *slot = Some(pending_permit);
        }

        let handler = ClientHandler::new(
            BrokerTransport::websocket(ws_stream),
            addr,
            self.config,
            self.router,
            self.auth_provider,
            self.storage,
            self.stats,
            self.resource_monitor,
            handler_shutdown,
        );
        let mut running = std::pin::pin!(handler.run());
        let finished_before_shutdown = tokio::select! {
            outcome = &mut running => Some(outcome),
            _ = phase.wait_for(|phase| *phase != ListenerPhase::Accepting) => None,
        };
        let outcome = match finished_before_shutdown {
            Some(outcome) => outcome,
            None if permit_slot.lock().is_ok_and(|slot| slot.is_some()) => {
                debug!("WebSocket connection closed by shutdown before its CONNACK");
                return;
            }
            None => tokio::select! {
                outcome = &mut running => outcome,
                _ = phase.wait_for(|phase| *phase == ListenerPhase::Closing) => {
                    debug!("WebSocket connection closed after the shutdown drain timed out");
                    return;
                }
            },
        };
        match outcome {
            Err(e) if e.is_normal_disconnect() => debug!("Client handler finished"),
            Err(e) => warn!(error = %e, "Client handler error"),
            Ok(()) => {}
        }
    }

    async fn upgrade(
        &self,
        mut stream: TcpStream,
        addr: SocketAddr,
        permit: PermitSlot,
    ) -> Option<WebSocketStreamWrapper<ReplayedStream>> {
        let head = match RequestHead::read(&mut stream).await {
            Ok(head) => head,
            Err(e) => {
                debug!(error = %e, "WebSocket request head not received");
                return None;
            }
        };

        if head.is_health_request() {
            if let Err(e) = respond_healthy(&mut stream).await {
                debug!(error = %e, "health response failed");
            }
            return None;
        }

        if !self.resource_monitor.can_accept_connection(addr.ip()).await {
            warn!("WebSocket connection rejected: resource limits exceeded");
            return None;
        }

        let (read_half, write_half) = stream.into_split();
        let replayed = ConnectGate {
            inner: tokio::io::join(head.into_reader().chain(read_half), write_half),
            permit,
            frames: FrameTracker::default(),
        };
        match accept_websocket_connection(replayed, &WebSocketServerConfig::new(), addr).await {
            Ok(ws_stream) => Some(ws_stream),
            Err(e) => {
                debug!(error = %e, "WebSocket handshake failed");
                None
            }
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ConnectGate<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ConnectGate<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(cx, buf);
        let this = self.get_mut();
        if let Poll::Ready(Ok(n)) = written
            && let Ok(mut slot) = this.permit.lock()
            && slot.is_some()
            && let Some(sent) = buf.get(..n)
            && this.frames.observe(sent)
        {
            slot.take();
        }
        written
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl FrameTracker {
    fn observe(&mut self, mut written: &[u8]) -> bool {
        while !self.connack_seen
            && let Some((&byte, tail)) = written.split_first()
        {
            if self.payload_left > 0 {
                if self.binary_payload_start {
                    self.binary_payload_start = false;
                    self.connack_seen = byte == CONNACK_PACKET;
                }
                let skip = usize::try_from(self.payload_left)
                    .unwrap_or(usize::MAX)
                    .min(written.len());
                self.payload_left = self
                    .payload_left
                    .saturating_sub(u64::try_from(skip).unwrap_or(u64::MAX));
                written = &written[skip..];
                continue;
            }
            self.header[self.header_len] = byte;
            self.header_len += 1;
            written = tail;
            if let Some(payload_len) = self.complete_header() {
                self.binary_payload_start = self.header[0] == BINARY_FRAME && payload_len > 0;
                self.payload_left = payload_len;
                self.header_len = 0;
            }
        }
        self.connack_seen
    }

    fn complete_header(&self) -> Option<u64> {
        let length_byte = *self.header.get(1).filter(|_| self.header_len >= 2)?;
        let extended = match length_byte & 0x7f {
            126 => 2,
            127 => 8,
            _ => 0,
        };
        if self.header_len < 2 + extended {
            return None;
        }
        Some(if extended == 0 {
            u64::from(length_byte & 0x7f)
        } else {
            self.header[2..2 + extended]
                .iter()
                .fold(0u64, |length, byte| (length << 8) | u64::from(*byte))
        })
    }
}

impl RequestHead {
    async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Self> {
        let mut head = Self {
            bytes: Box::new([0u8; MAX_REQUEST_HEAD]),
            len: 0,
        };
        while head.len < MAX_REQUEST_HEAD {
            let read = stream.read(&mut head.bytes[head.len..]).await?;
            if read == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            let scan_from = head.len.saturating_sub(HEAD_TERMINATOR.len() - 1);
            head.len += read;
            if head.bytes[scan_from..head.len]
                .windows(HEAD_TERMINATOR.len())
                .any(|window| window == HEAD_TERMINATOR)
            {
                break;
            }
        }
        Ok(head)
    }

    fn is_health_request(&self) -> bool {
        let head = &self.bytes[..self.len];
        let request_line = head.split(|&b| b == b'\r').next().unwrap_or_default();
        let mut parts = request_line.split(|&b| b == b' ');
        let method = parts.next();
        let path = parts
            .next()
            .and_then(|target| target.split(|&b| b == b'?').next());
        method == Some(b"GET") && path == Some(HEALTH_PATH)
    }

    fn into_reader(self) -> Cursor<Box<[u8]>> {
        Cursor::new(Box::from(&self.bytes[..self.len]))
    }
}

async fn respond_healthy(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(HEALTH_RESPONSE).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::{ConnectGate, FrameTracker, PermitSlot, RequestHead};

    fn writes_connack(bytes: &[u8]) -> bool {
        FrameTracker::default().observe(bytes)
    }
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, AsyncWriteExt};
    use tokio::sync::Semaphore;

    struct PartialWriter {
        accepts: usize,
    }

    impl AsyncWrite for PartialWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len().min(self.accepts)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn gate_with_permit(accepts: usize) -> (ConnectGate<PartialWriter>, PermitSlot) {
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let slot = PermitSlot::default();
        if let Ok(mut held) = slot.lock() {
            *held = Some(permit);
        }
        let gate = ConnectGate {
            inner: PartialWriter { accepts },
            permit: Arc::clone(&slot),
            frames: FrameTracker::default(),
        };
        (gate, slot)
    }

    fn pong_then_connack(pong_payload: u8) -> [u8; 132] {
        let mut out = [pong_payload; 132];
        out[0] = 0x8A;
        out[1] = 125;
        out[127..].copy_from_slice(&[0x82, 0x03, 0x20, 0x01, 0x00]);
        out
    }

    fn slot_released(slot: &PermitSlot) -> bool {
        slot.lock().is_ok_and(|held| held.is_none())
    }

    #[tokio::test]
    async fn connack_written_after_a_partial_frame_releases_the_slot() {
        let (mut gate, slot) = gate_with_permit(50).await;
        let out = pong_then_connack(0x7f);

        let mut sent = gate.write(&out[..127]).await.unwrap();
        while sent < out.len() {
            sent += gate.write(&out[sent..]).await.unwrap();
        }

        assert!(
            slot_released(&slot),
            "writing a CONNACK frame must release the handshake slot"
        );
    }

    #[tokio::test]
    async fn slot_is_kept_until_the_connack_bytes_are_written() {
        let (mut gate, slot) = gate_with_permit(50).await;
        let out = pong_then_connack(0x7f);

        let sent = gate.write(&out).await.unwrap();

        assert_eq!(sent, 50);
        assert!(
            !slot_released(&slot),
            "the slot must stay held while the CONNACK is still unsent"
        );
    }

    #[tokio::test]
    async fn connack_bytes_inside_a_pong_payload_do_not_release_the_slot() {
        let (mut gate, slot) = gate_with_permit(1024).await;
        let mut pong = [0u8; 12];
        pong[0] = 0x8A;
        pong[1] = 10;
        pong[2..7].copy_from_slice(&[0x82, 0x03, 0x20, 0x01, 0x00]);

        gate.write_all(&pong).await.unwrap();

        assert!(
            !slot_released(&slot),
            "a pong echoing CONNACK-like bytes must not release the slot"
        );
    }

    #[test]
    fn connack_is_found_only_in_a_binary_frame_carrying_it() {
        let connack = [0x82, 0x03, 0x20, 0x01, 0x00];
        let auth = [0x82, 0x03, 0xF0, 0x01, 0x18];
        let pong = [0x8A, 0x02, b'h', b'i'];
        let pong_then_connack = [0x8A, 0x02, b'h', b'i', 0x82, 0x03, 0x20, 0x01, 0x00];
        let auth_then_connack = [0x82, 0x03, 0xF0, 0x01, 0x18, 0x82, 0x03, 0x20, 0x01, 0x00];
        assert!(writes_connack(&connack));
        assert!(writes_connack(&pong_then_connack));
        assert!(writes_connack(&auth_then_connack));
        assert!(!writes_connack(&auth));
        assert!(!writes_connack(&pong));
        assert!(!writes_connack(b"HTTP/1.1 101 Switching Protocols\r\n"));
        assert!(!writes_connack(&[]));
    }

    async fn head_of(request: &[u8]) -> RequestHead {
        let mut reader = request;
        RequestHead::read(&mut reader).await.unwrap()
    }

    #[tokio::test]
    async fn health_request_matches_get_health_only() {
        assert!(
            head_of(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
                .await
                .is_health_request()
        );
        assert!(
            head_of(b"GET /health?probe=alb HTTP/1.1\r\n\r\n")
                .await
                .is_health_request()
        );
        assert!(
            !head_of(b"GET /mqtt HTTP/1.1\r\n\r\n")
                .await
                .is_health_request()
        );
        assert!(
            !head_of(b"POST /health HTTP/1.1\r\n\r\n")
                .await
                .is_health_request()
        );
        assert!(
            !head_of(b"GET /healthz HTTP/1.1\r\n\r\n")
                .await
                .is_health_request()
        );
    }

    #[tokio::test]
    async fn head_read_stops_at_terminator_split_across_reads() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let reading = tokio::spawn(async move { RequestHead::read(&mut server).await });
        for chunk in [&b"GET /health HTTP/1.1\r"[..], b"\n", b"\r", b"\n"] {
            tokio::io::AsyncWriteExt::write_all(&mut client, chunk)
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
        let head = reading.await.unwrap().unwrap();
        assert_eq!(head.len, 24);
        assert!(head.is_health_request());
    }

    #[tokio::test]
    async fn replay_reader_keeps_only_the_bytes_read() {
        let request = b"GET /mqtt HTTP/1.1\r\nHost: x\r\n\r\n";
        let head = head_of(request).await;
        let reader = head.into_reader();
        assert_eq!(reader.get_ref().len(), request.len());
    }
}
