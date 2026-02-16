// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportError};
use super::{NodeId, PartitionId};
use bebytes::BeBytes;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::CertificateDer;
use std::collections::{HashMap, VecDeque};
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, trace, warn};

const SEND_TIMEOUT_MS: u64 = 5000;
const INBOX_CHANNEL_CAPACITY: usize = 16384;

struct PeerConnection {
    _connection: Connection,
    send_stream: tokio::sync::Mutex<SendStream>,
}

const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

pub struct LocalPublishRequest {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub user_properties: Vec<(String, String)>,
}

pub struct QuicDirectTransport {
    node_id: NodeId,
    endpoint: Arc<RwLock<Option<Endpoint>>>,
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    inbox_tx: flume::Sender<InboundMessage>,
    inbox_rx: flume::Receiver<InboundMessage>,
    requeue_buffer: Arc<Mutex<VecDeque<InboundMessage>>>,
    message_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    server_addr: Arc<RwLock<Option<SocketAddr>>>,
    local_publish_tx: flume::Sender<LocalPublishRequest>,
    local_publish_rx: flume::Receiver<LocalPublishRequest>,
    ca_file: Arc<std::sync::RwLock<Option<std::path::PathBuf>>>,
    #[cfg(feature = "dev-insecure")]
    insecure: Arc<AtomicBool>,
}

impl std::fmt::Debug for QuicDirectTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicDirectTransport")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl Clone for QuicDirectTransport {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            endpoint: self.endpoint.clone(),
            peers: self.peers.clone(),
            inbox_tx: self.inbox_tx.clone(),
            inbox_rx: self.inbox_rx.clone(),
            requeue_buffer: self.requeue_buffer.clone(),
            message_notify: self.message_notify.clone(),
            connected: self.connected.clone(),
            server_addr: self.server_addr.clone(),
            local_publish_tx: self.local_publish_tx.clone(),
            local_publish_rx: self.local_publish_rx.clone(),
            ca_file: self.ca_file.clone(),
            #[cfg(feature = "dev-insecure")]
            insecure: self.insecure.clone(),
        }
    }
}

impl QuicDirectTransport {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        let (inbox_tx, inbox_rx) = flume::bounded(INBOX_CHANNEL_CAPACITY);
        let (local_publish_tx, local_publish_rx) = flume::bounded(100_000);

        Self {
            node_id,
            endpoint: Arc::new(RwLock::new(None)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            inbox_tx,
            inbox_rx,
            requeue_buffer: Arc::new(Mutex::new(VecDeque::new())),
            message_notify: Arc::new(Notify::new()),
            connected: Arc::new(AtomicBool::new(false)),
            server_addr: Arc::new(RwLock::new(None)),
            local_publish_tx,
            local_publish_rx,
            ca_file: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(feature = "dev-insecure")]
            insecure: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_ca_file(&self, ca_path: std::path::PathBuf) {
        if let Ok(mut guard) = self.ca_file.write() {
            *guard = Some(ca_path);
        }
    }

    #[cfg(feature = "dev-insecure")]
    pub fn set_insecure(&self, insecure: bool) {
        self.insecure.store(insecure, Ordering::SeqCst);
        if insecure {
            warn!(
                "QUIC transport configured with insecure TLS - certificates will NOT be verified"
            );
        }
    }

    #[must_use]
    pub fn local_publish_rx(&self) -> flume::Receiver<LocalPublishRequest> {
        self.local_publish_rx.clone()
    }

    pub fn log_queue_stats(&self) {
        let inbox_len = self.inbox_rx.len();
        let publish_len = self.local_publish_rx.len();
        if inbox_len > 100 || publish_len > 100 {
            warn!(inbox_len, publish_len, "quic transport queue backlog");
        }
    }

    /// Bind the QUIC endpoint to the given address.
    ///
    /// # Errors
    /// Returns an error if the endpoint cannot be bound or TLS config fails.
    pub async fn bind(
        &self,
        addr: SocketAddr,
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<(), TransportError> {
        let server_config = build_server_config(cert_path, key_path)?;

        let endpoint = Endpoint::server(server_config, addr).map_err(|e| {
            TransportError::SendFailed(format!("failed to bind QUIC endpoint: {e}"))
        })?;

        info!(addr = %addr, node = self.node_id.get(), "QUIC direct transport bound");

        *self.server_addr.write().await = Some(addr);
        *self.endpoint.write().await = Some(endpoint.clone());
        self.connected.store(true, Ordering::SeqCst);

        let inbox_tx = self.inbox_tx.clone();
        let notify = self.message_notify.clone();
        let local_node = self.node_id;
        let peers = self.peers.clone();

        tokio::spawn(async move {
            acceptor_task(endpoint, inbox_tx, notify, local_node, peers).await;
        });

        Ok(())
    }

    /// Connect to a peer node via QUIC.
    ///
    /// # Errors
    /// Returns an error if the connection fails or the stream cannot be opened.
    pub async fn connect_to_peer(
        &self,
        peer_id: NodeId,
        peer_addr: SocketAddr,
    ) -> Result<(), TransportError> {
        let endpoint_guard = self.endpoint.read().await;
        let endpoint = endpoint_guard
            .as_ref()
            .ok_or(TransportError::NotConnected)?;

        let ca_file = self.ca_file.read().ok().and_then(|g| g.clone());
        #[cfg(feature = "dev-insecure")]
        let client_config = if self.insecure.load(Ordering::SeqCst) {
            build_client_config_insecure()?
        } else {
            build_client_config_secure(ca_file.as_deref())?
        };
        #[cfg(not(feature = "dev-insecure"))]
        let client_config = build_client_config_secure(ca_file.as_deref())?;

        let connection = endpoint
            .connect_with(client_config, peer_addr, "localhost")
            .map_err(|e| TransportError::SendFailed(format!("failed to connect to peer: {e}")))?
            .await
            .map_err(|e| TransportError::SendFailed(format!("connection to peer failed: {e}")))?;

        #[cfg(feature = "dev-insecure")]
        info!(
            peer = peer_id.get(),
            addr = %peer_addr,
            insecure = self.insecure.load(Ordering::SeqCst),
            "connected to peer via QUIC"
        );
        #[cfg(not(feature = "dev-insecure"))]
        info!(
            peer = peer_id.get(),
            addr = %peer_addr,
            "connected to peer via QUIC"
        );

        let (send_stream, recv_stream) = connection
            .open_bi()
            .await
            .map_err(|e| TransportError::SendFailed(format!("failed to open stream: {e}")))?;

        let header = self.node_id.get().to_be_bytes();
        {
            let mut stream = tokio::sync::Mutex::new(send_stream);
            stream
                .get_mut()
                .write_all(&header)
                .await
                .map_err(|e| TransportError::SendFailed(format!("failed to send header: {e}")))?;

            let peer_conn = PeerConnection {
                _connection: connection.clone(),
                send_stream: stream,
            };

            self.peers.write().await.insert(peer_id, peer_conn);
        }

        let inbox_tx = self.inbox_tx.clone();
        let notify = self.message_notify.clone();
        let local_node = self.node_id;

        tokio::spawn(async move {
            receiver_task(recv_stream, peer_id, inbox_tx, notify, local_node).await;
        });

        Ok(())
    }

    #[must_use]
    pub fn inbox_rx(&self) -> flume::Receiver<InboundMessage> {
        self.inbox_rx.clone()
    }

    pub async fn wait_for_message(&self) {
        self.message_notify.notified().await;
    }

    fn serialize_message(&self, message: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.node_id.get().to_be_bytes());
        buf.push(message.message_type());

        match message {
            ClusterMessage::Heartbeat(hb) => {
                buf.extend_from_slice(&hb.to_be_bytes());
            }
            ClusterMessage::Write(w) | ClusterMessage::WriteRequest(w) => {
                buf.extend_from_slice(&w.to_bytes());
            }
            ClusterMessage::Ack(ack) => {
                buf.extend_from_slice(&ack.to_be_bytes());
            }
            ClusterMessage::DeathNotice { node_id }
            | ClusterMessage::DrainNotification { node_id } => {
                buf.extend_from_slice(&node_id.get().to_be_bytes());
            }
            ClusterMessage::RequestVote(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
            ClusterMessage::AppendEntries(req) => {
                buf.extend_from_slice(&req.to_bytes());
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
            ClusterMessage::CatchupRequest(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::CatchupResponse(resp) => {
                buf.extend_from_slice(&resp.to_bytes());
            }
            ClusterMessage::ForwardedPublish(fwd) => {
                buf.extend_from_slice(&fwd.to_bytes());
            }
            ClusterMessage::SnapshotRequest(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::SnapshotChunk(chunk) => {
                buf.extend_from_slice(&chunk.to_bytes());
            }
            ClusterMessage::SnapshotComplete(complete) => {
                buf.extend_from_slice(&complete.to_be_bytes());
            }
            ClusterMessage::QueryRequest { partition, request } => {
                buf.extend_from_slice(&partition.get().to_be_bytes());
                buf.extend_from_slice(&request.to_bytes());
            }
            ClusterMessage::QueryResponse(response) => {
                buf.extend_from_slice(&response.to_bytes());
            }
            ClusterMessage::BatchReadRequest(request) => {
                buf.extend_from_slice(&request.to_bytes());
            }
            ClusterMessage::BatchReadResponse(response) => {
                buf.extend_from_slice(&response.to_bytes());
            }
            ClusterMessage::WildcardBroadcast(broadcast) => {
                buf.extend_from_slice(&broadcast.to_be_bytes());
            }
            ClusterMessage::TopicSubscriptionBroadcast(broadcast) => {
                buf.extend_from_slice(&broadcast.to_be_bytes());
            }
            ClusterMessage::PartitionUpdate(update) => {
                buf.extend_from_slice(&update.to_be_bytes());
            }
            ClusterMessage::JsonDbRequest { partition, request } => {
                buf.extend_from_slice(&partition.get().to_be_bytes());
                buf.extend_from_slice(&request.to_bytes());
            }
            ClusterMessage::JsonDbResponse(response) => {
                buf.extend_from_slice(&response.to_bytes());
            }
            ClusterMessage::UniqueReserveRequest(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::UniqueReserveResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
            ClusterMessage::UniqueCommitRequest(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::UniqueCommitResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
            ClusterMessage::UniqueReleaseRequest(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::UniqueReleaseResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
        }

        buf
    }

    async fn send_to_peer(
        &self,
        peer_id: NodeId,
        message: &ClusterMessage,
    ) -> Result<(), TransportError> {
        let payload = self.serialize_message(message);

        #[allow(clippy::cast_possible_truncation)]
        let len_prefix = (payload.len() as u32).to_be_bytes();

        let peers = self.peers.read().await;
        let peer = peers
            .get(&peer_id)
            .ok_or(TransportError::NodeNotFound(peer_id))?;

        let mut stream = peer.send_stream.lock().await;

        let timeout = Duration::from_millis(SEND_TIMEOUT_MS);

        tokio::time::timeout(timeout, stream.write_all(&len_prefix))
            .await
            .map_err(|_| TransportError::SendFailed("send timeout (length prefix)".to_string()))?
            .map_err(|e| TransportError::SendFailed(format!("failed to write length: {e}")))?;

        tokio::time::timeout(timeout, stream.write_all(&payload))
            .await
            .map_err(|_| TransportError::SendFailed("send timeout (payload)".to_string()))?
            .map_err(|e| TransportError::SendFailed(format!("failed to write payload: {e}")))?;

        trace!(
            from = self.node_id.get(),
            to = peer_id.get(),
            msg_type = message.type_name(),
            "sent QUIC message"
        );

        Ok(())
    }
}

impl ClusterTransport for QuicDirectTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        self.send_to_peer(to, &message).await
    }

    async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        let peers: Vec<NodeId> = self.peers.read().await.keys().copied().collect();

        for peer_id in peers {
            if let Err(e) = self.send_to_peer(peer_id, &message).await {
                warn!(peer = peer_id.get(), error = %e, "failed to broadcast to peer");
            }
        }

        Ok(())
    }

    async fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        _message: ClusterMessage,
    ) -> Result<(), TransportError> {
        Err(TransportError::PartitionNotFound(partition))
    }

    fn recv(&self) -> Option<InboundMessage> {
        if let Ok(mut requeue) = self.requeue_buffer.try_lock()
            && let Some(msg) = requeue.pop_front()
        {
            return Some(msg);
        }
        self.inbox_rx.try_recv().ok()
    }

    fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
        self.recv()
    }

    fn pending_count(&self) -> usize {
        self.inbox_rx.len()
    }

    fn requeue(&self, msg: InboundMessage) {
        if let Ok(mut requeue) = self.requeue_buffer.lock() {
            requeue.push_front(msg);
        }
    }

    async fn queue_local_publish(&self, topic: String, payload: Vec<u8>, qos: u8) {
        let queue_len = self.local_publish_tx.len();
        if queue_len > 1000 && queue_len.is_multiple_of(1000) {
            warn!(topic, queue_len, "local_publish queue growing");
        }
        if let Err(e) = self.local_publish_tx.try_send(LocalPublishRequest {
            topic: topic.clone(),
            payload,
            qos,
            retain: false,
            user_properties: Vec::new(),
        }) {
            warn!(topic, "local_publish queue full, dropping message: {e}");
        }
    }

    async fn queue_local_publish_with_properties(
        &self,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        user_properties: Vec<(String, String)>,
    ) {
        let queue_len = self.local_publish_tx.len();
        if queue_len > 1000 && queue_len.is_multiple_of(1000) {
            warn!(topic, queue_len, "local_publish queue growing");
        }
        if let Err(e) = self.local_publish_tx.try_send(LocalPublishRequest {
            topic: topic.clone(),
            payload,
            qos,
            retain: false,
            user_properties,
        }) {
            warn!(topic, "local_publish queue full, dropping message: {e}");
        }
    }

    async fn queue_local_publish_retained(&self, topic: String, payload: Vec<u8>, qos: u8) {
        if let Err(e) = self.local_publish_tx.try_send(LocalPublishRequest {
            topic: topic.clone(),
            payload,
            qos,
            retain: true,
            user_properties: Vec::new(),
        }) {
            warn!(
                topic,
                "local_publish queue full, dropping retained message: {e}"
            );
        }
    }
}

async fn acceptor_task(
    endpoint: Endpoint,
    inbox_tx: flume::Sender<InboundMessage>,
    notify: Arc<Notify>,
    local_node: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
) {
    info!(node = local_node.get(), "QUIC acceptor task started");

    while let Some(incoming) = endpoint.accept().await {
        let connection = match incoming.await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "failed to accept QUIC connection");
                continue;
            }
        };

        let inbox_tx = inbox_tx.clone();
        let notify = notify.clone();
        let peers = peers.clone();

        tokio::spawn(async move {
            if let Err(e) =
                handle_incoming_connection(connection, inbox_tx, notify, local_node, peers).await
            {
                debug!(error = %e, "incoming connection handler failed");
            }
        });
    }
}

async fn handle_incoming_connection(
    connection: Connection,
    inbox_tx: flume::Sender<InboundMessage>,
    notify: Arc<Notify>,
    local_node: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
) -> Result<(), TransportError> {
    let (send_stream, mut recv_stream) = connection
        .accept_bi()
        .await
        .map_err(|e| TransportError::SendFailed(format!("failed to accept stream: {e}")))?;

    let mut header = [0u8; 2];
    recv_stream
        .read_exact(&mut header)
        .await
        .map_err(|e| TransportError::SendFailed(format!("failed to read header: {e}")))?;

    let peer_node_id = u16::from_be_bytes(header);
    let peer_node = NodeId::validated(peer_node_id)
        .ok_or_else(|| TransportError::SendFailed("invalid peer node ID".to_string()))?;

    info!(peer = peer_node.get(), "accepted incoming QUIC connection");

    {
        let peer_conn = PeerConnection {
            _connection: connection.clone(),
            send_stream: tokio::sync::Mutex::new(send_stream),
        };
        peers.write().await.insert(peer_node, peer_conn);
    }

    receiver_task(recv_stream, peer_node, inbox_tx, notify, local_node).await;
    Ok(())
}

async fn receiver_task(
    mut recv_stream: RecvStream,
    peer_node: NodeId,
    inbox_tx: flume::Sender<InboundMessage>,
    notify: Arc<Notify>,
    local_node: NodeId,
) {
    trace!(peer = peer_node.get(), "receiver task started");

    loop {
        let mut len_buf = [0u8; 4];
        if recv_stream.read_exact(&mut len_buf).await.is_err() {
            debug!(peer = peer_node.get(), "peer disconnected");
            break;
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > MAX_MESSAGE_SIZE {
            error!(peer = peer_node.get(), len = msg_len, "message too large");
            break;
        }

        let mut buf = vec![0u8; msg_len];
        if recv_stream.read_exact(&mut buf).await.is_err() {
            debug!(peer = peer_node.get(), "failed to read message body");
            break;
        }

        if let Some(msg) = parse_message(&buf, local_node) {
            if let Err(flume::TrySendError::Full(dropped)) = inbox_tx.try_send(msg) {
                let msg_type = match &dropped.message {
                    ClusterMessage::Heartbeat(_) => "Heartbeat",
                    ClusterMessage::Write(_) => "Write",
                    ClusterMessage::WriteRequest(_) => "WriteRequest",
                    ClusterMessage::Ack(_) => "Ack",
                    ClusterMessage::DeathNotice { .. } => "DeathNotice",
                    ClusterMessage::DrainNotification { .. } => "DrainNotification",
                    ClusterMessage::RequestVote(_) => "RequestVote",
                    ClusterMessage::RequestVoteResponse(_) => "RequestVoteResponse",
                    ClusterMessage::AppendEntries(_) => "AppendEntries",
                    ClusterMessage::AppendEntriesResponse(_) => "AppendEntriesResponse",
                    ClusterMessage::CatchupRequest(_) => "CatchupRequest",
                    ClusterMessage::CatchupResponse(_) => "CatchupResponse",
                    ClusterMessage::ForwardedPublish(_) => "ForwardedPublish",
                    ClusterMessage::SnapshotRequest(_) => "SnapshotRequest",
                    ClusterMessage::SnapshotChunk(_) => "SnapshotChunk",
                    ClusterMessage::SnapshotComplete(_) => "SnapshotComplete",
                    _ => "Other",
                };
                warn!(
                    peer = peer_node.get(),
                    msg_type, "inbox queue full, dropping message"
                );
            } else {
                notify.notify_one();
            }
        }
    }

    debug!(peer = peer_node.get(), "receiver task ended");
}

fn parse_message(payload: &[u8], local_node: NodeId) -> Option<InboundMessage> {
    let inbound = InboundMessage::parse_from_payload(payload, local_node)?;
    trace!(
        from = inbound.from.get(),
        msg_type = inbound.message.message_type(),
        "received QUIC cluster message"
    );
    Some(inbound)
}

fn build_server_config(cert_path: &Path, key_path: &Path) -> Result<ServerConfig, TransportError> {
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| TransportError::SendFailed(format!("failed to open cert file: {e}")))?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| TransportError::SendFailed(format!("failed to open key file: {e}")))?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .filter_map(Result::ok)
        .collect();

    if certs.is_empty() {
        return Err(TransportError::SendFailed(
            "no certificates found in cert file".to_string(),
        ));
    }

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| TransportError::SendFailed(format!("failed to parse key file: {e}")))?
        .ok_or_else(|| {
            TransportError::SendFailed("no private key found in key file".to_string())
        })?;

    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TransportError::SendFailed(format!("failed to build TLS config: {e}")))?;

    let server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).map_err(|e| {
            TransportError::SendFailed(format!("failed to create QUIC config: {e}"))
        })?,
    ));

    Ok(server_config)
}

fn build_client_config_secure(
    ca_file: Option<&Path>,
) -> Result<quinn::ClientConfig, TransportError> {
    let mut root_store = rustls::RootCertStore::empty();

    if let Some(ca_path) = ca_file {
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| TransportError::SendFailed(format!("failed to open CA file: {e}")))?;
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(ca_file))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| TransportError::SendFailed(format!("failed to parse CA cert: {e}")))?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| TransportError::SendFailed(format!("failed to add CA cert: {e}")))?;
        }
        debug!(ca_path = %ca_path.display(), "loaded custom CA certificate");
    } else {
        let cert_result = rustls_native_certs::load_native_certs();
        for err in &cert_result.errors {
            debug!(error = %err, "error loading native certificate");
        }
        for cert in cert_result.certs {
            if let Err(e) = root_store.add(cert) {
                debug!(error = %e, "failed to add certificate to root store");
            }
        }
    }

    if root_store.is_empty() {
        return Err(TransportError::SendFailed(
            "no trusted root certificates found - use --quic-ca to specify a CA certificate"
                .to_string(),
        ));
    }

    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(|e| {
        TransportError::SendFailed(format!("failed to create QUIC client config: {e}"))
    })?;

    Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

#[cfg(feature = "dev-insecure")]
fn build_client_config_insecure() -> Result<quinn::ClientConfig, TransportError> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(|e| {
        TransportError::SendFailed(format!("failed to create QUIC client config: {e}"))
    })?;

    Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

#[cfg(feature = "dev-insecure")]
#[derive(Debug)]
struct SkipServerVerification;

#[cfg(feature = "dev-insecure")]
impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::super::protocol::Heartbeat;
    use super::*;

    #[test]
    fn serialize_and_parse_heartbeat() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = QuicDirectTransport::new(node1);

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = transport.serialize_message(&msg);

        let node2 = NodeId::validated(2).unwrap();
        let parsed = parse_message(&bytes, node2);
        assert!(parsed.is_some());

        match parsed.unwrap().message {
            ClusterMessage::Heartbeat(h) => {
                assert_eq!(h.node_id(), 1);
            }
            _ => panic!("expected heartbeat"),
        }
    }

    #[test]
    fn parse_ignores_own_messages() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = QuicDirectTransport::new(node1);

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = transport.serialize_message(&msg);

        let parsed = parse_message(&bytes, node1);
        assert!(parsed.is_none());
    }

    #[test]
    fn new_transport_creates_channels() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = QuicDirectTransport::new(node1);

        assert_eq!(transport.local_node(), node1);
        assert!(!transport.inbox_rx.is_disconnected());
        assert!(!transport.local_publish_rx.is_disconnected());
        assert_eq!(transport.inbox_rx.capacity(), Some(INBOX_CHANNEL_CAPACITY));
        assert_eq!(transport.local_publish_rx.capacity(), Some(100_000));
    }

    #[test]
    fn parse_rejects_short_payload() {
        let node1 = NodeId::validated(1).unwrap();
        let result = parse_message(&[0u8, 1u8], node1);
        assert!(result.is_none());

        let result = parse_message(&[], node1);
        assert!(result.is_none());
    }

    #[test]
    fn parse_rejects_invalid_node_id() {
        let node1 = NodeId::validated(1).unwrap();
        let payload = [0u8, 0, 0];
        let result = parse_message(&payload, node1);
        assert!(result.is_none());
    }

    #[test]
    fn parse_rejects_unknown_message_type() {
        let node1 = NodeId::validated(1).unwrap();
        let payload = [0u8, 2, 255, 0, 0, 0, 0];
        let result = parse_message(&payload, node1);
        assert!(result.is_none());
    }

    #[test]
    fn max_message_size_constant_is_10mb() {
        assert_eq!(MAX_MESSAGE_SIZE, 10 * 1024 * 1024);
    }
}
