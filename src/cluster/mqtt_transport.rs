#![allow(deprecated)]

use super::protocol::{
    BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardedPublish,
    Heartbeat, JsonDbRequest, JsonDbResponse, QueryRequest, QueryResponse, ReplicationAck,
    ReplicationWrite, TopicSubscriptionBroadcast, UniqueCommitRequest, UniqueCommitResponse,
    UniqueReleaseRequest, UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse,
    WildcardBroadcast,
};
use super::raft::{
    AppendEntriesRequest, AppendEntriesResponse, PartitionUpdate, RequestVoteRequest,
    RequestVoteResponse,
};
use super::snapshot::{SnapshotChunk, SnapshotComplete, SnapshotRequest};
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportError};
use super::{NodeId, PartitionId};
use bebytes::BeBytes;
use mqtt5::QoS;
use mqtt5::client::MqttClient;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const CLUSTER_TOPIC_PREFIX: &str = "_mqdb/cluster";
const REPLICATION_TOPIC_PREFIX: &str = "_mqdb/repl";
const FORWARD_TOPIC_PREFIX: &str = "_mqdb/forward";
const INBOX_CHANNEL_CAPACITY: usize = 16384;

#[deprecated(since = "0.2.0", note = "use QuicDirectTransport instead - MQTT bridges are retained for historical reference only")]
#[derive(Clone)]
pub struct MqttTransport {
    node_id: NodeId,
    client: MqttClient,
    forward_client: MqttClient,
    inbox_tx: flume::Sender<InboundMessage>,
    inbox_rx: flume::Receiver<InboundMessage>,
    requeue_buffer: Arc<Mutex<VecDeque<InboundMessage>>>,
    connected: Arc<AtomicBool>,
    message_notify: Arc<Notify>,
}

impl std::fmt::Debug for MqttTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttTransport")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl MqttTransport {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        let client_id = format!("mqdb-node-{}", node_id.get());
        let client = MqttClient::new(&client_id);

        let forward_client_id = format!("mqdb-forward-{}", node_id.get());
        let forward_client = MqttClient::new(&forward_client_id);

        let (tx, rx) = flume::bounded(INBOX_CHANNEL_CAPACITY);

        Self {
            node_id,
            client,
            forward_client,
            inbox_tx: tx,
            inbox_rx: rx,
            requeue_buffer: Arc::new(Mutex::new(VecDeque::new())),
            connected: Arc::new(AtomicBool::new(false)),
            message_notify: Arc::new(Notify::new()),
        }
    }

    pub async fn wait_for_message(&self) {
        self.message_notify.notified().await;
    }

    #[must_use]
    pub fn inbox_rx(&self) -> flume::Receiver<InboundMessage> {
        self.inbox_rx.clone()
    }

    #[must_use]
    pub fn client(&self) -> &MqttClient {
        &self.client
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.inbox_rx.len()
    }

    /// # Errors
    /// Returns `SendFailed` if connection fails.
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    pub async fn connect(&self, broker_addr: &str) -> Result<(), TransportError> {
        Box::pin(self.connect_with_credentials(broker_addr, None, None)).await
    }

    /// # Errors
    /// Returns `SendFailed` if connection fails.
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    pub async fn connect_with_credentials(
        &self,
        broker_addr: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<(), TransportError> {
        if let (Some(user), Some(pass)) = (username, password) {
            let client_id = format!("mqdb-node-{}", self.node_id.get());
            let options =
                mqtt5::types::ConnectOptions::new(&client_id).with_credentials(user, pass);
            Box::pin(self.client.connect_with_options(broker_addr, options))
                .await
                .map_err(|e| TransportError::SendFailed(e.to_string()))?;

            let forward_client_id = format!("mqdb-forward-{}", self.node_id.get());
            let forward_options =
                mqtt5::types::ConnectOptions::new(&forward_client_id).with_credentials(user, pass);
            Box::pin(
                self.forward_client
                    .connect_with_options(broker_addr, forward_options),
            )
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;
        } else {
            self.client
                .connect(broker_addr)
                .await
                .map_err(|e| TransportError::SendFailed(e.to_string()))?;

            self.forward_client
                .connect(broker_addr)
                .await
                .map_err(|e| TransportError::SendFailed(e.to_string()))?;
        }

        self.connected.store(true, Ordering::SeqCst);

        self.subscribe_to_cluster_topics().await?;
        Ok(())
    }

    async fn subscribe_to_cluster_topics(&self) -> Result<(), TransportError> {
        let node_topic = format!("{}/nodes/{}", CLUSTER_TOPIC_PREFIX, self.node_id.get());
        let broadcast_topic = format!("{CLUSTER_TOPIC_PREFIX}/broadcast");
        let heartbeat_topic = format!("{CLUSTER_TOPIC_PREFIX}/heartbeat/+");
        let replication_topic = format!("{REPLICATION_TOPIC_PREFIX}/+/+");

        let tx = self.inbox_tx.clone();
        let notify = self.message_notify.clone();
        let node_id = self.node_id;

        self.client
            .subscribe(&node_topic, {
                let tx = tx.clone();
                let notify = notify.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                        notify.notify_one();
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.client
            .subscribe(&broadcast_topic, {
                let tx = tx.clone();
                let notify = notify.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                        notify.notify_one();
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.client
            .subscribe(&heartbeat_topic, {
                let tx = tx.clone();
                let notify = notify.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                        notify.notify_one();
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.client
            .subscribe(&replication_topic, {
                let tx = tx.clone();
                let notify = notify.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                        notify.notify_one();
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let forward_topic = format!("{FORWARD_TOPIC_PREFIX}/+");
        self.client
            .subscribe(&forward_topic, {
                let tx = tx.clone();
                let notify = notify.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                        notify.notify_one();
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn parse_message(payload: &[u8], local_node: NodeId) -> Option<InboundMessage> {
        if payload.len() < 3 {
            return None;
        }

        let from_id = u16::from_be_bytes([payload[0], payload[1]]);
        let from = NodeId::validated(from_id)?;

        if from == local_node {
            return None;
        }

        let msg_type = payload[2];
        let data = &payload[3..];

        let message = match msg_type {
            0 => {
                let (hb, _) = Heartbeat::try_from_be_bytes(data).ok()?;
                ClusterMessage::Heartbeat(hb)
            }
            10 => {
                let w = ReplicationWrite::from_bytes(data)?;
                ClusterMessage::Write(w)
            }
            15 => {
                let w = ReplicationWrite::from_bytes(data)?;
                ClusterMessage::WriteRequest(w)
            }
            11 => {
                let (ack, _) = ReplicationAck::try_from_be_bytes(data).ok()?;
                ClusterMessage::Ack(ack)
            }
            2 => {
                if data.len() < 2 {
                    return None;
                }
                let dead_id = u16::from_be_bytes([data[0], data[1]]);
                let dead_node = NodeId::validated(dead_id)?;
                ClusterMessage::DeathNotice { node_id: dead_node }
            }
            3 => {
                if data.len() < 2 {
                    return None;
                }
                let drain_id = u16::from_be_bytes([data[0], data[1]]);
                let drain_node = NodeId::validated(drain_id)?;
                ClusterMessage::DrainNotification {
                    node_id: drain_node,
                }
            }
            20 => {
                let (req, _) = RequestVoteRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::RequestVote(req)
            }
            21 => {
                let (resp, _) = RequestVoteResponse::try_from_be_bytes(data).ok()?;
                ClusterMessage::RequestVoteResponse(resp)
            }
            22 => {
                let req = AppendEntriesRequest::from_bytes(data)?;
                ClusterMessage::AppendEntries(req)
            }
            23 => {
                let (resp, _) = AppendEntriesResponse::try_from_be_bytes(data).ok()?;
                ClusterMessage::AppendEntriesResponse(resp)
            }
            12 => {
                let (req, _) = CatchupRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::CatchupRequest(req)
            }
            13 => {
                let resp = CatchupResponse::from_bytes(data)?;
                ClusterMessage::CatchupResponse(resp)
            }
            30 => {
                let fwd = ForwardedPublish::from_bytes(data)?;
                ClusterMessage::ForwardedPublish(fwd)
            }
            40 => {
                let (req, _) = SnapshotRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::SnapshotRequest(req)
            }
            41 => {
                let chunk = SnapshotChunk::from_bytes(data)?;
                ClusterMessage::SnapshotChunk(chunk)
            }
            42 => {
                let (complete, _) = SnapshotComplete::try_from_be_bytes(data).ok()?;
                ClusterMessage::SnapshotComplete(complete)
            }
            50 => {
                if data.len() < 2 {
                    return None;
                }
                let partition_id = u16::from_be_bytes([data[0], data[1]]);
                let partition = PartitionId::new(partition_id)?;
                let request = QueryRequest::from_bytes(&data[2..])?;
                ClusterMessage::QueryRequest { partition, request }
            }
            51 => {
                let response = QueryResponse::from_bytes(data)?;
                ClusterMessage::QueryResponse(response)
            }
            52 => {
                let request = BatchReadRequest::from_bytes(data)?;
                ClusterMessage::BatchReadRequest(request)
            }
            53 => {
                let response = BatchReadResponse::from_bytes(data)?;
                ClusterMessage::BatchReadResponse(response)
            }
            60 => {
                let (broadcast, _) = WildcardBroadcast::try_from_be_bytes(data).ok()?;
                ClusterMessage::WildcardBroadcast(broadcast)
            }
            61 => {
                let (broadcast, _) = TopicSubscriptionBroadcast::try_from_be_bytes(data).ok()?;
                ClusterMessage::TopicSubscriptionBroadcast(broadcast)
            }
            70 => {
                let (update, _) = PartitionUpdate::try_from_be_bytes(data).ok()?;
                ClusterMessage::PartitionUpdate(update)
            }
            54 => {
                if data.len() < 2 {
                    return None;
                }
                let partition_id = u16::from_be_bytes([data[0], data[1]]);
                let partition = PartitionId::new(partition_id)?;
                let request = JsonDbRequest::from_bytes(&data[2..])?;
                ClusterMessage::JsonDbRequest { partition, request }
            }
            55 => {
                let response = JsonDbResponse::from_bytes(data)?;
                ClusterMessage::JsonDbResponse(response)
            }
            80 => {
                let (req, _) = UniqueReserveRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueReserveRequest(req)
            }
            81 => {
                let (resp, _) = UniqueReserveResponse::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueReserveResponse(resp)
            }
            82 => {
                let (req, _) = UniqueCommitRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueCommitRequest(req)
            }
            83 => {
                let (resp, _) = UniqueCommitResponse::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueCommitResponse(resp)
            }
            84 => {
                let (req, _) = UniqueReleaseRequest::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueReleaseRequest(req)
            }
            85 => {
                let (resp, _) = UniqueReleaseResponse::try_from_be_bytes(data).ok()?;
                ClusterMessage::UniqueReleaseResponse(resp)
            }
            _ => return None,
        };

        #[allow(clippy::cast_possible_truncation)]
        let received_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        tracing::trace!(
            from = from.get(),
            msg_type = message.message_type(),
            "received cluster message"
        );

        Some(InboundMessage {
            from,
            message,
            received_at,
        })
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

    /// # Errors
    /// Returns `SendFailed` if publishing the replication write fails.
    pub async fn publish_replication_write(
        &self,
        partition: PartitionId,
        sequence: u64,
        message: &ClusterMessage,
    ) -> Result<(), TransportError> {
        let topic = format!(
            "{}/p{}/seq{}",
            REPLICATION_TOPIC_PREFIX,
            partition.get(),
            sequence
        );
        let payload = self.serialize_message(message);

        self.client
            .publish_qos(&topic, payload, QoS::AtLeastOnce)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// # Errors
    /// Returns `SendFailed` if publishing the forwarded message fails.
    pub async fn forward_publish(
        &self,
        partition: PartitionId,
        message: &ForwardedPublish,
    ) -> Result<(), TransportError> {
        let topic = format!("{}/{}", FORWARD_TOPIC_PREFIX, partition.get());
        let msg = ClusterMessage::ForwardedPublish(message.clone());
        let payload = self.serialize_message(&msg);

        self.forward_client
            .publish_qos(&topic, payload, QoS::AtLeastOnce)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Publish a message to the local broker using the forward client.
    /// This prevents the `on_client_publish` handler from re-forwarding.
    ///
    /// # Errors
    /// Returns `SendFailed` if publishing fails.
    pub async fn publish_locally_with_marker(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
    ) -> Result<(), TransportError> {
        let mqtt_qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        self.forward_client
            .publish_qos(topic, payload.to_vec(), mqtt_qos)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// # Errors
    /// Returns `SendFailed` if disconnection fails.
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    pub async fn disconnect(&self) -> Result<(), TransportError> {
        self.client
            .disconnect()
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let _ = self.forward_client.disconnect().await;

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl ClusterTransport for MqttTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        let start = std::time::Instant::now();
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        let topic = format!("{}/nodes/{}", CLUSTER_TOPIC_PREFIX, to.get());
        let payload = self.serialize_message(&message);
        let msg_type = message.type_name();
        let payload_len = payload.len();
        let t_serialize = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        let qos = match &message {
            ClusterMessage::ForwardedPublish(fwd) => match fwd.qos {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                _ => QoS::ExactlyOnce,
            },
            ClusterMessage::RequestVote(_)
            | ClusterMessage::RequestVoteResponse(_)
            | ClusterMessage::Write(_)
            | ClusterMessage::Ack(_) => QoS::AtMostOnce,
            _ => QoS::AtLeastOnce,
        };

        tracing::debug!(
            from = self.node_id.get(),
            to = to.get(),
            topic = %topic,
            msg_type = %msg_type,
            payload_len = payload.len(),
            "transport send"
        );

        self.client
            .publish_qos(&topic, payload, qos)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;
        let t_publish = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        if msg_type == "Write" {
            tracing::info!(
                node = self.node_id.get(),
                to = to.get(),
                t_serialize,
                t_publish,
                payload_len,
                "transport_send_timing"
            );
        }

        Ok(())
    }

    async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        let topic = match &message {
            ClusterMessage::Heartbeat(_) => {
                format!("{}/heartbeat/{}", CLUSTER_TOPIC_PREFIX, self.node_id.get())
            }
            _ => format!("{CLUSTER_TOPIC_PREFIX}/broadcast"),
        };

        let payload = self.serialize_message(&message);

        let qos = match &message {
            ClusterMessage::Heartbeat(_) => QoS::AtMostOnce,
            _ => QoS::AtLeastOnce,
        };

        self.client
            .publish_qos(&topic, payload, qos)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

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

    async fn queue_local_publish(&self, topic: String, payload: Vec<u8>, _qos: u8) {
        let _ = self
            .forward_client
            .publish_qos(&topic, payload, QoS::AtMostOnce)
            .await;
    }

    async fn queue_local_publish_retained(&self, topic: String, payload: Vec<u8>, _qos: u8) {
        let options = mqtt5::PublishOptions {
            qos: QoS::AtMostOnce,
            retain: true,
            ..Default::default()
        };
        let _ = self
            .forward_client
            .publish_with_options(&topic, payload, options)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize_message_for_test(node_id: NodeId, message: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&node_id.get().to_be_bytes());
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

    #[test]
    fn serialize_and_parse_heartbeat() {
        let node1 = NodeId::validated(1).unwrap();

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = serialize_message_for_test(node1, &msg);

        let node2 = NodeId::validated(2).unwrap();
        let parsed = MqttTransport::parse_message(&bytes, node2);
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

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = serialize_message_for_test(node1, &msg);

        let parsed = MqttTransport::parse_message(&bytes, node1);
        assert!(parsed.is_none());
    }
}
