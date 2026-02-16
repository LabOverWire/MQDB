// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

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
use super::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum ClusterMessage {
    Heartbeat(Heartbeat),
    Write(ReplicationWrite),
    WriteRequest(ReplicationWrite),
    Ack(ReplicationAck),
    DeathNotice {
        node_id: NodeId,
    },
    DrainNotification {
        node_id: NodeId,
    },
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    CatchupRequest(CatchupRequest),
    CatchupResponse(CatchupResponse),
    ForwardedPublish(ForwardedPublish),
    SnapshotRequest(SnapshotRequest),
    SnapshotChunk(SnapshotChunk),
    SnapshotComplete(SnapshotComplete),
    QueryRequest {
        partition: PartitionId,
        request: QueryRequest,
    },
    QueryResponse(QueryResponse),
    BatchReadRequest(BatchReadRequest),
    BatchReadResponse(BatchReadResponse),
    WildcardBroadcast(WildcardBroadcast),
    TopicSubscriptionBroadcast(TopicSubscriptionBroadcast),
    PartitionUpdate(PartitionUpdate),
    JsonDbRequest {
        partition: PartitionId,
        request: JsonDbRequest,
    },
    JsonDbResponse(JsonDbResponse),
    UniqueReserveRequest(UniqueReserveRequest),
    UniqueReserveResponse(UniqueReserveResponse),
    UniqueCommitRequest(UniqueCommitRequest),
    UniqueCommitResponse(UniqueCommitResponse),
    UniqueReleaseRequest(UniqueReleaseRequest),
    UniqueReleaseResponse(UniqueReleaseResponse),
}

impl ClusterMessage {
    #[must_use]
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Heartbeat(_) => 0,
            Self::Write(_) => 10,
            Self::WriteRequest(_) => 15,
            Self::Ack(_) => 11,
            Self::DeathNotice { .. } => 2,
            Self::DrainNotification { .. } => 3,
            Self::RequestVote(_) => 20,
            Self::RequestVoteResponse(_) => 21,
            Self::AppendEntries(_) => 22,
            Self::AppendEntriesResponse(_) => 23,
            Self::CatchupRequest(_) => 12,
            Self::CatchupResponse(_) => 13,
            Self::ForwardedPublish(_) => 30,
            Self::SnapshotRequest(_) => 40,
            Self::SnapshotChunk(_) => 41,
            Self::SnapshotComplete(_) => 42,
            Self::QueryRequest { .. } => 50,
            Self::QueryResponse(_) => 51,
            Self::BatchReadRequest(_) => 52,
            Self::BatchReadResponse(_) => 53,
            Self::WildcardBroadcast(_) => 60,
            Self::TopicSubscriptionBroadcast(_) => 61,
            Self::PartitionUpdate(_) => 70,
            Self::JsonDbRequest { .. } => 54,
            Self::JsonDbResponse(_) => 55,
            Self::UniqueReserveRequest(_) => 80,
            Self::UniqueReserveResponse(_) => 81,
            Self::UniqueCommitRequest(_) => 82,
            Self::UniqueCommitResponse(_) => 83,
            Self::UniqueReleaseRequest(_) => 84,
            Self::UniqueReleaseResponse(_) => 85,
        }
    }

    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Heartbeat(_) => "Heartbeat",
            Self::Write(_) => "Write",
            Self::WriteRequest(_) => "WriteRequest",
            Self::Ack(_) => "Ack",
            Self::DeathNotice { .. } => "DeathNotice",
            Self::DrainNotification { .. } => "DrainNotification",
            Self::RequestVote(_) => "RequestVote",
            Self::RequestVoteResponse(_) => "RequestVoteResponse",
            Self::AppendEntries(_) => "AppendEntries",
            Self::AppendEntriesResponse(_) => "AppendEntriesResponse",
            Self::CatchupRequest(_) => "CatchupRequest",
            Self::CatchupResponse(_) => "CatchupResponse",
            Self::ForwardedPublish(_) => "ForwardedPublish",
            Self::SnapshotRequest(_) => "SnapshotRequest",
            Self::SnapshotChunk(_) => "SnapshotChunk",
            Self::SnapshotComplete(_) => "SnapshotComplete",
            Self::QueryRequest { .. } => "QueryRequest",
            Self::QueryResponse(_) => "QueryResponse",
            Self::BatchReadRequest(_) => "BatchReadRequest",
            Self::BatchReadResponse(_) => "BatchReadResponse",
            Self::WildcardBroadcast(_) => "WildcardBroadcast",
            Self::TopicSubscriptionBroadcast(_) => "TopicSubscriptionBroadcast",
            Self::PartitionUpdate(_) => "PartitionUpdate",
            Self::JsonDbRequest { .. } => "JsonDbRequest",
            Self::JsonDbResponse(_) => "JsonDbResponse",
            Self::UniqueReserveRequest(_) => "UniqueReserveRequest",
            Self::UniqueReserveResponse(_) => "UniqueReserveResponse",
            Self::UniqueCommitRequest(_) => "UniqueCommitRequest",
            Self::UniqueCommitResponse(_) => "UniqueCommitResponse",
            Self::UniqueReleaseRequest(_) => "UniqueReleaseRequest",
            Self::UniqueReleaseResponse(_) => "UniqueReleaseResponse",
        }
    }
}

impl ClusterMessage {
    #[must_use]
    pub fn decode_from_wire(msg_type: u8, data: &[u8]) -> Option<Self> {
        match msg_type {
            0 => {
                let (hb, _) = Heartbeat::try_from_be_bytes(data).ok()?;
                Some(Self::Heartbeat(hb))
            }
            2 => Self::decode_node_id_message(data, |n| Self::DeathNotice { node_id: n }),
            3 => Self::decode_node_id_message(data, |n| Self::DrainNotification { node_id: n }),
            10 => Some(Self::Write(ReplicationWrite::from_bytes(data)?)),
            11 => {
                let (ack, _) = ReplicationAck::try_from_be_bytes(data).ok()?;
                Some(Self::Ack(ack))
            }
            12 => {
                let (req, _) = CatchupRequest::try_from_be_bytes(data).ok()?;
                Some(Self::CatchupRequest(req))
            }
            13 => Some(Self::CatchupResponse(CatchupResponse::from_bytes(data)?)),
            15 => Some(Self::WriteRequest(ReplicationWrite::from_bytes(data)?)),
            20 => {
                let (req, _) = RequestVoteRequest::try_from_be_bytes(data).ok()?;
                Some(Self::RequestVote(req))
            }
            21 => {
                let (resp, _) = RequestVoteResponse::try_from_be_bytes(data).ok()?;
                Some(Self::RequestVoteResponse(resp))
            }
            22 => Some(Self::AppendEntries(AppendEntriesRequest::from_bytes(data)?)),
            23 => {
                let (resp, _) = AppendEntriesResponse::try_from_be_bytes(data).ok()?;
                Some(Self::AppendEntriesResponse(resp))
            }
            30 => Some(Self::ForwardedPublish(ForwardedPublish::from_bytes(data)?)),
            40 => {
                let (req, _) = SnapshotRequest::try_from_be_bytes(data).ok()?;
                Some(Self::SnapshotRequest(req))
            }
            41 => Some(Self::SnapshotChunk(SnapshotChunk::from_bytes(data)?)),
            42 => {
                let (complete, _) = SnapshotComplete::try_from_be_bytes(data).ok()?;
                Some(Self::SnapshotComplete(complete))
            }
            50 => Self::decode_partitioned_request(data, |p, d| {
                QueryRequest::from_bytes(d).map(|r| Self::QueryRequest {
                    partition: p,
                    request: r,
                })
            }),
            51 => Some(Self::QueryResponse(QueryResponse::from_bytes(data)?)),
            52 => Some(Self::BatchReadRequest(BatchReadRequest::from_bytes(data)?)),
            53 => Some(Self::BatchReadResponse(BatchReadResponse::from_bytes(
                data,
            )?)),
            54 => Self::decode_partitioned_request(data, |p, d| {
                JsonDbRequest::from_bytes(d).map(|r| Self::JsonDbRequest {
                    partition: p,
                    request: r,
                })
            }),
            55 => Some(Self::JsonDbResponse(JsonDbResponse::from_bytes(data)?)),
            60 => {
                let (broadcast, _) = WildcardBroadcast::try_from_be_bytes(data).ok()?;
                Some(Self::WildcardBroadcast(broadcast))
            }
            61 => {
                let (broadcast, _) = TopicSubscriptionBroadcast::try_from_be_bytes(data).ok()?;
                Some(Self::TopicSubscriptionBroadcast(broadcast))
            }
            70 => {
                let (update, _) = PartitionUpdate::try_from_be_bytes(data).ok()?;
                Some(Self::PartitionUpdate(update))
            }
            80 => {
                let (req, _) = UniqueReserveRequest::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueReserveRequest(req))
            }
            81 => {
                let (resp, _) = UniqueReserveResponse::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueReserveResponse(resp))
            }
            82 => {
                let (req, _) = UniqueCommitRequest::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueCommitRequest(req))
            }
            83 => {
                let (resp, _) = UniqueCommitResponse::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueCommitResponse(resp))
            }
            84 => {
                let (req, _) = UniqueReleaseRequest::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueReleaseRequest(req))
            }
            85 => {
                let (resp, _) = UniqueReleaseResponse::try_from_be_bytes(data).ok()?;
                Some(Self::UniqueReleaseResponse(resp))
            }
            _ => None,
        }
    }

    fn decode_node_id_message(data: &[u8], f: fn(NodeId) -> Self) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        let id = u16::from_be_bytes([data[0], data[1]]);
        NodeId::validated(id).map(f)
    }

    fn decode_partitioned_request(
        data: &[u8],
        f: impl FnOnce(PartitionId, &[u8]) -> Option<Self>,
    ) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        let partition_id = u16::from_be_bytes([data[0], data[1]]);
        let partition = PartitionId::new(partition_id)?;
        f(partition, &data[2..])
    }
}

#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub from: NodeId,
    pub message: ClusterMessage,
    pub received_at: u64,
}

impl InboundMessage {
    #[must_use]
    pub fn parse_from_payload(payload: &[u8], local_node: NodeId) -> Option<Self> {
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
        let message = ClusterMessage::decode_from_wire(msg_type, data)?;

        #[allow(clippy::cast_possible_truncation)]
        let received_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        Some(Self {
            from,
            message,
            received_at,
        })
    }
}

pub trait ClusterTransport: Send + Sync + Debug + Clone {
    fn local_node(&self) -> NodeId;

    fn send(
        &self,
        to: NodeId,
        message: ClusterMessage,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    fn broadcast(
        &self,
        message: ClusterMessage,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        message: ClusterMessage,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    fn recv(&self) -> Option<InboundMessage>;

    fn pending_count(&self) -> usize;

    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage>;

    fn requeue(&self, msg: InboundMessage);

    fn queue_local_publish(
        &self,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn queue_local_publish_retained(
        &self,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn queue_local_publish_with_properties(
        &self,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        _user_properties: Vec<(String, String)>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.queue_local_publish(topic, payload, qos)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    NodeNotFound(NodeId),
    PartitionNotFound(PartitionId),
    NetworkPartitioned,
    SendFailed(String),
    NotConnected,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound(id) => write!(f, "node {} not found", id.get()),
            Self::PartitionNotFound(id) => write!(f, "partition {} not found", id.get()),
            Self::NetworkPartitioned => write!(f, "network partitioned"),
            Self::SendFailed(msg) => write!(f, "send failed: {msg}"),
            Self::NotConnected => write!(f, "not connected"),
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub ack_timeout_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_ms: 15000,
            ack_timeout_ms: 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_error_display() {
        let err = TransportError::NodeNotFound(NodeId::validated(5).unwrap());
        assert_eq!(err.to_string(), "node 5 not found");

        let err = TransportError::NetworkPartitioned;
        assert_eq!(err.to_string(), "network partitioned");
    }

    #[test]
    fn message_type_values() {
        let hb = ClusterMessage::Heartbeat(Heartbeat::create(NodeId::validated(1).unwrap(), 1000));
        assert_eq!(hb.message_type(), 0);

        let death = ClusterMessage::DeathNotice {
            node_id: NodeId::validated(1).unwrap(),
        };
        assert_eq!(death.message_type(), 2);
    }
}
