use super::protocol::{
    BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardedPublish,
    Heartbeat, QueryRequest, QueryResponse, ReplicationAck, ReplicationWrite, WildcardBroadcast,
};
use super::raft::{
    AppendEntriesRequest, AppendEntriesResponse, PartitionUpdate, RequestVoteRequest,
    RequestVoteResponse,
};
use super::snapshot::{SnapshotChunk, SnapshotComplete, SnapshotRequest};
use super::{NodeId, PartitionId};
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
    PartitionUpdate(PartitionUpdate),
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
            Self::PartitionUpdate(_) => 70,
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
            Self::PartitionUpdate(_) => "PartitionUpdate",
        }
    }
}

#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub from: NodeId,
    pub message: ClusterMessage,
    pub received_at: u64,
}

pub trait ClusterTransport: Send + Sync + Debug {
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

    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage>;

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
