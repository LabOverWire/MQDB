use super::protocol::{Heartbeat, ReplicationAck, ReplicationWrite};
use super::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::{NodeId, PartitionId};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum ClusterMessage {
    Heartbeat(Heartbeat),
    Write(ReplicationWrite),
    Ack(ReplicationAck),
    DeathNotice { node_id: NodeId },
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl ClusterMessage {
    #[must_use]
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Heartbeat(_) => 0,
            Self::Write(_) => 10,
            Self::Ack(_) => 11,
            Self::DeathNotice { .. } => 2,
            Self::RequestVote(_) => 20,
            Self::RequestVoteResponse(_) => 21,
            Self::AppendEntries(_) => 22,
            Self::AppendEntriesResponse(_) => 23,
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

    /// # Errors
    /// Returns a transport error if sending fails.
    fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError>;

    /// # Errors
    /// Returns a transport error if broadcasting fails.
    fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError>;

    /// # Errors
    /// Returns a transport error if sending fails or partition is not found.
    fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        message: ClusterMessage,
    ) -> Result<(), TransportError>;

    fn recv(&self) -> Option<InboundMessage>;

    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage>;
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
            heartbeat_timeout_ms: 5000,
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
