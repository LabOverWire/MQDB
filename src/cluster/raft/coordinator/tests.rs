// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::raft::node::RaftConfig;
use crate::cluster::raft::state::RaftCommand;
use crate::cluster::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportError};
use crate::cluster::{Epoch, NodeId, PartitionId};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct MockTransport {
    node_id: NodeId,
    outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
}

impl MockTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            outbox: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sent_messages(&self) -> Vec<(NodeId, ClusterMessage)> {
        self.outbox.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.outbox.lock().unwrap().clear();
    }
}

impl ClusterTransport for MockTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        self.outbox.lock().unwrap().push((to, message));
        Ok(())
    }

    async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        self.outbox.lock().unwrap().push((self.node_id, message));
        Ok(())
    }

    async fn send_to_partition_primary(
        &self,
        _partition: PartitionId,
        _message: ClusterMessage,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    fn recv(&self) -> Option<InboundMessage> {
        None
    }

    fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
        None
    }

    fn pending_count(&self) -> usize {
        0
    }

    fn requeue(&self, _msg: InboundMessage) {}

    async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

    async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
}

fn test_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        startup_grace_period_ms: 0,
    }
}

#[test]
fn coordinator_creates_as_follower() {
    let node_id = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node_id);
    let coord = RaftCoordinator::new(node_id, transport, test_config());

    assert!(!coord.is_leader());
    assert!(coord.leader_id().is_none());
}

#[tokio::test]
async fn coordinator_election_and_propose() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport1 = MockTransport::new(node1);
    let transport2 = MockTransport::new(node2);

    let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
    let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

    coord1.add_peer(node2);
    coord2.add_peer(node1);

    coord1.tick(1000).await;
    let sent = coord1.transport.sent_messages();
    assert_eq!(sent.len(), 1);

    let request = match &sent[0].1 {
        ClusterMessage::RequestVote(req) => *req,
        _ => panic!("expected RequestVote"),
    };

    let response = coord2.handle_request_vote(node1, request, 1000).await;
    assert!(response.is_granted());

    coord1.handle_request_vote_response(node2, response).await;
    assert!(coord1.is_leader());
}

#[tokio::test]
async fn coordinator_applies_partition_update() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport1 = MockTransport::new(node1);
    let transport2 = MockTransport::new(node2);

    let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
    let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

    coord1.add_peer(node2);
    coord2.add_peer(node1);

    coord1.tick(1000).await;
    let request = match &coord1.transport.sent_messages()[0].1 {
        ClusterMessage::RequestVote(req) => *req,
        _ => panic!("expected RequestVote"),
    };
    let response = coord2.handle_request_vote(node1, request, 1000).await;
    coord1.handle_request_vote_response(node2, response).await;
    assert!(coord1.is_leader());

    let partition = PartitionId::new(5).unwrap();
    let cmd = RaftCommand::update_partition(partition, node1, &[node2], Epoch::new(1));

    let idx = coord1.propose_partition_update(cmd).await.unwrap();
    assert_eq!(idx, 2);

    coord1.transport.clear();
    coord1.tick(1100).await;

    let append_req = coord1
        .transport
        .sent_messages()
        .iter()
        .find_map(|(_, msg)| match msg {
            ClusterMessage::AppendEntries(req) => Some(req.clone()),
            _ => None,
        })
        .unwrap();

    let response = coord2
        .handle_append_entries(node1, append_req.clone(), 1100)
        .await;
    assert!(response.is_success());

    coord1.handle_append_entries_response(node2, response).await;

    coord1.transport.clear();
    coord1.tick(1200).await;

    let commit_req = coord1
        .transport
        .sent_messages()
        .iter()
        .find_map(|(_, msg)| match msg {
            ClusterMessage::AppendEntries(req) => Some(req.clone()),
            _ => None,
        })
        .unwrap();

    coord2.handle_append_entries(node1, commit_req, 1200).await;

    assert_eq!(coord2.partition_map().primary(partition), Some(node1));
    assert_eq!(coord2.partition_map().replicas(partition), &[node2]);
}

#[tokio::test]
async fn coordinator_rejects_propose_when_not_leader() {
    let node1 = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node1);
    let mut coord = RaftCoordinator::new(node1, transport, test_config());

    let partition = PartitionId::ZERO;
    let cmd = RaftCommand::update_partition(partition, node1, &[], Epoch::new(1));

    let result = coord.propose_partition_update(cmd).await;
    assert!(matches!(result, Err(CoordinatorError::NotLeader(None))));
}

#[tokio::test]
async fn handle_node_death_reassigns_partitions() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let transport1 = MockTransport::new(node1);
    let transport2 = MockTransport::new(node2);

    let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
    let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

    coord1.add_peer(node2);
    coord1.add_peer(node3);
    coord2.add_peer(node1);
    coord2.add_peer(node3);

    coord1.tick(1000).await;
    let request = match &coord1.transport.sent_messages()[0].1 {
        ClusterMessage::RequestVote(req) => *req,
        _ => panic!("expected RequestVote"),
    };
    let response = coord2.handle_request_vote(node1, request, 1000).await;
    coord1.handle_request_vote_response(node2, response).await;
    assert!(coord1.is_leader());

    let partition = PartitionId::ZERO;
    let cmd = RaftCommand::update_partition(partition, node2, &[node3], Epoch::new(1));
    coord1.propose_partition_update(cmd).await.unwrap();

    coord1.transport.clear();
    coord1.tick(1100).await;

    let append_req = coord1
        .transport
        .sent_messages()
        .iter()
        .find_map(|(_, msg)| match msg {
            ClusterMessage::AppendEntries(req) => Some(req.clone()),
            _ => None,
        })
        .unwrap();

    let response = coord2.handle_append_entries(node1, append_req, 1100).await;
    coord1.handle_append_entries_response(node2, response).await;

    coord1.transport.clear();
    coord1.tick(1200).await;

    assert_eq!(coord1.partition_map().primary(partition), Some(node2));
    assert_eq!(coord1.partition_map().replicas(partition), &[node3]);

    let indices = coord1.handle_node_death(node2).await;
    assert!(!indices.is_empty());
}

#[tokio::test]
async fn handle_node_death_does_nothing_when_not_leader() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let transport = MockTransport::new(node1);
    let mut coord = RaftCoordinator::new(node1, transport, test_config());
    coord.add_peer(node2);

    let indices = coord.handle_node_death(node2).await;
    assert!(indices.is_empty());
}
