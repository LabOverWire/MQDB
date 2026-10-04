// Copyright 2025-2026 LabOverWire. All rights reserved.
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

    async fn direct_peers(&self) -> Option<Vec<NodeId>> {
        None
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

    coord1.tick(0).await;
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

    coord1.tick(0).await;
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

    coord1.tick(0).await;
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

#[tokio::test]
async fn coordinator_from_storage_respects_startup_grace_without_peers() {
    let node_id = NodeId::validated(1).unwrap();
    let transport = MockTransport::new(node_id);
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    let config = RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        startup_grace_period_ms: 10_000,
    };
    let mut coord = RaftCoordinator::new_with_storage(node_id, transport, config, backend).unwrap();
    let now = 1_790_000_000_000;

    coord.tick(now).await;
    coord.tick(now + 5_000).await;
    assert!(
        !coord.is_leader(),
        "a node without peers must wait out the startup grace period before electing itself"
    );

    coord.tick(now + 10_000).await;
    coord.tick(now + 10_400).await;
    assert!(coord.is_leader());
}

fn same_assignments(a: &PartitionMap, b: &PartitionMap) -> bool {
    PartitionId::all().all(|p| a.get(p) == b.get(p))
}

async fn replicate_round(
    leader: &mut RaftCoordinator<MockTransport>,
    follower: &mut RaftCoordinator<MockTransport>,
    now: u64,
) -> usize {
    let leader_id = leader.node_id();
    let follower_id = follower.node_id();
    leader.tick(now).await;
    let sent = leader.transport.sent_messages();
    leader.transport.clear();
    let mut snapshots = 0;
    for (to, message) in sent {
        if to != follower_id {
            continue;
        }
        let response = match message {
            ClusterMessage::AppendEntries(request) => {
                follower
                    .handle_append_entries(leader_id, request, now)
                    .await
            }
            ClusterMessage::InstallSnapshot(request) => {
                snapshots += 1;
                follower
                    .handle_install_snapshot(leader_id, *request, now)
                    .await
            }
            _ => continue,
        };
        leader
            .handle_append_entries_response(follower_id, response)
            .await;
    }
    follower.transport.clear();
    snapshots
}

#[tokio::test]
async fn follower_joining_after_compaction_receives_partition_map() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut leader = RaftCoordinator::new(node1, MockTransport::new(node1), test_config());
    leader.tick(0).await;
    leader.tick(1000).await;
    assert!(leader.is_leader());

    for i in 0..1200u64 {
        let partition = PartitionId::new(u16::try_from(i % 256).unwrap()).unwrap();
        let command = RaftCommand::update_partition(partition, node1, &[], Epoch::new(i + 1));
        leader.propose_partition_update(command).await.unwrap();
    }
    leader.tick(1001).await;
    assert!(leader.log_len() < usize::try_from(leader.last_log_index()).unwrap());

    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    let mut follower = RaftCoordinator::new_with_storage(
        node2,
        MockTransport::new(node2),
        test_config(),
        backend.clone(),
    )
    .unwrap();
    follower.add_peer(node1);
    leader.add_peer(node2);

    let mut snapshots = 0;
    for round in 0..10 {
        snapshots += replicate_round(&mut leader, &mut follower, 2000 + round * 100).await;
    }

    assert_eq!(follower.commit_index(), leader.commit_index());
    assert!(same_assignments(
        follower.partition_map(),
        leader.partition_map()
    ));
    assert!(
        snapshots > 0,
        "the follower must be brought up to date with a snapshot"
    );

    let restarted =
        RaftCoordinator::new_with_storage(node2, MockTransport::new(node2), test_config(), backend)
            .unwrap();
    assert!(same_assignments(
        restarted.partition_map(),
        leader.partition_map()
    ));
    assert!(restarted.cluster_members().contains(&node1));
}

#[tokio::test]
async fn node_alive_registers_raft_peer_for_member_known_from_snapshot() {
    let node1 = NodeId::validated(1).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    let snapshot =
        crate::cluster::raft::RaftSnapshot::capture(40, 2, &PartitionMap::new(), &[node1, node3]);
    crate::cluster::raft::RaftStorage::new(backend.clone())
        .install_snapshot(&snapshot, false)
        .unwrap();

    let mut coord =
        RaftCoordinator::new_with_storage(node1, MockTransport::new(node1), test_config(), backend)
            .unwrap();
    assert!(coord.cluster_members().contains(&node3));

    coord.handle_node_alive(node3).await;
    assert!(coord.node.peers().contains(&node3));
}
