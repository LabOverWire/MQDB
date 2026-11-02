// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::raft::node::RaftConfig;
use crate::cluster::raft::rpc::RequestVoteRequest;
use crate::cluster::raft::state::RaftCommand;
use crate::cluster::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportError};
use crate::cluster::{Epoch, NodeId, PartitionId};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct MockTransport {
    node_id: NodeId,
    outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    links: Arc<Mutex<Option<Vec<NodeId>>>>,
}

impl MockTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            outbox: Arc::new(Mutex::new(Vec::new())),
            links: Arc::new(Mutex::new(None)),
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
        self.links.lock().unwrap().clone()
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
    async fn queue_retained_refresh(&self, _topic: String) {}
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

fn grace_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        startup_grace_period_ms: 10_000,
    }
}

fn disk_with_log(term: u64, updates: &[RaftCommand]) -> Arc<dyn mqdb_core::StorageBackend> {
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    let storage = crate::cluster::raft::RaftStorage::new(Arc::clone(&backend));
    storage.persist_state(term, None).unwrap();
    for (position, command) in updates.iter().enumerate() {
        let entry = crate::cluster::raft::LogEntry::create(position as u64 + 1, term, *command);
        storage.append_log_entry(&entry).unwrap();
    }
    backend
}

fn assign(partition: u16, primary: u16, replica: u16, epoch: u64) -> RaftCommand {
    RaftCommand::update_partition(
        PartitionId::new(partition).unwrap(),
        NodeId::validated(primary).unwrap(),
        &[NodeId::validated(replica).unwrap()],
        Epoch::new(epoch),
    )
}

async fn tick_past_the_grace_period(coord: &mut RaftCoordinator<MockTransport>) {
    let start = 1_790_000_000_000;
    for elapsed in (0..=40_000).step_by(200) {
        coord.tick(start + elapsed).await;
    }
}

#[tokio::test]
async fn a_restarted_node_whose_log_names_other_nodes_does_not_elect_itself() {
    let node1 = NodeId::validated(1).unwrap();
    let backend = disk_with_log(3, &[assign(0, 1, 2, 1), assign(1, 3, 1, 1)]);
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();

    tick_past_the_grace_period(&mut coord).await;

    assert!(
        !coord.is_leader(),
        "a node whose persisted map names other nodes must not elect itself alone"
    );
}

#[tokio::test]
async fn a_restarted_node_counts_every_node_its_log_names_as_a_voter() {
    let node1 = NodeId::validated(1).unwrap();
    let backend = disk_with_log(
        3,
        &[assign(0, 2, 1, 1), assign(1, 3, 1, 1), assign(0, 3, 1, 2)],
    );
    let coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();

    let mut voters = coord.node.peers().to_vec();
    voters.sort_by_key(|node| node.get());
    assert_eq!(
        voters,
        [NodeId::validated(2).unwrap(), NodeId::validated(3).unwrap()],
        "the entry moving node 2 off may be uncommitted, so node 2 stays a voter"
    );
}

#[tokio::test]
async fn a_restarted_single_node_cluster_still_elects_itself_after_the_grace_period() {
    let node1 = NodeId::validated(1).unwrap();
    let only_self =
        RaftCommand::update_partition(PartitionId::new(0).unwrap(), node1, &[], Epoch::new(1));
    let backend = disk_with_log(3, &[only_self]);
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();
    let start = 1_790_000_000_000;

    coord.tick(start).await;
    coord.tick(start + 5_000).await;
    assert!(!coord.is_leader(), "the grace period still applies");

    tick_past_the_grace_period(&mut coord).await;
    assert!(coord.is_leader());
    assert_eq!(
        coord.partition_map().primary(PartitionId::new(0).unwrap()),
        Some(node1),
        "the restored log is applied once the node leads itself"
    );
}

#[tokio::test]
async fn a_restarted_node_wins_an_election_only_with_a_majority_of_its_restored_voters() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let backend = disk_with_log(3, &[assign(0, 1, 2, 1), assign(1, 3, 1, 1)]);
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();

    tick_past_the_grace_period(&mut coord).await;
    let term = coord.current_term();
    assert!(!coord.is_leader());

    coord
        .handle_request_vote_response(
            node2,
            crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
        )
        .await;
    assert!(coord.is_leader());
}

#[tokio::test]
async fn uncommitted_failover_tail_must_not_shrink_the_restored_voters() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    {
        let mut leader = RaftCoordinator::new_with_storage(
            node1,
            MockTransport::new(node1),
            grace_config(),
            Arc::clone(&backend),
        )
        .unwrap();
        leader.add_peer(node2);
        leader.add_peer(node3);
        let start = 1_790_000_000_000;
        leader.tick(start).await;
        leader.tick(start + 1_000).await;
        let term = leader.current_term();
        leader
            .handle_request_vote_response(
                node2,
                crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
            )
            .await;
        assert!(leader.is_leader());
        let last = leader.last_log_index();
        for partition in 0..3u16 {
            let cmd = assign(partition, partition % 3 + 1, (partition + 1) % 3 + 1, 1);
            leader.propose_partition_update(cmd).await.unwrap();
        }
        let tail = leader.last_log_index();
        for peer in [node2, node3] {
            leader
                .handle_append_entries_response(
                    peer,
                    crate::cluster::raft::rpc::AppendEntriesResponse::success(term, tail),
                )
                .await;
        }
        assert!(
            leader.commit_index() >= tail,
            "map naming 1,2,3 committed, base {last}"
        );
        leader.tick(start + 2_000).await;

        leader.handle_node_death(node2).await;
        leader.handle_node_death(node3).await;
        assert_eq!(
            leader.commit_index(),
            tail,
            "no quorum, so the failover tail is uncommitted"
        );
        assert!(
            leader.last_log_index() > tail,
            "the leader proposed failover entries"
        );
    }

    let restarted = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();

    assert!(
        !restarted.node.peers().is_empty(),
        "committed map names 2 and 3; uncommitted failover entries must not make this a single-node cluster"
    );
}

#[tokio::test]
async fn uncommitted_tail_after_second_failure_must_not_make_a_single_node_cluster() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    {
        let mut leader = RaftCoordinator::new_with_storage(
            node1,
            MockTransport::new(node1),
            grace_config(),
            Arc::clone(&backend),
        )
        .unwrap();
        leader.add_peer(node2);
        let start = 1_790_000_000_000;
        leader.tick(start).await;
        leader.tick(start + 1_000).await;
        let term = leader.current_term();
        leader
            .handle_request_vote_response(
                node2,
                crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
            )
            .await;
        assert!(leader.is_leader());
        leader
            .propose_partition_update(assign(0, 1, 2, 1))
            .await
            .unwrap();
        leader
            .propose_partition_update(assign(1, 2, 1, 1))
            .await
            .unwrap();
        let tail = leader.last_log_index();
        leader
            .handle_append_entries_response(
                node2,
                crate::cluster::raft::rpc::AppendEntriesResponse::success(term, tail),
            )
            .await;
        assert_eq!(
            leader.commit_index(),
            tail,
            "the map naming 1 and 2 is committed"
        );
        leader.tick(start + 2_000).await;
        assert_eq!(
            leader.partition_map().primary(PartitionId::new(1).unwrap()),
            Some(node2)
        );

        leader.handle_node_death(node2).await;
        assert_eq!(
            leader.commit_index(),
            tail,
            "no quorum, so the failover is uncommitted"
        );
        assert!(leader.last_log_index() > tail);
    }

    let restarted = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();
    assert_eq!(
        restarted.node.peers(),
        [node2],
        "the last committed map names node 2; only an uncommitted proposal removed it"
    );
}

#[tokio::test]
async fn the_coordinator_feeds_the_links_to_the_election_gate() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let transport = MockTransport::new(node1);
    *transport.links.lock().unwrap() = Some(Vec::new());
    let links = Arc::clone(&transport.links);
    let mut coord = RaftCoordinator::new(node1, transport, grace_config());
    coord.add_peer(node2);
    let start = 1_790_000_000_000;

    for elapsed in (0..=5_000).step_by(200) {
        coord.tick(start + elapsed).await;
    }
    assert_eq!(coord.current_term(), 0, "no links, so no term is raised");

    *links.lock().unwrap() = Some(vec![node2]);
    for elapsed in (5_000..=6_000).step_by(200) {
        coord.tick(start + elapsed).await;
    }
    assert!(
        coord.current_term() >= 1,
        "links reach a quorum, so an election starts"
    );
}

async fn restarted_leader_with_a_map_naming_three_nodes() -> RaftCoordinator<MockTransport> {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let backend = disk_with_log(
        3,
        &[assign(0, 1, 2, 1), assign(1, 2, 1, 1), assign(2, 3, 1, 1)],
    );
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();
    let start = 1_790_000_000_000;
    coord.tick(start).await;
    coord.tick(start + 1_000).await;
    let term = coord.current_term();
    coord
        .handle_request_vote_response(
            node2,
            crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
        )
        .await;
    assert!(coord.is_leader());
    let last = coord.last_log_index();
    coord
        .handle_append_entries_response(
            node2,
            crate::cluster::raft::rpc::AppendEntriesResponse::success(term, last),
        )
        .await;
    coord.tick(start + 1_200).await;
    assert_eq!(coord.commit_index(), last, "the restored log is committed");
    coord
}

#[tokio::test]
async fn a_restarted_leader_does_not_treat_a_member_without_a_heartbeat_yet_as_dead() {
    let mut coord = restarted_leader_with_a_map_naming_three_nodes().await;
    let node3 = NodeId::validated(3).unwrap();

    assert_eq!(
        coord.partition_map().primary(PartitionId::new(2).unwrap()),
        Some(node3)
    );
    assert_eq!(
        coord.last_log_index(),
        coord.commit_index(),
        "no reassignment was proposed for node 3 or node 2"
    );
    let start = 1_790_000_000_000;
    coord.tick(start + 5_000).await;
    assert_eq!(coord.last_log_index(), coord.commit_index());
}

#[tokio::test]
async fn a_restored_voter_is_a_member_with_no_timer_of_its_own() {
    let mut coord = restarted_leader_with_a_map_naming_three_nodes().await;
    let before = coord.last_log_index();
    let start = 1_790_000_000_000;

    for elapsed in (1_400..=120_000).step_by(1_000) {
        coord.tick(start + elapsed).await;
    }

    assert_eq!(
        coord.last_log_index(),
        before,
        "only a death verdict from heartbeats fails a member over"
    );
}

#[tokio::test]
async fn a_restored_snapshot_member_without_partitions_is_a_voter() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
    let mut map = PartitionMap::new();
    map.set(
        PartitionId::new(0).unwrap(),
        crate::cluster::PartitionAssignment::new(node1, vec![node2], Epoch::new(1)),
    );
    let snapshot = crate::cluster::raft::RaftSnapshot::capture(9, 1, &map, &[node1, node2, node4]);
    let storage = crate::cluster::raft::RaftStorage::new(Arc::clone(&backend));
    storage.persist_state(1, None).unwrap();
    storage.install_snapshot(&snapshot, false).unwrap();

    let coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();

    assert!(coord.voters().contains(&node4));
}

async fn restarted_leader_of_five(log: &[RaftCommand]) -> RaftCoordinator<MockTransport> {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let backend = disk_with_log(3, log);
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();
    let start = 1_790_000_000_000;
    coord.tick(start).await;
    coord.tick(start + 1_000).await;
    let term = coord.current_term();
    for voter in [node2, node3] {
        coord
            .handle_request_vote_response(
                voter,
                crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
            )
            .await;
    }
    assert!(coord.is_leader());
    commit_tail(&mut coord).await;
    coord.tick(start + 1_200).await;
    commit_tail(&mut coord).await;
    coord.handle_node_alive(node2).await;
    coord.handle_node_alive(node3).await;
    coord
}

async fn commit_tail(coord: &mut RaftCoordinator<MockTransport>) {
    let term = coord.current_term();
    let tail = coord.last_log_index();
    for voter in [2u16, 3] {
        coord
            .handle_append_entries_response(
                NodeId::validated(voter).unwrap(),
                crate::cluster::raft::rpc::AppendEntriesResponse::success(term, tail),
            )
            .await;
    }
    assert_eq!(coord.commit_index(), tail);
}

fn pair_on_nodes_4_and_5() -> Vec<RaftCommand> {
    vec![
        assign(0, 4, 5, 1),
        assign(1, 5, 4, 1),
        assign(2, 1, 2, 1),
        assign(3, 2, 3, 1),
        assign(4, 3, 1, 1),
    ]
}

#[tokio::test]
async fn both_holders_lost_keeps_the_partition_on_a_holder_until_they_return() {
    let node4 = NodeId::validated(4).unwrap();
    let node5 = NodeId::validated(5).unwrap();
    let start = 1_790_000_000_000u64;
    for order in [[node4, node5], [node5, node4]] {
        let mut coord = restarted_leader_of_five(&pair_on_nodes_4_and_5()).await;

        coord.handle_node_death(order[0]).await;
        coord.handle_node_death(order[1]).await;
        commit_tail(&mut coord).await;

        for partition in [0u16, 1] {
            let id = PartitionId::new(partition).unwrap();
            assert_eq!(
                coord.partition_map().primary(id),
                Some(order[1]),
                "partition {partition} must stay on the last holder"
            );
            assert!(coord.partition_map().replicas(id).is_empty());
        }

        coord.handle_node_alive(node4).await;
        coord.handle_node_alive(node5).await;
        commit_tail(&mut coord).await;
        for elapsed in (20_000u64..=200_000).step_by(1_000) {
            coord.tick(start + elapsed).await;
            commit_tail(&mut coord).await;
        }
        for partition in [0u16, 1] {
            let primary = coord
                .partition_map()
                .primary(PartitionId::new(partition).unwrap());
            assert!(primary == Some(node4) || primary == Some(node5));
        }
    }
}

#[tokio::test]
async fn a_lost_node_with_a_live_replica_is_failed_over_to_it() {
    let node1 = NodeId::validated(1).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut coord = restarted_leader_of_five(&[assign(0, 3, 1, 1), assign(1, 2, 3, 1)]).await;

    coord.handle_node_death(node3).await;
    commit_tail(&mut coord).await;

    let id = PartitionId::new(0).unwrap();
    assert_eq!(coord.partition_map().primary(id), Some(node1));
    assert_eq!(coord.partition_map().epoch(id), Epoch::new(2));
}

#[tokio::test]
async fn a_second_promotion_of_a_partition_uses_a_newer_epoch_than_the_first() {
    let node4 = NodeId::validated(4).unwrap();
    let node5 = NodeId::validated(5).unwrap();
    let id = PartitionId::new(0).unwrap();
    let two_replicas = RaftCommand::update_partition(
        id,
        node4,
        &[node5, NodeId::validated(2).unwrap()],
        Epoch::new(1),
    );
    let mut coord = restarted_leader_of_five(&[two_replicas, assign(1, 3, 1, 1)]).await;

    coord.handle_node_death(node4).await;
    coord.handle_node_death(node5).await;
    commit_tail(&mut coord).await;

    assert_eq!(
        coord.partition_map().primary(id),
        Some(NodeId::validated(2).unwrap())
    );
    assert_eq!(coord.partition_map().epoch(id), Epoch::new(3));
}

#[tokio::test]
async fn a_forced_rebalance_adopts_a_partition_whose_holders_are_gone() {
    let node1 = NodeId::validated(1).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let node5 = NodeId::validated(5).unwrap();
    let mut coord = restarted_leader_of_five(&pair_on_nodes_4_and_5()).await;
    coord.handle_node_death(node4).await;
    coord.handle_node_death(node5).await;
    commit_tail(&mut coord).await;

    coord.force_rebalance().await;
    commit_tail(&mut coord).await;

    for partition in [0u16, 1] {
        let primary = coord
            .partition_map()
            .primary(PartitionId::new(partition).unwrap());
        assert!(primary.is_some_and(|node| node != node4 && node != node5));
    }
    assert_eq!(coord.node_id(), node1);
}

#[tokio::test]
async fn a_node_whose_partitions_are_being_moved_off_counts_as_having_none() {
    let node4 = NodeId::validated(4).unwrap();
    let mut coord = restarted_leader_of_five(&[assign(0, 4, 1, 1), assign(1, 1, 2, 1)]).await;

    coord.handle_node_death(node4).await;
    coord.handle_node_alive(node4).await;

    assert!(
        coord.pending_new_nodes.contains(&node4),
        "the unapplied failover leaves node 4 with nothing, so it needs a rebalance"
    );
}

#[tokio::test]
async fn a_forced_rebalance_reads_the_unapplied_failover() {
    let node4 = NodeId::validated(4).unwrap();
    let mut coord =
        restarted_leader_of_five(&[assign(0, 1, 4, 1), assign(1, 2, 3, 1), assign(2, 3, 2, 1)])
            .await;

    coord.handle_node_death(node4).await;
    coord.force_rebalance().await;
    commit_tail(&mut coord).await;

    let replicas = coord.partition_map().replicas(PartitionId::new(0).unwrap());
    assert!(!replicas.contains(&node4));
    assert!(
        !replicas.is_empty(),
        "the failover left the partition with no replica, so the rebalance adds one"
    );
}

#[tokio::test]
async fn a_drain_reads_the_unapplied_failover() {
    let node2 = NodeId::validated(2).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let mut coord = restarted_leader_of_five(&[assign(0, 2, 4, 1), assign(1, 3, 1, 1)]).await;

    coord.handle_node_death(node4).await;
    coord.handle_drain_notification(node2).await;
    commit_tail(&mut coord).await;

    let id = PartitionId::new(0).unwrap();
    assert!(!coord.partition_map().replicas(id).contains(&node4));
    assert_ne!(coord.partition_map().primary(id), Some(node4));
}

#[tokio::test]
async fn the_scan_at_leader_start_does_not_repeat_a_failover_it_just_proposed() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let backend = disk_with_log(3, &[assign(0, 4, 1, 1), assign(1, 2, 3, 1)]);
    let mut coord = RaftCoordinator::new_with_storage(
        node1,
        MockTransport::new(node1),
        grace_config(),
        backend,
    )
    .unwrap();
    coord.handle_node_death(node4).await;
    let start = 1_790_000_000_000u64;
    coord.tick(start).await;
    coord.tick(start + 1_000).await;
    let term = coord.current_term();
    for voter in [node2, node3] {
        coord
            .handle_request_vote_response(
                voter,
                crate::cluster::raft::rpc::RequestVoteResponse::granted(term),
            )
            .await;
    }
    commit_tail(&mut coord).await;
    coord.tick(start + 1_200).await;

    let updates_of_partition_0 = coord
        .node
        .unapplied_commands()
        .iter()
        .filter(|command| {
            matches!(command, RaftCommand::UpdatePartition(update) if update.partition == 0)
        })
        .count();
    assert_eq!(updates_of_partition_0, 1);
}

#[tokio::test]
async fn a_replica_entry_on_a_departed_node_is_repaired_by_the_periodic_scan() {
    let node4 = NodeId::validated(4).unwrap();
    let mut coord =
        restarted_leader_of_five(&[assign(0, 1, 4, 1), assign(1, 2, 3, 1), assign(2, 3, 2, 1)])
            .await;
    coord.cluster_members.retain(|member| *member != node4);
    coord.processed_dead_nodes.insert(node4);
    let start = 1_790_000_000_000u64;

    coord.tick(start + 40_000).await;
    commit_tail(&mut coord).await;

    assert!(
        !coord
            .partition_map()
            .replicas(PartitionId::new(0).unwrap())
            .contains(&node4)
    );
}

#[tokio::test]
async fn a_late_member_returning_after_failovers_does_not_panic() {
    let node1 = NodeId::validated(1).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let node5 = NodeId::validated(5).unwrap();
    let mut log: Vec<RaftCommand> = (0..100u16).map(|p| assign(p, 1, 2, 1)).collect();
    log.extend((100..113u16).map(|p| assign(p, 5, 4, 1)));
    log.extend((113..177u16).map(|p| assign(p, 2, 3, 1)));
    log.extend((177..241u16).map(|p| assign(p, 3, 2, 1)));
    let mut coord = restarted_leader_of_five(&log).await;

    coord.handle_node_death(node4).await;
    coord.handle_node_death(node5).await;
    commit_tail(&mut coord).await;
    coord.handle_node_alive(node5).await;
    commit_tail(&mut coord).await;

    assert_eq!(coord.node_id(), node1);
}

#[tokio::test]
async fn a_failover_lost_with_leadership_is_repaired_by_the_next_leader_term() {
    let node2 = NodeId::validated(2).unwrap();
    let node4 = NodeId::validated(4).unwrap();
    let mut coord = restarted_leader_of_five(&[assign(0, 4, 1, 1), assign(1, 2, 3, 1)]).await;
    let committed = coord.commit_index();
    let start = 1_790_000_000_000u64;

    coord.handle_node_death(node4).await;
    assert!(coord.last_log_index() > committed);
    let term = coord.current_term();
    let entries = vec![crate::cluster::raft::LogEntry::create(
        committed + 1,
        term + 1,
        RaftCommand::Noop,
    )];
    let replaced = crate::cluster::raft::AppendEntriesRequest::create(
        term + 1,
        node2.get(),
        committed,
        coord.node.log_term_at_for_tests(committed),
        entries,
        committed + 1,
    );
    coord
        .handle_append_entries(node2, replaced, start + 2_000)
        .await;
    assert!(!coord.is_leader());
    assert_eq!(
        coord.partition_map().primary(PartitionId::new(0).unwrap()),
        Some(node4),
        "the failover proposal was overwritten"
    );
    coord.tick(start + 2_100).await;

    for elapsed in (3_000u64..=9_000).step_by(100) {
        coord.tick(start + elapsed).await;
        if coord.node.role() == crate::cluster::raft::RaftRole::Candidate {
            break;
        }
    }
    let new_term = coord.current_term();
    for voter in [node2, NodeId::validated(3).unwrap()] {
        coord
            .handle_request_vote_response(
                voter,
                crate::cluster::raft::rpc::RequestVoteResponse::granted(new_term),
            )
            .await;
    }
    assert!(coord.is_leader());
    commit_tail(&mut coord).await;
    coord.tick(start + 10_000).await;
    commit_tail(&mut coord).await;

    assert_ne!(
        coord.partition_map().primary(PartitionId::new(0).unwrap()),
        Some(node4),
        "the scan must not skip a node marked processed in an earlier term"
    );
}

#[tokio::test]
async fn the_periodic_check_repairs_a_partition_on_a_departed_primary() {
    let node4 = NodeId::validated(4).unwrap();
    let mut coord = restarted_leader_of_five(&[assign(0, 4, 1, 1), assign(1, 2, 3, 1)]).await;
    coord.cluster_members.retain(|member| *member != node4);
    coord.processed_dead_nodes.insert(node4);
    let start = 1_790_000_000_000u64;

    coord.tick(start + 40_000).await;
    commit_tail(&mut coord).await;

    assert_ne!(
        coord.partition_map().primary(PartitionId::new(0).unwrap()),
        Some(node4)
    );
}

#[tokio::test]
async fn the_periodic_check_adds_no_replica_soon_after_a_death() {
    let node4 = NodeId::validated(4).unwrap();
    let node5 = NodeId::validated(5).unwrap();
    let id = PartitionId::new(0).unwrap();
    let mut coord =
        restarted_leader_of_five(&[assign(0, 4, 5, 1), assign(1, 2, 3, 1), assign(2, 3, 2, 1)])
            .await;
    let start = 1_790_000_000_000u64;

    coord.tick(start + 40_000).await;
    coord.handle_node_death(node5).await;
    commit_tail(&mut coord).await;
    coord.tick(start + 40_200).await;
    commit_tail(&mut coord).await;
    assert!(
        coord.partition_map().replicas(id).is_empty(),
        "another death may be seconds away, and a new replica has no copy yet"
    );
    assert_eq!(coord.partition_map().primary(id), Some(node4));

    coord.tick(start + 71_000).await;
    coord.force_rebalance().await;
    commit_tail(&mut coord).await;
    assert!(!coord.partition_map().replicas(id).is_empty());
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

#[tokio::test]
async fn snapshot_install_clears_pending_partition_proposals() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut coord = RaftCoordinator::new(node2, MockTransport::new(node2), test_config());
    coord.add_peer(node1);
    coord.pending_partition_proposals = 7;

    let request = crate::cluster::raft::InstallSnapshotRequest {
        term: 1,
        leader_id: 1,
        snapshot: crate::cluster::raft::RaftSnapshot::capture(
            5,
            1,
            &PartitionMap::new(),
            &[node1, node2],
        ),
    };
    let response = coord.handle_install_snapshot(node1, request, 100).await;
    assert!(response.is_success());
    assert_eq!(coord.pending_partition_proposals, 0);
}

fn vote_request_from(coord: &RaftCoordinator<MockTransport>) -> RequestVoteRequest {
    coord
        .transport
        .sent_messages()
        .iter()
        .find_map(|(_, msg)| match msg {
            ClusterMessage::RequestVote(req) => Some(*req),
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn a_fresh_leader_initializes_partitions_once_after_catching_up() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let mut leader = RaftCoordinator::new(node1, MockTransport::new(node1), test_config());
    let mut follower = RaftCoordinator::new(node2, MockTransport::new(node2), test_config());
    leader.add_peer(node2);
    follower.add_peer(node1);

    leader.tick(0).await;
    leader.tick(1000).await;
    let response = follower
        .handle_request_vote(node1, vote_request_from(&leader), 1000)
        .await;
    leader.handle_request_vote_response(node2, response).await;
    assert!(leader.is_leader());

    leader.tick(1001).await;
    assert_eq!(
        leader.last_log_index(),
        1,
        "a leader must not initialize before it has applied its own term"
    );

    for round in 0..6 {
        replicate_round(&mut leader, &mut follower, 1100 + round * 100).await;
    }

    assert_eq!(
        leader.last_log_index(),
        1 + u64::from(crate::cluster::NUM_PARTITIONS)
    );
    assert!(PartitionId::all().all(|p| leader.partition_map().primary(p).is_some()));
    assert!(same_assignments(
        follower.partition_map(),
        leader.partition_map()
    ));
}

#[tokio::test]
async fn proposals_from_every_path_are_counted_until_applied() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut leader = RaftCoordinator::new(node1, MockTransport::new(node1), test_config());
    let mut follower = RaftCoordinator::new(node2, MockTransport::new(node2), test_config());
    leader.add_peer(node2);
    leader.add_peer(node3);
    follower.add_peer(node1);
    follower.add_peer(node3);

    leader.tick(0).await;
    leader.tick(1000).await;
    let response = follower
        .handle_request_vote(node1, vote_request_from(&leader), 1000)
        .await;
    leader.handle_request_vote_response(node2, response).await;
    for round in 0..6 {
        replicate_round(&mut leader, &mut follower, 1100 + round * 100).await;
    }
    assert!(leader.partitions_initialized());
    assert_eq!(leader.pending_partition_proposals, 0);

    let dead = leader.handle_node_death(node3).await;
    let drained = leader.handle_drain_notification(node2).await;
    assert!(!dead.is_empty());
    assert!(!drained.is_empty());
    assert_eq!(
        leader.pending_partition_proposals,
        dead.len() + drained.len()
    );

    for round in 0..6 {
        replicate_round(&mut leader, &mut follower, 2000 + round * 100).await;
    }
    assert_eq!(leader.pending_partition_proposals, 0);
}

#[tokio::test]
async fn a_new_leader_that_has_not_applied_the_log_does_not_initialize_again() {
    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();
    let mut old_leader = RaftCoordinator::new(node1, MockTransport::new(node1), test_config());
    let mut new_leader = RaftCoordinator::new(node2, MockTransport::new(node2), test_config());
    let mut voter = RaftCoordinator::new(node3, MockTransport::new(node3), test_config());
    old_leader.add_peer(node2);
    old_leader.add_peer(node3);
    new_leader.add_peer(node1);
    new_leader.add_peer(node3);
    voter.add_peer(node1);
    voter.add_peer(node2);

    old_leader.tick(0).await;
    old_leader.tick(1000).await;
    let response = new_leader
        .handle_request_vote(node1, vote_request_from(&old_leader), 1000)
        .await;
    old_leader
        .handle_request_vote_response(node2, response)
        .await;
    assert!(old_leader.is_leader());

    let partition = PartitionId::new(5).unwrap();
    let cmd = RaftCommand::update_partition(partition, node1, &[node2], Epoch::new(1));
    old_leader.propose_partition_update(cmd).await.unwrap();
    old_leader.transport.clear();
    old_leader.tick(1100).await;
    let append = old_leader
        .transport
        .sent_messages()
        .into_iter()
        .find_map(|(to, msg)| match msg {
            ClusterMessage::AppendEntries(req) if to == node2 => Some(req),
            _ => None,
        })
        .unwrap();
    let response = new_leader.handle_append_entries(node1, append, 1100).await;
    old_leader
        .handle_append_entries_response(node2, response)
        .await;
    assert_eq!(old_leader.commit_index(), 2);
    assert!(new_leader.partition_map().primary(partition).is_none());

    new_leader.transport.clear();
    new_leader.tick(5000).await;
    let response = voter
        .handle_request_vote(node2, vote_request_from(&new_leader), 5000)
        .await;
    new_leader
        .handle_request_vote_response(node3, response)
        .await;
    assert!(new_leader.is_leader());

    new_leader.tick(5001).await;
    assert_eq!(new_leader.last_log_index(), 3);

    for round in 0..6 {
        replicate_round(&mut new_leader, &mut voter, 5100 + round * 100).await;
    }

    assert_eq!(new_leader.partition_map().primary(partition), Some(node1));
    assert_eq!(
        new_leader.last_log_index(),
        3,
        "a leader that inherits partition assignments must not initialize them again"
    );
}
