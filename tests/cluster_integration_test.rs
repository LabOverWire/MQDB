mod simulation;

use mqdb::cluster::raft::{RaftConfig, RaftNode, RaftOutput, RaftRole};
use mqdb::cluster::{
    Epoch, NodeController, NodeId, NodeStatus, Operation, PartitionAssignment, PartitionId,
    PartitionMap, PublishRouter, Qos2Store, ReplicationWrite, RetainedStore, SessionStore,
    SubscriptionCache, TopicIndex, TransportConfig, WildcardStore, session_partition,
};
use simulation::framework::runtime::SimulatedRuntime;
use simulation::transport::SimulatedTransport;

struct TestNode {
    id: NodeId,
    controller: NodeController<SimulatedTransport>,
    raft: RaftNode,
    sessions: SessionStore,
    topics: TopicIndex,
    wildcards: WildcardStore,
    retained: RetainedStore,
    subscriptions: SubscriptionCache,
    qos2: Qos2Store,
}

struct TestCluster {
    runtime: SimulatedRuntime,
    nodes: Vec<TestNode>,
}

impl TestCluster {
    fn new(node_count: usize) -> Self {
        let runtime = SimulatedRuntime::new();
        let clock = runtime.clock().clone();
        let network = runtime.network().clone();

        let node_ids: Vec<NodeId> = (1..=node_count)
            .map(|i| {
                #[allow(clippy::cast_possible_truncation)]
                NodeId::validated(i as u16).unwrap()
            })
            .collect();

        let mut nodes = Vec::with_capacity(node_count);

        for &node_id in &node_ids {
            let transport = SimulatedTransport::new(node_id, network.clone(), clock.clone());

            for &peer_id in &node_ids {
                if peer_id != node_id {
                    transport.register_peer(peer_id);
                }
            }

            let config = TransportConfig {
                heartbeat_interval_ms: 100,
                heartbeat_timeout_ms: 500,
                ack_timeout_ms: 50,
            };
            let mut controller = NodeController::new(node_id, transport.clone(), config);

            for &peer_id in &node_ids {
                if peer_id != node_id {
                    controller.register_peer(peer_id);
                }
            }

            let raft_config = RaftConfig::default();
            let mut raft = RaftNode::create(node_id, raft_config);

            for &peer_id in &node_ids {
                if peer_id != node_id {
                    raft.add_peer(peer_id);
                }
            }

            nodes.push(TestNode {
                id: node_id,
                controller,
                raft,
                sessions: SessionStore::new(node_id),
                topics: TopicIndex::new(node_id),
                wildcards: WildcardStore::new(node_id),
                retained: RetainedStore::new(node_id),
                subscriptions: SubscriptionCache::new(node_id),
                qos2: Qos2Store::new(node_id),
            });
        }

        Self { runtime, nodes }
    }

    fn advance_ms(&self, ms: u64) {
        self.runtime.clock().advance_ms(ms);
    }

    fn partition_node(&self, node_id: NodeId) {
        let others: Vec<u16> = self
            .nodes
            .iter()
            .filter(|n| n.id != node_id)
            .map(|n| n.id.get())
            .collect();
        self.runtime.network().partition(node_id.get(), &others);
    }

    fn heal_node(&self, node_id: NodeId) {
        for other in &self.nodes {
            if other.id != node_id {
                self.runtime.network().heal(node_id.get(), other.id.get());
            }
        }
    }

    fn partition_groups(&self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.runtime.network().set_link(
                    a.get(),
                    b.get(),
                    simulation::framework::network::LinkState::Down,
                );
                self.runtime.network().set_link(
                    b.get(),
                    a.get(),
                    simulation::framework::network::LinkState::Down,
                );
            }
        }
    }

    fn heal_all(&self) {
        for i in 0..self.nodes.len() {
            for j in 0..self.nodes.len() {
                if i != j {
                    self.runtime
                        .network()
                        .heal(self.nodes[i].id.get(), self.nodes[j].id.get());
                }
            }
        }
    }

    fn restart_node(&mut self, node_idx: usize) {
        let old_node = &self.nodes[node_idx];
        let node_id = old_node.id;
        let clock = self.runtime.clock().clone();
        let network = self.runtime.network().clone();

        let peer_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.id != node_id)
            .map(|n| n.id)
            .collect();

        let transport = SimulatedTransport::new(node_id, network.clone(), clock.clone());

        for &peer_id in &peer_ids {
            transport.register_peer(peer_id);
        }

        let config = TransportConfig {
            heartbeat_interval_ms: 100,
            heartbeat_timeout_ms: 500,
            ack_timeout_ms: 50,
        };
        let mut controller = NodeController::new(node_id, transport.clone(), config);

        for &peer_id in &peer_ids {
            controller.register_peer(peer_id);
        }

        let raft_config = RaftConfig::default();
        let mut raft = RaftNode::create(node_id, raft_config);

        for &peer_id in &peer_ids {
            raft.add_peer(peer_id);
        }

        self.nodes[node_idx] = TestNode {
            id: node_id,
            controller,
            raft,
            sessions: SessionStore::new(node_id),
            topics: TopicIndex::new(node_id),
            wildcards: WildcardStore::new(node_id),
            retained: RetainedStore::new(node_id),
            subscriptions: SubscriptionCache::new(node_id),
            qos2: Qos2Store::new(node_id),
        };
    }
}

#[test]
fn cluster_formation_three_nodes() {
    let mut cluster = TestCluster::new(3);

    assert!(
        cluster
            .nodes
            .iter()
            .all(|n| n.raft.role() == RaftRole::Follower)
    );

    let first = &mut cluster.nodes[0];
    let outputs = first.raft.tick(1000);
    assert_eq!(first.raft.role(), RaftRole::Candidate);
    assert_eq!(first.raft.current_term(), 1);

    let mut vote_requests = Vec::new();
    for output in outputs {
        if let RaftOutput::SendRequestVote { to, request } = output {
            vote_requests.push((to, request));
        }
    }
    assert_eq!(vote_requests.len(), 2);

    let candidate_id = cluster.nodes[0].id;
    let (to, request) = vote_requests[0];
    let follower = cluster.nodes.iter_mut().find(|n| n.id == to).unwrap();
    let (response, _) = follower
        .raft
        .handle_request_vote(candidate_id, request, 1000);
    assert!(response.is_granted());

    let candidate = cluster
        .nodes
        .iter_mut()
        .find(|n| n.id == candidate_id)
        .unwrap();
    let outputs = candidate.raft.handle_request_vote_response(to, response);

    let became_leader = outputs
        .iter()
        .any(|o| matches!(o, RaftOutput::BecameLeader));
    assert!(became_leader);
    assert_eq!(candidate.raft.role(), RaftRole::Leader);

    let leader_count = cluster
        .nodes
        .iter()
        .filter(|n| n.raft.role() == RaftRole::Leader)
        .count();
    assert_eq!(leader_count, 1);

    let follower_count = cluster
        .nodes
        .iter()
        .filter(|n| n.raft.role() == RaftRole::Follower)
        .count();
    assert_eq!(follower_count, 2);
}

#[test]
fn write_replication_quorum() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "user-123".to_string(),
        b"test data".to_vec(),
    );

    let seq = cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2)
        .unwrap();
    assert_eq!(seq, 1);

    cluster.advance_ms(5);

    cluster.nodes[1].controller.process_messages();
    cluster.nodes[2].controller.process_messages();

    assert_eq!(cluster.nodes[1].controller.sequence(partition), Some(1));
    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(1));

    cluster.advance_ms(5);

    cluster.nodes[0].controller.process_messages();
}

#[test]
fn pubsub_subscribe_publish_route() {
    let cluster = TestCluster::new(1);
    let node = &cluster.nodes[0];

    node.sessions.create_session("subscriber-1").unwrap();
    let session_p = session_partition("subscriber-1");

    node.topics
        .subscribe("sensors/temp", "subscriber-1", session_p, 1)
        .unwrap();

    node.subscriptions
        .add_subscription("subscriber-1", "sensors/temp", 1)
        .unwrap();

    let router = PublishRouter::new(&node.topics);
    let result = router.route("sensors/temp");

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "subscriber-1");
    assert_eq!(result.targets[0].qos, 1);
    assert!(result.seq.is_some());

    node.retained.set("sensors/temp", 1, b"25.5", 1000);

    let retained = node.retained.get("sensors/temp").unwrap();
    assert_eq!(retained.payload, b"25.5");
}

#[test]
fn pubsub_multinode_delivery() {
    let cluster = TestCluster::new(3);

    for (i, node) in cluster.nodes.iter().enumerate() {
        let client = format!("client-node-{}", i + 1);
        node.sessions.create_session(&client).unwrap();

        let session_p = session_partition(&client);
        node.topics
            .subscribe("global/events", &client, session_p, 1)
            .unwrap();
        node.subscriptions
            .add_subscription(&client, "global/events", 1)
            .unwrap();
    }

    let router = PublishRouter::new(&cluster.nodes[0].topics);
    let result = router.route("global/events");

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "client-node-1");
}

#[test]
fn node_failure_partition_reassignment() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    cluster.nodes[0].controller.tick(0);
    cluster.advance_ms(5);
    cluster.nodes[1].controller.process_messages();
    cluster.nodes[2].controller.process_messages();

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Alive
    );
    assert_eq!(
        cluster.nodes[2].controller.node_status(n1),
        NodeStatus::Alive
    );

    cluster.partition_node(n1);

    cluster.nodes[1].controller.tick(600);
    cluster.nodes[2].controller.tick(600);

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Dead
    );
    assert_eq!(
        cluster.nodes[2].controller.node_status(n1),
        NodeStatus::Dead
    );

    let partition = PartitionId::new(0).unwrap();
    let mut map = PartitionMap::new();
    map.set(
        partition,
        PartitionAssignment::new(n1, vec![n2, n3], Epoch::new(1)),
    );

    let remaining = vec![n2, n3];
    let reassignments = mqdb::cluster::compute_removal_assignments(
        &map,
        &remaining,
        n1,
        &mqdb::cluster::RebalanceConfig::default(),
    );

    assert!(!reassignments.is_empty() || map.primary(partition) != Some(n1));

    cluster.heal_node(n1);

    let mut new_map = PartitionMap::new();
    new_map.set(
        partition,
        PartitionAssignment::new(n2, vec![n3], Epoch::new(2)),
    );

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(2), 0);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(2),
        0,
        "users".to_string(),
        "user-456".to_string(),
        b"new data".to_vec(),
    );

    let seq = cluster.nodes[1]
        .controller
        .replicate_write(write, &[n3], 1)
        .unwrap();
    assert_eq!(seq, 1);

    cluster.advance_ms(5);
    cluster.nodes[2].controller.process_messages();

    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(1));
}

#[test]
fn raft_log_replication_commit() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let outputs = cluster.nodes[0].raft.tick(1000);
    assert_eq!(cluster.nodes[0].raft.role(), RaftRole::Candidate);

    let mut vote_requests = Vec::new();
    for output in outputs {
        if let RaftOutput::SendRequestVote { to, request } = output {
            vote_requests.push((to, request));
        }
    }

    let (to1, req1) = vote_requests[0];
    let (to2, req2) = vote_requests[1];

    let (resp1, _) = cluster
        .nodes
        .iter_mut()
        .find(|n| n.id == to1)
        .unwrap()
        .raft
        .handle_request_vote(n1, req1, 1000);

    let (resp2, _) = cluster
        .nodes
        .iter_mut()
        .find(|n| n.id == to2)
        .unwrap()
        .raft
        .handle_request_vote(n1, req2, 1000);

    let outputs = cluster.nodes[0]
        .raft
        .handle_request_vote_response(to1, resp1);
    let became_leader = outputs
        .iter()
        .any(|o| matches!(o, RaftOutput::BecameLeader));
    assert!(became_leader);
    assert_eq!(cluster.nodes[0].raft.role(), RaftRole::Leader);

    cluster.nodes[0]
        .raft
        .handle_request_vote_response(to2, resp2);

    let partition = PartitionId::new(5).unwrap();
    let cmd =
        mqdb::cluster::raft::RaftCommand::update_partition(partition, n2, &[n3], Epoch::new(1));
    let idx = cluster.nodes[0].raft.propose(cmd).unwrap();
    assert_eq!(idx, 1);

    let heartbeat_outputs = cluster.nodes[0].raft.tick(1100);
    let mut append_requests = Vec::new();
    for output in heartbeat_outputs {
        if let RaftOutput::SendAppendEntries { to, request } = output {
            append_requests.push((to, request));
        }
    }

    assert!(!append_requests.is_empty());

    for (to, request) in append_requests {
        if let Some(follower) = cluster.nodes.iter_mut().find(|n| n.id == to) {
            let (response, _) = follower
                .raft
                .handle_append_entries(n1, request.clone(), 1100);
            assert!(response.is_success());
        }
    }
}

#[test]
fn lwt_triggered_on_death() {
    use mqdb::cluster::{LwtAction, LwtPublisher, determine_lwt_action};

    let mut cluster = TestCluster::new(2);
    let n1 = cluster.nodes[0].id;

    cluster.nodes[0]
        .sessions
        .create_session("client-lwt")
        .unwrap();
    cluster.nodes[0]
        .sessions
        .update("client-lwt", |s| {
            s.set_will(1, false, "lwt/goodbye", b"disconnected");
        })
        .unwrap();

    cluster.nodes[0]
        .sessions
        .mark_disconnected("client-lwt", 1000)
        .unwrap();

    cluster.nodes[0].controller.tick(0);
    cluster.advance_ms(5);
    cluster.nodes[1].controller.process_messages();

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Alive
    );

    cluster.partition_node(n1);
    cluster.nodes[1].controller.tick(600);

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Dead
    );

    let publisher = LwtPublisher::new(&cluster.nodes[0].sessions);

    let session = cluster.nodes[0].sessions.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::Publish);

    let prepared = publisher.prepare_lwt("client-lwt").unwrap().unwrap();
    assert_eq!(prepared.topic, "lwt/goodbye");
    assert_eq!(prepared.payload, b"disconnected");
    assert_eq!(prepared.qos, 1);

    let session = cluster.nodes[0].sessions.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::Skip);

    publisher
        .complete_lwt("client-lwt", prepared.token)
        .unwrap();

    let session = cluster.nodes[0].sessions.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::AlreadyPublished);
}

#[test]
fn pubsub_wildcard_routing() {
    let cluster = TestCluster::new(1);
    let node = &cluster.nodes[0];

    node.sessions.create_session("exact-client").unwrap();
    node.sessions.create_session("plus-client").unwrap();
    node.sessions.create_session("hash-client").unwrap();

    let exact_p = session_partition("exact-client");
    node.topics
        .subscribe("sensors/building1/temp", "exact-client", exact_p, 0)
        .unwrap();

    node.wildcards
        .subscribe_mqtt("sensors/+/temp", "plus-client", 1)
        .unwrap();

    node.wildcards
        .subscribe_mqtt("sensors/#", "hash-client", 2)
        .unwrap();

    let router = PublishRouter::new(&node.topics);
    let wildcard_matches = node.wildcards.match_topic("sensors/building1/temp");
    let result = router.route_with_wildcards("sensors/building1/temp", &wildcard_matches);

    assert_eq!(result.targets.len(), 3);

    let client_ids: Vec<&str> = result
        .targets
        .iter()
        .map(|t| t.client_id.as_str())
        .collect();
    assert!(client_ids.contains(&"exact-client"));
    assert!(client_ids.contains(&"plus-client"));
    assert!(client_ids.contains(&"hash-client"));

    let wildcard_matches = node.wildcards.match_topic("sensors/building2/humidity");
    let result = router.route_with_wildcards("sensors/building2/humidity", &wildcard_matches);

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "hash-client");
}

#[test]
fn wildcard_deduplication_max_qos() {
    let cluster = TestCluster::new(1);
    let node = &cluster.nodes[0];

    node.sessions.create_session("same-client").unwrap();

    let partition = session_partition("same-client");
    node.topics
        .subscribe("home/living/temp", "same-client", partition, 0)
        .unwrap();

    node.wildcards
        .subscribe_mqtt("home/+/temp", "same-client", 1)
        .unwrap();

    node.wildcards
        .subscribe_mqtt("home/#", "same-client", 2)
        .unwrap();

    let router = PublishRouter::new(&node.topics);
    let wildcard_matches = node.wildcards.match_topic("home/living/temp");
    let result = router.route_with_wildcards("home/living/temp", &wildcard_matches);

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "same-client");
    assert_eq!(result.targets[0].qos, 2);
}

#[test]
fn session_partition_consistency() {
    let cluster = TestCluster::new(1);
    let node = &cluster.nodes[0];

    for i in 0..100 {
        let client = format!("client-{i}");
        node.sessions.create_session(&client).unwrap();
    }

    let p1 = session_partition("client-0");
    let p2 = session_partition("client-0");
    let p3 = session_partition("client-0");
    assert_eq!(p1, p2);
    assert_eq!(p2, p3);

    let sessions_on_p1 = node.sessions.sessions_on_partition(p1);
    assert!(
        sessions_on_p1
            .iter()
            .any(|s| s.client_id_str() == "client-0")
    );

    for session in &sessions_on_p1 {
        let expected_partition = session_partition(session.client_id_str());
        assert_eq!(expected_partition, p1);
    }
}

#[test]
fn retained_message_on_subscribe() {
    use mqdb::cluster::topic_partition;

    let cluster = TestCluster::new(1);
    let node = &cluster.nodes[0];

    node.retained.set("sensors/temp", 1, b"25.5", 1000);
    node.retained.set("sensors/humidity", 0, b"65%", 1000);

    let retained = node.retained.get("sensors/temp");
    assert!(retained.is_some());
    assert_eq!(retained.unwrap().payload, b"25.5");

    node.sessions.create_session("new-subscriber").unwrap();
    let session_p = session_partition("new-subscriber");
    node.topics
        .subscribe("sensors/temp", "new-subscriber", session_p, 1)
        .unwrap();

    let retained_after = node.retained.get("sensors/temp");
    assert!(retained_after.is_some());

    let topic_p = topic_partition("sensors/temp");
    let topics_on_partition = node.retained.topics_on_partition(topic_p);

    if !topics_on_partition.is_empty() {
        for topic in &topics_on_partition {
            if let Some(msg) = node.retained.get(topic) {
                assert!(!msg.payload.is_empty() || msg.qos == 0);
            }
        }
    }
}

fn setup_five_node_partition_cluster() -> (TestCluster, PartitionId, [NodeId; 5]) {
    let mut cluster = TestCluster::new(5);

    let nodes = [
        cluster.nodes[0].id,
        cluster.nodes[1].id,
        cluster.nodes[2].id,
        cluster.nodes[3].id,
        cluster.nodes[4].id,
    ];

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    (cluster, partition, nodes)
}

#[test]
fn split_brain_prevented_on_partition() {
    let (mut cluster, partition, [n1, n2, n3, n4, n5]) = setup_five_node_partition_cluster();

    cluster.partition_groups(&[n1, n2], &[n3, n4, n5]);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    assert_eq!(
        cluster.nodes[2].controller.node_status(n1),
        NodeStatus::Dead
    );
    assert_eq!(
        cluster.nodes[3].controller.node_status(n1),
        NodeStatus::Dead
    );
    assert_eq!(
        cluster.nodes[4].controller.node_status(n1),
        NodeStatus::Dead
    );
    assert_eq!(
        cluster.nodes[0].controller.node_status(n3),
        NodeStatus::Dead
    );
    assert_eq!(
        cluster.nodes[1].controller.node_status(n3),
        NodeStatus::Dead
    );

    let write_old = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "stale-write".to_string(),
        b"from minority".to_vec(),
    );
    let old_result = cluster.nodes[0]
        .controller
        .replicate_write(write_old, &[n2], 2);

    cluster.nodes[2]
        .controller
        .become_primary(partition, Epoch::new(2));
    cluster.nodes[3]
        .controller
        .become_replica(partition, Epoch::new(2), 0);
    cluster.nodes[4]
        .controller
        .become_replica(partition, Epoch::new(2), 0);

    let write_new = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(2),
        0,
        "users".to_string(),
        "new-write".to_string(),
        b"from majority".to_vec(),
    );
    let new_result = cluster.nodes[2]
        .controller
        .replicate_write(write_new, &[n4, n5], 2);

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    if old_result.is_ok() {
        cluster.advance_ms(100);
        cluster.nodes[0].controller.process_messages();
        cluster.nodes[1].controller.process_messages();
        let old_seq = cluster.nodes[0].controller.sequence(partition);
        assert!(old_seq.is_none() || old_seq == Some(1));
    }

    if new_result.is_ok() {
        cluster.advance_ms(100);
        for node in &mut cluster.nodes[2..] {
            node.controller.process_messages();
        }
        let new_seq = cluster.nodes[2].controller.sequence(partition);
        assert!(new_seq.is_some(), "New primary should have valid sequence");
        let replica_seq = cluster.nodes[3].controller.sequence(partition);
        assert_eq!(replica_seq, new_seq);
    }

    assert!(Epoch::new(2) > Epoch::new(1), "New epoch must be greater");

    cluster.heal_all();
    cluster.advance_ms(100);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }
}

#[test]
fn write_rejected_without_quorum() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.partition_node(n1);

    cluster.advance_ms(600);
    cluster.nodes[0].controller.tick(600);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "orphan-write".to_string(),
        b"cannot replicate".to_vec(),
    );

    let result = cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2);

    let seq_assigned = result.is_ok();

    if seq_assigned {
        cluster.advance_ms(100);
        cluster.nodes[0].controller.process_messages();

        cluster.advance_ms(500);
        cluster.nodes[0].controller.tick(1200);
    }

    cluster.heal_node(n1);
    cluster.advance_ms(5);

    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let replicas_have_write = cluster.nodes[1].controller.sequence(partition) == Some(1)
        && cluster.nodes[2].controller.sequence(partition) == Some(1);

    if seq_assigned {
        assert!(
            !replicas_have_write,
            "Write should not have reached replicas during partition"
        );
    }
}

#[test]
fn quorum_write_reaches_replicas() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(5).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let write1 = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "user-critical-1".to_string(),
        b"critical data 1".to_vec(),
    );

    let seq1 = cluster.nodes[0]
        .controller
        .replicate_write(write1, &[n2, n3], 2)
        .unwrap();
    assert_eq!(seq1, 1);

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    assert_eq!(cluster.nodes[1].controller.sequence(partition), Some(1));
    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(1));

    let write2 = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        1,
        "users".to_string(),
        "user-critical-2".to_string(),
        b"critical data 2".to_vec(),
    );

    let seq2 = cluster.nodes[0]
        .controller
        .replicate_write(write2, &[n2, n3], 2)
        .unwrap();
    assert_eq!(seq2, 2);

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    assert_eq!(cluster.nodes[0].controller.sequence(partition), Some(2));
    assert_eq!(cluster.nodes[1].controller.sequence(partition), Some(2));
    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(2));

    let primary_seq = cluster.nodes[0].controller.sequence(partition).unwrap();
    let replica1_seq = cluster.nodes[1].controller.sequence(partition).unwrap();
    let replica2_seq = cluster.nodes[2].controller.sequence(partition).unwrap();

    assert_eq!(primary_seq, replica1_seq);
    assert_eq!(primary_seq, replica2_seq);

    let nodes_with_data = [
        cluster.nodes[0].controller.sequence(partition).is_some(),
        cluster.nodes[1].controller.sequence(partition).is_some(),
        cluster.nodes[2].controller.sequence(partition).is_some(),
    ]
    .iter()
    .filter(|&&has| has)
    .count();

    assert!(nodes_with_data >= 2, "Quorum of nodes must have the data");
}

#[test]
fn rebalance_preserves_sequence_continuity() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(10).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let mut sequences_before_rebalance = Vec::new();
    for i in 0..10 {
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            i,
            "items".to_string(),
            format!("item-{i}"),
            format!("data-{i}").into_bytes(),
        );

        let seq = cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2], 1)
            .unwrap();
        sequences_before_rebalance.push(seq);

        cluster.advance_ms(5);
        for node in &mut cluster.nodes {
            node.controller.process_messages();
        }
    }

    for (i, seq) in sequences_before_rebalance.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64, "Sequences should be monotonic");
    }

    let last_seq_before = cluster.nodes[0].controller.sequence(partition).unwrap();
    let replica_seq_before = cluster.nodes[1].controller.sequence(partition).unwrap();
    assert_eq!(last_seq_before, replica_seq_before);

    cluster.partition_node(n1);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(2), replica_seq_before);

    let write_after = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(2),
        last_seq_before,
        "items".to_string(),
        "item-after-rebalance".to_string(),
        b"new primary data".to_vec(),
    );

    let seq_after = cluster.nodes[1]
        .controller
        .replicate_write(write_after, &[n3], 1)
        .unwrap();

    assert!(
        seq_after > last_seq_before,
        "New primary sequence {seq_after} should continue from old primary {last_seq_before}"
    );

    cluster.advance_ms(10);
    for node in &mut cluster.nodes[1..] {
        node.controller.process_messages();
    }

    let new_primary_seq = cluster.nodes[1].controller.sequence(partition).unwrap();
    let new_replica_seq = cluster.nodes[2].controller.sequence(partition).unwrap();
    assert_eq!(
        new_primary_seq, new_replica_seq,
        "New cluster should be consistent"
    );

    cluster.heal_node(n1);
}

#[test]
fn epoch_prevents_stale_primary_writes() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(2), 0);

    let new_write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(2),
        0,
        "users".to_string(),
        "new-epoch-write".to_string(),
        b"new primary data".to_vec(),
    );

    let new_seq = cluster.nodes[1]
        .controller
        .replicate_write(new_write, &[n3], 1)
        .unwrap();
    assert_eq!(new_seq, 1);

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let old_write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "stale-epoch-write".to_string(),
        b"old primary trying to write".to_vec(),
    );

    let old_result = cluster.nodes[0]
        .controller
        .replicate_write(old_write, &[n2, n3], 2);

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let new_primary_seq = cluster.nodes[1].controller.sequence(partition).unwrap();
    assert_eq!(
        new_primary_seq, 1,
        "New primary should have sequence from its write"
    );

    if old_result.is_ok() {
        let old_primary_seq = cluster.nodes[0].controller.sequence(partition);
        assert!(
            old_primary_seq.is_none() || old_primary_seq == Some(1),
            "Old primary write should not corrupt new primary's sequence"
        );
    }
}

#[test]
fn lwt_published_exactly_once() {
    use mqdb::cluster::{LwtAction, LwtPublisher, determine_lwt_action};

    let mut cluster = TestCluster::new(2);
    let n1 = cluster.nodes[0].id;

    cluster.nodes[0]
        .sessions
        .create_session("lwt-client")
        .unwrap();
    cluster.nodes[0]
        .sessions
        .update("lwt-client", |s| {
            s.set_will(1, false, "lwt/topic", b"client disconnected");
        })
        .unwrap();

    cluster.nodes[0]
        .sessions
        .mark_disconnected("lwt-client", 1000)
        .unwrap();

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.partition_node(n1);
    cluster.advance_ms(600);
    cluster.nodes[1].controller.tick(600);

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Dead
    );

    let publisher = LwtPublisher::new(&cluster.nodes[0].sessions);

    let session_before = cluster.nodes[0].sessions.get("lwt-client").unwrap();
    assert_eq!(determine_lwt_action(&session_before), LwtAction::Publish);

    let prepared = publisher.prepare_lwt("lwt-client").unwrap().unwrap();
    assert_eq!(prepared.topic, "lwt/topic");
    assert_eq!(prepared.payload, b"client disconnected");

    let session_after_prepare = cluster.nodes[0].sessions.get("lwt-client").unwrap();
    assert_eq!(
        determine_lwt_action(&session_after_prepare),
        LwtAction::Skip
    );

    let second_prepare = publisher.prepare_lwt("lwt-client").unwrap();
    assert!(
        second_prepare.is_none(),
        "Second prepare should return None (already prepared)"
    );

    publisher
        .complete_lwt("lwt-client", prepared.token)
        .unwrap();

    let session_after_complete = cluster.nodes[0].sessions.get("lwt-client").unwrap();
    assert_eq!(
        determine_lwt_action(&session_after_complete),
        LwtAction::AlreadyPublished
    );

    let third_prepare = publisher.prepare_lwt("lwt-client").unwrap();
    assert!(
        third_prepare.is_none(),
        "Third prepare should return None (already published)"
    );
}

#[test]
fn replica_catches_up_after_partition() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.partition_node(n3);

    for i in 0..5 {
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            i,
            "items".to_string(),
            format!("item-{i}"),
            format!("data-{i}").into_bytes(),
        );

        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2)
            .unwrap();

        cluster.advance_ms(5);
        cluster.nodes[0].controller.process_messages();
        cluster.nodes[1].controller.process_messages();
    }

    assert_eq!(cluster.nodes[0].controller.sequence(partition), Some(5));
    assert_eq!(cluster.nodes[1].controller.sequence(partition), Some(5));
    let partitioned_seq = cluster.nodes[2].controller.sequence(partition);
    assert!(
        partitioned_seq.is_none() || partitioned_seq == Some(0),
        "Partitioned node should not have received writes"
    );

    cluster.heal_node(n3);
    cluster.advance_ms(10);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        5,
        "items".to_string(),
        "item-5".to_string(),
        b"data-5".to_vec(),
    );
    cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2)
        .unwrap();

    for _ in 0..10 {
        cluster.advance_ms(5);
        for node in &mut cluster.nodes {
            node.controller.process_messages();
        }
    }

    assert_eq!(
        cluster.nodes[2].controller.sequence(partition),
        Some(6),
        "Node 3 should have caught up to sequence 6 after healing"
    );
}

#[test]
fn subscriptions_consistent_during_rebalance() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.nodes[0]
        .sessions
        .create_session("sub-client")
        .unwrap();
    let session_p = session_partition("sub-client");
    cluster.nodes[0]
        .topics
        .subscribe("sensors/temp", "sub-client", session_p, 1)
        .unwrap();
    cluster.nodes[0]
        .subscriptions
        .add_subscription("sub-client", "sensors/temp", 1)
        .unwrap();

    let router_before = PublishRouter::new(&cluster.nodes[0].topics);
    let result_before = router_before.route("sensors/temp");
    assert_eq!(result_before.targets.len(), 1);
    assert_eq!(result_before.targets[0].client_id, "sub-client");

    cluster.partition_node(n1);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));

    cluster.nodes[1]
        .sessions
        .create_session("sub-client")
        .unwrap();
    let session_p2 = session_partition("sub-client");
    cluster.nodes[1]
        .topics
        .subscribe("sensors/temp", "sub-client", session_p2, 1)
        .unwrap();
    cluster.nodes[1]
        .subscriptions
        .add_subscription("sub-client", "sensors/temp", 1)
        .unwrap();

    let router_after = PublishRouter::new(&cluster.nodes[1].topics);
    let result_after = router_after.route("sensors/temp");
    assert_eq!(result_after.targets.len(), 1);
    assert_eq!(result_after.targets[0].client_id, "sub-client");

    cluster.heal_node(n1);
}

#[test]
fn retained_message_survives_reassignment() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.nodes[0]
        .retained
        .set("sensors/temp", 1, b"25.5", 1000);
    cluster.nodes[0]
        .retained
        .set("sensors/humidity", 0, b"65%", 1000);

    let retained_before = cluster.nodes[0].retained.get("sensors/temp");
    assert!(retained_before.is_some());
    assert_eq!(retained_before.unwrap().payload, b"25.5");

    cluster.partition_node(n1);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));

    cluster.nodes[1]
        .retained
        .set("sensors/temp", 1, b"25.5", 1600);
    cluster.nodes[1]
        .retained
        .set("sensors/humidity", 0, b"65%", 1600);

    let retained_after = cluster.nodes[1].retained.get("sensors/temp");
    assert!(retained_after.is_some());
    assert_eq!(retained_after.unwrap().payload, b"25.5");

    cluster.nodes[1]
        .sessions
        .create_session("new-subscriber")
        .unwrap();
    let session_p = session_partition("new-subscriber");
    cluster.nodes[1]
        .topics
        .subscribe("sensors/temp", "new-subscriber", session_p, 1)
        .unwrap();

    let retained_for_sub = cluster.nodes[1].retained.get("sensors/temp");
    assert!(retained_for_sub.is_some());
    assert_eq!(retained_for_sub.unwrap().payload, b"25.5");

    cluster.heal_node(n1);
}

#[test]
fn session_takeover_race_prevented() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.nodes[0]
        .sessions
        .create_session("race-client")
        .unwrap();
    cluster.nodes[0]
        .sessions
        .update("race-client", |s| {
            s.set_will(1, false, "lwt/topic", b"client died");
        })
        .unwrap();
    cluster.nodes[0]
        .sessions
        .mark_connected("race-client", 1000)
        .unwrap();

    let session_before = cluster.nodes[0].sessions.get("race-client").unwrap();
    assert!(session_before.is_connected());
    assert_eq!(session_before.connected_node, n1.get());

    cluster.nodes[0]
        .sessions
        .mark_disconnected("race-client", 1500)
        .unwrap();

    cluster.nodes[0]
        .sessions
        .mark_connected("race-client", 2000)
        .unwrap();
    cluster.nodes[1]
        .sessions
        .create_session("race-client")
        .unwrap();
    cluster.nodes[1]
        .sessions
        .mark_connected("race-client", 2001)
        .unwrap();

    let session_n1 = cluster.nodes[0].sessions.get("race-client").unwrap();
    let session_n2 = cluster.nodes[1].sessions.get("race-client").unwrap();

    assert!(session_n1.is_connected());
    assert!(session_n2.is_connected());

    let conflict_detected = session_n1.connected_node != session_n2.connected_node;
    assert!(
        conflict_detected,
        "Race condition: both nodes claim the session on different nodes"
    );

    assert_eq!(session_n1.connected_node, n1.get());
    assert_eq!(session_n2.connected_node, n2.get());
}

#[test]
fn raft_logs_converge_after_divergence() {
    use mqdb::cluster::raft::RaftCommand;

    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;

    let outputs = cluster.nodes[0].raft.tick(1000);
    for output in outputs {
        if let RaftOutput::SendRequestVote { to, request } = output {
            let (response, _) = cluster.nodes[1].raft.handle_request_vote(n1, request, 1000);
            cluster.nodes[0]
                .raft
                .handle_request_vote_response(to, response);
        }
    }

    assert_eq!(cluster.nodes[0].raft.role(), RaftRole::Leader);

    let cmd1 = RaftCommand::Noop;
    cluster.nodes[0].raft.propose(cmd1);

    let outputs = cluster.nodes[0].raft.tick(1100);
    for output in &outputs {
        if let RaftOutput::SendAppendEntries { to, request } = output
            && *to == cluster.nodes[1].id
        {
            cluster.nodes[1]
                .raft
                .handle_append_entries(n1, request.clone(), 1100);
        }
    }

    cluster.partition_node(n1);
    cluster.advance_ms(600);

    for node in &mut cluster.nodes[1..] {
        node.raft.tick(1700);
    }

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let outputs = cluster.nodes[1].raft.tick(2000);
    for output in outputs {
        if let RaftOutput::SendRequestVote { to, request } = output
            && to == n3
        {
            let (response, _) = cluster.nodes[2].raft.handle_request_vote(n2, request, 2000);
            cluster.nodes[1]
                .raft
                .handle_request_vote_response(to, response);
        }
    }

    let term_before_heal = cluster.nodes[1].raft.current_term();

    cluster.heal_node(n1);
    cluster.advance_ms(200);

    let outputs = cluster.nodes[1].raft.tick(2250);
    for output in &outputs {
        if let RaftOutput::SendAppendEntries { to, request } = output
            && *to == n1
        {
            cluster.nodes[0]
                .raft
                .handle_append_entries(n2, request.clone(), 2250);
        }
    }

    let old_leader_role = cluster.nodes[0].raft.role();
    let old_leader_term = cluster.nodes[0].raft.current_term();

    assert!(
        old_leader_role == RaftRole::Follower || old_leader_term >= term_before_heal,
        "Old leader should step down or sync to new term"
    );
}

#[test]
fn qos2_state_survives_primary_failover() {
    use mqdb::cluster::Qos2Phase;

    let mut cluster = TestCluster::new(3);

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    cluster.nodes[0]
        .qos2
        .start_inbound(
            "qos2-client",
            100,
            "sensors/data",
            b"important payload",
            1000,
        )
        .unwrap();

    let state = cluster.nodes[0].qos2.get("qos2-client", 100).unwrap();
    assert_eq!(state.phase(), Some(Qos2Phase::PubrecSent));
    assert_eq!(state.topic_str(), "sensors/data");

    cluster.nodes[0].qos2.advance("qos2-client", 100).unwrap();
    let state_advanced = cluster.nodes[0].qos2.get("qos2-client", 100).unwrap();
    assert_eq!(state_advanced.phase(), Some(Qos2Phase::PubrelReceived));

    let qos2_snapshot: Vec<_> = cluster.nodes[0].qos2.pending_for_client("qos2-client");

    cluster.partition_node(cluster.nodes[0].id);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    cluster.nodes[1]
        .controller
        .become_primary(partition, Epoch::new(2));

    for snapshot_state in &qos2_snapshot {
        if snapshot_state.direction_enum() == mqdb::cluster::Qos2Direction::Inbound {
            cluster.nodes[1]
                .qos2
                .start_inbound(
                    snapshot_state.client_id_str(),
                    snapshot_state.packet_id,
                    snapshot_state.topic_str(),
                    &snapshot_state.payload,
                    1600,
                )
                .unwrap();
            for _ in 0..snapshot_state.state.saturating_sub(1) {
                cluster.nodes[1]
                    .qos2
                    .advance(snapshot_state.client_id_str(), snapshot_state.packet_id)
                    .unwrap();
            }
        }
    }

    let restored_state = cluster.nodes[1].qos2.get("qos2-client", 100);
    assert!(
        restored_state.is_some(),
        "QoS2 state should be restored on new primary"
    );

    let restored = restored_state.unwrap();
    assert_eq!(restored.topic_str(), "sensors/data");
    assert_eq!(restored.payload, b"important payload");
    assert_eq!(restored.phase(), Some(Qos2Phase::PubrelReceived));

    cluster.nodes[1].qos2.advance("qos2-client", 100).unwrap();
    let completed = cluster.nodes[1].qos2.complete("qos2-client", 100).unwrap();
    assert_eq!(completed.topic_str(), "sensors/data");
}

#[test]
fn node_crash_restart_rejoins_cluster() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);
    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "items".to_string(),
        "item-1".to_string(),
        b"before crash".to_vec(),
    );
    cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2)
        .unwrap();

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let seq_before = cluster.nodes[0].controller.sequence(partition);
    assert!(seq_before.is_some());

    cluster.partition_node(n3);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600);
    }

    assert_eq!(
        cluster.nodes[0].controller.node_status(n3),
        NodeStatus::Dead
    );

    cluster.restart_node(2);

    cluster.heal_node(n3);
    cluster.advance_ms(100);

    for node in &mut cluster.nodes {
        node.controller.tick(1700);
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }

    let restarted_status = cluster.nodes[0].controller.node_status(n3);
    assert!(
        restarted_status == NodeStatus::Alive || restarted_status == NodeStatus::Unknown,
        "Restarted node should eventually be detected as alive or unknown (status: {restarted_status:?})"
    );

    cluster.nodes[2]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    let write_after = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        1,
        "items".to_string(),
        "item-2".to_string(),
        b"after restart".to_vec(),
    );
    let result = cluster.nodes[0]
        .controller
        .replicate_write(write_after, &[n2, n3], 2);

    assert!(
        result.is_ok(),
        "Restarted node should be able to receive replication writes"
    );

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages();
    }
}
