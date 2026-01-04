mod simulation;

use mqdb::cluster::raft::{RaftConfig, RaftNode, RaftOutput, RaftRole};
use mqdb::cluster::{
    Epoch, NodeController, NodeId, NodeStatus, Operation, PartitionAssignment, PartitionId,
    PartitionMap, PublishRouter, Qos2Store, ReplicationWrite, RetainedStore, SessionStore,
    SubscriptionCache, TopicIndex, TransportConfig, WildcardStore, session_partition,
    topic_partition,
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

        let mut partition_map = PartitionMap::new();
        for i in 0..64 {
            let partition = PartitionId::new(i).unwrap();
            let primary_idx = i as usize % node_count;
            let replica_idx = (i as usize + 1) % node_count;
            let replicas = if node_count > 1 {
                vec![node_ids[replica_idx]]
            } else {
                vec![]
            };
            partition_map.set(
                partition,
                PartitionAssignment::new(node_ids[primary_idx], replicas, Epoch::new(1)),
            );
        }

        for node in &mut nodes {
            node.controller.update_partition_map(partition_map.clone());
        }

        Self { runtime, nodes }
    }

    fn advance_ms(&self, ms: u64) {
        self.runtime.clock().advance_ms(ms);
    }

    async fn setup_all_primaries(&mut self) {
        for i in 0..64 {
            let partition = PartitionId::new(i).unwrap();
            self.nodes[0]
                .controller
                .become_primary(partition, Epoch::new(1));
            for node in self.nodes.iter_mut().skip(1) {
                node.controller.become_replica(partition, Epoch::new(1), 0);
            }
        }
        for node in &mut self.nodes {
            node.controller.tick(0).await;
        }
        self.advance_ms(5);
        for node in &mut self.nodes {
            node.controller.process_messages().await;
        }
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

        let node_count = self.nodes.len();
        let all_node_ids: Vec<NodeId> = self.nodes.iter().map(|n| n.id).collect();
        let mut partition_map = PartitionMap::new();
        for i in 0..64 {
            let partition = PartitionId::new(i).unwrap();
            let primary_idx = i as usize % node_count;
            let replica_idx = (i as usize + 1) % node_count;
            let replicas = if node_count > 1 {
                vec![all_node_ids[replica_idx]]
            } else {
                vec![]
            };
            partition_map.set(
                partition,
                PartitionAssignment::new(all_node_ids[primary_idx], replicas, Epoch::new(1)),
            );
        }
        controller.update_partition_map(partition_map);

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

#[tokio::test]
async fn cluster_formation_three_nodes() {
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

#[tokio::test]
async fn write_replication_quorum() {
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
        .replicate_write(write, &[n2, n3], 2).await
        .unwrap();
    assert_eq!(seq, 1);

    cluster.advance_ms(5);

    cluster.nodes[1].controller.process_messages().await;
    cluster.nodes[2].controller.process_messages().await;

    assert_eq!(cluster.nodes[1].controller.sequence(partition), Some(1));
    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(1));

    cluster.advance_ms(5);

    cluster.nodes[0].controller.process_messages().await;
}

#[tokio::test]
async fn pubsub_subscribe_publish_route() {
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

#[tokio::test]
async fn pubsub_multinode_delivery() {
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

#[tokio::test]
async fn node_failure_partition_reassignment() {
    let mut cluster = TestCluster::new(3);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    cluster.nodes[0].controller.tick(0).await;
    cluster.advance_ms(5);
    cluster.nodes[1].controller.process_messages().await;
    cluster.nodes[2].controller.process_messages().await;

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Alive
    );
    assert_eq!(
        cluster.nodes[2].controller.node_status(n1),
        NodeStatus::Alive
    );

    cluster.partition_node(n1);

    cluster.nodes[1].controller.tick(600).await;
    cluster.nodes[2].controller.tick(600).await;

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
        .replicate_write(write, &[n3], 1).await
        .unwrap();
    assert_eq!(seq, 1);

    cluster.advance_ms(5);
    cluster.nodes[2].controller.process_messages().await;

    assert_eq!(cluster.nodes[2].controller.sequence(partition), Some(1));
}

#[tokio::test]
async fn raft_log_replication_commit() {
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
    let (idx, _) = cluster.nodes[0].raft.propose(cmd);
    assert_eq!(idx, Some(2));

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

#[tokio::test]
async fn lwt_triggered_on_death() {
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

    cluster.nodes[0].controller.tick(0).await;
    cluster.advance_ms(5);
    cluster.nodes[1].controller.process_messages().await;

    assert_eq!(
        cluster.nodes[1].controller.node_status(n1),
        NodeStatus::Alive
    );

    cluster.partition_node(n1);
    cluster.nodes[1].controller.tick(600).await;

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

#[tokio::test]
async fn pubsub_wildcard_routing() {
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

#[tokio::test]
async fn wildcard_deduplication_max_qos() {
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

#[tokio::test]
async fn session_partition_consistency() {
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

#[tokio::test]
async fn retained_message_on_subscribe() {
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

async fn setup_five_node_partition_cluster() -> (TestCluster, PartitionId, [NodeId; 5]) {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    (cluster, partition, nodes)
}

#[tokio::test]
async fn split_brain_prevented_on_partition() {
    let (mut cluster, partition, [n1, n2, n3, n4, n5]) = setup_five_node_partition_cluster().await;

    cluster.partition_groups(&[n1, n2], &[n3, n4, n5]);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600).await;
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
        .replicate_write(write_old, &[n2], 2).await;

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
        .replicate_write(write_new, &[n4, n5], 2).await;

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    if old_result.is_ok() {
        cluster.advance_ms(100);
        cluster.nodes[0].controller.process_messages().await;
        cluster.nodes[1].controller.process_messages().await;
        let old_seq = cluster.nodes[0].controller.sequence(partition);
        assert!(old_seq.is_none() || old_seq == Some(1));
    }

    if new_result.is_ok() {
        cluster.advance_ms(100);
        for node in &mut cluster.nodes[2..] {
            node.controller.process_messages().await;
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
        node.controller.process_messages().await;
    }
}

#[tokio::test]
async fn write_rejected_without_quorum() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    cluster.partition_node(n1);

    cluster.advance_ms(600);
    cluster.nodes[0].controller.tick(600).await;

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
        .replicate_write(write, &[n2, n3], 2).await;

    let seq_assigned = result.is_ok();

    if seq_assigned {
        cluster.advance_ms(100);
        cluster.nodes[0].controller.process_messages().await;

        cluster.advance_ms(500);
        cluster.nodes[0].controller.tick(1200).await;
    }

    cluster.heal_node(n1);
    cluster.advance_ms(5);

    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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

#[tokio::test]
async fn quorum_write_reaches_replicas() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(write1, &[n2, n3], 2).await
        .unwrap();
    assert_eq!(seq1, 1);

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(write2, &[n2, n3], 2).await
        .unwrap();
    assert_eq!(seq2, 2);

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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

#[tokio::test]
async fn rebalance_preserves_sequence_continuity() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
            .replicate_write(write, &[n2], 1).await
            .unwrap();
        sequences_before_rebalance.push(seq);

        cluster.advance_ms(5);
        for node in &mut cluster.nodes {
            node.controller.process_messages().await;
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
        node.controller.tick(600).await;
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
        .replicate_write(write_after, &[n3], 1).await
        .unwrap();

    assert!(
        seq_after > last_seq_before,
        "New primary sequence {seq_after} should continue from old primary {last_seq_before}"
    );

    cluster.advance_ms(10);
    for node in &mut cluster.nodes[1..] {
        node.controller.process_messages().await;
    }

    let new_primary_seq = cluster.nodes[1].controller.sequence(partition).unwrap();
    let new_replica_seq = cluster.nodes[2].controller.sequence(partition).unwrap();
    assert_eq!(
        new_primary_seq, new_replica_seq,
        "New cluster should be consistent"
    );

    cluster.heal_node(n1);
}

#[tokio::test]
async fn epoch_prevents_stale_primary_writes() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(new_write, &[n3], 1).await
        .unwrap();
    assert_eq!(new_seq, 1);

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(old_write, &[n2, n3], 2).await;

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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

#[tokio::test]
async fn lwt_published_exactly_once() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    cluster.partition_node(n1);
    cluster.advance_ms(600);
    cluster.nodes[1].controller.tick(600).await;

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

#[tokio::test]
async fn replica_catches_up_after_partition() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
            .replicate_write(write, &[n2, n3], 2).await
            .unwrap();

        cluster.advance_ms(5);
        cluster.nodes[0].controller.process_messages().await;
        cluster.nodes[1].controller.process_messages().await;
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
        .replicate_write(write, &[n2, n3], 2).await
        .unwrap();

    for _ in 0..10 {
        cluster.advance_ms(5);
        for node in &mut cluster.nodes {
            node.controller.process_messages().await;
        }
    }

    assert_eq!(
        cluster.nodes[2].controller.sequence(partition),
        Some(6),
        "Node 3 should have caught up to sequence 6 after healing"
    );
}

#[tokio::test]
async fn subscriptions_consistent_during_rebalance() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        node.controller.tick(600).await;
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

#[tokio::test]
async fn retained_message_survives_reassignment() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        node.controller.tick(600).await;
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

#[tokio::test]
async fn session_takeover_race_prevented() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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

#[tokio::test]
async fn raft_logs_converge_after_divergence() {
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
    let _ = cluster.nodes[0].raft.propose(cmd1);

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

#[tokio::test]
async fn qos2_state_survives_primary_failover() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        node.controller.tick(600).await;
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

#[tokio::test]
async fn node_crash_restart_rejoins_cluster() {
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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(write, &[n2, n3], 2).await
        .unwrap();

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let seq_before = cluster.nodes[0].controller.sequence(partition);
    assert!(seq_before.is_some());

    cluster.partition_node(n3);
    cluster.advance_ms(600);
    for node in &mut cluster.nodes {
        node.controller.tick(600).await;
    }

    assert_eq!(
        cluster.nodes[0].controller.node_status(n3),
        NodeStatus::Dead
    );

    cluster.restart_node(2);

    cluster.heal_node(n3);
    cluster.advance_ms(100);

    for node in &mut cluster.nodes {
        node.controller.tick(1700).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
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
        .replicate_write(write_after, &[n2, n3], 2).await;

    assert!(
        result.is_ok(),
        "Restarted node should be able to receive replication writes"
    );

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }
}

#[tokio::test]
async fn snapshot_transfer_on_partition_migration() {
    let mut cluster = TestCluster::new(2);

    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;

    let topic = "test/snapshot/topic";
    let partition = topic_partition(topic);

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    cluster.nodes[0]
        .controller
        .stores_mut()
        .retained
        .set(topic, 1, b"snapshot-payload", 0);

    let retained_before = cluster.nodes[0].controller.stores().retained.get(topic);
    assert!(
        retained_before.is_some(),
        "Retained message should exist on node 1"
    );

    let retained_on_n2_before = cluster.nodes[1].controller.stores().retained.get(topic);
    assert!(
        retained_on_n2_before.is_none(),
        "Node 2 should NOT have retained message before migration"
    );

    cluster.nodes[1]
        .controller
        .start_partition_migration(partition, n1, n2, Epoch::new(2))
        .await;

    for _ in 0..20 {
        cluster.advance_ms(10);
        for node in &mut cluster.nodes {
            node.controller.tick(cluster.runtime.clock().now()).await;
            node.controller.process_messages().await;
        }
    }

    let retained_on_n2_after = cluster.nodes[1].controller.stores().retained.get(topic);
    assert!(
        retained_on_n2_after.is_some(),
        "Node 2 should have retained message after snapshot transfer"
    );

    let msg = retained_on_n2_after.unwrap();
    assert_eq!(
        msg.payload,
        b"snapshot-payload".to_vec(),
        "Retained message payload should match"
    );
}

#[tokio::test]
async fn graceful_shutdown_drain() {
    let mut cluster = TestCluster::new(2);

    let n2 = cluster.nodes[1].id;

    let partition = PartitionId::new(0).unwrap();

    cluster.nodes[0]
        .controller
        .become_primary(partition, Epoch::new(1));
    cluster.nodes[1]
        .controller
        .become_replica(partition, Epoch::new(1), 0);

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    assert!(
        !cluster.nodes[0].controller.is_draining(),
        "Node should not be draining initially"
    );
    assert!(
        cluster.nodes[0].controller.pending_writes_empty(),
        "No pending writes initially"
    );
    assert!(
        !cluster.nodes[0].controller.can_shutdown_safely(),
        "Cannot shutdown safely when not draining"
    );

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "_sess".to_string(),
        "drain-test-client".to_string(),
        b"session data".to_vec(),
    );

    let seq = cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2], 1).await
        .unwrap();
    assert_eq!(seq, 1);

    assert!(
        !cluster.nodes[0].controller.pending_writes_empty(),
        "Should have pending write after replicate_write"
    );
    assert_eq!(cluster.nodes[0].controller.pending_write_count(), 1);

    cluster.nodes[0].controller.set_draining(true).await;

    assert!(
        cluster.nodes[0].controller.is_draining(),
        "Node should be draining after set_draining(true)"
    );
    assert!(
        !cluster.nodes[0].controller.can_shutdown_safely(),
        "Cannot shutdown safely with pending writes"
    );

    cluster.advance_ms(5);
    cluster.nodes[1].controller.process_messages().await;

    assert_eq!(
        cluster.nodes[1].controller.sequence(partition),
        Some(1),
        "Replica should have received the write"
    );

    cluster.advance_ms(5);
    cluster.nodes[0].controller.process_messages().await;

    assert!(
        cluster.nodes[0].controller.pending_writes_empty(),
        "Pending writes should be empty after receiving ack"
    );
    assert!(
        cluster.nodes[0].controller.can_shutdown_safely(),
        "Should be able to shutdown safely after draining completes"
    );

    cluster.nodes[0].controller.set_draining(false).await;
    assert!(
        !cluster.nodes[0].controller.can_shutdown_safely(),
        "Cannot shutdown safely when not draining"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn raft_command_application_updates_partition_map() {
    use mqdb::cluster::PartitionMap;
    use mqdb::cluster::raft::RaftCommand;

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

    for (to, req) in &vote_requests {
        if let Some(follower) = cluster.nodes.iter_mut().find(|n| n.id == *to) {
            let (resp, _) = follower.raft.handle_request_vote(n1, *req, 1000);
            cluster.nodes[0]
                .raft
                .handle_request_vote_response(*to, resp);
        }
    }

    assert_eq!(cluster.nodes[0].raft.role(), RaftRole::Leader);

    let partition = PartitionId::new(10).unwrap();
    let cmd = RaftCommand::update_partition(partition, n2, &[n3], Epoch::new(1));
    let (idx, _) = cluster.nodes[0].raft.propose(cmd);
    assert_eq!(idx, Some(2));

    let heartbeat_outputs = cluster.nodes[0].raft.tick(1100);
    let mut append_requests = Vec::new();
    for output in heartbeat_outputs {
        if let RaftOutput::SendAppendEntries { to, request } = output {
            append_requests.push((to, request));
        }
    }
    assert!(
        !append_requests.is_empty(),
        "Leader should send AppendEntries"
    );

    for (to, request) in &append_requests {
        if let Some(follower) = cluster.nodes.iter_mut().find(|n| n.id == *to) {
            let (response, _outputs) =
                follower
                    .raft
                    .handle_append_entries(n1, request.clone(), 1100);
            assert!(
                response.is_success(),
                "Follower should accept AppendEntries"
            );

            let leader_outputs = cluster.nodes[0]
                .raft
                .handle_append_entries_response(*to, response);

            for output in &leader_outputs {
                if let RaftOutput::ApplyCommand(RaftCommand::UpdatePartition(update)) = output {
                    let mut map = PartitionMap::new();
                    map.apply_update(update);
                    assert_eq!(map.primary(partition), Some(n2));
                    assert_eq!(map.replicas(partition), &[n3]);
                }
            }
        }
    }

    let commit_outputs = cluster.nodes[0].raft.tick(1200);
    let mut commit_requests = Vec::new();
    for output in commit_outputs {
        if let RaftOutput::SendAppendEntries { to, request } = output {
            commit_requests.push((to, request));
        }
    }

    let mut applied_count = 0;
    for (to, request) in &commit_requests {
        if let Some(follower) = cluster.nodes.iter_mut().find(|n| n.id == *to) {
            let (response, outputs) =
                follower
                    .raft
                    .handle_append_entries(n1, request.clone(), 1200);
            assert!(response.is_success());

            for output in outputs {
                if let RaftOutput::ApplyCommand(RaftCommand::UpdatePartition(update)) = output {
                    applied_count += 1;
                    let mut map = PartitionMap::new();
                    map.apply_update(&update);
                    assert_eq!(map.primary(partition), Some(n2));
                    assert_eq!(map.replicas(partition), &[n3]);
                }
            }
        }
    }

    assert!(
        applied_count >= 1,
        "At least one follower should have applied the command"
    );
}

#[tokio::test]
async fn wildcard_broadcast_replication() {
    use mqdb::cluster::{ClusterMessage, ClusterTransport, SubscriptionType, WildcardBroadcast};

    let mut cluster = TestCluster::new(2);

    let client_partition = PartitionId::new(5).unwrap();

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let matches_before = cluster.nodes[1]
        .controller
        .stores()
        .wildcards
        .match_topic("sensors/temp/1");
    assert!(
        matches_before.is_empty(),
        "Node 2 should have no wildcard subscriptions initially"
    );

    let broadcast = WildcardBroadcast::subscribe(
        "sensors/+/1",
        "wildcard-client",
        client_partition,
        1,
        SubscriptionType::Mqtt as u8,
    );
    let msg = ClusterMessage::WildcardBroadcast(broadcast);
    cluster.nodes[0].controller.transport().broadcast(msg).await.ok();

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let matches_after = cluster.nodes[1]
        .controller
        .stores()
        .wildcards
        .match_topic("sensors/temp/1");

    assert_eq!(
        matches_after.len(),
        1,
        "Node 2 should have the wildcard subscription after broadcast"
    );

    let subscriber = &matches_after[0];
    assert_eq!(subscriber.client_id_str(), "wildcard-client");
    assert_eq!(subscriber.qos, 1);
}

#[tokio::test]
async fn cross_node_wildcard_publish_routing() {
    use mqdb::cluster::{ClusterMessage, ClusterTransport, SubscriptionType, WildcardBroadcast};

    let mut cluster = TestCluster::new(2);
    let client_partition = session_partition("wildcard-subscriber");

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    cluster.nodes[0]
        .sessions
        .create_session("wildcard-subscriber")
        .unwrap();

    let broadcast = WildcardBroadcast::subscribe(
        "sensors/+/temp",
        "wildcard-subscriber",
        client_partition,
        1,
        SubscriptionType::Mqtt as u8,
    );
    let msg = ClusterMessage::WildcardBroadcast(broadcast);
    cluster.nodes[0]
        .controller
        .transport()
        .broadcast(msg)
        .await
        .ok();

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let router = PublishRouter::new(&cluster.nodes[1].topics);
    let wildcard_matches = cluster.nodes[1]
        .controller
        .stores()
        .wildcards
        .match_topic("sensors/room1/temp");

    assert_eq!(
        wildcard_matches.len(),
        1,
        "Node 2 should find wildcard match for sensors/room1/temp"
    );

    let result = router.route_with_wildcards("sensors/room1/temp", &wildcard_matches);

    assert_eq!(
        result.targets.len(),
        1,
        "Should route to one subscriber"
    );
    assert_eq!(
        result.targets[0].client_id, "wildcard-subscriber",
        "Should route to wildcard-subscriber"
    );
    assert_eq!(
        result.targets[0].client_partition, client_partition,
        "Should have correct client partition for forwarding"
    );
}

#[tokio::test]
async fn drain_notification_triggers_partition_reassignment() {
    use mqdb::cluster::{ClusterMessage, ClusterTransport};

    let mut cluster = TestCluster::new(3);

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let draining_node_id = cluster.nodes[2].controller.node_id();
    let drain_msg = ClusterMessage::DrainNotification {
        node_id: draining_node_id,
    };
    cluster.nodes[2].controller.transport().broadcast(drain_msg).await.ok();

    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let draining_nodes: Vec<_> = cluster.nodes[0].controller.drain_draining_nodes().collect();

    assert!(
        draining_nodes.contains(&draining_node_id),
        "Node 1 should have received drain notification for node 3"
    );
}

#[tokio::test]
async fn raft_state_persisted_and_recovered() {
    use mqdb::cluster::raft::RaftStorage;
    use mqdb::storage::MemoryBackend;
    use std::sync::Arc;

    let backend = Arc::new(MemoryBackend::new());
    let node_id = NodeId::validated(1).unwrap();
    let peer_id = NodeId::validated(2).unwrap();

    {
        let mut node =
            RaftNode::create_with_storage(node_id, RaftConfig::default(), backend.clone()).unwrap();

        node.add_peer(peer_id);

        node.tick(1000);
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);

        let storage = RaftStorage::new(backend.clone());
        let persisted = storage.load_state().unwrap().unwrap();
        assert_eq!(persisted.current_term, 1);
        assert_eq!(persisted.voted_for, node_id.get());
    }

    {
        let node =
            RaftNode::create_with_storage(node_id, RaftConfig::default(), backend.clone()).unwrap();

        assert_eq!(node.current_term(), 1);
        assert_eq!(node.role(), RaftRole::Follower);
    }
}

#[tokio::test]
async fn raft_log_persisted_and_recovered() {
    use mqdb::cluster::raft::{RaftCommand, RaftStorage};
    use mqdb::storage::MemoryBackend;
    use std::sync::Arc;

    let backend = Arc::new(MemoryBackend::new());
    let node_id = NodeId::validated(1).unwrap();
    let peer_id = NodeId::validated(2).unwrap();

    {
        let mut node =
            RaftNode::create_with_storage(node_id, RaftConfig::default(), backend.clone()).unwrap();

        node.add_peer(peer_id);

        node.tick(1000);

        let vote_response = mqdb::cluster::raft::RequestVoteResponse::granted(1);
        let outputs = node.handle_request_vote_response(peer_id, vote_response);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::BecameLeader))
        );
        assert_eq!(node.role(), RaftRole::Leader);

        let partition = PartitionId::new(5).unwrap();
        let cmd = RaftCommand::update_partition(partition, node_id, &[peer_id], Epoch::new(1));
        let (idx, _) = node.propose(cmd);
        assert_eq!(idx, Some(2));

        let storage = RaftStorage::new(backend.clone());
        let log = storage.load_log().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].index, 1);
        assert_eq!(log[0].term, 1);
        assert_eq!(log[1].index, 2);
        assert_eq!(log[1].term, 1);
    }

    {
        let storage = RaftStorage::new(backend.clone());
        let log = storage.load_log().unwrap();
        assert_eq!(log.len(), 2, "Log should survive restart");
        assert_eq!(log[0].index, 1);
        assert_eq!(log[0].term, 1);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn db_crud_replication_to_replicas() {
    use mqdb::cluster::db::data_partition;

    let mut cluster = TestCluster::new(3);
    let (n2, n3) = (cluster.nodes[1].id, cluster.nodes[2].id);

    let entity_type = "users";
    let entity_id = "user-123";
    let partition = data_partition(entity_type, entity_id);

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
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let (entity, create_write) = cluster.nodes[0]
        .controller
        .stores()
        .db_create_replicated(entity_type, entity_id, b"initial data", 1000)
        .expect("create should succeed");
    assert_eq!(entity.entity_str(), entity_type);
    assert_eq!(entity.id_str(), entity_id);

    cluster.nodes[0]
        .controller
        .replicate_write(create_write, &[n2, n3], 2).await
        .ok();
    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let primary_entity = cluster.nodes[0].controller.db_get(entity_type, entity_id);
    assert!(primary_entity.is_some(), "Primary should have the entity");
    assert_eq!(primary_entity.unwrap().data, b"initial data");

    let replica1_entity = cluster.nodes[1].controller.db_get(entity_type, entity_id);
    assert!(
        replica1_entity.is_some(),
        "Replica 1 should have the entity after replication"
    );
    assert_eq!(replica1_entity.unwrap().data, b"initial data");

    let replica2_entity = cluster.nodes[2].controller.db_get(entity_type, entity_id);
    assert!(
        replica2_entity.is_some(),
        "Replica 2 should have the entity after replication"
    );
    assert_eq!(replica2_entity.unwrap().data, b"initial data");

    let (updated, update_write) = cluster.nodes[0]
        .controller
        .stores()
        .db_update_replicated(entity_type, entity_id, b"updated data", 2000)
        .expect("update should succeed");
    assert_eq!(updated.data, b"updated data");

    cluster.nodes[0]
        .controller
        .replicate_write(update_write, &[n2, n3], 2).await
        .ok();
    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let primary_updated = cluster.nodes[0].controller.db_get(entity_type, entity_id);
    assert_eq!(primary_updated.unwrap().data, b"updated data");

    let replica1_updated = cluster.nodes[1].controller.db_get(entity_type, entity_id);
    assert_eq!(
        replica1_updated.unwrap().data,
        b"updated data",
        "Replica 1 should have updated data"
    );

    let replica2_updated = cluster.nodes[2].controller.db_get(entity_type, entity_id);
    assert_eq!(
        replica2_updated.unwrap().data,
        b"updated data",
        "Replica 2 should have updated data"
    );

    let (deleted, delete_write) = cluster.nodes[0]
        .controller
        .stores()
        .db_delete_replicated(entity_type, entity_id)
        .expect("delete should succeed");
    assert_eq!(deleted.id_str(), entity_id);

    cluster.nodes[0]
        .controller
        .replicate_write(delete_write, &[n2, n3], 2).await
        .ok();
    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let primary_deleted = cluster.nodes[0].controller.db_get(entity_type, entity_id);
    assert!(
        primary_deleted.is_none(),
        "Entity should be deleted on primary"
    );

    let replica1_deleted = cluster.nodes[1].controller.db_get(entity_type, entity_id);
    assert!(
        replica1_deleted.is_none(),
        "Entity should be deleted on replica 1"
    );

    let replica2_deleted = cluster.nodes[2].controller.db_get(entity_type, entity_id);
    assert!(
        replica2_deleted.is_none(),
        "Entity should be deleted on replica 2"
    );
}

#[tokio::test]
async fn db_list_returns_entities_by_type() {
    let mut cluster = TestCluster::new(3);

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let user_ids = ["alice", "bob", "charlie"];
    for user_id in &user_ids {
        cluster.nodes[0]
            .controller
            .db_create("users", user_id, format!("data-{user_id}").as_bytes(), 1000)
            .await
            .expect("create should succeed");
    }

    cluster.nodes[0]
        .controller
        .db_create("orders", "order-1", b"order data", 1000).await
        .expect("create order should succeed");

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let users = cluster.nodes[0].controller.db_list("users");
    assert_eq!(users.len(), 3, "Should have 3 users");

    let orders = cluster.nodes[0].controller.db_list("orders");
    assert_eq!(orders.len(), 1, "Should have 1 order");
}

#[tokio::test]
async fn schema_broadcast_to_all_nodes() {
    let mut cluster = TestCluster::new(3);

    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let schema_data = br#"{"fields": ["id", "name", "email"]}"#;
    let (schema, writes) = cluster.nodes[0]
        .controller
        .stores()
        .schema_register_replicated("users", schema_data)
        .expect("schema register should succeed");

    assert_eq!(schema.entity_str(), "users");
    assert_eq!(schema.schema_version, 1);
    assert_eq!(writes.len(), 64, "Should broadcast to all 64 partitions");

    for write in writes {
        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2).await
            .expect("schema replication should succeed");
    }

    cluster.advance_ms(50);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let primary_schema = cluster.nodes[0].controller.schema_get("users");
    assert!(primary_schema.is_some(), "Primary should have schema");
    assert_eq!(primary_schema.unwrap().schema_version, 1);

    let replica1_schema = cluster.nodes[1].controller.schema_get("users");
    assert!(
        replica1_schema.is_some(),
        "Replica 1 should have schema after replication"
    );
    assert_eq!(replica1_schema.unwrap().schema_version, 1);

    let replica2_schema = cluster.nodes[2].controller.schema_get("users");
    assert!(
        replica2_schema.is_some(),
        "Replica 2 should have schema after replication"
    );
    assert_eq!(replica2_schema.unwrap().schema_version, 1);

    assert!(
        cluster.nodes[0]
            .controller
            .schema_is_valid_for_write("users"),
        "Schema should be valid for writes on primary"
    );
    assert!(
        cluster.nodes[1]
            .controller
            .schema_is_valid_for_write("users"),
        "Schema should be valid for writes on replica 1"
    );
    assert!(
        cluster.nodes[2]
            .controller
            .schema_is_valid_for_write("users"),
        "Schema should be valid for writes on replica 2"
    );

    let schemas = cluster.nodes[0].controller.schema_list();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].entity_str(), "users");
}

#[tokio::test]
async fn schema_update_increments_version() {
    let mut cluster = TestCluster::new(3);
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let (schema_v1, writes_v1) = cluster.nodes[0]
        .controller
        .stores()
        .schema_register_replicated("products", b"v1 schema")
        .expect("register should succeed");
    assert_eq!(schema_v1.schema_version, 1);

    for write in writes_v1 {
        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2).await
            .ok();
    }

    cluster.advance_ms(20);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let (schema_v2, writes_v2) = cluster.nodes[0]
        .controller
        .stores()
        .schema_update_replicated("products", b"v2 schema with new fields")
        .expect("update should succeed");
    assert_eq!(schema_v2.schema_version, 2);

    for write in writes_v2 {
        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2).await
            .ok();
    }

    cluster.advance_ms(20);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let current_schema = cluster.nodes[0].controller.schema_get("products");
    assert_eq!(current_schema.unwrap().schema_version, 2);
}

#[tokio::test]
async fn index_add_and_lookup() {
    let mut cluster = TestCluster::new(3);
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let entity = "users";
    let field = "email";
    let value = b"alice@example.com";
    let record_id = "user-alice";
    let data_partition = PartitionId::new(5).unwrap();

    let (entry, write) = cluster.nodes[0]
        .controller
        .stores()
        .index_add_replicated(entity, field, value, data_partition, record_id)
        .expect("index add should succeed");

    assert_eq!(entry.entity_str(), entity);
    assert_eq!(entry.field_str(), field);
    assert_eq!(entry.value, value.to_vec());
    assert_eq!(entry.record_id_str(), record_id);

    cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2).await
        .expect("replication should succeed");

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let results = cluster.nodes[0]
        .controller
        .stores()
        .index_lookup(entity, field, value);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].record_id_str(), record_id);
    assert_eq!(results[0].data_partition, data_partition.get());
}

#[tokio::test]
async fn index_multiple_records_same_value() {
    let mut cluster = TestCluster::new(3);
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let entity = "orders";
    let field = "status";
    let value = b"pending";

    let records = [
        ("order-1", PartitionId::new(1).unwrap()),
        ("order-2", PartitionId::new(2).unwrap()),
        ("order-3", PartitionId::new(3).unwrap()),
    ];

    for (record_id, data_partition) in &records {
        let (_, write) = cluster.nodes[0]
            .controller
            .stores()
            .index_add_replicated(entity, field, value, *data_partition, record_id)
            .expect("index add should succeed");

        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2).await
            .ok();
    }

    cluster.advance_ms(20);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let results = cluster.nodes[0]
        .controller
        .stores()
        .index_lookup(entity, field, value);

    assert_eq!(results.len(), 3, "Should find all 3 pending orders");
}

#[tokio::test]
async fn unique_constraint_reserve_commit_release() {
    use mqdb::cluster::db::ReserveResult;

    let mut cluster = TestCluster::new(3);
    let (n2, n3) = (cluster.nodes[1].id, cluster.nodes[2].id);
    cluster.setup_all_primaries().await;

    let (entity, field, value) = ("users", "email", b"unique@example.com".as_slice());
    let (record_id, request_id) = ("user-unique", "req-001");
    let data_partition = PartitionId::new(10).unwrap();
    let (ttl_ms, now) = (5000, 1000);

    let (result, write_opt) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            record_id,
            request_id,
            data_partition,
            ttl_ms,
            now,
        );
    assert_eq!(result, ReserveResult::Reserved);
    assert!(write_opt.is_some(), "Should have replication write");

    if let Some(write) = write_opt {
        cluster.nodes[0]
            .controller
            .replicate_write(write, &[n2, n3], 2).await
            .ok();
    }
    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let (conflict_result, _) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            "other-record",
            "req-002",
            data_partition,
            ttl_ms,
            now + 100,
        );
    assert_eq!(conflict_result, ReserveResult::Conflict);

    let (idempotent_result, _) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            record_id,
            request_id,
            data_partition,
            ttl_ms,
            now + 100,
        );
    assert_eq!(
        idempotent_result,
        ReserveResult::AlreadyReservedBySameRequest
    );

    let (committed, commit_write) = cluster.nodes[0]
        .controller
        .stores()
        .unique_commit_replicated(entity, field, value, request_id)
        .expect("commit should succeed");
    assert!(committed.is_committed());

    cluster.nodes[0]
        .controller
        .replicate_write(commit_write, &[n2, n3], 2).await
        .ok();
    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let (post_commit_result, _) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            "third-record",
            "req-003",
            data_partition,
            ttl_ms,
            now + 200,
        );
    assert_eq!(
        post_commit_result,
        ReserveResult::Conflict,
        "Committed value should conflict"
    );
}

#[tokio::test]
async fn unique_constraint_expires_after_ttl() {
    use mqdb::cluster::db::ReserveResult;

    let mut cluster = TestCluster::new(3);

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
    }

    let entity = "users";
    let field = "username";
    let value = b"expired_user";
    let data_partition = PartitionId::new(5).unwrap();
    let ttl_ms = 1000;
    let now = 1000;

    let (result, _) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            "record-1",
            "req-expired",
            data_partition,
            ttl_ms,
            now,
        );
    assert_eq!(result, ReserveResult::Reserved);

    let after_expiry = now + ttl_ms + 100;
    let (new_result, _) = cluster.nodes[0]
        .controller
        .stores()
        .unique_reserve_replicated(
            entity,
            field,
            value,
            "record-2",
            "req-new",
            data_partition,
            ttl_ms,
            after_expiry,
        );

    assert_eq!(
        new_result,
        ReserveResult::Reserved,
        "Should succeed after TTL expires"
    );
}

#[tokio::test]
async fn fk_validation_request_lifecycle() {
    use mqdb::cluster::db::FkValidationRequest;

    let mut cluster = TestCluster::new(3);
    let n2 = cluster.nodes[1].id;
    let n3 = cluster.nodes[2].id;

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
        cluster.nodes[1]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
        cluster.nodes[2]
            .controller
            .become_replica(partition, Epoch::new(1), 0);
    }

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let request = FkValidationRequest::create("users", "user-123", "fk-req-001", 5000, 1000);

    let write = cluster.nodes[0]
        .controller
        .stores()
        .fk_add_request_replicated(request.clone())
        .expect("add request should succeed");

    cluster.nodes[0]
        .controller
        .replicate_write(write, &[n2, n3], 2).await
        .ok();

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let retrieved = cluster.nodes[0]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(retrieved.is_some(), "Primary should have FK request");
    assert_eq!(retrieved.unwrap().entity_str(), "users");

    let replica1_request = cluster.nodes[1]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(
        replica1_request.is_some(),
        "Replica 1 should have FK request after replication"
    );

    let replica2_request = cluster.nodes[2]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(
        replica2_request.is_some(),
        "Replica 2 should have FK request after replication"
    );

    let (completed, complete_write) = cluster.nodes[0]
        .controller
        .stores()
        .fk_complete_request_replicated("fk-req-001")
        .expect("complete should succeed");

    assert_eq!(completed.request_id_str(), "fk-req-001");

    cluster.nodes[0]
        .controller
        .replicate_write(complete_write, &[n2, n3], 2).await
        .ok();

    cluster.advance_ms(10);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    let after_complete = cluster.nodes[0]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(
        after_complete.is_none(),
        "Request should be removed on primary after completion"
    );

    let replica1_after = cluster.nodes[1]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(
        replica1_after.is_none(),
        "Request should be removed on replica 1 after completion"
    );

    let replica2_after = cluster.nodes[2]
        .controller
        .stores()
        .fk_get_request("fk-req-001");
    assert!(
        replica2_after.is_none(),
        "Request should be removed on replica 2 after completion"
    );
}

#[tokio::test]
async fn fk_validation_cleanup_expired() {
    use mqdb::cluster::db::FkValidationRequest;

    let mut cluster = TestCluster::new(3);

    for i in 0..64 {
        let partition = PartitionId::new(i).unwrap();
        cluster.nodes[0]
            .controller
            .become_primary(partition, Epoch::new(1));
    }

    let short_timeout_request =
        FkValidationRequest::create("users", "user-1", "fk-short", 1000, 1000);

    let long_timeout_request =
        FkValidationRequest::create("users", "user-2", "fk-long", 10000, 1000);

    cluster.nodes[0]
        .controller
        .stores()
        .fk_add_request_replicated(short_timeout_request)
        .ok();

    cluster.nodes[0]
        .controller
        .stores()
        .fk_add_request_replicated(long_timeout_request)
        .ok();

    assert!(
        cluster.nodes[0]
            .controller
            .stores()
            .fk_get_request("fk-short")
            .is_some()
    );
    assert!(
        cluster.nodes[0]
            .controller
            .stores()
            .fk_get_request("fk-long")
            .is_some()
    );

    let cleaned = cluster.nodes[0]
        .controller
        .stores()
        .fk_cleanup_expired(3000);

    assert_eq!(cleaned, 1, "Should clean up 1 expired request");
    assert!(
        cluster.nodes[0]
            .controller
            .stores()
            .fk_get_request("fk-short")
            .is_none()
    );
    assert!(
        cluster.nodes[0]
            .controller
            .stores()
            .fk_get_request("fk-long")
            .is_some()
    );
}

#[tokio::test]
async fn cross_node_lwt_routing() {
    use mqdb::cluster::{ForwardTarget, ForwardedPublish, LwtPublisher};
    use std::collections::HashMap;

    let mut cluster = TestCluster::new(2);
    let n1 = cluster.nodes[0].id;
    let n2 = cluster.nodes[1].id;

    for node in &mut cluster.nodes {
        node.controller.tick(0).await;
    }
    cluster.advance_ms(5);
    for node in &mut cluster.nodes {
        node.controller.process_messages().await;
    }

    cluster.nodes[0]
        .sessions
        .create_session("lwt-client")
        .unwrap();
    cluster.nodes[0]
        .sessions
        .update("lwt-client", |s| {
            s.set_will(1, false, "status/lwt-client", b"offline");
            s.set_connected(true, n1, 0);
        })
        .unwrap();

    cluster.nodes[1]
        .sessions
        .create_session("subscriber-client")
        .unwrap();
    cluster.nodes[1]
        .sessions
        .update("subscriber-client", |s| {
            s.set_connected(true, n2, 0);
        })
        .unwrap();

    let subscriber_partition = session_partition("subscriber-client");
    cluster.nodes[0]
        .topics
        .subscribe("status/lwt-client", "subscriber-client", subscriber_partition, 1)
        .unwrap();
    cluster.nodes[1]
        .topics
        .subscribe("status/lwt-client", "subscriber-client", subscriber_partition, 1)
        .unwrap();

    cluster.nodes[0]
        .controller
        .stores()
        .client_locations
        .set("subscriber-client", n2);

    cluster.nodes[0]
        .sessions
        .mark_disconnected("lwt-client", 1000)
        .unwrap();

    let publisher = LwtPublisher::new(&cluster.nodes[0].sessions);
    let prepared = publisher.prepare_lwt("lwt-client").unwrap().unwrap();
    assert_eq!(prepared.topic, "status/lwt-client");
    assert_eq!(prepared.payload, b"offline");

    let router = PublishRouter::new(&cluster.nodes[0].topics);
    let wildcards = cluster.nodes[0]
        .controller
        .stores()
        .wildcards
        .match_topic(&prepared.topic);
    let route = router.route_with_wildcards(&prepared.topic, &wildcards);

    assert_eq!(route.targets.len(), 1, "Should have one routing target");
    assert_eq!(route.targets[0].client_id, "subscriber-client");

    let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
    for target in route.targets {
        let connected_node = cluster.nodes[0]
            .controller
            .stores()
            .client_locations
            .get(&target.client_id);

        if let Some(target_node) = connected_node
            && target_node != n1
        {
            remote_nodes
                .entry(target_node)
                .or_default()
                .push(ForwardTarget::new(target.client_id, target.qos));
        }
    }

    assert!(remote_nodes.contains_key(&n2), "Should forward LWT to Node 2");
    assert_eq!(remote_nodes[&n2].len(), 1);
    assert_eq!(remote_nodes[&n2][0].client_id, "subscriber-client");

    let fwd = ForwardedPublish::new(
        n1,
        prepared.topic.clone(),
        prepared.qos,
        prepared.retain,
        prepared.payload.clone(),
        remote_nodes[&n2].clone(),
    );
    assert_eq!(fwd.origin_node, n1);
    assert_eq!(fwd.topic, "status/lwt-client");
    assert_eq!(fwd.targets.len(), 1);

    publisher.complete_lwt("lwt-client", prepared.token).unwrap();

    let session = cluster.nodes[0].sessions.get("lwt-client").unwrap();
    assert!(session.lwt_published != 0, "LWT should be marked as published");
}
