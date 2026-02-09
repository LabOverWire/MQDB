// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod simulation;

use mqdb::cluster::raft::RaftConfig;
use mqdb::cluster::{
    Epoch, NUM_PARTITIONS, NodeController, NodeId, Operation, PartitionAssignment, PartitionId,
    PartitionMap, PartitionRole, ReplicationWrite, TransportConfig,
};
use simulation::transport::SimulatedTransport;

fn test_raft_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        startup_grace_period_ms: 0,
    }
}

fn create_test_controller(
    node_id: NodeId,
    transport: SimulatedTransport,
    config: TransportConfig,
) -> NodeController<SimulatedTransport> {
    let (tx_raft_messages, _rx_raft_messages) = flume::unbounded();
    let (tx_raft_events, _rx_raft_events) = flume::unbounded();
    NodeController::new(node_id, transport, config, tx_raft_messages, tx_raft_events)
}

#[tokio::test]
async fn partition_assignment_determinism() {
    let p1 = PartitionId::from_entity_id("users", "123");
    let p2 = PartitionId::from_entity_id("users", "123");
    assert_eq!(p1, p2);

    let p3 = PartitionId::from_entity_id("users", "456");
    assert_eq!(PartitionId::from_entity_id("users", "456"), p3);
}

#[tokio::test]
async fn different_keys_may_hash_differently() {
    let p1 = PartitionId::from_entity_id("users", "123");
    let p2 = PartitionId::from_entity_id("orders", "789");
    let _ = (p1, p2);
}

#[tokio::test]
async fn partition_id_always_in_valid_range() {
    for i in 0..1000 {
        let entity = format!("entity_{i}");
        let id = format!("id_{}", i * 7);
        let partition = PartitionId::from_entity_id(&entity, &id);
        assert!(
            partition.get() < NUM_PARTITIONS,
            "partition {} >= {NUM_PARTITIONS}",
            partition.get()
        );
    }
}

#[tokio::test]
async fn epoch_ordering() {
    let e1 = Epoch::new(1);
    let e2 = Epoch::new(2);
    assert!(e1 < e2);
    assert_eq!(e1.next(), e2);
}

#[tokio::test]
async fn node_id_validation() {
    let node = NodeId::validated(1).unwrap();
    assert_eq!(node.get(), 1);
    assert!(node.is_valid());
    assert!(NodeId::validated(0).is_none());
}

#[tokio::test]
async fn partition_map_assignment() {
    let mut map = PartitionMap::new();

    let p0 = PartitionId::new(0).unwrap();
    let p1 = PartitionId::new(1).unwrap();
    let n1 = NodeId::validated(1).unwrap();
    let n2 = NodeId::validated(2).unwrap();

    map.set(p0, PartitionAssignment::new(n1, vec![n2], Epoch::new(1)));
    map.set(p1, PartitionAssignment::new(n2, vec![n1], Epoch::new(1)));

    assert_eq!(map.primary(p0).map(NodeId::get), Some(1));
    assert_eq!(map.primary(p1).map(NodeId::get), Some(2));
    assert_eq!(map.role_for(p0, n1), PartitionRole::Primary);
    assert_eq!(map.role_for(p0, n2), PartitionRole::Replica);
}

#[tokio::test]
async fn simulation_framework_three_nodes() {
    use simulation::framework::runtime::SimulatedRuntime;

    let rt = SimulatedRuntime::new();
    let net = rt.network();

    net.register_node(1);
    net.register_node(2);
    net.register_node(3);

    net.send(1, 2, b"hello from 1".to_vec());
    net.send(2, 3, b"hello from 2".to_vec());
    net.send(3, 1, b"hello from 3".to_vec());

    rt.clock().advance_ms(10);

    let msg_to_2 = net.receive(2).unwrap();
    assert_eq!(msg_to_2.from, 1);

    let msg_to_3 = net.receive(3).unwrap();
    assert_eq!(msg_to_3.from, 2);

    let msg_to_1 = net.receive(1).unwrap();
    assert_eq!(msg_to_1.from, 3);
}

#[tokio::test]
async fn simulation_network_partition() {
    use simulation::framework::runtime::SimulatedRuntime;

    let rt = SimulatedRuntime::new();
    let net = rt.network();

    net.register_node(1);
    net.register_node(2);
    net.register_node(3);

    net.partition(1, &[2, 3]);

    assert!(!net.send(1, 2, b"should fail".to_vec()));
    assert!(!net.send(1, 3, b"should fail".to_vec()));
    assert!(net.send(2, 3, b"should work".to_vec()));

    rt.clock().advance_ms(10);
    assert!(net.receive(3).is_some());
}

#[tokio::test]
async fn simulation_scheduled_tasks() {
    use simulation::framework::runtime::SimulatedRuntime;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let rt = SimulatedRuntime::new();
    let counter = Arc::new(AtomicU32::new(0));

    for i in 1..=3 {
        let c = counter.clone();
        rt.schedule_after(i * 10, move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
    }

    assert_eq!(rt.pending_tasks(), 3);

    rt.run_for_ms(25);
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    rt.run_for_ms(20);
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn two_node_replication() {
    use simulation::framework::VirtualClock;
    use simulation::framework::VirtualNetwork;
    use simulation::transport::SimulatedTransport;

    let clock = VirtualClock::new();
    let network = VirtualNetwork::new(clock.clone());

    let node1_id = NodeId::validated(1).unwrap();
    let node2_id = NodeId::validated(2).unwrap();

    let t1 = SimulatedTransport::new(node1_id, network.clone(), clock.clone());
    let t2 = SimulatedTransport::new(node2_id, network.clone(), clock.clone());

    t1.register_peer(node2_id);
    t2.register_peer(node1_id);

    let config = TransportConfig::default();
    let mut ctrl1 = create_test_controller(node1_id, t1, config);
    let mut ctrl2 = create_test_controller(node2_id, t2, config);

    let partition = PartitionId::new(0).unwrap();

    ctrl1.become_primary(partition, Epoch::new(1));
    ctrl2.become_replica(partition, Epoch::new(1), 0);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "123".to_string(),
        b"test data".to_vec(),
    );

    let seq = ctrl1.replicate_write(write, &[node2_id], 1).await.unwrap();
    assert_eq!(seq, 1);

    clock.advance_ms(5);

    ctrl2.process_messages().await;
    assert_eq!(ctrl2.sequence(partition), Some(1));

    clock.advance_ms(5);

    ctrl1.process_messages().await;
}

#[tokio::test]
async fn heartbeat_detection() {
    use mqdb::cluster::NodeStatus;
    use simulation::framework::VirtualClock;
    use simulation::framework::VirtualNetwork;
    use simulation::transport::SimulatedTransport;

    let clock = VirtualClock::new();
    let network = VirtualNetwork::new(clock.clone());

    let node1_id = NodeId::validated(1).unwrap();
    let node2_id = NodeId::validated(2).unwrap();

    let t1 = SimulatedTransport::new(node1_id, network.clone(), clock.clone());
    let t2 = SimulatedTransport::new(node2_id, network.clone(), clock.clone());

    t1.register_peer(node2_id);
    t2.register_peer(node1_id);

    let config = TransportConfig {
        heartbeat_interval_ms: 100,
        heartbeat_timeout_ms: 500,
        ack_timeout_ms: 50,
    };

    let mut ctrl1 = create_test_controller(node1_id, t1, config);
    let mut ctrl2 = create_test_controller(node2_id, t2, config);

    ctrl1.register_peer(node2_id);
    ctrl2.register_peer(node1_id);

    let mut partition_map = PartitionMap::new();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        let primary = if i % 2 == 0 { node1_id } else { node2_id };
        let replica = if i % 2 == 0 { node2_id } else { node1_id };
        partition_map.set(
            partition,
            PartitionAssignment::new(primary, vec![replica], Epoch::new(1)),
        );
    }
    ctrl1.update_partition_map(partition_map.clone());
    ctrl2.update_partition_map(partition_map);

    {
        let output = ctrl1.tick(0);
        ctrl1.send_tick_output(output).await;
    }
    clock.advance_ms(5);
    ctrl2.process_messages().await;

    assert_eq!(ctrl2.node_status(node1_id), NodeStatus::Alive);

    {
        let output = ctrl2.tick(600);
        ctrl2.send_tick_output(output).await;
    }
    assert_eq!(ctrl2.node_status(node1_id), NodeStatus::Dead);
}

#[tokio::test]
async fn raft_leader_election_three_nodes() {
    use mqdb::cluster::raft::{RaftNode, RaftOutput, RaftRole};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let config = test_raft_config();
    let mut n1 = RaftNode::create(node1, config);
    let config = test_raft_config();
    let mut n2 = RaftNode::create(node2, config);
    let config = test_raft_config();
    let mut n3 = RaftNode::create(node3, config);

    n1.add_peer(node2);
    n1.add_peer(node3);
    n2.add_peer(node1);
    n2.add_peer(node3);
    n3.add_peer(node1);
    n3.add_peer(node2);

    assert_eq!(n1.role(), RaftRole::Follower);
    assert_eq!(n2.role(), RaftRole::Follower);
    assert_eq!(n3.role(), RaftRole::Follower);

    let outputs = n1.tick(1000);
    assert_eq!(n1.role(), RaftRole::Candidate);
    assert_eq!(n1.current_term(), 1);

    let mut vote_requests = Vec::new();
    for output in outputs {
        if let RaftOutput::SendRequestVote { to, request } = output {
            vote_requests.push((to, request));
        }
    }
    assert_eq!(vote_requests.len(), 2);

    let (_, req1) = &vote_requests[0];
    let (resp1, _) = n2.handle_request_vote(node1, *req1, 1000);
    assert!(resp1.is_granted());

    let outputs = n1.handle_request_vote_response(node2, resp1);

    let became_leader = outputs
        .iter()
        .any(|o| matches!(o, RaftOutput::BecameLeader));
    assert!(became_leader);
    assert_eq!(n1.role(), RaftRole::Leader);
    assert_eq!(n1.leader_id(), Some(node1));

    assert_eq!(n2.role(), RaftRole::Follower);
    assert_eq!(n3.role(), RaftRole::Follower);
}

#[tokio::test]
async fn raft_step_down_on_higher_term() {
    use mqdb::cluster::raft::{AppendEntriesRequest, RaftNode, RaftOutput, RaftRole};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let config = test_raft_config();
    let mut n1 = RaftNode::create(node1, config);
    let config = test_raft_config();
    let mut n2 = RaftNode::create(node2, config);

    n1.add_peer(node2);
    n1.add_peer(node3);
    n2.add_peer(node1);
    n2.add_peer(node3);

    n1.tick(1000);
    assert_eq!(n1.role(), RaftRole::Candidate);
    assert_eq!(n1.current_term(), 1);

    let request = AppendEntriesRequest::heartbeat(5, 2, 0, 0, 0);
    let (response, outputs) = n1.handle_append_entries(node2, request, 1000);

    assert!(response.is_success());
    assert!(
        outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::BecameFollower { .. }))
    );
    assert_eq!(n1.role(), RaftRole::Follower);
    assert_eq!(n1.current_term(), 5);
}

#[tokio::test]
async fn raft_partition_map_updates() {
    use mqdb::cluster::raft::{RaftCommand, RaftNode, RaftOutput, RaftRole};
    use mqdb::cluster::{PartitionAssignment, PartitionMap};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let config = test_raft_config();
    let mut leader = RaftNode::create(node1, config);
    let config = test_raft_config();
    let mut follower = RaftNode::create(node2, config);

    leader.add_peer(node2);
    leader.add_peer(node3);
    follower.add_peer(node1);
    follower.add_peer(node3);

    let outputs = leader.tick(1000);
    assert_eq!(leader.role(), RaftRole::Candidate);

    let mut request = None;
    for output in outputs {
        if let RaftOutput::SendRequestVote { request: req, .. } = output {
            request = Some(req);
            break;
        }
    }
    let request = request.unwrap();

    let (response, _) = follower.handle_request_vote(node1, request, 1000);
    assert!(response.is_granted());

    let outputs = leader.handle_request_vote_response(node2, response);
    assert!(
        outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::BecameLeader))
    );
    assert_eq!(leader.role(), RaftRole::Leader);

    let partition = PartitionId::new(5).unwrap();
    let cmd = RaftCommand::update_partition(partition, node2, &[node3], Epoch::new(1));
    let (idx, _) = leader.propose(cmd);
    assert_eq!(idx, Some(2));

    let mut partition_map = PartitionMap::new();
    partition_map.set(
        partition,
        PartitionAssignment::new(node2, vec![node3], Epoch::new(1)),
    );

    assert_eq!(partition_map.primary(partition), Some(node2));
    assert_eq!(partition_map.replicas(partition), &[node3]);

    let new_assignment = PartitionAssignment::new(node3, vec![], Epoch::new(2));
    partition_map.set(partition, new_assignment);

    assert_eq!(partition_map.primary(partition), Some(node3));
    assert!(partition_map.replicas(partition).is_empty());
}

#[tokio::test]
async fn session_store_basic_operations() {
    use mqdb::cluster::{SessionStore, session_partition};

    let node1 = NodeId::validated(1).unwrap();
    let store = SessionStore::new(node1);

    let session = store.create_session("client1").unwrap();
    assert_eq!(session.client_id_str(), "client1");
    assert!(session.is_connected());

    let partition = session_partition("client1");
    let sessions_on_p = store.sessions_on_partition(partition);
    assert!(sessions_on_p.iter().any(|s| s.client_id_str() == "client1"));

    store.mark_disconnected("client1", 1000).unwrap();
    let session = store.get("client1").unwrap();
    assert!(!session.is_connected());

    store
        .update("client1", |s| {
            s.set_will(1, true, "lwt/topic", b"goodbye");
        })
        .unwrap();

    let pending = store.sessions_with_pending_lwt();
    assert_eq!(pending.len(), 1);

    store
        .update("client1", mqdb::cluster::SessionData::mark_lwt_published)
        .unwrap();
    let pending = store.sessions_with_pending_lwt();
    assert!(pending.is_empty());

    let removed = store.remove("client1");
    assert!(removed.is_some());
    assert!(store.get("client1").is_none());
}

#[tokio::test]
async fn topic_index_subscribe_and_routing() {
    use mqdb::cluster::{
        PublishRouter, SubscriptionCache, TopicIndex, session_partition, topic_partition,
    };

    let node1 = NodeId::validated(1).unwrap();
    let index = TopicIndex::new(node1);
    let cache = SubscriptionCache::new(node1);

    let client1_partition = session_partition("client1");
    let client2_partition = session_partition("client2");

    index
        .subscribe("sensors/temp", "client1", client1_partition, 1)
        .unwrap();
    index
        .subscribe("sensors/temp", "client2", client2_partition, 2)
        .unwrap();

    cache
        .add_subscription("client1", "sensors/temp", 1)
        .unwrap();
    cache
        .add_subscription("client2", "sensors/temp", 2)
        .unwrap();

    let router = PublishRouter::new(&index);
    let result = router.route("sensors/temp");

    assert_eq!(result.targets.len(), 2);
    assert!(!result.target_partitions.is_empty());
    assert!(result.seq.is_some());

    let topic_p = topic_partition("sensors/temp");
    let topics = index.topics_on_partition(topic_p);
    assert!(topics.contains(&"sensors/temp".to_string()));

    index.unsubscribe("sensors/temp", "client1").unwrap();
    let result = router.route("sensors/temp");
    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "client2");
}

#[tokio::test]
async fn retained_message_lifecycle() {
    use mqdb::cluster::{RetainedStore, topic_partition};

    let node1 = NodeId::validated(1).unwrap();
    let store = RetainedStore::new(node1);

    store.set("sensors/temp", 1, b"25.5", 1000);
    store.set("sensors/humidity", 1, b"65%", 1000);

    assert_eq!(store.message_count(), 2);

    let msg = store.get("sensors/temp").unwrap();
    assert_eq!(msg.payload, b"25.5");
    assert_eq!(msg.qos, 1);

    store.set("sensors/temp", 2, b"30.0", 2000);
    let msg = store.get("sensors/temp").unwrap();
    assert_eq!(msg.payload, b"30.0");
    assert_eq!(msg.timestamp_ms, 2000);

    store.set("sensors/temp", 0, b"", 3000);
    assert!(store.get("sensors/temp").is_none());
    assert_eq!(store.message_count(), 1);

    let temp_partition = topic_partition("sensors/temp");
    let humidity_partition = topic_partition("sensors/humidity");

    if temp_partition == humidity_partition {
        let topics = store.topics_on_partition(humidity_partition);
        assert!(topics.contains(&"sensors/humidity".to_string()));
    }
}

#[tokio::test]
async fn end_to_end_subscribe_publish_flow() {
    use mqdb::cluster::{
        PublishRouter, RetainedStore, SubscriptionCache, TopicIndex, effective_qos,
        session_partition,
    };

    let node1 = NodeId::validated(1).unwrap();
    let index = TopicIndex::new(node1);
    let cache = SubscriptionCache::new(node1);
    let retained = RetainedStore::new(node1);

    let client1_p = session_partition("subscriber-1");
    let client2_p = session_partition("subscriber-2");

    index
        .subscribe("home/living/temp", "subscriber-1", client1_p, 1)
        .unwrap();
    index
        .subscribe("home/living/temp", "subscriber-2", client2_p, 2)
        .unwrap();

    cache
        .add_subscription("subscriber-1", "home/living/temp", 1)
        .unwrap();
    cache
        .add_subscription("subscriber-2", "home/living/temp", 2)
        .unwrap();

    let router = PublishRouter::new(&index);

    retained.set("home/living/temp", 1, b"22.5", 1000);

    let publish_result = router.route("home/living/temp");
    assert_eq!(publish_result.targets.len(), 2);
    assert!(publish_result.seq.is_some());

    for target in &publish_result.targets {
        let delivery_qos = effective_qos(1, target.qos);
        assert_eq!(delivery_qos, 1);
    }

    if let Some(retained_msg) = retained.get("home/living/temp") {
        assert_eq!(retained_msg.payload, b"22.5");
    }
}

#[tokio::test]
async fn wildcard_store_routing_integration() {
    use mqdb::cluster::{PublishRouter, TopicIndex, WildcardStore, session_partition};

    let node1 = NodeId::validated(1).unwrap();
    let index = TopicIndex::new(node1);
    let wildcards = WildcardStore::new(node1);

    let exact_partition = session_partition("exact-client");
    index
        .subscribe("sensors/building1/temp", "exact-client", exact_partition, 1)
        .unwrap();

    wildcards
        .subscribe_mqtt("sensors/+/temp", "wildcard-client", 2)
        .unwrap();
    wildcards
        .subscribe_mqtt("sensors/#", "multi-client", 1)
        .unwrap();

    let router = PublishRouter::new(&index);

    let wildcard_matches = wildcards.match_topic("sensors/building1/temp");
    let result = router.route_with_wildcards("sensors/building1/temp", &wildcard_matches);

    assert_eq!(result.targets.len(), 3);

    let client_ids: Vec<&str> = result
        .targets
        .iter()
        .map(|t| t.client_id.as_str())
        .collect();
    assert!(client_ids.contains(&"exact-client"));
    assert!(client_ids.contains(&"wildcard-client"));
    assert!(client_ids.contains(&"multi-client"));

    let wildcard_matches = wildcards.match_topic("sensors/building2/humidity");
    let result = router.route_with_wildcards("sensors/building2/humidity", &wildcard_matches);

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "multi-client");

    wildcards
        .unsubscribe("sensors/+/temp", "wildcard-client")
        .unwrap();
    let wildcard_matches = wildcards.match_topic("sensors/building1/temp");
    let result = router.route_with_wildcards("sensors/building1/temp", &wildcard_matches);

    let client_ids: Vec<&str> = result
        .targets
        .iter()
        .map(|t| t.client_id.as_str())
        .collect();
    assert!(!client_ids.contains(&"wildcard-client"));
    assert!(client_ids.contains(&"exact-client"));
    assert!(client_ids.contains(&"multi-client"));
}

#[tokio::test]
async fn wildcard_deduplication_max_qos() {
    use mqdb::cluster::{PublishRouter, TopicIndex, WildcardStore, session_partition};

    let node1 = NodeId::validated(1).unwrap();
    let index = TopicIndex::new(node1);
    let wildcards = WildcardStore::new(node1);

    let partition = session_partition("same-client");
    index
        .subscribe("home/living/temp", "same-client", partition, 0)
        .unwrap();

    wildcards
        .subscribe_mqtt("home/+/temp", "same-client", 1)
        .unwrap();
    wildcards
        .subscribe_mqtt("home/#", "same-client", 2)
        .unwrap();

    let router = PublishRouter::new(&index);
    let wildcard_matches = wildcards.match_topic("home/living/temp");
    let result = router.route_with_wildcards("home/living/temp", &wildcard_matches);

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].client_id, "same-client");
    assert_eq!(result.targets[0].qos, 2);
}

#[tokio::test]
async fn wildcard_sys_topic_protection() {
    use mqdb::cluster::WildcardStore;

    let node1 = NodeId::validated(1).unwrap();
    let wildcards = WildcardStore::new(node1);

    wildcards.subscribe_mqtt("#", "catch-all", 1).unwrap();

    let matches = wildcards.match_topic("$SYS/broker/stats");
    assert!(matches.is_empty());

    let matches = wildcards.match_topic("normal/topic");
    assert_eq!(matches.len(), 1);
}

#[tokio::test]
async fn lwt_idempotency_lifecycle() {
    use mqdb::cluster::{LwtAction, LwtPublisher, SessionStore, determine_lwt_action};

    let node1 = NodeId::validated(1).unwrap();
    let store = SessionStore::new(node1);

    store.create_session("client-lwt").unwrap();
    store
        .update("client-lwt", |s| {
            s.set_will(1, false, "lwt/goodbye", b"disconnected");
        })
        .unwrap();

    let session = store.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::Publish);

    let publisher = LwtPublisher::new(&store);
    let prepared = publisher.prepare_lwt("client-lwt").unwrap().unwrap();
    assert_eq!(prepared.topic, "lwt/goodbye");
    assert_eq!(prepared.payload, b"disconnected");

    let session = store.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::Skip);

    let second = publisher.prepare_lwt("client-lwt").unwrap();
    assert!(second.is_none());

    publisher
        .complete_lwt("client-lwt", prepared.token)
        .unwrap();

    let session = store.get("client-lwt").unwrap();
    assert_eq!(determine_lwt_action(&session), LwtAction::AlreadyPublished);
}

#[tokio::test]
async fn qos2_state_lifecycle() {
    use mqdb::cluster::{Qos2Phase, Qos2Store};

    let node1 = NodeId::validated(1).unwrap();
    let store = Qos2Store::new(node1);

    store
        .start_inbound("client1", 100, "sensor/temp", b"25.5", 1000)
        .unwrap();

    let state = store.get("client1", 100).unwrap();
    assert_eq!(state.phase(), Some(Qos2Phase::PubrecSent));

    store.advance("client1", 100).unwrap();

    let state = store.get("client1", 100).unwrap();
    assert_eq!(state.phase(), Some(Qos2Phase::PubrelReceived));

    store.complete("client1", 100).unwrap();

    let state = store.get("client1", 100);
    assert!(state.is_none());
}

#[tokio::test]
async fn inflight_retry_tracking() {
    use mqdb::cluster::InflightStore;

    let node1 = NodeId::validated(1).unwrap();
    let store = InflightStore::new(node1);

    store
        .add("client1", 1, "sensor/temp", b"25.5", 1, 1000)
        .unwrap();

    let msg = store.get("client1", 1).unwrap();
    assert_eq!(msg.attempts, 1);

    let attempts = store.mark_retry("client1", 1, 2000).unwrap();
    assert_eq!(attempts, 2);

    let msg = store.get("client1", 1).unwrap();
    assert_eq!(msg.attempts, 2);
    assert_eq!(msg.last_attempt, 2000);

    let acked = store.acknowledge("client1", 1).unwrap();
    assert_eq!(acked.topic_str(), "sensor/temp");

    assert!(store.get("client1", 1).is_none());
}

#[tokio::test]
async fn offset_store_tracking() {
    use mqdb::cluster::{OffsetStore, PartitionId};

    let node1 = NodeId::validated(1).unwrap();
    let store = OffsetStore::new(node1);
    let partition = PartitionId::new(5).unwrap();

    store.commit("consumer1", partition, 100, 1000);

    let offset = store.get("consumer1", partition).unwrap();
    assert_eq!(offset.sequence, 100);

    store.commit("consumer1", partition, 150, 2000);

    let offset = store.get("consumer1", partition).unwrap();
    assert_eq!(offset.sequence, 150);
}

#[tokio::test]
async fn rebalancer_node_failure() {
    use mqdb::cluster::{
        Epoch, PartitionId, RebalanceConfig, compute_balanced_assignments,
        compute_removal_assignments,
    };

    let nodes = vec![
        NodeId::validated(1).unwrap(),
        NodeId::validated(2).unwrap(),
        NodeId::validated(3).unwrap(),
    ];
    let config = RebalanceConfig::default();
    let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

    let partition0_primary = map.primary(PartitionId::new(0).unwrap());
    assert!(partition0_primary.is_some());

    let remaining = vec![NodeId::validated(2).unwrap(), NodeId::validated(3).unwrap()];
    let reassignments =
        compute_removal_assignments(&map, &remaining, NodeId::validated(1).unwrap(), &config);

    for r in &reassignments {
        assert!(r.new_primary == remaining[0] || r.new_primary == remaining[1]);
    }
}

#[tokio::test]
async fn rebalancer_assigns_partitions_to_node_with_zero_primaries() {
    use mqdb::cluster::{
        Epoch, PartitionAssignment, PartitionId, PartitionMap, RebalanceConfig,
        compute_incremental_assignments,
    };

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let mut map = PartitionMap::new();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        let primary = if i % 2 == 0 { node1 } else { node2 };
        let replica = if i % 2 == 0 { node2 } else { node1 };
        map.set(
            partition,
            PartitionAssignment::new(primary, vec![replica], Epoch::new(1)),
        );
    }

    assert_eq!(map.primary_count(node1), NUM_PARTITIONS as usize / 2);
    assert_eq!(map.primary_count(node2), NUM_PARTITIONS as usize / 2);
    assert_eq!(map.primary_count(node3), 0);

    let all_nodes = vec![node1, node2, node3];
    let config = RebalanceConfig::default();
    let reassignments = compute_incremental_assignments(&map, &all_nodes, &config);

    assert!(
        !reassignments.is_empty(),
        "rebalancer must propose changes when node has zero partitions"
    );

    let expected_per_node = NUM_PARTITIONS as usize / 3;
    let node3_primaries = reassignments
        .iter()
        .filter(|r| r.new_primary == node3)
        .count();
    assert!(
        node3_primaries >= expected_per_node,
        "node3 should receive at least {expected_per_node} primaries (got {node3_primaries})"
    );
}

#[tokio::test]
async fn rebalancer_distributes_partitions_fairly() {
    use mqdb::cluster::{Epoch, PartitionId, RebalanceConfig, compute_balanced_assignments};

    let nodes = vec![
        NodeId::validated(1).unwrap(),
        NodeId::validated(2).unwrap(),
        NodeId::validated(3).unwrap(),
    ];
    let config = RebalanceConfig::default();
    let map = compute_balanced_assignments(&nodes, &config, Epoch::ZERO);

    let n1_count = map.primary_count(nodes[0]);
    let n2_count = map.primary_count(nodes[1]);
    let n3_count = map.primary_count(nodes[2]);

    let per_node_min = NUM_PARTITIONS as usize / 3;
    let per_node_max = per_node_min + 1;
    assert!(
        (per_node_min..=per_node_max).contains(&n1_count),
        "node1 should have ~{per_node_min}-{per_node_max} primaries, got {n1_count}"
    );
    assert!(
        (per_node_min..=per_node_max).contains(&n2_count),
        "node2 should have ~{per_node_min}-{per_node_max} primaries, got {n2_count}"
    );
    assert!(
        (per_node_min..=per_node_max).contains(&n3_count),
        "node3 should have ~{per_node_min}-{per_node_max} primaries, got {n3_count}"
    );
    assert_eq!(
        n1_count + n2_count + n3_count,
        NUM_PARTITIONS as usize,
        "total should be {NUM_PARTITIONS} partitions"
    );

    for partition in PartitionId::all() {
        assert!(
            map.primary(partition).is_some(),
            "partition {partition:?} must have a primary"
        );
    }
}
