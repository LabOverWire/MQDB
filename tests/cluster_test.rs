mod simulation;

use mqdb::cluster::{
    Epoch, NodeController, NodeId, Operation, PartitionAssignment, PartitionId, PartitionMap,
    PartitionRole, ReplicationWrite, TransportConfig,
};

#[test]
fn partition_assignment_determinism() {
    let p1 = PartitionId::from_entity_id("users", "123");
    let p2 = PartitionId::from_entity_id("users", "123");
    assert_eq!(p1, p2);

    let p3 = PartitionId::from_entity_id("users", "456");
    assert_eq!(PartitionId::from_entity_id("users", "456"), p3);
}

#[test]
fn different_keys_may_hash_differently() {
    let p1 = PartitionId::from_entity_id("users", "123");
    let p2 = PartitionId::from_entity_id("orders", "789");
    let _ = (p1, p2);
}

#[test]
fn partition_id_always_in_valid_range() {
    for i in 0..1000 {
        let entity = format!("entity_{i}");
        let id = format!("id_{}", i * 7);
        let partition = PartitionId::from_entity_id(&entity, &id);
        assert!(partition.get() < 64, "partition {} >= 64", partition.get());
    }
}

#[test]
fn epoch_ordering() {
    let e1 = Epoch::new(1);
    let e2 = Epoch::new(2);
    assert!(e1 < e2);
    assert_eq!(e1.next(), e2);
}

#[test]
fn node_id_validation() {
    let node = NodeId::validated(1).unwrap();
    assert_eq!(node.get(), 1);
    assert!(node.is_valid());
    assert!(NodeId::validated(0).is_none());
}

#[test]
fn partition_map_assignment() {
    let mut map = PartitionMap::new();

    let p0 = PartitionId::new(0).unwrap();
    let p1 = PartitionId::new(1).unwrap();
    let n1 = NodeId::validated(1).unwrap();
    let n2 = NodeId::validated(2).unwrap();

    map.set(p0, PartitionAssignment::new(n1, vec![n2], Epoch::new(1)));
    map.set(p1, PartitionAssignment::new(n2, vec![n1], Epoch::new(1)));

    assert_eq!(map.primary(p0).map(|n| n.get()), Some(1));
    assert_eq!(map.primary(p1).map(|n| n.get()), Some(2));
    assert_eq!(map.role_for(p0, n1), PartitionRole::Primary);
    assert_eq!(map.role_for(p0, n2), PartitionRole::Replica);
}

#[test]
fn simulation_framework_three_nodes() {
    use simulation::framework::SimulatedRuntime;

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

#[test]
fn simulation_network_partition() {
    use simulation::framework::SimulatedRuntime;

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

#[test]
fn simulation_scheduled_tasks() {
    use simulation::framework::SimulatedRuntime;
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

#[test]
fn two_node_replication() {
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
    let mut ctrl1 = NodeController::new(node1_id, t1, config);
    let mut ctrl2 = NodeController::new(node2_id, t2, config);

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

    let seq = ctrl1.replicate_write(write, &[node2_id], 1).unwrap();
    assert_eq!(seq, 1);

    clock.advance_ms(5);

    ctrl2.process_messages();
    assert_eq!(ctrl2.sequence(partition), Some(1));

    clock.advance_ms(5);

    ctrl1.process_messages();
}

#[test]
fn heartbeat_detection() {
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

    let mut ctrl1 = NodeController::new(node1_id, t1, config);
    let mut ctrl2 = NodeController::new(node2_id, t2, config);

    ctrl1.register_peer(node2_id);
    ctrl2.register_peer(node1_id);

    ctrl1.tick(0);
    clock.advance_ms(5);
    ctrl2.process_messages();

    assert_eq!(ctrl2.node_status(node1_id), NodeStatus::Alive);

    ctrl2.tick(600);
    assert_eq!(ctrl2.node_status(node1_id), NodeStatus::Dead);
}

#[test]
fn raft_leader_election_three_nodes() {
    use mqdb::cluster::raft::{RaftConfig, RaftNode, RaftOutput, RaftRole};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let config = RaftConfig::default();
    let mut n1 = RaftNode::create(node1, config);
    let config = RaftConfig::default();
    let mut n2 = RaftNode::create(node2, config);
    let config = RaftConfig::default();
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
    let (resp1, _) = n2.handle_request_vote(node1, req1.clone(), 1000);
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

#[test]
fn raft_step_down_on_higher_term() {
    use mqdb::cluster::raft::{AppendEntriesRequest, RaftConfig, RaftNode, RaftOutput, RaftRole};

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();
    let node3 = NodeId::validated(3).unwrap();

    let config = RaftConfig::default();
    let mut n1 = RaftNode::create(node1, config);
    let config = RaftConfig::default();
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
