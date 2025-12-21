mod simulation;

use mqdb::cluster::{Epoch, NodeId, PartitionAssignment, PartitionId, PartitionMap, PartitionRole};

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
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

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
