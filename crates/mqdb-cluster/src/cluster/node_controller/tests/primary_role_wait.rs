// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::data_partition;
use crate::cluster::{PartitionAssignment, PartitionMap};

const ENTITY: &str = "item";
const ID: &str = "role-wait";
const THIS: u16 = 1;
const OLD_PRIMARY: u16 = 2;
const REPLACED: u16 = 3;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn map_with(primary: u16, replicas: &[u16], epoch: u64) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        data_partition(ENTITY, ID),
        PartitionAssignment::new(
            node(primary),
            replicas.iter().map(|id| node(*id)).collect(),
            Epoch::new(epoch),
        ),
    );
    map
}

fn heard(ctrl: &mut NodeController<MockTransport>, from: u16) {
    ctrl.heartbeat.receive_heartbeat(
        node(from),
        &crate::cluster::protocol::Heartbeat::create(node(from), 0),
        0,
    );
}

fn requests_to(ctrl: &NodeController<MockTransport>, to: u16) -> usize {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter(|(target, message)| {
            *target == node(to) && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count()
}

fn on_backend(
    id: u16,
    backend: &Arc<crate::cluster::test_support::FaultyBackend>,
) -> NodeController<MockTransport> {
    let storage: Arc<dyn mqdb_core::storage::StorageBackend> = backend.clone();
    NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(storage),
        flume::unbounded().0,
        flume::unbounded().0,
    )
}

async fn primary_built_from_a_snapshot(backend: &Arc<crate::cluster::test_support::FaultyBackend>) {
    let partition = data_partition(ENTITY, ID);
    let mut holder = controller(REPLACED);
    holder.become_primary(partition, Epoch::new(1));
    holder
        .apply_raft_partition_map(map_with(REPLACED, &[OLD_PRIMARY], 1))
        .await;
    holder
        .db_create(ENTITY, ID, br#"{"id":"role-wait"}"#, 1)
        .await
        .unwrap();
    holder
        .apply_raft_partition_map(map_with(THIS, &[REPLACED], 2))
        .await;
    holder.transport.take_sent();
    let mut first = on_backend(THIS, backend);
    first
        .apply_raft_partition_map(map_with(THIS, &[REPLACED], 2))
        .await;
    holder.transport.inject_message(
        node(THIS),
        ClusterMessage::SnapshotRequest(crate::cluster::snapshot::SnapshotRequest::create(
            partition,
            node(THIS),
        )),
    );
    holder.process_messages().await;
    for (to, message) in holder.transport.take_sent() {
        if to == node(THIS) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            first.transport.inject_message(node(REPLACED), message);
        }
    }
    first.process_messages().await;
    for (to, message) in first.transport.take_sent() {
        if to == node(REPLACED) && matches!(message, ClusterMessage::CatchupRequest(_)) {
            holder.transport.inject_message(node(THIS), message);
        }
    }
    holder.process_messages().await;
    for (to, message) in holder.transport.take_sent() {
        if to == node(THIS) && matches!(message, ClusterMessage::CatchupResponse(_)) {
            first.transport.inject_message(node(REPLACED), message);
        }
    }
    first.process_messages().await;
    assert_eq!(first.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_dead_named_source_keeps_the_wait_until_the_map_changes() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(THIS);
    heard(&mut ctrl, REPLACED);
    ctrl.apply_raft_partition_map(map_with(THIS, &[REPLACED], 1))
        .await;
    ctrl.apply_dead_nodes(&[node(REPLACED)]);
    for now in (1_000u64..400_000).step_by(6_000) {
        ctrl.drive_snapshots(now).await;
    }
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));

    ctrl.apply_raft_partition_map(map_with(THIS, &[], 2)).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.can_serve_reads(partition));
}

#[tokio::test]
async fn a_primary_built_from_a_snapshot_waits_for_a_replica_never_heard_from() {
    let partition = data_partition(ENTITY, ID);
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    primary_built_from_a_snapshot(&backend).await;

    let mut restarted = on_backend(THIS, &backend);
    restarted.stores().recover().unwrap();
    restarted
        .apply_raft_partition_map(map_with(THIS, &[REPLACED], 2))
        .await;
    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!restarted.can_serve_reads(partition));

    restarted.drive_snapshots(1).await;
    restarted.drive_snapshots(60_000).await;
    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);

    restarted.drive_snapshots(100_000).await;
    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_primary_built_from_a_snapshot_waits_for_the_map_when_its_replica_is_dead() {
    let partition = data_partition(ENTITY, ID);
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    primary_built_from_a_snapshot(&backend).await;

    let mut restarted = on_backend(THIS, &backend);
    restarted.stores().recover().unwrap();
    heard(&mut restarted, REPLACED);
    restarted
        .apply_raft_partition_map(map_with(THIS, &[REPLACED], 2))
        .await;
    restarted.apply_dead_nodes(&[node(REPLACED)]);
    for now in (1_000u64..200_000).step_by(6_000) {
        restarted.drive_snapshots(now).await;
    }
    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);

    restarted
        .apply_raft_partition_map(map_with(THIS, &[], 3))
        .await;
    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_node_whose_first_map_is_the_move_takes_the_role_with_the_replaced_replica_alive() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(THIS);
    heard(&mut ctrl, OLD_PRIMARY);
    heard(&mut ctrl, REPLACED);
    ctrl.apply_raft_partition_map(map_with(OLD_PRIMARY, &[THIS], 2))
        .await;
    ctrl.apply_dead_nodes(&[node(OLD_PRIMARY)]);

    ctrl.apply_raft_partition_map(map_with(THIS, &[], 3)).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
    assert!(ctrl.can_serve_reads(partition));
    assert_eq!(requests_to(&ctrl, REPLACED), 0);
}

#[tokio::test]
async fn a_node_that_saw_the_move_asks_the_replaced_replica_before_taking_the_role() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(THIS);
    heard(&mut ctrl, OLD_PRIMARY);
    heard(&mut ctrl, REPLACED);
    ctrl.apply_raft_partition_map(map_with(OLD_PRIMARY, &[REPLACED], 1))
        .await;
    ctrl.apply_raft_partition_map(map_with(OLD_PRIMARY, &[THIS], 2))
        .await;
    ctrl.apply_dead_nodes(&[node(OLD_PRIMARY)]);

    ctrl.apply_raft_partition_map(map_with(THIS, &[], 3)).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));
    assert_eq!(requests_to(&ctrl, REPLACED), 1);
}

#[tokio::test]
async fn a_held_verdict_does_not_land_after_the_node_is_back() {
    use crate::cluster::message_processor::MessageProcessor;
    let peer = node(OLD_PRIMARY);
    let mut ctrl = controller(THIS);
    let (tx_events, rx_events) = flume::bounded(4);
    ctrl.tx_raft_events = tx_events.clone();
    ctrl.heartbeat.register_node(peer);
    let (tx_inbox, rx_inbox) = flume::unbounded();
    let (tx_tick, rx_tick) = flume::unbounded();
    let (tx_batch, rx_batch) = flume::unbounded();
    let mut processor = MessageProcessor::new(
        node(THIS),
        TransportConfig {
            heartbeat_interval_ms: 100,
            heartbeat_timeout_ms: 500,
            ack_timeout_ms: 50,
        },
        flume::unbounded().0,
        tx_events.clone(),
        flume::unbounded().0,
        rx_inbox,
        rx_tick,
    );
    processor.register_peer(peer);
    tokio::spawn(processor.run(tx_batch));
    let heartbeat_at = |at: u64| InboundMessage {
        from: peer,
        message: ClusterMessage::Heartbeat(crate::cluster::protocol::Heartbeat::create(peer, at)),
        received_at: at,
    };

    tx_inbox.send(heartbeat_at(1_000)).unwrap();
    rx_events.recv_async().await.unwrap();
    tx_tick.send(1_100).unwrap();
    let batch = rx_batch.recv_async().await.unwrap();
    ctrl.apply_heartbeat_updates(&batch.heartbeat_updates);

    while tx_events.try_send(RaftEvent::NodeAlive(node(9))).is_ok() {}
    tx_tick.send(2_000).unwrap();
    let batch = rx_batch.recv_async().await.unwrap();
    assert_eq!(batch.dead_nodes, [peer]);
    ctrl.apply_dead_nodes(&batch.dead_nodes);
    drop(rx_events.drain());

    tx_inbox.send(heartbeat_at(2_050)).unwrap();
    let revived = rx_events.recv_async().await.unwrap();
    assert!(matches!(revived, RaftEvent::NodeAlive(node) if node == peer));

    tx_tick.send(2_100).unwrap();
    rx_batch.recv_async().await.unwrap();

    assert!(
        !rx_events
            .drain()
            .any(|event| matches!(event, RaftEvent::NodeDead(_)))
    );
}

#[tokio::test]
async fn a_verdict_refused_by_a_full_queue_is_sent_by_the_running_processor() {
    use crate::cluster::message_processor::MessageProcessor;
    let peer = node(OLD_PRIMARY);
    let (tx_events, rx_events) = flume::bounded(2);
    let (tx_inbox, rx_inbox) = flume::unbounded::<InboundMessage>();
    let (tx_tick, rx_tick) = flume::unbounded();
    let (tx_batch, rx_batch) = flume::unbounded();
    let mut processor = MessageProcessor::new(
        node(THIS),
        TransportConfig {
            heartbeat_interval_ms: 100,
            heartbeat_timeout_ms: 500,
            ack_timeout_ms: 50,
        },
        flume::unbounded().0,
        tx_events.clone(),
        flume::unbounded().0,
        rx_inbox,
        rx_tick,
    );
    processor.expect_peer(peer);
    tokio::spawn(processor.run(tx_batch));

    tx_tick.send(1_000).unwrap();
    rx_batch.recv_async().await.unwrap();
    while tx_events.try_send(RaftEvent::NodeAlive(node(9))).is_ok() {}
    tx_tick.send(5_000).unwrap();
    assert_eq!(rx_batch.recv_async().await.unwrap().dead_nodes, [peer]);
    assert!(
        !rx_events
            .drain()
            .any(|event| matches!(event, RaftEvent::NodeDead(_)))
    );

    tx_tick.send(5_100).unwrap();
    rx_batch.recv_async().await.unwrap();

    assert!(
        rx_events
            .drain()
            .any(|event| matches!(event, RaftEvent::NodeDead(dead) if dead == peer))
    );
    drop(tx_inbox);
}

#[test]
fn a_verdict_on_a_node_never_registered_here_is_not_in_the_dead_list() {
    let peer = node(OLD_PRIMARY);
    let mut ctrl = controller(THIS);

    ctrl.apply_dead_nodes(&[peer]);
    assert!(ctrl.dead_nodes().is_empty());

    ctrl.heartbeat.register_node(peer);
    ctrl.apply_dead_nodes(&[peer]);
    assert_eq!(ctrl.dead_nodes(), [peer]);
}

#[test]
fn applying_a_verdict_sends_nothing_to_raft() {
    let peer = node(OLD_PRIMARY);
    let mut ctrl = controller(THIS);
    let (tx_events, rx_events) = flume::bounded(4);
    ctrl.tx_raft_events = tx_events;

    ctrl.apply_dead_nodes(&[peer]);

    assert!(rx_events.is_empty());
}
