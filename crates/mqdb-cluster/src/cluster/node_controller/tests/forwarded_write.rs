// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::protocol::CatchupResponse;
use crate::cluster::test_support::FaultyBackend;
use crate::cluster::{PartitionAssignment, PartitionMap};
use mqdb_core::storage::StorageBackend;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn controller_with_storage(id: u16, backend: &Arc<FaultyBackend>) -> NodeController<MockTransport> {
    let (tx_raft_messages, _rx_raft_messages) = flume::unbounded();
    let (tx_raft_events, _rx_raft_events) = flume::unbounded();
    let storage: Arc<dyn StorageBackend> = backend.clone();
    NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(storage),
        tx_raft_messages,
        tx_raft_events,
    )
}

fn map_with(
    partition: PartitionId,
    primary: NodeId,
    replicas: &[NodeId],
    epoch: u64,
) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        partition,
        PartitionAssignment::new(primary, replicas.to_vec(), Epoch::new(epoch)),
    );
    map
}

fn session_write(client_id: &str, epoch: u64) -> ReplicationWrite {
    let (_, mut write) = controller(9)
        .stores()
        .create_session_replicated(client_id)
        .unwrap();
    write.epoch = Epoch::new(epoch);
    write
}

fn replicated_ids(ctrl: &NodeController<MockTransport>, to: NodeId) -> Vec<String> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(target, message)| match message {
            ClusterMessage::Write(write) if target == to => Some(write.id),
            _ => None,
        })
        .collect()
}

fn forwarded(ctrl: &NodeController<MockTransport>) -> Vec<(NodeId, String, Epoch)> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(target, message)| match message {
            ClusterMessage::WriteRequest(write) => Some((target, write.id, write.epoch)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_write_for_a_newer_assignment_is_applied_once_the_receiver_catches_up() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;

    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::WriteRequest(session_write("c1", 2)),
    );
    ctrl.process_messages().await;
    assert!(ctrl.stores().sessions.get("c1").is_none());
    assert!(forwarded(&ctrl).is_empty());

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 2))
        .await;

    assert!(ctrl.stores().sessions.get("c1").is_some());
    assert_eq!(replicated_ids(&ctrl, node(3)), ["c1"]);
}

#[tokio::test]
async fn a_write_for_an_older_assignment_is_not_sent_on() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 2))
        .await;

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    assert!(forwarded(&ctrl).is_empty());
    assert!(ctrl.stores().sessions.get("c1").is_none());
}

#[tokio::test]
async fn a_write_for_a_newer_assignment_waits_even_on_the_current_primary() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 3)),
    );
    ctrl.process_messages().await;
    assert!(ctrl.stores().sessions.get("c1").is_none());

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 3))
        .await;

    assert!(ctrl.stores().sessions.get("c1").is_some());
    assert_eq!(replicated_ids(&ctrl, node(3)), ["c1"]);
}

#[tokio::test]
async fn a_write_for_the_same_assignment_is_not_sent_on() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(3), &[node(2)], 2))
        .await;

    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::WriteRequest(session_write("c1", 2)),
    );
    ctrl.process_messages().await;

    assert!(forwarded(&ctrl).is_empty());
}

#[tokio::test]
async fn a_forwarded_write_carries_the_epoch_of_the_sender_map() {
    let partition = session_partition("c1");
    let mut ctrl = controller(1);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 5))
        .await;

    ctrl.write_or_forward(session_write("c1", 0)).await;

    assert_eq!(
        forwarded(&ctrl),
        [(node(2), "c1".to_string(), Epoch::new(5))]
    );
}

#[tokio::test]
async fn a_forwarded_write_is_stored_once_by_the_primary() {
    let partition = session_partition("c1");
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_storage(2, &backend);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let before = backend.inserts();

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    assert_eq!(backend.inserts() - before, 1);
    assert!(ctrl.stores().sessions.get("c1").is_some());
    assert_eq!(replicated_ids(&ctrl, node(3)), ["c1"]);
}

#[tokio::test]
async fn a_forwarded_write_is_not_stored_by_a_primary_outside_the_primary_role() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let mut far_ahead = session_write("c1", 1);
    far_ahead.sequence = 5000;
    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::CatchupResponse(CatchupResponse::create(
            partition,
            node(3),
            vec![far_ahead],
        )),
    );
    ctrl.process_messages().await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(ctrl.is_primary_for_partition(partition));

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    assert!(ctrl.stores().sessions.get("c1").is_none());
    assert!(replicated_ids(&ctrl, node(3)).is_empty());
}

#[tokio::test]
async fn a_forwarded_write_the_primary_cannot_store_is_not_replicated() {
    let partition = session_partition("c1");
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_storage(2, &backend);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    backend.fail_writes(true);

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::WriteRequest(session_write("c1", 1)),
    );
    ctrl.process_messages().await;

    assert!(replicated_ids(&ctrl, node(3)).is_empty());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert_eq!(ctrl.sequence(partition), Some(0));
}

#[tokio::test]
async fn a_write_the_primary_cannot_store_is_not_replicated() {
    let partition = session_partition("c1");
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_storage(2, &backend);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    backend.fail_writes(true);

    let result = ctrl
        .replicate_write_async(session_write("c1", 1), &[node(3)], None)
        .await;

    assert_eq!(result, Err(ReplicationError::StoreFailed));
    assert!(replicated_ids(&ctrl, node(3)).is_empty());
    assert_eq!(ctrl.write_log().entry_count(partition), 0);
    assert_eq!(ctrl.sequence(partition), Some(0));

    backend.fail_writes(false);
    let result = ctrl
        .replicate_write_async(session_write("c1", 1), &[node(3)], None)
        .await;

    assert_eq!(result, Ok(1));
    assert_eq!(replicated_ids(&ctrl, node(3)), ["c1"]);
}

#[tokio::test]
async fn a_persisted_write_the_primary_cannot_apply_in_memory_is_still_replicated() {
    let partition = session_partition("c1");
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_storage(2, &backend);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    let mut unreadable = session_write("c1", 1);
    unreadable.data = vec![0xff];

    let result = ctrl
        .replicate_write_async(unreadable, &[node(3)], None)
        .await;

    assert_eq!(result, Ok(1));
    assert_eq!(backend.keys_containing("c1").unwrap(), 1);
    assert_eq!(replicated_ids(&ctrl, node(3)), ["c1"]);
    assert_eq!(ctrl.write_log().entry_count(partition), 1);
}
