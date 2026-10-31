// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::raft::PartitionUpdate;
use crate::cluster::{PartitionAssignment, PartitionMap};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
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

#[tokio::test]
async fn a_partition_update_copy_from_another_node_changes_nothing() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    ctrl.transport.inject_message(
        node(3),
        ClusterMessage::PartitionUpdate(PartitionUpdate {
            partition: 201,
            primary: 2,
            replica1: 1,
            replica2: 0,
            epoch: 1,
        }),
    );
    ctrl.process_messages().await;

    assert_eq!(ctrl.partition_map().primary(partition), Some(node(1)));
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_primary_left_out_of_the_raft_map_steps_down() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(1);
    ctrl.become_primary(partition, Epoch::new(1));
    assert!(
        ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
            .await
    );
    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);

    let became_primary = ctrl
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 2))
        .await;

    assert!(!became_primary);
    assert_eq!(ctrl.partition_map().primary(partition), Some(node(2)));
    assert_eq!(ctrl.role(partition), ReplicaRole::None);
}

#[tokio::test]
async fn a_replica_serves_reads_only_after_its_snapshot_arrives() {
    let partition = PartitionId::new(201).unwrap();
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));

    ctrl.become_replica(partition, Epoch::new(1), 0);

    assert!(ctrl.can_serve_reads(partition));
}

fn snapshot_requests_to(ctrl: &NodeController<MockTransport>, to: NodeId) -> usize {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter(|(target, message)| {
            *target == to && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count()
}

async fn serve_snapshot(
    source: &mut NodeController<MockTransport>,
    target: &mut NodeController<MockTransport>,
    partition: PartitionId,
) {
    source.transport.inject_message(
        target.node_id(),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, target.node_id())),
    );
    source.process_messages().await;
    for (to, message) in source.transport.sent_messages() {
        if to == target.node_id() && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            target.transport.inject_message(source.node_id(), message);
        }
    }
    target.process_messages().await;
}

fn controller_on(
    id: u16,
    backend: &Arc<crate::cluster::test_support::FaultyBackend>,
) -> NodeController<MockTransport> {
    let (tx_raft_messages, _rx_raft_messages) = flume::unbounded();
    let (tx_raft_events, _rx_raft_events) = flume::unbounded();
    let storage: Arc<dyn mqdb_core::storage::StorageBackend> = backend.clone();
    NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(storage),
        tx_raft_messages,
        tx_raft_events,
    )
}

#[tokio::test]
async fn a_node_without_the_data_waits_for_the_replica_before_taking_the_primary_role() {
    let partition = session_partition("c1");
    let mut replica = controller(3);
    replica.become_replica(partition, Epoch::new(1), 4);
    replica.stores().create_session_replicated("c1").unwrap();

    let mut wiped = controller(2);
    wiped
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    assert_eq!(wiped.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!wiped.can_serve_reads(partition));
    assert_eq!(snapshot_requests_to(&wiped, node(3)), 1);

    serve_snapshot(&mut replica, &mut wiped, partition).await;

    assert_eq!(wiped.role(partition), ReplicaRole::Primary);
    assert!(wiped.can_serve_reads(partition));
    assert!(wiped.stores().sessions.get("c1").is_some());
    assert_eq!(wiped.sequence(partition), Some(4));
}

#[tokio::test]
async fn a_restarted_primary_trusts_its_disk_only_for_the_assignment_it_held() {
    let partition = session_partition("c1");
    let backend = crate::cluster::test_support::FaultyBackend::shared();
    let mut before_crash = controller_on(2, &backend);
    before_crash
        .apply_raft_partition_map(map_with(partition, node(2), &[], 1))
        .await;
    assert_eq!(before_crash.role(partition), ReplicaRole::Primary);
    drop(before_crash);

    let mut same_assignment = controller_on(2, &backend);
    same_assignment
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    assert_eq!(same_assignment.role(partition), ReplicaRole::Primary);

    let mut newer_assignment = controller_on(2, &backend);
    newer_assignment
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 3))
        .await;
    assert_eq!(
        newer_assignment.role(partition),
        ReplicaRole::AwaitingSnapshot
    );
    assert_eq!(snapshot_requests_to(&newer_assignment, node(3)), 1);
}

#[tokio::test]
async fn a_replica_promoted_before_its_snapshot_lands_waits_for_it() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);

    let became_primary = ctrl
        .apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 2))
        .await;

    assert!(!became_primary);
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(!ctrl.can_serve_reads(partition));
}

#[tokio::test]
async fn a_waiting_node_asks_the_new_source_when_the_assignment_changes() {
    let partition = session_partition("c1");
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 2))
        .await;

    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(snapshot_requests_to(&ctrl, node(4)), 1);
}

#[tokio::test]
async fn a_stalled_snapshot_request_is_sent_again() {
    let partition = session_partition("c1");
    let map = map_with(partition, node(2), &[node(3)], 1);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    ctrl.apply_raft_partition_map(map.clone()).await;
    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 1);

    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    for request in ctrl.requested_snapshots.values_mut() {
        request.last_activity = stalled_since;
    }
    ctrl.apply_raft_partition_map(map).await;

    assert_eq!(snapshot_requests_to(&ctrl, node(3)), 2);
}

#[tokio::test]
async fn a_snapshot_for_a_partition_no_longer_assigned_leaves_no_role() {
    let partition = session_partition("c1");
    let mut source = controller(1);
    source.become_primary(partition, Epoch::new(1));
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 2))
        .await;

    serve_snapshot(&mut source, &mut ctrl, partition).await;

    assert_eq!(ctrl.role(partition), ReplicaRole::None);
    assert!(!ctrl.can_serve_reads(partition));
}

#[tokio::test]
async fn a_forwarded_read_for_a_partition_not_held_is_refused() {
    let entity = "bigdocs";
    let id = "r2";
    let partition = crate::cluster::db::data_partition(entity, id);
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::JsonDbRequest {
            partition,
            request: JsonDbRequest {
                request_id: 7,
                op: JsonDbOp::Read,
                entity: entity.to_string(),
                id: Some(id.to_string()),
                payload: b"{}".to_vec(),
                response_topic: "resp/r2".to_string(),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        },
    );
    ctrl.process_messages().await;

    let payload = ctrl
        .transport
        .sent_messages()
        .into_iter()
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(1) => Some(response.payload),
            _ => None,
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(body["code"], 503);
}

#[tokio::test]
async fn a_snapshot_from_a_node_it_was_not_requested_from_is_ignored() {
    let partition = session_partition("c1");
    let mut stranger = controller(4);
    stranger.stores().create_session_replicated("c1").unwrap();
    let mut wiped = controller(2);
    wiped
        .apply_raft_partition_map(map_with(partition, node(2), &[node(3)], 1))
        .await;

    serve_snapshot(&mut stranger, &mut wiped, partition).await;

    assert_eq!(wiped.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(wiped.stores().sessions.get("c1").is_none());
}
