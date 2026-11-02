// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::data_partition;
use crate::cluster::{PartitionAssignment, PartitionMap};
use std::sync::atomic::Ordering;

const ENTITY: &str = "item";
const ID: &str = "n5-19";

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

async fn primary_holding_the_record(partition: PartitionId) -> NodeController<MockTransport> {
    let mut primary = controller(4);
    primary.become_primary(partition, Epoch::new(1));
    primary
        .apply_raft_partition_map(map_with(partition, node(4), &[node(3)], 2))
        .await;
    primary
        .db_create(ENTITY, ID, br#"{"n":2}"#, 1000)
        .await
        .unwrap();
    primary
        .apply_raft_partition_map(map_with(partition, node(4), &[node(1)], 3))
        .await;
    primary
}

async fn restarted_node_without_a_link_to_the_primary(
    partition: PartitionId,
) -> NodeController<MockTransport> {
    let mut restarted = controller(1);
    restarted
        .transport
        .refuse_sends
        .store(true, Ordering::SeqCst);
    restarted
        .apply_raft_partition_map(map_with(partition, node(4), &[node(1)], 3))
        .await;
    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    restarted
        .transport
        .refuse_sends
        .store(false, Ordering::SeqCst);
    restarted
}

fn snapshot_requests(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::SnapshotRequest(_)))
        .count()
}

#[tokio::test]
async fn a_replica_promoted_after_its_snapshot_request_was_lost_serves_without_the_record() {
    let partition = data_partition(ENTITY, ID);
    assert_eq!(partition.get(), 186);
    let mut primary = primary_holding_the_record(partition).await;
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;
    assert_eq!(snapshot_requests(&restarted), 0);

    let promoted = map_with(partition, node(1), &[node(4)], 4);
    primary.apply_raft_partition_map(promoted.clone()).await;
    restarted.apply_raft_partition_map(promoted).await;

    assert!(primary.stores().db_get(ENTITY, ID).is_some());
    assert_eq!(primary.role(partition), ReplicaRole::Replica);
    let serves_without_the_record =
        restarted.can_serve_reads(partition) && restarted.stores().db_get(ENTITY, ID).is_none();
    assert!(
        !serves_without_the_record,
        "node 1 role {:?} serves partition {} as primary without {ID}, which node 4 still holds",
        restarted.role(partition),
        partition.get(),
    );
}

#[tokio::test]
async fn control_the_record_survives_promotion_when_the_snapshot_request_is_delivered() {
    let partition = data_partition(ENTITY, ID);
    let mut primary = primary_holding_the_record(partition).await;
    let mut restarted = controller(1);
    restarted
        .apply_raft_partition_map(map_with(partition, node(4), &[node(1)], 3))
        .await;

    for (_, message) in restarted.transport.sent_messages() {
        if matches!(message, ClusterMessage::SnapshotRequest(_)) {
            primary.transport.inject_message(node(1), message);
        }
    }
    primary.process_messages().await;
    for (to, message) in primary.transport.sent_messages() {
        if to == node(1) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            restarted.transport.inject_message(node(4), message);
        }
    }
    restarted.process_messages().await;

    let promoted = map_with(partition, node(1), &[node(4)], 4);
    primary.apply_raft_partition_map(promoted.clone()).await;
    restarted.apply_raft_partition_map(promoted).await;

    assert_eq!(restarted.role(partition), ReplicaRole::Primary);
    assert!(restarted.stores().db_get(ENTITY, ID).is_some());
}

#[tokio::test]
async fn control_tick_resends_once_the_lost_request_is_no_longer_marked_as_requested() {
    let partition = data_partition(ENTITY, ID);
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;
    restarted.requested_snapshots.remove(partition);

    let output = restarted.tick(60_000);

    let resent = output
        .catchup_requests
        .iter()
        .filter(|(to, message)| {
            *to == node(4) && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count();
    assert_eq!(resent, 1);
}

#[tokio::test]
async fn a_lost_snapshot_request_is_sent_again_after_the_retry_interval() {
    let partition = data_partition(ENTITY, ID);
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;
    assert_eq!(snapshot_requests(&restarted), 0);
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = restarted.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }

    restarted
        .apply_raft_partition_map(map_with(partition, node(4), &[node(1)], 3))
        .await;

    assert_eq!(snapshot_requests(&restarted), 1);
}

#[tokio::test]
async fn a_lost_snapshot_request_is_sent_again_when_the_primary_changes() {
    let partition = data_partition(ENTITY, ID);
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;

    restarted
        .apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 4))
        .await;

    assert_eq!(restarted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(
        snapshot_requests(&restarted),
        1,
        "no snapshot request reached the new primary node 2"
    );
}

fn heard_from(ctrl: &mut NodeController<MockTransport>, from: NodeId) {
    ctrl.heartbeat.receive_heartbeat(
        from,
        &crate::cluster::protocol::Heartbeat::create(from, 0),
        0,
    );
}

async fn holder_moved_out_of_the_assignment(
    partition: PartitionId,
) -> NodeController<MockTransport> {
    let mut holder = controller(3);
    holder.become_replica(partition, Epoch::new(1), 0);
    let (_, write) = holder
        .stores()
        .db_create_write(ENTITY, ID, br#"{"n":2}"#, 1000)
        .unwrap();
    holder.stores().apply_write(&write).unwrap();
    holder.step_down(partition);
    holder
}

async fn replica_moved_in_while_the_primary_is_dead(
    partition: PartitionId,
) -> NodeController<MockTransport> {
    let mut moved_in = controller(2);
    heard_from(&mut moved_in, node(3));
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 1))
        .await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 2))
        .await;
    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
    moved_in
}

#[tokio::test]
async fn a_replica_promoted_alone_asks_the_replica_it_replaced_before_taking_the_role() {
    let partition = data_partition(ENTITY, ID);
    let mut holder = holder_moved_out_of_the_assignment(partition).await;
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;
    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);

    holder.transport.inject_message(
        node(2),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(2))),
    );
    holder.process_messages().await;
    for (to, message) in holder.transport.sent_messages() {
        if to == node(2) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            moved_in.transport.inject_message(node(3), message);
        }
    }
    moved_in.process_messages().await;

    assert_eq!(moved_in.role(partition), ReplicaRole::Primary);
    assert!(moved_in.stores().db_get(ENTITY, ID).is_some());
}

#[tokio::test]
async fn a_replica_promoted_alone_keeps_asking_the_replica_it_replaced_when_a_new_replica_arrives()
{
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 4))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
    let to_new_replica = moved_in
        .transport
        .sent_messages()
        .iter()
        .filter(|(to, message)| {
            *to == node(4) && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count();
    assert_eq!(to_new_replica, 0);
}

#[tokio::test]
async fn a_replica_promoted_alone_waits_for_a_replaced_replica_it_has_not_heard_from() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = controller(2);
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(3)], 1))
        .await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 2))
        .await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_replica_promoted_alone_takes_the_role_when_the_replaced_replica_is_dead() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in.apply_dead_nodes(&[node(3)]);

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_primary_alone_with_no_earlier_assignment_takes_the_role() {
    let partition = data_partition(ENTITY, ID);
    let mut fresh = controller(2);

    fresh
        .apply_raft_partition_map(map_with(partition, node(2), &[], 1))
        .await;

    assert_eq!(fresh.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn the_replaced_replica_is_remembered_when_the_primary_changes_during_the_wait() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(4), &[node(2)], 3))
        .await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 4))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_primary_waiting_on_a_prior_holder_that_died_takes_the_role() {
    let partition = data_partition(ENTITY, ID);
    let alone = map_with(partition, node(2), &[], 3);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in.apply_raft_partition_map(alone.clone()).await;
    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);

    moved_in.apply_dead_nodes(&[node(3)]);
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = moved_in.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
    assert!(moved_in.apply_raft_partition_map(alone).await);

    assert_eq!(moved_in.role(partition), ReplicaRole::Primary);
}

fn requests_to(ctrl: &NodeController<MockTransport>, to: NodeId) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(target, message)| {
            *target == to && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count()
}

#[tokio::test]
async fn a_primary_alone_whose_kept_source_died_asks_the_replica_it_replaced() {
    let partition = data_partition(ENTITY, ID);
    let alone = map_with(partition, node(2), &[], 4);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    heard_from(&mut moved_in, node(1));
    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 3))
        .await;
    moved_in.apply_raft_partition_map(alone.clone()).await;
    assert_eq!(requests_to(&moved_in, node(3)), 0);

    moved_in.apply_dead_nodes(&[node(1)]);
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = moved_in.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }
    moved_in.apply_raft_partition_map(alone).await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(requests_to(&moved_in, node(3)), 1);
}

#[tokio::test]
async fn a_replica_promoted_with_a_new_replica_asks_the_replica_it_replaced() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 3))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(requests_to(&moved_in, node(4)), 0);
    assert_eq!(requests_to(&moved_in, node(3)), 1);
}

#[tokio::test]
async fn a_replica_swapped_to_primary_asks_the_old_primary_not_the_replica_it_replaced() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    assert_eq!(requests_to(&moved_in, node(1)), 1);

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 3))
        .await;

    assert_eq!(requests_to(&moved_in, node(3)), 0);
    assert_eq!(
        moved_in.requested_snapshots.get(partition).unwrap().source,
        node(1)
    );
}

#[tokio::test]
async fn a_waiting_replica_retries_its_primary_not_the_replica_it_replaced() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    let stalled_since = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(11))
        .unwrap();
    if let Some(request) = moved_in.requested_snapshots.get_mut(partition) {
        request.last_activity = stalled_since;
    }

    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 2))
        .await;

    assert_eq!(requests_to(&moved_in, node(1)), 2);
    assert_eq!(requests_to(&moved_in, node(3)), 0);
}
