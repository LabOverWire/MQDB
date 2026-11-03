// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::data_partition;
use crate::cluster::snapshot::SnapshotRequest;
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
async fn a_replica_promoted_after_its_snapshot_request_was_lost_does_not_serve_without_the_record()
{
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
async fn control_a_drive_asks_once_the_lost_request_is_no_longer_recorded() {
    let partition = data_partition(ENTITY, ID);
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;
    restarted.incoming_snapshots.remove(&partition);
    restarted.drive_snapshots(60_000).await;

    assert_eq!(snapshot_requests(&restarted), 1);
}

#[tokio::test]
async fn a_lost_snapshot_request_is_sent_again_after_the_retry_interval() {
    let partition = data_partition(ENTITY, ID);
    let mut restarted = restarted_node_without_a_link_to_the_primary(partition).await;
    assert_eq!(snapshot_requests(&restarted), 0);
    restarted.snapshot_request_stalled(partition);
    restarted
        .drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;

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
    for (to, message) in moved_in.transport.sent_messages() {
        if to == node(3) && matches!(message, ClusterMessage::CatchupRequest(_)) {
            holder.transport.inject_message(node(2), message);
        }
    }
    holder.process_messages().await;
    for (to, message) in holder.transport.sent_messages() {
        if to == node(2) && matches!(message, ClusterMessage::CatchupResponse(_)) {
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
    moved_in.snapshot_request_stalled(partition);
    moved_in
        .drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
    moved_in.snapshot_request_stalled(partition);
    moved_in
        .drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
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
    assert_eq!(moved_in.snapshot_asked_from(partition).unwrap(), node(1));
}

#[tokio::test]
async fn a_waiting_replica_retries_its_primary_not_the_replica_it_replaced() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in.snapshot_request_stalled(partition);
    moved_in
        .drive_snapshots(crate::cluster::node_controller::snapshot::SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 2))
        .await;

    assert_eq!(requests_to(&moved_in, node(1)), 2);
    assert_eq!(requests_to(&moved_in, node(3)), 0);
}

#[tokio::test]
async fn a_replica_whose_first_map_is_the_move_has_nobody_else_to_ask_when_promoted_alone() {
    let partition = data_partition(ENTITY, ID);
    let mut joined = controller(2);
    heard_from(&mut joined, node(3));
    joined
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 2))
        .await;

    joined
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;

    assert_eq!(joined.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_request_abandoned_because_the_primary_moved_remembers_the_replaced_replica() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in.update_partition_map(map_with(partition, node(4), &[node(2)], 3));
    moved_in.drive_snapshots(1).await;
    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(4)));
    moved_in.update_partition_map(map_with(partition, node(4), &[node(2)], 3));

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 4))
        .await;

    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(3)));
}

#[tokio::test]
async fn the_replaced_replica_stays_remembered_after_the_import_while_catching_up() {
    let partition = data_partition(ENTITY, ID);
    let mut old_primary = controller(1);
    old_primary.become_replica(partition, Epoch::new(1), 0);
    let (_, write) = old_primary
        .stores()
        .db_create_write(ENTITY, ID, br#"{"n":2}"#, 1000)
        .unwrap();
    old_primary.stores().apply_write(&write).unwrap();
    old_primary.step_down(partition);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    heard_from(&mut moved_in, node(1));
    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 3))
        .await;
    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(1)));

    old_primary.transport.inject_message(
        node(2),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(2))),
    );
    old_primary.process_messages().await;
    for (to, message) in old_primary.transport.sent_messages() {
        if to == node(2) && matches!(message, ClusterMessage::SnapshotChunk(_)) {
            moved_in.transport.inject_message(node(1), message);
        }
    }
    moved_in.process_messages().await;
    assert!(moved_in.db_get(ENTITY, ID).is_some());
    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);

    moved_in.apply_dead_nodes(&[node(1)]);
    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 4))
        .await;

    assert_eq!(moved_in.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(3)));
}

#[tokio::test]
async fn a_repeated_request_to_the_same_source_remembers_the_replaced_replica() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;

    moved_in.request_snapshot(partition, node(1)).await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 3))
        .await;

    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(3)));
}

#[tokio::test]
async fn the_first_replaced_replica_stays_remembered_across_later_moves() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2), node(5)], 3))
        .await;
    moved_in
        .apply_raft_partition_map(map_with(partition, node(1), &[node(2), node(6)], 4))
        .await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(2), &[], 5))
        .await;

    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(3)));
}

#[tokio::test]
async fn a_repeated_request_keeps_a_waiting_primary_on_its_source_through_two_moves() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(2);
    heard_from(&mut ctrl, node(1));
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 1))
        .await;
    ctrl.request_snapshot(partition, node(1)).await;

    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(4)], 2))
        .await;
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(5)], 3))
        .await;

    assert_eq!(ctrl.snapshot_asked_from(partition), Some(node(1)));
}

#[tokio::test]
async fn a_wait_switched_to_a_new_source_by_the_map_starts_a_new_grace() {
    let partition = data_partition(ENTITY, ID);
    let mut moved_in = replica_moved_in_while_the_primary_is_dead(partition).await;
    moved_in.drive_snapshots(1).await;
    assert_eq!(moved_in.snapshot_first_asked(partition), Some(1));
    moved_in.drive_snapshots(50).await;

    moved_in
        .apply_raft_partition_map(map_with(partition, node(4), &[node(2)], 3))
        .await;

    assert_eq!(moved_in.snapshot_asked_from(partition), Some(node(4)));
    assert_eq!(moved_in.snapshot_first_asked(partition), Some(50));
}

#[tokio::test]
async fn a_dead_source_is_replaced_on_the_next_drive() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(2);
    heard_from(&mut ctrl, node(1));
    ctrl.apply_raft_partition_map(map_with(partition, node(1), &[node(2)], 1))
        .await;
    ctrl.drive_snapshots(1_000_000).await;
    ctrl.update_partition_map(map_with(partition, node(4), &[node(2)], 2));
    assert_eq!(requests_to(&ctrl, node(4)), 0);

    ctrl.apply_dead_nodes(&[node(1)]);
    ctrl.drive_snapshots(1_000_001).await;

    assert_eq!(requests_to(&ctrl, node(4)), 1);
}

#[tokio::test]
async fn a_repeated_request_by_a_waiting_primary_is_not_abandoned_as_moved() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(2);
    heard_from(&mut ctrl, node(1));
    ctrl.apply_raft_partition_map(map_with(partition, node(2), &[node(1)], 1))
        .await;
    ctrl.request_snapshot(partition, node(1)).await;
    ctrl.update_partition_map(map_with(partition, node(2), &[node(1)], 1));

    ctrl.drive_snapshots(1).await;

    assert_eq!(requests_to(&ctrl, node(1)), 1);
}
