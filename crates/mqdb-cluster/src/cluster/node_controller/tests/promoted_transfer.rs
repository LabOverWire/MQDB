// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::node_controller::snapshot::{
    SNAPSHOT_RETRY_BACKOFF_MS, SNAPSHOT_STALL_TIMEOUT_MS,
};
use crate::cluster::protocol::{
    CatchupResponse, FkCheckRequest, FkReverseLookupRequest, Operation, ReplicationWrite,
};
use crate::cluster::snapshot::SnapshotChunk;
use crate::cluster::{PartitionAssignment, PartitionMap};

const ENTITY: &str = "snap";
const ID: &str = "big";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn map_for(primary: NodeId, replicas: &[NodeId], epoch: u64) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        data_partition(ENTITY, ID),
        PartitionAssignment::new(primary, replicas.to_vec(), Epoch::new(epoch)),
    );
    map
}

fn value(n: u64, blob: usize) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "v": n, "blob": "x".repeat(blob)})).unwrap()
}

fn counter(ctrl: &NodeController<MockTransport>) -> Option<u64> {
    let found = ctrl.db_get(ENTITY, ID)?;
    let json: serde_json::Value = serde_json::from_slice(&found.data).unwrap();
    json["v"].as_u64()
}

async fn deliver(from: &NodeController<MockTransport>, to: &mut NodeController<MockTransport>) {
    for (dest, message) in from.transport.take_sent() {
        if dest == to.node_id {
            to.transport.inject_message(from.node_id, message);
        }
    }
    to.process_messages().await;
}

fn chunks_in(sent: Vec<(NodeId, ClusterMessage)>) -> Vec<SnapshotChunk> {
    sent.into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::SnapshotChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect()
}

fn heard(ctrl: &mut NodeController<MockTransport>, from: NodeId) {
    ctrl.heartbeat.receive_heartbeat(
        from,
        &crate::cluster::protocol::Heartbeat::create(from, 0),
        0,
    );
}

async fn primary_with_record(blob: usize) -> NodeController<MockTransport> {
    let mut primary = controller(1);
    primary.become_primary(data_partition(ENTITY, ID), Epoch::new(1));
    primary
        .apply_raft_partition_map(map_for(node(1), &[node(3)], 1))
        .await;
    primary
        .db_create(ENTITY, ID, &value(1, blob), 100)
        .await
        .unwrap();
    primary
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    primary.transport.take_sent();
    primary
}

#[tokio::test]
async fn write_acked_after_export_survives_promotion_mid_transfer() {
    let partition = data_partition(ENTITY, ID);
    let mut old_primary = primary_with_record(1024 * 1024).await;
    let mut promoted = controller(2);
    promoted
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    deliver(&promoted, &mut old_primary).await;
    for now in 1..40 {
        old_primary.drive_snapshots(now).await;
    }
    let chunks = chunks_in(old_primary.transport.take_sent());
    assert!(chunks.len() > 1, "need a multi-chunk transfer");
    let (last, earlier) = chunks.split_last().unwrap();
    for chunk in earlier {
        promoted
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    promoted.process_messages().await;

    old_primary
        .db_update(ENTITY, ID, &value(2, 16), 200)
        .await
        .unwrap();
    assert_eq!(counter(&old_primary), Some(2));

    let swapped = map_for(node(2), &[node(1)], 3);
    promoted.apply_raft_partition_map(swapped.clone()).await;
    old_primary.apply_raft_partition_map(swapped.clone()).await;
    promoted
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(last.clone()));
    promoted.process_messages().await;
    promoted.apply_raft_partition_map(swapped).await;
    for _ in 0..6 {
        deliver(&old_primary, &mut promoted).await;
        deliver(&promoted, &mut old_primary).await;
    }

    assert_eq!(promoted.role(partition), ReplicaRole::Primary);
    assert_eq!(
        counter(&promoted),
        Some(2),
        "new primary lacks the write the old primary acked; old primary holds {:?}",
        counter(&old_primary)
    );
}

#[tokio::test]
async fn control_write_acked_before_export_survives_promotion_mid_transfer() {
    let partition = data_partition(ENTITY, ID);
    let mut old_primary = primary_with_record(1024 * 1024).await;
    old_primary
        .db_update(ENTITY, ID, &value(2, 1024 * 1024), 200)
        .await
        .unwrap();
    let mut promoted = controller(2);
    promoted
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    old_primary.transport.take_sent();
    deliver(&promoted, &mut old_primary).await;
    for now in 1..40 {
        old_primary.drive_snapshots(now).await;
    }
    let chunks = chunks_in(old_primary.transport.take_sent());
    let (last, earlier) = chunks.split_last().unwrap();
    for chunk in earlier {
        promoted
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    promoted.process_messages().await;
    let swapped = map_for(node(2), &[node(1)], 3);
    promoted.apply_raft_partition_map(swapped.clone()).await;
    old_primary.apply_raft_partition_map(swapped).await;
    promoted
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(last.clone()));
    promoted.process_messages().await;
    deliver(&promoted, &mut old_primary).await;
    deliver(&old_primary, &mut promoted).await;
    assert_eq!(promoted.role(partition), ReplicaRole::Primary);
    assert_eq!(counter(&promoted), Some(2));
}

#[tokio::test]
async fn a_switch_to_a_silent_source_gets_the_full_grace() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(2);
    heard(&mut ctrl, node(3));
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;
    ctrl.drive_snapshots(1).await;
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(4)], 2))
        .await;
    assert_eq!(ctrl.snapshot_asked_from(partition), Some(node(3)));
    ctrl.apply_dead_nodes(&[node(3)]);
    ctrl.drive_snapshots(100_000).await;
    assert_eq!(ctrl.snapshot_asked_from(partition), Some(node(4)));
    let carried = ctrl.snapshot_first_asked(partition);
    ctrl.snapshot_request_stalled(partition);
    ctrl.drive_snapshots(130_001).await;
    assert_eq!(
        ctrl.role(partition),
        ReplicaRole::AwaitingSnapshot,
        "took the primary role 30 s after first asking a node never heard from; first_asked {carried:?}"
    );
}

#[tokio::test]
async fn a_waiting_primary_does_not_answer_fk_check_that_the_target_is_missing() {
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;
    let partition = data_partition(ENTITY, ID);
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    ctrl.transport.take_sent();
    let req = FkCheckRequest::create(7, ENTITY, ID).unwrap();
    ctrl.handle_fk_check_request(node(1), &req).await;
    let answer = ctrl
        .transport
        .take_sent()
        .into_iter()
        .find_map(|(_, message)| match message {
            ClusterMessage::FkCheckResponse(resp) => Some(resp.exists()),
            _ => None,
        });
    assert_ne!(
        answer,
        Some(false),
        "a primary without the data told the requester the target does not exist"
    );
}

#[tokio::test]
async fn a_waiting_primary_does_not_answer_a_list_as_complete() {
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;
    let partition = data_partition(ENTITY, ID);
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    ctrl.transport.take_sent();
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::JsonDbRequest {
            partition,
            request: JsonDbRequest {
                request_id: 7,
                op: JsonDbOp::List,
                entity: ENTITY.to_string(),
                id: None,
                payload: b"{}".to_vec(),
                response_topic: "resp/l".to_string(),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        },
    );
    ctrl.process_messages().await;
    let payload = ctrl
        .transport
        .take_sent()
        .into_iter()
        .find_map(|(_, message)| match message {
            ClusterMessage::JsonDbResponse(response) => Some(response.payload),
            _ => None,
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_ne!(body["status"], "ok", "list while waiting answered {body}");
}

#[tokio::test]
async fn a_promoted_replica_asks_its_live_old_primary_when_the_replica_also_moves() {
    let partition = data_partition(ENTITY, ID);
    let mut ctrl = controller(2);
    heard(&mut ctrl, node(1));
    heard(&mut ctrl, node(3));
    ctrl.apply_raft_partition_map(map_for(node(1), &[node(2)], 1))
        .await;
    assert_eq!(ctrl.snapshot_asked_from(partition), Some(node(1)));
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 2))
        .await;
    assert_eq!(ctrl.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(
        ctrl.snapshot_asked_from(partition),
        Some(node(1)),
        "dropped the transfer from the live holder for a replica that never held the data"
    );
}

async fn imported_and_catching_up_from_node_1() -> (NodeController<MockTransport>, PartitionId) {
    let partition = data_partition(ENTITY, ID);
    let mut old_primary = primary_with_record(16).await;
    let mut promoted = controller(2);
    heard(&mut promoted, node(1));
    promoted
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    deliver(&promoted, &mut old_primary).await;
    for now in 1..5 {
        old_primary.drive_snapshots(now).await;
    }
    let swapped = map_for(node(2), &[node(1)], 3);
    promoted.drive_snapshots(10).await;
    promoted.apply_raft_partition_map(swapped).await;
    deliver(&old_primary, &mut promoted).await;
    assert!(promoted.db_get(ENTITY, ID).is_some());
    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
    (promoted, partition)
}

#[tokio::test]
async fn an_imported_node_keeps_refusing_until_its_source_answers() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;

    promoted.drive_snapshots(11).await;

    assert!(promoted.waits_for_primary_data(partition));
    assert!(!promoted.can_serve_reads(partition));
}

#[tokio::test]
async fn an_unanswered_catchup_is_asked_again_then_the_role_is_taken() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    promoted.transport.take_sent();

    promoted
        .drive_snapshots(10 + SNAPSHOT_RETRY_BACKOFF_MS + 1)
        .await;
    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
    let asked = promoted
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, message)| {
            *to == node(1) && matches!(message, ClusterMessage::CatchupRequest(_))
        })
        .count();
    assert_eq!(asked, 1);

    promoted
        .drive_snapshots(10 + SNAPSHOT_STALL_TIMEOUT_MS + 1)
        .await;
    assert_eq!(promoted.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn an_imported_node_takes_the_role_when_its_source_dies_during_catchup() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;

    promoted.apply_dead_nodes(&[node(1)]);
    promoted
        .drive_snapshots(SNAPSHOT_RETRY_BACKOFF_MS + 1)
        .await;

    assert_eq!(promoted.role(partition), ReplicaRole::Primary);
    assert!(promoted.db_get(ENTITY, ID).is_some());
}

#[tokio::test]
async fn a_catchup_answer_from_another_node_does_not_end_the_wait() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let from_seq = promoted.sequence(partition).unwrap_or(0) + 1;

    promoted.transport.inject_message(
        node(3),
        ClusterMessage::CatchupResponse(CatchupResponse::end_marker(
            partition,
            node(3),
            0,
            from_seq,
        )),
    );
    promoted.process_messages().await;

    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_marker_covering_more_than_was_sent_leads_to_a_new_snapshot() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let from_seq = promoted.sequence(partition).unwrap_or(0) + 1;
    promoted.transport.take_sent();

    promoted.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::end_marker(
            partition,
            node(1),
            from_seq + 5,
            from_seq,
        )),
    );
    promoted.process_messages().await;

    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(promoted.snapshot_asked_from(partition), Some(node(1)));
    assert!(promoted.transport.take_sent().iter().any(
        |(to, message)| *to == node(1) && matches!(message, ClusterMessage::SnapshotRequest(_))
    ));
}

#[tokio::test]
async fn a_gap_in_catchup_writes_leads_to_a_new_snapshot_not_the_role() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let held = promoted.sequence(partition).unwrap_or(0);
    promoted.transport.take_sent();
    let write = ReplicationWrite::new(
        partition,
        Operation::Update,
        Epoch::new(2),
        held + 5,
        "_db_data".to_string(),
        "x".to_string(),
        Vec::new(),
    );

    promoted.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::create(partition, node(1), vec![write])),
    );
    promoted.process_messages().await;

    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(
        promoted
            .transport
            .take_sent()
            .iter()
            .any(|(_, message)| matches!(message, ClusterMessage::SnapshotRequest(_)))
    );
}

#[tokio::test]
async fn a_held_write_is_applied_after_the_catchup_not_before() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let from_seq = promoted.sequence(partition).unwrap_or(0) + 1;
    let (_, write) = promoted
        .stores
        .db_update_write(ENTITY, ID, &value(9, 16), 300)
        .unwrap();
    promoted.write_or_forward(write).await;
    assert_eq!(promoted.held_writes.len(), 1);
    assert_eq!(counter(&promoted), Some(1));

    promoted.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::end_marker(
            partition,
            node(1),
            from_seq - 1,
            from_seq,
        )),
    );
    promoted.process_messages().await;

    assert_eq!(promoted.role(partition), ReplicaRole::Primary);
    assert_eq!(counter(&promoted), Some(9));
}

#[tokio::test]
async fn a_waiting_primary_does_not_answer_a_reverse_lookup() {
    let mut ctrl = controller(2);
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;
    ctrl.transport.take_sent();
    let req = FkReverseLookupRequest::create(9, "posts", "author_id", "u1", "users", "");

    ctrl.handle_fk_reverse_lookup_request(node(1), &req).await;

    assert!(ctrl.transport.take_sent().is_empty());
}

#[tokio::test]
async fn a_waiting_primary_refuses_a_local_fk_check_it_cannot_answer() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};
    let mut ctrl = controller(2);
    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_target_fk",
        "target",
        ENTITY,
        "id",
        OnDeleteAction::Restrict,
    )
    .unwrap();
    ctrl.stores.db_constraints.add(fk).unwrap();
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;

    let Err(message) = ctrl
        .start_fk_existence_check("posts", &serde_json::json!({"target": ID}))
        .await
    else {
        panic!("a waiting primary answered an FK check about data it does not have");
    };

    assert!(message.contains("not ready"), "{message}");
}

#[tokio::test]
async fn a_waiting_node_refuses_to_start_a_reverse_lookup() {
    use crate::cluster::db::{ClusterConstraint, OnDeleteAction};
    let mut ctrl = controller(2);
    let fk = ClusterConstraint::foreign_key(
        "posts",
        "posts_target_fk",
        "target",
        ENTITY,
        "id",
        OnDeleteAction::Restrict,
    )
    .unwrap();
    ctrl.stores.db_constraints.add(fk).unwrap();
    ctrl.apply_raft_partition_map(map_for(node(2), &[node(3)], 1))
        .await;

    assert!(
        ctrl.start_fk_reverse_lookup(ENTITY, ID, None)
            .await
            .is_err()
    );
}

fn update_at(ctrl: &NodeController<MockTransport>, sequence: u64, n: u64) -> ReplicationWrite {
    let (_, mut write) = ctrl
        .stores
        .db_update_write(ENTITY, ID, &value(n, 16), 300)
        .unwrap();
    write.sequence = sequence;
    write
}

async fn answer_with(
    ctrl: &mut NodeController<MockTransport>,
    partition: PartitionId,
    writes: Vec<ReplicationWrite>,
) {
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::create(partition, node(1), writes)),
    );
    ctrl.process_messages().await;
}

#[tokio::test]
async fn a_catchup_write_advances_the_sequence_and_the_log_before_the_role_is_taken() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let held = promoted.sequence(partition).unwrap_or(0);
    let write = update_at(&promoted, held + 1, 2);

    answer_with(&mut promoted, partition, vec![write]).await;

    assert_eq!(counter(&promoted), Some(2));
    assert_eq!(promoted.sequence(partition), Some(held + 1));
    assert_eq!(
        promoted.write_log.newest_sequence(partition),
        Some(held + 1)
    );
    assert_eq!(promoted.role(partition), ReplicaRole::AwaitingSnapshot);
}

#[tokio::test]
async fn a_catchup_write_past_a_gap_is_not_applied() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let held = promoted.sequence(partition).unwrap_or(0);
    let write = update_at(&promoted, held + 5, 2);

    answer_with(&mut promoted, partition, vec![write]).await;

    assert_eq!(counter(&promoted), Some(1));
}

#[tokio::test]
async fn a_catchup_write_already_held_does_not_roll_the_record_back() {
    let (mut promoted, partition) = imported_and_catching_up_from_node_1().await;
    let held = promoted.sequence(partition).unwrap_or(0);
    assert!(held > 0);
    let write = update_at(&promoted, held, 7);

    answer_with(&mut promoted, partition, vec![write]).await;

    assert_eq!(counter(&promoted), Some(1));
    assert_eq!(promoted.sequence(partition), Some(held));
}
