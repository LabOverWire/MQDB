// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::node_controller::snapshot::{
    SNAPSHOT_CHUNKS_PER_PUMP, SNAPSHOT_RETRY_BACKOFF_MS, SNAPSHOT_STALL_TIMEOUT_MS,
};
use crate::cluster::snapshot::{SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender};
use crate::cluster::{PartitionAssignment, PartitionMap};
use std::sync::atomic::Ordering;

const ENTITY: &str = "snap";
const ID: &str = "big";
const START: u64 = 1_000;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn assign(ctrl: &mut NodeController<MockTransport>, primary: NodeId) -> PartitionId {
    let partition = data_partition(ENTITY, ID);
    let mut map = PartitionMap::default();
    map.set(
        partition,
        PartitionAssignment {
            primary: Some(primary),
            replicas: vec![node(2)],
            epoch: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);
    partition
}

fn record(blob_len: usize) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "blob": "x".repeat(blob_len)})).unwrap()
}

fn primary_holding(id: u16, blob_len: usize) -> (NodeController<MockTransport>, PartitionId) {
    let mut primary = controller(id);
    let partition = assign(&mut primary, node(id));
    primary.become_primary(partition, Epoch::new(1));
    primary
        .stores
        .db_create_replicated(ENTITY, ID, &record(blob_len), 0)
        .unwrap();
    primary.transport.take_sent();
    (primary, partition)
}

async fn awaiting_replica(source: NodeId) -> (NodeController<MockTransport>, PartitionId) {
    let mut replica = controller(2);
    let partition = assign(&mut replica, source);
    replica
        .become_replica_with_snapshot(partition, Epoch::new(1), source)
        .await;
    (replica, partition)
}

async fn deliver(from: &NodeController<MockTransport>, to: &mut NodeController<MockTransport>) {
    for (dest, message) in from.transport.take_sent() {
        if dest == to.node_id {
            to.transport.inject_message(from.node_id, message);
        }
    }
    to.process_messages().await;
}

fn chunks_in(sent: &[(NodeId, ClusterMessage)]) -> Vec<SnapshotChunk> {
    sent.iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::SnapshotChunk(chunk) => Some(chunk.clone()),
            _ => None,
        })
        .collect()
}

fn requests_in(sent: &[(NodeId, ClusterMessage)]) -> usize {
    sent.iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::SnapshotRequest(_)))
        .count()
}

fn failures_in(sent: &[(NodeId, ClusterMessage)]) -> usize {
    sent.iter()
        .filter(|(_, message)| {
            matches!(message, ClusterMessage::SnapshotComplete(complete) if !complete.is_ok())
        })
        .count()
}

fn imported(ctrl: &NodeController<MockTransport>) -> bool {
    ctrl.db_get(ENTITY, ID).is_some()
}

fn export_of(blob_len: usize, chunk_size: usize, transfer_id: u64) -> Vec<SnapshotChunk> {
    let (primary, partition) = primary_holding(9, blob_len);
    let mut sender = SnapshotSender::new(partition, primary.stores.export_partition(partition), 1)
        .unwrap()
        .with_chunk_size(chunk_size)
        .unwrap()
        .with_transfer_id(transfer_id);
    std::iter::from_fn(|| {
        let chunk = sender.current_chunk()?;
        sender.advance();
        Some(chunk)
    })
    .collect()
}

#[tokio::test]
async fn a_snapshot_larger_than_one_chunk_is_imported() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;

    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    assert!(imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

#[tokio::test]
async fn a_snapshot_over_the_pump_budget_finishes_on_later_drives() {
    let (mut primary, _) = primary_holding(1, 17 * 64 * 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    deliver(&replica, &mut primary).await;

    let first = chunks_in(&primary.transport.sent_messages());
    assert_eq!(first.len(), SNAPSHOT_CHUNKS_PER_PUMP as usize);
    let total = first[0].total_chunks as usize;
    assert!(total > 2 * SNAPSHOT_CHUNKS_PER_PUMP as usize);

    let mut now = START;
    for _ in 0..total {
        if primary.outgoing_snapshots.is_empty() {
            break;
        }
        now += 10;
        primary.drive_snapshots(now).await;
    }
    assert!(primary.outgoing_snapshots.is_empty());
    let sent = chunks_in(&primary.transport.sent_messages());
    let indexes: Vec<u32> = sent.iter().map(|chunk| chunk.chunk_index).collect();
    assert_eq!(
        indexes,
        (0..u32::try_from(total).unwrap()).collect::<Vec<_>>()
    );

    deliver(&primary, &mut replica).await;
    assert!(imported(&replica));
}

#[tokio::test]
async fn a_full_send_queue_delays_chunks_without_skipping_any() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;

    primary.transport.queue_full.store(true, Ordering::SeqCst);
    deliver(&replica, &mut primary).await;
    assert!(chunks_in(&primary.transport.sent_messages()).is_empty());
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.transport.queue_full.store(false, Ordering::SeqCst);
    primary.drive_snapshots(START).await;
    let indexes: Vec<u32> = chunks_in(&primary.transport.sent_messages())
        .iter()
        .map(|chunk| chunk.chunk_index)
        .collect();
    assert_eq!(indexes, vec![0, 1, 2, 3]);

    deliver(&primary, &mut replica).await;
    assert!(imported(&replica));
}

#[tokio::test]
async fn a_send_error_other_than_a_full_queue_drops_the_transfer() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (replica, _) = awaiting_replica(node(1)).await;
    primary.transport.queue_full.store(true, Ordering::SeqCst);
    deliver(&replica, &mut primary).await;
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.transport.queue_full.store(false, Ordering::SeqCst);
    primary.transport.refuse_sends.store(true, Ordering::SeqCst);
    primary.drive_snapshots(START).await;

    assert!(primary.outgoing_snapshots.is_empty());
}

#[tokio::test]
async fn a_repeated_request_restarts_the_transfer_in_place() {
    let (mut primary, partition) = primary_holding(1, 17 * 64 * 1024);
    primary.transport.queue_full.store(true, Ordering::SeqCst);
    let request = || ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(2)));
    primary.transport.inject_message(node(2), request());
    primary.process_messages().await;
    primary.transport.queue_full.store(false, Ordering::SeqCst);
    primary.drive_snapshots(START).await;
    let first_id = chunks_in(&primary.transport.take_sent())[0].transfer_id;

    primary.transport.queue_full.store(true, Ordering::SeqCst);
    primary.transport.inject_message(node(2), request());
    primary.process_messages().await;
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.transport.queue_full.store(false, Ordering::SeqCst);
    primary.drive_snapshots(START + 10).await;
    let restarted = chunks_in(&primary.transport.take_sent());
    assert_eq!(restarted[0].chunk_index, 0);
    assert_ne!(restarted[0].transfer_id, first_id);
    assert!(restarted[0].transfer_id.is_some());
}

#[tokio::test]
async fn a_failed_completion_leaves_a_newer_transfer_running() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    primary.transport.queue_full.store(true, Ordering::SeqCst);
    primary.transport.inject_message(
        node(2),
        ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(2))),
    );
    primary.process_messages().await;
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.transport.inject_message(
        node(2),
        ClusterMessage::SnapshotComplete(SnapshotComplete::failed(partition)),
    );
    primary.process_messages().await;
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.transport.queue_full.store(false, Ordering::SeqCst);
    primary.drive_snapshots(START).await;
    assert_eq!(chunks_in(&primary.transport.take_sent()).len(), 4);
}

#[tokio::test]
async fn a_stalled_request_is_abandoned_and_sent_again() {
    let (mut replica, _) = awaiting_replica(node(1)).await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);

    replica.drive_snapshots(START).await;
    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS - 1)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 0);

    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn a_chunk_that_makes_progress_resets_the_stall_timer() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    deliver(&replica, &mut primary).await;

    let first = chunks_in(&primary.transport.take_sent()).remove(0);
    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS - 1)
        .await;
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(first));
    replica.process_messages().await;

    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 0);
}

#[tokio::test]
async fn a_chunk_nobody_asked_for_is_not_imported() {
    let (primary, partition) = primary_holding(1, 1024);
    let mut replica = controller(2);
    assign(&mut replica, node(1));

    let chunk = SnapshotSender::new(partition, primary.stores.export_partition(partition), 1)
        .unwrap()
        .current_chunk()
        .unwrap();
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk));
    replica.process_messages().await;

    assert!(!imported(&replica));
}

#[tokio::test]
async fn a_chunk_from_a_node_that_was_not_asked_is_not_imported() {
    let (other, partition) = primary_holding(3, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;

    let chunk = SnapshotSender::new(partition, other.stores.export_partition(partition), 1)
        .unwrap()
        .current_chunk()
        .unwrap();
    replica
        .transport
        .inject_message(node(3), ClusterMessage::SnapshotChunk(chunk));
    replica.process_messages().await;

    assert!(!imported(&replica));
}

#[tokio::test]
async fn chunks_of_a_restarted_transfer_are_not_spliced_into_the_first() {
    let first = export_of(1024, 512, 1);
    let restarted = export_of(1500, 768, 2);
    assert_eq!(first.len(), restarted.len());
    let (mut replica, _) = awaiting_replica(node(1)).await;

    let inject = |replica: &NodeController<MockTransport>, chunk: &SnapshotChunk| {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    };
    inject(&replica, &first[0]);
    for chunk in &restarted[1..] {
        inject(&replica, chunk);
    }
    replica.process_messages().await;
    assert!(!imported(&replica));
    assert_eq!(failures_in(&replica.transport.take_sent()), 0);

    for chunk in &first[1..] {
        inject(&replica, chunk);
    }
    replica.process_messages().await;
    let blob = replica.db_get(ENTITY, ID).unwrap();
    assert_eq!(blob.data, record(1024));
}

#[tokio::test]
async fn a_gap_abandons_the_transfer_and_backs_off_before_asking_again() {
    let chunks = export_of(1024, 256, 5);
    assert!(chunks.len() >= 3);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    replica.transport.take_sent();

    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(chunks[0].clone()));
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(chunks[2].clone()));
    replica.process_messages().await;
    assert_eq!(failures_in(&replica.transport.take_sent()), 1);

    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS - 1)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 0);

    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);

    for chunk in &chunks {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    replica.process_messages().await;
    assert!(imported(&replica));
}

#[tokio::test]
async fn an_unsent_request_backs_off_and_is_retried() {
    let mut replica = controller(2);
    let partition = assign(&mut replica, node(1));
    replica.drive_snapshots(START).await;
    replica.transport.refuse_sends.store(true, Ordering::SeqCst);
    replica
        .become_replica_with_snapshot(partition, Epoch::new(1), node(1))
        .await;
    replica
        .transport
        .refuse_sends
        .store(false, Ordering::SeqCst);

    replica.drive_snapshots(START + 10).await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 0);

    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn a_legacy_single_chunk_without_a_transfer_id_is_imported() {
    let (primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;

    let chunk = SnapshotSender::new(partition, primary.stores.export_partition(partition), 1)
        .unwrap()
        .current_chunk()
        .unwrap();
    assert_eq!(chunk.transfer_id, None);
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk));
    replica.process_messages().await;

    assert!(imported(&replica));
}

fn map_for(primary: NodeId, replicas: &[NodeId], epoch: u64) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        data_partition(ENTITY, ID),
        PartitionAssignment {
            primary: Some(primary),
            replicas: replicas.to_vec(),
            epoch: Epoch::new(epoch),
        },
    );
    map
}

async fn awaiting_replica_from_map() -> (NodeController<MockTransport>, PartitionId) {
    let mut replica = controller(2);
    replica
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 1))
        .await;
    (replica, data_partition(ENTITY, ID))
}

#[tokio::test]
async fn a_snapshot_finishing_after_the_node_became_primary_is_not_imported() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (mut replica, partition) = awaiting_replica_from_map().await;
    deliver(&replica, &mut primary).await;

    replica
        .apply_raft_partition_map(map_for(node(2), &[node(3)], 2))
        .await;
    assert_eq!(replica.role(partition), ReplicaRole::Primary);
    deliver(&primary, &mut replica).await;

    assert!(!imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_snapshot_finishing_after_step_down_is_not_imported() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (mut replica, partition) = awaiting_replica_from_map().await;
    deliver(&replica, &mut primary).await;

    replica
        .apply_raft_partition_map(map_for(node(1), &[node(3)], 2))
        .await;
    deliver(&primary, &mut replica).await;

    assert!(!imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::None);
}

#[tokio::test]
async fn the_new_primary_is_asked_when_the_primary_moves_mid_transfer() {
    let (mut replica, _) = awaiting_replica_from_map().await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);

    replica
        .apply_raft_partition_map(map_for(node(3), &[node(2)], 2))
        .await;

    let sent = replica.transport.take_sent();
    let to_new_primary = sent
        .iter()
        .filter(|(to, message)| {
            *to == node(3) && matches!(message, ClusterMessage::SnapshotRequest(_))
        })
        .count();
    assert_eq!(to_new_primary, 1);
}

#[tokio::test]
async fn outgoing_transfers_to_a_dead_node_are_dropped() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (replica, _) = awaiting_replica(node(1)).await;
    primary.transport.queue_full.store(true, Ordering::SeqCst);
    deliver(&replica, &mut primary).await;
    assert_eq!(primary.outgoing_snapshots.len(), 1);

    primary.apply_dead_nodes(&[node(2)]);

    assert!(primary.outgoing_snapshots.is_empty());
}

#[tokio::test]
async fn a_request_to_a_dead_source_is_abandoned() {
    let (mut replica, partition) = awaiting_replica(node(1)).await;
    replica.transport.take_sent();

    replica.apply_dead_nodes(&[node(1)]);

    assert!(!replica.incoming_snapshots.contains_key(&partition));
}

#[tokio::test]
async fn only_the_transfer_being_sent_holds_an_export() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    primary.transport.queue_full.store(true, Ordering::SeqCst);
    for offset in 0..3 {
        let requested = PartitionId::new((partition.get() + offset) % 256).unwrap();
        primary.transport.inject_message(
            node(2),
            ClusterMessage::SnapshotRequest(SnapshotRequest::create(requested, node(2))),
        );
    }
    primary.process_messages().await;

    assert_eq!(primary.outgoing_snapshots.len(), 3);
    let exported = primary
        .outgoing_snapshots
        .iter()
        .filter(|outgoing| outgoing.sender.is_some())
        .count();
    assert_eq!(exported, 1);
}

#[tokio::test]
async fn a_peer_with_a_full_queue_does_not_hold_up_another_peer() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    primary.transport.full_peers.lock().unwrap().insert(node(2));
    for requester in [2, 3] {
        primary.transport.inject_message(
            node(requester),
            ClusterMessage::SnapshotRequest(SnapshotRequest::create(partition, node(requester))),
        );
    }
    primary.process_messages().await;

    let sent = primary.transport.take_sent();
    assert!(sent.iter().all(|(to, _)| *to == node(3)));
    assert_eq!(chunks_in(&sent).len(), 4);
    assert_eq!(primary.outgoing_snapshots.len(), 1);
}

#[tokio::test]
async fn a_stale_chunk_does_not_reset_the_stall_timer() {
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let first = export_of(1024, 256, 1);
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(first[0].clone()));
    replica.process_messages().await;

    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS - 1)
        .await;
    let stale = export_of(1024, 256, 2);
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(stale[1].clone()));
    replica
        .transport
        .inject_message(node(3), ClusterMessage::SnapshotChunk(first[1].clone()));
    replica.process_messages().await;
    replica.transport.take_sent();

    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS)
        .await;
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn a_replica_that_is_not_awaiting_a_snapshot_does_not_ask_for_one() {
    let mut owner = controller(2);
    let partition = assign(&mut owner, node(2));
    owner.become_primary(partition, Epoch::new(1));
    let mut replica = controller(2);
    assign(&mut replica, node(1));
    replica.become_replica(partition, Epoch::new(1), 0);

    owner.drive_snapshots(START).await;
    replica.drive_snapshots(START).await;

    assert_eq!(requests_in(&owner.transport.take_sent()), 0);
    assert_eq!(requests_in(&replica.transport.take_sent()), 0);
}

#[tokio::test]
async fn a_new_chunk_zero_restarts_the_transfer() {
    let first = export_of(1024, 256, 1);
    let restarted = export_of(1500, 256, 2);
    let (mut replica, _) = awaiting_replica(node(1)).await;

    for chunk in first.iter().take(2).chain(restarted.iter()) {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    replica.process_messages().await;

    assert_eq!(replica.db_get(ENTITY, ID).unwrap().data, record(1500));
}

#[tokio::test]
async fn a_legacy_transfer_spanning_several_chunks_is_imported_in_order() {
    let (primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let mut sender = SnapshotSender::new(partition, primary.stores.export_partition(partition), 1)
        .unwrap()
        .with_chunk_size(256)
        .unwrap();
    while let Some(chunk) = sender.current_chunk() {
        assert_eq!(chunk.transfer_id, None);
        sender.advance();
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk));
    }
    replica.process_messages().await;

    assert!(imported(&replica));
}

#[tokio::test]
async fn a_drive_abandons_a_request_to_a_node_that_is_no_longer_primary() {
    let (mut replica, _) = awaiting_replica_from_map().await;
    replica.drive_snapshots(START).await;
    replica.transport.take_sent();

    replica.update_partition_map(map_for(node(3), &[node(2)], 2));
    replica.drive_snapshots(START + 10).await;

    let sent = replica.transport.take_sent();
    assert!(sent.iter().any(
        |(to, message)| *to == node(3) && matches!(message, ClusterMessage::SnapshotRequest(_))
    ));
}
