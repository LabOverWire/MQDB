// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::node_controller::catchup::catchup_pieces;
use crate::cluster::node_controller::snapshot::{
    SNAPSHOT_CHUNKS_PER_PUMP, SNAPSHOT_RETRY_BACKOFF_MS, SNAPSHOT_STALL_TIMEOUT_MS,
};
use crate::cluster::protocol::{CatchupRequest, CatchupResponse, Operation, ReplicationWrite};
use crate::cluster::snapshot::{SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender};
use crate::cluster::store_manager::StoreManager;
use crate::cluster::test_support::FaultyBackend;
use crate::cluster::write_log::PartitionWriteLog;
use crate::cluster::{PartitionAssignment, PartitionMap};
use mqdb_core::storage::{MemoryBackend, StorageBackend};
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
            primary_since: Epoch::new(1),
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
    let seeded = primary
        .stores
        .db_create_write(ENTITY, ID, &record(blob_len), 0)
        .map(|(_, write)| write)
        .unwrap();
    primary.stores.apply_write(&seeded).unwrap();
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
    let mut sender = SnapshotSender::new(
        partition,
        primary.stores.export_partition(partition).unwrap(),
        1,
    )
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

fn completions_in(sent: &[(NodeId, ClusterMessage)]) -> usize {
    sent.iter()
        .filter(|(_, message)| {
            matches!(message, ClusterMessage::SnapshotComplete(complete) if complete.is_ok())
        })
        .count()
}

async fn awaiting_replica_on(
    disk: &Arc<dyn StorageBackend>,
    source: NodeId,
) -> (NodeController<MockTransport>, PartitionId) {
    let mut replica = NodeController::new_with_storage(
        node(2),
        MockTransport::new(node(2)),
        TransportConfig::default(),
        Some(Arc::clone(disk)),
        flume::unbounded().0,
        flume::unbounded().0,
    );
    let partition = assign(&mut replica, source);
    replica
        .become_replica_with_snapshot(partition, Epoch::new(1), source)
        .await;
    (replica, partition)
}

#[tokio::test]
async fn an_imported_snapshot_is_on_disk_when_the_replica_restarts() {
    let (mut primary, partition) = primary_holding(1, 64);
    let disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (mut replica, _) = awaiting_replica_on(&disk, node(1)).await;

    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;
    assert!(imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::Replica);

    let restarted = StoreManager::new_with_storage(node(2), Some(disk));
    restarted.recover().unwrap();
    assert!(restarted.db_get(ENTITY, ID).is_some());
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_persisted_is_refused_and_not_imported() {
    let (mut primary, partition) = primary_holding(1, 64);
    let faulty = FaultyBackend::shared();
    let disk: Arc<dyn StorageBackend> = faulty.clone();
    let (mut replica, _) = awaiting_replica_on(&disk, node(1)).await;
    faulty.fail_writes(true);

    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    let sent = replica.transport.take_sent();
    assert_eq!(failures_in(&sent), 1);
    assert_eq!(completions_in(&sent), 0);
    assert!(!imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::AwaitingSnapshot);
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

    let chunk = SnapshotSender::new(
        partition,
        primary.stores.export_partition(partition).unwrap(),
        1,
    )
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

    let chunk = SnapshotSender::new(
        partition,
        other.stores.export_partition(partition).unwrap(),
        1,
    )
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

    let chunk = SnapshotSender::new(
        partition,
        primary.stores.export_partition(partition).unwrap(),
        1,
    )
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
            primary_since: Epoch::new(epoch),
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

    replica.heartbeat.receive_heartbeat(
        node(1),
        &crate::cluster::protocol::Heartbeat::create(node(1), 0),
        0,
    );
    replica.apply_dead_nodes(&[node(1)]);
    replica
        .apply_raft_partition_map(map_for(node(2), &[], 2))
        .await;
    assert_eq!(replica.role(partition), ReplicaRole::Primary);
    deliver(&primary, &mut replica).await;

    assert!(!imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::Primary);
}

#[tokio::test]
async fn a_replica_promoted_during_a_multi_chunk_transfer_keeps_it_and_takes_the_role_on_import() {
    let (mut primary, _) = primary_holding(1, 1024 * 1024);
    let (mut replica, partition) = awaiting_replica_from_map().await;
    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;
    assert!(replica.snapshot_in_progress(partition));

    let swapped = map_for(node(2), &[node(1)], 2);
    replica.apply_raft_partition_map(swapped.clone()).await;
    assert_eq!(replica.role(partition), ReplicaRole::AwaitingSnapshot);
    assert!(replica.snapshot_in_progress(partition));
    primary.apply_raft_partition_map(swapped).await;

    for now in 1..40 {
        primary.drive_snapshots(now).await;
        deliver(&primary, &mut replica).await;
        deliver(&replica, &mut primary).await;
    }

    assert!(imported(&replica));
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

    assert!(!replica.snapshot_transfer_running(partition));
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
    let mut sender = SnapshotSender::new(
        partition,
        primary.stores.export_partition(partition).unwrap(),
        1,
    )
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

#[tokio::test]
async fn a_late_chunk_of_an_abandoned_transfer_does_not_abort_the_retry() {
    let old = export_of(1024, 256, 1);
    let new = export_of(1024, 256, 2);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    replica
        .drive_snapshots(START + SNAPSHOT_STALL_TIMEOUT_MS)
        .await;
    replica.transport.take_sent();

    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(old[2].clone()));
    for chunk in &new {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    replica.process_messages().await;

    assert_eq!(failures_in(&replica.transport.take_sent()), 0);
    assert!(imported(&replica));
}

#[tokio::test]
async fn a_snapshot_finishing_after_the_node_became_a_replica_is_not_imported() {
    let (mut primary, _) = primary_holding(1, 200 * 1024);
    let (mut replica, partition) = awaiting_replica(node(1)).await;
    deliver(&replica, &mut primary).await;

    replica.become_replica(partition, Epoch::new(1), 0);
    deliver(&primary, &mut replica).await;

    assert!(!imported(&replica));
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

#[tokio::test]
async fn a_migration_request_is_kept_when_the_map_names_another_primary() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    let mut target = controller(2);
    assign(&mut target, node(3));
    target.become_replica(partition, Epoch::new(1), 0);
    target
        .start_partition_migration(partition, node(1), node(2), Epoch::new(2))
        .await;
    deliver(&target, &mut primary).await;

    target.drive_snapshots(START).await;
    for _ in 0..8 {
        deliver(&primary, &mut target).await;
        primary.drive_snapshots(START).await;
    }

    assert!(imported(&target));
}

#[tokio::test]
async fn a_write_made_during_a_transfer_reaches_the_replica_after_the_import() {
    let (mut primary, partition) = primary_holding(1, 200 * 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    deliver(&replica, &mut primary).await;
    let chunks = chunks_in(&primary.transport.take_sent());
    let (last, earlier) = chunks.split_last().unwrap();
    assert!(!earlier.is_empty());
    for chunk in earlier {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }

    primary.db_update(ENTITY, ID, &record(16), 1).await.unwrap();
    deliver(&primary, &mut replica).await;
    replica
        .transport
        .inject_message(node(1), ClusterMessage::SnapshotChunk(last.clone()));
    replica.process_messages().await;
    assert!(imported(&replica));

    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

fn counter_value(n: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "v": n})).unwrap()
}

fn counter(ctrl: &NodeController<MockTransport>) -> Option<u64> {
    let found = ctrl.db_get(ENTITY, ID)?;
    let json: serde_json::Value = serde_json::from_slice(&found.data).unwrap();
    json["v"].as_u64()
}

async fn write_counter(primary: &mut NodeController<MockTransport>, n: u64) {
    primary
        .db_update(ENTITY, ID, &counter_value(n), n + 1)
        .await
        .unwrap();
}

async fn start_transfer(
    primary: &mut NodeController<MockTransport>,
    replica: &mut NodeController<MockTransport>,
) -> Vec<SnapshotChunk> {
    deliver(replica, primary).await;
    let chunks = chunks_in(&primary.transport.take_sent());
    assert!(!chunks.is_empty());
    chunks
}

async fn finish_transfer(replica: &mut NodeController<MockTransport>, chunks: &[SnapshotChunk]) {
    for chunk in chunks {
        replica
            .transport
            .inject_message(node(1), ClusterMessage::SnapshotChunk(chunk.clone()));
    }
    replica.process_messages().await;
}

async fn exchange(
    primary: &mut NodeController<MockTransport>,
    replica: &mut NodeController<MockTransport>,
) {
    for _ in 0..6 {
        deliver(replica, primary).await;
        deliver(primary, replica).await;
    }
}

#[tokio::test]
async fn several_writes_and_a_delete_during_a_transfer_reach_the_replica() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    write_counter(&mut primary, 2).await;
    deliver(&primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;
    assert_eq!(counter(&replica), Some(2));
    assert_eq!(replica.sequence(partition), Some(2));

    let (mut primary, _) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    primary.db_delete(ENTITY, ID).await.unwrap();
    deliver(&primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;
    assert!(replica.db_get(ENTITY, ID).is_none());
}

#[tokio::test]
async fn a_log_that_no_longer_reaches_the_replica_leads_to_a_new_snapshot() {
    let (mut primary, partition) = primary_holding(1, 1024);
    primary.write_log = PartitionWriteLog::with_max_entries(2);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    for n in 1..=5 {
        write_counter(&mut primary, n).await;
    }
    deliver(&primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(counter(&replica), Some(5));
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

#[tokio::test]
async fn an_epoch_bump_during_a_transfer_does_not_strand_the_replica() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    deliver(&primary, &mut replica).await;

    let bumped = map_for(node(1), &[node(2), node(3)], 2);
    primary.apply_raft_partition_map(bumped.clone()).await;
    replica.apply_raft_partition_map(bumped).await;
    write_counter(&mut primary, 2).await;
    deliver(&primary, &mut replica).await;

    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;
    for n in 3..=6 {
        write_counter(&mut primary, n).await;
        exchange(&mut primary, &mut replica).await;
    }

    assert_eq!(counter(&replica), Some(6));
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

#[tokio::test]
async fn a_lost_post_import_catchup_is_asked_for_again() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    deliver(&primary, &mut replica).await;
    replica.transport.take_sent();

    replica.transport.queue_full.store(true, Ordering::SeqCst);
    finish_transfer(&mut replica, &chunks).await;
    replica.transport.queue_full.store(false, Ordering::SeqCst);
    exchange(&mut primary, &mut replica).await;
    assert_eq!(replica.sequence(partition), Some(0));

    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), Some(1));
    assert_eq!(counter(&replica), Some(1));
}

#[tokio::test]
async fn a_catchup_too_large_for_one_frame_arrives_in_pieces() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    let large = record(100 * 1024);
    for n in 0..110 {
        primary.db_update(ENTITY, ID, &large, n + 1).await.unwrap();
    }
    primary.transport.take_sent();
    finish_transfer(&mut replica, &chunks).await;

    let mut responses = 0;
    for _ in 0..6 {
        deliver(&replica, &mut primary).await;
        for (_, message) in primary.transport.sent_messages() {
            if let ClusterMessage::CatchupResponse(response) = message {
                responses += 1;
                let bytes = response.to_bytes().unwrap().len();
                assert!(bytes <= 8 * 1024 * 1024, "response of {bytes} bytes");
            }
        }
        deliver(&primary, &mut replica).await;
    }

    assert!(responses >= 2);
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

#[tokio::test]
async fn a_late_catchup_response_does_not_demote_a_primary() {
    let mut ctrl = controller(2);
    let partition = assign(&mut ctrl, node(2));
    ctrl.become_primary(partition, Epoch::new(2));
    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        2,
        ENTITY.to_string(),
        format!("{ENTITY}/{ID}"),
        counter_value(1),
    );
    ctrl.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::create(partition, node(1), vec![write])),
    );
    ctrl.process_messages().await;

    assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
}

fn catchup_requests_in(sent: &[(NodeId, ClusterMessage)]) -> usize {
    sent.iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::CatchupRequest(_)))
        .count()
}

#[tokio::test]
async fn log_entries_above_the_primary_sequence_do_not_cause_another_snapshot() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let leftover = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        50,
        ENTITY.to_string(),
        format!("{ENTITY}/{ID}"),
        counter_value(1),
    );
    primary.write_log.append(partition, 50, leftover);
    primary
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    primary.transport.take_sent();

    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica
        .apply_raft_partition_map(map_for(node(1), &[node(2)], 2))
        .await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    replica.drive_snapshots(START).await;
    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    assert_eq!(requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

#[tokio::test]
async fn a_complete_catchup_response_ends_catching_up_under_live_writes() {
    let (mut primary, _) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    deliver(&primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    deliver(&replica, &mut primary).await;
    write_counter(&mut primary, 2).await;
    deliver(&primary, &mut replica).await;

    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(counter(&replica), Some(2));
}

#[tokio::test]
async fn catching_up_survives_an_assignment_change() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    deliver(&primary, &mut replica).await;
    replica.transport.take_sent();
    replica.transport.queue_full.store(true, Ordering::SeqCst);
    finish_transfer(&mut replica, &chunks).await;
    replica.transport.queue_full.store(false, Ordering::SeqCst);

    replica
        .apply_raft_partition_map(map_for(node(1), &[node(2), node(3)], 1))
        .await;
    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), Some(1));
    assert_eq!(counter(&replica), Some(1));
}

#[test]
fn the_catchup_limit_counts_everything_a_write_encodes() {
    let partition = data_partition(ENTITY, ID);
    let writes: Vec<ReplicationWrite> = (1..=200)
        .map(|sequence| {
            ReplicationWrite::new(
                partition,
                Operation::Insert,
                Epoch::new(1),
                sequence,
                ENTITY.to_string(),
                "i".repeat(100 * 1024),
                Vec::new(),
            )
        })
        .collect();

    let pieces = catchup_pieces(writes);

    assert!(pieces.len() >= 3);
    assert_eq!(pieces.iter().map(Vec::len).sum::<usize>(), 200);
    for piece in pieces {
        let response = CatchupResponse::create(partition, node(1), piece);
        assert!(response.to_bytes().unwrap().len() <= 8 * 1024 * 1024);
    }
}

async fn caught_up_replica(
    primary: &mut NodeController<MockTransport>,
) -> NodeController<MockTransport> {
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    exchange(primary, &mut replica).await;
    replica
}

#[tokio::test]
async fn a_buffered_write_reaches_the_store_when_its_gap_is_filled() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;

    write_counter(&mut primary, 1).await;
    write_counter(&mut primary, 2).await;
    write_counter(&mut primary, 3).await;
    let last_write = primary
        .transport
        .take_sent()
        .into_iter()
        .rfind(|(to, message)| *to == node(2) && matches!(message, ClusterMessage::Write(_)))
        .unwrap();
    replica.transport.inject_message(node(1), last_write.1);
    replica.process_messages().await;
    assert_eq!(replica.sequence(partition), Some(0));

    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), Some(3));
    assert_eq!(counter(&replica), Some(3));
    assert!(replica.write_log.has_sequence(partition, 3));
}

#[tokio::test]
async fn a_gap_larger_than_one_catchup_response_is_filled_without_further_writes() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;

    let large = record(100 * 1024);
    for n in 0..110 {
        primary.db_update(ENTITY, ID, &large, n + 1).await.unwrap();
    }
    let last_write = primary
        .transport
        .take_sent()
        .into_iter()
        .rfind(|(to, message)| *to == node(2) && matches!(message, ClusterMessage::Write(_)))
        .unwrap();
    replica.transport.inject_message(node(1), last_write.1);
    replica.process_messages().await;

    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

#[tokio::test]
async fn a_catchup_cut_short_by_one_large_write_is_still_complete() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    primary
        .db_update(ENTITY, ID, &record(3 * 1024 * 1024), 1)
        .await
        .unwrap();
    primary
        .db_update(ENTITY, ID, &record(6 * 1024 * 1024), 2)
        .await
        .unwrap();
    primary.transport.take_sent();
    finish_transfer(&mut replica, &chunks).await;

    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

#[tokio::test]
async fn duplicate_gap_acks_do_not_start_more_catchup_requests() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;
    primary.transport.take_sent();
    replica.transport.take_sent();
    let large = record(100 * 1024);
    for n in 0..200 {
        primary.db_update(ENTITY, ID, &large, n + 1).await.unwrap();
    }
    let live: Vec<ClusterMessage> = primary
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, message)| *to == node(2) && matches!(message, ClusterMessage::Write(_)))
        .map(|(_, message)| message)
        .collect();
    for message in live.into_iter().rev().take(5).rev() {
        replica.transport.inject_message(node(1), message);
    }
    replica.process_messages().await;
    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

#[tokio::test]
async fn catching_up_lasts_until_the_primary_says_it_has_sent_everything() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    primary.transport.take_sent();
    finish_transfer(&mut replica, &chunks).await;
    deliver(&replica, &mut primary).await;

    let sent = primary.transport.take_sent();
    let responses: Vec<ClusterMessage> = sent
        .into_iter()
        .filter(|(to, message)| {
            *to == node(2) && matches!(message, ClusterMessage::CatchupResponse(_))
        })
        .map(|(_, message)| message)
        .collect();
    assert_eq!(responses.len(), 2);
    replica
        .transport
        .inject_message(node(1), responses[0].clone());
    replica.process_messages().await;
    replica.transport.take_sent();

    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 1);

    replica
        .transport
        .inject_message(node(1), responses[1].clone());
    replica.process_messages().await;
    replica
        .drive_snapshots(START + 2 * SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    assert_eq!(catchup_requests_in(&replica.transport.sent_messages()), 1);

    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;
    replica
        .drive_snapshots(START + 3 * SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(replica.sequence(partition), Some(1));
}

fn blob_of(ctrl: &NodeController<MockTransport>) -> usize {
    let found = ctrl.db_get(ENTITY, ID).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&found.data).unwrap();
    json["blob"].as_str().unwrap().len()
}

fn catchup_responses(primary: &NodeController<MockTransport>) -> Vec<ClusterMessage> {
    primary
        .transport
        .take_sent()
        .into_iter()
        .filter(|(to, message)| {
            *to == node(2) && matches!(message, ClusterMessage::CatchupResponse(_))
        })
        .map(|(_, message)| message)
        .collect()
}

#[tokio::test]
async fn a_lost_catchup_piece_does_not_leave_a_buffered_write_unstored() {
    let (first, second) = (3 * 1024 * 1024, 6 * 1024 * 1024);
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;
    primary.transport.take_sent();
    replica.transport.take_sent();
    primary
        .db_update(ENTITY, ID, &record(first), 1)
        .await
        .unwrap();
    primary
        .db_update(ENTITY, ID, &record(second), 2)
        .await
        .unwrap();
    let live_second = primary
        .transport
        .take_sent()
        .into_iter()
        .rfind(|(to, message)| *to == node(2) && matches!(message, ClusterMessage::Write(_)))
        .unwrap();
    replica.transport.inject_message(node(1), live_second.1);
    replica.process_messages().await;
    deliver(&replica, &mut primary).await;
    let pieces = catchup_responses(&primary);
    assert_eq!(pieces.len(), 2);

    replica.transport.inject_message(node(1), pieces[0].clone());
    replica.process_messages().await;
    write_counter(&mut primary, 3).await;
    exchange(&mut primary, &mut replica).await;
    assert_eq!(replica.sequence(partition), primary.sequence(partition));

    assert!(replica.write_log.has_sequence(partition, 2));
}

#[tokio::test]
async fn a_partial_duplicate_catchup_does_not_roll_a_record_back() {
    let (first, second) = (3 * 1024 * 1024, 6 * 1024 * 1024);
    let (mut primary, _) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;
    primary.transport.take_sent();
    replica.transport.take_sent();
    primary
        .db_update(ENTITY, ID, &record(first), 1)
        .await
        .unwrap();
    primary
        .db_update(ENTITY, ID, &record(second), 2)
        .await
        .unwrap();
    let live_second = primary
        .transport
        .take_sent()
        .into_iter()
        .rfind(|(to, message)| *to == node(2) && matches!(message, ClusterMessage::Write(_)))
        .unwrap();
    replica.transport.inject_message(node(1), live_second.1);
    replica.process_messages().await;
    deliver(&replica, &mut primary).await;
    let pieces = catchup_responses(&primary);
    for piece in &pieces {
        replica.transport.inject_message(node(1), piece.clone());
    }
    replica.process_messages().await;
    assert_eq!(blob_of(&replica), second);

    replica.transport.inject_message(node(1), pieces[0].clone());
    replica.process_messages().await;

    assert_eq!(blob_of(&replica), second);
}

#[tokio::test]
async fn a_catchup_write_that_cannot_be_stored_leads_to_a_new_snapshot() {
    use crate::cluster::test_support::FaultyBackend;
    let (mut primary, partition) = primary_holding(1, 1024);
    write_counter(&mut primary, 1).await;
    let write = primary.write_log.get_range(partition, 1, 1);
    assert_eq!(write.len(), 1);

    let backend = FaultyBackend::shared();
    let storage: Arc<dyn mqdb_core::storage::StorageBackend> = backend.clone();
    let mut replica = NodeController::new_with_storage(
        node(2),
        MockTransport::new(node(2)),
        TransportConfig::default(),
        Some(storage),
        flume::unbounded().0,
        flume::unbounded().0,
    );
    assign(&mut replica, node(1));
    replica.become_replica(partition, Epoch::new(1), 0);

    backend.fail_writes(true);
    replica.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::create(partition, node(1), write)),
    );
    replica.process_messages().await;

    assert_eq!(replica.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);
    assert!(!replica.write_log.has_sequence(partition, 1));
}

#[tokio::test]
async fn an_epoch_bump_during_a_transfer_is_repaired_without_a_later_write() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    deliver(&primary, &mut replica).await;

    let bumped = map_for(node(1), &[node(2), node(3)], 2);
    primary.apply_raft_partition_map(bumped.clone()).await;
    replica.apply_raft_partition_map(bumped).await;

    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(counter(&replica), Some(1));
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}

fn is_end_marker(message: &ClusterMessage) -> bool {
    matches!(message, ClusterMessage::CatchupResponse(response) if response.writes.is_empty())
}

async fn replica_catching_up_with_writes_pending(
    primary: &mut NodeController<MockTransport>,
) -> NodeController<MockTransport> {
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let chunks = start_transfer(primary, &mut replica).await;
    for n in 1..=5 {
        write_counter(primary, n).await;
    }
    primary.transport.take_sent();
    finish_transfer(&mut replica, &chunks).await;
    replica
}

#[tokio::test]
async fn an_end_marker_does_not_finish_a_catchup_whose_piece_was_dropped() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = replica_catching_up_with_writes_pending(&mut primary).await;
    deliver(&replica, &mut primary).await;
    let marker = catchup_responses(&primary)
        .into_iter()
        .find(is_end_marker)
        .unwrap();

    replica.transport.inject_message(node(1), marker);
    replica.process_messages().await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), Some(5));
    assert_eq!(counter(&replica), Some(5));
}

#[tokio::test]
async fn an_end_marker_after_a_partial_catchup_asks_for_the_rest() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    let large = record(5 * 1024 * 1024);
    for n in 0..3 {
        primary.db_update(ENTITY, ID, &large, n + 1).await.unwrap();
    }
    primary.transport.take_sent();
    finish_transfer(&mut replica, &chunks).await;
    deliver(&replica, &mut primary).await;
    let responses = catchup_responses(&primary);
    assert_eq!(responses.len(), 4);

    replica
        .transport
        .inject_message(node(1), responses[0].clone());
    replica
        .transport
        .inject_message(node(1), responses[3].clone());
    replica.process_messages().await;

    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn a_primary_with_nothing_to_send_for_a_replica_behind_it_leads_to_a_snapshot() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = replica_catching_up_with_writes_pending(&mut primary).await;
    primary.write_log = PartitionWriteLog::with_max_entries(10);
    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;

    assert_eq!(replica.role(partition), ReplicaRole::AwaitingSnapshot);
    assert_eq!(requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn an_end_marker_from_a_node_that_is_not_the_primary_does_not_finish_a_catchup() {
    let (mut primary, _) = primary_holding(1, 1024);
    let mut replica = replica_catching_up_with_writes_pending(&mut primary).await;
    replica.transport.take_sent();

    let partition = data_partition(ENTITY, ID);
    replica.transport.inject_message(
        node(3),
        ClusterMessage::CatchupResponse(CatchupResponse::empty(partition, node(3))),
    );
    replica.process_messages().await;
    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;

    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 1);
}

#[tokio::test]
async fn a_bounded_catchup_request_past_the_log_is_answered_empty() {
    let (mut primary, partition) = primary_holding(1, 1024);
    primary.write_log = PartitionWriteLog::with_max_entries(2);
    for n in 1..=5 {
        write_counter(&mut primary, n).await;
    }
    primary.transport.take_sent();
    primary.transport.inject_message(
        node(2),
        ClusterMessage::CatchupRequest(CatchupRequest::create(partition, 1, 5, node(2))),
    );
    primary.process_messages().await;

    let responses = catchup_responses(&primary);
    assert_eq!(responses.len(), 1);
    assert!(is_end_marker(&responses[0]));
}

#[test]
fn an_end_marker_carries_its_coverage_and_an_old_response_has_none() {
    let partition = data_partition(ENTITY, ID);
    let marker = CatchupResponse::end_marker(partition, node(1), 42, 7);
    let decoded = CatchupResponse::from_bytes(&marker.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded.covers_through, Some(42));
    assert_eq!(decoded.answers_from, Some(7));
    assert!(decoded.writes.is_empty());

    let old = CatchupResponse::empty(partition, node(1));
    let decoded = CatchupResponse::from_bytes(&old.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded.covers_through, None);
    assert_eq!(decoded.answers_from, None);
}

#[tokio::test]
async fn an_empty_response_from_an_old_primary_still_finishes_a_catchup() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = replica_catching_up_with_writes_pending(&mut primary).await;
    replica.transport.take_sent();

    replica.transport.inject_message(
        node(1),
        ClusterMessage::CatchupResponse(CatchupResponse::empty(partition, node(1))),
    );
    replica.process_messages().await;
    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;

    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
}

#[tokio::test]
async fn a_marker_from_an_earlier_exchange_does_not_finish_a_later_catchup() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    replica.drive_snapshots(START).await;
    let first_chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 1).await;
    primary.transport.take_sent();
    finish_transfer(&mut replica, &first_chunks).await;
    deliver(&replica, &mut primary).await;
    let exchange_a = catchup_responses(&primary);
    assert_eq!(exchange_a.len(), 2);

    let bumped = map_for(node(1), &[node(2), node(3)], 2);
    primary.apply_raft_partition_map(bumped.clone()).await;
    replica.apply_raft_partition_map(bumped).await;
    primary.transport.take_sent();
    replica.transport.take_sent();
    replica
        .transport
        .inject_message(node(1), exchange_a[0].clone());
    replica.process_messages().await;
    assert_eq!(replica.role(partition), ReplicaRole::AwaitingSnapshot);

    let second_chunks = start_transfer(&mut primary, &mut replica).await;
    write_counter(&mut primary, 2).await;
    primary.transport.take_sent();
    finish_transfer(&mut replica, &second_chunks).await;
    replica.transport.take_sent();

    replica
        .transport
        .inject_message(node(1), exchange_a[1].clone());
    replica.process_messages().await;
    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.sequence(partition), primary.sequence(partition));
    assert_eq!(counter(&replica), Some(2));
}

#[tokio::test]
async fn a_replica_ahead_of_its_primary_still_finishes_catching_up() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = replica_catching_up_with_writes_pending(&mut primary).await;
    deliver(&replica, &mut primary).await;
    primary.transport.take_sent();
    replica
        .replicas
        .get_mut(&partition.get())
        .unwrap()
        .set_sequence(9);
    replica
        .drive_snapshots(START + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    deliver(&replica, &mut primary).await;
    deliver(&primary, &mut replica).await;
    replica.transport.take_sent();

    replica
        .drive_snapshots(START + 2 * SNAPSHOT_RETRY_BACKOFF_MS)
        .await;

    assert_eq!(catchup_requests_in(&replica.transport.take_sent()), 0);
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert_eq!(replica.sequence(partition), Some(9));
}

#[tokio::test]
async fn a_wildcard_write_with_no_sequence_is_still_stored_by_a_replica() {
    let partition = data_partition(ENTITY, ID);
    let mut sender = controller(1);
    let writes = sender
        .subscribe_wildcard_broadcast("sensors/+/temp", "client-1", partition, 1)
        .unwrap();
    let write = writes
        .into_iter()
        .find(|write| write.partition == partition)
        .unwrap();
    let mut replica = controller(2);
    replica.become_replica(partition, Epoch::ZERO, 0);

    replica
        .transport
        .inject_message(node(1), ClusterMessage::Write(write));
    replica.process_messages().await;

    assert_eq!(
        replica
            .stores
            .wildcards
            .match_topic("sensors/room/temp")
            .len(),
        1
    );
}

#[tokio::test]
async fn a_write_from_a_primary_restarted_at_a_lower_sequence_is_still_stored() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let mut replica = caught_up_replica(&mut primary).await;
    primary.transport.take_sent();
    replica.transport.take_sent();
    write_counter(&mut primary, 1).await;
    write_counter(&mut primary, 2).await;
    deliver(&primary, &mut replica).await;
    assert_eq!(replica.sequence(partition), Some(2));
    assert_eq!(counter(&replica), Some(2));

    let (mut restarted, _) = primary_holding(1, 1024);
    write_counter(&mut restarted, 9).await;
    assert_eq!(restarted.sequence(partition), Some(1));
    deliver(&restarted, &mut replica).await;

    assert_eq!(counter(&replica), Some(9));
}

#[tokio::test]
async fn a_primary_restarted_while_a_replica_catches_up_does_not_lose_acked_writes() {
    let (mut primary, partition) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    write_counter(&mut primary, 1).await;
    write_counter(&mut primary, 2).await;
    primary.transport.take_sent();
    let chunks = start_transfer(&mut primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    assert_eq!(replica.sequence(partition), Some(2));
    assert!(replica.catchup_asked_from(partition).is_some());

    let (mut restarted, _) = primary_holding(1, 1024);
    write_counter(&mut restarted, 77).await;
    assert_eq!(restarted.sequence(partition), Some(1));
    deliver(&restarted, &mut replica).await;

    assert_eq!(counter(&replica), Some(77));
}
