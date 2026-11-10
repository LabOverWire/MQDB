// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::data_partition;
use crate::cluster::db::{SchemaStore, UniqueReserveParams, unique_key, unique_partition};
use crate::cluster::entity;
use crate::cluster::node_controller::snapshot::SNAPSHOT_RETRY_BACKOFF_MS;
use crate::cluster::protocol::{Operation, ReplicationWrite};
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

fn assign(ctrl: &mut NodeController<MockTransport>, primary: NodeId) -> PartitionId {
    let mut map = PartitionMap::default();
    map.set(
        data_partition(ENTITY, ID),
        PartitionAssignment {
            primary: Some(primary),
            replicas: vec![node(2)],
            epoch: Epoch::new(1),
            primary_since: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);
    data_partition(ENTITY, ID)
}

fn record(len: usize) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "blob": "x".repeat(len)})).unwrap()
}

fn seed(ctrl: &NodeController<MockTransport>, data: &[u8]) {
    let write = ctrl
        .stores
        .db_create_write(ENTITY, ID, data, 0)
        .map(|(_, write)| write)
        .unwrap();
    ctrl.stores.apply_write(&write).unwrap();
}

fn primary_holding(id: u16, len: usize) -> (NodeController<MockTransport>, PartitionId) {
    let mut primary = controller(id);
    let partition = assign(&mut primary, node(id));
    primary.become_primary(partition, Epoch::new(1));
    seed(&primary, &record(len));
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

async fn start_transfer(
    primary: &mut NodeController<MockTransport>,
    replica: &mut NodeController<MockTransport>,
) -> Vec<SnapshotChunk> {
    deliver(replica, primary).await;
    let chunks: Vec<SnapshotChunk> = primary
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::SnapshotChunk(chunk) => Some(chunk),
            _ => None,
        })
        .collect();
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
async fn st4_import_keeps_a_record_the_primary_no_longer_has() {
    let mut primary = controller(1);
    let partition = assign(&mut primary, node(1));
    primary.become_primary(partition, Epoch::new(1));
    let (mut replica, _) = awaiting_replica(node(1)).await;
    seed(&replica, &record(16));
    let chunks = start_transfer(&mut primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert!(primary.db_get(ENTITY, ID).is_none());
    assert!(
        replica.db_get(ENTITY, ID).is_none(),
        "ghost record survives import"
    );
}

#[tokio::test]
async fn st5a_schema_update_during_transfer_is_undone_by_import() {
    let (mut primary, _) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let first = primary
        .schema_register(ENTITY, b"{\"fields\":[]}")
        .await
        .unwrap();
    replica
        .stores
        .db_schema
        .apply_replicated(Operation::Insert, ENTITY, &SchemaStore::serialize(&first))
        .unwrap();
    let chunks = start_transfer(&mut primary, &mut replica).await;
    let updated = primary
        .schema_update(ENTITY, b"{\"fields\":[\"new\"]}")
        .await
        .unwrap();
    let write = ReplicationWrite::new(
        PartitionId::ZERO,
        Operation::Update,
        Epoch::ZERO,
        0,
        entity::DB_SCHEMA.to_string(),
        ENTITY.to_string(),
        SchemaStore::serialize(&updated),
    );
    replica
        .transport
        .inject_message(node(1), ClusterMessage::WriteRequest(write));
    replica.process_messages().await;
    assert_eq!(
        replica.stores.db_schema.get(ENTITY).unwrap().data,
        updated.data
    );
    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;
    assert_eq!(
        String::from_utf8_lossy(&replica.stores.db_schema.get(ENTITY).unwrap().data),
        String::from_utf8_lossy(&updated.data),
    );
}

#[tokio::test]
async fn st5b_schema_delete_during_transfer_is_undone_by_import() {
    let (mut primary, _) = primary_holding(1, 1024);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    primary
        .schema_register("gone", b"{\"fields\":[]}")
        .await
        .unwrap();
    let chunks = start_transfer(&mut primary, &mut replica).await;
    primary
        .stores
        .db_schema
        .apply_replicated(Operation::Delete, "gone", &[])
        .unwrap();
    let write = ReplicationWrite::new(
        PartitionId::ZERO,
        Operation::Delete,
        Epoch::ZERO,
        0,
        entity::DB_SCHEMA.to_string(),
        "gone".to_string(),
        Vec::new(),
    );
    replica
        .transport
        .inject_message(node(1), ClusterMessage::WriteRequest(write));
    replica.process_messages().await;
    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;
    assert!(primary.stores.db_schema.get("gone").is_none());
    assert!(
        replica.stores.db_schema.get("gone").is_none(),
        "schema resurrected"
    );
}

#[tokio::test]
async fn st5c_unique_release_during_transfer_is_undone_by_import() {
    let mut primary = controller(1);
    let value = b"a@b";
    let up = unique_partition(ENTITY, "email", value);
    primary.become_primary(up, Epoch::new(1));
    primary.stores.db_unique.reserve(
        &UniqueReserveParams {
            entity: ENTITY,
            field: "email",
            value,
            record_id: "r1",
            request_id: "q1",
            data_partition: data_partition(ENTITY, "r1"),
            ttl_ms: 3_600_000,
        },
        1,
    );
    let reservation = primary
        .stores
        .db_unique
        .get(ENTITY, "email", value)
        .unwrap();
    let key = unique_key(&reservation);
    let mut replica = controller(2);
    replica
        .become_replica_with_snapshot(up, Epoch::new(1), node(1))
        .await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    let release = ReplicationWrite::new(
        up,
        Operation::Delete,
        Epoch::ZERO,
        0,
        entity::DB_UNIQUE.to_string(),
        key,
        Vec::new(),
    );
    primary.stores.apply_write(&release).unwrap();
    replica.transport.inject_message(
        node(1),
        ClusterMessage::UniqueReplicate {
            request_id: 0,
            write: release,
        },
    );
    replica.process_messages().await;
    assert!(
        replica
            .stores
            .db_unique
            .get(ENTITY, "email", value)
            .is_none()
    );
    finish_transfer(&mut replica, &chunks).await;
    assert!(
        replica
            .stores
            .db_unique
            .get(ENTITY, "email", value)
            .is_none(),
        "released reservation resurrected"
    );
}

const VALUE: &[u8] = b"a@b";

fn reserve(ctrl: &NodeController<MockTransport>, record_id: &str) {
    ctrl.stores.db_unique.reserve(
        &UniqueReserveParams {
            entity: ENTITY,
            field: "email",
            value: VALUE,
            record_id,
            request_id: "q1",
            data_partition: data_partition(ENTITY, record_id),
            ttl_ms: 3_600_000,
        },
        1,
    );
}

async fn unique_transfer() -> (
    NodeController<MockTransport>,
    NodeController<MockTransport>,
    PartitionId,
) {
    let mut primary = controller(1);
    let up = unique_partition(ENTITY, "email", VALUE);
    primary.become_primary(up, Epoch::new(1));
    let mut replica = controller(2);
    replica
        .become_replica_with_snapshot(up, Epoch::new(1), node(1))
        .await;
    (primary, replica, up)
}

#[tokio::test]
async fn a_unique_claim_the_source_lacks_is_kept_by_import() {
    let mut primary = controller(1);
    let up = unique_partition(ENTITY, "email", VALUE);
    primary.become_primary(up, Epoch::new(1));
    let coordinator = controller(3);
    reserve(&coordinator, "r1");
    let claim = coordinator
        .stores
        .db_unique
        .get(ENTITY, "email", VALUE)
        .unwrap();
    let mut replica = controller(2);
    replica.transport.inject_message(
        node(3),
        ClusterMessage::UniqueReplicate {
            request_id: 0,
            write: ReplicationWrite::new(
                up,
                Operation::Insert,
                Epoch::ZERO,
                0,
                entity::DB_UNIQUE.to_string(),
                unique_key(&claim),
                crate::cluster::db::UniqueStore::serialize(&claim),
            ),
        },
    );
    replica.process_messages().await;
    replica
        .become_replica_with_snapshot(up, Epoch::new(1), node(1))
        .await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    finish_transfer(&mut replica, &chunks).await;
    assert_eq!(replica.role(up), ReplicaRole::Replica);
    assert!(
        replica
            .stores
            .db_unique
            .get(ENTITY, "email", VALUE)
            .is_some(),
        "a claim acked through this node was dropped"
    );
}

#[tokio::test]
async fn a_unique_reserve_during_transfer_survives_import() {
    let (mut primary, mut replica, up) = unique_transfer().await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    reserve(&primary, "r1");
    let reservation = primary
        .stores
        .db_unique
        .get(ENTITY, "email", VALUE)
        .unwrap();
    let write = ReplicationWrite::new(
        up,
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::DB_UNIQUE.to_string(),
        unique_key(&reservation),
        crate::cluster::db::UniqueStore::serialize(&reservation),
    );
    replica.transport.inject_message(
        node(1),
        ClusterMessage::UniqueReplicate {
            request_id: 0,
            write,
        },
    );
    replica.process_messages().await;
    finish_transfer(&mut replica, &chunks).await;
    assert_eq!(
        replica
            .stores
            .db_unique
            .get(ENTITY, "email", VALUE)
            .map(|claim| claim.record_id_str().to_string()),
        Some("r1".to_string())
    );
}

fn schema_write(from: &SchemaStore, name: &str, operation: Operation) -> ClusterMessage {
    let data = from
        .get(name)
        .map(|schema| SchemaStore::serialize(&schema))
        .unwrap_or_default();
    ClusterMessage::WriteRequest(ReplicationWrite::new(
        PartitionId::ZERO,
        operation,
        Epoch::ZERO,
        0,
        entity::DB_SCHEMA.to_string(),
        name.to_string(),
        data,
    ))
}

async fn retry_transfer_if_refused(
    primary: &mut NodeController<MockTransport>,
    replica: &mut NodeController<MockTransport>,
    partition: PartitionId,
) {
    if replica.role(partition) != ReplicaRole::AwaitingSnapshot {
        return;
    }
    replica.transport.take_sent();
    replica.drive_snapshots(1_000).await;
    replica
        .drive_snapshots(1_000 + SNAPSHOT_RETRY_BACKOFF_MS)
        .await;
    let chunks = start_transfer(primary, replica).await;
    finish_transfer(replica, &chunks).await;
}

#[tokio::test]
async fn a_global_write_while_the_request_cannot_be_sent_survives_import() {
    let (mut primary, partition) = primary_holding(1, 16);
    let first = primary
        .schema_register(ENTITY, b"{\"fields\":[]}")
        .await
        .unwrap();
    primary.transport.take_sent();
    let elsewhere = controller(3);
    elsewhere
        .stores
        .db_schema
        .apply_replicated(Operation::Insert, ENTITY, &SchemaStore::serialize(&first))
        .unwrap();
    let updated = elsewhere
        .stores
        .db_schema
        .update(ENTITY, b"{\"fields\":[\"new\"]}")
        .unwrap();

    let mut replica = controller(2);
    assign(&mut replica, node(1));
    replica
        .stores
        .db_schema
        .apply_replicated(Operation::Insert, ENTITY, &SchemaStore::serialize(&first))
        .unwrap();
    replica
        .transport
        .refuse_sends
        .store(true, std::sync::atomic::Ordering::SeqCst);
    replica
        .become_replica_with_snapshot(partition, Epoch::new(1), node(1))
        .await;
    replica.transport.inject_message(
        node(3),
        schema_write(&elsewhere.stores.db_schema, ENTITY, Operation::Update),
    );
    replica.process_messages().await;
    replica
        .transport
        .refuse_sends
        .store(false, std::sync::atomic::Ordering::SeqCst);
    retry_transfer_if_refused(&mut primary, &mut replica, partition).await;

    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert_eq!(
        String::from_utf8_lossy(&replica.stores.db_schema.get(ENTITY).unwrap().data),
        String::from_utf8_lossy(&updated.data),
    );
}

fn v(n: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"id": ID, "v": n})).unwrap()
}

fn val(ctrl: &NodeController<MockTransport>) -> Option<u64> {
    let found = ctrl.db_get(ENTITY, ID)?;
    let json: serde_json::Value = serde_json::from_slice(&found.data).unwrap();
    json["v"].as_u64()
}

async fn upsert(primary: &mut NodeController<MockTransport>, n: u64) {
    primary.db_update(ENTITY, ID, &v(n), n + 1).await.unwrap();
}

#[tokio::test]
async fn a_write_refused_while_awaiting_is_caught_up_after_import() {
    let (mut primary, partition) = primary_holding(1, 16);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    upsert(&mut primary, 7).await;
    deliver(&primary, &mut replica).await;
    assert_eq!(val(&replica), None);
    let refused = replica.transport.take_sent().into_iter().any(|(_, message)| {
        matches!(message, ClusterMessage::Ack(ack) if ack.status() == Some(crate::cluster::protocol::AckStatus::NotReplica))
    });
    assert!(refused, "the write was not refused while awaiting");

    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert_eq!(val(&replica), Some(7));
}

#[tokio::test]
async fn a_catchup_past_the_evicted_log_falls_back_to_a_new_snapshot() {
    let (mut primary, partition) = primary_holding(1, 16);
    primary.write_log = crate::cluster::write_log::PartitionWriteLog::with_max_entries(2);
    let (mut replica, _) = awaiting_replica(node(1)).await;
    let chunks = start_transfer(&mut primary, &mut replica).await;
    for n in 1..=5 {
        upsert(&mut primary, n).await;
    }
    primary.transport.take_sent();
    assert_eq!(primary.write_log.oldest_sequence(partition), Some(4));

    finish_transfer(&mut replica, &chunks).await;
    exchange(&mut primary, &mut replica).await;

    assert_eq!(replica.role(partition), ReplicaRole::Replica);
    assert_eq!(val(&replica), Some(5));
    assert_eq!(replica.sequence(partition), primary.sequence(partition));
}
