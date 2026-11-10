// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::db::{
    ClusterConstraint, FkValidationRequest, OnDeleteAction, SchemaStore, UniqueReserveParams,
    data_partition,
};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::session::session_partition;
use crate::cluster::store_manager::{StoreApplyError, StoreManager};
use crate::cluster::test_support::FaultyBackend;
use crate::cluster::{Epoch, NodeId, PartitionId, entity};
use mqdb_core::storage::{MemoryBackend, StorageBackend};
use std::collections::BTreeMap;
use std::sync::Arc;

const CLIENT: &str = "client-287";
const EXACT_TOPIC: &str = "sensors/temp";
const WILDCARD_TOPIC: &str = "sensors/+/hum";
const ENTITY: &str = "notes";
const RECORD: &str = "rec-1";
const UNIQUE_VALUE: &[u8] = b"a@example.com";
const IDEMPOTENCY_KEY: &str = "idem-1";
const FK_REQUEST: &str = "fk-req-1";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn with_storage(id: u16, backend: &Arc<dyn StorageBackend>) -> StoreManager {
    StoreManager::new_with_storage(node(id), Some(Arc::clone(backend)))
}

fn persist(store: &StoreManager, write: ReplicationWrite) {
    store.persist_writes_batch(&[write]).unwrap();
}

fn written_by_normal_paths(backend: &Arc<dyn StorageBackend>) -> StoreManager {
    let store = with_storage(1, backend);
    let session_part = session_partition(CLIENT);

    persist(&store, store.create_session_replicated(CLIENT).unwrap().1);
    persist(
        &store,
        store
            .start_qos2_inbound_replicated(CLIENT, 7, "t/q", b"q", 1)
            .unwrap()
            .1,
    );
    persist(
        &store,
        store.add_subscription_replicated(CLIENT, EXACT_TOPIC, 1).1,
    );
    persist(
        &store,
        store
            .add_subscription_replicated(CLIENT, WILDCARD_TOPIC, 1)
            .1,
    );
    store
        .subscribe_wildcard_replicated(
            WILDCARD_TOPIC,
            CLIENT,
            session_part,
            1,
            crate::cluster::SubscriptionType::Mqtt,
        )
        .unwrap();
    store
        .topics
        .subscribe(EXACT_TOPIC, CLIENT, session_part, 1)
        .unwrap();
    persist(
        &store,
        store.set_retained_replicated(EXACT_TOPIC, 1, b"21", 1).1,
    );
    persist(
        &store,
        store
            .add_inflight_replicated(CLIENT, 9, "t/i", b"i", 1, 1)
            .unwrap()
            .1,
    );
    persist(
        &store,
        store
            .commit_offset_replicated(CLIENT, PartitionId::ZERO, 42, 1)
            .1,
    );
    store
        .idempotency
        .check_or_insert_processing(
            IDEMPOTENCY_KEY,
            session_part,
            Epoch::new(1),
            ENTITY,
            RECORD,
            1,
        )
        .unwrap();
    persist(
        &store,
        store.commit_idempotency(session_part, IDEMPOTENCY_KEY, b"{\"ok\":true}"),
    );

    let schema = store
        .db_schema
        .register(ENTITY, b"{\"fields\":[]}")
        .unwrap();
    persist(
        &store,
        ReplicationWrite::new(
            PartitionId::ZERO,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_SCHEMA.to_string(),
            ENTITY.to_string(),
            SchemaStore::serialize(&schema),
        ),
    );
    for constraint in [
        ClusterConstraint::unique(ENTITY, "uniq_email", "email").unwrap(),
        ClusterConstraint::foreign_key(
            ENTITY,
            "fk_owner",
            "owner_id",
            "users",
            "id",
            OnDeleteAction::Cascade,
        )
        .unwrap(),
    ] {
        let write = store.constraint_add_replicated(&constraint).unwrap();
        store.apply_write(&write).unwrap();
    }

    let record = store
        .db_create_write(
            ENTITY,
            RECORD,
            b"{\"email\":\"a@example.com\",\"owner_id\":\"u1\"}",
            1,
        )
        .unwrap()
        .1;
    store.apply_write(&record).unwrap();
    let data_part = data_partition(ENTITY, RECORD);
    persist(
        &store,
        store
            .index_add_replicated(ENTITY, "email", UNIQUE_VALUE, data_part, RECORD)
            .unwrap()
            .1,
    );
    let reserved = store
        .unique_reserve_replicated(
            &UniqueReserveParams {
                entity: ENTITY,
                field: "email",
                value: UNIQUE_VALUE,
                record_id: RECORD,
                request_id: "req-1",
                data_partition: data_part,
                ttl_ms: 60_000,
            },
            1,
        )
        .1
        .unwrap();
    persist(&store, reserved);
    persist(
        &store,
        store
            .unique_commit_replicated(ENTITY, "email", UNIQUE_VALUE, "req-1")
            .unwrap()
            .1,
    );
    persist(
        &store,
        store
            .fk_add_request_replicated(FkValidationRequest::create(
                "users", "u1", FK_REQUEST, 60_000, 1,
            ))
            .unwrap(),
    );
    store
}

fn import_every_partition(from: &StoreManager, into: &StoreManager) {
    for partition in PartitionId::all() {
        into.import_partition(partition, &from.export_partition(partition).unwrap())
            .unwrap();
    }
}

fn disk(backend: &Arc<dyn StorageBackend>) -> BTreeMap<String, Vec<u8>> {
    backend
        .prefix_scan(b"")
        .unwrap()
        .into_iter()
        .map(|(key, value)| (String::from_utf8_lossy(&key).into_owned(), value))
        .collect()
}

fn recovered(backend: &Arc<dyn StorageBackend>) -> StoreManager {
    let fresh = with_storage(3, backend);
    fresh.recover().unwrap();
    fresh
}

fn assert_holds_everything(store: &StoreManager) {
    let session_part = session_partition(CLIENT);
    assert!(store.sessions.get(CLIENT).is_some(), "session");
    assert!(store.qos2.get(CLIENT, 7).is_some(), "qos2");
    let subscriptions = store
        .subscriptions
        .get_snapshot(CLIENT)
        .expect("subscriptions");
    assert!(subscriptions.has_subscription(EXACT_TOPIC));
    assert!(subscriptions.has_subscription(WILDCARD_TOPIC));
    assert!(
        !store.topics.get_subscribers(EXACT_TOPIC).is_empty(),
        "topic index"
    );
    assert!(store.wildcards.pattern_count() > 0, "wildcards");
    assert!(store.retained.get(EXACT_TOPIC).is_some(), "retained");
    assert!(store.inflight.get(CLIENT, 9).is_some(), "inflight");
    assert_eq!(
        store
            .offsets
            .get(CLIENT, PartitionId::ZERO)
            .map(|offset| offset.sequence),
        Some(42),
        "offsets"
    );
    assert!(
        store
            .idempotency
            .get(session_part, IDEMPOTENCY_KEY)
            .is_some_and(|record| record.is_committed()),
        "idempotency"
    );
    assert!(store.db_data.get(ENTITY, RECORD).is_some(), "db data");
    assert!(store.db_schema.get(ENTITY).is_some(), "schema");
    assert!(
        !store
            .db_index
            .lookup(ENTITY, "email", UNIQUE_VALUE)
            .is_empty(),
        "index"
    );
    assert!(
        store
            .db_unique
            .get(ENTITY, "email", UNIQUE_VALUE)
            .is_some_and(|claim| claim.is_committed()),
        "unique"
    );
    assert!(store.db_fk.get_request(FK_REQUEST).is_some(), "fk request");
    assert!(
        store.db_constraints.get(ENTITY, "uniq_email").is_some(),
        "unique constraint"
    );
    assert!(
        store.db_constraints.get(ENTITY, "fk_owner").is_some(),
        "fk constraint"
    );
}

#[test]
fn an_imported_snapshot_is_recovered_after_a_restart() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    assert_holds_everything(&source);

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    import_every_partition(&source, &with_storage(2, &joiner_disk));

    assert_holds_everything(&recovered(&joiner_disk));
}

#[test]
fn an_import_writes_the_keys_and_values_normal_writes_write() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    import_every_partition(&source, &with_storage(2, &joiner_disk));

    let wildcard_infix = format!("/{}/", entity::WILDCARDS);
    let mut expected = disk(&source_disk);
    expected.retain(|key, _| !key.contains(&wildcard_infix));
    assert!(
        expected.len() > 10,
        "normal writes must have persisted the fixture"
    );
    assert_eq!(disk(&joiner_disk), expected);
}

#[test]
fn a_delete_after_an_import_is_not_undone_by_a_restart() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = with_storage(2, &joiner_disk);
    import_every_partition(&source, &joiner);

    let deletes = [
        joiner.remove_session_replicated(CLIENT).unwrap().1,
        joiner.db_delete_write(ENTITY, RECORD).unwrap().1,
        joiner
            .constraint_remove_replicated(ENTITY, "uniq_email")
            .unwrap(),
        joiner
            .unique_release_replicated(ENTITY, "email", UNIQUE_VALUE, RECORD)
            .unwrap(),
        joiner.fk_complete_request_replicated(FK_REQUEST).unwrap().1,
        joiner.set_retained_replicated(EXACT_TOPIC, 0, b"", 2).1,
    ];
    joiner.persist_writes_batch(&deletes).unwrap();

    let restarted = recovered(&joiner_disk);
    assert!(restarted.sessions.get(CLIENT).is_none(), "session");
    assert!(restarted.db_data.get(ENTITY, RECORD).is_none(), "db data");
    assert!(
        restarted.db_constraints.get(ENTITY, "uniq_email").is_none(),
        "constraint"
    );
    assert!(
        restarted
            .db_unique
            .get(ENTITY, "email", UNIQUE_VALUE)
            .is_none(),
        "unique"
    );
    assert!(restarted.db_fk.get_request(FK_REQUEST).is_none(), "fk");
    assert!(restarted.retained.get(EXACT_TOPIC).is_none(), "retained");
}

#[test]
fn an_import_that_cannot_be_persisted_fails_and_leaves_memory_untouched() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    let partition = data_partition(ENTITY, RECORD);
    let payload = source.export_partition(partition).unwrap();

    let faulty = FaultyBackend::shared();
    faulty.fail_writes(true);
    let faulty_disk: Arc<dyn StorageBackend> = faulty.clone();
    let joiner = with_storage(2, &faulty_disk);

    assert_eq!(
        joiner.import_partition(partition, &payload),
        Err(StoreApplyError::PersistenceError)
    );
    assert!(joiner.db_data.get(ENTITY, RECORD).is_none());
    assert!(joiner.db_schema.get(ENTITY).is_none());
    assert_eq!(faulty.keys_containing(ENTITY).unwrap(), 0);
}

#[test]
fn an_import_whose_flush_fails_is_not_reported_complete() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    let partition = data_partition(ENTITY, RECORD);
    let payload = source.export_partition(partition).unwrap();

    let faulty = FaultyBackend::shared();
    faulty.fail_flushes(true);
    let faulty_disk: Arc<dyn StorageBackend> = faulty.clone();
    let joiner = with_storage(2, &faulty_disk);

    assert_eq!(
        joiner.import_partition(partition, &payload),
        Err(StoreApplyError::PersistenceError)
    );
    assert!(joiner.db_data.get(ENTITY, RECORD).is_none());
}

#[test]
fn an_imported_processing_idempotency_claim_is_not_recovered_after_a_restart() {
    let source = StoreManager::new(node(1));
    let partition = session_partition(CLIENT);
    source
        .idempotency
        .check_or_insert_processing("in-flight", partition, Epoch::new(1), ENTITY, RECORD, 1)
        .unwrap();

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = with_storage(2, &joiner_disk);
    joiner
        .import_partition(partition, &source.export_partition(partition).unwrap())
        .unwrap();
    assert!(
        joiner
            .idempotency
            .get(partition, "in-flight")
            .is_some_and(|record| record.is_processing()),
        "the in-memory import keeps the claim"
    );

    let restarted = recovered(&joiner_disk);
    assert!(
        !restarted
            .idempotency
            .get(partition, "in-flight")
            .is_some_and(|record| record.is_processing()),
        "a processing claim must not survive a restart"
    );
}

fn assert_holds_no_partition_data(store: &StoreManager, context: &str) {
    assert!(store.sessions.get(CLIENT).is_none(), "{context}: session");
    assert!(store.qos2.get(CLIENT, 7).is_none(), "{context}: qos2");
    assert!(
        store.subscriptions.get_snapshot(CLIENT).is_none(),
        "{context}: subscriptions"
    );
    assert!(
        store.retained.get(EXACT_TOPIC).is_none(),
        "{context}: retained"
    );
    assert!(
        store.inflight.get(CLIENT, 9).is_none(),
        "{context}: inflight"
    );
    assert!(
        store.offsets.get(CLIENT, PartitionId::ZERO).is_none(),
        "{context}: offsets"
    );
    assert!(
        store
            .idempotency
            .get(session_partition(CLIENT), IDEMPOTENCY_KEY)
            .is_none(),
        "{context}: idempotency"
    );
    assert!(
        store.db_data.get(ENTITY, RECORD).is_none(),
        "{context}: db data"
    );
    assert!(
        store
            .db_index
            .lookup(ENTITY, "email", UNIQUE_VALUE)
            .is_empty(),
        "{context}: index"
    );
    assert!(
        store.db_fk.get_request(FK_REQUEST).is_none(),
        "{context}: fk request"
    );
}

#[test]
fn an_import_replaces_partition_data_and_the_replacement_survives_a_restart() {
    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = written_by_normal_paths(&joiner_disk);
    let empty_source = StoreManager::new(node(1));

    import_every_partition(&empty_source, &joiner);

    assert_holds_no_partition_data(&joiner, "after import");
    assert_holds_no_partition_data(&recovered(&joiner_disk), "after restart");
}

#[test]
fn an_import_keeps_the_unique_claims_the_source_lacks() {
    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = written_by_normal_paths(&joiner_disk);
    let empty_source = StoreManager::new(node(1));

    import_every_partition(&empty_source, &joiner);

    for store in [&joiner, &recovered(&joiner_disk)] {
        assert!(
            store
                .db_unique
                .get(ENTITY, "email", UNIQUE_VALUE)
                .is_some_and(|claim| claim.is_committed()),
            "unique"
        );
    }
}

#[test]
fn an_import_keeps_the_global_catalog_the_source_lacks() {
    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = written_by_normal_paths(&joiner_disk);
    let empty_source = StoreManager::new(node(1));

    import_every_partition(&empty_source, &joiner);

    for store in [&joiner, &recovered(&joiner_disk)] {
        assert!(store.db_schema.get(ENTITY).is_some(), "schema");
        assert!(
            store.db_constraints.get(ENTITY, "uniq_email").is_some(),
            "constraint"
        );
    }
}

fn schema_delete(name: &str) -> ReplicationWrite {
    ReplicationWrite::new(
        PartitionId::ZERO,
        Operation::Delete,
        Epoch::ZERO,
        0,
        entity::DB_SCHEMA.to_string(),
        name.to_string(),
        Vec::new(),
    )
}

#[test]
fn a_global_write_during_two_transfers_survives_both_imports() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    let first = data_partition(ENTITY, RECORD);
    let second = session_partition(CLIENT);
    assert_ne!(first, second);
    let first_snapshot = source.export_partition(first).unwrap();
    let second_snapshot = source.export_partition(second).unwrap();

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = with_storage(2, &joiner_disk);
    joiner.open_transfer_window(first);
    joiner.open_transfer_window(second);
    joiner.apply_write(&schema_delete(ENTITY)).unwrap();

    joiner.import_partition(first, &first_snapshot).unwrap();
    joiner.import_partition(second, &second_snapshot).unwrap();

    assert!(joiner.db_schema.get(ENTITY).is_none());
    assert!(recovered(&joiner_disk).db_schema.get(ENTITY).is_none());
}

#[test]
fn a_global_write_before_the_transfer_window_is_overwritten_by_the_snapshot() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    let partition = data_partition(ENTITY, RECORD);
    let snapshot = source.export_partition(partition).unwrap();

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = with_storage(2, &joiner_disk);
    joiner.apply_write(&schema_delete(ENTITY)).unwrap();
    joiner.open_transfer_window(partition);

    joiner.import_partition(partition, &snapshot).unwrap();

    assert!(joiner.db_schema.get(ENTITY).is_some());
}

#[test]
fn a_transfer_window_that_overflows_still_takes_the_keys_this_node_lacks() {
    let source_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let source = written_by_normal_paths(&source_disk);
    let partition = data_partition(ENTITY, RECORD);
    let snapshot = source.export_partition(partition).unwrap();

    let joiner_disk: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let joiner = with_storage(2, &joiner_disk);
    joiner.open_transfer_window(partition);
    for n in 0..=super::transfer_window::TRANSFER_WINDOW_CAPACITY {
        joiner
            .apply_write(&schema_delete(&format!("gone-{n}")))
            .unwrap();
    }

    joiner.import_partition(partition, &snapshot).unwrap();

    assert!(joiner.db_data.get(ENTITY, RECORD).is_some());
    assert!(joiner.db_schema.get(ENTITY).is_some());
    assert!(joiner.db_constraints.get(ENTITY, "uniq_email").is_some());
}
