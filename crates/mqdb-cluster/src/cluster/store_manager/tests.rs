// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::store_manager::{StoreApplyError, StoreManager};
use crate::cluster::{Epoch, NodeId, PartitionId, entity};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn partition() -> PartitionId {
    PartitionId::ZERO
}

#[test]
fn apply_session_insert() {
    let manager = StoreManager::new(node(1));

    let session = crate::cluster::SessionData::create("client1", node(1));
    let data = crate::cluster::SessionStore::serialize(&session);

    let write = ReplicationWrite::new(
        partition(),
        Operation::Insert,
        Epoch::new(1),
        1,
        entity::SESSIONS.to_string(),
        "client1".to_string(),
        data,
    );

    manager.apply_write(&write).unwrap();
    assert!(manager.sessions.get("client1").is_some());
}

#[test]
fn apply_session_delete() {
    let manager = StoreManager::new(node(1));
    manager.sessions.create_session("client1").unwrap();

    let session = crate::cluster::SessionData::create("client1", node(1));
    let data = crate::cluster::SessionStore::serialize(&session);

    let write = ReplicationWrite::new(
        partition(),
        Operation::Delete,
        Epoch::new(1),
        1,
        entity::SESSIONS.to_string(),
        "client1".to_string(),
        data,
    );

    manager.apply_write(&write).unwrap();
    assert!(manager.sessions.get("client1").is_none());
}

#[test]
fn apply_unknown_entity_fails() {
    let manager = StoreManager::new(node(1));

    let write = ReplicationWrite::new(
        partition(),
        Operation::Insert,
        Epoch::new(1),
        1,
        "_unknown".to_string(),
        "id".to_string(),
        vec![],
    );

    let result = manager.apply_write(&write);
    assert!(matches!(result, Err(StoreApplyError::UnknownEntity)));
}

#[test]
fn recovery_restores_unique_and_fk_constraints() {
    use crate::cluster::db::{ClusterConstraint, ConstraintStore, OnDeleteAction, constraint_key};

    let backend: std::sync::Arc<dyn mqdb_core::StorageBackend> =
        std::sync::Arc::new(mqdb_core::MemoryBackend::new());
    let writer = StoreManager::new_with_storage(node(1), Some(backend.clone()));
    let constraints = [
        ClusterConstraint::unique("docs", "unique_docs_code", "code"),
        ClusterConstraint::foreign_key(
            "docs",
            "fk_docs_ref",
            "ref",
            "docs",
            "id",
            OnDeleteAction::SetNull,
        ),
    ];
    for constraint in &constraints {
        writer
            .apply_write(&ReplicationWrite::new(
                PartitionId::ZERO,
                Operation::Insert,
                Epoch::ZERO,
                0,
                entity::DB_CONSTRAINT.to_string(),
                constraint_key(constraint.entity_str(), constraint.name_str()),
                ConstraintStore::serialize(constraint),
            ))
            .unwrap();
    }

    let recovered = StoreManager::new_with_storage(node(1), Some(backend));
    let stats = recovered.recover().unwrap();

    assert_eq!(stats.db_constraint, 2);
    assert_eq!(
        recovered.constraint_get_unique_fields("docs"),
        vec!["code".to_string()]
    );
    assert_eq!(recovered.constraint_get_fk_constraints("docs").len(), 1);
}

#[test]
fn recovery_rebuilds_index_without_response_topics() {
    let backend: std::sync::Arc<dyn mqdb_core::StorageBackend> =
        std::sync::Arc::new(mqdb_core::MemoryBackend::new());
    let writer = StoreManager::new_with_storage(node(1), Some(backend.clone()));

    let mut snapshot = crate::cluster::MqttSubscriptionSnapshot::create("client1");
    snapshot.add_subscription("sensors/a", 1);
    snapshot.add_subscription("resp/client1", 0);
    let write = ReplicationWrite::new(
        crate::cluster::session_partition("client1"),
        Operation::Insert,
        Epoch::new(1),
        1,
        entity::SUBSCRIPTIONS.to_string(),
        "client1".to_string(),
        crate::cluster::SubscriptionCache::serialize(&snapshot),
    );
    writer.apply_write(&write).unwrap();

    let recovered = StoreManager::new_with_storage(node(1), Some(backend));
    recovered.recover().unwrap();

    assert_eq!(
        recovered.subscriptions.get_subscriptions("client1").len(),
        2
    );
    assert_eq!(
        recovered.topics.get_client_topics("client1"),
        vec![("sensors/a".to_string(), 1)]
    );
}
