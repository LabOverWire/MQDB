// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::client_location::{ClientLocationEntry, client_location_key};
use crate::cluster::db::{ClusterConstraint, ConstraintStoreError, SchemaStoreError};
use crate::cluster::test_support::FaultyBackend;
use bebytes::BeBytes;
use mqdb_core::storage::StorageBackend;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller_with_peers(backend: &Arc<FaultyBackend>) -> NodeController<MockTransport> {
    let storage: Arc<dyn StorageBackend> = backend.clone();
    let mut ctrl = NodeController::new_with_storage(
        node(1),
        MockTransport::new(node(1)),
        TransportConfig::default(),
        Some(storage),
        flume::unbounded().0,
        flume::unbounded().0,
    );
    for peer in [node(2), node(3)] {
        ctrl.heartbeat
            .receive_heartbeat(peer, &Heartbeat::create(peer, 0), 0);
    }
    ctrl
}

fn broadcasts_of(ctrl: &NodeController<MockTransport>, entity_name: &str) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(_, message)| {
            matches!(message, ClusterMessage::WriteRequest(w) if w.entity == entity_name)
        })
        .count()
}

fn unique_code() -> ClusterConstraint {
    ClusterConstraint::unique("items", "uniq_code", "code").unwrap()
}

#[tokio::test]
async fn admin_writes_the_node_cannot_store_are_refused() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    ctrl.become_primary(PartitionId::ZERO, Epoch::new(1));
    backend.fail_writes(true);

    let schema = ctrl.schema_register("items", br#"{"fields":[]}"#).await;
    let constraint = ctrl.constraint_add(&unique_code()).await;

    assert_eq!(
        (schema.err(), constraint.err()),
        (
            Some(SchemaStoreError::StoreFailed),
            Some(ConstraintStoreError::StoreFailed)
        )
    );
    assert!(ctrl.schema_get("items").is_none());
    assert!(ctrl.constraint_list("items").is_empty());
}

#[tokio::test]
async fn a_schema_register_the_node_cannot_store_is_refused_and_not_broadcast() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    backend.fail_writes(true);

    let registered = ctrl.schema_register("items", br#"{"fields":[]}"#).await;

    assert_eq!(registered.err(), Some(SchemaStoreError::StoreFailed));
    assert!(ctrl.schema_get("items").is_none());
    assert_eq!(broadcasts_of(&ctrl, entity::DB_SCHEMA), 0);

    backend.fail_writes(false);
    let retried = ctrl.schema_register("items", br#"{"fields":[]}"#).await;
    assert_eq!(retried.map(|schema| schema.schema_version), Ok(1));
    assert_eq!(broadcasts_of(&ctrl, entity::DB_SCHEMA), 2);
}

#[tokio::test]
async fn a_schema_update_the_node_cannot_store_is_refused_and_keeps_the_old_schema() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    ctrl.schema_register("items", b"{v1}").await.unwrap();
    let sent_before = broadcasts_of(&ctrl, entity::DB_SCHEMA);
    backend.fail_writes(true);

    let updated = ctrl.schema_update("items", b"{v2}").await;

    assert_eq!(updated.err(), Some(SchemaStoreError::StoreFailed));
    let kept = ctrl.schema_get("items").unwrap();
    assert_eq!(
        (kept.schema_version, kept.data.as_slice()),
        (1, &b"{v1}"[..])
    );
    assert_eq!(broadcasts_of(&ctrl, entity::DB_SCHEMA), sent_before);
}

#[tokio::test]
async fn a_constraint_add_the_node_cannot_store_is_refused_and_not_broadcast() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    backend.fail_writes(true);

    let added = ctrl.constraint_add(&unique_code()).await;

    assert_eq!(added.err(), Some(ConstraintStoreError::StoreFailed));
    assert!(ctrl.constraint_list("items").is_empty());
    assert_eq!(broadcasts_of(&ctrl, entity::DB_CONSTRAINT), 0);
}

#[tokio::test]
async fn a_constraint_remove_the_node_cannot_store_is_refused_and_keeps_the_constraint() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    ctrl.constraint_add(&unique_code()).await.unwrap();
    let sent_before = broadcasts_of(&ctrl, entity::DB_CONSTRAINT);
    backend.fail_writes(true);

    let removed = ctrl.constraint_remove("items", "uniq_code").await;

    assert_eq!(removed.err(), Some(ConstraintStoreError::StoreFailed));
    assert_eq!(ctrl.constraint_list("items"), vec![unique_code()]);
    assert_eq!(broadcasts_of(&ctrl, entity::DB_CONSTRAINT), sent_before);
}

#[tokio::test]
async fn a_client_location_the_node_cannot_store_is_not_broadcast() {
    let backend = FaultyBackend::shared();
    let mut ctrl = controller_with_peers(&backend);
    backend.fail_writes(true);
    let location = ReplicationWrite::new(
        crate::cluster::session_partition("c1"),
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::CLIENT_LOCATIONS.to_string(),
        client_location_key("c1"),
        ClientLocationEntry::create("c1", node(1)).to_be_bytes(),
    );

    let written = ctrl.write_or_forward_with_event(location, None, &[]).await;

    assert_eq!(written, Err(ReplicationError::StoreFailed));
    assert_eq!(ctrl.stores().client_locations.get("c1"), None);
    assert_eq!(broadcasts_of(&ctrl, entity::CLIENT_LOCATIONS), 0);
}
