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
    ctrl_with(1, backend, &[2, 3])
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
async fn a_client_location_the_node_cannot_store_is_kept_and_broadcast() {
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

    assert_eq!(written, Ok(()));
    assert_eq!(ctrl.stores().client_locations.get("c1"), Some(node(1)));
    assert_eq!(broadcasts_of(&ctrl, entity::CLIENT_LOCATIONS), 2);
}

fn ctrl_with(
    id: u16,
    backend: &Arc<FaultyBackend>,
    peers: &[u16],
) -> NodeController<MockTransport> {
    let storage: Arc<dyn StorageBackend> = backend.clone();
    let mut ctrl = NodeController::new_with_storage(
        node(id),
        MockTransport::new(node(id)),
        TransportConfig::default(),
        Some(storage),
        flume::unbounded().0,
        flume::unbounded().0,
    );
    for &peer in peers {
        ctrl.heartbeat
            .receive_heartbeat(node(peer), &Heartbeat::create(node(peer), 0), 0);
    }
    ctrl
}

fn sent_write_requests(ctrl: &NodeController<MockTransport>, to: NodeId) -> Vec<ReplicationWrite> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(dest, message)| match message {
            ClusterMessage::WriteRequest(w) if dest == to => Some(w),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_peer_that_cannot_store_a_client_location_still_keeps_it_in_memory() {
    let healthy = FaultyBackend::shared();
    let mut origin = ctrl_with(1, &healthy, &[2]);
    let failing = FaultyBackend::shared();
    let mut peer = ctrl_with(2, &failing, &[1]);
    failing.fail_writes(true);
    let location = ReplicationWrite::new(
        crate::cluster::session_partition("c1"),
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::CLIENT_LOCATIONS.to_string(),
        client_location_key("c1"),
        ClientLocationEntry::create("c1", node(1)).to_be_bytes(),
    );
    origin.write_or_forward(location).await;
    for write in sent_write_requests(&origin, node(2)) {
        peer.handle_write_request(node(1), write).await;
    }
    let on_origin = origin.stores().client_locations.get("c1");
    let on_peer = peer.stores().client_locations.get("c1");
    assert_eq!(on_origin, Some(node(1)));
    assert_eq!(
        on_peer,
        Some(node(1)),
        "peer that cannot store has no location for c1"
    );
}

#[tokio::test]
async fn a_primary_peer_that_cannot_store_a_client_location_still_keeps_it_in_memory() {
    let healthy = FaultyBackend::shared();
    let mut origin = ctrl_with(1, &healthy, &[2]);
    let failing = FaultyBackend::shared();
    let mut peer = ctrl_with(2, &failing, &[1]);
    let partition = crate::cluster::session_partition("c1");
    peer.become_primary(partition, Epoch::new(1));
    failing.fail_writes(true);
    let location = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::CLIENT_LOCATIONS.to_string(),
        client_location_key("c1"),
        ClientLocationEntry::create("c1", node(1)).to_be_bytes(),
    );
    origin.write_or_forward(location).await;
    for write in sent_write_requests(&origin, node(2)) {
        peer.handle_write_request(node(1), write).await;
    }

    assert_eq!(peer.stores().client_locations.get("c1"), Some(node(1)));
}

#[tokio::test]
async fn a_replica_that_cannot_store_a_client_location_from_its_primary_keeps_it() {
    let healthy = FaultyBackend::shared();
    let partition = crate::cluster::session_partition("c1");
    let mut primary = ctrl_with(2, &healthy, &[1, 3]);
    primary.become_primary(partition, Epoch::new(1));
    primary.partition_map.set(
        partition,
        crate::cluster::PartitionAssignment::new(node(2), vec![node(3)], Epoch::new(1)),
    );
    let failing = FaultyBackend::shared();
    let mut replica = ctrl_with(3, &failing, &[1, 2]);
    replica.become_replica(partition, Epoch::new(1), 0);
    failing.fail_writes(true);
    let location = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::ZERO,
        0,
        entity::CLIENT_LOCATIONS.to_string(),
        client_location_key("c1"),
        ClientLocationEntry::create("c1", node(1)).to_be_bytes(),
    );
    primary.handle_write_request(node(1), location).await;
    let replicated: Vec<ReplicationWrite> = primary
        .transport
        .sent_messages()
        .into_iter()
        .filter_map(|(dest, message)| match message {
            ClusterMessage::Write(w) if dest == node(3) => Some(w),
            _ => None,
        })
        .collect();
    assert_eq!(
        replicated.len(),
        1,
        "primary did not replicate to its replica"
    );
    for write in &replicated {
        replica.handle_write(node(2), write).await;
    }
    assert_eq!(
        (
            primary.stores().client_locations.get("c1"),
            replica.stores().client_locations.get("c1")
        ),
        (Some(node(1)), Some(node(1))),
        "replica that cannot store drops the client location from memory"
    );
}
