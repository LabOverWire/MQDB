// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction};
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, MqttRequestContext};
use crate::cluster::test_support::FaultyBackend;
use crate::cluster::{NUM_PARTITIONS, PartitionAssignment, PartitionMap};
use mqdb_core::storage::StorageBackend;
use serde_json::{Value, json};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn primary_of_every_partition(backend: &Arc<FaultyBackend>) -> NodeController<MockTransport> {
    let storage: Arc<dyn StorageBackend> = backend.clone();
    let mut ctrl = NodeController::new_with_storage(
        node(1),
        MockTransport::new(node(1)),
        TransportConfig::default(),
        Some(storage),
        flume::unbounded().0,
        flume::unbounded().0,
    );
    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        map.set(
            partition,
            PartitionAssignment {
                primary: Some(node(1)),
                replicas: vec![node(2)],
                epoch: Epoch::new(1),
            },
        );
    }
    ctrl.update_partition_map(map);
    for i in 0..NUM_PARTITIONS {
        ctrl.become_primary(PartitionId::new(i).unwrap(), Epoch::new(1));
    }
    ctrl
}

async fn request(ctrl: &mut NodeController<MockTransport>, topic: &str, payload: &Value) -> Value {
    let ctx = MqttRequestContext {
        response_topic: Some("resp/t"),
        correlation_data: None,
        sender: None,
        client_id: None,
    };
    let bytes = if payload.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(payload).unwrap()
    };
    let DbPublishResult::Response(response) = DbRequestHandler::new(node(1))
        .handle_publish(ctrl, topic, &bytes, &ctx)
        .await
    else {
        panic!("{topic} got no response");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

fn data_writes_sent(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(_, message)| {
            matches!(message, ClusterMessage::Write(w) if w.entity == crate::cluster::entity::DB_DATA)
        })
        .count()
}

fn events_published(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport.queued_publishes().len()
}

#[tokio::test]
async fn a_create_the_primary_cannot_store_is_refused_and_not_readable() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    backend.fail_writes(true);

    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": "r1", "n": 1})).await;
    let read = request(&mut ctrl, "$DB/items/r1", &Value::Null).await;

    assert_eq!(
        (&created["status"], &created["code"], &read["code"]),
        (&json!("error"), &json!(500), &json!(404))
    );
    assert!(ctrl.db_get("items", "r1").is_none());
    assert_eq!(data_writes_sent(&ctrl), 0);
    assert_eq!(events_published(&ctrl), 0);
}

#[tokio::test]
async fn an_update_the_primary_cannot_store_is_refused_and_leaves_the_record() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": "r1", "n": 1})).await;
    assert_eq!(created["status"], "ok");
    let writes_before = data_writes_sent(&ctrl);
    let events_before = events_published(&ctrl);
    backend.fail_writes(true);

    let updated = request(&mut ctrl, "$DB/items/r1/update", &json!({"n": 2})).await;
    let read = request(&mut ctrl, "$DB/items/r1", &Value::Null).await;

    assert_eq!(
        (&updated["status"], &updated["code"], &read["data"]["n"]),
        (&json!("error"), &json!(500), &json!(1))
    );
    assert_eq!(data_writes_sent(&ctrl), writes_before);
    assert_eq!(events_published(&ctrl), events_before);
}

#[tokio::test]
async fn a_delete_the_primary_cannot_store_is_refused_and_keeps_its_unique_claim() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let created = request(
        &mut ctrl,
        "$DB/items/create",
        &json!({"id": "r1", "code": "a"}),
    )
    .await;
    assert_eq!(created["status"], "ok");
    ctrl.constraint_add(&ClusterConstraint::unique("items", "uniq_code", "code"))
        .await
        .unwrap();
    let code = serde_json::to_vec(&json!("a")).unwrap();
    let partition = crate::cluster::db::data_partition("items", "r1");
    ctrl.stores()
        .db_unique
        .reassert("items", "code", &code, "r1", partition, 1000);
    let writes_before = data_writes_sent(&ctrl);
    let events_before = events_published(&ctrl);
    backend.fail_writes(true);

    let deleted = request(&mut ctrl, "$DB/items/r1/delete", &Value::Null).await;
    let read = request(&mut ctrl, "$DB/items/r1", &Value::Null).await;

    assert_eq!(
        (&deleted["status"], &deleted["code"], &read["data"]["code"]),
        (&json!("error"), &json!(500), &json!("a"))
    );
    assert!(ctrl.stores().unique_get("items", "code", &code).is_some());
    assert_eq!(data_writes_sent(&ctrl), writes_before);
    assert_eq!(events_published(&ctrl), events_before);
}

#[tokio::test]
async fn a_binary_write_the_primary_cannot_store_returns_an_error() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    ctrl.db_create("items", "kept", b"{\"n\":1}", 1000)
        .await
        .unwrap();
    backend.fail_writes(true);

    let created = ctrl.db_create("items", "r1", b"{\"n\":1}", 1000).await;
    let updated = ctrl.db_update("items", "kept", b"{\"n\":2}", 2000).await;
    let deleted = ctrl.db_delete("items", "kept").await;

    let refused = Some("commit failed: failed to apply write to the local store".to_string());
    assert_eq!(created.err().map(|e| e.to_string()), refused);
    assert_eq!(updated.err().map(|e| e.to_string()), refused);
    assert_eq!(deleted.err().map(|e| e.to_string()), refused);
    assert!(ctrl.db_get("items", "r1").is_none());
    assert_eq!(ctrl.db_get("items", "kept").unwrap().data, b"{\"n\":1}");
}

async fn author_with_post(ctrl: &mut NodeController<MockTransport>) {
    let author = request(ctrl, "$DB/users/create", &json!({"id": "u1", "name": "a"})).await;
    let post = request(
        ctrl,
        "$DB/posts/create",
        &json!({"id": "p1", "author_id": "u1"}),
    )
    .await;
    assert_eq!(
        (&author["status"], &post["status"]),
        (&json!("ok"), &json!("ok"))
    );
    ctrl.constraint_add(&ClusterConstraint::foreign_key(
        "posts",
        "posts_author_fk",
        "author_id",
        "users",
        "id",
        OnDeleteAction::Cascade,
    ))
    .await
    .unwrap();
}

fn hand_partition_to(
    ctrl: &mut NodeController<MockTransport>,
    moved: PartitionId,
    primary: NodeId,
) {
    ctrl.step_down(moved);
    let mut map = PartitionMap::default();
    for i in 0..NUM_PARTITIONS {
        let partition = PartitionId::new(i).unwrap();
        let (owner, replicas) = if partition == moved {
            (primary, vec![node(1)])
        } else {
            (node(1), vec![node(2)])
        };
        map.set(
            partition,
            PartitionAssignment {
                primary: Some(owner),
                replicas,
                epoch: Epoch::new(2),
            },
        );
    }
    ctrl.update_partition_map(map);
}

fn cascade_requests_sent(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter(|(_, message)| matches!(message, ClusterMessage::JsonDbRequest { .. }))
        .count()
}

#[tokio::test]
async fn a_parent_delete_the_primary_cannot_store_keeps_its_local_children() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    author_with_post(&mut ctrl).await;
    backend.fail_writes_to_keys_containing("users/u1");

    let deleted = request(&mut ctrl, "$DB/users/u1/delete", &Value::Null).await;

    assert_eq!(
        (
            &deleted["code"],
            ctrl.db_get("users", "u1").is_some(),
            ctrl.db_get("posts", "p1").is_some()
        ),
        (&json!(500), true, true)
    );
}

#[tokio::test]
async fn a_parent_delete_the_primary_cannot_store_sends_no_remote_cascade() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let author = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "name": "a"}),
    )
    .await;
    assert_eq!(author["status"], "ok");
    hand_partition_to(
        &mut ctrl,
        crate::cluster::db::data_partition("posts", "p1"),
        node(2),
    );
    let requests_before = cascade_requests_sent(&ctrl);
    backend.fail_writes_to_keys_containing("users/u1");
    let remote_child = crate::cluster::node_controller::fk::FkReverseLookupResult {
        constraint_name: "posts_author_fk".to_string(),
        source_entity: "posts".to_string(),
        source_field: "author_id".to_string(),
        on_delete: OnDeleteAction::Cascade,
        referencing_ids: vec!["p1".to_string()],
        cross_owned_ids: Vec::new(),
        target_id: "u1".to_string(),
    };

    let response = DbRequestHandler::new(node(1))
        .complete_pending_fk_delete(
            &mut ctrl,
            None,
            vec![remote_child],
            FkDeleteContinuation::DeleteFromDbHandler {
                entity: "users".to_string(),
                id: "u1".to_string(),
                sender: None,
                client_id: None,
                response_topic: "resp/t".to_string(),
                correlation_data: None,
            },
        )
        .await
        .unwrap();
    let deleted: Value = serde_json::from_slice(&response.payload).unwrap();

    assert_eq!(
        (
            &deleted["code"],
            cascade_requests_sent(&ctrl) - requests_before
        ),
        (&json!(500), 0)
    );
}
