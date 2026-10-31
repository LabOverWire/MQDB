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
    request_as(ctrl, &DbRequestHandler::new(node(1)), topic, payload, None).await
}

async fn request_as(
    ctrl: &mut NodeController<MockTransport>,
    handler: &DbRequestHandler,
    topic: &str,
    payload: &Value,
    sender: Option<&str>,
) -> Value {
    let ctx = MqttRequestContext {
        response_topic: Some("resp/t"),
        correlation_data: None,
        sender,
        client_id: None,
    };
    let bytes = if payload.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(payload).unwrap()
    };
    let DbPublishResult::Response(response) =
        handler.handle_publish(ctrl, topic, &bytes, &ctx).await
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
async fn a_controller_write_the_primary_cannot_store_returns_an_error() {
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

#[tokio::test]
async fn a_local_cascade_child_that_cannot_be_stored_stays_pending() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    author_with_post(&mut ctrl).await;
    backend.fail_writes_to_keys_containing("posts/p1");

    let deleted = request(&mut ctrl, "$DB/users/u1/delete", &Value::Null).await;
    tokio::task::yield_now().await;
    let pending = ctrl
        .stores()
        .cluster_outbox()
        .unwrap()
        .pending_cascade_ops()
        .unwrap();

    assert_eq!(
        (
            &deleted["status"],
            ctrl.db_get("posts", "p1").is_some(),
            pending.len()
        ),
        (&json!("ok"), true, 1)
    );
    backend.stop_failing_keys();
    ctrl.replay_cascade_entries(&pending).await;
    tokio::task::yield_now().await;
    assert!(ctrl.db_get("posts", "p1").is_none());
}

fn sharing_handler(ctrl: &mut NodeController<MockTransport>) -> DbRequestHandler {
    let mut fields = std::collections::HashMap::new();
    fields.insert("diagrams".to_string(), "userId".to_string());
    let ownership = Arc::new(mqdb_core::types::OwnershipConfig::new(fields));
    ctrl.set_ownership(Arc::clone(&ownership));
    DbRequestHandler::new(node(1)).with_ownership(ownership)
}

async fn shared_diagram(ctrl: &mut NodeController<MockTransport>, handler: &DbRequestHandler) {
    let created = request_as(
        ctrl,
        handler,
        "$DB/diagrams/create",
        &json!({"id": "d1", "userId": "alice", "title": "D"}),
        Some("alice"),
    )
    .await;
    let shared = request_as(
        ctrl,
        handler,
        "$DB/diagrams/d1/share",
        &json!({"grantee": "bob", "permission": "edit"}),
        Some("alice"),
    )
    .await;
    assert_eq!(
        (&created["status"], &shared["status"]),
        (&json!("ok"), &json!("ok"))
    );
}

#[tokio::test]
async fn an_unshare_the_primary_cannot_store_is_refused() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    backend.fail_writes_to_keys_containing("_shares/");

    let unshared = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/unshare",
        &json!({"grantee": "bob"}),
        Some("alice"),
    )
    .await;
    let read = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1",
        &Value::Null,
        Some("bob"),
    )
    .await;

    assert_eq!(
        (&unshared["status"], &read["status"]),
        (&json!("error"), &json!("ok"))
    );
}

#[tokio::test]
async fn a_downgrade_whose_old_grant_cannot_be_removed_is_refused() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    let old_grant = ctrl.db_list("_shares")[0].id_str().to_string();
    backend.fail_writes_to_keys_containing(&format!("_shares/{old_grant}"));

    let downgraded = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/share",
        &json!({"grantee": "bob", "permission": "view"}),
        Some("alice"),
    )
    .await;
    let update = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/update",
        &json!({"title": "x"}),
        Some("bob"),
    )
    .await;

    assert_eq!(
        (
            &downgraded["status"],
            &update["code"],
            ctrl.db_list("_shares").len()
        ),
        (&json!("error"), &json!(null), 1)
    );
}

#[tokio::test]
async fn a_grant_that_cannot_be_removed_with_its_resource_is_retried() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    backend.fail_writes_to_keys_containing("_shares/");

    let deleted = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/delete",
        &Value::Null,
        Some("alice"),
    )
    .await;
    let pending = ctrl
        .stores()
        .cluster_outbox()
        .unwrap()
        .pending_cascade_ops()
        .unwrap();
    assert_eq!(
        (
            &deleted["data"]["deleted"],
            ctrl.db_get("diagrams", "d1").is_some(),
            pending.len()
        ),
        (&json!(true), false, 1)
    );

    backend.stop_failing_keys();
    ctrl.replay_cascade_entries(&pending).await;

    assert!(ctrl.db_list("_shares").is_empty());
}

#[tokio::test]
async fn a_cascade_its_receiver_could_not_store_is_not_acked() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let (tx, rx) = tokio::sync::oneshot::channel();
    ctrl.pending_constraints().insert_cascade_ack(42, tx);
    let refused = crate::cluster::protocol::JsonDbResponse::new(
        0,
        serde_json::to_vec(&json!({"status": "error", "code": 500, "message": "internal error"}))
            .unwrap(),
        "_mqdb/cascade_ack/1/42".to_string(),
        None,
    );

    ctrl.handle_json_db_response(&refused).await;

    assert!(!rx.await.unwrap());
}

#[tokio::test]
async fn a_replayed_cascade_spares_a_child_recreated_for_another_parent() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    author_with_post(&mut ctrl).await;
    let other = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u9", "name": "b"}),
    )
    .await;
    assert_eq!(other["status"], "ok");
    hand_partition_to(
        &mut ctrl,
        crate::cluster::db::data_partition("posts", "p2"),
        node(2),
    );
    let children = crate::cluster::node_controller::fk::FkReverseLookupResult {
        constraint_name: "posts_author_fk".to_string(),
        source_entity: "posts".to_string(),
        source_field: "author_id".to_string(),
        on_delete: OnDeleteAction::Cascade,
        referencing_ids: vec!["p1".to_string(), "p2".to_string()],
        cross_owned_ids: Vec::new(),
        target_id: "u1".to_string(),
    };
    let response = DbRequestHandler::new(node(1))
        .complete_pending_fk_delete(
            &mut ctrl,
            None,
            vec![children],
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
    assert_eq!(deleted["data"]["deleted"], true);
    assert!(ctrl.db_get("posts", "p1").is_none());
    let recreated = request(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p1", "author_id": "u9"}),
    )
    .await;
    assert_eq!(recreated["status"], "ok");
    let pending = ctrl
        .stores()
        .cluster_outbox()
        .unwrap()
        .pending_cascade_ops()
        .unwrap();
    assert_eq!(pending.len(), 1);

    ctrl.replay_cascade_entries(&pending).await;

    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn a_replayed_set_null_bumps_the_child_version() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let author = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "name": "a"}),
    )
    .await;
    let post = request(
        &mut ctrl,
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
        OnDeleteAction::SetNull,
    ))
    .await
    .unwrap();
    backend.fail_writes_to_keys_containing("posts/p1");
    let deleted = request(&mut ctrl, "$DB/users/u1/delete", &Value::Null).await;
    assert_eq!(deleted["data"]["deleted"], true);
    let pending = ctrl
        .stores()
        .cluster_outbox()
        .unwrap()
        .pending_cascade_ops()
        .unwrap();
    backend.stop_failing_keys();

    ctrl.replay_cascade_entries(&pending).await;
    let read = request(&mut ctrl, "$DB/posts/p1", &Value::Null).await;

    assert_eq!(
        (&read["data"]["author_id"], &read["data"]["_version"]),
        (&Value::Null, &json!(2))
    );
}

#[tokio::test]
async fn a_resource_delete_the_primary_cannot_store_keeps_its_grants() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    backend.fail_writes_to_keys_containing("diagrams/d1");

    let deleted = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/delete",
        &Value::Null,
        Some("alice"),
    )
    .await;

    assert_eq!(
        (
            &deleted["status"],
            ctrl.db_get("diagrams", "d1").is_some(),
            ctrl.db_list("_shares").len()
        ),
        (&json!("error"), true, 1)
    );
}

#[tokio::test]
async fn a_cascaded_child_loses_its_grants() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    let child = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/create",
        &json!({"id": "d2", "userId": "alice", "parent_id": "d1"}),
        Some("alice"),
    )
    .await;
    let shared = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d2/share",
        &json!({"grantee": "carol", "permission": "view"}),
        Some("alice"),
    )
    .await;
    assert_eq!(
        (&child["status"], &shared["status"]),
        (&json!("ok"), &json!("ok"))
    );
    ctrl.constraint_add(&ClusterConstraint::foreign_key(
        "diagrams",
        "diagrams_parent_fk",
        "parent_id",
        "diagrams",
        "id",
        OnDeleteAction::Cascade,
    ))
    .await
    .unwrap();

    let deleted = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/delete",
        &Value::Null,
        Some("alice"),
    )
    .await;

    assert_eq!(
        (
            &deleted["data"]["deleted"],
            ctrl.db_get("diagrams", "d2").is_some(),
            ctrl.db_list("_shares").len()
        ),
        (&json!(true), false, 0)
    );
}

#[tokio::test]
async fn a_regrant_interrupted_after_its_first_write_keeps_the_grantee_level() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    backend.fail_writes_after(1);

    let regranted = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/share",
        &json!({"grantee": "bob", "permission": "view"}),
        Some("alice"),
    )
    .await;
    let permissions: Vec<Value> = ctrl
        .db_list("_shares")
        .iter()
        .map(|grant| serde_json::from_slice::<Value>(&grant.data).unwrap()["permission"].clone())
        .collect();

    let highest = permissions
        .iter()
        .filter_map(Value::as_str)
        .filter_map(mqdb_core::types::AccessLevel::parse)
        .max();

    assert_eq!(
        (&regranted["status"], highest),
        (
            &json!("error"),
            mqdb_core::types::AccessLevel::parse("edit")
        )
    );
}

#[tokio::test]
async fn a_create_whose_id_cannot_be_encoded_is_refused() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let id = "a".repeat(usize::from(u16::MAX) + 1);

    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": id, "n": 1})).await;

    assert_eq!(
        (&created["status"], ctrl.db_get("items", &id).is_some()),
        (&json!("error"), false)
    );
}

#[tokio::test]
async fn a_cascade_share_that_cannot_grant_every_member_is_refused() {
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    let handler = sharing_handler(&mut ctrl);
    let member = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/create",
        &json!({"id": "d2", "userId": "alice"}),
        Some("alice"),
    )
    .await;
    let root = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/create",
        &json!({"id": "d1", "userId": "alice", "embeds": "d2"}),
        Some("alice"),
    )
    .await;
    assert_eq!(
        (&member["status"], &root["status"]),
        (&json!("ok"), &json!("ok"))
    );
    ctrl.constraint_add(&ClusterConstraint::foreign_key(
        "diagrams",
        "diagrams_embeds_fk",
        "embeds",
        "diagrams",
        "id",
        OnDeleteAction::SetNull,
    ))
    .await
    .unwrap();
    backend.fail_writes_after(1);

    let shared = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/share",
        &json!({"grantee": "bob", "permission": "view", "cascade": true}),
        Some("alice"),
    )
    .await;

    assert_eq!(
        (&shared["status"], ctrl.db_list("_shares").len()),
        (&json!("error"), 1)
    );
}

#[tokio::test]
async fn a_binary_create_the_primary_cannot_store_is_refused() {
    use crate::cluster::db_protocol::{DbResponse, DbStatus, DbWriteRequest};
    use bebytes::BeBytes;
    let backend = FaultyBackend::shared();
    let mut ctrl = primary_of_every_partition(&backend);
    backend.fail_writes(true);
    let partition = PartitionId::new(7).unwrap();
    let topic = format!("$DB/p{}/items/create", partition.get());
    let ctx = MqttRequestContext {
        response_topic: Some("resp/t"),
        correlation_data: None,
        sender: None,
        client_id: None,
    };

    let DbPublishResult::Response(response) = DbRequestHandler::new(node(1))
        .handle_publish(
            &mut ctrl,
            &topic,
            &DbWriteRequest::create(b"{\"n\":1}", 1000).to_be_bytes(),
            &ctx,
        )
        .await
    else {
        panic!("binary create got no response");
    };
    let (status, _) = DbResponse::try_from_be_bytes(&response.payload).unwrap();

    assert_eq!(
        (status.status(), ctrl.db_list("items").len()),
        (DbStatus::Error, 0)
    );
}

fn cascade_of(
    target: &str,
    ids: &[&str],
) -> crate::cluster::node_controller::fk::FkReverseLookupResult {
    crate::cluster::node_controller::fk::FkReverseLookupResult {
        constraint_name: "posts_author_fk".to_string(),
        source_entity: "posts".to_string(),
        source_field: "author_id".to_string(),
        on_delete: OnDeleteAction::Cascade,
        referencing_ids: ids.iter().map(|id| (*id).to_string()).collect(),
        cross_owned_ids: Vec::new(),
        target_id: target.to_string(),
    }
}

async fn finish_author_delete(
    ctrl: &mut NodeController<MockTransport>,
    results: Vec<crate::cluster::node_controller::fk::FkReverseLookupResult>,
) -> Value {
    let response = DbRequestHandler::new(node(1))
        .complete_pending_fk_delete(
            ctrl,
            None,
            results,
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
    serde_json::from_slice(&response.payload).unwrap()
}

fn cascade_delete_requests(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. }
                if request.op == crate::cluster::protocol::JsonDbOp::Delete =>
            {
                Some(serde_json::from_slice(&request.payload).unwrap_or(Value::Null))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_cascade_spares_a_child_re_pointed_while_the_lookup_ran() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    author_with_post(&mut ctrl).await;
    let other = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u9", "name": "b"}),
    )
    .await;
    let moved = request(
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": "u9"}),
    )
    .await;
    assert_eq!(
        (&other["status"], &moved["status"]),
        (&json!("ok"), &json!("ok"))
    );

    let deleted = finish_author_delete(&mut ctrl, vec![cascade_of("u1", &["p1"])]).await;

    assert_eq!(
        (
            &deleted["data"]["deleted"],
            ctrl.db_get("posts", "p1").is_some()
        ),
        (&json!(true), true)
    );
}

#[tokio::test]
async fn a_remote_cascade_delete_carries_its_guard() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let author = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "name": "a"}),
    )
    .await;
    assert_eq!(author["status"], "ok");
    hand_partition_to(
        &mut ctrl,
        crate::cluster::db::data_partition("posts", "p2"),
        node(2),
    );

    let deleted = finish_author_delete(&mut ctrl, vec![cascade_of("u1", &["p2"])]).await;

    assert_eq!(
        (&deleted["data"]["deleted"], cascade_delete_requests(&ctrl)),
        (
            &json!(true),
            vec![json!({"__mqdb_fk_expected": "author_id=u1"})]
        )
    );
}

fn cascade_delete_request(
    entity: &str,
    id: &str,
    payload: &Value,
) -> crate::cluster::protocol::JsonDbRequest {
    crate::cluster::protocol::JsonDbRequest {
        request_id: 7,
        op: crate::cluster::protocol::JsonDbOp::Delete,
        entity: entity.to_string(),
        id: Some(id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "_mqdb/cascade_ack/2/7".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
    }
}

#[tokio::test]
async fn a_guarded_cascade_delete_spares_a_re_pointed_child() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    author_with_post(&mut ctrl).await;
    let other = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u9", "name": "b"}),
    )
    .await;
    let moved = request(
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": "u9"}),
    )
    .await;
    assert_eq!(
        (&other["status"], &moved["status"]),
        (&json!("ok"), &json!("ok"))
    );
    let partition = crate::cluster::db::data_partition("posts", "p1");

    ctrl.handle_json_db_request(
        node(2),
        partition,
        &cascade_delete_request(
            "posts",
            "p1",
            &json!({"__mqdb_fk_expected": "author_id=u1"}),
        ),
    )
    .await;

    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn a_guarded_cascade_delete_of_an_orphaned_grant_is_applied() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;
    let grant = ctrl.db_list("_shares")[0].id_str().to_string();
    let partition = crate::cluster::db::data_partition("_shares", &grant);
    crate::cluster::test_support::remove_record(&ctrl.stores().db_data, "diagrams", "d1");

    ctrl.handle_json_db_request(
        node(2),
        partition,
        &cascade_delete_request(
            "_shares",
            &grant,
            &json!({"__mqdb_fk_expected": "resource_id=d1"}),
        ),
    )
    .await;

    assert!(ctrl.db_list("_shares").is_empty());
}

#[tokio::test]
async fn a_replayed_cascade_spares_a_child_whose_reference_was_cleared() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let author = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "null", "name": "a"}),
    )
    .await;
    let post = request(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p1", "author_id": null}),
    )
    .await;
    assert_eq!(
        (&author["status"], &post["status"]),
        (&json!("ok"), &json!("ok"))
    );
    let ops = [crate::cluster::CascadeRemoteOp::GuardedDelete {
        entity: "posts".to_string(),
        id: "p1".to_string(),
        field: "author_id".to_string(),
        expected_value: "null".to_string(),
        sender: None,
    }];
    let entry = crate::cluster::CascadePendingEntry {
        operation_id: "op-null".to_string(),
        operations: ops.to_vec(),
    };

    ctrl.replay_cascade_entries(&[entry]).await;

    assert!(ctrl.db_get("posts", "p1").is_some());
}

fn grant_events(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport
        .queued_publishes()
        .iter()
        .filter(|publish| format!("{publish:?}").contains("$DB/_shares/events"))
        .count()
}

#[tokio::test]
async fn removing_the_grants_of_a_deleted_resource_publishes_no_grant_events() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let handler = sharing_handler(&mut ctrl);
    shared_diagram(&mut ctrl, &handler).await;

    let deleted = request_as(
        &mut ctrl,
        &handler,
        "$DB/diagrams/d1/delete",
        &Value::Null,
        Some("alice"),
    )
    .await;

    assert_eq!(
        (
            &deleted["data"]["deleted"],
            ctrl.db_list("_shares").len(),
            grant_events(&ctrl)
        ),
        (&json!(true), 0, 0)
    );
}

#[tokio::test]
async fn a_cascade_nulls_every_reference_a_child_holds() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let user = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "name": "a"}),
    )
    .await;
    let task = request(
        &mut ctrl,
        "$DB/tasks/create",
        &json!({"id": "t1", "assignee": "u1", "reviewer": "u1"}),
    )
    .await;
    assert_eq!(
        (&user["status"], &task["status"]),
        (&json!("ok"), &json!("ok"))
    );
    for field in ["assignee", "reviewer"] {
        ctrl.constraint_add(&ClusterConstraint::foreign_key(
            "tasks",
            &format!("tasks_{field}_fk"),
            field,
            "users",
            "id",
            OnDeleteAction::SetNull,
        ))
        .await
        .unwrap();
    }

    let deleted = request(&mut ctrl, "$DB/users/u1/delete", &Value::Null).await;
    let read = request(&mut ctrl, "$DB/tasks/t1", &Value::Null).await;

    assert_eq!(
        (
            &deleted["data"]["deleted"],
            &read["data"]["assignee"],
            &read["data"]["reviewer"]
        ),
        (&json!(true), &Value::Null, &Value::Null)
    );
}

#[tokio::test]
async fn a_guarded_cascade_delete_is_rechecked_after_its_lookups() {
    use crate::cluster::message_processor::HeartbeatUpdate;
    use crate::cluster::protocol::Heartbeat;
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    author_with_post(&mut ctrl).await;
    let other = request(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u9", "name": "b"}),
    )
    .await;
    let comment = request(
        &mut ctrl,
        "$DB/comments/create",
        &json!({"id": "c1", "post_id": "p1"}),
    )
    .await;
    assert_eq!(
        (&other["status"], &comment["status"]),
        (&json!("ok"), &json!("ok"))
    );
    ctrl.constraint_add(&ClusterConstraint::foreign_key(
        "comments",
        "comments_post_fk",
        "post_id",
        "posts",
        "id",
        OnDeleteAction::Cascade,
    ))
    .await
    .unwrap();
    ctrl.apply_heartbeat_updates(&[HeartbeatUpdate {
        from: node(2),
        heartbeat: Heartbeat::create(node(2), 1),
        received_at: 1,
    }]);
    let partition = crate::cluster::db::data_partition("posts", "p1");
    let elsewhere = (0..NUM_PARTITIONS)
        .map(|i| PartitionId::new(i).unwrap())
        .find(|candidate| {
            *candidate != partition
                && *candidate != crate::cluster::db::data_partition("users", "u9")
                && *candidate != crate::cluster::db::data_partition("comments", "c1")
        })
        .unwrap();
    hand_partition_to(&mut ctrl, elsewhere, node(2));

    let pending = ctrl
        .handle_json_db_request(
            node(2),
            partition,
            &cascade_delete_request(
                "posts",
                "p1",
                &json!({"__mqdb_fk_expected": "author_id=u1"}),
            ),
        )
        .await;
    let Some(crate::cluster::node_controller::PendingConstraintWork::FkDelete(work)) = pending
    else {
        panic!("the delete should wait for remote reverse lookups");
    };
    let moved = request(
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": "u9"}),
    )
    .await;
    assert_eq!(moved["status"], "ok");
    ctrl.complete_pending_fk_delete_work(None, work.local_results, work.continuation)
        .await;

    assert!(ctrl.db_get("posts", "p1").is_some());
}

#[tokio::test]
async fn a_spared_cascade_child_keeps_its_grants() {
    let mut ctrl = primary_of_every_partition(&FaultyBackend::shared());
    let mut fields = std::collections::HashMap::new();
    fields.insert("posts".to_string(), "owner".to_string());
    let ownership = Arc::new(mqdb_core::types::OwnershipConfig::new(fields));
    ctrl.set_ownership(Arc::clone(&ownership));
    let handler = DbRequestHandler::new(node(1)).with_ownership(ownership);
    for (topic, payload) in [
        ("$DB/users/create", json!({"id": "u1", "name": "a"})),
        ("$DB/users/create", json!({"id": "u9", "name": "b"})),
        (
            "$DB/posts/create",
            json!({"id": "p1", "author_id": "u1", "owner": "alice"}),
        ),
        (
            "$DB/posts/p1/share",
            json!({"grantee": "bob", "permission": "view"}),
        ),
        ("$DB/posts/p1/update", json!({"author_id": "u9"})),
    ] {
        let done = request_as(&mut ctrl, &handler, topic, &payload, Some("alice")).await;
        assert_eq!(done["status"], "ok", "{topic}");
    }
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

    let deleted = finish_author_delete(&mut ctrl, vec![cascade_of("u1", &["p1"])]).await;

    assert_eq!(
        (
            &deleted["data"]["deleted"],
            ctrl.db_get("posts", "p1").is_some(),
            ctrl.db_list("_shares").len()
        ),
        (&json!(true), true, 1)
    );
}
