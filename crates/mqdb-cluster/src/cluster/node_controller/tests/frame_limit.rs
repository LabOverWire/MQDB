// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::{controller_where, publish_completing_unique, response_ctx};
use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition};
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, FkCheckCompletion};
use crate::cluster::node_controller::db_ops::CLIENT_WRITE_LIMIT;
use crate::cluster::protocol::{JsonDbOp, JsonDbRequest};
use serde_json::{Value, json};

const MIB: usize = 1024 * 1024;

fn limit_text() -> String {
    format!("bytes, max {CLIENT_WRITE_LIMIT})")
}

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn primary_with_replica() -> NodeController<MockTransport> {
    controller_where(None, |_| node(1), &[node(2)])
}

fn blob(len: usize) -> String {
    "x".repeat(len)
}

async fn publish(
    ctrl: &mut NodeController<MockTransport>,
    handler: &DbRequestHandler,
    topic: &str,
    payload: &Value,
    sender: Option<&str>,
) -> Value {
    let bytes = serde_json::to_vec(payload).unwrap();
    let DbPublishResult::Response(response) = handler
        .handle_publish(ctrl, topic, &bytes, &response_ctx(sender))
        .await
    else {
        panic!("{topic} got no response");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

async fn request(ctrl: &mut NodeController<MockTransport>, topic: &str, payload: &Value) -> Value {
    publish(ctrl, &DbRequestHandler::new(node(1)), topic, payload, None).await
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

fn refused_as_too_large(reply: &Value) -> bool {
    reply["code"] == json!(400)
        && reply["message"].as_str().is_some_and(|message| {
            message.contains("too large to replicate") && message.contains(&limit_text())
        })
}

fn outcome(reply: &Value) -> String {
    format!(
        "status={} code={} message={}",
        reply["status"], reply["code"], reply["message"]
    )
}

fn stored(ctrl: &NodeController<MockTransport>, entity: &str, id: &str) -> Option<Value> {
    ctrl.db_get(entity, id)
        .map(|record| serde_json::from_slice(&record.data).unwrap())
}

fn forwarded(op: JsonDbOp, entity: &str, id: Option<&str>, payload: &Value) -> JsonDbRequest {
    JsonDbRequest {
        request_id: 9,
        op,
        entity: entity.to_string(),
        id: id.map(str::to_string),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

fn forwarded_replies(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
    ctrl.transport
        .sent_messages()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbResponse(response) => {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn the_controller_refuses_a_record_over_the_frame_limit() {
    let mut ctrl = primary_with_replica();
    ctrl.db_create("items", "kept", b"{\"n\":1}", 1000)
        .await
        .unwrap();
    let writes_before = data_writes_sent(&ctrl);
    let too_big = serde_json::to_vec(&json!({"blob": blob(11 * MIB)})).unwrap();

    let created = ctrl.db_create("items", "r1", &too_big, 1000).await;
    let updated = ctrl.db_update("items", "kept", &too_big, 2000).await;

    let messages: Vec<String> = [created.err(), updated.err()]
        .iter()
        .map(|error| error.map_or_else(|| "accepted".to_string(), |e| e.to_string()))
        .collect();
    assert!(
        messages
            .iter()
            .all(|message| message.contains(&limit_text())),
        "{messages:?}"
    );
    assert!(ctrl.db_get("items", "r1").is_none());
    assert_eq!(ctrl.db_get("items", "kept").unwrap().data, b"{\"n\":1}");
    assert_eq!(data_writes_sent(&ctrl), writes_before);
}

#[tokio::test]
async fn a_create_whose_replication_write_is_over_the_frame_limit_is_refused() {
    let mut ctrl = primary_with_replica();

    let created = request(
        &mut ctrl,
        "$DB/items/create",
        &json!({"id": "r1", "blob": blob(6 * MIB)}),
    )
    .await;

    assert!(refused_as_too_large(&created), "{}", outcome(&created));
    assert!(ctrl.db_get("items", "r1").is_none());
    assert_eq!(data_writes_sent(&ctrl), 0);
}

#[tokio::test]
async fn an_update_whose_replication_write_is_over_the_frame_limit_is_refused() {
    let mut ctrl = primary_with_replica();
    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": "r1", "n": 1})).await;
    assert_eq!(created["status"], "ok");
    let writes_before = data_writes_sent(&ctrl);

    let updated = request(
        &mut ctrl,
        "$DB/items/r1/update",
        &json!({"blob": blob(6 * MIB)}),
    )
    .await;

    assert!(refused_as_too_large(&updated), "{}", outcome(&updated));
    assert_eq!(
        stored(&ctrl, "items", "r1"),
        Some(json!({"id": "r1", "n": 1, "_version": 1}))
    );
    assert_eq!(data_writes_sent(&ctrl), writes_before);
}

#[tokio::test]
async fn a_forwarded_create_over_the_frame_limit_is_refused() {
    let mut ctrl = primary_with_replica();

    ctrl.handle_json_db_request(
        node(2),
        data_partition("items", "r1"),
        &forwarded(
            JsonDbOp::Create,
            "items",
            None,
            &json!({"id": "r1", "blob": blob(6 * MIB)}),
        ),
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert!(
        refused_as_too_large(&replies[0]),
        "{}",
        outcome(&replies[0])
    );
    assert!(ctrl.db_get("items", "r1").is_none());
    assert_eq!(data_writes_sent(&ctrl), 0);
}

#[tokio::test]
async fn a_forwarded_update_over_the_frame_limit_is_refused() {
    let mut ctrl = primary_with_replica();
    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": "r1", "n": 1})).await;
    assert_eq!(created["status"], "ok");
    let writes_before = data_writes_sent(&ctrl);

    ctrl.handle_json_db_request(
        node(2),
        data_partition("items", "r1"),
        &forwarded(
            JsonDbOp::Update,
            "items",
            Some("r1"),
            &json!({"blob": blob(6 * MIB)}),
        ),
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert!(
        refused_as_too_large(&replies[0]),
        "{}",
        outcome(&replies[0])
    );
    assert_eq!(stored(&ctrl, "items", "r1").unwrap()["n"], json!(1));
    assert_eq!(data_writes_sent(&ctrl), writes_before);
}

async fn users_with_unique_email() -> NodeController<MockTransport> {
    let mut ctrl = primary_with_replica();
    ctrl.seed_unique_voters(std::iter::once(node(1)).collect());
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();
    ctrl
}

#[tokio::test]
async fn a_create_over_the_frame_limit_after_its_unique_check_is_refused_and_releases_the_value() {
    let mut ctrl = users_with_unique_email().await;

    let refused = publish_completing_unique(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "email": "a@x", "blob": blob(6 * MIB)}),
    )
    .await;
    let retried = publish_completing_unique(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u2", "email": "a@x"}),
    )
    .await;

    assert!(refused_as_too_large(&refused), "{}", outcome(&refused));
    assert_eq!(retried["status"], "ok", "{retried}");
    assert!(ctrl.db_get("users", "u1").is_none());
}

#[tokio::test]
async fn a_forwarded_update_over_the_frame_limit_after_its_unique_check_is_refused() {
    let mut ctrl = users_with_unique_email().await;
    let created = publish_completing_unique(
        &mut ctrl,
        "$DB/users/create",
        &json!({"id": "u1", "email": "old@x"}),
    )
    .await;
    assert_eq!(created["status"], "ok");

    let Some(PendingConstraintWork::Unique(pending)) = ctrl
        .handle_json_db_request(
            node(2),
            data_partition("users", "u1"),
            &forwarded(
                JsonDbOp::Update,
                "users",
                Some("u1"),
                &json!({"email": "a@x", "blob": blob(6 * MIB)}),
            ),
        )
        .await
    else {
        panic!("the update should wait on its unique reserve");
    };
    ctrl.complete_pending_unique_work(
        pending.phase1.local_reserved,
        Ok(Vec::new()),
        pending.continuation,
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert!(
        refused_as_too_large(&replies[0]),
        "{}",
        outcome(&replies[0])
    );
    assert_eq!(
        stored(&ctrl, "users", "u1").unwrap()["email"],
        json!("old@x")
    );
}

#[tokio::test]
async fn a_create_over_the_frame_limit_after_a_remote_fk_check_is_refused() {
    let remote = data_partition("users", "u9");
    let mut ctrl = controller_where(
        None,
        |partition| {
            if partition == remote {
                node(2)
            } else {
                node(1)
            }
        },
        &[node(2)],
    );
    ctrl.constraint_add(
        &ClusterConstraint::foreign_key(
            "posts",
            "posts_author_fk",
            "author_id",
            "users",
            "id",
            OnDeleteAction::Cascade,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let handler = DbRequestHandler::new(node(1));
    let payload = json!({"id": "p1", "author_id": "u9", "blob": blob(6 * MIB)});
    let bytes = serde_json::to_vec(&payload).unwrap();
    let DbPublishResult::PendingFkCheck(pending) = handler
        .handle_publish(&mut ctrl, "$DB/posts/create", &bytes, &response_ctx(None))
        .await
    else {
        panic!("the create should wait for the remote FK check");
    };

    let FkCheckCompletion::Done(Some(response)) = handler
        .complete_pending_fk_check(&mut ctrl, true, None, pending.continuation)
        .await
    else {
        panic!("the create should be answered once the FK check passes");
    };

    let reply: Value = serde_json::from_slice(&response.payload).unwrap();
    assert!(refused_as_too_large(&reply), "{}", outcome(&reply));
    assert!(ctrl.db_get("posts", "p1").is_none());
}

fn vault_handler(ctrl: &mut NodeController<MockTransport>) -> DbRequestHandler {
    let mut fields = std::collections::HashMap::new();
    fields.insert("notes".to_string(), "userId".to_string());
    let ownership = Arc::new(mqdb_core::types::OwnershipConfig::new(fields));
    ctrl.set_ownership(Arc::clone(&ownership));
    let key_store = Arc::new(mqdb_vault::VaultKeyStore::new());
    let (_, key) = mqdb_vault::VaultCrypto::derive_with_raw_key(
        "passphrase",
        &mqdb_vault::VaultCrypto::generate_salt(),
    );
    key_store.set("alice", key);
    DbRequestHandler::new(node(1))
        .with_ownership(ownership)
        .with_vault_key_store(key_store)
}

#[tokio::test]
async fn a_record_that_fits_in_plaintext_is_refused_once_the_vault_expands_it() {
    let mut ctrl = primary_with_replica();
    let handler = vault_handler(&mut ctrl);
    let body = blob(4 * MIB - 1024);

    let plain = publish(
        &mut ctrl,
        &handler,
        "$DB/notes/create",
        &json!({"id": "n1", "userId": "bob", "body": body}),
        Some("bob"),
    )
    .await;
    let encrypted = publish(
        &mut ctrl,
        &handler,
        "$DB/notes/create",
        &json!({"id": "n2", "userId": "alice", "body": body}),
        Some("alice"),
    )
    .await;

    assert_eq!(plain["status"], "ok", "{}", outcome(&plain));
    assert!(refused_as_too_large(&encrypted), "{}", outcome(&encrypted));
    assert!(ctrl.db_get("notes", "n2").is_none());
}

fn frame_len_of(ctrl: &NodeController<MockTransport>, id: &str, blob_len: usize) -> usize {
    let data = serde_json::to_vec(&json!({"blob": blob(blob_len)})).unwrap();
    let (_, write) = ctrl.db_create_prepare("items", id, &data, 1000).unwrap();
    super::super::catchup::replication_frame_len(&write).unwrap()
}

#[tokio::test]
async fn a_record_whose_frame_is_exactly_the_limit_is_stored_and_one_byte_more_is_refused() {
    let mut ctrl = primary_with_replica();
    let probe = frame_len_of(&ctrl, "r1", MIB);
    let at_limit = MIB + (CLIENT_WRITE_LIMIT - probe);
    assert_eq!(frame_len_of(&ctrl, "r1", at_limit), CLIENT_WRITE_LIMIT);

    let over = serde_json::to_vec(&json!({"blob": blob(at_limit + 1)})).unwrap();
    let refused = ctrl.db_create("items", "r2", &over, 1000).await;
    let fits = serde_json::to_vec(&json!({"blob": blob(at_limit)})).unwrap();
    let stored = ctrl.db_create("items", "r1", &fits, 1000).await;

    assert_eq!(
        refused.err(),
        Some(crate::cluster::db::DbDataStoreError::TooLarge {
            size: CLIENT_WRITE_LIMIT + 1,
            limit: CLIENT_WRITE_LIMIT,
        })
    );
    assert!(stored.is_ok());
    assert_eq!(data_writes_sent(&ctrl), 1);
}

#[test]
fn the_frame_length_is_a_catchup_response_carrying_the_write_alone() {
    let ctrl = primary_with_replica();
    let data = serde_json::to_vec(&json!({"blob": blob(1024)})).unwrap();
    let (_, write) = ctrl.db_create_prepare("items", "r1", &data, 1000).unwrap();
    let write = write.with_event(Some(crate::cluster::protocol::ChangeEventRecord {
        topic: "$DB/items/events/created".to_string(),
        payload: data.clone(),
        user_properties: vec![("k".to_string(), "v".to_string())],
        order_key: "items/r1".to_string(),
    }));
    let encoded_len = |message: &ClusterMessage| {
        let mut payload = Vec::new();
        message.encode_payload(&mut payload);
        crate::cluster::transport::MESSAGE_HEADER_LEN + payload.len()
    };
    let catchup = ClusterMessage::CatchupResponse(crate::cluster::protocol::CatchupResponse {
        partition: write.partition,
        responder_id: node(1),
        writes: vec![write.clone()],
        covers_through: Some(u64::MAX),
        answers_from: Some(u64::MAX),
    });
    let frame_len = super::super::catchup::replication_frame_len(&write).unwrap();

    assert_eq!(frame_len, encoded_len(&catchup));
    assert!(frame_len >= encoded_len(&ClusterMessage::Write(write.clone())));
    assert!(frame_len >= encoded_len(&ClusterMessage::WriteRequest(write)));
}

fn reported_size(reply: &Value) -> usize {
    let message = reply["message"].as_str().unwrap();
    let start = message.find('(').unwrap() + 1;
    let end = message.find(" bytes").unwrap();
    message[start..end].parse().unwrap()
}

async fn child_at_the_limit(
    ctrl: &mut NodeController<MockTransport>,
    entity: &str,
    fk_value: &str,
) {
    let mut guess = 6 * MIB;
    for _ in 0..8 {
        let attempt = request(
            ctrl,
            &format!("$DB/{entity}/create"),
            &json!({"id": "c1", "author_id": fk_value, "blob": blob(guess)}),
        )
        .await;
        if attempt["status"] == "ok" {
            return;
        }
        assert!(refused_as_too_large(&attempt), "{}", outcome(&attempt));
        let over = reported_size(&attempt) - CLIENT_WRITE_LIMIT;
        guess -= over.div_ceil(2);
    }
    panic!("no child could be created at the limit");
}

#[tokio::test]
async fn r388_set_null_cascade_on_a_child_near_the_limit_leaves_a_dangling_reference() {
    let mut ctrl = primary_with_replica();
    ctrl.constraint_add(
        &ClusterConstraint::foreign_key(
            "posts",
            "posts_author_fk",
            "author_id",
            "users",
            "id",
            OnDeleteAction::SetNull,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let parent = request(&mut ctrl, "$DB/users/create", &json!({"id": "u"})).await;
    assert_eq!(parent["status"], "ok");
    child_at_the_limit(&mut ctrl, "posts", "u").await;

    let deleted = request(&mut ctrl, "$DB/users/u/delete", &json!({})).await;
    let child = stored(&ctrl, "posts", "c1").unwrap();
    eprintln!(
        "R388 parent delete: {} | parent stored={} | child author_id={}",
        outcome(&deleted),
        ctrl.db_get("users", "u").is_some(),
        child["author_id"]
    );
    assert!(
        child["author_id"].is_null() || ctrl.db_get("users", "u").is_some(),
        "parent deleted but child still references it"
    );
}

#[tokio::test]
async fn r388_a_record_stored_at_the_limit_cannot_be_deleted_through_json() {
    let mut ctrl = primary_with_replica();
    let probe = frame_len_of(&ctrl, "r1", MIB);
    let at_limit = MIB + (crate::cluster::transport::MAX_MESSAGE_SIZE - probe);
    let fits = serde_json::to_vec(&json!({"blob": blob(at_limit)})).unwrap();
    crate::cluster::test_support::put_record(&ctrl.stores().db_data, "items", "r1", &fits, 1000);

    let deleted = request(&mut ctrl, "$DB/items/r1/delete", &json!({})).await;
    eprintln!(
        "R388 delete of a stored record: {} | still stored={}",
        outcome(&deleted),
        ctrl.db_get("items", "r1").is_some()
    );
    assert_eq!(deleted["status"], "ok", "{}", outcome(&deleted));
}

fn cascade_request(op: JsonDbOp, entity: &str, id: &str, payload: &Value) -> JsonDbRequest {
    JsonDbRequest {
        response_topic: "_mqdb/cascade_ack/2/7".to_string(),
        ..forwarded(op, entity, Some(id), payload)
    }
}

#[tokio::test]
async fn a_remote_set_null_on_a_child_near_the_limit_is_acknowledged() {
    let mut ctrl = primary_with_replica();
    child_at_the_limit(&mut ctrl, "posts", "u").await;

    ctrl.handle_json_db_request(
        node(2),
        data_partition("posts", "c1"),
        &cascade_request(
            JsonDbOp::Update,
            "posts",
            "c1",
            &json!({"author_id": null, "__mqdb_fk_expected": "author_id=u"}),
        ),
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert_eq!(replies[0]["status"], "ok", "{}", outcome(&replies[0]));
    assert!(stored(&ctrl, "posts", "c1").unwrap()["author_id"].is_null());
}

#[tokio::test]
async fn a_remote_cascade_delete_of_a_record_at_the_frame_limit_is_acknowledged() {
    let mut ctrl = primary_with_replica();
    let probe = frame_len_of(&ctrl, "c1", MIB);
    let at_limit = MIB + (crate::cluster::transport::MAX_MESSAGE_SIZE - probe);
    let fits = serde_json::to_vec(&json!({"blob": blob(at_limit)})).unwrap();
    crate::cluster::test_support::put_record(&ctrl.stores().db_data, "posts", "c1", &fits, 1000);

    ctrl.handle_json_db_request(
        node(2),
        data_partition("posts", "c1"),
        &cascade_request(JsonDbOp::Delete, "posts", "c1", &json!({})),
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert_eq!(replies[0]["status"], "ok", "{}", outcome(&replies[0]));
    assert!(ctrl.db_get("posts", "c1").is_none());
}

#[tokio::test]
async fn a_delete_too_large_with_its_record_replicates_an_event_without_it() {
    let mut ctrl = primary_with_replica();
    let probe = frame_len_of(&ctrl, "r1", MIB);
    let at_limit = MIB + (crate::cluster::transport::MAX_MESSAGE_SIZE - probe);
    let fits = serde_json::to_vec(&json!({"blob": blob(at_limit)})).unwrap();
    crate::cluster::test_support::put_record(&ctrl.stores().db_data, "items", "r1", &fits, 1000);

    let deleted = request(&mut ctrl, "$DB/items/r1/delete", &json!({})).await;

    let sent = ctrl.transport.sent_messages();
    let write = sent
        .iter()
        .rev()
        .find_map(|(_, message)| match message {
            ClusterMessage::Write(w) if w.entity == crate::cluster::entity::DB_DATA => Some(w),
            _ => None,
        })
        .unwrap();
    let event: Value = serde_json::from_slice(&write.event.as_ref().unwrap().payload).unwrap();
    assert_eq!(deleted["status"], "ok", "{}", outcome(&deleted));
    assert!(
        super::super::catchup::replication_frame_len(write).unwrap()
            <= crate::cluster::transport::MAX_MESSAGE_SIZE
    );
    assert_eq!(
        (&event["id"], &event["operation"], &event["data"]),
        (&json!("r1"), &json!("Delete"), &Value::Null)
    );
}

fn largest_data_write_frame(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport
        .sent_messages()
        .iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::Write(w) if w.entity == crate::cluster::entity::DB_DATA => {
                crate::cluster::node_controller::catchup::replication_frame_len(w)
            }
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

#[tokio::test]
async fn an_update_that_adds_null_fields_past_the_frame_limit_is_refused() {
    let mut ctrl = primary_with_replica();
    let created = request(&mut ctrl, "$DB/items/create", &json!({"id": "r1", "n": 1})).await;
    assert_eq!(created["status"], "ok");
    let mut refused = 0;
    for round in 0..3 {
        let mut fields = serde_json::Map::new();
        fields.insert(format!("k{round}_{}", blob(4 * MIB)), Value::Null);
        let reply = request(&mut ctrl, "$DB/items/r1/update", &Value::Object(fields)).await;
        if refused_as_too_large(&reply) {
            refused += 1;
        }
    }

    let largest = largest_data_write_frame(&ctrl);
    assert!(
        refused > 0 && largest <= crate::cluster::transport::MAX_MESSAGE_SIZE,
        "{refused} updates refused, largest replicated write {largest} bytes"
    );
}

#[tokio::test]
async fn a_null_for_a_field_the_record_lacks_is_not_a_clearing_update() {
    let mut ctrl = primary_with_replica();
    let record = json!({"id": "r1", "blob": blob(4 * MIB + 900 * 1024)});
    let created = request(&mut ctrl, "$DB/items/create", &record).await;
    assert_eq!(created["status"], "ok", "{}", outcome(&created));
    let writes_before = data_writes_sent(&ctrl);
    let mut fields = serde_json::Map::new();
    fields.insert(format!("pad_{}", blob(200 * 1024)), Value::Null);

    let updated = request(&mut ctrl, "$DB/items/r1/update", &Value::Object(fields)).await;

    assert!(refused_as_too_large(&updated), "{}", outcome(&updated));
    assert_eq!(data_writes_sent(&ctrl), writes_before);
}

async fn posts_with_set_null_author(ctrl: &mut NodeController<MockTransport>) {
    ctrl.constraint_add(
        &ClusterConstraint::foreign_key(
            "posts",
            "posts_author_fk",
            "author_id",
            "users",
            "id",
            OnDeleteAction::SetNull,
        )
        .unwrap(),
    )
    .await
    .unwrap();
}

fn store_child_over_the_frame_limit(ctrl: &NodeController<MockTransport>) {
    let oversized =
        serde_json::to_vec(&json!({"id": "c1", "author_id": "u", "blob": blob(11 * MIB)})).unwrap();
    crate::cluster::test_support::put_record(
        &ctrl.stores().db_data,
        "posts",
        "c1",
        &oversized,
        1000,
    );
}

#[tokio::test]
async fn a_local_set_null_on_a_child_over_the_frame_limit_is_applied() {
    let mut ctrl = primary_with_replica();
    posts_with_set_null_author(&mut ctrl).await;
    let parent = request(&mut ctrl, "$DB/users/create", &json!({"id": "u"})).await;
    assert_eq!(parent["status"], "ok");
    let child = request(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "c1", "author_id": "u"}),
    )
    .await;
    assert_eq!(child["status"], "ok");
    store_child_over_the_frame_limit(&ctrl);

    let deleted = request(&mut ctrl, "$DB/users/u/delete", &json!({})).await;

    assert_eq!(deleted["status"], "ok", "{}", outcome(&deleted));
    assert!(stored(&ctrl, "posts", "c1").unwrap()["author_id"].is_null());
}

#[tokio::test]
async fn a_remote_set_null_on_a_child_over_the_frame_limit_is_acknowledged() {
    let mut ctrl = primary_with_replica();
    store_child_over_the_frame_limit(&ctrl);

    ctrl.handle_json_db_request(
        node(2),
        data_partition("posts", "c1"),
        &cascade_request(
            JsonDbOp::Update,
            "posts",
            "c1",
            &json!({"author_id": null, "__mqdb_fk_expected": "author_id=u"}),
        ),
    )
    .await;

    let replies = forwarded_replies(&ctrl);
    assert_eq!(replies[0]["status"], "ok", "{}", outcome(&replies[0]));
    assert!(stored(&ctrl, "posts", "c1").unwrap()["author_id"].is_null());
}

#[tokio::test]
async fn clearing_numeric_fields_cannot_grow_a_record_past_the_frame_limit() {
    let mut ctrl = primary_with_replica();
    let mut record = serde_json::Map::new();
    record.insert("id".to_string(), json!("r1"));
    for n in 0..30_000 {
        record.insert(format!("k{n:06}"), json!(0));
    }
    record.insert("blob".to_string(), json!(""));
    let probe = serde_json::to_vec(&Value::Object(record.clone())).unwrap();
    let (_, write) = ctrl.db_create_prepare("items", "r1", &probe, 1000).unwrap();
    let overhead = super::super::catchup::replication_frame_len(&write).unwrap() - probe.len();
    let blob_len = CLIENT_WRITE_LIMIT - overhead - probe.len() - 1000;
    record.insert("blob".to_string(), json!(blob(blob_len)));
    let data = serde_json::to_vec(&Value::Object(record)).unwrap();
    ctrl.db_create("items", "r1", &data, 1000).await.unwrap();
    let mut clear = serde_json::Map::new();
    for n in 0..30_000 {
        clear.insert(format!("k{n:06}"), Value::Null);
    }

    let reply = request(&mut ctrl, "$DB/items/r1/update", &Value::Object(clear)).await;

    assert!(refused_as_too_large(&reply), "{}", outcome(&reply));
    assert!(largest_data_write_frame(&ctrl) <= crate::cluster::transport::MAX_MESSAGE_SIZE);
}
