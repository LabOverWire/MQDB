// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::{controller_where, publish_completing_unique, response_ctx};
use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition, unique_partition};
use crate::cluster::db_handler::{
    DbPublishResponse, DbPublishResult, DbRequestHandler, FkCheckCompletion,
};
use serde_json::{Value, json};

const REMOTE_AUTHOR: &str = "u9";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn encoded(value: &str) -> Vec<u8> {
    serde_json::to_vec(&json!(value)).unwrap()
}

fn remote_partitions() -> [PartitionId; 3] {
    [
        unique_partition("users", "email", &encoded("new@x")),
        unique_partition("users", "email", &encoded("b@x")),
        data_partition("users", REMOTE_AUTHOR),
    ]
}

async fn cluster() -> NodeController<MockTransport> {
    let remote = remote_partitions();
    for local in [
        data_partition("users", "u1"),
        data_partition("users", "u2"),
        data_partition("users", "u3"),
        data_partition("posts", "p1"),
        unique_partition("users", "email", &encoded("old@x")),
        unique_partition("users", "email", &encoded("u2@x")),
        unique_partition("users", "email", &encoded("u3@x")),
    ] {
        assert!(!remote.contains(&local));
    }
    let mut ctrl = controller_where(
        None,
        |partition| {
            if remote.contains(&partition) {
                node(2)
            } else {
                node(1)
            }
        },
        &[],
    );
    ctrl.seed_unique_voters(std::iter::once(node(1)).collect());
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();
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
    for (topic, payload) in [
        (
            "$DB/users/create",
            json!({"id": "u1", "email": "old@x", "name": "n0"}),
        ),
        ("$DB/users/create", json!({"id": "u2", "email": "u2@x"})),
        ("$DB/users/create", json!({"id": "u3", "email": "u3@x"})),
        (
            "$DB/posts/create",
            json!({"id": "p1", "author_id": "u2", "title": "t0"}),
        ),
    ] {
        let reply = publish_completing_unique(&mut ctrl, topic, &payload).await;
        assert_eq!(reply["status"], "ok", "{reply}");
    }
    ctrl
}

fn stored(ctrl: &NodeController<MockTransport>, entity: &str, id: &str) -> Option<Value> {
    ctrl.db_get(entity, id)
        .map(|record| serde_json::from_slice(&record.data).unwrap())
}

fn forwarded_update(request_id: u64, entity: &str, id: &str, payload: &Value) -> JsonDbRequest {
    JsonDbRequest {
        request_id,
        op: JsonDbOp::Update,
        entity: entity.to_string(),
        id: Some(id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: format!("resp/{request_id}"),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

async fn forward(
    ctrl: &mut NodeController<MockTransport>,
    request: &JsonDbRequest,
) -> Option<PendingConstraintWork> {
    let id = request.id.as_deref().unwrap();
    ctrl.handle_json_db_request(node(2), data_partition(&request.entity, id), request)
        .await
}

fn forwarded_reply(ctrl: &NodeController<MockTransport>, request_id: u64) -> Value {
    let topic = format!("resp/{request_id}");
    let payload = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .rev()
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response)
                if to == node(2) && response.response_topic == topic =>
            {
                Some(response.payload)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no reply on {topic}"));
    serde_json::from_slice(&payload).unwrap()
}

async fn finish_forwarded_unique(
    ctrl: &mut NodeController<MockTransport>,
    work: PendingUniqueWork,
    confirmed: RemoteReservations,
) {
    ctrl.complete_pending_unique_work(work.phase1.local_reserved, Ok(confirmed), work.continuation)
        .await;
}

async fn finish_forwarded(ctrl: &mut NodeController<MockTransport>, work: PendingConstraintWork) {
    match work {
        PendingConstraintWork::Unique(work) => {
            finish_forwarded_unique(ctrl, work, Vec::new()).await;
        }
        PendingConstraintWork::Fk(work) => {
            if let FkThenUniqueOutcome::NeedUnique(unique) = ctrl
                .complete_pending_fk_work(true, None, work.continuation)
                .await
            {
                finish_forwarded_unique(ctrl, *unique, Vec::new()).await;
            }
        }
        PendingConstraintWork::FkDelete(_) => panic!("an update does not wait on a delete lookup"),
    }
}

async fn publish(
    handler: &DbRequestHandler,
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &Value,
) -> DbPublishResult {
    let bytes = serde_json::to_vec(payload).unwrap();
    handler
        .handle_publish(ctrl, topic, &bytes, &response_ctx(None))
        .await
}

fn reply(response: Option<DbPublishResponse>) -> Value {
    let response = response.unwrap_or_else(|| panic!("the pending update was not answered"));
    serde_json::from_slice(&response.payload).unwrap()
}

async fn finish_published_unique(
    handler: &DbRequestHandler,
    ctrl: &mut NodeController<MockTransport>,
    work: PendingUniqueWork,
    confirmed: RemoteReservations,
) -> Value {
    reply(
        handler
            .complete_pending_unique_check(
                ctrl,
                work.phase1.local_reserved,
                Ok(confirmed),
                work.continuation,
            )
            .await,
    )
}

async fn finish_published(
    handler: &DbRequestHandler,
    ctrl: &mut NodeController<MockTransport>,
    pending: DbPublishResult,
) -> Value {
    match pending {
        DbPublishResult::PendingUniqueCheck(work) => {
            finish_published_unique(handler, ctrl, *work, Vec::new()).await
        }
        DbPublishResult::PendingFkCheck(work) => {
            match handler
                .complete_pending_fk_check(ctrl, true, None, work.continuation)
                .await
            {
                FkCheckCompletion::Done(response) => reply(response),
                FkCheckCompletion::NeedUniqueCheck(unique) => {
                    finish_published_unique(handler, ctrl, *unique, Vec::new()).await
                }
            }
        }
        DbPublishResult::Response(_)
        | DbPublishResult::NoResponse
        | DbPublishResult::PendingFkDelete(_) => panic!("the update did not wait on a check"),
    }
}

fn versions(replies: &[&Value]) -> Vec<Value> {
    replies
        .iter()
        .map(|reply| reply["data"]["_version"].clone())
        .collect()
}

#[tokio::test]
async fn forwarded_pending_unique_update_keeps_a_concurrent_other_field_update() {
    let mut ctrl = cluster().await;
    let Some(PendingConstraintWork::Unique(work)) = forward(
        &mut ctrl,
        &forwarded_update(80, "users", "u1", &json!({"email": "new@x"})),
    )
    .await
    else {
        panic!("the email update should wait on the remote unique reserve");
    };
    assert!(
        forward(
            &mut ctrl,
            &forwarded_update(81, "users", "u1", &json!({"name": "n1"}))
        )
        .await
        .is_none()
    );

    finish_forwarded_unique(&mut ctrl, work, Vec::new()).await;

    let record = stored(&ctrl, "users", "u1").unwrap();
    assert_eq!(
        (
            &record["email"],
            &record["name"],
            versions(&[&forwarded_reply(&ctrl, 81), &forwarded_reply(&ctrl, 80)]),
        ),
        (&json!("new@x"), &json!("n1"), vec![json!(2), json!(3)]),
        "{record}"
    );
}

#[tokio::test]
async fn published_pending_unique_update_keeps_a_concurrent_other_field_update() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/users/u1/update",
        &json!({"email": "new@x"}),
    )
    .await;
    assert!(matches!(pending, DbPublishResult::PendingUniqueCheck(_)));
    let name =
        publish_completing_unique(&mut ctrl, "$DB/users/u1/update", &json!({"name": "n1"})).await;

    let email = finish_published(&handler, &mut ctrl, pending).await;

    let record = stored(&ctrl, "users", "u1").unwrap();
    assert_eq!(
        (
            &record["email"],
            &record["name"],
            versions(&[&name, &email])
        ),
        (&json!("new@x"), &json!("n1"), vec![json!(2), json!(3)]),
        "{record}"
    );
}

#[tokio::test]
async fn forwarded_pending_fk_update_keeps_a_concurrent_other_field_update() {
    let mut ctrl = cluster().await;
    let Some(work @ PendingConstraintWork::Fk(_)) = forward(
        &mut ctrl,
        &forwarded_update(90, "posts", "p1", &json!({"author_id": REMOTE_AUTHOR})),
    )
    .await
    else {
        panic!("the author update should wait on the remote FK check");
    };
    assert!(
        forward(
            &mut ctrl,
            &forwarded_update(91, "posts", "p1", &json!({"title": "t1"}))
        )
        .await
        .is_none()
    );

    finish_forwarded(&mut ctrl, work).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (
            &record["author_id"],
            &record["title"],
            versions(&[&forwarded_reply(&ctrl, 91), &forwarded_reply(&ctrl, 90)]),
        ),
        (
            &json!(REMOTE_AUTHOR),
            &json!("t1"),
            vec![json!(2), json!(3)]
        ),
        "{record}"
    );
}

#[tokio::test]
async fn published_pending_fk_update_keeps_a_concurrent_other_field_update() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": REMOTE_AUTHOR}),
    )
    .await;
    assert!(matches!(pending, DbPublishResult::PendingFkCheck(_)));
    let title =
        publish_completing_unique(&mut ctrl, "$DB/posts/p1/update", &json!({"title": "t1"})).await;

    let author = finish_published(&handler, &mut ctrl, pending).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (
            &record["author_id"],
            &record["title"],
            versions(&[&title, &author])
        ),
        (
            &json!(REMOTE_AUTHOR),
            &json!("t1"),
            vec![json!(2), json!(3)]
        ),
        "{record}"
    );
}

#[tokio::test]
async fn forwarded_pending_fk_update_conflicts_with_a_concurrent_update_of_the_same_field() {
    let mut ctrl = cluster().await;
    let Some(work) = forward(
        &mut ctrl,
        &forwarded_update(100, "posts", "p1", &json!({"author_id": REMOTE_AUTHOR})),
    )
    .await
    else {
        panic!("the author update should wait on the remote FK check");
    };
    assert!(
        forward(
            &mut ctrl,
            &forwarded_update(101, "posts", "p1", &json!({"author_id": "u3"}))
        )
        .await
        .is_none()
    );
    assert_eq!(forwarded_reply(&ctrl, 101)["status"], "ok");

    finish_forwarded(&mut ctrl, work).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (
            &forwarded_reply(&ctrl, 100)["code"],
            &record["author_id"],
            &record["_version"]
        ),
        (&json!(409), &json!("u3"), &json!(2)),
        "{record}"
    );
}

#[tokio::test]
async fn published_pending_fk_update_conflicts_with_a_concurrent_update_of_the_same_field() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": REMOTE_AUTHOR}),
    )
    .await;
    let moved = publish_completing_unique(
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": "u3"}),
    )
    .await;
    assert_eq!(moved["status"], "ok", "{moved}");

    let author = finish_published(&handler, &mut ctrl, pending).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (&author["code"], &record["author_id"], &record["_version"]),
        (&json!(409), &json!("u3"), &json!(2)),
        "{author} {record}"
    );
}

fn releases_sent_to_node2(ctrl: &NodeController<MockTransport>) -> Vec<Vec<u8>> {
    ctrl.transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::UniqueReleaseRequest(request) if to == node(2) => Some(request.value),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn second_of_two_pending_unique_updates_of_one_field_conflicts_and_releases_its_claim() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let first = publish(
        &handler,
        &mut ctrl,
        "$DB/users/u1/update",
        &json!({"email": "new@x"}),
    )
    .await;
    let second = publish(
        &handler,
        &mut ctrl,
        "$DB/users/u1/update",
        &json!({"email": "b@x"}),
    )
    .await;
    let (DbPublishResult::PendingUniqueCheck(first), DbPublishResult::PendingUniqueCheck(second)) =
        (first, second)
    else {
        panic!("both email updates should wait on the remote unique reserve");
    };

    let first_reply = finish_published_unique(
        &handler,
        &mut ctrl,
        *first,
        vec![("email".to_string(), encoded("new@x"), node(2))],
    )
    .await;
    let second_reply = finish_published_unique(
        &handler,
        &mut ctrl,
        *second,
        vec![("email".to_string(), encoded("b@x"), node(2))],
    )
    .await;

    let record = stored(&ctrl, "users", "u1").unwrap();
    assert_eq!(
        (
            &first_reply["status"],
            &second_reply["code"],
            &record["email"],
            &record["_version"],
            releases_sent_to_node2(&ctrl).contains(&encoded("b@x")),
        ),
        (&json!("ok"), &json!(409), &json!("new@x"), &json!(2), true),
        "{second_reply} {record}"
    );
}

#[tokio::test]
async fn pending_update_of_a_record_deleted_during_the_wait_is_not_found() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": REMOTE_AUTHOR}),
    )
    .await;
    let deleted = publish_completing_unique(&mut ctrl, "$DB/posts/p1/delete", &json!({})).await;
    assert_eq!(deleted["status"], "ok", "{deleted}");

    let author = finish_published(&handler, &mut ctrl, pending).await;

    assert_eq!(
        (&author["code"], stored(&ctrl, "posts", "p1")),
        (&json!(404), None),
        "{author}"
    );
}

#[tokio::test]
async fn pending_update_keeps_a_record_recreated_during_the_wait() {
    let mut ctrl = cluster().await;
    let Some(work) = forward(
        &mut ctrl,
        &forwarded_update(110, "posts", "p1", &json!({"author_id": REMOTE_AUTHOR})),
    )
    .await
    else {
        panic!("the author update should wait on the remote FK check");
    };
    for (topic, payload) in [
        ("$DB/posts/p1/delete", json!({})),
        (
            "$DB/posts/create",
            json!({"id": "p1", "author_id": "u2", "title": "fresh"}),
        ),
    ] {
        let reply = publish_completing_unique(&mut ctrl, topic, &payload).await;
        assert_eq!(reply["status"], "ok", "{reply}");
    }

    finish_forwarded(&mut ctrl, work).await;

    assert_eq!(
        (
            stored(&ctrl, "posts", "p1"),
            forwarded_reply(&ctrl, 110)["status"].clone()
        ),
        (
            Some(json!({"id": "p1", "author_id": REMOTE_AUTHOR, "title": "fresh", "_version": 2})),
            json!("ok")
        )
    );
}

#[tokio::test]
async fn forwarded_pending_update_with_an_expected_version_fails_when_the_record_moved() {
    let mut ctrl = cluster().await;
    let Some(work) = forward(
        &mut ctrl,
        &forwarded_update(
            120,
            "posts",
            "p1",
            &json!({"author_id": REMOTE_AUTHOR, "_expected_version": 1}),
        ),
    )
    .await
    else {
        panic!("the author update should wait on the remote FK check");
    };
    assert!(
        forward(
            &mut ctrl,
            &forwarded_update(121, "posts", "p1", &json!({"title": "t1"}))
        )
        .await
        .is_none()
    );

    finish_forwarded(&mut ctrl, work).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (
            &forwarded_reply(&ctrl, 120)["code"],
            &record["author_id"],
            &record["title"]
        ),
        (&json!(412), &json!("u2"), &json!("t1")),
        "{record}"
    );
}

#[tokio::test]
async fn published_pending_update_with_an_expected_version_fails_when_the_record_moved() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": REMOTE_AUTHOR, "_expected_version": 1}),
    )
    .await;
    let title =
        publish_completing_unique(&mut ctrl, "$DB/posts/p1/update", &json!({"title": "t1"})).await;
    assert_eq!(title["status"], "ok", "{title}");

    let author = finish_published(&handler, &mut ctrl, pending).await;

    let record = stored(&ctrl, "posts", "p1").unwrap();
    assert_eq!(
        (&author["code"], &record["author_id"], &record["title"]),
        (&json!(412), &json!("u2"), &json!("t1")),
        "{author} {record}"
    );
}

#[tokio::test]
async fn pending_set_null_whose_field_moved_keeps_the_new_value_and_acks_the_cascade() {
    let mut ctrl = cluster().await;
    let created = publish_completing_unique(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p1x", "author_id": "u2", "editor_id": REMOTE_AUTHOR}),
    )
    .await;
    assert_eq!(created["status"], "ok", "{created}");
    assert_ne!(
        data_partition("posts", "p1x"),
        data_partition("users", REMOTE_AUTHOR)
    );
    ctrl.constraint_add(
        &ClusterConstraint::foreign_key(
            "posts",
            "posts_editor_fk",
            "editor_id",
            "users",
            "id",
            OnDeleteAction::SetNull,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let handler = DbRequestHandler::new(node(1));
    let Some(set_null) = forward(
        &mut ctrl,
        &forwarded_update(
            140,
            "posts",
            "p1x",
            &json!({"author_id": null, "__mqdb_fk_expected": "author_id=u2"}),
        ),
    )
    .await
    else {
        panic!("the set-null should wait on the remote editor FK check");
    };
    let moved = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1x/update",
        &json!({"author_id": "u3"}),
    )
    .await;
    let moved = finish_published(&handler, &mut ctrl, moved).await;
    assert_eq!(moved["status"], "ok", "{moved}");

    finish_forwarded(&mut ctrl, set_null).await;
    let (ack_tx, ack_rx) = oneshot::channel();
    ctrl.pending_constraints.insert_cascade_ack(77, ack_tx);
    ctrl.handle_json_db_response(&JsonDbResponse::new(
        77,
        serde_json::to_vec(&forwarded_reply(&ctrl, 140)).unwrap(),
        "_mqdb/cascade_ack/1/77".to_string(),
        None,
    ))
    .await;

    assert_eq!(
        (
            stored(&ctrl, "posts", "p1x").unwrap()["author_id"].clone(),
            ack_rx.await
        ),
        (json!("u3"), Ok(true))
    );
}

async fn publish_as(
    handler: &DbRequestHandler,
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &Value,
    sender: &str,
) -> DbPublishResult {
    let bytes = serde_json::to_vec(payload).unwrap();
    handler
        .handle_publish(ctrl, topic, &bytes, &response_ctx(Some(sender)))
        .await
}

#[tokio::test]
async fn published_pending_fk_update_under_vault_keeps_a_concurrent_update_readable_by_its_owner() {
    let mut ctrl = cluster().await;
    let ownership = Arc::new(OwnershipConfig::new(
        [("posts".to_string(), "owner".to_string())]
            .into_iter()
            .collect(),
    ));
    ctrl.set_ownership(Arc::clone(&ownership));
    let key_store = Arc::new(mqdb_vault::VaultKeyStore::new());
    let (crypto, raw_key) = mqdb_vault::VaultCrypto::derive_with_raw_key(
        "passphrase",
        &mqdb_vault::VaultCrypto::generate_salt(),
    );
    key_store.set("alice", raw_key);
    let handler = DbRequestHandler::new(node(1))
        .with_ownership(Arc::clone(&ownership))
        .with_vault_key_store(key_store);
    let DbPublishResult::Response(created) = publish_as(
        &handler,
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "pv", "owner": "alice", "author_id": "u2", "title": "t0"}),
        "alice",
    )
    .await
    else {
        panic!("the vault create should be answered at once");
    };
    let created: Value = serde_json::from_slice(&created.payload).unwrap();
    assert_eq!(created["status"], "ok", "{created}");

    let pending = publish_as(
        &handler,
        &mut ctrl,
        "$DB/posts/pv/update",
        &json!({"author_id": REMOTE_AUTHOR}),
        "alice",
    )
    .await;
    assert!(matches!(pending, DbPublishResult::PendingFkCheck(_)));
    let DbPublishResult::Response(title) = publish_as(
        &handler,
        &mut ctrl,
        "$DB/posts/pv/update",
        &json!({"title": "t1"}),
        "alice",
    )
    .await
    else {
        panic!("the title update should be answered at once");
    };
    let title: Value = serde_json::from_slice(&title.payload).unwrap();

    let author = finish_published(&handler, &mut ctrl, pending).await;

    let raw = stored(&ctrl, "posts", "pv").unwrap();
    let mut readable = raw.clone();
    mqdb_vault::transform::vault_decrypt_fields(
        &crypto,
        "posts",
        "pv",
        &mut readable,
        &mqdb_vault::transform::build_vault_skip_fields("posts", &ownership),
    );
    assert_eq!(
        (
            raw["title"] != json!("t1"),
            &readable["author_id"],
            &readable["title"],
            &readable["_version"],
            versions(&[&title, &author]),
            &author["data"]["title"],
        ),
        (
            true,
            &json!(REMOTE_AUTHOR),
            &json!("t1"),
            &json!(3),
            vec![json!(2), json!(3)],
            &json!("t1"),
        ),
        "{raw} {author}"
    );
}

#[tokio::test]
async fn published_pending_update_is_validated_against_the_schema_when_it_completes() {
    let mut ctrl = cluster().await;
    let handler = DbRequestHandler::new(node(1));
    let pending = publish(
        &handler,
        &mut ctrl,
        "$DB/posts/p1/update",
        &json!({"author_id": REMOTE_AUTHOR}),
    )
    .await;
    let schema = mqdb_core::schema::Schema::new("posts").add_field(
        mqdb_core::schema::FieldDefinition::new("title", mqdb_core::schema::FieldType::Number),
    );
    ctrl.schema_register("posts", &serde_json::to_vec(&schema).unwrap())
        .await
        .unwrap();

    let author = finish_published(&handler, &mut ctrl, pending).await;

    assert_eq!(
        (
            &author["code"],
            &stored(&ctrl, "posts", "p1").unwrap()["author_id"]
        ),
        (&json!(400), &json!("u2")),
        "{author}"
    );
}
