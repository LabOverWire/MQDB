// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::{controller_where, publish_completing_unique};
use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition, unique_partition};
use crate::cluster::node_controller::db_ops::ForwardedJsonRequest;
use crate::cluster::protocol::JsonDbOp;
use serde_json::{Value, json};

const REMOTE_AUTHOR: &str = "u9";
const NEW_EMAIL: &str = "new@x";
const RESPONSE_TOPIC: &str = "resp/client";
const VAULT_USER: &str = "u9";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn encoded(value: &str) -> Vec<u8> {
    serde_json::to_vec(&json!(value)).unwrap()
}

fn remote_partitions() -> [PartitionId; 2] {
    [
        unique_partition("users", "email", &encoded(NEW_EMAIL)),
        data_partition("users", REMOTE_AUTHOR),
    ]
}

async fn primary() -> NodeController<MockTransport> {
    let remote = remote_partitions();
    for local in [
        data_partition("users", "u1"),
        data_partition("users", "u2"),
        data_partition("users", "u5"),
        data_partition("posts", "p1"),
        data_partition("posts", "p5"),
        unique_partition("users", "email", &encoded("old@x")),
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
        ("$DB/users/create", json!({"id": "u1", "email": "old@x"})),
        ("$DB/users/create", json!({"id": "u2"})),
        (
            "$DB/posts/create",
            json!({"id": "p1", "author_id": "u2", "title": "t0"}),
        ),
    ] {
        let reply = publish_completing_unique(&mut ctrl, topic, &payload).await;
        assert_eq!(reply["status"], "ok", "{reply}");
    }
    ctrl.heartbeat.receive_heartbeat(
        node(2),
        &crate::cluster::protocol::Heartbeat::create(node(2), 0),
        0,
    );
    ctrl
}

fn forwarded(
    request_id: u64,
    op: JsonDbOp,
    entity: &str,
    id: &str,
    payload: &Value,
) -> JsonDbRequest {
    JsonDbRequest {
        request_id,
        op,
        entity: entity.to_string(),
        id: Some(id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: RESPONSE_TOPIC.to_string(),
        correlation_data: Some(b"corr".to_vec()),
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

async fn deferred(
    ctrl: &mut NodeController<MockTransport>,
    request: &JsonDbRequest,
) -> PendingConstraintWork {
    let id = request.id.as_deref().unwrap();
    let Some(work) = ctrl
        .handle_json_db_request(node(2), data_partition(&request.entity, id), request)
        .await
    else {
        panic!("the forwarded request should wait on a constraint check, it completed");
    };
    work
}

async fn finish_unique(ctrl: &mut NodeController<MockTransport>, work: PendingUniqueWork) {
    ctrl.complete_pending_unique_work(
        work.phase1.local_reserved,
        Ok(Vec::new()),
        work.continuation,
    )
    .await;
}

async fn finish_fk(ctrl: &mut NodeController<MockTransport>, work: PendingFkWork) {
    if let FkThenUniqueOutcome::NeedUnique(unique) = ctrl
        .complete_pending_fk_work(true, None, work.continuation)
        .await
    {
        finish_unique(ctrl, *unique).await;
    }
}

fn reply_to_forwarder(ctrl: &NodeController<MockTransport>) -> JsonDbResponse {
    let replies: Vec<JsonDbResponse> = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response)
                if to == node(2) && response.response_topic == RESPONSE_TOPIC =>
            {
                Some(response)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        replies.len(),
        1,
        "expected exactly one reply to the forwarder"
    );
    replies.into_iter().next().unwrap()
}

fn assert_reply(response: &JsonDbResponse, request_id: u64, status: &str) {
    let payload: Value = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(
        (response.request_id, payload["status"].as_str()),
        (request_id, Some(status)),
        "reply payload {payload}"
    );
    assert_eq!(response.correlation_data.as_deref(), Some(&b"corr"[..]));
}

#[tokio::test]
async fn a_forwarded_create_deferred_by_a_unique_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        41,
        JsonDbOp::Create,
        "users",
        "u5",
        &json!({"id": "u5", "email": NEW_EMAIL}),
    );
    let PendingConstraintWork::Unique(work) = deferred(&mut ctrl, &request).await else {
        panic!("the create should wait on the remote unique reserve");
    };

    finish_unique(&mut ctrl, work).await;

    assert_reply(&reply_to_forwarder(&ctrl), 41, "ok");
}

#[tokio::test]
async fn a_forwarded_update_deferred_by_a_unique_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        42,
        JsonDbOp::Update,
        "users",
        "u1",
        &json!({"email": NEW_EMAIL}),
    );
    let PendingConstraintWork::Unique(work) = deferred(&mut ctrl, &request).await else {
        panic!("the update should wait on the remote unique reserve");
    };

    finish_unique(&mut ctrl, work).await;

    assert_reply(&reply_to_forwarder(&ctrl), 42, "ok");
}

#[tokio::test]
async fn a_forwarded_create_deferred_by_an_fk_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        43,
        JsonDbOp::Create,
        "posts",
        "p5",
        &json!({"id": "p5", "author_id": REMOTE_AUTHOR}),
    );
    let PendingConstraintWork::Fk(work) = deferred(&mut ctrl, &request).await else {
        panic!("the create should wait on the remote FK check");
    };

    finish_fk(&mut ctrl, work).await;

    assert_reply(&reply_to_forwarder(&ctrl), 43, "ok");
}

#[tokio::test]
async fn a_forwarded_create_refused_by_an_fk_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        44,
        JsonDbOp::Create,
        "posts",
        "p5",
        &json!({"id": "p5", "author_id": REMOTE_AUTHOR}),
    );
    let PendingConstraintWork::Fk(work) = deferred(&mut ctrl, &request).await else {
        panic!("the create should wait on the remote FK check");
    };

    let outcome = ctrl
        .complete_pending_fk_work(false, Some("author missing".to_string()), work.continuation)
        .await;

    assert!(matches!(outcome, FkThenUniqueOutcome::Done));
    assert_reply(&reply_to_forwarder(&ctrl), 44, "error");
}

async fn primary_with_slug_taken() -> NodeController<MockTransport> {
    let mut ctrl = primary().await;
    assert!(!remote_partitions().contains(&unique_partition("posts", "slug", &encoded("s1"))));
    ctrl.constraint_add(&ClusterConstraint::unique("posts", "uniq_slug", "slug").unwrap())
        .await
        .unwrap();
    let reply = publish_completing_unique(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p2", "author_id": "u1", "slug": "s1"}),
    )
    .await;
    assert_eq!(reply["status"], "ok", "{reply}");
    ctrl
}

async fn refused_after_fk(ctrl: &mut NodeController<MockTransport>, request: &JsonDbRequest) {
    let PendingConstraintWork::Fk(work) = deferred(ctrl, request).await else {
        panic!("the write should wait on the remote FK check");
    };

    let outcome = ctrl
        .complete_pending_fk_work(true, None, work.continuation)
        .await;

    assert!(matches!(outcome, FkThenUniqueOutcome::Done));
}

#[tokio::test]
async fn a_forwarded_create_refused_by_the_unique_check_after_an_fk_check_replies_with_its_request_id()
 {
    let mut ctrl = primary_with_slug_taken().await;
    let request = forwarded(
        48,
        JsonDbOp::Create,
        "posts",
        "p5",
        &json!({"id": "p5", "author_id": REMOTE_AUTHOR, "slug": "s1"}),
    );

    refused_after_fk(&mut ctrl, &request).await;

    assert_reply(&reply_to_forwarder(&ctrl), 48, "error");
}

#[tokio::test]
async fn a_forwarded_update_refused_by_the_unique_check_after_an_fk_check_replies_with_its_request_id()
 {
    let mut ctrl = primary_with_slug_taken().await;
    let request = forwarded(
        49,
        JsonDbOp::Update,
        "posts",
        "p1",
        &json!({"author_id": REMOTE_AUTHOR, "slug": "s1"}),
    );

    refused_after_fk(&mut ctrl, &request).await;

    assert_reply(&reply_to_forwarder(&ctrl), 49, "error");
}

#[tokio::test]
async fn a_forwarded_update_deferred_by_an_fk_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        45,
        JsonDbOp::Update,
        "posts",
        "p1",
        &json!({"author_id": REMOTE_AUTHOR}),
    );
    let PendingConstraintWork::Fk(work) = deferred(&mut ctrl, &request).await else {
        panic!("the update should wait on the remote FK check");
    };

    finish_fk(&mut ctrl, work).await;

    assert_reply(&reply_to_forwarder(&ctrl), 45, "ok");
}

#[tokio::test]
async fn a_forwarded_delete_deferred_by_an_fk_lookup_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(46, JsonDbOp::Delete, "users", "u2", &json!({}));
    let PendingConstraintWork::FkDelete(work) = deferred(&mut ctrl, &request).await else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };

    ctrl.complete_pending_fk_delete_work(None, work.local_results, work.continuation)
        .await;

    assert_reply(&reply_to_forwarder(&ctrl), 46, "ok");
    assert!(ctrl.db_get("users", "u2").is_none());
}

#[tokio::test]
async fn a_forwarded_delete_restricted_by_an_fk_lookup_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(47, JsonDbOp::Delete, "users", "u2", &json!({}));
    let PendingConstraintWork::FkDelete(work) = deferred(&mut ctrl, &request).await else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };

    ctrl.complete_pending_fk_delete_work(
        Some("referenced by posts".to_string()),
        Vec::new(),
        work.continuation,
    )
    .await;

    assert_reply(&reply_to_forwarder(&ctrl), 47, "error");
    assert!(ctrl.db_get("users", "u2").is_some());
}

#[tokio::test]
async fn a_forwarded_delete_refused_after_the_lookup_when_the_role_is_lost_replies_with_its_request_id()
 {
    let mut ctrl = primary().await;
    let request = forwarded(50, JsonDbOp::Delete, "users", "u2", &json!({}));
    let PendingConstraintWork::FkDelete(work) = deferred(&mut ctrl, &request).await else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };
    ctrl.step_down(data_partition("users", "u2"));

    ctrl.complete_pending_fk_delete_work(None, work.local_results, work.continuation)
        .await;

    assert_reply(&reply_to_forwarder(&ctrl), 50, "error");
    assert!(ctrl.db_get("users", "u2").is_some());
}

#[tokio::test]
async fn a_forwarded_update_refused_by_an_fk_check_replies_with_its_request_id() {
    let mut ctrl = primary().await;
    let request = forwarded(
        51,
        JsonDbOp::Update,
        "posts",
        "p1",
        &json!({"author_id": REMOTE_AUTHOR}),
    );
    let PendingConstraintWork::Fk(work) = deferred(&mut ctrl, &request).await else {
        panic!("the update should wait on the remote FK check");
    };

    let outcome = ctrl
        .complete_pending_fk_work(false, Some("author missing".to_string()), work.continuation)
        .await;

    assert!(matches!(outcome, FkThenUniqueOutcome::Done));
    assert_reply(&reply_to_forwarder(&ctrl), 51, "error");
}

fn replies_with_an_empty_topic(ctrl: &NodeController<MockTransport>) -> Vec<u64> {
    ctrl.transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbResponse(response) if response.response_topic.is_empty() => {
                Some(response.request_id)
            }
            _ => None,
        })
        .collect()
}

fn without_response_topic(mut request: JsonDbRequest) -> JsonDbRequest {
    request.response_topic = String::new();
    request
}

#[tokio::test]
async fn a_deferred_create_without_a_response_topic_sends_no_reply() {
    let mut ctrl = primary().await;
    let request = without_response_topic(forwarded(
        41,
        JsonDbOp::Create,
        "users",
        "u5",
        &json!({"id": "u5", "email": NEW_EMAIL}),
    ));
    let PendingConstraintWork::Unique(work) = deferred(&mut ctrl, &request).await else {
        panic!("the create should wait on the remote unique reserve");
    };
    ctrl.transport().take_sent();

    finish_unique(&mut ctrl, work).await;

    let empty_topic_replies = replies_with_an_empty_topic(&ctrl);
    assert!(
        empty_topic_replies.is_empty(),
        "replies sent with an empty topic: {empty_topic_replies:?}"
    );
    assert!(ctrl.db_get("users", "u5").is_some());
}

#[tokio::test]
async fn a_deferred_update_without_a_response_topic_sends_no_reply() {
    let mut ctrl = primary().await;
    let request = without_response_topic(forwarded(
        42,
        JsonDbOp::Update,
        "users",
        "u1",
        &json!({"email": NEW_EMAIL}),
    ));
    let PendingConstraintWork::Unique(work) = deferred(&mut ctrl, &request).await else {
        panic!("the update should wait on the remote unique reserve");
    };
    ctrl.transport().take_sent();

    finish_unique(&mut ctrl, work).await;

    let empty_topic_replies = replies_with_an_empty_topic(&ctrl);
    assert!(
        empty_topic_replies.is_empty(),
        "replies sent with an empty topic: {empty_topic_replies:?}"
    );
}

#[tokio::test]
async fn a_deferred_fk_refusal_without_a_response_topic_sends_no_reply() {
    let mut ctrl = primary().await;
    let request = without_response_topic(forwarded(
        44,
        JsonDbOp::Create,
        "posts",
        "p5",
        &json!({"id": "p5", "author_id": REMOTE_AUTHOR}),
    ));
    let PendingConstraintWork::Fk(work) = deferred(&mut ctrl, &request).await else {
        panic!("the create should wait on the remote FK check");
    };
    ctrl.transport().take_sent();

    ctrl.complete_pending_fk_work(false, Some("author missing".to_string()), work.continuation)
        .await;

    let empty_topic_replies = replies_with_an_empty_topic(&ctrl);
    assert!(
        empty_topic_replies.is_empty(),
        "replies sent with an empty topic: {empty_topic_replies:?}"
    );
}

#[tokio::test]
async fn a_unique_refusal_after_an_fk_check_without_a_response_topic_sends_no_reply() {
    let mut ctrl = primary_with_slug_taken().await;
    let request = without_response_topic(forwarded(
        48,
        JsonDbOp::Create,
        "posts",
        "p5",
        &json!({"id": "p5", "author_id": REMOTE_AUTHOR, "slug": "s1"}),
    ));
    ctrl.transport().take_sent();

    refused_after_fk(&mut ctrl, &request).await;

    let empty_topic_replies = replies_with_an_empty_topic(&ctrl);
    assert!(
        empty_topic_replies.is_empty(),
        "replies sent with an empty topic: {empty_topic_replies:?}"
    );
}

fn vault_key() -> Vec<u8> {
    vec![7u8; 32]
}

fn ownership() -> Arc<mqdb_core::types::OwnershipConfig> {
    Arc::new(mqdb_core::types::OwnershipConfig::parse("posts=author_id,users=owner").unwrap())
}

fn vault_encrypted(entity: &str, id: &str, plain: &Value) -> Value {
    let crypto = mqdb_vault::VaultCrypto::from_key_bytes(&vault_key()).unwrap();
    let skip = mqdb_vault::transform::build_vault_skip_fields(entity, &ownership());
    let mut data = plain.clone();
    mqdb_vault::transform::vault_encrypt_fields(&crypto, entity, id, &mut data, &skip);
    data
}

fn vault_forwarder(partition: PartitionId) -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(2), MockTransport::new(node(2)));
    ctrl.set_ownership(ownership());
    let keys = Arc::new(VaultKeyStore::new());
    keys.set(VAULT_USER, zeroize::Zeroizing::new(vault_key()));
    ctrl.set_vault_key_store(keys);
    let mut map = PartitionMap::new();
    map.set(
        partition,
        crate::cluster::PartitionAssignment {
            primary: Some(node(1)),
            replicas: vec![],
            epoch: Epoch::new(1),
            primary_since: Epoch::new(1),
        },
    );
    ctrl.update_partition_map(map);
    ctrl
}

async fn forward_create(
    forwarder: &mut NodeController<MockTransport>,
    entity: &str,
    id: &str,
    data: &Value,
) -> (PartitionId, JsonDbRequest) {
    let partition = data_partition(entity, id);
    let payload = serde_json::to_vec(data).unwrap();
    assert!(
        forwarder
            .forward_json_db_request(
                partition,
                ForwardedJsonRequest {
                    op: JsonDbOp::Create,
                    entity,
                    id: Some(id),
                    payload: &payload,
                    response_topic: RESPONSE_TOPIC,
                    correlation_data: None,
                    origin: EventOrigin::new(Some(VAULT_USER), None),
                    requested_owner: None,
                },
            )
            .await
    );
    assert_eq!(forwarder.pending_vault_decrypts.len(), 1);
    let request = forwarder
        .transport()
        .take_sent()
        .into_iter()
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } if to == node(1) => Some(request),
            _ => None,
        })
        .unwrap();
    (partition, request)
}

async fn client_reply(
    forwarder: &mut NodeController<MockTransport>,
    primary: &NodeController<MockTransport>,
) -> Value {
    forwarder
        .handle_json_db_response(&reply_to_forwarder(primary))
        .await;
    let payload = forwarder
        .transport()
        .queued_publishes()
        .into_iter()
        .find_map(|publish| match publish {
            QueuedPublish::Plain(topic, payload, _) if topic == RESPONSE_TOPIC => Some(payload),
            _ => None,
        })
        .unwrap();
    serde_json::from_slice(&payload).unwrap()
}

#[tokio::test]
async fn a_vault_create_deferred_by_an_fk_check_reaches_the_client_decrypted() {
    let mut primary = primary().await;
    primary.set_ownership(ownership());
    let plain = json!({"id": "p5", "author_id": VAULT_USER, "title": "hello"});
    let encrypted = vault_encrypted("posts", "p5", &plain);
    assert_ne!(encrypted["title"], plain["title"]);
    let mut forwarder = vault_forwarder(data_partition("posts", "p5"));
    let (partition, request) = forward_create(&mut forwarder, "posts", "p5", &encrypted).await;

    let Some(PendingConstraintWork::Fk(work)) = primary
        .handle_json_db_request(node(2), partition, &request)
        .await
    else {
        panic!("the create should wait on the remote FK check");
    };
    finish_fk(&mut primary, work).await;
    let reply = client_reply(&mut forwarder, &primary).await;

    assert_eq!(reply["data"]["title"], "hello", "client reply {reply}");
    assert!(forwarder.pending_vault_decrypts.is_empty());
}

#[tokio::test]
async fn a_vault_create_deferred_by_a_unique_check_reaches_the_client_decrypted() {
    let local = data_partition("users", "u7");
    let mut primary = controller_where(
        None,
        |partition| if partition == local { node(1) } else { node(2) },
        &[],
    );
    primary.set_ownership(ownership());
    primary.seed_unique_voters(std::iter::once(node(1)).collect());
    primary
        .constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();
    let plain = json!({"id": "u7", "owner": VAULT_USER, "email": "v@x"});
    let encrypted = std::iter::repeat_with(|| vault_encrypted("users", "u7", &plain))
        .find(|data| {
            unique_partition(
                "users",
                "email",
                &serde_json::to_vec(&data["email"]).unwrap(),
            ) != local
        })
        .unwrap();
    let mut forwarder = vault_forwarder(local);
    let (partition, request) = forward_create(&mut forwarder, "users", "u7", &encrypted).await;

    let Some(PendingConstraintWork::Unique(work)) = primary
        .handle_json_db_request(node(2), partition, &request)
        .await
    else {
        panic!("the create should wait on the remote unique reserve");
    };
    finish_unique(&mut primary, work).await;
    let reply = client_reply(&mut forwarder, &primary).await;

    assert_eq!(reply["data"]["email"], "v@x", "client reply {reply}");
    assert!(forwarder.pending_vault_decrypts.is_empty());
}

#[tokio::test]
async fn back_to_back_vault_forwards_each_keep_their_decrypt_entry() {
    const FORWARDS: usize = 2000;
    let partition = data_partition("posts", "p5");
    let mut forwarder = vault_forwarder(partition);
    for _ in 0..FORWARDS {
        assert!(
            forwarder
                .forward_json_db_request(
                    partition,
                    ForwardedJsonRequest {
                        op: JsonDbOp::Read,
                        entity: "posts",
                        id: Some("p5"),
                        payload: &[],
                        response_topic: RESPONSE_TOPIC,
                        correlation_data: None,
                        origin: EventOrigin::new(Some(VAULT_USER), None),
                        requested_owner: None,
                    },
                )
                .await
        );
    }
    let ids: std::collections::HashSet<u64> = forwarder
        .transport()
        .take_sent()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } => Some(request.request_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        (ids.len(), forwarder.pending_vault_decrypts.len()),
        (FORWARDS, FORWARDS)
    );
}

#[tokio::test]
async fn forwarded_request_ids_come_from_the_node_request_counter() {
    let mut forwarder = vault_forwarder(data_partition("posts", "p5"));
    let plain = json!({"id": "p5", "author_id": VAULT_USER, "title": "hello"});
    let (_, request) = forward_create(&mut forwarder, "posts", "p5", &plain).await;
    let next = forwarder.pending_constraints.allocate_request_id();
    assert_eq!(next, request.request_id + 1);
}

#[tokio::test]
async fn an_early_refusal_without_a_response_topic_sends_no_reply() {
    let mut ctrl = primary().await;
    let request = without_response_topic(forwarded(
        61,
        JsonDbOp::Create,
        "users",
        REMOTE_AUTHOR,
        &json!({"id": REMOTE_AUTHOR}),
    ));
    ctrl.transport().take_sent();

    let pending = ctrl
        .handle_json_db_request(node(2), data_partition("users", REMOTE_AUTHOR), &request)
        .await;

    assert!(pending.is_none());
    let empty_topic_replies = replies_with_an_empty_topic(&ctrl);
    assert!(
        empty_topic_replies.is_empty(),
        "replies sent with an empty topic: {empty_topic_replies:?}"
    );
}

#[tokio::test]
async fn a_deferred_forwarded_delete_rechecks_its_expected_version() {
    let mut ctrl = primary().await;
    let request = forwarded(
        64,
        JsonDbOp::Delete,
        "users",
        "u2",
        &json!({"_expected_version": 1}),
    );
    let PendingConstraintWork::FkDelete(work) = deferred(&mut ctrl, &request).await else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };
    let updated =
        publish_completing_unique(&mut ctrl, "$DB/users/u2/update", &json!({"name": "b"})).await;
    assert_eq!(updated["data"]["_version"], 2, "{updated}");

    ctrl.complete_pending_fk_delete_work(None, work.local_results, work.continuation)
        .await;

    assert_reply(&reply_to_forwarder(&ctrl), 64, "error");
    let reply: Value = serde_json::from_slice(&reply_to_forwarder(&ctrl).payload).unwrap();
    assert_eq!(reply["code"], 412, "{reply}");
    assert!(ctrl.db_get("users", "u2").is_some());
}

#[tokio::test]
async fn a_deferred_mqtt_delete_rechecks_its_expected_version() {
    let mut ctrl = primary().await;
    let handler = crate::cluster::db_handler::DbRequestHandler::new(node(1));
    let ctx = super::store_failure::response_ctx(None);
    let payload = serde_json::to_vec(&json!({"_expected_version": 1})).unwrap();
    let crate::cluster::db_handler::DbPublishResult::PendingFkDelete(work) = handler
        .handle_publish(&mut ctrl, "$DB/users/u2/delete", &payload, &ctx)
        .await
    else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };
    let updated =
        publish_completing_unique(&mut ctrl, "$DB/users/u2/update", &json!({"name": "b"})).await;
    assert_eq!(updated["data"]["_version"], 2, "{updated}");

    let response = handler
        .complete_pending_fk_delete(&mut ctrl, None, work.local_results, work.continuation)
        .await
        .unwrap();

    let reply: Value = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(reply["code"], 412, "{reply}");
    assert!(ctrl.db_get("users", "u2").is_some());
}

#[tokio::test]
async fn a_deferred_forwarded_delete_refuses_a_stale_version_before_a_restrict() {
    let mut ctrl = primary().await;
    let request = forwarded(
        90,
        JsonDbOp::Delete,
        "users",
        "u2",
        &json!({"_expected_version": 1}),
    );
    let PendingConstraintWork::FkDelete(work) = deferred(&mut ctrl, &request).await else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };
    let updated =
        publish_completing_unique(&mut ctrl, "$DB/users/u2/update", &json!({"name": "b"})).await;
    assert_eq!(updated["data"]["_version"], 2, "{updated}");

    ctrl.complete_pending_fk_delete_work(
        Some("restricted by a remote child".to_string()),
        work.local_results,
        work.continuation,
    )
    .await;

    let reply: Value = serde_json::from_slice(&reply_to_forwarder(&ctrl).payload).unwrap();
    assert_eq!(reply["code"], 412, "{reply}");
}

#[tokio::test]
async fn a_deferred_mqtt_delete_refuses_a_stale_version_before_a_restrict() {
    let mut ctrl = primary().await;
    let handler = crate::cluster::db_handler::DbRequestHandler::new(node(1));
    let ctx = super::store_failure::response_ctx(None);
    let payload = serde_json::to_vec(&json!({"_expected_version": 1})).unwrap();
    let crate::cluster::db_handler::DbPublishResult::PendingFkDelete(work) = handler
        .handle_publish(&mut ctrl, "$DB/users/u2/delete", &payload, &ctx)
        .await
    else {
        panic!("the delete should wait on the remote FK reverse lookup");
    };
    let updated =
        publish_completing_unique(&mut ctrl, "$DB/users/u2/update", &json!({"name": "b"})).await;
    assert_eq!(updated["data"]["_version"], 2, "{updated}");

    let response = handler
        .complete_pending_fk_delete(
            &mut ctrl,
            Some("restricted by a remote child".to_string()),
            work.local_results,
            work.continuation,
        )
        .await
        .unwrap();

    let reply: Value = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(reply["code"], 412, "{reply}");
}

#[tokio::test]
async fn a_client_fk_expected_field_does_not_turn_a_forwarded_update_into_a_no_op() {
    let mut ctrl = primary().await;
    let request = forwarded(
        70,
        JsonDbOp::Update,
        "users",
        "u2",
        &json!({"name": "changed", "__mqdb_fk_expected": "name=nope"}),
    );

    let pending = ctrl
        .handle_json_db_request(node(2), data_partition("users", "u2"), &request)
        .await;

    assert!(pending.is_none());
    assert_reply(&reply_to_forwarder(&ctrl), 70, "ok");
    let stored: Value = serde_json::from_slice(&ctrl.db_get("users", "u2").unwrap().data).unwrap();
    assert_eq!(stored["name"], "changed", "{stored}");
    assert!(stored.get("__mqdb_fk_expected").is_none(), "{stored}");
}

#[tokio::test]
async fn a_forwarded_client_update_drops_the_fk_expected_field() {
    let partition = data_partition("posts", "p5");
    let mut forwarder = vault_forwarder(partition);
    let payload =
        serde_json::to_vec(&json!({"title": "x", "__mqdb_fk_expected": "title=nope"})).unwrap();

    assert!(
        forwarder
            .forward_json_db_request(
                partition,
                ForwardedJsonRequest {
                    op: JsonDbOp::Update,
                    entity: "posts",
                    id: Some("p5"),
                    payload: &payload,
                    response_topic: RESPONSE_TOPIC,
                    correlation_data: None,
                    origin: EventOrigin::new(None, None),
                    requested_owner: None,
                },
            )
            .await
    );

    let sent: Vec<Value> = forwarder
        .transport()
        .take_sent()
        .into_iter()
        .filter_map(|(_, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } => {
                serde_json::from_slice(&request.payload).ok()
            }
            _ => None,
        })
        .collect();
    assert_eq!(sent, vec![json!({"title": "x"})]);
}
