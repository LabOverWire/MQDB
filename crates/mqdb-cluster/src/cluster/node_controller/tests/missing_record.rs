// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::{
    ClusterConstraint, OnDeleteAction, data_partition, generate_id_for_partition,
};
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, MqttRequestContext};
use crate::cluster::protocol::{JsonDbOp, JsonDbRequest};
use mqdb_core::types::{OwnershipConfig, SHARES_ENTITY};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

const OPS: [JsonDbOp; 6] = [
    JsonDbOp::Read,
    JsonDbOp::Update,
    JsonDbOp::Delete,
    JsonDbOp::Share,
    JsonDbOp::Unshare,
    JsonDbOp::Shares,
];

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn ownership() -> Arc<OwnershipConfig> {
    let fields = HashMap::from([("docs".to_string(), "userId".to_string())]);
    let admins = HashSet::from(["root".to_string()]);
    Arc::new(OwnershipConfig::new(fields).with_admin_users(admins))
}

async fn docs_primary() -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    make_all_primary(&mut ctrl, node(1));
    ctrl.set_ownership(ownership());
    let owned = serde_json::to_vec(&json!({"userId": "alice", "title": "a"})).unwrap();
    ctrl.db_create("docs", "owned-by-alice", &owned, 1000)
        .await
        .unwrap();
    ctrl
}

fn op_topic(op: JsonDbOp, id: &str) -> String {
    match op {
        JsonDbOp::Read => format!("$DB/docs/{id}"),
        JsonDbOp::Update => format!("$DB/docs/{id}/update"),
        JsonDbOp::Delete => format!("$DB/docs/{id}/delete"),
        JsonDbOp::Share => format!("$DB/docs/{id}/share"),
        JsonDbOp::Unshare => format!("$DB/docs/{id}/unshare"),
        JsonDbOp::Shares => format!("$DB/docs/{id}/shares"),
        other => panic!("not a record op: {other:?}"),
    }
}

fn op_payload(op: JsonDbOp) -> Vec<u8> {
    let body = match op {
        JsonDbOp::Update => json!({"title": "changed"}),
        JsonDbOp::Share => json!({"grantee": "carol", "permission": "view"}),
        JsonDbOp::Unshare => json!({"grantee": "carol"}),
        _ => json!({}),
    };
    serde_json::to_vec(&body).unwrap()
}

async fn local_response(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    id: &str,
    sender: &str,
    payload: &[u8],
) -> Value {
    let result = DbRequestHandler::new(node(1))
        .with_ownership(ownership())
        .handle_publish(
            ctrl,
            &op_topic(op, id),
            payload,
            &MqttRequestContext {
                response_topic: Some("resp/local"),
                correlation_data: None,
                sender: Some(sender),
                client_id: None,
            },
        )
        .await;
    let DbPublishResult::Response(response) = result else {
        panic!("{op:?} {id} was not answered on the primary");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

async fn forwarded_response(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    id: &str,
    sender: &str,
    payload: &[u8],
) -> Value {
    let request = JsonDbRequest {
        request_id: 1,
        op,
        entity: "docs".to_string(),
        id: Some(id.to_string()),
        payload: payload.to_vec(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: Some(sender.to_string()),
        client_id: None,
    };
    let already_sent = forwarded_replies(ctrl).len();
    ctrl.handle_json_db_request(node(2), data_partition("docs", id), &request)
        .await;
    let replies = forwarded_replies(ctrl);
    assert_eq!(
        replies.len(),
        already_sent + 1,
        "{op:?} {id} forwarded must answer exactly once"
    );
    replies[already_sent].clone()
}

fn forwarded_replies(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
    ctrl.transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(2) => {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .collect()
}

async fn codes_on_both_paths(
    ctrl: &mut NodeController<MockTransport>,
    id: &str,
    sender: &str,
) -> Vec<(JsonDbOp, &'static str, Value)> {
    let mut codes = Vec::new();
    for op in OPS {
        let local = local_response(ctrl, op, id, sender, &op_payload(op)).await;
        codes.push((op, "local", local["code"].clone()));
        let forwarded = forwarded_response(ctrl, op, id, sender, &op_payload(op)).await;
        codes.push((op, "forwarded", forwarded["code"].clone()));
    }
    codes
}

fn expected(code: u16) -> Vec<(JsonDbOp, &'static str, Value)> {
    OPS.iter()
        .flat_map(|&op| [(op, "local", json!(code)), (op, "forwarded", json!(code))])
        .collect()
}

#[tokio::test]
async fn missing_record_is_not_found_and_existing_one_forbidden_on_every_path() {
    let mut ctrl = docs_primary().await;

    let missing = codes_on_both_paths(&mut ctrl, "never-created", "bob").await;
    let existing = codes_on_both_paths(&mut ctrl, "owned-by-alice", "bob").await;

    assert_eq!(missing, expected(404));
    assert_eq!(existing, expected(403));
    let record: Value =
        serde_json::from_slice(&ctrl.db_get("docs", "owned-by-alice").unwrap().data).unwrap();
    assert_eq!(record["title"], "a");
    assert!(ctrl.db_list(SHARES_ENTITY).is_empty());
    assert!(ctrl.db_get("docs", "never-created").is_none());

    for answer in [
        local_response(
            &mut ctrl,
            JsonDbOp::Share,
            "never-created",
            "root",
            &op_payload(JsonDbOp::Share),
        )
        .await,
        forwarded_response(
            &mut ctrl,
            JsonDbOp::Share,
            "never-created",
            "root",
            &op_payload(JsonDbOp::Share),
        )
        .await,
    ] {
        assert_eq!(answer["code"], 404, "admin share {answer}");
    }
    for op in [JsonDbOp::Unshare, JsonDbOp::Shares] {
        for answer in [
            local_response(&mut ctrl, op, "never-created", "root", &op_payload(op)).await,
            forwarded_response(&mut ctrl, op, "never-created", "root", &op_payload(op)).await,
        ] {
            assert_eq!(answer["status"], "ok", "admin {op:?} {answer}");
        }
    }
}

#[tokio::test]
async fn grant_left_on_a_missing_record_answers_not_found_on_every_path() {
    let mut ctrl = docs_primary().await;
    let share_id =
        generate_id_for_partition(SHARES_ENTITY, data_partition("docs", "deleted-later"));
    let grant = serde_json::to_vec(&json!({
        "resource_entity": "docs",
        "resource_id": "deleted-later",
        "grantee": "bob",
        "grantee_key": "bob",
        "permission": "edit",
    }))
    .unwrap();
    ctrl.db_create(SHARES_ENTITY, &share_id, &grant, 1000)
        .await
        .unwrap();

    let codes = codes_on_both_paths(&mut ctrl, "deleted-later", "bob").await;

    assert_eq!(codes, expected(404));
    assert!(ctrl.db_get("docs", "deleted-later").is_none());
    assert_eq!(ctrl.db_list(SHARES_ENTITY).len(), 1);
}

async fn codes_for_payload(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    id: &str,
    sender: &str,
    payload: &[u8],
) -> [Value; 2] {
    [
        local_response(ctrl, op, id, sender, payload).await["code"].clone(),
        forwarded_response(ctrl, op, id, sender, payload).await["code"].clone(),
    ]
}

#[tokio::test]
async fn malformed_payload_is_refused_before_the_ownership_check_on_every_path() {
    let mut ctrl = docs_primary().await;
    let bad_version =
        serde_json::to_vec(&json!({"title": "x", "_expected_version": "one"})).unwrap();
    let cases: [(JsonDbOp, &str, &[u8]); 6] = [
        (JsonDbOp::Update, "never-created", b"not json"),
        (JsonDbOp::Update, "owned-by-alice", b"not json"),
        (JsonDbOp::Update, "never-created", &bad_version),
        (JsonDbOp::Update, "owned-by-alice", &bad_version),
        (JsonDbOp::Delete, "owned-by-alice", b"not json"),
        (JsonDbOp::Delete, "owned-by-alice", &bad_version),
    ];

    let mut codes = Vec::new();
    for (op, id, payload) in cases {
        codes.push((
            op,
            id,
            codes_for_payload(&mut ctrl, op, id, "bob", payload).await,
        ));
    }

    let refused: Vec<(JsonDbOp, &str, [Value; 2])> = cases
        .iter()
        .map(|&(op, id, _)| (op, id, [json!(400), json!(400)]))
        .collect();
    assert_eq!(codes, refused);
    let record: Value =
        serde_json::from_slice(&ctrl.db_get("docs", "owned-by-alice").unwrap().data).unwrap();
    assert_eq!(record["title"], "a");
}

#[tokio::test]
async fn unknown_projection_field_answers_like_agent_mode_on_every_path() {
    let mut ctrl = docs_primary().await;
    let schema = mqdb_core::schema::Schema::new("docs")
        .add_field(mqdb_core::schema::FieldDefinition::new(
            "userId",
            mqdb_core::schema::FieldType::String,
        ))
        .add_field(mqdb_core::schema::FieldDefinition::new(
            "title",
            mqdb_core::schema::FieldType::String,
        ));
    ctrl.schema_register("docs", &serde_json::to_vec(&schema).unwrap())
        .await
        .unwrap();
    let projection = serde_json::to_vec(&json!({"projection": ["nope"]})).unwrap();
    let cases = [
        ("never-created", "bob", 404),
        ("never-created", "alice", 404),
        ("never-created", "root", 404),
        ("owned-by-alice", "bob", 403),
        ("owned-by-alice", "alice", 400),
        ("owned-by-alice", "root", 400),
    ];

    let mut codes = Vec::new();
    for (id, sender, _) in cases {
        codes.push((
            id,
            sender,
            codes_for_payload(&mut ctrl, JsonDbOp::Read, id, sender, &projection).await,
        ));
    }

    let agent_answers: Vec<(&str, &str, [Value; 2])> = cases
        .iter()
        .map(|&(id, sender, code)| (id, sender, [json!(code), json!(code)]))
        .collect();
    assert_eq!(codes, agent_answers);
}

#[cfg(feature = "http-api")]
fn suspend_share(
    ctrl: &mut NodeController<MockTransport>,
    request_id: u64,
    id: &str,
    granted_by: &str,
) {
    let continuation = ShareResolveContinuation {
        email_hash: "hash".to_string(),
        entity: "docs".to_string(),
        id: id.to_string(),
        grantee_key: "carol-key".to_string(),
        grantee_email: None,
        permission: "view".to_string(),
        cascade: false,
        granted_by: granted_by.to_string(),
        response_topic: "resp/share".to_string(),
        correlation_data: None,
        invalidated: false,
    };
    ctrl.pending_scatter_requests.insert(
        request_id,
        PendingScatterRequest {
            expected_count: 1,
            received: Vec::new(),
            client_response_topic: "resp/share".to_string(),
            created_at_ms: 0,
            filters: Vec::new(),
            sorts: Vec::new(),
            projection: None,
            pagination: None,
            entity: "_identity_links".to_string(),
            vault_sender: None,
            continuation: Some(continuation),
        },
    );
}

#[cfg(feature = "http-api")]
fn invalidated(ctrl: &NodeController<MockTransport>, request_id: u64) -> bool {
    ctrl.pending_scatter_requests[&request_id]
        .continuation
        .as_ref()
        .is_some_and(|cont| cont.invalidated)
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn only_the_issuer_owner_or_admin_cancels_a_share_whatever_the_record_state() {
    let mut ctrl = docs_primary().await;
    let shares = [
        (1, "never-created", "dave"),
        (2, "owned-by-alice", "dave"),
        (3, "owned-by-alice", "dave"),
        (4, "never-created", "dave"),
    ];
    for (request_id, id, issuer) in shares {
        suspend_share(&mut ctrl, request_id, id, issuer);
    }

    for (sender, id) in [
        ("bob", "never-created"),
        ("bob", "owned-by-alice"),
        ("alice", "never-created"),
    ] {
        ctrl.invalidate_share_resolutions("docs", id, "carol-key", Some(sender));
    }
    let after_strangers = shares.map(|(request_id, ..)| invalidated(&ctrl, request_id));
    ctrl.invalidate_share_resolutions("docs", "owned-by-alice", "carol-key", Some("alice"));
    let after_owner = shares.map(|(request_id, ..)| invalidated(&ctrl, request_id));
    ctrl.invalidate_share_resolutions("docs", "never-created", "carol-key", Some("dave"));
    let after_issuer = shares.map(|(request_id, ..)| invalidated(&ctrl, request_id));

    assert_eq!(after_strangers, [false, false, false, false]);
    assert_eq!(after_owner, [false, true, true, false]);
    assert_eq!(after_issuer, [true, true, true, true]);
}

#[tokio::test]
async fn empty_update_payload_is_graded_by_ownership_first_on_every_path() {
    let mut ctrl = docs_primary().await;

    let missing = codes_for_payload(&mut ctrl, JsonDbOp::Update, "never-created", "bob", b"").await;
    let existing =
        codes_for_payload(&mut ctrl, JsonDbOp::Update, "owned-by-alice", "bob", b"").await;

    assert_eq!(missing, [json!(404), json!(404)]);
    assert_eq!(existing, [json!(403), json!(403)]);
}

fn stored_version(ctrl: &NodeController<MockTransport>, id: &str) -> u64 {
    let record: Value = serde_json::from_slice(&ctrl.db_get("docs", id).unwrap().data).unwrap();
    record["_version"].as_u64().unwrap_or(0)
}

#[tokio::test]
async fn empty_update_payload_from_owner_or_admin_bumps_the_version_like_agent_mode() {
    let mut ctrl = docs_primary().await;
    let before = stored_version(&ctrl, "owned-by-alice");

    let mut answers = Vec::new();
    for sender in ["alice", "root"] {
        let local =
            local_response(&mut ctrl, JsonDbOp::Update, "owned-by-alice", sender, b"").await;
        let forwarded =
            forwarded_response(&mut ctrl, JsonDbOp::Update, "owned-by-alice", sender, b"").await;
        answers.push((sender, local["status"].clone(), forwarded["status"].clone()));
    }

    assert_eq!(
        answers,
        [
            ("alice", json!("ok"), json!("ok")),
            ("root", json!("ok"), json!("ok")),
        ]
    );
    let record: Value =
        serde_json::from_slice(&ctrl.db_get("docs", "owned-by-alice").unwrap().data).unwrap();
    assert_eq!(record["title"], "a");
    assert_eq!(record["userId"], "alice");
    assert_eq!(stored_version(&ctrl, "owned-by-alice"), before + 4);
}

fn not_yet_primary(partition: PartitionId) -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    ctrl.set_ownership(ownership());
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment::new(node(1), vec![], Epoch::new(1)),
    );
    ctrl.update_partition_map(map);
    assert!(!ctrl.holds_primary_role(partition));
    ctrl
}

#[tokio::test]
async fn writes_reaching_a_not_yet_primary_stay_retryable_on_every_path() {
    let partition = data_partition("docs", "child-1");
    let mut ctrl = not_yet_primary(partition);
    let update = serde_json::to_vec(&json!({"title": "x"})).unwrap();

    let mut codes = Vec::new();
    for sender in ["bob", "root"] {
        for (op, payload) in [
            (JsonDbOp::Update, update.as_slice()),
            (JsonDbOp::Delete, &[]),
        ] {
            codes.push((
                sender,
                op,
                codes_for_payload(&mut ctrl, op, "child-1", sender, payload).await,
            ));
        }
    }

    assert_eq!(
        codes,
        [
            ("bob", JsonDbOp::Update, [json!(503), json!(503)]),
            ("bob", JsonDbOp::Delete, [json!(503), json!(503)]),
            ("root", JsonDbOp::Update, [json!(503), json!(503)]),
            ("root", JsonDbOp::Delete, [json!(503), json!(503)]),
        ]
    );
}

#[tokio::test]
async fn cascade_delete_reaching_a_not_yet_primary_with_a_stale_copy_is_not_acknowledged() {
    let partition = data_partition("docs", "child-1");
    let mut origin = create_test_controller(node(2), MockTransport::new(node(2)));
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment::new(node(1), vec![], Epoch::new(1)),
    );
    origin.update_partition_map(map);
    let acked = origin
        .send_cascade_request(
            partition,
            JsonDbOp::Delete,
            "docs",
            "child-1",
            &[],
            super::super::db_ops::EventOrigin::new(Some("alice"), None),
        )
        .await
        .unwrap();
    let request = origin
        .transport()
        .sent_messages()
        .into_iter()
        .find_map(|(_, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } => Some(request),
            _ => None,
        })
        .unwrap();

    let mut receiver = not_yet_primary(partition);
    let copy_owned_by_bob = serde_json::to_vec(&json!({"userId": "bob", "title": "b"})).unwrap();
    receiver
        .stores
        .db_create_replicated("docs", "child-1", &copy_owned_by_bob, 1000)
        .unwrap();
    receiver
        .handle_json_db_request(node(2), partition, &request)
        .await;
    let reply = receiver
        .transport()
        .sent_messages()
        .into_iter()
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(2) => Some(response),
            _ => None,
        })
        .unwrap();
    origin.handle_json_db_response(&reply).await;

    assert!(!acked.await.unwrap());
}

#[tokio::test]
async fn unreadable_stored_record_is_never_treated_as_owned() {
    let mut ctrl = docs_primary().await;
    ctrl.db_create("docs", "garbled", b"not json", 1000)
        .await
        .unwrap();

    let mut codes = Vec::new();
    for op in [
        JsonDbOp::Read,
        JsonDbOp::Update,
        JsonDbOp::Share,
        JsonDbOp::Unshare,
        JsonDbOp::Shares,
        JsonDbOp::Delete,
    ] {
        codes.push((
            op,
            codes_for_payload(&mut ctrl, op, "garbled", "bob", &op_payload(op)).await,
        ));
    }

    assert!(
        codes
            .iter()
            .all(|(_, answers)| answers == &[json!(403), json!(403)]),
        "{codes:?}"
    );
    assert!(ctrl.db_get("docs", "garbled").is_some());
}

fn ghost_note() -> Vec<u8> {
    serde_json::to_vec(&json!({"userId": "carol", "parent_id": "ghost"})).unwrap()
}

async fn note_referencing_a_missing_doc(
    on_delete: OnDeleteAction,
) -> NodeController<MockTransport> {
    let mut ctrl = docs_primary().await;
    let fk = ClusterConstraint::foreign_key(
        "notes",
        "notes_parent_fk",
        "parent_id",
        "docs",
        "id",
        on_delete,
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();
    ctrl.db_create("notes", "n1", &ghost_note(), 1000)
        .await
        .unwrap();
    ctrl
}

async fn internal_local_code(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    id: &str,
    payload: &[u8],
) -> Value {
    let result = DbRequestHandler::new(node(1))
        .with_ownership(ownership())
        .handle_publish(
            ctrl,
            &op_topic(op, id),
            payload,
            &MqttRequestContext {
                response_topic: Some("resp/local"),
                correlation_data: None,
                sender: None,
                client_id: None,
            },
        )
        .await;
    let DbPublishResult::Response(response) = result else {
        panic!("{op:?} {id} was not answered locally");
    };
    let body: Value = serde_json::from_slice(&response.payload).unwrap();
    body["code"].clone()
}

async fn internal_forwarded_code(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    id: &str,
    payload: &[u8],
) -> Value {
    let request = JsonDbRequest {
        request_id: 7,
        op,
        entity: "docs".to_string(),
        id: Some(id.to_string()),
        payload: payload.to_vec(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
    };
    let already_sent = forwarded_replies(ctrl).len();
    ctrl.handle_json_db_request(node(2), data_partition("docs", id), &request)
        .await;
    let replies = forwarded_replies(ctrl);
    assert_eq!(replies.len(), already_sent + 1);
    replies[already_sent]["code"].clone()
}

fn parent_of_note(ctrl: &NodeController<MockTransport>) -> Value {
    let note: Value = serde_json::from_slice(&ctrl.db_get("notes", "n1").unwrap().data).unwrap();
    note["parent_id"].clone()
}

#[tokio::test]
async fn internal_writes_reaching_a_not_yet_primary_stay_retryable_on_every_path() {
    let partition = data_partition("docs", "child-1");
    let mut ctrl = not_yet_primary(partition);
    let update = serde_json::to_vec(&json!({"title": "x"})).unwrap();

    let codes = [
        internal_local_code(&mut ctrl, JsonDbOp::Update, "child-1", &update).await,
        internal_local_code(&mut ctrl, JsonDbOp::Delete, "child-1", &[]).await,
        internal_forwarded_code(&mut ctrl, JsonDbOp::Update, "child-1", &update).await,
        internal_forwarded_code(&mut ctrl, JsonDbOp::Delete, "child-1", &[]).await,
    ];

    assert_eq!(codes, [json!(503), json!(503), json!(503), json!(503)]);
}

#[tokio::test]
async fn internal_delete_of_a_missing_record_touches_no_referencing_rows_on_every_path() {
    let mut local_ctrl = note_referencing_a_missing_doc(OnDeleteAction::SetNull).await;
    let local = internal_local_code(&mut local_ctrl, JsonDbOp::Delete, "ghost", &[]).await;
    let mut forwarded_ctrl = note_referencing_a_missing_doc(OnDeleteAction::SetNull).await;
    let forwarded =
        internal_forwarded_code(&mut forwarded_ctrl, JsonDbOp::Delete, "ghost", &[]).await;

    assert_eq!(
        [
            local,
            parent_of_note(&local_ctrl),
            forwarded,
            parent_of_note(&forwarded_ctrl)
        ],
        [json!(404), json!("ghost"), json!(404), json!("ghost")]
    );
}

#[tokio::test]
async fn deleting_a_missing_record_cascades_to_no_referencing_rows_on_every_path() {
    let mut answers = Vec::new();
    for path in [
        "admin local",
        "admin forwarded",
        "internal local",
        "internal forwarded",
    ] {
        let mut ctrl = note_referencing_a_missing_doc(OnDeleteAction::Cascade).await;
        let code = match path {
            "admin local" => local_response(&mut ctrl, JsonDbOp::Delete, "ghost", "root", &[])
                .await["code"]
                .clone(),
            "admin forwarded" => {
                forwarded_response(&mut ctrl, JsonDbOp::Delete, "ghost", "root", &[]).await["code"]
                    .clone()
            }
            "internal local" => {
                internal_local_code(&mut ctrl, JsonDbOp::Delete, "ghost", &[]).await
            }
            _ => internal_forwarded_code(&mut ctrl, JsonDbOp::Delete, "ghost", &[]).await,
        };
        answers.push((path, code, ctrl.db_get("notes", "n1").is_some()));
    }

    assert_eq!(
        answers,
        [
            ("admin local", json!(404), true),
            ("admin forwarded", json!(404), true),
            ("internal local", json!(404), true),
            ("internal forwarded", json!(404), true),
        ]
    );
}

#[tokio::test]
async fn a_restricting_reference_does_not_hide_that_the_record_is_missing() {
    let mut ctrl = note_referencing_a_missing_doc(OnDeleteAction::Restrict).await;

    let local = local_response(&mut ctrl, JsonDbOp::Delete, "ghost", "root", &[]).await;
    let forwarded = forwarded_response(&mut ctrl, JsonDbOp::Delete, "ghost", "root", &[]).await;
    let internal = internal_local_code(&mut ctrl, JsonDbOp::Delete, "ghost", &[]).await;

    assert_eq!(
        [local["code"].clone(), forwarded["code"].clone(), internal],
        [json!(404), json!(404), json!(404)]
    );
}

#[tokio::test]
async fn deleting_a_missing_record_touches_no_referencing_rows_on_every_path() {
    let mut ctrl = note_referencing_a_missing_doc(OnDeleteAction::SetNull).await;
    let note = ghost_note();

    let mut answers = Vec::new();
    for sender in ["bob", "root"] {
        let local = local_response(&mut ctrl, JsonDbOp::Delete, "ghost", sender, &[]).await;
        let after_local: Value =
            serde_json::from_slice(&ctrl.db_get("notes", "n1").unwrap().data).unwrap();
        ctrl.db_update("notes", "n1", &note, 2000).await.unwrap();
        let forwarded = forwarded_response(&mut ctrl, JsonDbOp::Delete, "ghost", sender, &[]).await;
        let after_forwarded: Value =
            serde_json::from_slice(&ctrl.db_get("notes", "n1").unwrap().data).unwrap();
        ctrl.db_update("notes", "n1", &note, 3000).await.unwrap();
        answers.push((
            sender,
            [local["code"].clone(), after_local["parent_id"].clone()],
            [
                forwarded["code"].clone(),
                after_forwarded["parent_id"].clone(),
            ],
        ));
    }

    let untouched = [json!(404), json!("ghost")];
    assert_eq!(
        answers,
        [
            ("bob", untouched.clone(), untouched.clone()),
            ("root", untouched.clone(), untouched),
        ]
    );
}

#[tokio::test]
async fn a_node_that_is_not_primary_forwards_writes_it_cannot_see() {
    let partition = data_partition("docs", "held-elsewhere");
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    ctrl.set_ownership(ownership());
    let mut map = PartitionMap::default();
    map.set(
        partition,
        crate::cluster::PartitionAssignment::new(node(2), vec![], Epoch::new(1)),
    );
    ctrl.update_partition_map(map);
    let update = serde_json::to_vec(&json!({"title": "x"})).unwrap();

    let mut answered_locally = Vec::new();
    for (topic, payload) in [
        ("$DB/docs/held-elsewhere/update", update.as_slice()),
        ("$DB/docs/held-elsewhere/delete", &[]),
    ] {
        let result = DbRequestHandler::new(node(1))
            .with_ownership(ownership())
            .handle_publish(
                &mut ctrl,
                topic,
                payload,
                &MqttRequestContext {
                    response_topic: Some("resp/local"),
                    correlation_data: None,
                    sender: Some("bob"),
                    client_id: None,
                },
            )
            .await;
        answered_locally.push(!matches!(result, DbPublishResult::NoResponse));
    }

    let forwarded: Vec<JsonDbOp> = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbRequest { request, .. } if to == node(2) => Some(request.op),
            _ => None,
        })
        .collect();
    assert_eq!(answered_locally, [false, false]);
    assert_eq!(forwarded, [JsonDbOp::Update, JsonDbOp::Delete]);
}

#[tokio::test]
async fn a_not_yet_primary_refuses_malformed_payloads_and_stale_copy_writes_alike() {
    let partition = data_partition("docs", "child-1");
    let mut ctrl = not_yet_primary(partition);
    let stale = serde_json::to_vec(&json!({"userId": "alice", "title": "a"})).unwrap();
    ctrl.stores
        .db_create_replicated("docs", "child-1", &stale, 1000)
        .unwrap();
    let update = serde_json::to_vec(&json!({"title": "x"})).unwrap();
    let cases: [(JsonDbOp, &[u8], u16); 4] = [
        (JsonDbOp::Update, b"not json", 400),
        (JsonDbOp::Delete, b"not json", 400),
        (JsonDbOp::Update, &update, 503),
        (JsonDbOp::Delete, &[], 503),
    ];

    let mut codes = Vec::new();
    for (op, payload, _) in cases {
        codes.push((
            op,
            codes_for_payload(&mut ctrl, op, "child-1", "bob", payload).await,
        ));
    }

    let expected: Vec<(JsonDbOp, [Value; 2])> = cases
        .iter()
        .map(|&(op, _, code)| (op, [json!(code), json!(code)]))
        .collect();
    assert_eq!(codes, expected);
    assert!(ctrl.db_get("docs", "child-1").is_some());
}
