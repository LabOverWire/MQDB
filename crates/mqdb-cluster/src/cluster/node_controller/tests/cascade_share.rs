// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition};
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, MqttRequestContext};
use crate::cluster::protocol::{JsonDbOp, JsonDbRequest};
use mqdb_core::types::{OwnershipConfig, SHARES_ENTITY};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn ownership() -> Arc<OwnershipConfig> {
    let fields = HashMap::from([("diagrams".to_string(), "userId".to_string())]);
    let admins = HashSet::from(["root".to_string()]);
    Arc::new(OwnershipConfig::new(fields).with_admin_users(admins))
}

async fn primary_with(records: &[(&str, Value)]) -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    make_all_primary(&mut ctrl, node(1));
    ctrl.set_ownership(ownership());
    let fk = ClusterConstraint::foreign_key(
        "diagrams",
        "diagrams_parent_fk",
        "parent_id",
        "diagrams",
        "id",
        OnDeleteAction::Restrict,
    )
    .unwrap();
    ctrl.constraint_add(&fk).await.unwrap();
    for (id, record) in records {
        ctrl.db_create("diagrams", id, &bytes(record), 1000)
            .await
            .unwrap();
    }
    ctrl
}

async fn local(
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &[u8],
    sender: &str,
) -> Value {
    let result = DbRequestHandler::new(node(1))
        .with_ownership(ownership())
        .handle_publish(
            ctrl,
            topic,
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
        panic!("{topic} was not answered on the primary");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

fn replies(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
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

async fn forwarded_share(
    ctrl: &mut NodeController<MockTransport>,
    id: &str,
    payload: &[u8],
    sender: &str,
) -> Value {
    let request = JsonDbRequest {
        request_id: 1,
        op: JsonDbOp::Share,
        entity: "diagrams".to_string(),
        id: Some(id.to_string()),
        payload: payload.to_vec(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: Some(sender.to_string()),
        client_id: None,
    };
    let before = replies(ctrl).len();
    ctrl.handle_json_db_request(node(2), data_partition("diagrams", id), &request)
        .await;
    replies(ctrl)[before].clone()
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn answer(response: &Value) -> Value {
    if response["status"] == "ok" {
        json!("ok")
    } else {
        response["code"].clone()
    }
}

fn counts(reply: &Value) -> (Value, Value) {
    (
        reply["data"]["resources_shared"].clone(),
        reply["data"]["resources_skipped"].clone(),
    )
}

async fn carol_access(ctrl: &mut NodeController<MockTransport>, id: &str) -> (Value, Value) {
    let read = local(ctrl, &format!("$DB/diagrams/{id}"), b"{}", "carol").await;
    let update = local(
        ctrl,
        &format!("$DB/diagrams/{id}/update"),
        &bytes(&json!({"title": "carol-was-here"})),
        "carol",
    )
    .await;
    (answer(&read), answer(&update))
}

fn grants_on(ctrl: &NodeController<MockTransport>, id: &str) -> usize {
    ctrl.db_list(SHARES_ENTITY)
        .into_iter()
        .filter(|entity| {
            serde_json::from_slice::<Value>(&entity.data)
                .is_ok_and(|grant| grant["resource_id"] == id)
        })
        .count()
}

fn carol_edit_cascade() -> Vec<u8> {
    bytes(&json!({"grantee": "carol", "permission": "edit", "cascade": true}))
}

fn alice_over_bob() -> [(&'static str, Value); 2] {
    [
        ("bob-d", json!({"userId": "bob", "title": "bob-secret"})),
        (
            "alice-d",
            json!({"userId": "alice", "title": "a", "parent_id": "bob-d"}),
        ),
    ]
}

#[tokio::test]
async fn cascade_share_does_not_grant_on_a_referenced_record_owned_by_someone_else() {
    let mut answers = Vec::new();
    for path in ["local", "forwarded"] {
        let mut ctrl = primary_with(&alice_over_bob()).await;
        let shared = match path {
            "local" => {
                local(
                    &mut ctrl,
                    "$DB/diagrams/alice-d/share",
                    &carol_edit_cascade(),
                    "alice",
                )
                .await
            }
            _ => forwarded_share(&mut ctrl, "alice-d", &carol_edit_cascade(), "alice").await,
        };
        answers.push((
            path,
            counts(&shared),
            carol_access(&mut ctrl, "alice-d").await,
            carol_access(&mut ctrl, "bob-d").await,
        ));
    }

    let expected = (
        (json!(1), json!(1)),
        (json!("ok"), json!("ok")),
        (json!(403), json!(403)),
    );
    assert_eq!(
        answers,
        [
            (
                "local",
                expected.0.clone(),
                expected.1.clone(),
                expected.2.clone()
            ),
            ("forwarded", expected.0, expected.1, expected.2),
        ]
    );
}

#[tokio::test]
async fn cascade_share_grants_the_sharers_own_closure_and_stops_at_a_foreign_record() {
    let mut ctrl = primary_with(&[
        ("alice-far", json!({"userId": "alice", "title": "far"})),
        (
            "bob-mid",
            json!({"userId": "bob", "title": "mid", "parent_id": "alice-far"}),
        ),
        (
            "alice-near",
            json!({"userId": "alice", "title": "near", "parent_id": "bob-mid"}),
        ),
        (
            "alice-root",
            json!({"userId": "alice", "title": "root", "parent_id": "alice-near"}),
        ),
    ])
    .await;

    let shared = local(
        &mut ctrl,
        "$DB/diagrams/alice-root/share",
        &carol_edit_cascade(),
        "alice",
    )
    .await;

    let mut access = Vec::new();
    for id in ["alice-root", "alice-near", "bob-mid", "alice-far"] {
        access.push((id, carol_access(&mut ctrl, id).await));
    }
    assert_eq!(
        access,
        [
            ("alice-root", (json!("ok"), json!("ok"))),
            ("alice-near", (json!("ok"), json!("ok"))),
            ("bob-mid", (json!(403), json!(403))),
            ("alice-far", (json!(403), json!(403))),
        ]
    );
    assert_eq!(counts(&shared), (json!(2), json!(1)));
}

#[tokio::test]
async fn admin_cascade_share_grants_every_owner_in_the_closure() {
    let mut ctrl = primary_with(&alice_over_bob()).await;

    let shared = local(
        &mut ctrl,
        "$DB/diagrams/alice-d/share",
        &carol_edit_cascade(),
        "root",
    )
    .await;

    assert_eq!(
        carol_access(&mut ctrl, "bob-d").await,
        (json!("ok"), json!("ok"))
    );
    assert_eq!(counts(&shared), (json!(2), json!(0)));
}

#[tokio::test]
async fn cascade_share_does_not_grant_on_a_dangling_reference_created_later() {
    let mut ctrl = primary_with(&[]).await;
    let alice = bytes(&json!({"userId": "alice", "title": "a", "parent_id": "later-d"}));
    ctrl.db_create("diagrams", "alice-d", &alice, 1000)
        .await
        .unwrap();

    let shared = local(
        &mut ctrl,
        "$DB/diagrams/alice-d/share",
        &carol_edit_cascade(),
        "alice",
    )
    .await;
    let bob = bytes(&json!({"userId": "bob", "title": "bob-secret"}));
    ctrl.db_create("diagrams", "later-d", &bob, 1000)
        .await
        .unwrap();

    assert_eq!(
        carol_access(&mut ctrl, "later-d").await,
        (json!(403), json!(403))
    );
    assert_eq!(counts(&shared), (json!(1), json!(1)));
}

#[tokio::test]
async fn pending_cascade_share_writes_no_grant_on_a_foreign_record() {
    let mut ctrl = primary_with(&alice_over_bob()).await;
    let pending = bytes(&json!({
        "grantee_key": "carol-email-hash",
        "permission": "edit",
        "cascade": true
    }));

    let reply = ctrl
        .handle_share_local("diagrams", "alice-d", &pending, Some("alice"))
        .await;
    let reply: Value = serde_json::from_slice(&reply).unwrap();

    assert_eq!(reply["data"]["status"], "pending");
    assert_eq!(
        (grants_on(&ctrl, "alice-d"), grants_on(&ctrl, "bob-d")),
        (1, 0)
    );
    assert_eq!(counts(&reply), (json!(1), json!(1)));
}
