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

#[tokio::test]
async fn cascade_share_stops_at_the_same_bound_as_agent_mode() {
    let chain: Vec<(String, Value)> = (0..300)
        .map(|i| {
            let id = format!("c{i}");
            let record =
                json!({"userId": "alice", "title": id, "parent_id": format!("c{}", i + 1)});
            (id, record)
        })
        .collect();
    let records: Vec<(&str, Value)> = chain
        .iter()
        .map(|(id, record)| (id.as_str(), record.clone()))
        .collect();
    let mut ctrl = primary_with(&records).await;

    let shared = local(
        &mut ctrl,
        "$DB/diagrams/c0/share",
        &carol_edit_cascade(),
        "alice",
    )
    .await;

    assert_eq!(counts(&shared), (json!(256), json!(0)));
}

fn ladder(rungs: usize) -> Vec<(String, Value)> {
    (0..rungs)
        .rev()
        .flat_map(|i| {
            let mut rung = json!({"userId": "alice", "title": "c", "ref2": format!("b{i}")});
            if i + 1 < rungs {
                rung["parent_id"] = json!(format!("c{}", i + 1));
            }
            [
                (format!("b{i}"), json!({"userId": "bob", "title": "b"})),
                (format!("c{i}"), rung),
            ]
        })
        .collect()
}

#[tokio::test]
async fn cascade_share_at_the_bound_follows_reference_fields_in_a_fixed_order() {
    let rungs = ladder(200);
    let records: Vec<(&str, Value)> = rungs
        .iter()
        .map(|(id, record)| (id.as_str(), record.clone()))
        .collect();
    let mut outcomes = Vec::new();
    for _ in 0..12 {
        let mut ctrl = primary_with(&records).await;
        let second = ClusterConstraint::foreign_key(
            "diagrams",
            "diagrams_ref2_fk",
            "ref2",
            "diagrams",
            "id",
            OnDeleteAction::Restrict,
        )
        .unwrap();
        ctrl.constraint_add(&second).await.unwrap();
        let shared = local(
            &mut ctrl,
            "$DB/diagrams/c0/share",
            &carol_edit_cascade(),
            "alice",
        )
        .await;
        outcomes.push(counts(&shared));
    }

    assert_eq!(outcomes, vec![(json!(129), json!(127)); 12]);
}

#[cfg(feature = "http-api")]
#[tokio::test]
async fn a_superseded_share_reports_the_same_counts_as_any_share() {
    let mut ctrl = primary_with(&alice_over_bob()).await;
    let continuation = crate::cluster::node_controller::ShareResolveContinuation {
        email_hash: "carol-email-hash".to_string(),
        entity: "diagrams".to_string(),
        id: "alice-d".to_string(),
        grantee_key: "carol-email-hash".to_string(),
        grantee_email: None,
        permission: "edit".to_string(),
        cascade: true,
        granted_by: "alice".to_string(),
        response_topic: "resp/superseded".to_string(),
        correlation_data: None,
        invalidated: true,
    };

    ctrl.complete_share_resolution(Vec::new(), continuation)
        .await;
    let replies: Vec<Value> = ctrl
        .transport()
        .queued_publishes()
        .into_iter()
        .filter_map(|published| match published {
            QueuedPublish::Plain(topic, payload, _) if topic == "resp/superseded" => {
                serde_json::from_slice(&payload).ok()
            }
            _ => None,
        })
        .collect();

    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["data"]["status"], "superseded");
    assert_eq!(counts(&replies[0]), (json!(0), json!(0)));
    assert_eq!(replies[0]["data"]["cascade_truncated"], false);
}

fn chain(length: usize, last_points_at: &str) -> Vec<(String, Value)> {
    (0..length)
        .map(|i| {
            let next = if i + 1 < length {
                format!("c{}", i + 1)
            } else {
                last_points_at.to_string()
            };
            let record = json!({"userId": "alice", "title": "c", "parent_id": next});
            (format!("c{i}"), record)
        })
        .collect()
}

#[tokio::test]
async fn cascade_share_reports_a_reference_dropped_at_the_bound() {
    let mut answers = Vec::new();
    for (length, last_points_at) in [(300, "c300"), (256, "c0"), (10, "c0")] {
        let records = chain(length, last_points_at);
        let records: Vec<(&str, Value)> = records
            .iter()
            .map(|(id, record)| (id.as_str(), record.clone()))
            .collect();
        let mut ctrl = primary_with(&records).await;
        let shared = local(
            &mut ctrl,
            "$DB/diagrams/c0/share",
            &carol_edit_cascade(),
            "alice",
        )
        .await;
        answers.push((
            length,
            shared["data"]["resources_shared"].clone(),
            shared["data"]["cascade_truncated"].clone(),
        ));
    }

    assert_eq!(
        answers,
        [
            (300, json!(256), json!(true)),
            (256, json!(256), json!(false)),
            (10, json!(10), json!(false)),
        ]
    );
}

async fn share_as(
    ctrl: &mut NodeController<MockTransport>,
    id: &str,
    body: Value,
    sender: &str,
) -> Value {
    local(
        ctrl,
        &format!("$DB/diagrams/{id}/share"),
        &bytes(&body),
        sender,
    )
    .await
}

fn alice_tree() -> [(&'static str, Value); 2] {
    [
        ("kid", json!({"userId": "alice", "title": "k"})),
        (
            "top",
            json!({"userId": "alice", "title": "t", "parent_id": "kid"}),
        ),
    ]
}

#[tokio::test]
async fn cascade_share_counts_a_remote_child_as_skipped() {
    let mut ctrl = primary_with(&alice_tree()).await;
    ctrl.become_replica(data_partition("diagrams", "kid"), Epoch::new(1), 0);
    let shared = share_as(
        &mut ctrl,
        "top",
        json!({"grantee": "carol", "permission": "edit", "cascade": true}),
        "alice",
    )
    .await;
    assert_eq!(counts(&shared), (json!(1), json!(1)));
    assert_eq!(grants_on(&ctrl, "kid"), 0);
}

#[tokio::test]
async fn cascade_share_does_not_count_a_remote_child_with_a_local_grant_as_shared() {
    let mut ctrl = primary_with(&alice_tree()).await;
    share_as(
        &mut ctrl,
        "kid",
        json!({"grantee": "carol", "permission": "edit", "cascade": false}),
        "alice",
    )
    .await;
    ctrl.become_replica(data_partition("diagrams", "kid"), Epoch::new(1), 0);
    let shared = share_as(
        &mut ctrl,
        "top",
        json!({"grantee": "carol", "permission": "edit", "cascade": true}),
        "alice",
    )
    .await;
    assert_eq!(counts(&shared), (json!(1), json!(1)));
}

#[tokio::test]
async fn admin_cascade_share_skips_a_dangling_reference() {
    let mut ctrl =
        primary_with(&[("top", json!({"userId": "alice", "parent_id": "later-d"}))]).await;
    let shared = share_as(
        &mut ctrl,
        "top",
        json!({"grantee": "carol", "permission": "edit", "cascade": true}),
        "root",
    )
    .await;
    assert_eq!(counts(&shared), (json!(1), json!(1)));
    assert_eq!(grants_on(&ctrl, "later-d"), 0);
}

#[tokio::test]
async fn cascade_share_raises_a_lower_grant() {
    let mut ctrl = primary_with(&alice_tree()).await;
    share_as(
        &mut ctrl,
        "kid",
        json!({"grantee": "carol", "permission": "view", "cascade": false}),
        "alice",
    )
    .await;
    share_as(
        &mut ctrl,
        "top",
        json!({"grantee": "carol", "permission": "edit", "cascade": true}),
        "alice",
    )
    .await;
    assert_eq!(
        carol_access(&mut ctrl, "kid").await,
        (json!("ok"), json!("ok"))
    );
}

#[tokio::test]
async fn cascade_share_does_not_lower_a_higher_grant() {
    let mut ctrl = primary_with(&alice_tree()).await;
    share_as(
        &mut ctrl,
        "kid",
        json!({"grantee": "carol", "permission": "edit", "cascade": false}),
        "alice",
    )
    .await;
    share_as(
        &mut ctrl,
        "top",
        json!({"grantee": "carol", "permission": "view", "cascade": true}),
        "alice",
    )
    .await;
    assert_eq!(
        carol_access(&mut ctrl, "kid").await,
        (json!("ok"), json!("ok"))
    );
}

#[tokio::test]
async fn cascade_share_without_a_sender_reaches_every_owner() {
    let mut ctrl = primary_with(&alice_over_bob()).await;
    let reply = ctrl
        .handle_share_local("diagrams", "alice-d", &carol_edit_cascade(), None)
        .await;
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(counts(&reply), (json!(2), json!(0)));
    assert_eq!(grants_on(&ctrl, "bob-d"), 1);
}
