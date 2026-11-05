// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::data_partition;
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, MqttRequestContext};
use crate::cluster::protocol::{JsonDbOp, JsonDbRequest};
use mqdb_core::types::OwnershipConfig;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

const REFUSAL: &str = "the ownership field 'userId' cannot be changed";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn ownership() -> Arc<OwnershipConfig> {
    let fields = HashMap::from([("docs".to_string(), "userId".to_string())]);
    let admins = HashSet::from(["root".to_string()]);
    Arc::new(OwnershipConfig::new(fields).with_admin_users(admins))
}

async fn publish(
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &Value,
    sender: &str,
) -> DbPublishResult {
    DbRequestHandler::new(ctrl.node_id())
        .with_ownership(ownership())
        .handle_publish(
            ctrl,
            topic,
            &serde_json::to_vec(payload).unwrap(),
            &MqttRequestContext {
                response_topic: Some("resp/client"),
                correlation_data: None,
                sender: Some(sender),
                client_id: None,
            },
        )
        .await
}

async fn primary_with_docs() -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(1), MockTransport::new(node(1)));
    make_all_primary(&mut ctrl, node(1));
    ctrl.set_ownership(ownership());
    for (id, record) in [
        (
            "d1",
            json!({"userId": "alice", "title": "first", "_version": 1}),
        ),
        ("no-owner", json!({"title": "first", "_version": 1})),
        ("null-owner", json!({"userId": null, "_version": 1})),
        ("number-owner", json!({"userId": 7, "_version": 1})),
    ] {
        let bytes = serde_json::to_vec(&record).unwrap();
        ctrl.db_create("docs", id, &bytes, 1000).await.unwrap();
    }
    for (id, grantor, grantee, permission) in [
        ("d1", "alice", "carol", "edit"),
        ("d1", "alice", "dave", "view"),
        ("no-owner", "root", "carol", "edit"),
        ("null-owner", "root", "carol", "edit"),
        ("number-owner", "root", "carol", "edit"),
    ] {
        let answer = local(
            &mut ctrl,
            &format!("$DB/docs/{id}/share"),
            &json!({"grantee": grantee, "permission": permission}),
            grantor,
        )
        .await;
        assert_eq!(answer["status"], "ok", "{answer}");
    }
    ctrl
}

fn receiver_of_another_primary() -> NodeController<MockTransport> {
    let mut ctrl = create_test_controller(node(2), MockTransport::new(node(2)));
    let mut map = PartitionMap::new();
    for partition_id in 0..NUM_PARTITIONS {
        if let Some(partition) = PartitionId::new(partition_id) {
            map.set(
                partition,
                crate::cluster::PartitionAssignment {
                    primary: Some(node(1)),
                    replicas: vec![],
                    epoch: Epoch::new(1),
                    primary_since: Epoch::new(1),
                },
            );
        }
    }
    ctrl.update_partition_map(map);
    ctrl.set_ownership(ownership());
    ctrl
}

async fn local(
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &Value,
    sender: &str,
) -> Value {
    let DbPublishResult::Response(response) = publish(ctrl, topic, payload, sender).await else {
        panic!("{topic} was not answered on the primary");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

async fn update_on_primary(
    ctrl: &mut NodeController<MockTransport>,
    id: &str,
    payload: &Value,
    sender: &str,
) -> Value {
    local(ctrl, &format!("$DB/docs/{id}/update"), payload, sender).await
}

fn last_forwarded(receiver: &NodeController<MockTransport>) -> JsonDbRequest {
    let sent = receiver.transport().sent_messages();
    let (to, message) = sent.last().unwrap().clone();
    assert_eq!(to, node(1));
    let ClusterMessage::JsonDbRequest { request, .. } = message else {
        panic!("the receiver did not forward: {message:?}");
    };
    let bytes = request.to_bytes().unwrap();
    JsonDbRequest::from_bytes(&bytes).unwrap()
}

async fn answer_forwarded(
    primary: &mut NodeController<MockTransport>,
    request: &JsonDbRequest,
) -> Value {
    let already = primary.transport().sent_messages().len();
    let id = request.id.as_deref().unwrap();
    primary
        .handle_json_db_request(node(2), data_partition("docs", id), request)
        .await;
    primary
        .transport()
        .sent_messages()
        .into_iter()
        .skip(already)
        .find_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response) if to == node(2) => {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .expect("the forwarded update was not answered")
}

async fn update_via_receiver(
    primary: &mut NodeController<MockTransport>,
    id: &str,
    payload: &Value,
    sender: &str,
) -> Value {
    let mut receiver = receiver_of_another_primary();
    publish(
        &mut receiver,
        &format!("$DB/docs/{id}/update"),
        payload,
        sender,
    )
    .await;
    let request = last_forwarded(&receiver);
    answer_forwarded(primary, &request).await
}

async fn update_forwarded_unstripped(
    primary: &mut NodeController<MockTransport>,
    id: &str,
    payload: &Value,
    sender: &str,
) -> Value {
    let request = JsonDbRequest {
        request_id: 1,
        op: JsonDbOp::Update,
        entity: "docs".to_string(),
        id: Some(id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "resp/client".to_string(),
        correlation_data: None,
        sender: Some(sender.to_string()),
        client_id: None,
        requested_owner: None,
    };
    answer_forwarded(primary, &request).await
}

async fn answers_on_every_path(
    primary: &mut NodeController<MockTransport>,
    id: &str,
    payload: &Value,
    sender: &str,
) -> [Value; 3] {
    [
        update_on_primary(primary, id, payload, sender).await,
        update_via_receiver(primary, id, payload, sender).await,
        update_forwarded_unstripped(primary, id, payload, sender).await,
    ]
}

fn stored(ctrl: &NodeController<MockTransport>, id: &str) -> Value {
    serde_json::from_slice(&ctrl.db_get("docs", id).unwrap().data).unwrap()
}

fn events(ctrl: &NodeController<MockTransport>) -> usize {
    ctrl.transport().queued_publishes().len()
}

fn assert_refused(answers: &[Value], context: &str) {
    for answer in answers {
        assert_eq!(answer["code"], 403, "{context}: {answer}");
        assert_eq!(answer["message"], REFUSAL, "{context}: {answer}");
    }
}

#[tokio::test]
async fn owner_and_editor_are_refused_on_every_path_and_nothing_is_applied() {
    let mut primary = primary_with_docs().await;
    let before = stored(&primary, "d1");
    let events_before = events(&primary);

    for sender in ["alice", "carol"] {
        for payload in [
            json!({"userId": "bob"}),
            json!({"userId": "bob", "title": "changed"}),
            json!({"userId": sender, "title": "changed"}),
            json!({"userId": null}),
            json!({"userId": 7}),
            json!({"userId": ["alice"]}),
        ] {
            if sender == "alice" && payload["userId"] == "alice" {
                continue;
            }
            let answers = answers_on_every_path(&mut primary, "d1", &payload, sender).await;
            assert_refused(&answers, &format!("{sender} {payload}"));
        }
    }

    assert_eq!(stored(&primary, "d1"), before);
    assert_eq!(events(&primary), events_before);
}

#[tokio::test]
async fn a_value_equal_to_the_current_owner_is_allowed_on_every_path() {
    let mut primary = primary_with_docs().await;
    let mut version = 1;

    for sender in ["alice", "carol"] {
        let payload = json!({"userId": "alice", "title": sender});
        let events_before = events(&primary);
        for answer in answers_on_every_path(&mut primary, "d1", &payload, sender).await {
            assert_eq!(answer["status"], "ok", "{sender}: {answer}");
            version += 1;
            assert_eq!(answer["data"]["_version"], version, "{sender}: {answer}");
            assert_eq!(answer["data"]["userId"], "alice");
            assert_eq!(answer["data"]["title"], sender);
        }
        assert!(events(&primary) > events_before);
    }
}

#[tokio::test]
async fn others_get_the_same_answers_as_before_on_every_path() {
    let mut primary = primary_with_docs().await;
    let before = stored(&primary, "d1");

    for sender in ["bob", "dave"] {
        for owner in ["alice", "bob", "dave"] {
            let payload = json!({"userId": owner});
            for answer in answers_on_every_path(&mut primary, "d1", &payload, sender).await {
                assert_eq!(answer["code"], 403, "{sender} {owner}: {answer}");
                assert_eq!(answer["message"], "permission denied", "{sender}: {answer}");
            }
        }
    }
    for sender in ["alice", "bob", "carol"] {
        let payload = json!({"userId": sender});
        for answer in answers_on_every_path(&mut primary, "missing", &payload, sender).await {
            assert_eq!(answer["code"], 404, "{sender}: {answer}");
        }
    }
    assert_eq!(stored(&primary, "d1"), before);
    assert!(primary.db_get("docs", "missing").is_none());
}

#[tokio::test]
async fn an_admin_keeps_changing_the_owner_on_every_path() {
    let mut primary = primary_with_docs().await;

    let answer = update_on_primary(&mut primary, "d1", &json!({"userId": "bob"}), "root").await;
    assert_eq!(answer["status"], "ok", "{answer}");
    assert_eq!(stored(&primary, "d1")["userId"], "bob");

    let answer = update_via_receiver(&mut primary, "d1", &json!({"userId": "erin"}), "root").await;
    assert_eq!(answer["status"], "ok", "{answer}");
    assert_eq!(stored(&primary, "d1")["userId"], "erin");
}

#[tokio::test]
async fn an_editor_cannot_take_a_record_with_no_owner_or_a_null_owner() {
    let mut primary = primary_with_docs().await;
    let no_owner = stored(&primary, "no-owner");
    let null_owner = stored(&primary, "null-owner");

    for payload in [json!({"userId": "carol"}), json!({"userId": "bob"})] {
        for id in ["no-owner", "null-owner"] {
            let answers = answers_on_every_path(&mut primary, id, &payload, "carol").await;
            assert_refused(&answers, id);
        }
    }
    let answers =
        answers_on_every_path(&mut primary, "no-owner", &json!({"userId": null}), "carol").await;
    assert_refused(&answers, "null onto a missing owner");
    assert_eq!(stored(&primary, "no-owner"), no_owner);
    assert_eq!(stored(&primary, "null-owner"), null_owner);

    let same = answers_on_every_path(
        &mut primary,
        "null-owner",
        &json!({"userId": null}),
        "carol",
    )
    .await;
    for answer in same {
        assert_eq!(answer["status"], "ok", "{answer}");
    }
    let by_admin = update_via_receiver(
        &mut primary,
        "no-owner",
        &json!({"userId": "alice"}),
        "root",
    )
    .await;
    assert_eq!(by_admin["status"], "ok", "{by_admin}");
    assert_eq!(stored(&primary, "no-owner")["userId"], "alice");
}

#[tokio::test]
async fn a_stored_non_string_owner_compares_as_json() {
    let mut primary = primary_with_docs().await;

    let answers = answers_on_every_path(
        &mut primary,
        "number-owner",
        &json!({"userId": "7"}),
        "carol",
    )
    .await;
    assert_refused(&answers, "string against number");
    for answer in
        answers_on_every_path(&mut primary, "number-owner", &json!({"userId": 7}), "carol").await
    {
        assert_eq!(answer["status"], "ok", "{answer}");
    }
    assert_eq!(stored(&primary, "number-owner")["userId"], 7);
}

#[tokio::test]
async fn a_receiver_without_the_record_moves_the_owner_out_of_the_payload() {
    let mut receiver = receiver_of_another_primary();
    assert!(receiver.db_get("docs", "d1").is_none());

    let payload = json!({"userId": "bob", "title": "changed"});
    let answered = publish(&mut receiver, "$DB/docs/d1/update", &payload, "alice").await;
    assert!(!matches!(answered, DbPublishResult::Response(_)));

    let request = last_forwarded(&receiver);
    let fields: Value = serde_json::from_slice(&request.payload).unwrap();
    assert_eq!(fields, json!({"title": "changed"}));
    assert_eq!(request.requested_owner, Some(json!("bob")));

    let payload = json!({"title": "changed"});
    publish(&mut receiver, "$DB/docs/d1/update", &payload, "alice").await;
    assert_eq!(last_forwarded(&receiver).requested_owner, None);

    let payload = json!({"userId": "bob"});
    publish(&mut receiver, "$DB/docs/d1/update", &payload, "root").await;
    let by_admin = last_forwarded(&receiver);
    assert_eq!(by_admin.requested_owner, None);
    assert_eq!(
        serde_json::from_slice::<Value>(&by_admin.payload).unwrap(),
        payload
    );
}

#[tokio::test]
async fn the_refusal_comes_before_a_stale_expected_version_on_every_path() {
    let mut primary = primary_with_docs().await;

    let changing = json!({"userId": "bob", "_expected_version": 9});
    let answers = answers_on_every_path(&mut primary, "d1", &changing, "alice").await;
    assert_refused(&answers, "stale version");

    let equal = json!({"userId": "alice", "_expected_version": 9});
    for answer in answers_on_every_path(&mut primary, "d1", &equal, "alice").await {
        assert_eq!(answer["code"], 412, "{answer}");
    }
}

#[tokio::test]
async fn an_older_receiver_that_stripped_the_field_is_answered_as_before() {
    let mut primary = primary_with_docs().await;
    let answer =
        update_forwarded_unstripped(&mut primary, "d1", &json!({"title": "stripped"}), "alice")
            .await;
    assert_eq!(answer["status"], "ok", "{answer}");
    assert_eq!(stored(&primary, "d1")["userId"], "alice");
}
