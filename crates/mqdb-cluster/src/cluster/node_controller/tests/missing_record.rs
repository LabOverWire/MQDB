// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::cluster::db::{data_partition, generate_id_for_partition};
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
) -> Value {
    let result = DbRequestHandler::new(node(1))
        .with_ownership(ownership())
        .handle_publish(
            ctrl,
            &op_topic(op, id),
            &op_payload(op),
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
) -> Value {
    let request = JsonDbRequest {
        request_id: 1,
        op,
        entity: "docs".to_string(),
        id: Some(id.to_string()),
        payload: op_payload(op),
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
        let local = local_response(ctrl, op, id, sender).await;
        codes.push((op, "local", local["code"].clone()));
        let forwarded = forwarded_response(ctrl, op, id, sender).await;
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
        local_response(&mut ctrl, JsonDbOp::Share, "never-created", "root").await,
        forwarded_response(&mut ctrl, JsonDbOp::Share, "never-created", "root").await,
    ] {
        assert_eq!(answer["code"], 404, "admin share {answer}");
    }
    for op in [JsonDbOp::Unshare, JsonDbOp::Shares] {
        for answer in [
            local_response(&mut ctrl, op, "never-created", "root").await,
            forwarded_response(&mut ctrl, op, "never-created", "root").await,
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
