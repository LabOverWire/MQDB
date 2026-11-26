// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::controller_where;
use super::*;
use crate::cluster::db::{ClusterConstraint, data_partition};
use serde_json::{Value, json};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn forwarded(op: JsonDbOp, entity: &str, id: &str, payload: &Value) -> JsonDbRequest {
    JsonDbRequest {
        request_id: 9,
        op,
        entity: entity.to_string(),
        id: Some(id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

#[tokio::test]
async fn a_conflict_on_one_remote_reserve_releases_the_others() {
    use crate::cluster::db::unique_partition;
    use crate::cluster::node_controller::unique::await_unique_phase1;
    let email = serde_json::to_vec(&json!("a@x")).unwrap();
    let phone = serde_json::to_vec(&json!("555")).unwrap();
    let remote = [
        unique_partition("users", "email", &email),
        unique_partition("users", "phone", &phone),
    ];
    assert_ne!(data_partition("users", "u1"), remote[0]);
    assert_ne!(data_partition("users", "u1"), remote[1]);
    let mut ctrl = controller_where(
        None,
        |p| {
            if remote.contains(&p) {
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
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_phone", "phone").unwrap())
        .await
        .unwrap();
    let mut request = forwarded(
        JsonDbOp::Create,
        "users",
        "u1",
        &json!({"id": "u1", "email": "a@x", "phone": "555"}),
    );
    request.id = None;
    let work = ctrl
        .handle_json_db_request(node(2), data_partition("users", "u1"), &request)
        .await;
    let Some(PendingConstraintWork::Unique(work)) = work else {
        panic!("create should wait on the two remote reserves");
    };
    assert_eq!(work.phase1.pending_remote.len(), 2);
    let reserves: Vec<(u64, String)> = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::UniqueReserveRequest(r) => {
                Some((r.request_id, String::from_utf8(r.field.clone()).unwrap()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(reserves.len(), 2);
    ctrl.pending_constraints()
        .resolve_unique(&UniqueReserveResponse::create(
            reserves[0].0,
            UniqueReserveStatus::Conflict,
        ));
    ctrl.pending_constraints()
        .resolve_unique(&UniqueReserveResponse::create(
            reserves[1].0,
            UniqueReserveStatus::Reserved,
        ));
    let results = await_unique_phase1(work.phase1.pending_remote, work.phase1.pending_quorum).await;
    ctrl.complete_pending_unique_work(work.phase1.local_reserved, results, work.continuation)
        .await;
    let released: Vec<String> = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::UniqueReleaseRequest(r) => {
                Some(String::from_utf8(r.field.clone()).unwrap())
            }
            _ => None,
        })
        .collect();
    let replies: Vec<Value> = ctrl
        .transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::JsonDbResponse(r) => serde_json::from_slice(&r.payload).ok(),
            _ => None,
        })
        .collect();
    assert_eq!(
        released,
        vec![reserves[1].1.clone()],
        "node 2 reserved {} for u1 and was never told to release it; replies {replies:?}",
        reserves[1].1
    );
}
