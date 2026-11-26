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

async fn releases_after_the_first_remote_reserve_answers(
    first: UniqueReserveStatus,
) -> (Vec<String>, Vec<String>) {
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
        .resolve_unique(&UniqueReserveResponse::create(reserves[0].0, first));
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
    let fields = reserves.into_iter().map(|(_, field)| field).collect();
    (fields, released)
}

#[tokio::test]
async fn a_conflict_on_one_remote_reserve_releases_the_others() {
    let (reserved, released) =
        releases_after_the_first_remote_reserve_answers(UniqueReserveStatus::Conflict).await;
    assert_eq!(released, vec![reserved[1].clone()]);
}

#[tokio::test]
async fn a_failed_remote_reserve_is_released_with_the_others() {
    let (reserved, released) =
        releases_after_the_first_remote_reserve_answers(UniqueReserveStatus::Error).await;
    assert_eq!(released, reserved);
}

async fn reserves_and_releases_after_a_local_conflict(
    op: JsonDbOp,
) -> (Vec<String>, Vec<String>, String) {
    use crate::cluster::db::{ReserveResult, UniqueReserveParams, unique_partition};
    let fields = ["email", "phone"];
    let remote_value = serde_json::to_vec(&json!("remote-value")).unwrap();
    let local_value = serde_json::to_vec(&json!("local-value")).unwrap();
    let remote_partitions: Vec<PartitionId> = fields
        .iter()
        .map(|field| unique_partition("users", field, &remote_value))
        .collect();
    for field in fields {
        assert!(!remote_partitions.contains(&unique_partition("users", field, &local_value)));
    }
    assert!(!remote_partitions.contains(&data_partition("users", "u1")));
    let mut ctrl = controller_where(
        None,
        |p| {
            if remote_partitions.contains(&p) {
                node(2)
            } else {
                node(1)
            }
        },
        &[],
    );
    ctrl.seed_unique_voters(std::iter::once(node(1)).collect());
    for field in fields {
        ctrl.constraint_add(
            &ClusterConstraint::unique("users", &format!("uniq_{field}"), field).unwrap(),
        )
        .await
        .unwrap();
    }
    let order = ctrl.stores().constraint_get_unique_fields("users");
    let (remote_field, local_field) = (order[0].clone(), order[1].clone());
    let (taken, _) = ctrl.stores().unique_reserve_replicated(
        &UniqueReserveParams {
            entity: "users",
            field: &local_field,
            value: &local_value,
            record_id: "other",
            request_id: "other-request",
            data_partition: data_partition("users", "other"),
            ttl_ms: 30_000,
        },
        NodeController::<MockTransport>::current_time_ms(),
    );
    assert!(matches!(taken, ReserveResult::Reserved));
    if op == JsonDbOp::Update {
        crate::cluster::test_support::put_record(
            &ctrl.stores().db_data,
            "users",
            "u1",
            br#"{"id":"u1"}"#,
            1,
        );
    }
    let mut record = serde_json::Map::new();
    record.insert("id".into(), json!("u1"));
    record.insert(remote_field.clone(), json!("remote-value"));
    record.insert(local_field.clone(), json!("local-value"));
    let request = forwarded(op, "users", "u1", &Value::Object(record));

    let work = ctrl
        .handle_json_db_request(node(2), data_partition("users", "u1"), &request)
        .await;

    assert!(
        work.is_none(),
        "the write should be refused on the local conflict"
    );
    let sent = ctrl.transport().sent_messages();
    let reserved: Vec<String> = sent
        .iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::UniqueReserveRequest(r) => {
                Some(String::from_utf8(r.field.clone()).unwrap())
            }
            _ => None,
        })
        .collect();
    let released: Vec<String> = sent
        .iter()
        .filter_map(|(_, m)| match m {
            ClusterMessage::UniqueReleaseRequest(r) => {
                Some(String::from_utf8(r.field.clone()).unwrap())
            }
            _ => None,
        })
        .collect();
    (reserved, released, remote_field)
}

#[tokio::test]
async fn a_create_refused_on_a_local_conflict_releases_the_remote_reserves_already_sent() {
    let (reserved, released, remote_field) =
        reserves_and_releases_after_a_local_conflict(JsonDbOp::Create).await;
    assert_eq!(
        (reserved, released),
        (vec![remote_field.clone()], vec![remote_field])
    );
}

#[tokio::test]
async fn an_update_refused_on_a_local_conflict_releases_the_remote_reserves_already_sent() {
    let (reserved, released, remote_field) =
        reserves_and_releases_after_a_local_conflict(JsonDbOp::Update).await;
    assert_eq!(
        (reserved, released),
        (vec![remote_field.clone()], vec![remote_field])
    );
}
