// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::{controller_where, publish_completing_unique};
use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition, unique_partition};
use crate::cluster::node_controller::unique::{await_unique_quorum, await_unique_reserves};
use serde_json::{Value, json};

const REMOTE_AUTHOR: &str = "u9";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn encoded(value: &str) -> Vec<u8> {
    serde_json::to_vec(&json!(value)).unwrap()
}

async fn three_voter_primary() -> NodeController<MockTransport> {
    let remote = data_partition("users", REMOTE_AUTHOR);
    for local in [
        data_partition("users", "u1"),
        data_partition("users", "u2"),
        data_partition("posts", "p1"),
        data_partition("posts", "p2"),
        unique_partition("users", "email", &encoded("old@x")),
        unique_partition("users", "email", &encoded("a@x")),
        unique_partition("posts", "slug", &encoded("s0")),
        unique_partition("posts", "slug", &encoded("s1")),
    ] {
        assert_ne!(local, remote);
    }
    let mut ctrl = controller_where(
        None,
        |partition| {
            if partition == remote {
                node(2)
            } else {
                node(1)
            }
        },
        &[node(2), node(3)],
    );
    ctrl.seed_unique_voters(std::iter::once(node(1)).collect());
    ctrl.constraint_add(&ClusterConstraint::unique("users", "uniq_email", "email").unwrap())
        .await
        .unwrap();
    ctrl.constraint_add(&ClusterConstraint::unique("posts", "uniq_slug", "slug").unwrap())
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
        (
            "$DB/posts/create",
            json!({"id": "p2", "slug": "s0", "author_id": "u1"}),
        ),
    ] {
        let reply = publish_completing_unique(&mut ctrl, topic, &payload).await;
        assert_eq!(reply["status"], "ok", "{reply}");
    }
    ctrl.seed_unique_voters([node(1), node(2), node(3)].into_iter().collect());
    ctrl
}

fn forwarded(op: JsonDbOp, entity: &str, id: &str, payload: &Value) -> JsonDbRequest {
    JsonDbRequest {
        request_id: 9,
        op,
        entity: entity.to_string(),
        id: (op != JsonDbOp::Create).then(|| id.to_string()),
        payload: serde_json::to_vec(payload).unwrap(),
        response_topic: "resp/forwarded".to_string(),
        correlation_data: None,
        sender: None,
        client_id: None,
        requested_owner: None,
    }
}

async fn forward(
    ctrl: &mut NodeController<MockTransport>,
    op: JsonDbOp,
    entity: &str,
    id: &str,
    payload: &Value,
) -> Option<PendingConstraintWork> {
    ctrl.handle_json_db_request(
        node(2),
        data_partition(entity, id),
        &forwarded(op, entity, id, payload),
    )
    .await
}

fn forwarded_replies(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
    ctrl.transport()
        .sent_messages()
        .into_iter()
        .filter_map(|(to, message)| match message {
            ClusterMessage::JsonDbResponse(response)
                if to == node(2) && response.response_topic == "resp/forwarded" =>
            {
                Some(serde_json::from_slice(&response.payload).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn stored(ctrl: &NodeController<MockTransport>, entity: &str, id: &str) -> Option<Value> {
    ctrl.db_get(entity, id)
        .map(|record| serde_json::from_slice(&record.data).unwrap())
}

fn waiting_on_the_majority(
    ctrl: &NodeController<MockTransport>,
    work: Option<PendingConstraintWork>,
) -> PendingUniqueWork {
    let Some(PendingConstraintWork::Unique(work)) = work else {
        panic!(
            "answered without waiting for the 3-voter quorum: {:?}",
            forwarded_replies(ctrl)
        );
    };
    assert!(work.phase1.pending_remote.is_empty());
    assert!(!work.phase1.pending_quorum.is_empty());
    assert_eq!(forwarded_replies(ctrl), Vec::<Value>::new());
    work
}

async fn after_the_remote_fk_check(
    ctrl: &mut NodeController<MockTransport>,
    work: Option<PendingConstraintWork>,
) -> PendingUniqueWork {
    let Some(PendingConstraintWork::Fk(fk)) = work else {
        panic!("the write should wait on the FK check sent to node 2");
    };
    let FkThenUniqueOutcome::NeedUnique(unique) = ctrl
        .complete_pending_fk_work(true, None, fk.continuation)
        .await
    else {
        panic!(
            "answered after the FK check without the unique reserve: {:?}",
            forwarded_replies(ctrl)
        );
    };
    waiting_on_the_majority(ctrl, Some(PendingConstraintWork::Unique(*unique)))
}

async fn fail_the_majority(ctrl: &mut NodeController<MockTransport>, work: PendingUniqueWork) {
    ctrl.sweep_unique_quorum(u64::MAX).await;
    let remote_results = match tokio::join!(
        await_unique_reserves(work.phase1.pending_remote),
        await_unique_quorum(work.phase1.pending_quorum),
    ) {
        (Ok(confirmed), Ok(_)) => Ok(confirmed),
        (Ok(confirmed), Err(field)) => Err((ReserveFailure::NotDurable(field), confirmed)),
        (Err(failure), _) => Err(failure),
    };
    ctrl.complete_pending_unique_work(
        work.phase1.local_reserved,
        remote_results,
        work.continuation,
    )
    .await;
}

fn refused_codes(ctrl: &NodeController<MockTransport>) -> Vec<Value> {
    forwarded_replies(ctrl)
        .into_iter()
        .map(|reply| reply["code"].clone())
        .collect()
}

#[tokio::test]
async fn forwarded_update_with_local_unique_waits_for_quorum() {
    let mut ctrl = three_voter_primary().await;

    let work = forward(
        &mut ctrl,
        JsonDbOp::Update,
        "users",
        "u1",
        &json!({"email": "a@x"}),
    )
    .await;
    let work = waiting_on_the_majority(&ctrl, work);
    assert_eq!(stored(&ctrl, "users", "u1").unwrap()["email"], "old@x");

    fail_the_majority(&mut ctrl, work).await;
    assert_eq!(
        (
            refused_codes(&ctrl),
            stored(&ctrl, "users", "u1").unwrap()["email"].clone()
        ),
        (vec![json!(503)], json!("old@x"))
    );
}

#[tokio::test]
async fn forwarded_create_with_local_unique_waits_for_quorum() {
    let mut ctrl = three_voter_primary().await;

    let work = forward(
        &mut ctrl,
        JsonDbOp::Create,
        "users",
        "u2",
        &json!({"id": "u2", "email": "a@x"}),
    )
    .await;
    let work = waiting_on_the_majority(&ctrl, work);
    assert!(stored(&ctrl, "users", "u2").is_none());

    fail_the_majority(&mut ctrl, work).await;
    assert_eq!(
        (refused_codes(&ctrl), stored(&ctrl, "users", "u2")),
        (vec![json!(503)], None)
    );
}

#[tokio::test]
async fn forwarded_create_after_a_remote_fk_check_waits_for_quorum() {
    let mut ctrl = three_voter_primary().await;

    let work = forward(
        &mut ctrl,
        JsonDbOp::Create,
        "posts",
        "p1",
        &json!({"id": "p1", "slug": "s1", "author_id": REMOTE_AUTHOR}),
    )
    .await;
    let work = after_the_remote_fk_check(&mut ctrl, work).await;
    assert!(stored(&ctrl, "posts", "p1").is_none());

    fail_the_majority(&mut ctrl, work).await;
    assert_eq!(
        (refused_codes(&ctrl), stored(&ctrl, "posts", "p1")),
        (vec![json!(503)], None)
    );
}

#[tokio::test]
async fn forwarded_update_after_a_remote_fk_check_waits_for_quorum() {
    let mut ctrl = three_voter_primary().await;

    let work = forward(
        &mut ctrl,
        JsonDbOp::Update,
        "posts",
        "p2",
        &json!({"slug": "s1", "author_id": REMOTE_AUTHOR}),
    )
    .await;
    let work = after_the_remote_fk_check(&mut ctrl, work).await;
    assert_eq!(stored(&ctrl, "posts", "p2").unwrap()["slug"], "s0");

    fail_the_majority(&mut ctrl, work).await;
    let post = stored(&ctrl, "posts", "p2").unwrap();
    assert_eq!(
        (refused_codes(&ctrl), &post["slug"], &post["author_id"]),
        (vec![json!(503)], &json!("s0"), &json!("u1"))
    );
}
