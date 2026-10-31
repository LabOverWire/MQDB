// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::store_failure::{controller_where, publish_completing_unique, response_ctx};
use super::*;
use crate::cluster::db::{ClusterConstraint, OnDeleteAction, data_partition, unique_partition};
use crate::cluster::db_handler::{DbPublishResult, DbRequestHandler, FkCheckCompletion};
use serde_json::{Value, json};

const REMOTE_AUTHOR: &str = "u9";

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn primary_except_remote_author() -> NodeController<MockTransport> {
    let remote = data_partition("users", REMOTE_AUTHOR);
    let mut ctrl = controller_where(
        None,
        |partition| {
            if partition == remote {
                node(2)
            } else {
                node(1)
            }
        },
        &[],
    );
    ctrl.seed_unique_voters(std::iter::once(node(1)).collect());
    ctrl
}

async fn publish_local(ctrl: &mut NodeController<MockTransport>, topic: &str, payload: &Value) {
    let reply = publish_completing_unique(ctrl, topic, payload).await;
    assert_eq!(reply["status"], "ok", "{reply}");
}

async fn publish_after_remote_fk_ok(
    ctrl: &mut NodeController<MockTransport>,
    topic: &str,
    payload: &Value,
) -> Value {
    let handler = DbRequestHandler::new(node(1));
    let bytes = serde_json::to_vec(payload).unwrap();
    let DbPublishResult::PendingFkCheck(pending) = handler
        .handle_publish(ctrl, topic, &bytes, &response_ctx(None))
        .await
    else {
        panic!("{topic} should wait for the remote FK check");
    };
    let completion = handler
        .complete_pending_fk_check(ctrl, true, None, pending.continuation)
        .await;
    let FkCheckCompletion::Done(Some(response)) = completion else {
        panic!("a local unique conflict should be answered without waiting");
    };
    serde_json::from_slice(&response.payload).unwrap()
}

async fn posts_with_unique_slug_and_author_fk() -> NodeController<MockTransport> {
    let mut ctrl = primary_except_remote_author();
    let slug = serde_json::to_vec(&json!("taken")).unwrap();
    let remote = data_partition("users", REMOTE_AUTHOR);
    assert_ne!(unique_partition("posts", "slug", &slug), remote);
    assert_ne!(data_partition("posts", "p1"), remote);
    assert_ne!(data_partition("posts", "p2"), remote);
    ctrl.constraint_add(&ClusterConstraint::unique("posts", "uniq_slug", "slug").unwrap())
        .await
        .unwrap();
    publish_local(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p1", "slug": "taken"}),
    )
    .await;
    publish_local(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p2", "slug": "free"}),
    )
    .await;
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
    ctrl
}

#[tokio::test]
async fn a_create_with_a_remote_fk_still_enforces_unique() {
    let mut ctrl = posts_with_unique_slug_and_author_fk().await;

    let reply = publish_after_remote_fk_ok(
        &mut ctrl,
        "$DB/posts/create",
        &json!({"id": "p3", "slug": "taken", "author_id": REMOTE_AUTHOR}),
    )
    .await;

    assert_eq!(
        (&reply["code"], ctrl.db_get("posts", "p3").is_some()),
        (&json!(409), false),
        "{reply}"
    );
}

#[tokio::test]
async fn an_update_with_a_remote_fk_still_enforces_unique() {
    let mut ctrl = posts_with_unique_slug_and_author_fk().await;

    let reply = publish_after_remote_fk_ok(
        &mut ctrl,
        "$DB/posts/p2/update",
        &json!({"slug": "taken", "author_id": REMOTE_AUTHOR}),
    )
    .await;

    let stored: Value = serde_json::from_slice(&ctrl.db_get("posts", "p2").unwrap().data).unwrap();
    assert_eq!(
        (&reply["code"], &stored["slug"]),
        (&json!(409), &json!("free")),
        "{reply}"
    );
}
