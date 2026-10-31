// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_agent::vault_backend::DbAccess;
use mqdb_core::protocol::{build_request, parse_db_topic};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_core::{Request, Response};
use serde_json::{Value, json};
use std::collections::HashSet;
use tempfile::TempDir;

fn ownership() -> OwnershipConfig {
    OwnershipConfig::parse("diagrams=userId")
        .unwrap()
        .with_admin_users(HashSet::from(["root".to_string()]))
}

async fn diagrams(records: &[Value]) -> (TempDir, Database) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();
    db.add_relationship("diagrams".into(), "parent".into(), "diagrams".into())
        .await;
    for record in records {
        db.execute(Request::Create {
            entity: "diagrams".into(),
            data: record.clone(),
        })
        .await;
    }
    (tmp, db)
}

async fn publish(db: &Database, topic: &str, payload: &[u8], sender: &str) -> Response {
    let op = parse_db_topic(topic).unwrap();
    let request = build_request(op, payload).unwrap();
    db.execute_with_sender(
        request,
        Some(sender),
        None,
        &ownership(),
        &ScopeConfig::default(),
        None,
    )
    .await
}

fn answer(response: &Response) -> Value {
    match response {
        Response::Ok { .. } => json!("ok"),
        Response::Error { code, .. } => json!(code),
    }
}

fn data(response: Response) -> Value {
    match response {
        Response::Ok { data } => data,
        Response::Error { code, message } => panic!("{code}: {message}"),
    }
}

async fn carol_access(db: &Database, id: &str) -> (Value, Value) {
    let read = publish(db, &format!("$DB/diagrams/{id}"), b"{}", "carol").await;
    let update = publish(
        db,
        &format!("$DB/diagrams/{id}/update"),
        br#"{"title":"carol-was-here"}"#,
        "carol",
    )
    .await;
    (answer(&read), answer(&update))
}

const CAROL_EDIT_CASCADE: &[u8] = br#"{"grantee":"carol","permission":"edit","cascade":true}"#;

#[tokio::test]
async fn cascade_share_does_not_grant_on_a_referenced_record_owned_by_someone_else() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"}),
        json!({"id": "alice-d", "userId": "alice", "title": "a", "parent_id": "bob-d"}),
    ])
    .await;

    let shared = data(
        publish(
            &db,
            "$DB/diagrams/alice-d/share",
            CAROL_EDIT_CASCADE,
            "alice",
        )
        .await,
    );

    assert_eq!(
        carol_access(&db, "alice-d").await,
        (json!("ok"), json!("ok"))
    );
    assert_eq!(carol_access(&db, "bob-d").await, (json!(403), json!(403)));
    assert_eq!(
        (&shared["resources_shared"], &shared["resources_skipped"]),
        (&json!(1), &json!(1))
    );
}

#[tokio::test]
async fn cascade_share_grants_the_sharers_own_closure_and_stops_at_a_foreign_record() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "alice-far", "userId": "alice", "title": "far"}),
        json!({"id": "bob-mid", "userId": "bob", "title": "mid", "parent_id": "alice-far"}),
        json!({"id": "alice-near", "userId": "alice", "title": "near", "parent_id": "bob-mid"}),
        json!({"id": "alice-root", "userId": "alice", "title": "root", "parent_id": "alice-near"}),
    ])
    .await;

    let shared = data(
        publish(
            &db,
            "$DB/diagrams/alice-root/share",
            CAROL_EDIT_CASCADE,
            "alice",
        )
        .await,
    );

    let mut access = Vec::new();
    for id in ["alice-root", "alice-near", "bob-mid", "alice-far"] {
        access.push((id, carol_access(&db, id).await));
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
    assert_eq!(
        (&shared["resources_shared"], &shared["resources_skipped"]),
        (&json!(2), &json!(1))
    );
}

#[tokio::test]
async fn admin_cascade_share_grants_every_owner_in_the_closure() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"}),
        json!({"id": "alice-d", "userId": "alice", "title": "a", "parent_id": "bob-d"}),
    ])
    .await;

    let shared = data(
        publish(
            &db,
            "$DB/diagrams/alice-d/share",
            CAROL_EDIT_CASCADE,
            "root",
        )
        .await,
    );

    assert_eq!(carol_access(&db, "bob-d").await, (json!("ok"), json!("ok")));
    assert_eq!(
        (&shared["resources_shared"], &shared["resources_skipped"]),
        (&json!(2), &json!(0))
    );
}

#[tokio::test]
async fn cascade_share_does_not_grant_on_a_dangling_reference_created_later() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "alice-d", "userId": "alice", "title": "a", "parent_id": "later-d"}),
    ])
    .await;

    let shared = data(
        publish(
            &db,
            "$DB/diagrams/alice-d/share",
            CAROL_EDIT_CASCADE,
            "alice",
        )
        .await,
    );
    db.execute(Request::Create {
        entity: "diagrams".into(),
        data: json!({"id": "later-d", "userId": "bob", "title": "bob-secret"}),
    })
    .await;

    assert_eq!(carol_access(&db, "later-d").await, (json!(403), json!(403)));
    assert_eq!(
        (&shared["resources_shared"], &shared["resources_skipped"]),
        (&json!(1), &json!(1))
    );
}

#[tokio::test]
async fn cascade_unshare_keeps_a_grant_on_a_referenced_record_owned_by_someone_else() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"}),
        json!({"id": "alice-d", "userId": "alice", "title": "a", "parent_id": "bob-d"}),
    ])
    .await;
    data(
        publish(
            &db,
            "$DB/diagrams/bob-d/share",
            br#"{"grantee":"carol","permission":"edit","cascade":false}"#,
            "bob",
        )
        .await,
    );
    data(
        publish(
            &db,
            "$DB/diagrams/alice-d/share",
            br#"{"grantee":"carol","permission":"edit","cascade":false}"#,
            "alice",
        )
        .await,
    );

    data(
        publish(
            &db,
            "$DB/diagrams/alice-d/unshare",
            br#"{"grantee":"carol","cascade":true}"#,
            "alice",
        )
        .await,
    );

    assert_eq!(carol_access(&db, "alice-d").await, (json!(403), json!(403)));
    assert_eq!(carol_access(&db, "bob-d").await, (json!("ok"), json!("ok")));
}

#[tokio::test]
async fn pending_cascade_share_does_not_reach_a_foreign_record_after_the_sweep() {
    let (_tmp, db) = diagrams(&[
        json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"}),
        json!({"id": "alice-d", "userId": "alice", "title": "a", "parent_id": "bob-d"}),
    ])
    .await;
    let shared = db
        .execute_with_sender(
            Request::Share {
                entity: "diagrams".into(),
                id: "alice-d".into(),
                grantee: String::new(),
                grantee_key: Some("carol@example.com".into()),
                grantee_email: None,
                permission: "edit".into(),
                cascade: true,
            },
            Some("alice"),
            None,
            &ownership(),
            &ScopeConfig::default(),
            None,
        )
        .await;
    assert_eq!(data(shared)["status"], "pending");
    let filled = DbAccess::resolve_pending_grants(&db, "carol@example.com", "carol")
        .await
        .unwrap();

    assert_eq!(
        carol_access(&db, "alice-d").await,
        (json!("ok"), json!("ok"))
    );
    assert_eq!(carol_access(&db, "bob-d").await, (json!(403), json!(403)));
    assert_eq!(filled, 1);
}

#[tokio::test]
async fn cascade_share_stops_at_the_same_bound_as_cluster_mode() {
    let chain: Vec<Value> = (0..300)
        .map(|i| json!({"id": format!("c{i}"), "userId": "alice", "title": "c", "parent_id": format!("c{}", i + 1)}))
        .collect();
    let (_tmp, db) = diagrams(&chain).await;

    let shared = data(publish(&db, "$DB/diagrams/c0/share", CAROL_EDIT_CASCADE, "alice").await);

    assert_eq!(
        (&shared["resources_shared"], &shared["resources_skipped"]),
        (&json!(256), &json!(0))
    );
}
