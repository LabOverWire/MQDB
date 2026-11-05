// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::protocol::{build_request, parse_db_topic};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_core::{Request, Response};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Barrier;

const REFUSAL: &str = "the ownership field 'userId' cannot be changed";

fn ownership() -> OwnershipConfig {
    OwnershipConfig::parse("docs=userId")
        .unwrap()
        .with_admin_users(HashSet::from(["root".to_string()]))
}

async fn open() -> (TempDir, Database) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();
    (tmp, db)
}

async fn seed(db: &Database, record: Value) {
    let created = db
        .execute(Request::Create {
            entity: "docs".into(),
            data: record,
        })
        .await;
    assert!(matches!(created, Response::Ok { .. }), "{created:?}");
}

async fn publish(db: &Database, topic: &str, payload: &Value, sender: &str) -> Response {
    let op = parse_db_topic(topic).unwrap();
    let request = build_request(op, &serde_json::to_vec(payload).unwrap()).unwrap();
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

async fn update(db: &Database, id: &str, payload: Value, sender: &str) -> Response {
    publish(db, &format!("$DB/docs/{id}/update"), &payload, sender).await
}

async fn grant(db: &Database, id: &str, grantor: &str, grantee: &str, permission: &str) {
    let answer = publish(
        db,
        &format!("$DB/docs/{id}/share"),
        &json!({"grantee": grantee, "permission": permission}),
        grantor,
    )
    .await;
    assert!(matches!(answer, Response::Ok { .. }), "{answer:?}");
}

async fn stored(db: &Database, id: &str) -> Value {
    db.read("docs".into(), id.into(), vec![], None)
        .await
        .unwrap()
}

fn assert_refused(answer: &Response) {
    match answer {
        Response::Error { code, message } => {
            assert_eq!(*code, 403);
            assert_eq!(message, REFUSAL);
        }
        Response::Ok { data } => panic!("update answered ok: {data}"),
    }
}

fn assert_code(answer: &Response, expected: u16, text: &str) {
    match answer {
        Response::Error { code, message } => {
            assert_eq!(*code, expected, "{message}");
            assert!(message.contains(text), "{message}");
        }
        Response::Ok { data } => panic!("update answered ok: {data}"),
    }
}

fn changing_payloads() -> [Value; 7] {
    [
        json!({"userId": "bob"}),
        json!({"userId": "bob", "title": "changed"}),
        json!({"userId": "carol", "title": "changed"}),
        json!({"userId": null}),
        json!({"userId": 7}),
        json!({"userId": ["alice"]}),
        json!({"userId": "ALICE"}),
    ]
}

#[tokio::test]
async fn owner_and_editor_are_refused_and_nothing_is_applied() {
    let (_tmp, db) = open().await;
    seed(
        &db,
        json!({"id": "d1", "userId": "alice", "title": "first"}),
    )
    .await;
    grant(&db, "d1", "alice", "carol", "edit").await;
    let before = stored(&db, "d1").await;
    let mut events = db.event_receiver();

    for sender in ["alice", "carol"] {
        for payload in changing_payloads() {
            let answer = update(&db, "d1", payload, sender).await;
            assert_refused(&answer);
        }
    }

    assert_eq!(stored(&db, "d1").await, before);
    assert!(
        events.try_recv().is_err(),
        "a refused update emitted an event"
    );
}

#[tokio::test]
async fn a_value_equal_to_the_current_owner_is_allowed() {
    let (_tmp, db) = open().await;
    seed(
        &db,
        json!({"id": "d1", "userId": "alice", "title": "first"}),
    )
    .await;
    grant(&db, "d1", "alice", "carol", "edit").await;

    let from_owner = update(
        &db,
        "d1",
        json!({"userId": "alice", "title": "from owner"}),
        "alice",
    )
    .await;
    assert!(matches!(from_owner, Response::Ok { .. }), "{from_owner:?}");
    let from_editor = update(
        &db,
        "d1",
        json!({"userId": "alice", "title": "from editor"}),
        "carol",
    )
    .await;
    assert!(
        matches!(from_editor, Response::Ok { .. }),
        "{from_editor:?}"
    );

    let record = stored(&db, "d1").await;
    assert_eq!(record["userId"], "alice");
    assert_eq!(record["title"], "from editor");
    assert_eq!(record["_version"], 3);
}

#[tokio::test]
async fn updates_without_the_field_still_work() {
    let (_tmp, db) = open().await;
    seed(
        &db,
        json!({"id": "d1", "userId": "alice", "title": "first"}),
    )
    .await;
    grant(&db, "d1", "alice", "carol", "edit").await;

    for sender in ["alice", "carol"] {
        let answer = update(&db, "d1", json!({"title": sender}), sender).await;
        assert!(matches!(answer, Response::Ok { .. }), "{answer:?}");
    }
    assert_eq!(stored(&db, "d1").await["_version"], 3);
}

#[tokio::test]
async fn others_get_the_same_answers_as_before_whatever_the_owner_value() {
    let (_tmp, db) = open().await;
    seed(
        &db,
        json!({"id": "d1", "userId": "alice", "title": "first"}),
    )
    .await;
    grant(&db, "d1", "alice", "dave", "view").await;
    let before = stored(&db, "d1").await;

    for owner in ["alice", "bob", "dave"] {
        for sender in ["bob", "dave"] {
            let answer = update(&db, "d1", json!({"userId": owner}), sender).await;
            assert_code(&answer, 403, "forbidden");
        }
    }
    for (sender, owner) in [("bob", "bob"), ("alice", "alice"), ("carol", "alice")] {
        let answer = update(&db, "missing", json!({"userId": owner}), sender).await;
        assert_code(&answer, 404, "not found");
    }
    assert_eq!(stored(&db, "d1").await, before);
}

#[tokio::test]
async fn an_admin_keeps_changing_the_owner() {
    let (_tmp, db) = open().await;
    seed(
        &db,
        json!({"id": "d1", "userId": "alice", "title": "first"}),
    )
    .await;

    let answer = update(&db, "d1", json!({"userId": "bob"}), "root").await;
    assert!(matches!(answer, Response::Ok { .. }), "{answer:?}");
    assert_eq!(stored(&db, "d1").await["userId"], "bob");

    let old_owner = update(&db, "d1", json!({"title": "alice again"}), "alice").await;
    assert_code(&old_owner, 403, "forbidden");
    let new_owner = update(&db, "d1", json!({"title": "bob now"}), "bob").await;
    assert!(matches!(new_owner, Response::Ok { .. }), "{new_owner:?}");
}

#[tokio::test]
async fn an_editor_cannot_take_a_record_with_no_owner() {
    let (_tmp, db) = open().await;
    seed(&db, json!({"id": "d1", "title": "first"})).await;
    grant(&db, "d1", "root", "carol", "edit").await;
    let before = stored(&db, "d1").await;

    for payload in [json!({"userId": "carol"}), json!({"userId": null})] {
        assert_refused(&update(&db, "d1", payload, "carol").await);
    }
    assert_code(
        &update(&db, "d1", json!({"userId": "bob"}), "bob").await,
        403,
        "forbidden",
    );
    assert_eq!(stored(&db, "d1").await, before);

    let by_admin = update(&db, "d1", json!({"userId": "alice"}), "root").await;
    assert!(matches!(by_admin, Response::Ok { .. }), "{by_admin:?}");
    assert_eq!(stored(&db, "d1").await["userId"], "alice");
}

#[tokio::test]
async fn a_stored_non_string_owner_compares_as_json() {
    let (_tmp, db) = open().await;
    seed(&db, json!({"id": "d1", "userId": 7, "title": "first"})).await;
    grant(&db, "d1", "root", "carol", "edit").await;

    let same = update(&db, "d1", json!({"userId": 7, "title": "same"}), "carol").await;
    assert!(matches!(same, Response::Ok { .. }), "{same:?}");
    assert_refused(&update(&db, "d1", json!({"userId": "7"}), "carol").await);
    assert_refused(&update(&db, "d1", json!({"userId": "carol"}), "carol").await);
    assert_eq!(stored(&db, "d1").await["userId"], 7);
}

#[tokio::test]
async fn the_refusal_comes_before_a_stale_expected_version() {
    let (_tmp, db) = open().await;
    seed(&db, json!({"id": "d1", "userId": "alice"})).await;
    let answer = update(
        &db,
        "d1",
        json!({"userId": "bob", "_expected_version": 9}),
        "alice",
    )
    .await;
    assert_refused(&answer);

    let equal_owner = update(
        &db,
        "d1",
        json!({"userId": "alice", "_expected_version": 9}),
        "alice",
    )
    .await;
    assert_code(&equal_owner, 412, "expected _version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_editor_racing_an_admin_never_restores_the_old_owner() {
    let (_tmp, db) = open().await;
    let db = Arc::new(db);
    for round in 0..300 {
        let id = format!("d{round}");
        seed(&db, json!({"id": id, "userId": "alice", "n": 0})).await;
        grant(&db, &id, "alice", "carol", "edit").await;

        let barrier = Arc::new(Barrier::new(2));
        let mut tasks = Vec::new();
        for (sender, payload) in [
            ("root", json!({"userId": "bob"})),
            ("carol", json!({"userId": "alice", "n": round})),
        ] {
            let (db, barrier, id) = (Arc::clone(&db), Arc::clone(&barrier), id.clone());
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                update(&db, &id, payload, sender).await
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(stored(&db, &id).await["userId"], "bob", "round {round}");
    }
}
