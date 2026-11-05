// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::protocol::{build_request, parse_db_topic};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_core::{Filter, FilterOp, Request, Response};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Barrier;

const EXISTS: &str = "entity already exists";

fn ownership() -> OwnershipConfig {
    OwnershipConfig::parse("docs=userId")
        .unwrap()
        .with_admin_users(HashSet::from(["root".to_string()]))
}

async fn fresh() -> (TempDir, Database) {
    let tmp = TempDir::new().unwrap();
    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();
    (tmp, db)
}

async fn as_user(db: &Database, topic: &str, payload: &Value, sender: &str) -> Response {
    let op = parse_db_topic(topic).unwrap();
    let request = build_request(op, payload.to_string().as_bytes()).unwrap();
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

async fn create(db: &Database, entity: &str, data: Value) -> Response {
    db.execute(Request::Create {
        entity: entity.to_string(),
        data,
    })
    .await
}

fn assert_refused_as_existing(response: &Response) {
    match response {
        Response::Error { code, message } => {
            assert_eq!(*code, 409, "{message}");
            assert_eq!(message, EXISTS);
        }
        Response::Ok { data } => panic!("create on an existing id returned ok: {data}"),
    }
}

async fn stored(db: &Database, entity: &str, id: &str) -> Value {
    db.read(entity.to_string(), id.to_string(), vec![], None)
        .await
        .unwrap()
}

async fn tagged(db: &Database, tag: &str) -> Vec<Value> {
    let filter = Filter::new("tag".to_string(), FilterOp::Eq, json!(tag));
    db.list("hold".into(), vec![filter], vec![], None, vec![], None)
        .await
        .unwrap()
}

#[tokio::test]
async fn another_user_cannot_take_over_a_record() {
    let (_tmp, db) = fresh().await;
    let original = json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"});
    assert!(
        as_user(&db, "$DB/docs/create", &original, "bob")
            .await
            .is_ok()
    );

    let takeover = as_user(
        &db,
        "$DB/docs/create",
        &json!({"id": "bob-d", "userId": "mallory", "title": "mine"}),
        "mallory",
    )
    .await;
    let bob_read = as_user(&db, "$DB/docs/bob-d", &json!({}), "bob").await;
    let share = as_user(
        &db,
        "$DB/docs/bob-d/share",
        &json!({"grantee": "carol", "permission": "edit"}),
        "mallory",
    )
    .await;

    assert_refused_as_existing(&takeover);
    let Response::Ok { data } = bob_read else {
        panic!("the owner lost read access after the refused create: {bob_read:?}");
    };
    assert_eq!(data["userId"], "bob");
    assert_eq!(data["title"], "bob-secret");
    assert!(share.is_error(), "{share:?}");
}

#[tokio::test]
async fn an_admin_create_on_an_existing_id_is_refused_too() {
    let (_tmp, db) = fresh().await;
    let original = json!({"id": "bob-d", "userId": "bob", "title": "bob-secret"});
    assert!(
        as_user(&db, "$DB/docs/create", &original, "bob")
            .await
            .is_ok()
    );

    let over = as_user(
        &db,
        "$DB/docs/create",
        &json!({"id": "bob-d", "userId": "root", "title": "admin"}),
        "root",
    )
    .await;

    assert_refused_as_existing(&over);
    assert_eq!(stored(&db, "docs", "bob-d").await["userId"], "bob");
}

#[tokio::test]
async fn a_replay_keeps_the_version_and_later_updates() {
    let (_tmp, db) = fresh().await;
    let record = json!({"id": "a", "n": 1});
    assert!(create(&db, "items", record.clone()).await.is_ok());
    let updated = db
        .execute(Request::Update {
            entity: "items".to_string(),
            id: "a".to_string(),
            fields: json!({"n": 2}),
            expected_version: None,
        })
        .await;
    assert!(updated.is_ok(), "{updated:?}");

    assert_refused_as_existing(&create(&db, "items", record).await);

    let after = stored(&db, "items", "a").await;
    assert_eq!(after["n"], 2);
    assert_eq!(after["_version"], 2);
}

#[tokio::test]
async fn a_refused_create_emits_no_change_event() {
    let (_tmp, db) = fresh().await;
    let mut receiver = db.event_receiver();
    db.subscribe("items/#".into(), Some("items".into()), None)
        .await
        .unwrap();

    assert!(
        create(&db, "items", json!({"id": "a", "n": 1}))
            .await
            .is_ok()
    );
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("the first create must emit an event")
        .unwrap();

    assert_refused_as_existing(&create(&db, "items", json!({"id": "a", "n": 2})).await);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), receiver.recv())
            .await
            .is_err(),
        "a refused create must not emit an event"
    );
}

#[tokio::test]
async fn a_replay_on_a_unique_entity_says_the_record_exists() {
    let (_tmp, db) = fresh().await;
    db.add_unique_constraint("hold".to_string(), vec!["seat".to_string()])
        .await
        .unwrap();
    let record = json!({"id": "h1", "seat": "S1"});
    assert!(create(&db, "hold", record.clone()).await.is_ok());

    assert_refused_as_existing(&create(&db, "hold", record).await);

    let other_id = create(&db, "hold", json!({"id": "h2", "seat": "S1"})).await;
    let Response::Error { code, message } = other_id else {
        panic!("another id took a held value: {other_id:?}");
    };
    assert_eq!(code, 409);
    assert!(message.contains("unique constraint violation"), "{message}");
}

#[tokio::test]
async fn a_refused_create_leaves_guards_and_indexes_alone() {
    let (_tmp, db) = fresh().await;
    db.add_unique_constraint("hold".to_string(), vec!["seat".to_string()])
        .await
        .unwrap();
    db.add_index("hold".to_string(), vec!["tag".to_string()])
        .await
        .unwrap();
    let first = json!({"id": "h1", "seat": "S1", "tag": "x"});
    assert!(create(&db, "hold", first).await.is_ok());

    assert_refused_as_existing(
        &create(&db, "hold", json!({"id": "h1", "seat": "S2", "tag": "y"})).await,
    );

    let free_value = create(&db, "hold", json!({"id": "h2", "seat": "S2", "tag": "z"})).await;
    assert!(
        free_value.is_ok(),
        "the refused value is held: {free_value:?}"
    );
    let held_value = create(&db, "hold", json!({"id": "h3", "seat": "S1", "tag": "z"})).await;
    assert!(held_value.is_error(), "the original value was freed");
    assert!(tagged(&db, "y").await.is_empty());
    assert_eq!(tagged(&db, "x").await.len(), 1);
}

#[tokio::test]
async fn a_deleted_id_can_be_created_again() {
    let (_tmp, db) = fresh().await;
    assert!(
        create(&db, "items", json!({"id": "a", "n": 1}))
            .await
            .is_ok()
    );
    let deleted = db
        .execute(Request::Delete {
            entity: "items".to_string(),
            id: "a".to_string(),
            expected_version: None,
        })
        .await;
    assert!(deleted.is_ok(), "{deleted:?}");

    let again = create(&db, "items", json!({"id": "a", "n": 2})).await;
    assert!(again.is_ok(), "{again:?}");
    let after = stored(&db, "items", "a").await;
    assert_eq!(after["n"], 2);
    assert_eq!(after["_version"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_creates_of_one_id_have_exactly_one_winner() {
    const ROUNDS: usize = 200;
    const WRITERS: usize = 8;

    let (_tmp, db) = fresh().await;
    let db = Arc::new(db);

    for round in 0..ROUNDS {
        let id = format!("r{round}");
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let response = create(&db, "items", json!({"id": id, "writer": writer})).await;
                (writer, response)
            }));
        }

        let mut winners = Vec::with_capacity(WRITERS);
        let mut refusals = Vec::with_capacity(WRITERS);
        for handle in handles {
            let (writer, response) = handle.await.unwrap();
            if response.is_ok() {
                winners.push(writer);
            } else {
                refusals.push(response);
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
        refusals.iter().for_each(assert_refused_as_existing);
        assert_eq!(
            stored(&db, "items", &id).await["writer"],
            json!(winners[0]),
            "round {round}: the record is not the winner's"
        );
    }
}

#[tokio::test]
async fn a_restore_still_replaces_an_existing_record() {
    let (tmp, db) = fresh().await;
    assert!(
        create(&db, "items", json!({"id": "a", "n": 1}))
            .await
            .is_ok()
    );
    let backup = tmp.path().join("backup.jsonl");
    let line = json!({"_entity": "items", "_data": {"id": "a", "n": 2}});
    std::fs::write(&backup, line.to_string()).unwrap();

    assert_eq!(db.restore_logical(&backup).await.unwrap(), 1);

    assert_eq!(stored(&db, "items", "a").await["n"], 2);
}
