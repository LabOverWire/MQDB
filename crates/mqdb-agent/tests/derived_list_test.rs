// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_core::config::DatabaseConfig;
use mqdb_core::protocol::{build_request, parse_db_topic};
use mqdb_core::types::{OwnershipConfig, Pagination, SHARES_ENTITY, ScopeConfig, SortOrder};
use mqdb_core::{Filter, FilterOp, Request, Response};
use serde_json::{Value, json};
use std::collections::HashSet;
use tempfile::TempDir;

fn ownership() -> OwnershipConfig {
    OwnershipConfig::parse("collars=ownerId")
        .unwrap()
        .with_derivations("fixes=collarId>collars,logs=collarId>plain")
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

async fn open_capped(max_list_results: usize, max_sort_buffer: usize) -> (TempDir, Database) {
    let tmp = TempDir::new().unwrap();
    let mut config = DatabaseConfig::new(tmp.path())
        .without_background_tasks()
        .with_max_list_results(Some(max_list_results));
    config.max_sort_buffer = max_sort_buffer;
    let db = Database::open_with_config(config).await.unwrap();
    (tmp, db)
}

async fn seed(db: &Database, entity: &str, record: Value) {
    let created = db
        .execute(Request::Create {
            entity: entity.into(),
            data: record,
        })
        .await;
    assert!(matches!(created, Response::Ok { .. }), "{created:?}");
}

async fn collar(db: &Database, id: &str, owner: &str) {
    seed(db, "collars", json!({"id": id, "ownerId": owner})).await;
}

async fn fix(db: &Database, id: &str, parent: &str, lat: i64) {
    seed(
        db,
        "fixes",
        json!({"id": id, "collarId": parent, "lat": lat}),
    )
    .await;
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

async fn share(db: &Database, id: &str, grantor: &str, grantee: &str, permission: &str) {
    let answer = publish(
        db,
        &format!("$DB/collars/{id}/share"),
        &json!({"grantee": grantee, "permission": permission}),
        grantor,
    )
    .await;
    assert!(matches!(answer, Response::Ok { .. }), "{answer:?}");
}

async fn raw_grant(db: &Database, record: Value) {
    db.create(
        SHARES_ENTITY.to_string(),
        record,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
}

#[derive(Default)]
struct Query {
    filters: Vec<Filter>,
    sort: Vec<SortOrder>,
    pagination: Option<Pagination>,
    projection: Option<Vec<String>>,
}

async fn list_as(db: &Database, entity: &str, sender: Option<&str>, query: Query) -> Vec<Value> {
    let answer = db
        .execute_with_sender(
            Request::List {
                entity: entity.into(),
                filters: query.filters,
                sort: query.sort,
                pagination: query.pagination,
                includes: vec![],
                projection: query.projection,
            },
            sender,
            None,
            &ownership(),
            &ScopeConfig::default(),
            None,
        )
        .await;
    let Response::Ok { data } = answer else {
        panic!("list refused: {answer:?}");
    };
    data.as_array().unwrap().clone()
}

fn ids(rows: &[Value]) -> Vec<&str> {
    rows.iter().map(|r| r["id"].as_str().unwrap()).collect()
}

async fn list_ids(db: &Database, sender: &str, query: Query) -> Vec<String> {
    ids(&list_as(db, "fixes", Some(sender), query).await)
        .into_iter()
        .map(String::from)
        .collect()
}

fn eq(field: &str, value: Value) -> Filter {
    Filter::new(field.into(), FilterOp::Eq, value)
}

fn at_least(field: &str, value: i64) -> Filter {
    Filter::new(field.into(), FilterOp::Gte, json!(value))
}

async fn two_owners(db: &Database) {
    collar(db, "theirs", "bob").await;
    collar(db, "mine", "alice").await;
    fix(db, "a1", "theirs", 1).await;
    fix(db, "a2", "theirs", 2).await;
    fix(db, "a3", "theirs", 3).await;
    fix(db, "z1", "mine", 4).await;
    fix(db, "z2", "mine", 5).await;
    fix(db, "z3", "mine", 6).await;
}

#[tokio::test]
async fn list_returns_only_children_of_parents_the_caller_owns() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;

    assert_eq!(
        list_ids(&db, "alice", Query::default()).await,
        ["z1", "z2", "z3"]
    );
    assert_eq!(
        list_ids(&db, "bob", Query::default()).await,
        ["a1", "a2", "a3"]
    );
    assert!(list_ids(&db, "mallory", Query::default()).await.is_empty());

    let by_parent = Query {
        filters: vec![eq("collarId", json!("theirs"))],
        ..Query::default()
    };
    assert!(list_ids(&db, "mallory", by_parent).await.is_empty());
}

#[tokio::test]
async fn view_and_edit_shares_on_the_parent_admit_its_children_until_revoked() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;

    share(&db, "mine", "alice", "viewer", "view").await;
    share(&db, "theirs", "bob", "editor", "edit").await;
    assert_eq!(
        list_ids(&db, "viewer", Query::default()).await,
        ["z1", "z2", "z3"]
    );
    assert_eq!(
        list_ids(&db, "editor", Query::default()).await,
        ["a1", "a2", "a3"]
    );

    let revoked = publish(
        &db,
        "$DB/collars/mine/unshare",
        &json!({"grantee": "viewer"}),
        "alice",
    )
    .await;
    assert!(matches!(revoked, Response::Ok { .. }), "{revoked:?}");
    assert!(list_ids(&db, "viewer", Query::default()).await.is_empty());
}

#[tokio::test]
async fn a_cascade_share_admits_children_of_the_referenced_parent() {
    let (_tmp, db) = open().await;
    db.add_relationship("collars".into(), "ref".into(), "collars".into())
        .await;
    collar(&db, "leaf", "alice").await;
    seed(
        &db,
        "collars",
        json!({"id": "root-collar", "ownerId": "alice", "ref_id": "leaf"}),
    )
    .await;
    collar(&db, "elsewhere", "alice").await;
    fix(&db, "f-leaf", "leaf", 1).await;
    fix(&db, "f-root", "root-collar", 2).await;
    fix(&db, "f-elsewhere", "elsewhere", 3).await;

    let cascaded = publish(
        &db,
        "$DB/collars/root-collar/share",
        &json!({"grantee": "viewer", "permission": "view", "cascade": true}),
        "alice",
    )
    .await;
    assert!(matches!(cascaded, Response::Ok { .. }), "{cascaded:?}");

    assert_eq!(
        list_ids(&db, "viewer", Query::default()).await,
        ["f-leaf", "f-root"]
    );
}

#[tokio::test]
async fn admins_and_internal_callers_keep_every_row() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;
    seed(&db, "fixes", json!({"id": "orphan", "collarId": "gone"})).await;
    seed(&db, "fixes", json!({"id": "noref", "lat": 9})).await;

    let everything = ["a1", "a2", "a3", "noref", "orphan", "z1", "z2", "z3"];
    assert_eq!(list_ids(&db, "root", Query::default()).await, everything);
    assert_eq!(
        ids(&list_as(&db, "fixes", None, Query::default()).await),
        everything
    );
}

#[tokio::test]
async fn a_page_counts_only_rows_the_caller_may_see() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;

    let page = |limit, offset| Query {
        pagination: Some(Pagination::new(limit, offset)),
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", page(2, 0)).await, ["z1", "z2"]);
    assert_eq!(list_ids(&db, "alice", page(2, 1)).await, ["z2", "z3"]);
    assert!(list_ids(&db, "alice", page(2, 3)).await.is_empty());
    assert_eq!(list_ids(&db, "alice", page(1, 2)).await, ["z3"]);
    assert!(list_ids(&db, "mallory", page(2, 0)).await.is_empty());
}

#[tokio::test]
async fn sorted_and_filtered_pages_count_only_rows_the_caller_may_see() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;

    let sorted = Query {
        sort: vec![SortOrder::asc("lat".into())],
        pagination: Some(Pagination::new(2, 0)),
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", sorted).await, ["z1", "z2"]);

    let sorted_desc = Query {
        sort: vec![SortOrder::desc("lat".into())],
        pagination: Some(Pagination::new(2, 1)),
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "bob", sorted_desc).await, ["a2", "a1"]);

    let filtered = Query {
        filters: vec![at_least("lat", 2)],
        pagination: Some(Pagination::new(2, 0)),
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", filtered).await, ["z1", "z2"]);
}

#[tokio::test]
async fn indexed_filters_answer_only_rows_the_caller_may_see() {
    let (_tmp, db) = open().await;
    db.add_index("fixes".into(), vec!["collarId".into(), "lat".into()])
        .await
        .unwrap();
    two_owners(&db).await;

    let by_parent = |parent: &str| Query {
        filters: vec![eq("collarId", json!(parent))],
        ..Query::default()
    };
    assert_eq!(
        list_ids(&db, "alice", by_parent("mine")).await,
        ["z1", "z2", "z3"]
    );
    assert!(list_ids(&db, "alice", by_parent("theirs")).await.is_empty());

    let by_range = Query {
        filters: vec![at_least("lat", 1)],
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", by_range).await, ["z1", "z2", "z3"]);
}

#[tokio::test]
async fn a_projection_without_the_reference_field_is_still_filtered() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;

    let projected = || Query {
        projection: Some(vec!["lat".to_string()]),
        ..Query::default()
    };
    let rows = list_as(&db, "fixes", Some("alice"), projected()).await;
    assert_eq!(
        rows,
        [
            json!({"id": "z1", "lat": 4}),
            json!({"id": "z2", "lat": 5}),
            json!({"id": "z3", "lat": 6})
        ]
    );
    assert!(
        list_as(&db, "fixes", Some("mallory"), projected())
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn list_answers_exactly_the_rows_read_answers() {
    let (_tmp, db) = open().await;
    two_owners(&db).await;
    collar(&db, "shared-view", "bob").await;
    collar(&db, "shared-edit", "bob").await;
    collar(&db, "bad-grant", "bob").await;
    seed(&db, "collars", json!({"id": "ownerless", "name": "x"})).await;
    fix(&db, "s-view", "shared-view", 1).await;
    fix(&db, "s-edit", "shared-edit", 1).await;
    fix(&db, "s-bad", "bad-grant", 1).await;
    fix(&db, "s-ownerless", "ownerless", 1).await;
    fix(&db, "orphan", "gone", 1).await;
    seed(&db, "fixes", json!({"id": "noref", "lat": 1})).await;
    seed(&db, "fixes", json!({"id": "nullref", "collarId": null})).await;
    seed(&db, "fixes", json!({"id": "numref", "collarId": 7})).await;
    seed(&db, "fixes", json!({"id": "emptyref", "collarId": ""})).await;

    share(&db, "shared-view", "bob", "viewer", "view").await;
    share(&db, "shared-edit", "bob", "viewer", "edit").await;
    raw_grant(
        &db,
        json!({
            "resource_entity": "collars",
            "resource_id": "bad-grant",
            "grantee": "viewer",
            "grantee_key": "viewer",
            "permission": "root",
        }),
    )
    .await;
    raw_grant(
        &db,
        json!({
            "resource_entity": "fixes",
            "resource_id": "theirs",
            "grantee": "viewer",
            "grantee_key": "viewer",
            "permission": "edit",
        }),
    )
    .await;

    let everyone = ["alice", "bob", "viewer", "mallory", "root"];
    let rows = list_as(&db, "fixes", None, Query::default()).await;
    let all_ids = ids(&rows);
    assert_eq!(all_ids.len(), 15, "{all_ids:?}");

    for user in everyone {
        let listed = list_ids(&db, user, Query::default()).await;
        for id in &all_ids {
            let read = publish(&db, &format!("$DB/fixes/{id}"), &json!({}), user).await;
            let readable = matches!(read, Response::Ok { .. });
            assert_eq!(
                listed.iter().any(|row| row == id),
                readable,
                "{user} on {id}: listed {listed:?}, read {read:?}",
            );
        }
    }

    assert_eq!(
        list_ids(&db, "viewer", Query::default()).await,
        ["s-edit", "s-view"]
    );
}

#[tokio::test]
async fn a_parent_without_ownership_admits_children_that_name_one() {
    let (_tmp, db) = open().await;
    seed(&db, "logs", json!({"id": "named", "collarId": "gone"})).await;
    seed(&db, "logs", json!({"id": "unnamed", "line": "x"})).await;

    let rows = list_as(&db, "logs", Some("mallory"), Query::default()).await;
    assert_eq!(ids(&rows), ["named"]);
}

#[tokio::test]
async fn the_result_cap_counts_only_rows_the_caller_may_see() {
    let (_tmp, db) = open_capped(2, 10_000).await;
    two_owners(&db).await;

    assert_eq!(list_ids(&db, "alice", Query::default()).await, ["z1", "z2"]);

    let scanned = Query {
        filters: vec![at_least("lat", 1)],
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", scanned).await, ["z1", "z2"]);
}

#[tokio::test]
async fn the_index_lookup_cap_counts_only_rows_the_caller_may_see() {
    let (_tmp, db) = open_capped(2, 10_000).await;
    db.add_index("fixes".into(), vec!["lat".into()])
        .await
        .unwrap();
    two_owners(&db).await;

    let ranged = Query {
        filters: vec![at_least("lat", 1)],
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", ranged).await, ["z1", "z2"]);
}

#[tokio::test]
async fn the_sort_buffer_counts_only_rows_the_caller_may_see() {
    let (_tmp, db) = open_capped(10_000, 2).await;
    two_owners(&db).await;

    let sorted = Query {
        sort: vec![SortOrder::desc("lat".into())],
        ..Query::default()
    };
    assert_eq!(list_ids(&db, "alice", sorted).await, ["z2", "z1"]);
}

#[tokio::test]
async fn grants_beyond_the_result_cap_still_admit_children() {
    let (_tmp, db) = open_capped(2, 10_000).await;
    let parents = ["p1", "p2", "p3", "p4", "p5", "p6"];
    for parent in parents {
        collar(&db, parent, "bob").await;
        fix(&db, &format!("f-{parent}"), parent, 1).await;
        share(&db, parent, "bob", "viewer", "view").await;
    }

    for parent in parents {
        let by_parent = Query {
            filters: vec![eq("collarId", json!(parent))],
            ..Query::default()
        };
        assert_eq!(
            list_ids(&db, "viewer", by_parent).await,
            [format!("f-{parent}")]
        );
    }
}
