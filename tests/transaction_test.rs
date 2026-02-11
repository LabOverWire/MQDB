// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, ScopeConfig};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_update_race_condition_within_transaction() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    let user = json!({
        "name": "Alice",
        "value": "initial",
    });
    let created = db
        .create("users".into(), user, None, None, &ScopeConfig::default())
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let mut handles = vec![];
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let id_clone = id.clone();
        let handle = tokio::spawn(async move {
            db_clone
                .update(
                    "users".into(),
                    id_clone,
                    json!({"value": format!("update-{}", i)}),
                    None,
                    None,
                    &ScopeConfig::default(),
                )
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().ok();
    }

    let final_user = db.read("users".into(), id, vec![], None).await.unwrap();
    let value = final_user["value"].as_str().unwrap();
    assert!(
        value.starts_with("update-"),
        "final value should be from one of the updates: {value}",
    );
}

#[tokio::test]
async fn test_dirty_read_prevented() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    let user = json!({
        "name": "Bob",
        "status": "active",
    });
    let created = db
        .create("users".into(), user, None, None, &ScopeConfig::default())
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let db1 = Arc::clone(&db);
    let id1 = id.clone();

    let handle = tokio::spawn(async move {
        db1.update(
            "users".into(),
            id1,
            json!({"status": "suspended"}),
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;

    let user = db
        .read("users".into(), id.clone(), vec![], None)
        .await
        .unwrap();
    let status = user["status"].as_str().unwrap();

    handle.await.unwrap().unwrap();

    assert!(
        status == "active" || status == "suspended",
        "should see either old or new value, not intermediate"
    );
}

#[tokio::test]
async fn test_atomicity_all_or_nothing() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_index("users".into(), vec!["group".into()]).await;

    let user1 = json!({"name": "User1", "group": "A"});
    let user2 = json!({"name": "User2", "group": "A"});
    let user3 = json!({"name": "User3", "group": "A"});

    db.create("users".into(), user1, None, None, &ScopeConfig::default())
        .await
        .unwrap();
    db.create("users".into(), user2, None, None, &ScopeConfig::default())
        .await
        .unwrap();
    db.create("users".into(), user3, None, None, &ScopeConfig::default())
        .await
        .unwrap();

    let all_users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_users.len(), 3);

    let indexed_users = db
        .list(
            "users".into(),
            vec![mqdb::Filter::new(
                "group".into(),
                mqdb::FilterOp::Eq,
                json!("A"),
            )],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(indexed_users.len(), 3, "all users should be indexed");
}

#[tokio::test]
async fn test_concurrent_updates_to_different_entities() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    let user1 = db
        .create(
            "users".into(),
            json!({"name": "User1", "count": 0}),
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let user2 = db
        .create(
            "users".into(),
            json!({"name": "User2", "count": 0}),
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();

    let id1 = user1["id"].as_str().unwrap().to_string();
    let id2 = user2["id"].as_str().unwrap().to_string();

    let mut handles = vec![];

    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let id1_clone = id1.clone();
        let handle = tokio::spawn(async move {
            db_clone
                .update(
                    "users".into(),
                    id1_clone,
                    json!({"count": i}),
                    None,
                    None,
                    &ScopeConfig::default(),
                )
                .await
        });
        handles.push(handle);
    }

    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let id2_clone = id2.clone();
        let handle = tokio::spawn(async move {
            db_clone
                .update(
                    "users".into(),
                    id2_clone,
                    json!({"count": i + 100}),
                    None,
                    None,
                    &ScopeConfig::default(),
                )
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().ok();
    }

    let u1 = db.read("users".into(), id1, vec![], None).await.unwrap();
    let u2 = db.read("users".into(), id2, vec![], None).await.unwrap();

    assert!(u1["count"].as_i64().unwrap() >= 0 && u1["count"].as_i64().unwrap() < 10);
    assert!(u2["count"].as_i64().unwrap() >= 100 && u2["count"].as_i64().unwrap() < 110);
}

#[tokio::test]
async fn test_create_operations_are_atomic() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    db.add_index("users".into(), vec!["email".into()]).await;

    let mut handles = vec![];
    for i in 0..50 {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            db_clone
                .create(
                    "users".into(),
                    json!({
                        "name": format!("User{}", i),
                        "email": format!("user{}@example.com", i),
                    }),
                    None,
                    None,
                    &ScopeConfig::default(),
                )
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(users.len(), 50);

    for i in 0..50 {
        let filter = mqdb::Filter::new(
            "email".into(),
            mqdb::FilterOp::Eq,
            json!(format!("user{}@example.com", i)),
        );
        let result = db
            .list("users".into(), vec![filter], vec![], None, vec![], None)
            .await
            .unwrap();
        assert_eq!(result.len(), 1, "each email should be indexed exactly once");
    }
}

#[tokio::test]
async fn test_delete_operations_are_atomic() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    db.add_index("users".into(), vec!["status".into()]).await;

    let mut ids = vec![];
    for i in 0..20 {
        let user = db
            .create(
                "users".into(),
                json!({
                    "name": format!("User{}", i),
                    "status": "active",
                }),
                None,
                None,
                &ScopeConfig::default(),
            )
            .await
            .unwrap();
        ids.push(user["id"].as_str().unwrap().to_string());
    }

    let mut handles = vec![];
    for id in &ids[0..10] {
        let db_clone = Arc::clone(&db);
        let id_clone = id.clone();
        let handle = tokio::spawn(async move {
            db_clone
                .delete(
                    "users".into(),
                    id_clone,
                    None,
                    None,
                    &ScopeConfig::default(),
                )
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(users.len(), 10);

    let filter = mqdb::Filter::new("status".into(), mqdb::FilterOp::Eq, json!("active"));
    let active_users = db
        .list("users".into(), vec![filter], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(active_users.len(), 10, "index should match actual data");
}
