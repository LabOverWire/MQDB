use mqdb::{Database, DatabaseConfig, Filter, FilterOp, ScopeConfig};
use serde_json::json;
use std::collections::HashSet;
use tempfile::TempDir;

#[tokio::test]
async fn test_concurrent_id_generation_no_duplicates() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let mut handles = vec![];

    for i in 0..100 {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            let user = json!({
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
            });
            db_clone
                .create("users".into(), user, None, &ScopeConfig::default())
                .await
        });
        handles.push(handle);
    }

    let mut ids = Vec::new();
    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        let id = result["id"].as_str().unwrap().to_string();
        ids.push(id);
    }

    let unique_ids: HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), 100, "Should create 100 entities");
    assert_eq!(
        unique_ids.len(),
        100,
        "All IDs should be unique - no duplicates"
    );

    for id in &ids {
        let entity = db
            .read("users".into(), id.clone(), vec![], None)
            .await
            .unwrap();
        assert!(entity.get("name").is_some(), "Entity should exist in DB");
    }
}

#[tokio::test]
async fn test_concurrent_delete_and_read() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_index("users".into(), vec!["status".into()]).await;

    for i in 0..50 {
        let user = json!({
            "name": format!("User {}", i),
            "status": "active"
        });
        db.create("users".into(), user, None, &ScopeConfig::default())
            .await
            .unwrap();
    }

    let mut handles = vec![];

    for i in 1..=25 {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            let _ = db_clone
                .delete("users".into(), i.to_string(), None, &ScopeConfig::default())
                .await;
        });
        handles.push(handle);
    }

    for _ in 0..5 {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
                let _ = db_clone
                    .list("users".into(), vec![filter], vec![], None, vec![], None)
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let all_users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_users.len(), 25, "Should have 25 entities remaining");

    let filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
    let indexed_results = db
        .list("users".into(), vec![filter], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        indexed_results.len(),
        25,
        "Index should only contain remaining 25 entities, no stale entries"
    );

    for i in 1..=25 {
        let result = db.read("users".into(), i.to_string(), vec![], None).await;
        assert!(result.is_err(), "Deleted entity {i} should not exist");
    }

    for i in 26..=50 {
        let result = db.read("users".into(), i.to_string(), vec![], None).await;
        assert!(result.is_ok(), "Non-deleted entity {i} should exist");
    }
}

#[tokio::test]
async fn test_subscription_limit_enforcement_concurrent() {
    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path()).with_max_subscriptions(Some(10));
    let db = Database::open_with_config(config).await.unwrap();

    let mut handles = vec![];

    for i in 0..50 {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            db_clone
                .subscribe(format!("users/{i}"), Some("users".into()))
                .await
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    let mut error_count = 0;

    for handle in handles {
        match handle.await.unwrap() {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    assert_eq!(success_count, 10, "Exactly 10 subscriptions should succeed");
    assert_eq!(
        error_count, 40,
        "40 subscriptions should fail with limit error"
    );
}

#[tokio::test]
async fn test_max_list_results_limit_enforcement() {
    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path()).with_max_list_results(Some(10));
    let db = Database::open_with_config(config).await.unwrap();

    for i in 0..50 {
        let user = json!({
            "name": format!("User {i}"),
            "email": format!("user{i}@example.com"),
        });
        db.create("users".into(), user, None, &ScopeConfig::default())
            .await
            .unwrap();
    }

    let all_users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        all_users.len(),
        10,
        "List should be limited to max_list_results (10), not return all 50"
    );
}
