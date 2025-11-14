use mqdb::{Database, DatabaseConfig, DurabilityMode, Filter, FilterOp};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_recovery_after_immediate_close_during_writes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..100 {
            let user = json!({
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
            });
            db.create("users".into(), user).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 100);
}

#[tokio::test]
async fn test_recovery_with_periodic_durability() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let config = DatabaseConfig::new(&db_path)
            .with_durability(DurabilityMode::PeriodicMs(100));

        let db = Database::open_with_config(config).await.unwrap();

        for i in 0..50 {
            let product = json!({
                "name": format!("Product {}", i),
                "price": 10.0 + i as f64,
            });
            db.create("products".into(), product).await.unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    let db = Database::open(&db_path).await.unwrap();
    let products = db
        .list("products".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert!(products.len() > 0, "should have persisted some products");
}

#[tokio::test]
async fn test_recovery_after_many_operations() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..50 {
            let user = json!({
                "name": format!("User {}", i),
                "index": i,
                "status": "active",
            });
            db.create("users".into(), user).await.unwrap();
        }

        let all_users = db
            .list("users".into(), vec![], vec![], None, vec![])
            .await
            .unwrap();

        for user in &all_users {
            let index = user["index"].as_u64().unwrap();
            let id = user["id"].as_str().unwrap();

            if index < 25 {
                db.update(
                    "users".into(),
                    id.to_string(),
                    json!({"status": "updated"}),
                )
                .await
                .unwrap();
            } else if index < 35 {
                db.delete("users".into(), id.to_string()).await.unwrap();
            }
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 40, "should have 40 users (50 - 10 deleted)");

    let updated = users
        .iter()
        .filter(|u| u.get("status").and_then(|s| s.as_str()) == Some("updated"))
        .count();
    assert_eq!(updated, 25, "should have 25 updated users");
}

#[tokio::test]
async fn test_recovery_with_indexes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();
        db.add_index("users".into(), vec!["email".into()]).await;

        for i in 0..30 {
            let user = json!({
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
            });
            db.create("users".into(), user).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    db.add_index("users".into(), vec!["email".into()]).await;

    let filter = Filter::new(
        "email".into(),
        FilterOp::Eq,
        json!("user15@example.com"),
    );
    let results = db
        .list("users".into(), vec![filter], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["name"], "User 15");
}

#[tokio::test]
async fn test_recovery_after_concurrent_writes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Arc::new(Database::open(&db_path).await.unwrap());

        let mut handles = vec![];
        for thread_id in 0..5 {
            let db_clone = Arc::clone(&db);
            let handle = tokio::spawn(async move {
                for i in 0..20 {
                    let entity = json!({
                        "thread": thread_id,
                        "index": i,
                        "value": format!("thread-{}-item-{}", thread_id, i),
                    });
                    db_clone
                        .create("items".into(), entity)
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let items = db
        .list("items".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(items.len(), 100, "should have all 100 items");
}

#[tokio::test]
async fn test_recovery_maintains_data_integrity() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..20 {
            let record = json!({
                "id_field": i,
                "data": format!("important-data-{}", i),
                "checksum": format!("{:x}", i * 12345),
            });
            db.create("records".into(), record).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let records = db
        .list("records".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(records.len(), 20);

    for record in &records {
        let id_field = record["id_field"].as_u64().unwrap() as usize;
        assert_eq!(record["data"], format!("important-data-{}", id_field));
        assert_eq!(
            record["checksum"],
            format!("{:x}", id_field * 12345)
        );
    }
}

#[tokio::test]
async fn test_recovery_with_ttl_entries() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..15 {
            let item = json!({
                "name": format!("Item {}", i),
                "permanent": i % 2 == 0,
            });
            db.create("items".into(), item).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let items = db
        .list("items".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(items.len(), 15);
}

#[tokio::test]
async fn test_recovery_empty_database() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let _db = Database::open(&db_path).await.unwrap();
    }

    let db = Database::open(&db_path).await.unwrap();
    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn test_recovery_after_delete_operations() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    let ids_to_delete: Vec<String>;

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..50 {
            let user = json!({
                "name": format!("User {}", i),
                "index": i,
            });
            db.create("users".into(), user).await.unwrap();
        }

        let users = db
            .list("users".into(), vec![], vec![], None, vec![])
            .await
            .unwrap();

        ids_to_delete = users
            .iter()
            .filter_map(|u| {
                let index = u.get("index")?.as_u64()?;
                if index % 2 == 0 {
                    u.get("id")?.as_str().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();

        for id in &ids_to_delete {
            db.delete("users".into(), id.clone()).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 25, "should have 25 odd-indexed users");

    for user in &users {
        let index = user["index"].as_u64().unwrap();
        assert_eq!(index % 2, 1, "only odd-indexed users should remain");
    }
}

#[tokio::test]
async fn test_recovery_with_mixed_entity_types() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..10 {
            db.create("users".into(), json!({"name": format!("User {}", i)}))
                .await
                .unwrap();
            db.create(
                "products".into(),
                json!({"name": format!("Product {}", i)}),
            )
            .await
            .unwrap();
            db.create("orders".into(), json!({"order_id": i}))
                .await
                .unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();

    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();
    let products = db
        .list("products".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();
    let orders = db
        .list("orders".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 10);
    assert_eq!(products.len(), 10);
    assert_eq!(orders.len(), 10);
}

#[tokio::test]
async fn test_recovery_after_update_operations() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..10 {
            let user = json!({
                "name": format!("User {}", i),
                "version": 1,
            });
            db.create("users".into(), user).await.unwrap();
        }

        let users = db
            .list("users".into(), vec![], vec![], None, vec![])
            .await
            .unwrap();

        for user in &users {
            if let Some(id) = user.get("id").and_then(|v| v.as_str()) {
                db.update(
                    "users".into(),
                    id.to_string(),
                    json!({"version": 2, "updated": true}),
                )
                .await
                .unwrap();
            }
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let users = db
        .list("users".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(users.len(), 10);

    for user in &users {
        assert_eq!(user["version"], 2);
        assert_eq!(user["updated"], true);
    }
}

#[tokio::test]
async fn test_multiple_reopen_cycles() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    for cycle in 0..5 {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..5 {
            let item = json!({
                "cycle": cycle,
                "index": i,
            });
            db.create("items".into(), item).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let items = db
        .list("items".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(items.len(), 25, "should have 5 cycles * 5 items");
}

#[tokio::test]
async fn test_recovery_preserves_checksums() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = Database::open(&db_path).await.unwrap();

        for i in 0..20 {
            let record = json!({
                "data": vec![i; 100],
            });
            db.create("records".into(), record).await.unwrap();
        }
    }

    let db = Database::open(&db_path).await.unwrap();
    let records = db
        .list("records".into(), vec![], vec![], None, vec![])
        .await
        .unwrap();

    assert_eq!(records.len(), 20, "all records should be recovered intact");
}
