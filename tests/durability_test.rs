use mqdb::{Database, DatabaseConfig, DurabilityMode, Filter, FilterOp};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_durability_immediate_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let id: String;
    {
        let config = DatabaseConfig::new(path.clone())
            .with_durability(DurabilityMode::Immediate)
            .without_background_tasks();
        let db = Database::open_with_config(config).await.unwrap();

        let user = json!({
            "name": "Alice",
            "email": "alice@example.com",
            "status": "active"
        });

        let created = db.create("users".into(), user).await.unwrap();
        id = created["id"].as_str().unwrap().to_string();
    }

    {
        let db = Database::open_without_background_tasks(&path).await.unwrap();

        let retrieved = db
            .read("users".into(), id.clone(), vec![], None)
            .await
            .unwrap();
        assert_eq!(retrieved["name"], "Alice");
        assert_eq!(retrieved["email"], "alice@example.com");
        assert_eq!(retrieved["status"], "active");
        assert_eq!(retrieved["id"].as_str().unwrap(), id);
    }
}

#[tokio::test]
async fn test_index_consistency_after_crash_during_delete() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let id: String;
    {
        let config = DatabaseConfig::new(path.clone())
            .with_durability(DurabilityMode::Immediate)
            .without_background_tasks();
        let db = Database::open_with_config(config).await.unwrap();

        db.add_index("users".into(), vec!["email".into(), "status".into()])
            .await;

        let user = json!({
            "name": "Charlie",
            "email": "charlie@example.com",
            "status": "active"
        });

        let created = db.create("users".into(), user).await.unwrap();
        id = created["id"].as_str().unwrap().to_string();

        let verify_exists = db
            .read("users".into(), id.clone(), vec![], None)
            .await
            .unwrap();
        assert_eq!(verify_exists["name"], "Charlie");

        let filter = Filter::new("email".into(), FilterOp::Eq, json!("charlie@example.com"));
        let before_delete = db
            .list(
                "users".into(),
                vec![filter.clone()],
                vec![],
                None,
                vec![],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            before_delete.len(),
            1,
            "Should find entity by index before delete"
        );

        db.delete("users".into(), id.clone()).await.unwrap();
    }

    {
        let db = Database::open_without_background_tasks(&path).await.unwrap();

        let result = db.read("users".into(), id.clone(), vec![], None).await;
        assert!(
            result.is_err(),
            "Deleted entity should not exist after reopen"
        );

        let email_filter = Filter::new("email".into(), FilterOp::Eq, json!("charlie@example.com"));
        let by_email = db
            .list(
                "users".into(),
                vec![email_filter],
                vec![],
                None,
                vec![],
                None,
            )
            .await
            .unwrap();
        assert!(
            by_email.is_empty(),
            "Index by email should not have stale entries"
        );

        let status_filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
        let by_status = db
            .list(
                "users".into(),
                vec![status_filter],
                vec![],
                None,
                vec![],
                None,
            )
            .await
            .unwrap();
        assert!(
            by_status.is_empty(),
            "Index by status should not have stale entries"
        );

        let all_users = db
            .list("users".into(), vec![], vec![], None, vec![], None)
            .await
            .unwrap();
        assert!(
            all_users.is_empty(),
            "No entities should exist after delete"
        );
    }
}
