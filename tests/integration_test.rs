use mqdb::{Database, Filter, FilterOp, SortDirection, SortOrder};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_crud_operations() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({
        "name": "Alice",
        "email": "alice@example.com",
        "status": "active"
    });

    let created = db.create("users".into(), user).await.unwrap();
    assert_eq!(created["name"], "Alice");
    assert!(created.get("id").is_some());

    let id = created["id"].as_str().unwrap().to_string();

    let retrieved = db
        .read("users".into(), id.clone(), vec![], None)
        .await
        .unwrap();
    assert_eq!(retrieved["name"], "Alice");
    assert_eq!(retrieved["email"], "alice@example.com");

    let updates = json!({
        "name": "Alice Smith"
    });

    let updated_user = db
        .update("users".into(), id.clone(), updates)
        .await
        .unwrap();
    assert_eq!(updated_user["name"], "Alice Smith");
    assert_eq!(updated_user["email"], "alice@example.com");

    db.delete("users".into(), id.clone()).await.unwrap();

    let result = db.read("users".into(), id, vec![], None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_list_operations() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_index("users".into(), vec!["status".into()]).await;

    let users = vec![
        json!({"name": "Alice", "email": "alice@example.com", "status": "active"}),
        json!({"name": "Bob", "email": "bob@example.com", "status": "active"}),
        json!({"name": "Charlie", "email": "charlie@example.com", "status": "inactive"}),
    ];

    for user in users {
        db.create("users".into(), user).await.unwrap();
    }

    let all_users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_users.len(), 3);

    let active_filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
    let active_users = db
        .list(
            "users".into(),
            vec![active_filter],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(active_users.len(), 2);

    for user in active_users {
        assert_eq!(user["status"], "active");
    }

    let neq_filter = Filter::new("status".into(), FilterOp::Neq, json!("inactive"));
    let not_inactive_users = db
        .list("users".into(), vec![neq_filter], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        not_inactive_users.len(),
        2,
        "Neq filter should return 2 active users"
    );
    for user in not_inactive_users {
        assert_ne!(user["status"], "inactive");
    }
}

#[tokio::test]
async fn test_reactive_subscriptions() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let mut receiver = db.event_receiver();

    let sub_id = db
        .subscribe("users/#".into(), Some("users".into()))
        .await
        .unwrap();

    let user = json!({"name": "Test User", "email": "test@example.com"});
    let created = db.create("users".into(), user).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let event = tokio::time::timeout(tokio::time::Duration::from_millis(100), receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(event.entity, "users");
    assert_eq!(event.id, id);

    db.unsubscribe(&sub_id).await.unwrap();
}

#[tokio::test]
async fn test_subscription_persistence() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open(tmp.path()).await.unwrap();
        db.subscribe("users/#".into(), Some("users".into()))
            .await
            .unwrap();
    }

    {
        let db = Database::open(tmp.path()).await.unwrap();
        let mut receiver = db.event_receiver();

        let user = json!({"name": "Persisted Test", "email": "test@example.com"});
        db.create("users".into(), user).await.unwrap();

        let event = tokio::time::timeout(tokio::time::Duration::from_millis(100), receiver.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(event.entity, "users");
    }
}

#[tokio::test]
async fn test_wildcard_subscriptions() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let mut receiver = db.event_receiver();

    db.subscribe("users/+".into(), Some("users".into()))
        .await
        .unwrap();
    db.subscribe("posts/#".into(), Some("posts".into()))
        .await
        .unwrap();

    let user = json!({"name": "User 1"});
    db.create("users".into(), user).await.unwrap();

    let post = json!({"title": "Post 1"});
    db.create("posts".into(), post).await.unwrap();

    let event1 = tokio::time::timeout(tokio::time::Duration::from_millis(100), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event1.entity, "users");

    let event2 = tokio::time::timeout(tokio::time::Duration::from_millis(100), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event2.entity, "posts");
}

#[tokio::test]
async fn test_extended_filter_operators() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_index("users".into(), vec!["status".into(), "role".into()])
        .await;

    let users = vec![
        json!({"name": "Alice", "email": "alice@example.com", "status": "active", "role": "admin", "age": 30}),
        json!({"name": "Bob", "email": "bob@example.com", "status": "active", "role": "user", "age": 25}),
        json!({"name": "Charlie", "email": "charlie@example.com", "status": "inactive", "role": "user", "age": 35}),
        json!({"name": "David", "email": "david@example.com", "status": "pending", "role": null, "age": 28}),
    ];

    for user in users {
        db.create("users".into(), user).await.unwrap();
    }

    let in_filter = Filter::new("status".into(), FilterOp::In, json!(["active", "pending"]));
    let in_results = db
        .list("users".into(), vec![in_filter], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(in_results.len(), 3);
    for user in &in_results {
        let status = user["status"].as_str().unwrap();
        assert!(status == "active" || status == "pending");
    }

    let like_filter = Filter::new("email".into(), FilterOp::Like, json!("*@example.com"));
    let like_results = db
        .list(
            "users".into(),
            vec![like_filter],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(like_results.len(), 4);

    let like_prefix = Filter::new("name".into(), FilterOp::Like, json!("*li*"));
    let like_prefix_results = db
        .list(
            "users".into(),
            vec![like_prefix],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(like_prefix_results.len(), 2);

    let is_null_filter = Filter::new("role".into(), FilterOp::IsNull, json!(null));
    let is_null_results = db
        .list(
            "users".into(),
            vec![is_null_filter],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(is_null_results.len(), 1);
    assert_eq!(is_null_results[0]["name"], "David");

    let is_not_null_filter = Filter::new("role".into(), FilterOp::IsNotNull, json!(null));
    let is_not_null_results = db
        .list(
            "users".into(),
            vec![is_not_null_filter],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(is_not_null_results.len(), 3);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_sorting_and_pagination() {
    use mqdb::{Pagination, SortOrder};

    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    for i in 0..10 {
        let user = json!({
            "name": format!("User{}", 10 - i),
            "age": 20 + i,
            "score": (i * 3) % 10,
        });
        db.create("users".into(), user).await.unwrap();
    }

    let all_users = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_users.len(), 10);

    let sorted_by_age_asc = db
        .list(
            "users".into(),
            vec![],
            vec![SortOrder::asc("age".into())],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(sorted_by_age_asc[0]["age"], 20);
    assert_eq!(sorted_by_age_asc[9]["age"], 29);

    let sorted_by_age_desc = db
        .list(
            "users".into(),
            vec![],
            vec![SortOrder::desc("age".into())],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(sorted_by_age_desc[0]["age"], 29);
    assert_eq!(sorted_by_age_desc[9]["age"], 20);

    let multi_sort = db
        .list(
            "users".into(),
            vec![],
            vec![
                SortOrder::asc("score".into()),
                SortOrder::desc("age".into()),
            ],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(multi_sort.len(), 10);

    let paginated_page1 = db
        .list(
            "users".into(),
            vec![],
            vec![SortOrder::asc("age".into())],
            Some(Pagination::new(3, 0)),
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(paginated_page1.len(), 3);
    assert_eq!(paginated_page1[0]["age"], 20);
    assert_eq!(paginated_page1[2]["age"], 22);

    let paginated_page2 = db
        .list(
            "users".into(),
            vec![],
            vec![SortOrder::asc("age".into())],
            Some(Pagination::new(3, 3)),
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(paginated_page2.len(), 3);
    assert_eq!(paginated_page2[0]["age"], 23);
    assert_eq!(paginated_page2[2]["age"], 25);

    let paginated_last = db
        .list(
            "users".into(),
            vec![],
            vec![SortOrder::asc("age".into())],
            Some(Pagination::new(3, 9)),
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(paginated_last.len(), 1);
    assert_eq!(paginated_last[0]["age"], 29);

    let empty_page = db
        .list(
            "users".into(),
            vec![],
            vec![],
            Some(Pagination::new(5, 20)),
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(empty_page.len(), 0);
}

#[tokio::test]
async fn test_relationships_and_includes() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_relationship("posts".into(), "author".into(), "users".into())
        .await;
    db.add_relationship("comments".into(), "post".into(), "posts".into())
        .await;

    let author = json!({
        "name": "Alice",
        "email": "alice@example.com"
    });
    let created_author = db.create("users".into(), author).await.unwrap();
    let author_id = created_author["id"].as_str().unwrap().to_string();

    let post = json!({
        "title": "My First Post",
        "content": "Hello World!",
        "author_id": author_id.clone()
    });
    let created_post = db.create("posts".into(), post).await.unwrap();
    let post_id = created_post["id"].as_str().unwrap().to_string();

    let comment = json!({
        "text": "Great post!",
        "post_id": post_id.clone()
    });
    let created_comment = db.create("comments".into(), comment).await.unwrap();
    let comment_id = created_comment["id"].as_str().unwrap().to_string();

    let post_with_author = db
        .read("posts".into(), post_id.clone(), vec!["author".into()], None)
        .await
        .unwrap();
    assert_eq!(post_with_author["title"], "My First Post");
    assert_eq!(post_with_author["author"]["name"], "Alice");
    assert_eq!(post_with_author["author"]["email"], "alice@example.com");
    assert_eq!(post_with_author["author"]["id"], author_id);

    let comment_with_post = db
        .read("comments".into(), comment_id, vec!["post".into()], None)
        .await
        .unwrap();
    assert_eq!(comment_with_post["text"], "Great post!");
    assert_eq!(comment_with_post["post"]["title"], "My First Post");
    assert_eq!(comment_with_post["post"]["id"], post_id);

    let posts = db
        .list(
            "posts".into(),
            vec![],
            vec![],
            None,
            vec!["author".into()],
            None,
        )
        .await
        .unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["author"]["name"], "Alice");
}

#[tokio::test]
async fn test_ttl_expiration() {
    use mqdb::DatabaseConfig;

    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path()).with_ttl_cleanup_interval(Some(1));
    let db = Database::open_with_config(config).await.unwrap();

    let mut receiver = db.event_receiver();

    db.subscribe("temp/#".into(), Some("temp".into()))
        .await
        .unwrap();

    let short_lived = json!({
        "name": "Temporary Entity",
        "ttl_secs": 2
    });

    let created = db.create("temp".into(), short_lived).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    assert!(created.get("_expires_at").is_some());
    assert!(created.get("ttl_secs").is_none());

    let retrieved = db
        .read("temp".into(), id.clone(), vec![], None)
        .await
        .unwrap();
    assert_eq!(retrieved["name"], "Temporary Entity");

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let result = db.read("temp".into(), id.clone(), vec![], None).await;
    assert!(result.is_err());

    let create_event =
        tokio::time::timeout(tokio::time::Duration::from_millis(100), receiver.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(create_event.entity, "temp");

    let delete_event = tokio::time::timeout(tokio::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(delete_event.entity, "temp");
    assert_eq!(delete_event.id, id);
}

#[tokio::test]
async fn test_ttl_disabled() {
    use mqdb::DatabaseConfig;

    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path()).with_ttl_cleanup_interval(None);
    let db = Database::open_with_config(config).await.unwrap();

    let entity = json!({
        "name": "Should Persist",
        "ttl_secs": 1
    });

    let created = db.create("items".into(), entity).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    assert!(created.get("_expires_at").is_some());

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let retrieved = db.read("items".into(), id, vec![], None).await.unwrap();
    assert_eq!(retrieved["name"], "Should Persist");
}

#[tokio::test]
async fn test_ttl_with_indexes() {
    use mqdb::DatabaseConfig;

    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path()).with_ttl_cleanup_interval(Some(1));
    let db = Database::open_with_config(config).await.unwrap();

    db.add_index("sessions".into(), vec!["user_id".into()])
        .await;

    let session = json!({
        "user_id": "user123",
        "token": "abc123",
        "ttl_secs": 2
    });

    db.create("sessions".into(), session).await.unwrap();

    let filter = Filter::new("user_id".into(), FilterOp::Eq, json!("user123"));
    let results = db
        .list(
            "sessions".into(),
            vec![filter.clone()],
            vec![],
            None,
            vec![],
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let results_after = db
        .list("sessions".into(), vec![filter], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(results_after.len(), 0);
}

#[tokio::test]
async fn test_cursor_api() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    for i in 0..50 {
        let user = json!({
            "name": format!("User {}", i),
            "age": 20 + (i % 30),
            "status": if i % 2 == 0 { "active" } else { "inactive" }
        });
        db.create("users".into(), user).await.unwrap();
    }

    let mut cursor = db.cursor("users".into(), vec![], vec![]).unwrap();

    let mut count = 0;
    while let Some(_user) = cursor.next().unwrap() {
        count += 1;
    }
    assert_eq!(count, 50);

    let filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
    let mut filtered_cursor = db.cursor("users".into(), vec![filter], vec![]).unwrap();

    let mut active_count = 0;
    while let Some(user) = filtered_cursor.next().unwrap() {
        assert_eq!(user["status"], "active");
        active_count += 1;
    }
    assert_eq!(active_count, 25);

    let age_filter = Filter::new("age".into(), FilterOp::Gt, json!(30));
    let mut age_cursor = db.cursor("users".into(), vec![age_filter], vec![]).unwrap();

    let batch = age_cursor.next_batch(10).unwrap();
    assert!(!batch.is_empty());
    for user in batch {
        let age = user["age"].as_u64().unwrap();
        assert!(age > 30);
    }
}

#[tokio::test]
async fn test_cursor_with_sorting() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    for i in 0..20 {
        let user = json!({
            "name": format!("User {}", i),
            "age": 50 - i,
            "score": (i * 3) % 10,
        });
        db.create("users".into(), user).await.unwrap();
    }

    let sort_by_age_asc = vec![SortOrder::new("age".into(), SortDirection::Asc)];
    let mut cursor = db.cursor("users".into(), vec![], sort_by_age_asc).unwrap();

    let mut prev_age = 0;
    let mut count = 0;
    while let Some(user) = cursor.next().unwrap() {
        let age = user["age"].as_u64().unwrap();
        assert!(age >= prev_age, "ages should be in ascending order");
        prev_age = age;
        count += 1;
    }
    assert_eq!(count, 20);

    let sort_by_age_desc = vec![SortOrder::new("age".into(), SortDirection::Desc)];
    let mut desc_cursor = db.cursor("users".into(), vec![], sort_by_age_desc).unwrap();

    let mut prev_age = u64::MAX;
    while let Some(user) = desc_cursor.next().unwrap() {
        let age = user["age"].as_u64().unwrap();
        assert!(age <= prev_age, "ages should be in descending order");
        prev_age = age;
    }

    let multi_sort = vec![
        SortOrder::new("score".into(), SortDirection::Asc),
        SortOrder::new("age".into(), SortDirection::Desc),
    ];
    let mut multi_cursor = db.cursor("users".into(), vec![], multi_sort).unwrap();

    let mut results = vec![];
    while let Some(user) = multi_cursor.next().unwrap() {
        results.push((
            user["score"].as_u64().unwrap(),
            user["age"].as_u64().unwrap(),
        ));
    }

    for i in 1..results.len() {
        let (prev_score, prev_age) = results[i - 1];
        let (curr_score, curr_age) = results[i];
        if prev_score == curr_score {
            assert!(
                curr_age <= prev_age,
                "when scores are equal, ages should be descending"
            );
        } else {
            assert!(curr_score >= prev_score, "scores should be ascending");
        }
    }
}

#[tokio::test]
async fn test_physical_backup_and_restore() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let backup_path = tmp.path().join("backup");

    let db = Database::open(&db_path).await.unwrap();

    for i in 0..10 {
        let user = json!({
            "name": format!("User {}", i),
            "email": format!("user{}@example.com", i),
        });
        db.create("users".into(), user).await.unwrap();
    }

    db.backup_physical(&backup_path).unwrap();

    drop(db);

    let restored_db = Database::open(&backup_path).await.unwrap();
    let users = restored_db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();

    assert_eq!(users.len(), 10);
}

#[tokio::test]
async fn test_logical_backup_and_restore() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let backup_file = tmp.path().join("backup.jsonl");

    let db = Database::open(&db_path).await.unwrap();

    for i in 0..20 {
        let product = json!({
            "name": format!("Product {}", i),
            "price": 10.0 + f64::from(i),
            "stock": 100 - i,
        });
        db.create("products".into(), product).await.unwrap();
    }

    db.backup_logical(&backup_file).unwrap();

    let new_db_path = tmp.path().join("restored_db");
    let new_db = Database::open(&new_db_path).await.unwrap();

    let count = new_db.restore_logical(&backup_file).await.unwrap();
    assert_eq!(count, 20);

    let products = new_db
        .list("products".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();

    assert_eq!(products.len(), 20);

    for product in &products {
        assert!(product["name"].as_str().unwrap().starts_with("Product"));
        assert!(product["price"].as_f64().unwrap() >= 10.0);
    }
}

#[tokio::test]
async fn test_backup_fails_if_destination_exists() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let backup_path = tmp.path().join("backup");

    let db = Database::open(&db_path).await.unwrap();

    db.create("users".into(), json!({"name": "Test"}))
        .await
        .unwrap();

    std::fs::create_dir(&backup_path).unwrap();

    let result = db.backup_physical(&backup_path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[tokio::test]
async fn test_restore_fails_if_source_not_found() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let missing_backup = tmp.path().join("missing.jsonl");

    let db = Database::open(&db_path).await.unwrap();

    let result = db.restore_logical(&missing_backup).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}
