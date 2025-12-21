use mqdb::{Database, Error, FieldDefinition, FieldType, OnDeleteAction, Schema};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_not_null_constraint_create_with_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let user = json!({"name": "Alice", "email": null});
    let result = db.create("users".into(), user).await;

    assert!(matches!(result, Err(Error::NotNullViolation { .. })));
}

#[tokio::test]
async fn test_not_null_constraint_create_missing_field() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let user = json!({"name": "Alice"});
    let result = db.create("users".into(), user).await;

    assert!(matches!(result, Err(Error::NotNullViolation { .. })));
}

#[tokio::test]
async fn test_not_null_constraint_create_success() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let user = json!({"name": "Alice", "email": "alice@example.com"});
    let result = db.create("users".into(), user).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_not_null_constraint_update_to_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice", "email": "alice@example.com"});
    let created = db.create("users".into(), user).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let result = db.update("users".into(), id, json!({"email": null})).await;

    assert!(matches!(result, Err(Error::NotNullViolation { .. })));
}

#[tokio::test]
async fn test_unique_constraint_violation() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();

    let user1 = json!({"name": "Alice", "email": "alice@example.com"});
    db.create("users".into(), user1).await.unwrap();

    let user2 = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db.create("users".into(), user2).await;

    assert!(matches!(result, Err(Error::UniqueViolation { .. })));
}

#[tokio::test]
async fn test_unique_constraint_different_values() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();

    let user1 = json!({"name": "Alice", "email": "alice@example.com"});
    db.create("users".into(), user1).await.unwrap();

    let user2 = json!({"name": "Bob", "email": "bob@example.com"});
    let result = db.create("users".into(), user2).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_unique_constraint_update_to_duplicate() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();

    let user1 = json!({"name": "Alice", "email": "alice@example.com"});
    db.create("users".into(), user1).await.unwrap();

    let user2 = json!({"name": "Bob", "email": "bob@example.com"});
    let created = db.create("users".into(), user2).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update("users".into(), id, json!({"email": "alice@example.com"}))
        .await;

    assert!(matches!(result, Err(Error::UniqueViolation { .. })));
}

#[tokio::test]
async fn test_unique_constraint_update_same_value() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();

    let user = json!({"name": "Alice", "email": "alice@example.com"});
    let created = db.create("users".into(), user).await.unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update(
            "users".into(),
            id,
            json!({"name": "Alice Updated", "email": "alice@example.com"}),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_unique_constraint_concurrent_create() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(Database::open(tmp.path()).await.unwrap());

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    let handle1 = tokio::spawn(async move {
        let user = json!({"name": "Alice", "email": "same@example.com"});
        db1.create("users".into(), user).await
    });

    let handle2 = tokio::spawn(async move {
        let user = json!({"name": "Bob", "email": "same@example.com"});
        db2.create("users".into(), user).await
    });

    let result1 = handle1.await.unwrap();
    let result2 = handle2.await.unwrap();

    let has_violation = matches!(result1, Err(Error::UniqueViolation { .. }))
        || matches!(result2, Err(Error::UniqueViolation { .. }));
    assert!(
        has_violation,
        "one create should fail with unique violation"
    );
}

#[tokio::test]
async fn test_foreign_key_valid_reference() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await
    .unwrap();

    let post = json!({"title": "Hello", "author_id": user_id});
    let result = db.create("posts".into(), post).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_foreign_key_invalid_reference() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await
    .unwrap();

    let post = json!({"title": "Hello", "author_id": "nonexistent"});
    let result = db.create("posts".into(), post).await;

    assert!(matches!(result, Err(Error::ForeignKeyViolation { .. })));
}

#[tokio::test]
async fn test_foreign_key_null_allowed() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await
    .unwrap();

    let post = json!({"title": "Hello", "author_id": null});
    let result = db.create("posts".into(), post).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_foreign_key_update_to_invalid() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await
    .unwrap();

    let post = json!({"title": "Hello", "author_id": user_id});
    let created = db.create("posts".into(), post).await.unwrap();
    let post_id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update("posts".into(), post_id, json!({"author_id": "nonexistent"}))
        .await;

    assert!(matches!(result, Err(Error::ForeignKeyViolation { .. })));
}

#[tokio::test]
async fn test_foreign_key_restrict_delete() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await
    .unwrap();

    let post = json!({"title": "Hello", "author_id": user_id.clone()});
    db.create("posts".into(), post).await.unwrap();

    let result = db.delete("users".into(), user_id).await;

    assert!(matches!(result, Err(Error::ForeignKeyRestrict { .. })));
}

#[tokio::test]
async fn test_foreign_key_cascade_delete_simple() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let post1 = json!({"title": "Post 1", "author_id": user_id.clone()});
    let post2 = json!({"title": "Post 2", "author_id": user_id.clone()});
    db.create("posts".into(), post1).await.unwrap();
    db.create("posts".into(), post2).await.unwrap();

    let result = db.delete("users".into(), user_id).await;
    assert!(result.is_ok());

    let posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(posts.len(), 0, "all posts should be cascaded deleted");
}

#[tokio::test]
async fn test_foreign_key_cascade_delete_multilevel() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let post = json!({"title": "Post 1", "author_id": user_id.clone()});
    let created_post = db.create("posts".into(), post).await.unwrap();
    let post_id = created_post["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "comments".into(),
        "post_id".into(),
        "posts".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let comment1 = json!({"text": "Comment 1", "post_id": post_id.clone()});
    let comment2 = json!({"text": "Comment 2", "post_id": post_id.clone()});
    db.create("comments".into(), comment1).await.unwrap();
    db.create("comments".into(), comment2).await.unwrap();

    let result = db.delete("users".into(), user_id).await;
    assert!(result.is_ok());

    let posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(posts.len(), 0, "posts should be cascaded");

    let comments = db
        .list("comments".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(comments.len(), 0, "comments should be cascaded from posts");
}

#[tokio::test]
async fn test_foreign_key_set_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await.unwrap();
    let user_id = created_user["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::SetNull,
    )
    .await
    .unwrap();

    let post1 = json!({"title": "Post 1", "author_id": user_id.clone()});
    let post2 = json!({"title": "Post 2", "author_id": user_id.clone()});
    db.create("posts".into(), post1).await.unwrap();
    db.create("posts".into(), post2).await.unwrap();

    let result = db.delete("users".into(), user_id).await;
    assert!(result.is_ok());

    let all_posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_posts.len(), 2, "both posts should still exist");

    for post in &all_posts {
        assert_eq!(post["author_id"], json!(null), "author_id should be null");
    }
}

#[tokio::test]
async fn test_schema_type_validation() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String))
        .add_field(FieldDefinition::new("age", FieldType::Number));

    db.add_schema(schema).await.unwrap();

    let valid_user = json!({"name": "Alice", "age": 30});
    assert!(db.create("users".into(), valid_user).await.is_ok());

    let invalid_user = json!({"name": "Bob", "age": "thirty"});
    let result = db.create("users".into(), invalid_user).await;
    assert!(matches!(result, Err(Error::SchemaViolation { .. })));
}

#[tokio::test]
async fn test_schema_required_fields() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String).required())
        .add_field(FieldDefinition::new("email", FieldType::String).required());

    db.add_schema(schema).await.unwrap();

    let invalid_user = json!({"name": "Alice"});
    let result = db.create("users".into(), invalid_user).await;
    assert!(matches!(result, Err(Error::SchemaViolation { .. })));

    let valid_user = json!({"name": "Bob", "email": "bob@example.com"});
    assert!(db.create("users".into(), valid_user).await.is_ok());
}

#[tokio::test]
async fn test_schema_default_values() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String))
        .add_field(FieldDefinition::new("status", FieldType::String).with_default(json!("active")));

    db.add_schema(schema).await.unwrap();

    let user = json!({"name": "Alice"});
    let created = db.create("users".into(), user).await.unwrap();

    assert_eq!(created["status"], "active");
}

#[tokio::test]
async fn test_constraint_persistence_unique() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open(tmp.path()).await.unwrap();
        db.add_unique_constraint("users".into(), vec!["email".into()])
            .await
            .unwrap();

        let user = json!({"name": "Alice", "email": "alice@example.com"});
        db.create("users".into(), user).await.unwrap();
    }

    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db.create("users".into(), user).await;

    assert!(
        matches!(result, Err(Error::UniqueViolation { .. })),
        "unique constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_constraint_persistence_not_null() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open(tmp.path()).await.unwrap();
        db.add_not_null("users".into(), "email".into())
            .await
            .unwrap();
    }

    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let result = db.create("users".into(), user).await;

    assert!(
        matches!(result, Err(Error::NotNullViolation { .. })),
        "not null constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_constraint_persistence_foreign_key() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open(tmp.path()).await.unwrap();
        db.add_foreign_key(
            "posts".into(),
            "author_id".into(),
            "users".into(),
            "id".into(),
            OnDeleteAction::Restrict,
        )
        .await
        .unwrap();
    }

    let db = Database::open(tmp.path()).await.unwrap();

    let post = json!({"title": "Hello", "author_id": "nonexistent"});
    let result = db.create("posts".into(), post).await;

    assert!(
        matches!(result, Err(Error::ForeignKeyViolation { .. })),
        "foreign key constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_schema_persistence() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open(tmp.path()).await.unwrap();
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String).required());
        db.add_schema(schema).await.unwrap();
    }

    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"age": 30});
    let result = db.create("users".into(), user).await;

    assert!(
        matches!(result, Err(Error::SchemaViolation { .. })),
        "schema should persist across restarts"
    );
}

#[tokio::test]
async fn test_combined_schema_and_constraints() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String).required())
        .add_field(FieldDefinition::new("email", FieldType::String).required())
        .add_field(FieldDefinition::new("status", FieldType::String).with_default(json!("active")));

    db.add_schema(schema).await.unwrap();
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await
        .unwrap();
    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let user1 = json!({"name": "Alice", "email": "alice@example.com"});
    let created = db.create("users".into(), user1).await.unwrap();
    assert_eq!(created["status"], "active");

    let user2 = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db.create("users".into(), user2).await;
    assert!(matches!(result, Err(Error::UniqueViolation { .. })));

    let user3 = json!({"name": "Charlie"});
    let result = db.create("users".into(), user3).await;
    assert!(matches!(result, Err(Error::SchemaViolation { .. })));
}
