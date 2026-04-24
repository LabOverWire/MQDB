// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::Database;
use mqdb_agent::database::CallerContext;
use mqdb_core::schema::{FieldDefinition, FieldType, Schema};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_core::{ChangeEvent, Error, OnDeleteAction, Operation};
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
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_not_null_constraint_update_to_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice", "email": "alice@example.com"});
    let created = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    db.add_not_null("users".into(), "email".into())
        .await
        .unwrap();

    let result = db
        .update(
            "users".into(),
            id,
            json!({"email": null}),
            None,
            &CallerContext {
                sender: None,
                client_id: None,
                scope_config: &ScopeConfig::default(),
            },
        )
        .await;

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
    db.create(
        "users".into(),
        user1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let user2 = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db
        .create(
            "users".into(),
            user2,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    db.create(
        "users".into(),
        user1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let user2 = json!({"name": "Bob", "email": "bob@example.com"});
    let result = db
        .create(
            "users".into(),
            user2,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    db.create(
        "users".into(),
        user1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let user2 = json!({"name": "Bob", "email": "bob@example.com"});
    let created = db
        .create(
            "users".into(),
            user2,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update(
            "users".into(),
            id,
            json!({"email": "alice@example.com"}),
            None,
            &CallerContext {
                sender: None,
                client_id: None,
                scope_config: &ScopeConfig::default(),
            },
        )
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
    let created = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update(
            "users".into(),
            id,
            json!({"name": "Alice Updated", "email": "alice@example.com"}),
            None,
            &CallerContext {
                sender: None,
                client_id: None,
                scope_config: &ScopeConfig::default(),
            },
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
        db1.create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
    });

    let handle2 = tokio::spawn(async move {
        let user = json!({"name": "Bob", "email": "same@example.com"});
        db2.create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
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
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    let result = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    let result = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    let result = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_foreign_key_update_to_invalid() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    let created = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let post_id = created["id"].as_str().unwrap().to_string();

    let result = db
        .update(
            "posts".into(),
            post_id,
            json!({"author_id": "nonexistent"}),
            None,
            &CallerContext {
                sender: None,
                client_id: None,
                scope_config: &ScopeConfig::default(),
            },
        )
        .await;

    assert!(matches!(result, Err(Error::ForeignKeyViolation { .. })));
}

#[tokio::test]
async fn test_foreign_key_restrict_delete() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    db.create(
        "posts".into(),
        post,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let result = db
        .delete(
            "users".into(),
            user_id,
            None,
            None,
            &ScopeConfig::default(),
            &OwnershipConfig::default(),
        )
        .await;

    assert!(matches!(result, Err(Error::ForeignKeyRestrict { .. })));
}

#[tokio::test]
async fn test_foreign_key_cascade_delete_simple() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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

    let post_entry1 = json!({"title": "Post 1", "author_id": user_id.clone()});
    let post_entry2 = json!({"title": "Post 2", "author_id": user_id.clone()});
    db.create(
        "posts".into(),
        post_entry1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
    db.create(
        "posts".into(),
        post_entry2,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let result = db
        .delete(
            "users".into(),
            user_id,
            None,
            None,
            &ScopeConfig::default(),
            &OwnershipConfig::default(),
        )
        .await;
    assert!(result.is_ok());

    let all_posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_posts.len(), 0, "all posts should be cascaded deleted");
}

#[tokio::test]
async fn test_foreign_key_cascade_delete_multilevel() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    let created_post = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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

    let comment_entry1 = json!({"text": "Comment 1", "post_id": post_id.clone()});
    let comment_entry2 = json!({"text": "Comment 2", "post_id": post_id.clone()});
    db.create(
        "comments".into(),
        comment_entry1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
    db.create(
        "comments".into(),
        comment_entry2,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let result = db
        .delete(
            "users".into(),
            user_id,
            None,
            None,
            &ScopeConfig::default(),
            &OwnershipConfig::default(),
        )
        .await;
    assert!(result.is_ok());

    let all_posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(all_posts.len(), 0, "posts should be cascaded");

    let all_comments = db
        .list("comments".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        all_comments.len(),
        0,
        "comments should be cascaded from posts"
    );
}

#[tokio::test]
async fn test_foreign_key_set_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    db.create(
        "posts".into(),
        post1,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
    db.create(
        "posts".into(),
        post2,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let result = db
        .delete(
            "users".into(),
            user_id,
            None,
            None,
            &ScopeConfig::default(),
            &OwnershipConfig::default(),
        )
        .await;
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
    assert!(
        db.create(
            "users".into(),
            valid_user,
            None,
            None,
            None,
            &ScopeConfig::default()
        )
        .await
        .is_ok()
    );

    let invalid_user = json!({"name": "Bob", "age": "thirty"});
    let result = db
        .create(
            "users".into(),
            invalid_user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;
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
    let result = db
        .create(
            "users".into(),
            invalid_user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;
    assert!(matches!(result, Err(Error::SchemaViolation { .. })));

    let valid_user = json!({"name": "Bob", "email": "bob@example.com"});
    assert!(
        db.create(
            "users".into(),
            valid_user,
            None,
            None,
            None,
            &ScopeConfig::default()
        )
        .await
        .is_ok()
    );
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
    let created = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(created["status"], "active");
}

#[tokio::test]
async fn test_constraint_persistence_unique() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        db.add_unique_constraint("users".into(), vec!["email".into()])
            .await
            .unwrap();

        let user = json!({"name": "Alice", "email": "alice@example.com"});
        db.create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    }

    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();

    let user = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

    assert!(
        matches!(result, Err(Error::UniqueViolation { .. })),
        "unique constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_constraint_persistence_not_null() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        db.add_not_null("users".into(), "email".into())
            .await
            .unwrap();
    }

    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();

    let user = json!({"name": "Alice"});
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

    assert!(
        matches!(result, Err(Error::NotNullViolation { .. })),
        "not null constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_constraint_persistence_foreign_key() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
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

    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();

    let post = json!({"title": "Hello", "author_id": "nonexistent"});
    let result = db
        .create(
            "posts".into(),
            post,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

    assert!(
        matches!(result, Err(Error::ForeignKeyViolation { .. })),
        "foreign key constraint should persist across restarts"
    );
}

#[tokio::test]
async fn test_schema_persistence() {
    let tmp = TempDir::new().unwrap();

    {
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String).required());
        db.add_schema(schema).await.unwrap();
    }

    let db = Database::open_without_background_tasks(tmp.path())
        .await
        .unwrap();

    let user = json!({"age": 30});
    let result = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;

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
    let created = db
        .create(
            "users".into(),
            user1,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    assert_eq!(created["status"], "active");

    let user2 = json!({"name": "Bob", "email": "alice@example.com"});
    let result = db
        .create(
            "users".into(),
            user2,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;
    assert!(matches!(result, Err(Error::UniqueViolation { .. })));

    let user3 = json!({"name": "Charlie"});
    let result = db
        .create(
            "users".into(),
            user3,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await;
    assert!(matches!(result, Err(Error::SchemaViolation { .. })));
}

#[tokio::test]
async fn test_cascade_delete_emits_change_events() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let mut rx = db.event_receiver();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    let created_post = db
        .create(
            "posts".into(),
            post1,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let post_id = created_post["id"].as_str().unwrap().to_string();

    while rx.try_recv().is_ok() {}

    db.delete(
        "users".into(),
        user_id,
        None,
        None,
        &ScopeConfig::default(),
        &OwnershipConfig::default(),
    )
    .await
    .unwrap();

    let mut events: Vec<ChangeEvent> = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }

    let parent_delete = events
        .iter()
        .find(|e| e.entity == "users" && e.operation == Operation::Delete);
    assert!(parent_delete.is_some(), "parent delete event expected");

    let child_delete = events
        .iter()
        .find(|e| e.entity == "posts" && e.id == post_id && e.operation == Operation::Delete);
    assert!(
        child_delete.is_some(),
        "cascade child delete event expected"
    );
}

#[tokio::test]
async fn test_set_null_emits_update_change_events() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();
    let mut rx = db.event_receiver();

    let user = json!({"name": "Alice"});
    let created_user = db
        .create(
            "users".into(),
            user,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
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
    let created_p1 = db
        .create(
            "posts".into(),
            post1,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let created_p2 = db
        .create(
            "posts".into(),
            post2,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let p1_id = created_p1["id"].as_str().unwrap().to_string();
    let p2_id = created_p2["id"].as_str().unwrap().to_string();

    while rx.try_recv().is_ok() {}

    db.delete(
        "users".into(),
        user_id,
        None,
        None,
        &ScopeConfig::default(),
        &OwnershipConfig::default(),
    )
    .await
    .unwrap();

    let mut events: Vec<ChangeEvent> = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }

    let parent_delete = events
        .iter()
        .find(|e| e.entity == "users" && e.operation == Operation::Delete);
    assert!(parent_delete.is_some(), "parent delete event expected");

    let update_events: Vec<&ChangeEvent> = events
        .iter()
        .filter(|e| e.entity == "posts" && e.operation == Operation::Update)
        .collect();
    assert_eq!(
        update_events.len(),
        2,
        "expected 2 set-null Update events for posts"
    );

    let mut updated_ids: Vec<&str> = update_events.iter().map(|e| e.id.as_str()).collect();
    updated_ids.sort_unstable();
    let mut expected_ids = vec![p1_id.as_str(), p2_id.as_str()];
    expected_ids.sort_unstable();
    assert_eq!(updated_ids, expected_ids);

    for ev in &update_events {
        let data = ev.data.as_ref().unwrap();
        assert_eq!(data["author_id"], json!(null));
        assert_eq!(data["_version"], json!(2));
    }
}

#[tokio::test]
async fn test_owner_aware_cascade_same_owner_deletes() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId,tasks=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let task = json!({"name": "T1", "userId": "alice", "projectId": project_id.clone()});
    db.create(
        "tasks".into(),
        task,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    db.delete(
        "projects".into(),
        project_id,
        Some("alice"),
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 0, "same-owner task should be cascade deleted");
}

#[tokio::test]
async fn test_owner_aware_cascade_cross_owner_survives_with_null_fk() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId,tasks=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let task = json!({"name": "T1", "userId": "bob", "projectId": project_id.clone()});
    let created_task = db
        .create(
            "tasks".into(),
            task,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let task_id = created_task["id"].as_str().unwrap().to_string();

    db.delete(
        "projects".into(),
        project_id,
        Some("alice"),
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1, "cross-owned task should survive");

    let surviving = db
        .read("tasks".into(), task_id, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        surviving["projectId"],
        json!(null),
        "FK field should be set to null"
    );
}

#[tokio::test]
async fn test_owner_aware_cascade_cross_owner_blocked_by_not_null() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId,tasks=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    db.add_not_null("tasks".into(), "projectId".into())
        .await
        .unwrap();

    let task = json!({"name": "T1", "userId": "bob", "projectId": project_id.clone()});
    db.create(
        "tasks".into(),
        task,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    let result = db
        .delete(
            "projects".into(),
            project_id,
            Some("alice"),
            None,
            &ScopeConfig::default(),
            &ownership,
        )
        .await;

    assert!(
        matches!(result, Err(Error::CascadeBlocked(_))),
        "should be blocked by cross-owned entity with NotNull FK"
    );
}

#[tokio::test]
async fn test_owner_aware_cascade_admin_bypasses_ownership() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let mut ownership = OwnershipConfig::parse("projects=userId,tasks=userId").unwrap();
    ownership.add_admin_user("admin".to_string());

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let task = json!({"name": "T1", "userId": "bob", "projectId": project_id.clone()});
    db.create(
        "tasks".into(),
        task,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    db.delete(
        "projects".into(),
        project_id,
        Some("admin"),
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 0, "admin should blind cascade all entities");
}

#[tokio::test]
async fn test_owner_aware_cascade_no_sender_blind() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId,tasks=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let task = json!({"name": "T1", "userId": "bob", "projectId": project_id.clone()});
    db.create(
        "tasks".into(),
        task,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    db.delete(
        "projects".into(),
        project_id,
        None,
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 0, "no sender means blind cascade");
}

#[tokio::test]
async fn test_owner_aware_cascade_unowned_entity_always_cascades() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let task = json!({"name": "T1", "projectId": project_id.clone()});
    db.create(
        "tasks".into(),
        task,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    db.delete(
        "projects".into(),
        project_id,
        Some("alice"),
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        tasks.len(),
        0,
        "unowned entity should always be cascade deleted"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_owner_aware_cascade_mixed_ownership_multilevel() {
    let tmp = TempDir::new().unwrap();
    let db = Database::open(tmp.path()).await.unwrap();

    let ownership = OwnershipConfig::parse("projects=userId,tasks=userId,subtasks=userId").unwrap();

    let project = json!({"name": "P1", "userId": "alice"});
    let created_project = db
        .create(
            "projects".into(),
            project,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let project_id = created_project["id"].as_str().unwrap().to_string();

    db.add_foreign_key(
        "tasks".into(),
        "projectId".into(),
        "projects".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();
    db.add_foreign_key(
        "subtasks".into(),
        "taskId".into(),
        "tasks".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await
    .unwrap();

    let alice_task = json!({"name": "AT1", "userId": "alice", "projectId": project_id.clone()});
    let created_alice_task = db
        .create(
            "tasks".into(),
            alice_task,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let alice_task_id = created_alice_task["id"].as_str().unwrap().to_string();

    let bob_task = json!({"name": "BT1", "userId": "bob", "projectId": project_id.clone()});
    let created_bob_task = db
        .create(
            "tasks".into(),
            bob_task,
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
    let bob_task_id = created_bob_task["id"].as_str().unwrap().to_string();

    let subtask = json!({"name": "S1", "userId": "alice", "taskId": alice_task_id.clone()});
    db.create(
        "subtasks".into(),
        subtask,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();

    db.delete(
        "projects".into(),
        project_id,
        Some("alice"),
        None,
        &ScopeConfig::default(),
        &ownership,
    )
    .await
    .unwrap();

    let tasks = db
        .list("tasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1, "bob's task should survive");
    assert_eq!(tasks[0]["id"], bob_task_id);
    assert_eq!(
        tasks[0]["projectId"],
        json!(null),
        "bob's task FK should be null"
    );

    let alice_tasks: Vec<_> = tasks.iter().filter(|t| t["userId"] == "alice").collect();
    assert_eq!(alice_tasks.len(), 0, "alice's task should be deleted");

    let subtasks = db
        .list("subtasks".into(), vec![], vec![], None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        subtasks.len(),
        0,
        "alice's subtask should cascade from alice's task"
    );
}
