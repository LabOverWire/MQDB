// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, OnDeleteAction, ScopeConfig};
use serde_json::{Value, json};

async fn create_record(db: &Database, entity: &str, data: Value) -> Value {
    db.create(
        entity.into(),
        data,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap()
}

async fn setup_constraints(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await?;

    db.add_foreign_key(
        "comments".into(),
        "post_id".into(),
        "posts".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await?;
    Ok(())
}

async fn seed_data(db: &Database) -> String {
    println!("Creating user...");
    let created_user = create_record(db, "users", json!({"name": "Alice"})).await;
    let user_id = created_user["id"].as_str().unwrap().to_string();
    println!("✓ Created user: {user_id}\n");

    println!("Creating 2 posts by this user...");
    let created_post1 = create_record(
        db,
        "posts",
        json!({"title": "First Post", "author_id": &user_id}),
    )
    .await;
    let post1_id = created_post1["id"].as_str().unwrap();

    let created_post2 = create_record(
        db,
        "posts",
        json!({"title": "Second Post", "author_id": &user_id}),
    )
    .await;
    let post2_id = created_post2["id"].as_str().unwrap();
    println!("✓ Created posts: {post1_id}, {post2_id}\n");

    println!("Creating 3 comments on posts...");
    create_record(
        db,
        "comments",
        json!({"text": "Nice!", "post_id": post1_id}),
    )
    .await;
    create_record(
        db,
        "comments",
        json!({"text": "Great!", "post_id": post1_id}),
    )
    .await;
    create_record(
        db,
        "comments",
        json!({"text": "Awesome!", "post_id": post2_id}),
    )
    .await;
    println!("✓ Created 3 comments\n");

    user_id
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/fk_cascade_example").await?;
    setup_constraints(&db).await?;

    println!("=== Foreign Key CASCADE Delete ===\n");

    let user_id = seed_data(&db).await;

    let posts_before = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    let comments_before = db
        .list("comments".into(), vec![], vec![], None, vec![], None)
        .await?;
    println!("Before deletion:");
    println!("  Posts: {}", posts_before.len());
    println!("  Comments: {}\n", comments_before.len());

    println!("Deleting user (should cascade to posts and comments)...");
    db.delete("users".into(), user_id, None, None, &ScopeConfig::default())
        .await?;
    println!("✓ User deleted\n");

    let posts_after = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    let comments_after = db
        .list("comments".into(), vec![], vec![], None, vec![], None)
        .await?;
    println!("After deletion:");
    println!("  Posts: {} (cascaded)", posts_after.len());
    println!(
        "  Comments: {} (cascaded from posts)\n",
        comments_after.len()
    );

    println!("Summary:");
    println!("- CASCADE deletes all referencing entities automatically");
    println!("- Works recursively (User → Posts → Comments)");
    println!("- Maintains referential integrity");
    println!("- All deletions happen atomically in one transaction");

    Ok(())
}
