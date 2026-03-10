// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, ScopeConfig};
use serde_json::{Value, json};

async fn create_record(db: &Database, entity: &str, data: Value) -> mqdb::Result<Value> {
    db.create(
        entity.into(),
        data,
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
}

async fn demo_single_field(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await?;

    println!("Creating user with email alice@example.com...");
    let created = create_record(
        db,
        "users",
        json!({"name": "Alice", "email": "alice@example.com"}),
    )
    .await?;
    println!("✓ Created: {created}\n");

    println!("Creating another user with different email...");
    let created = create_record(
        db,
        "users",
        json!({"name": "Bob", "email": "bob@example.com"}),
    )
    .await?;
    println!("✓ Created: {created}\n");

    println!("Attempting to create user with duplicate email...");
    match create_record(
        db,
        "users",
        json!({"name": "Charlie", "email": "alice@example.com"}),
    )
    .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Unique constraint violation: {e}\n"),
    }
    Ok(())
}

async fn demo_composite(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    db.add_unique_constraint("posts".into(), vec!["user_id".into(), "slug".into()])
        .await?;

    println!("Creating post with user_id=1, slug='hello'...");
    create_record(
        db,
        "posts",
        json!({"user_id": "1", "slug": "hello", "title": "Hello World"}),
    )
    .await?;
    println!("✓ Created\n");

    println!("Creating post with same slug but different user_id...");
    create_record(
        db,
        "posts",
        json!({"user_id": "2", "slug": "hello", "title": "Hello from Bob"}),
    )
    .await?;
    println!("✓ Created (different user_id, so allowed)\n");

    println!("Attempting to create duplicate (user_id=1, slug='hello')...");
    match create_record(
        db,
        "posts",
        json!({"user_id": "1", "slug": "hello", "title": "Duplicate"}),
    )
    .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Unique constraint violation: {e}\n"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/unique_example").await?;

    println!("=== Unique Constraints ===\n");
    demo_single_field(&db).await?;

    println!("=== Composite Unique Constraints ===\n");
    demo_composite(&db).await?;

    println!("Summary:");
    println!("- Unique constraints prevent duplicate values");
    println!("- Can be single field or composite (multiple fields)");
    println!("- Automatically creates an index for performance");

    Ok(())
}
