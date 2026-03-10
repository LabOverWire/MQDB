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

async fn delete_record(db: &Database, entity: &str, id: &str) {
    db.delete(
        entity.into(),
        id.to_string(),
        None,
        None,
        &ScopeConfig::default(),
    )
    .await
    .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/fk_set_null_example").await?;

    db.add_foreign_key(
        "posts".into(),
        "category_id".into(),
        "categories".into(),
        "id".into(),
        OnDeleteAction::SetNull,
    )
    .await?;

    println!("=== Foreign Key SET NULL ===\n");

    println!("Creating categories...");
    let created_tech = create_record(&db, "categories", json!({"name": "Technology"})).await;
    let tech_id = created_tech["id"].as_str().unwrap();

    let created_sports = create_record(&db, "categories", json!({"name": "Sports"})).await;
    let sports_id = created_sports["id"].as_str().unwrap();
    println!("✓ Created categories: {tech_id}, {sports_id}\n");

    println!("Creating posts with categories...");
    create_record(
        &db,
        "posts",
        json!({"title": "AI Trends", "category_id": tech_id}),
    )
    .await;
    create_record(
        &db,
        "posts",
        json!({"title": "Cloud Computing", "category_id": tech_id}),
    )
    .await;
    create_record(
        &db,
        "posts",
        json!({"title": "World Cup", "category_id": sports_id}),
    )
    .await;
    println!("✓ Created 3 posts\n");

    let posts_before = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    println!("Before deletion:");
    for post in &posts_before {
        println!("  {} - category: {:?}", post["title"], post["category_id"]);
    }
    println!();

    println!("Deleting Technology category...");
    delete_record(&db, "categories", tech_id).await;
    println!("✓ Category deleted\n");

    let posts_after = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    println!("After deletion:");
    println!("  Total posts: {} (all still exist)", posts_after.len());
    for post in &posts_after {
        println!("  {} - category: {:?}", post["title"], post["category_id"]);
    }
    println!();

    println!("Summary:");
    println!("- SET NULL preserves child records when parent is deleted");
    println!("- Foreign key field is set to null automatically");
    println!("- Useful for optional relationships");
    println!("- Posts can exist without a category");

    Ok(())
}
