// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, OnDeleteAction, ScopeConfig};
use serde_json::json;

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
    let tech = json!({"name": "Technology"});
    let created_tech = db
        .create(
            "categories".into(),
            tech,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;
    let tech_id = created_tech["id"].as_str().unwrap();

    let sports = json!({"name": "Sports"});
    let created_sports = db
        .create(
            "categories".into(),
            sports,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;
    let sports_id = created_sports["id"].as_str().unwrap();
    println!("✓ Created categories: {tech_id}, {sports_id}\n");

    println!("Creating posts with categories...");
    let post1 = json!({"title": "AI Trends", "category_id": tech_id});
    db.create("posts".into(), post1, None, None, &ScopeConfig::default())
        .await?;

    let post2 = json!({"title": "Cloud Computing", "category_id": tech_id});
    db.create("posts".into(), post2, None, None, &ScopeConfig::default())
        .await?;

    let post3 = json!({"title": "World Cup", "category_id": sports_id});
    db.create("posts".into(), post3, None, None, &ScopeConfig::default())
        .await?;
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
    db.delete(
        "categories".into(),
        tech_id.to_string(),
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;
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
