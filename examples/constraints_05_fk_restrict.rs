// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, OnDeleteAction, ScopeConfig};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/fk_restrict_example").await?;

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await?;

    println!("=== Foreign Key RESTRICT Delete ===\n");

    println!("Creating user...");
    let user = json!({"name": "Alice"});
    let created_user = db
        .create("users".into(), user, None, &ScopeConfig::default())
        .await?;
    let user_id = created_user["id"].as_str().unwrap();
    println!("✓ Created user: {user_id}\n");

    println!("Creating post by this user...");
    let post = json!({"title": "My Post", "author_id": user_id});
    db.create("posts".into(), post, None, &ScopeConfig::default())
        .await?;
    println!("✓ Created post\n");

    println!("Attempting to delete user (has posts referencing it)...");
    match db
        .delete(
            "users".into(),
            user_id.to_string(),
            None,
            &ScopeConfig::default(),
        )
        .await
    {
        Ok(()) => println!("✗ Should have been prevented!"),
        Err(e) => println!("✓ Deletion prevented: {e}\n"),
    }

    let users_after = db
        .list("users".into(), vec![], vec![], None, vec![], None)
        .await?;
    let posts_after = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    println!("After failed deletion:");
    println!("  Users: {} (still exists)", users_after.len());
    println!("  Posts: {} (still exists)\n", posts_after.len());

    println!("Deleting the post first...");
    let posts = db
        .list("posts".into(), vec![], vec![], None, vec![], None)
        .await?;
    let post_id = posts[0]["id"].as_str().unwrap();
    db.delete(
        "posts".into(),
        post_id.to_string(),
        None,
        &ScopeConfig::default(),
    )
    .await?;
    println!("✓ Post deleted\n");

    println!("Now attempting to delete user (no references)...");
    db.delete(
        "users".into(),
        user_id.to_string(),
        None,
        &ScopeConfig::default(),
    )
    .await?;
    println!("✓ User deleted successfully\n");

    println!("Summary:");
    println!("- RESTRICT prevents deletion when references exist");
    println!("- Protects against orphaned data");
    println!("- Must manually delete child records first");
    println!("- Useful for critical relationships");

    Ok(())
}
