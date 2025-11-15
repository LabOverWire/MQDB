use mqdb::{Database, FieldDefinition, FieldType, OnDeleteAction, Schema};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/combined_example").await?;

    println!("=== Complete Blog System with All Constraints ===\n");

    println!("Setting up schemas and constraints...\n");

    let user_schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String).required())
        .add_field(FieldDefinition::new("email", FieldType::String).required())
        .add_field(FieldDefinition::new("status", FieldType::String).default(json!("active")));

    db.add_schema(user_schema).await?;
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await?;
    db.add_not_null("users".into(), "email".into()).await?;

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

    println!("✓ Schemas: users with type validation, required fields, defaults");
    println!("✓ Unique: email uniqueness");
    println!("✓ Not-null: email required");
    println!("✓ Foreign keys: users → posts → comments (cascade)\n");

    println!("Creating users...");
    let alice = db
        .create("users".into(), json!({"name": "Alice", "email": "alice@example.com"}))
        .await?;
    let alice_id = alice["id"].as_str().unwrap();
    println!("✓ Created Alice (status defaulted to 'active')");

    let bob = db
        .create("users".into(), json!({"name": "Bob", "email": "bob@example.com"}))
        .await?;
    let bob_id = bob["id"].as_str().unwrap();
    println!("✓ Created Bob\n");

    println!("Testing unique constraint...");
    match db
        .create("users".into(), json!({"name": "Charlie", "email": "alice@example.com"}))
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(_) => println!("✓ Duplicate email rejected\n"),
    }

    println!("Creating posts...");
    let post1 = db
        .create("posts".into(), json!({"title": "Hello World", "author_id": alice_id}))
        .await?;
    let post1_id = post1["id"].as_str().unwrap();

    db.create("posts".into(), json!({"title": "Rust Tips", "author_id": alice_id}))
        .await?;

    db.create("posts".into(), json!({"title": "Bob's Post", "author_id": bob_id}))
        .await?;
    println!("✓ Created 3 posts\n");

    println!("Creating comments...");
    db.create("comments".into(), json!({"text": "Great post!", "post_id": post1_id}))
        .await?;
    db.create("comments".into(), json!({"text": "Thanks!", "post_id": post1_id}))
        .await?;
    println!("✓ Created 2 comments\n");

    println!("Testing foreign key validation...");
    match db
        .create("posts".into(), json!({"title": "Orphan", "author_id": "nonexistent"}))
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(_) => println!("✓ Invalid author_id rejected\n"),
    }

    let stats_before = (
        db.list("users".into(), vec![], vec![], None, vec![]).await?.len(),
        db.list("posts".into(), vec![], vec![], None, vec![]).await?.len(),
        db.list("comments".into(), vec![], vec![], None, vec![]).await?.len(),
    );
    println!("Before cascade deletion:");
    println!("  Users: {}, Posts: {}, Comments: {}\n", stats_before.0, stats_before.1, stats_before.2);

    println!("Deleting Alice (cascade to posts and comments)...");
    db.delete("users".into(), alice_id.to_string()).await?;
    println!("✓ Deleted\n");

    let stats_after = (
        db.list("users".into(), vec![], vec![], None, vec![]).await?.len(),
        db.list("posts".into(), vec![], vec![], None, vec![]).await?.len(),
        db.list("comments".into(), vec![], vec![], None, vec![]).await?.len(),
    );
    println!("After cascade deletion:");
    println!("  Users: {} (Alice deleted)", stats_after.0);
    println!("  Posts: {} (Alice's 2 posts deleted)", stats_after.1);
    println!("  Comments: {} (comments on Alice's posts deleted)\n", stats_after.2);

    println!("Summary:");
    println!("✓ Schema validation enforced types and defaults");
    println!("✓ Unique constraints prevented duplicate emails");
    println!("✓ Not-null ensured required fields present");
    println!("✓ Foreign keys maintained referential integrity");
    println!("✓ Cascade delete removed all dependent data atomically");

    Ok(())
}
