use mqdb::Database;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/unique_example").await?;

    db.add_unique_constraint("users".into(), vec!["email".into()])
        .await?;

    println!("=== Unique Constraints ===\n");

    println!("Creating user with email alice@example.com...");
    let user1 = json!({
        "name": "Alice",
        "email": "alice@example.com"
    });
    let created = db.create("users".into(), user1).await?;
    println!("✓ Created: {created}\n");

    println!("Creating another user with different email...");
    let user2 = json!({
        "name": "Bob",
        "email": "bob@example.com"
    });
    let created = db.create("users".into(), user2).await?;
    println!("✓ Created: {created}\n");

    println!("Attempting to create user with duplicate email...");
    let duplicate = json!({
        "name": "Charlie",
        "email": "alice@example.com"
    });
    match db.create("users".into(), duplicate).await {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Unique constraint violation: {e}\n"),
    }

    println!("=== Composite Unique Constraints ===\n");

    db.add_unique_constraint("posts".into(), vec!["user_id".into(), "slug".into()])
        .await?;

    println!("Creating post with user_id=1, slug='hello'...");
    let post1 = json!({
        "user_id": "1",
        "slug": "hello",
        "title": "Hello World"
    });
    db.create("posts".into(), post1).await?;
    println!("✓ Created\n");

    println!("Creating post with same slug but different user_id...");
    let post2 = json!({
        "user_id": "2",
        "slug": "hello",
        "title": "Hello from Bob"
    });
    db.create("posts".into(), post2).await?;
    println!("✓ Created (different user_id, so allowed)\n");

    println!("Attempting to create duplicate (user_id=1, slug='hello')...");
    let duplicate = json!({
        "user_id": "1",
        "slug": "hello",
        "title": "Duplicate"
    });
    match db.create("posts".into(), duplicate).await {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Unique constraint violation: {e}\n"),
    }

    println!("Summary:");
    println!("- Unique constraints prevent duplicate values");
    println!("- Can be single field or composite (multiple fields)");
    println!("- Automatically creates an index for performance");

    Ok(())
}
