use mqdb::{Database, ScopeConfig};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/not_null_example").await?;

    db.add_not_null("users".into(), "email".into()).await?;

    println!("=== Not-Null Constraints ===\n");

    println!("Creating user with email present...");
    let user1 = json!({
        "name": "Alice",
        "email": "alice@example.com"
    });
    let created = db
        .create("users".into(), user1, None, &ScopeConfig::default())
        .await?;
    println!("✓ Created: {created}\n");

    println!("Attempting to create user with missing email field...");
    let missing_field = json!({
        "name": "Bob"
    });
    match db
        .create("users".into(), missing_field, None, &ScopeConfig::default())
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Not-null constraint violation: {e}\n"),
    }

    println!("Attempting to create user with explicit null email...");
    let null_email = json!({
        "name": "Charlie",
        "email": null
    });
    match db
        .create("users".into(), null_email, None, &ScopeConfig::default())
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Not-null constraint violation: {e}\n"),
    }

    println!("Creating valid user for update test...");
    let user = json!({
        "name": "David",
        "email": "david@example.com"
    });
    let created = db
        .create("users".into(), user, None, &ScopeConfig::default())
        .await?;
    let user_id = created["id"].as_str().unwrap();
    println!("✓ Created user: {user_id}\n");

    println!("Attempting to update email to null...");
    let update = json!({
        "name": "David",
        "email": null
    });
    match db
        .update(
            "users".into(),
            user_id.to_string(),
            update,
            None,
            &ScopeConfig::default(),
        )
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Not-null constraint violation: {e}\n"),
    }

    println!("Summary:");
    println!("- Not-null constraints ensure critical fields are always present");
    println!("- Prevents both missing fields and explicit null values");
    println!("- Enforced on both create and update operations");

    Ok(())
}
