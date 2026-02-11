// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{Database, FieldDefinition, FieldType, Schema, ScopeConfig};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/schema_example").await?;

    let schema = Schema::new("users")
        .add_field(FieldDefinition::new("name", FieldType::String).required())
        .add_field(FieldDefinition::new("age", FieldType::Number))
        .add_field(FieldDefinition::new("active", FieldType::Boolean))
        .add_field(FieldDefinition::new("status", FieldType::String).with_default(json!("active")));

    db.add_schema(schema).await?;

    println!("=== Schema with Type Validation ===\n");

    println!("Creating user with valid types...");
    let user1 = json!({
        "name": "Alice",
        "age": 30,
        "active": true
    });
    let created = db
        .create("users".into(), user1, None, None, &ScopeConfig::default())
        .await?;
    println!("✓ Created user: {created}");
    println!("  Note: 'status' defaulted to 'active'\n");

    println!("Creating user with missing optional fields...");
    let user2 = json!({
        "name": "Bob"
    });
    let created = db
        .create("users".into(), user2, None, None, &ScopeConfig::default())
        .await?;
    println!("✓ Created user: {created}\n");

    println!("Attempting to create user with wrong type (age as string)...");
    let invalid_user = json!({
        "name": "Charlie",
        "age": "thirty"
    });
    match db
        .create(
            "users".into(),
            invalid_user,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Validation error: {e}\n"),
    }

    println!("Attempting to create user without required field (name)...");
    let missing_required = json!({
        "age": 25
    });
    match db
        .create(
            "users".into(),
            missing_required,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
    {
        Ok(_) => println!("✗ Should have failed!"),
        Err(e) => println!("✓ Validation error: {e}\n"),
    }

    println!("Summary:");
    println!("- Schemas enforce field types at runtime");
    println!("- Required fields must be present");
    println!("- Default values are automatically applied");
    println!("- Type mismatches are caught before insertion");

    Ok(())
}
