use mqdb::{Database, Filter, FilterOp};
use serde_json::json;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("./data/example_db").await?;

    println!("Creating index on email field...");
    db.add_index("users".into(), vec!["email".into(), "status".into()]).await;

    println!("\nCreating users...");
    let alice = json!({
        "name": "Alice",
        "email": "alice@example.com",
        "status": "active",
        "age": 30
    });

    let bob = json!({
        "name": "Bob",
        "email": "bob@example.com",
        "status": "active",
        "age": 25
    });

    let charlie = json!({
        "name": "Charlie",
        "email": "charlie@example.com",
        "status": "inactive",
        "age": 35
    });

    let created_alice = db.create("users".into(), alice).await?;
    println!("Created: {}", created_alice);

    let created_bob = db.create("users".into(), bob).await?;
    println!("Created: {}", created_bob);

    let created_charlie = db.create("users".into(), charlie).await?;
    println!("Created: {}", created_charlie);

    println!("\nReading user...");
    let alice_id = created_alice["id"].as_str().unwrap();
    let alice_data = db.read("users".into(), alice_id.to_string(), vec![]).await?;
    println!("Read: {}", alice_data);

    println!("\nUpdating user...");
    let updates = json!({"age": 31});
    let updated_alice = db.update("users".into(), alice_id.to_string(), updates).await?;
    println!("Updated: {}", updated_alice);

    println!("\nListing all users...");
    let all_users = db.list("users".into(), vec![], vec![], None, vec![]).await?;
    println!("Total users: {}", all_users.len());
    for user in &all_users {
        println!("  - {}: {}", user["name"], user["email"]);
    }

    println!("\nFiltering active users...");
    let active_filter = Filter::new("status".into(), FilterOp::Eq, json!("active"));
    let active_users = db.list("users".into(), vec![active_filter], vec![], None, vec![]).await?;
    println!("Active users: {}", active_users.len());
    for user in &active_users {
        println!("  - {}: {}", user["name"], user["status"]);
    }

    println!("\nSetting up reactive subscription...");
    let sub_id = db.subscribe("users/#".into(), Some("users".into())).await?;
    println!("Subscription ID: {}", sub_id);

    let mut receiver = db.event_receiver();

    tokio::spawn(async move {
        while let Ok(event) = receiver.recv().await {
            println!("Event received: {:?} on {}/{}", event.operation, event.entity, event.id);
        }
    });

    println!("\nCreating new user to trigger event...");
    let david = json!({
        "name": "David",
        "email": "david@example.com",
        "status": "active"
    });
    db.create("users".into(), david).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\nDeleting user...");
    db.delete("users".into(), created_charlie["id"].as_str().unwrap().to_string()).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\nUnsubscribing...");
    db.unsubscribe(&sub_id).await?;

    println!("\nExample completed successfully!");

    Ok(())
}
