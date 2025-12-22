use mqdb::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let tmp = tempfile::TempDir::new()?;
    let db = Database::open(tmp.path()).await?;

    let bind_addr: SocketAddr = "127.0.0.1:1884".parse()?;
    let agent = Arc::new(MqdbAgent::new(db).with_bind_address(bind_addr));

    let agent_handle = {
        let agent = Arc::clone(&agent);
        tokio::spawn(async move {
            if let Err(e) = agent.run().await {
                tracing::error!("Agent error: {}", e);
            }
        })
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let client = MqttClient::new("demo-client");
    client.connect("127.0.0.1:1884").await?;
    info!("Client connected to MQDB Agent");

    let (event_tx, mut event_rx) = mpsc::channel::<String>(32);
    let callback_tx = event_tx.clone();
    client
        .subscribe("$DB/users/events/#", move |msg| {
            let event_info = format!(
                "Event on {}: {}",
                msg.topic,
                String::from_utf8_lossy(&msg.payload)
            );
            let _ = callback_tx.try_send(event_info);
        })
        .await?;
    info!("Subscribed to $DB/users/events/#");

    let (response_tx, mut response_rx) = mpsc::channel::<String>(32);
    let callback_tx = response_tx.clone();
    client
        .subscribe("client/responses", move |msg| {
            let _ = callback_tx.try_send(String::from_utf8_lossy(&msg.payload).to_string());
        })
        .await?;

    info!("\n=== Creating users ===");

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some("client/responses".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let payload = json!({"name": "Alice", "email": "alice@example.com", "age": 30});
    client
        .publish_with_options(
            "$DB/users/create",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    if let Some(response) = response_rx.recv().await {
        info!("Create Alice response: {}", response);
    }

    let payload = json!({"name": "Bob", "email": "bob@example.com", "age": 25});
    client
        .publish_with_options(
            "$DB/users/create",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    if let Some(response) = response_rx.recv().await {
        info!("Create Bob response: {}", response);
    }

    let payload = json!({"name": "Charlie", "email": "charlie@example.com", "age": 35});
    client
        .publish_with_options(
            "$DB/users/create",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    let charlie_id = if let Some(response) = response_rx.recv().await {
        info!("Create Charlie response: {}", response);
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        parsed["data"]["id"].as_str().unwrap_or("").to_string()
    } else {
        String::new()
    };

    info!("\n=== Listing all users ===");

    let payload = json!({});
    client
        .publish_with_options(
            "$DB/users/list",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    if let Some(response) = response_rx.recv().await {
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        if let Some(users) = parsed["data"].as_array() {
            info!("Found {} users:", users.len());
            for user in users {
                info!(
                    "  - {} ({})",
                    user["name"].as_str().unwrap_or("?"),
                    user["email"].as_str().unwrap_or("?")
                );
            }
        }
    }

    info!("\n=== Reading Charlie by ID ===");

    if !charlie_id.is_empty() {
        let topic = format!("$DB/users/{charlie_id}");
        client
            .publish_with_options(&topic, vec![], opts.clone())
            .await?;

        if let Some(response) = response_rx.recv().await {
            info!("Read Charlie: {}", response);
        }
    }

    info!("\n=== Updating Charlie's age ===");

    if !charlie_id.is_empty() {
        let topic = format!("$DB/users/{charlie_id}/update");
        let payload = json!({"age": 36});
        client
            .publish_with_options(&topic, serde_json::to_vec(&payload)?, opts.clone())
            .await?;

        if let Some(response) = response_rx.recv().await {
            info!("Update response: {}", response);
        }
    }

    info!("\n=== Listing with filter (age > 28) ===");

    let payload = json!({
        "filters": [{"field": "age", "op": "gt", "value": 28}]
    });
    client
        .publish_with_options(
            "$DB/users/list",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    if let Some(response) = response_rx.recv().await {
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        if let Some(users) = parsed["data"].as_array() {
            info!("Users older than 28: {}", users.len());
            for user in users {
                info!(
                    "  - {} (age {})",
                    user["name"].as_str().unwrap_or("?"),
                    user["age"]
                );
            }
        }
    }

    info!("\n=== Deleting Charlie ===");

    if !charlie_id.is_empty() {
        let topic = format!("$DB/users/{charlie_id}/delete");
        client
            .publish_with_options(&topic, vec![], opts.clone())
            .await?;

        if let Some(response) = response_rx.recv().await {
            info!("Delete response: {}", response);
        }
    }

    info!("\n=== Events received ===");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    while let Ok(event) = event_rx.try_recv() {
        info!("{}", event);
    }

    info!("\n=== Demo complete ===");

    agent.shutdown();
    let _ = agent_handle.await;

    Ok(())
}
