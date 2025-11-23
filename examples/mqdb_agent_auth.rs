use mqdb::{Database, MqdbAgent};
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
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

    let agent = Arc::new(
        MqdbAgent::new(db)
            .with_bind_address(bind_addr)
            .with_password_file("examples/passwd.txt".into())
            .with_acl_file("examples/acl.txt".into())
            .with_anonymous(false)
            .with_service_credentials("admin".to_string(), "admin123".to_string()),
    );

    let agent_handle = {
        let agent = Arc::clone(&agent);
        tokio::spawn(async move {
            if let Err(e) = agent.run().await {
                tracing::error!("Agent error: {}", e);
            }
        })
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    info!("\n=== Testing as 'admin' (full access) ===");
    test_user("admin", &["users", "orders"]).await?;

    info!("\n=== Testing as 'alice' (users only) ===");
    test_user("alice", &["users"]).await?;

    info!("\n=== Testing as 'bob' (read-only) ===");
    test_read_only_user("bob").await?;

    info!("\n=== Demo complete ===");

    agent.shutdown();
    let _ = agent_handle.await;

    Ok(())
}

async fn test_user(
    username: &str,
    allowed_entities: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::new(format!("{}-client", username));

    let options = ConnectOptions::new(format!("{}-client", username))
        .with_credentials(username, format!("{}123", username));

    client
        .connect_with_options("127.0.0.1:1884", options)
        .await?;
    info!("{} connected successfully", username);

    let (response_tx, mut response_rx) = mpsc::channel::<String>(32);
    let callback_tx = response_tx.clone();
    client
        .subscribe(&format!("{}/responses", username), move |msg| {
            let _ = callback_tx.try_send(String::from_utf8_lossy(&msg.payload).to_string());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(format!("{}/responses", username)),
            ..Default::default()
        },
        ..Default::default()
    };

    for entity in allowed_entities {
        let payload = json!({"name": format!("Test from {}", username), "value": 42});
        client
            .publish_with_options(
                &format!("$DB/{}/create", entity),
                serde_json::to_vec(&payload)?,
                opts.clone(),
            )
            .await?;

        if let Some(response) = response_rx.recv().await {
            let parsed: serde_json::Value = serde_json::from_str(&response)?;
            let status = parsed["status"].as_str().unwrap_or("unknown");
            info!("{} -> $DB/{}/create: {}", username, entity, status);
        }
    }

    client.disconnect().await?;
    Ok(())
}

async fn test_read_only_user(username: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::new(format!("{}-client", username));

    let options = ConnectOptions::new(format!("{}-client", username))
        .with_credentials(username, format!("{}123", username));

    client
        .connect_with_options("127.0.0.1:1884", options)
        .await?;
    info!("{} connected successfully", username);

    let (response_tx, mut response_rx) = mpsc::channel::<String>(32);
    let callback_tx = response_tx.clone();
    client
        .subscribe(&format!("{}/responses", username), move |msg| {
            let _ = callback_tx.try_send(String::from_utf8_lossy(&msg.payload).to_string());
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(format!("{}/responses", username)),
            ..Default::default()
        },
        ..Default::default()
    };

    client
        .publish_with_options(
            "$DB/users/list",
            serde_json::to_vec(&json!({}))?,
            opts.clone(),
        )
        .await?;

    if let Some(response) = response_rx.recv().await {
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        let status = parsed["status"].as_str().unwrap_or("unknown");
        info!("{} -> $DB/users/list: {}", username, status);
    }

    let payload = json!({"name": "Should fail", "value": 0});
    client
        .publish_with_options(
            "$DB/users/create",
            serde_json::to_vec(&payload)?,
            opts.clone(),
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    if let Ok(response) = response_rx.try_recv() {
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        let status = parsed["status"].as_str().unwrap_or("unknown");
        info!(
            "{} -> $DB/users/create: {} (expected: denied)",
            username, status
        );
    } else {
        info!("{} -> $DB/users/create: denied (no response)", username);
    }

    client.disconnect().await?;
    Ok(())
}
