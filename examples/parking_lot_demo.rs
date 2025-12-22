mod parking_lot_mqtt;

use mqdb::{Database, MqdbAgent};
use mqtt5::broker::auth::PasswordAuthProvider;
use mqtt5::client::MqttClient;
use parking_lot_mqtt::{Result, now_timestamp, publish_request};
use serde_json::json;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("\n=== MQDB Parking Lot - Real MQTT Integration ===\n");

    let (agent_handle, _db_path) = start_agent().await?;

    sleep(Duration::from_millis(500)).await;

    admin_setup().await?;

    let dashboard_handle = tokio::spawn(async {
        if let Err(e) = run_dashboard().await {
            eprintln!("Dashboard error: {e}");
        }
    });

    sleep(Duration::from_millis(500)).await;

    run_parking_scenario().await?;

    sleep(Duration::from_secs(2)).await;

    dashboard_handle.abort();
    agent_handle.abort();

    println!("\n=== Parking Lot Demo Complete ===\n");

    Ok(())
}

async fn start_agent() -> Result<(tokio::task::JoinHandle<()>, tempfile::TempDir)> {
    let tmp = tempfile::TempDir::new()?;
    let db_path = tmp.path().join("parking_db");
    let db = Database::open(&db_path).await?;

    let passwd_path = tmp.path().join("passwd.txt");
    let mut passwd_content = String::new();
    for (user, pass) in [
        ("admin", "admin123"),
        ("gate", "gate123"),
        ("dashboard", "dash123"),
        ("mqdb-service", "service123"),
    ] {
        let hash = PasswordAuthProvider::hash_password(pass)?;
        use std::fmt::Write;
        let _ = writeln!(passwd_content, "{user}:{hash}");
    }
    std::fs::write(&passwd_path, passwd_content)?;

    let acl_path = tmp.path().join("acl.txt");
    std::fs::write(
        &acl_path,
        r"user mqdb-service topic # permission readwrite
user * topic +/responses/# permission readwrite
user admin topic $DB/# permission readwrite
user admin topic $DB/_admin/# permission readwrite
user gate topic $DB/vehicles/list permission write
user gate topic $DB/spots/+ permission write
user gate topic $DB/parking_sessions/+ permission write
user gate topic gate/+/responses permission readwrite
user dashboard topic $DB/+/events/# permission read
user dashboard topic gate/+/status permission read
",
    )?;

    let agent = MqdbAgent::new(db).with_bind_address("127.0.0.1:1884".parse()?);

    let handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("Agent error: {e}");
        }
    });

    Ok((handle, tmp))
}

async fn admin_setup() -> Result<()> {
    println!("📋 Admin: Setting up database schema and seed data...");

    let client = MqttClient::new("admin-setup");
    client.connect("127.0.0.1:1884").await?;

    let spots_schema = json!({
        "entity": "spots",
        "fields": {
            "spot_number": {"name": "spot_number", "field_type": "String", "required": true},
            "status": {"name": "status", "field_type": "String", "required": false, "default": "available"},
            "location": {"name": "location", "field_type": "String", "required": false}
        }
    });
    publish_request(&client, "$DB/_admin/schema/spots/set", spots_schema).await?;

    let unique_constraint = json!({"type": "unique", "fields": ["spot_number"]});
    publish_request(
        &client,
        "$DB/_admin/constraint/spots/add",
        unique_constraint,
    )
    .await?;

    for i in 1..=3 {
        let spot = json!({
            "spot_number": format!("A-{:03}", i),
            "location": format!("Floor 1, Section A, Spot {}", i),
            "status": "available"
        });
        publish_request(&client, "$DB/spots/create", spot).await?;
    }

    let vehicle = json!({
        "license_plate": "ABC-123",
        "owner": "Alice",
        "vehicle_type": "sedan"
    });
    publish_request(&client, "$DB/vehicles/create", vehicle).await?;

    println!("✅ Admin: Setup complete\n");
    Ok(())
}

async fn run_dashboard() -> Result<()> {
    let client = MqttClient::new("dashboard");
    client.connect("127.0.0.1:1884").await?;

    println!("📊 Dashboard: Monitoring events...\n");

    client
        .subscribe("$DB/spots/events/#", |msg| {
            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                let op = event["operation"].as_str().unwrap_or("unknown");
                let id = event["id"].as_str().unwrap_or("?");
                println!("📊 [SPOT] {op} - ID:{id}");
                if let Some(data) = event.get("data")
                    && let Some(status) = data.get("status")
                {
                    println!("   └─ Status: {status}");
                }
            }
        })
        .await?;

    client
        .subscribe("$DB/parking_sessions/events/#", |msg| {
            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                let op = event["operation"].as_str().unwrap_or("unknown");
                println!("📊 [SESSION] {op}");
            }
        })
        .await?;

    client
        .subscribe("gate/+/status", |msg| {
            if let Ok(status) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                let gate = msg.topic.split('/').nth(1).unwrap_or("?");
                let state = status["state"].as_str().unwrap_or("?");
                println!("📊 [GATE-{gate}] {state}");
            }
        })
        .await?;

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_parking_scenario() -> Result<()> {
    println!("🚗 === Parking Scenario: Vehicle Entry and Exit ===\n");

    let gate_client = MqttClient::new("gate-entry");
    gate_client.connect("127.0.0.1:1884").await?;

    println!("🚪 Gate: Camera detected license plate ABC-123");
    sleep(Duration::from_secs(1)).await;

    let filters = json!([{"field": "license_plate", "op": "Eq", "value": "ABC-123"}]);
    let response = publish_request(
        &gate_client,
        "$DB/vehicles/list",
        json!({"filters": filters}),
    )
    .await?;

    let vehicles = response["data"].as_array().ok_or("No vehicles data")?;
    if vehicles.is_empty() {
        return Err("Vehicle not authorized".into());
    }
    let vehicle_id = vehicles[0]["id"].as_str().ok_or("No vehicle ID")?;
    println!("✅ Gate: Vehicle authorized - ID:{vehicle_id}");

    sleep(Duration::from_millis(500)).await;

    let spot_response =
        publish_request(&gate_client, "$DB/spots/list", json!({"limit": 1})).await?;
    let spots = spot_response["data"].as_array().ok_or("No spots data")?;
    if spots.is_empty() {
        return Err("No spots available".into());
    }
    let spot = &spots[0];
    let spot_id = spot["id"].as_str().ok_or("No spot ID")?;
    let spot_number = spot["spot_number"].as_str().ok_or("No spot number")?;
    println!("🅿️  Gate: Assigned spot {spot_number}");

    sleep(Duration::from_millis(500)).await;

    gate_client
        .publish(
            "gate/entry/status",
            json!({"state": "opening"}).to_string().as_bytes(),
        )
        .await?;
    println!("🚪 Gate: Opening barrier...");

    sleep(Duration::from_secs(1)).await;

    let session = json!({
        "vehicle_id": vehicle_id,
        "spot_id": spot_id,
        "entry_time": now_timestamp(),
        "status": "active"
    });
    let session_response =
        publish_request(&gate_client, "$DB/parking_sessions/create", session).await?;
    let session_id = session_response["data"]["id"]
        .as_str()
        .ok_or("No session ID")?;
    println!("✅ Gate: Session created - ID:{session_id}");

    sleep(Duration::from_millis(500)).await;

    publish_request(
        &gate_client,
        &format!("$DB/spots/{spot_id}/update"),
        json!({"status": "occupied"}),
    )
    .await?;
    println!("🅿️  Gate: Spot marked as occupied\n");

    sleep(Duration::from_secs(2)).await;

    println!("💳 === Vehicle Exit ===\n");
    println!("🚪 Exit Gate: Scanner detected receipt");

    sleep(Duration::from_millis(500)).await;

    publish_request(
        &gate_client,
        &format!("$DB/parking_sessions/{session_id}/update"),
        json!({"exit_time": now_timestamp(), "status": "completed"}),
    )
    .await?;
    println!("✅ Exit Gate: Session completed");

    sleep(Duration::from_millis(500)).await;

    publish_request(
        &gate_client,
        &format!("$DB/spots/{spot_id}/update"),
        json!({"status": "available"}),
    )
    .await?;
    println!("🅿️  Exit Gate: Spot marked as available");

    gate_client
        .publish(
            "gate/exit/status",
            json!({"state": "opening"}).to_string().as_bytes(),
        )
        .await?;
    println!("🚪 Exit Gate: Opening barrier...");
    println!("✅ Exit Gate: Vehicle departed\n");

    Ok(())
}
