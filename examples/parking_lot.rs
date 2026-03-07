// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::{
    Database, DatabaseConfig, FieldDefinition, FieldType, Filter, FilterOp, OnDeleteAction, Schema,
    ScopeConfig,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};

const DATA_PATH: &str = "./data/parking_lot";

fn now_timestamp() -> i64 {
    #[allow(clippy::cast_possible_wrap)]
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
    ts
}

#[allow(clippy::too_many_lines)]
async fn backend_setup() -> Result<Arc<Database>, Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     BACKEND SETUP: MQTT Broker + Database (ONE-TIME)        ║");
    println!("║     This runs once when the broker/db service starts        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let _ = std::fs::remove_dir_all(DATA_PATH);

    let config = DatabaseConfig::new(DATA_PATH).with_ttl_cleanup_interval(Some(60));
    let db = Arc::new(Database::open_with_config(config).await?);

    println!("Creating schemas...");
    let spots_schema = Schema::new("spots")
        .add_field(FieldDefinition::new("spot_number", FieldType::String).required())
        .add_field(FieldDefinition::new("spot_type", FieldType::String).required())
        .add_field(FieldDefinition::new("location", FieldType::String).required())
        .add_field(
            FieldDefinition::new("status", FieldType::String).with_default(json!("available")),
        )
        .add_field(FieldDefinition::new("gate_device_id", FieldType::String).required());

    let vehicles_schema = Schema::new("vehicles")
        .add_field(FieldDefinition::new("license_plate", FieldType::String).required())
        .add_field(FieldDefinition::new("vehicle_type", FieldType::String).required())
        .add_field(FieldDefinition::new("owner_name", FieldType::String))
        .add_field(FieldDefinition::new("phone", FieldType::String));

    let reservations_schema = Schema::new("reservations")
        .add_field(FieldDefinition::new("vehicle_id", FieldType::String).required())
        .add_field(FieldDefinition::new("spot_id", FieldType::String).required())
        .add_field(FieldDefinition::new("reserved_at", FieldType::Number).required())
        .add_field(
            FieldDefinition::new("status", FieldType::String).with_default(json!("pending")),
        );

    let sessions_schema = Schema::new("parking_sessions")
        .add_field(FieldDefinition::new("vehicle_id", FieldType::String).required())
        .add_field(FieldDefinition::new("spot_id", FieldType::String).required())
        .add_field(FieldDefinition::new("entry_time", FieldType::Number).required())
        .add_field(
            FieldDefinition::new("session_status", FieldType::String)
                .with_default(json!("in_progress")),
        );

    let payments_schema = Schema::new("payments")
        .add_field(FieldDefinition::new("session_id", FieldType::String).required())
        .add_field(FieldDefinition::new("amount", FieldType::Number).required())
        .add_field(
            FieldDefinition::new("payment_status", FieldType::String)
                .with_default(json!("pending")),
        )
        .add_field(FieldDefinition::new("receipt_code", FieldType::String).required());

    db.add_schema(spots_schema).await?;
    db.add_schema(vehicles_schema).await?;
    db.add_schema(reservations_schema).await?;
    db.add_schema(sessions_schema).await?;
    db.add_schema(payments_schema).await?;

    println!("Creating constraints...");
    db.add_unique_constraint("spots".into(), vec!["spot_number".into()])
        .await?;
    db.add_unique_constraint("vehicles".into(), vec!["license_plate".into()])
        .await?;
    db.add_unique_constraint("payments".into(), vec!["receipt_code".into()])
        .await?;

    db.add_foreign_key(
        "reservations".into(),
        "vehicle_id".into(),
        "vehicles".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await?;

    db.add_foreign_key(
        "reservations".into(),
        "spot_id".into(),
        "spots".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await?;

    db.add_foreign_key(
        "parking_sessions".into(),
        "vehicle_id".into(),
        "vehicles".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await?;

    db.add_foreign_key(
        "parking_sessions".into(),
        "spot_id".into(),
        "spots".into(),
        "id".into(),
        OnDeleteAction::Restrict,
    )
    .await?;

    db.add_foreign_key(
        "payments".into(),
        "session_id".into(),
        "parking_sessions".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await?;

    println!("Creating indexes...");
    db.add_index("spots".into(), vec!["status".into(), "spot_type".into()])
        .await?;
    db.add_index("vehicles".into(), vec!["license_plate".into()])
        .await?;
    db.add_index(
        "parking_sessions".into(),
        vec!["session_status".into(), "spot_id".into()],
    )
    .await?;
    db.add_index("payments".into(), vec!["receipt_code".into()])
        .await?;

    println!("Seeding parking spots...");
    for i in 1..=5 {
        db.create(
            "spots".into(),
            json!({
                "spot_number": format!("A-{:03}", i),
                "spot_type": "small",
                "location": format!("Floor 1, Section A, Spot {}", i),
                "gate_device_id": format!("gate_a{}", i)
            }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;
    }

    for i in 1..=3 {
        db.create(
            "spots".into(),
            json!({
                "spot_number": format!("B-{:03}", i),
                "spot_type": "large",
                "location": format!("Floor 2, Section B, Spot {}", i),
                "gate_device_id": format!("gate_b{}", i)
            }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;
    }

    db.create(
        "vehicles".into(),
        json!({
            "license_plate": "ABC-123",
            "vehicle_type": "small",
            "owner_name": "Alice Johnson"
        }),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;

    println!("✅ Backend setup complete\n");

    Ok(db)
}

fn mqtt_publish(topic: &str, payload: &serde_json::Value) {
    println!("📤 mqtt.publish('{topic}', {payload})\n");
}

fn mqtt_subscribe(topic: &str) {
    println!("📡 mqtt.subscribe('{topic}')\n");
}

fn mqtt_event(topic: &str, payload: &serde_json::Value) {
    println!("⚡ mqtt.on('{topic}'): {payload}\n");
}

fn mqtt_response(correlation_id: &str, result: &serde_json::Value) {
    println!(
        "📥 Response [{correlation_id}]: {}\n",
        serde_json::to_string_pretty(result).unwrap()
    );
}

#[allow(clippy::too_many_lines)]
async fn kiosk_client(db: Arc<Database>) -> Result<(String, String), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     KIOSK CLIENT: Driver Requests Parking Spot              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("Driver selects 'small' spot type\n");

    mqtt_publish(
        "db/spots/list",
        &json!({
            "op": "list",
            "entity": "spots",
            "data": {
                "filters": [
                    {"field": "status", "op": "eq", "value": "available"},
                    {"field": "spot_type", "op": "eq", "value": "small"}
                ],
                "pagination": {"limit": 1, "offset": 0}
            },
            "reply_to": "kiosk/response",
            "correlation_id": "req-1"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    let spots = db
        .list(
            "spots".into(),
            vec![
                Filter::new("status".into(), FilterOp::Eq, json!("available")),
                Filter::new("spot_type".into(), FilterOp::Eq, json!("small")),
            ],
            vec![],
            Some(mqdb::Pagination::new(1, 0)),
            vec![],
            None,
        )
        .await?;

    let spot = &spots[0];
    let spot_id = spot["id"].as_str().unwrap().to_string();

    mqtt_response(
        "req-1",
        &json!({
            "status": "ok",
            "result": {"items": spots, "total": 5}
        }),
    );

    let vehicles = db
        .list(
            "vehicles".into(),
            vec![Filter::new(
                "license_plate".into(),
                FilterOp::Eq,
                json!("ABC-123"),
            )],
            vec![],
            None,
            vec![],
            None,
        )
        .await?;
    let vehicle_id = vehicles[0]["id"].as_str().unwrap();

    mqtt_publish(
        "db/reservations/create",
        &json!({
            "op": "create",
            "entity": "reservations",
            "data": {
                "vehicle_id": vehicle_id,
                "spot_id": &spot_id,
                "reserved_at": now_timestamp(),
                "ttl_secs": 900
            },
            "reply_to": "kiosk/response",
            "correlation_id": "req-2"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    let reservation = db
        .create(
            "reservations".into(),
            json!({
                "vehicle_id": vehicle_id,
                "spot_id": &spot_id,
                "reserved_at": now_timestamp(),
                "ttl_secs": 900
            }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;

    let reservation_id = reservation["id"].as_str().unwrap().to_string();

    mqtt_response(
        "req-2",
        &json!({
            "status": "ok",
            "result": reservation
        }),
    );

    mqtt_publish(
        "db/spots/update",
        &json!({
            "op": "update",
            "entity": "spots",
            "data": {
                "id": &spot_id,
                "fields": {"status": "reserved"}
            },
            "reply_to": "kiosk/response",
            "correlation_id": "req-3"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    db.update(
        "spots".into(),
        spot_id.clone(),
        json!({"status": "reserved"}),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;

    mqtt_response("req-3", &json!({"status": "ok"}));

    println!(
        "🖥️  Display: 'Proceed to {} at {}'\n",
        spot["spot_number"], spot["location"]
    );

    Ok((spot_id, reservation_id))
}

#[allow(clippy::too_many_lines)]
async fn gate_camera_client(
    db: Arc<Database>,
    spot_id: String,
    reservation_id: String,
) -> Result<String, Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     ENTRY GATE: Camera Detection + Gate Controller          ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("--- IoT Device: Camera (gate_a1) ---\n");
    println!("📷 Camera detects license plate: ABC-123\n");

    mqtt_publish(
        "camera/a1/detection",
        &json!({
            "license_plate": "ABC-123",
            "timestamp": now_timestamp(),
            "confidence": 0.98
        }),
    );

    sleep(Duration::from_millis(200)).await;

    println!("--- IoT Device: Gate Controller (gate_a1) ---\n");

    mqtt_event(
        "camera/a1/detection",
        &json!({"license_plate": "ABC-123", "confidence": 0.98}),
    );

    println!("Gate controller validates plate against DB...\n");

    mqtt_publish(
        "db/vehicles/list",
        &json!({
            "op": "list",
            "entity": "vehicles",
            "data": {
                "filters": [{"field": "license_plate", "op": "eq", "value": "ABC-123"}]
            },
            "reply_to": "gate/a1/response",
            "correlation_id": "gate-1"
        }),
    );

    sleep(Duration::from_millis(200)).await;

    let vehicles = db
        .list(
            "vehicles".into(),
            vec![Filter::new(
                "license_plate".into(),
                FilterOp::Eq,
                json!("ABC-123"),
            )],
            vec![],
            None,
            vec![],
            None,
        )
        .await?;

    let vehicle_id = vehicles[0]["id"].as_str().unwrap();

    mqtt_response(
        "gate-1",
        &json!({"status": "ok", "result": {"items": vehicles}}),
    );

    println!("✅ Vehicle validated, opening gate...\n");

    mqtt_publish(
        "gate/a1/status",
        &json!({
            "state": "opening",
            "reason": "vehicle_entry",
            "license_plate": "ABC-123"
        }),
    );

    sleep(Duration::from_millis(200)).await;

    mqtt_publish(
        "db/parking_sessions/create",
        &json!({
            "op": "create",
            "entity": "parking_sessions",
            "data": {
                "reservation_id": &reservation_id,
                "vehicle_id": vehicle_id,
                "spot_id": &spot_id,
                "entry_time": now_timestamp()
            },
            "reply_to": "gate/a1/response",
            "correlation_id": "gate-2"
        }),
    );

    sleep(Duration::from_millis(200)).await;

    let session = db
        .create(
            "parking_sessions".into(),
            json!({
                "reservation_id": &reservation_id,
                "vehicle_id": vehicle_id,
                "spot_id": &spot_id,
                "entry_time": now_timestamp()
            }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;

    let session_id = session["id"].as_str().unwrap().to_string();

    mqtt_response("gate-2", &json!({"status": "ok", "result": session}));

    mqtt_publish(
        "db/spots/update",
        &json!({
            "op": "update",
            "entity": "spots",
            "data": {"id": &spot_id, "fields": {"status": "occupied"}}
        }),
    );

    db.update(
        "spots".into(),
        spot_id,
        json!({"status": "occupied"}),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;

    mqtt_publish(
        "gate/a1/status",
        &json!({
            "state": "closed",
            "reason": "vehicle_entered"
        }),
    );

    println!("🚦 Gate opened → vehicle entered → gate closed\n");

    Ok(session_id)
}

async fn payment_booth_client(
    db: Arc<Database>,
    session_id: String,
) -> Result<String, Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     PAYMENT BOOTH CLIENT: Driver Pays                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("Driver scans license plate at payment booth\n");

    mqtt_publish(
        "db/parking_sessions/read",
        &json!({
            "op": "read",
            "entity": "parking_sessions",
            "data": {"id": &session_id},
            "reply_to": "booth/response",
            "correlation_id": "pay-1"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    let session = db
        .read("parking_sessions".into(), session_id.clone(), vec![], None)
        .await?;

    let entry_time = session["entry_time"].as_i64().unwrap();
    let duration = (now_timestamp() - entry_time) / 3600;
    #[allow(clippy::cast_precision_loss)]
    let amount = (duration.max(1) * 5) as f64;

    mqtt_response("pay-1", &json!({"status": "ok", "result": session}));

    println!(
        "💰 Fee calculated: ${} ({} hours)\n",
        amount,
        duration.max(1)
    );

    let receipt_code = format!("RCP-{}", now_timestamp());

    mqtt_publish(
        "db/payments/create",
        &json!({
            "op": "create",
            "entity": "payments",
            "data": {
                "session_id": &session_id,
                "amount": amount,
                "receipt_code": &receipt_code
            },
            "reply_to": "booth/response",
            "correlation_id": "pay-2"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    let payment = db
        .create(
            "payments".into(),
            json!({
                "session_id": &session_id,
                "amount": amount,
                "receipt_code": &receipt_code,
                "payment_status": "completed",
                "paid_at": now_timestamp()
            }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await?;

    mqtt_response("pay-2", &json!({"status": "ok", "result": payment}));

    println!("🧾 Receipt printed: {receipt_code}\n");

    Ok(receipt_code)
}

#[allow(clippy::too_many_lines)]
async fn exit_gate_client(
    db: Arc<Database>,
    receipt_code: String,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     EXIT GATE: Receipt Scanner + Gate Controller            ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("--- IoT Device: Receipt Scanner (exit_b1) ---\n");
    println!("Driver scans receipt: {receipt_code}\n");

    mqtt_publish(
        "scanner/b1/scan",
        &json!({
            "receipt_code": &receipt_code,
            "timestamp": now_timestamp()
        }),
    );

    sleep(Duration::from_millis(200)).await;

    println!("--- IoT Device: Exit Gate Controller (exit_b1) ---\n");

    mqtt_event("scanner/b1/scan", &json!({"receipt_code": &receipt_code}));

    println!("Gate controller validates receipt against DB...\n");

    mqtt_publish(
        "db/payments/list",
        &json!({
            "op": "list",
            "entity": "payments",
            "data": {
                "filters": [
                    {"field": "receipt_code", "op": "eq", "value": &receipt_code},
                    {"field": "payment_status", "op": "eq", "value": "completed"}
                ]
            },
            "reply_to": "exit/response",
            "correlation_id": "exit-1"
        }),
    );

    sleep(Duration::from_millis(300)).await;

    let payments = db
        .list(
            "payments".into(),
            vec![
                Filter::new("receipt_code".into(), FilterOp::Eq, json!(receipt_code)),
                Filter::new("payment_status".into(), FilterOp::Eq, json!("completed")),
            ],
            vec![],
            None,
            vec![],
            None,
        )
        .await?;

    let payment = &payments[0];
    let session_id = payment["session_id"].as_str().unwrap();

    mqtt_response(
        "exit-1",
        &json!({"status": "ok", "result": {"items": payments}}),
    );

    let session = db
        .read(
            "parking_sessions".into(),
            session_id.to_string(),
            vec![],
            None,
        )
        .await?;

    let spot_id = session["spot_id"].as_str().unwrap();

    println!("✅ Payment validated, opening exit gate...\n");

    mqtt_publish(
        "gate/b1/status",
        &json!({
            "state": "opening",
            "reason": "vehicle_exit",
            "receipt_code": &receipt_code
        }),
    );

    mqtt_publish(
        "db/parking_sessions/update",
        &json!({
            "op": "update",
            "entity": "parking_sessions",
            "data": {
                "id": session_id,
                "fields": {
                    "exit_time": now_timestamp(),
                    "session_status": "completed"
                }
            }
        }),
    );

    db.update(
        "parking_sessions".into(),
        session_id.to_string(),
        json!({
            "exit_time": now_timestamp(),
            "session_status": "completed"
        }),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;

    mqtt_publish(
        "db/spots/update",
        &json!({
            "op": "update",
            "entity": "spots",
            "data": {"id": spot_id, "fields": {"status": "available"}}
        }),
    );

    db.update(
        "spots".into(),
        spot_id.to_string(),
        json!({"status": "available"}),
        None,
        None,
        None,
        &ScopeConfig::default(),
    )
    .await?;

    mqtt_publish(
        "gate/b1/status",
        &json!({
            "state": "closed",
            "reason": "vehicle_exited"
        }),
    );

    println!("🚦 Exit gate opened → vehicle exited → gate closed\n");
    println!("✅ Spot {spot_id} now available\n");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = backend_setup().await?;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     MONITORING DASHBOARD: Subscribes to IoT Events          ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("Dashboard subscribes to ephemeral IoT topics (no persistence needed):\n");
    mqtt_subscribe("gate/+/status");
    mqtt_subscribe("camera/+/detection");
    mqtt_subscribe("scanner/+/scan");

    println!("These are real-time device events - no DB storage required.\n");

    let (spot_id, reservation_id) = kiosk_client(db.clone()).await?;

    let session_id = gate_camera_client(db.clone(), spot_id, reservation_id).await?;

    let receipt_code = payment_booth_client(db.clone(), session_id).await?;

    exit_gate_client(db.clone(), receipt_code).await?;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Example Complete                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("TWO COMMUNICATION PATTERNS DEMONSTRATED:\n");

    println!("1. PERSISTENT (db/* topics):");
    println!("   - db/spots/list, db/payments/create, etc.");
    println!("   - Request/response pattern with reply_to");
    println!("   - Data stored in database\n");

    println!("2. EPHEMERAL (device topics):");
    println!("   - camera/a1/detection, gate/a1/status, scanner/b1/scan");
    println!("   - Real-time IoT device communication");
    println!("   - No persistence needed\n");

    println!("BENEFIT:");
    println!("  Traditional REST: request → response (polling for updates)");
    println!("  MQTT/DB approach: request → response + real-time events\n");

    println!("  Dashboard sees gate/a1/status events without polling,");
    println!("  while kiosk still gets db/spots/list responses.\n");

    Ok(())
}
