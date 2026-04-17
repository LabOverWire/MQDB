use mqtt5::client::MqttClient;
use serde_json::{Value, json};

use super::store::Store;

pub async fn handle_create(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    payload: &[u8],
    response_topic: &str,
) {
    let (response, emitted_id) = create_impl(store, entity, payload).await;
    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
    if let Some(id) = emitted_id {
        emit_event(client, entity, &id, "create", response.get("data")).await;
    }
}

async fn create_impl(store: &Store, entity: &str, payload: &[u8]) -> (Value, Option<String>) {
    let data: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            return (
                json!({"status": "error", "code": 400, "message": e.to_string()}),
                None,
            );
        }
    };

    let mut record = match data {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    let id = record
        .get("id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
    record.insert("id".to_string(), json!(id));
    let full_record = Value::Object(record);

    match store.insert_record(&id, entity, &full_record).await {
        Ok(()) => (
            json!({"status": "ok", "data": full_record}),
            Some(id),
        ),
        Err(e) => (
            json!({"status": "error", "code": 500, "message": e.to_string()}),
            None,
        ),
    }
}

pub async fn handle_get(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    id: &str,
    response_topic: &str,
) {
    let response = match store.get_record(entity, id).await {
        Ok(Some(data)) => json!({"status": "ok", "data": data}),
        Ok(None) => json!({"status": "error", "code": 404, "message": "not found"}),
        Err(e) => json!({"status": "error", "code": 500, "message": e.to_string()}),
    };

    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
}

pub async fn handle_update(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    id: &str,
    payload: &[u8],
    response_topic: &str,
) {
    let (response, emitted) = update_impl(store, entity, id, payload).await;
    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
    if emitted {
        emit_event(client, entity, id, "update", response.get("data")).await;
    }
}

async fn update_impl(store: &Store, entity: &str, id: &str, payload: &[u8]) -> (Value, bool) {
    let data: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            return (
                json!({"status": "error", "code": 400, "message": e.to_string()}),
                false,
            );
        }
    };

    let mut record = match data {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    record.insert("id".to_string(), json!(id));
    let full_record = Value::Object(record);

    match store.update_record(entity, id, &full_record).await {
        Ok(true) => (json!({"status": "ok", "data": full_record}), true),
        Ok(false) => (
            json!({"status": "error", "code": 404, "message": "not found"}),
            false,
        ),
        Err(e) => (
            json!({"status": "error", "code": 500, "message": e.to_string()}),
            false,
        ),
    }
}

pub async fn handle_delete(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    id: &str,
    response_topic: &str,
) {
    let cascaded = match store.delete_with_cascade(entity, id).await {
        Ok(v) => v,
        Err(e) => {
            let response = json!({"status": "error", "code": 500, "message": e.to_string()});
            let _ = client
                .publish(
                    response_topic,
                    serde_json::to_vec(&response).unwrap_or_default(),
                )
                .await;
            return;
        }
    };

    if cascaded.is_empty() {
        let response = json!({"status": "error", "code": 404, "message": "not found"});
        let _ = client
            .publish(
                response_topic,
                serde_json::to_vec(&response).unwrap_or_default(),
            )
            .await;
        return;
    }

    let response = json!({
        "status": "ok",
        "data": {"id": id, "deleted": true, "cascaded": cascaded.len().saturating_sub(1)}
    });
    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;

    for (child_entity, child_id) in cascaded {
        emit_event(client, &child_entity, &child_id, "delete", None).await;
    }
}

#[allow(clippy::cast_possible_wrap)]
pub async fn handle_list(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    payload: &[u8],
    response_topic: &str,
) {
    let limit = serde_json::from_slice::<Value>(payload)
        .ok()
        .and_then(|v| v.get("limit").and_then(Value::as_i64))
        .unwrap_or(100);

    let response = match store.list_records(entity, limit).await {
        Ok(records) => json!({"status": "ok", "data": records}),
        Err(e) => json!({"status": "error", "code": 500, "message": e.to_string()}),
    };

    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
}

pub async fn handle_health(client: &MqttClient, response_topic: &str) {
    let response = json!({
        "status": "ok",
        "data": {
            "status": "healthy",
            "ready": true,
            "mode": "baseline-bridge"
        }
    });

    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
}

pub async fn handle_constraint(
    client: &MqttClient,
    store: &Store,
    entity: &str,
    payload: &[u8],
    response_topic: &str,
) {
    let response = constraint_impl(store, entity, payload).await;
    let _ = client
        .publish(response_topic, serde_json::to_vec(&response).unwrap_or_default())
        .await;
}

async fn constraint_impl(store: &Store, entity: &str, payload: &[u8]) -> Value {
    let parsed: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => return json!({"status": "error", "code": 400, "message": e.to_string()}),
    };
    let ctype = parsed.get("type").and_then(Value::as_str).unwrap_or("");
    match ctype {
        "unique" => {
            let fields = parsed
                .get("fields")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(ToString::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            match store.add_unique_constraint(entity, &fields).await {
                Ok(()) => json!({"status": "ok", "data": {"type": "unique", "entity": entity, "fields": fields}}),
                Err(e) => json!({"status": "error", "code": 500, "message": e.to_string()}),
            }
        }
        "foreign_key" | "fk" => {
            let field = parsed
                .get("field")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let target_entity = parsed
                .get("target_entity")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let target_field = parsed
                .get("target_field")
                .and_then(Value::as_str)
                .unwrap_or("id")
                .to_string();
            let on_delete = parsed
                .get("on_delete")
                .and_then(Value::as_str)
                .unwrap_or("restrict")
                .to_string();
            match store
                .add_fk_constraint(entity, &field, &target_entity, &target_field, &on_delete)
                .await
            {
                Ok(()) => json!({"status": "ok", "data": {
                    "type": "foreign_key",
                    "entity": entity,
                    "field": field,
                    "target_entity": target_entity,
                    "target_field": target_field,
                    "on_delete": on_delete,
                }}),
                Err(e) => json!({"status": "error", "code": 500, "message": e.to_string()}),
            }
        }
        _ => json!({"status": "error", "code": 400, "message": "unsupported constraint type"}),
    }
}

async fn emit_event(
    client: &MqttClient,
    entity: &str,
    id: &str,
    operation: &str,
    data: Option<&Value>,
) {
    let topic = format!("$DB/{entity}/events/{id}");
    let mut payload = serde_json::Map::new();
    payload.insert("id".to_string(), json!(id));
    payload.insert("entity".to_string(), json!(entity));
    payload.insert("operation".to_string(), json!(operation));
    if let Some(d) = data {
        payload.insert("data".to_string(), d.clone());
    }
    let body = serde_json::to_vec(&Value::Object(payload)).unwrap_or_default();
    let _ = client.publish(&topic, body).await;
}
