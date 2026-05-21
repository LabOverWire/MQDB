// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::database::{CallerContext, Database};
use crate::vault_backend::{DbAccess, VaultFuture};
use mqdb_core::error::Error;
use mqdb_core::types::ScopeConfig;
use mqdb_core::{Filter, FilterOp};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::sync::Arc;

/// # Errors
/// Returns any storage, validation, or constraint error from the underlying create.
pub async fn create_entity_db(db: &Database, entity: &str, data: &Value) -> Result<Value, Error> {
    let scope = ScopeConfig::default();
    db.create(entity.to_string(), data.clone(), None, None, None, &scope)
        .await
}

/// # Errors
/// Returns the underlying storage error. `Ok(None)` is returned when the record does not exist.
pub async fn read_entity_db(db: &Database, entity: &str, id: &str) -> Result<Option<Value>, Error> {
    match db
        .read(entity.to_string(), id.to_string(), vec![], None)
        .await
    {
        Ok(v) => Ok(Some(v)),
        Err(Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

/// # Errors
/// Returns `Error::NotFound` if the record does not exist, or any other storage/validation error from the underlying update.
pub async fn update_entity_db(
    db: &Database,
    entity: &str,
    id: &str,
    data: &Value,
) -> Result<Value, Error> {
    let scope = ScopeConfig::default();
    let caller = CallerContext {
        sender: None,
        client_id: None,
        scope_config: &scope,
    };
    db.update(
        entity.to_string(),
        id.to_string(),
        data.clone(),
        None,
        &caller,
    )
    .await
}

/// # Errors
/// Returns any storage error from the underlying list. An empty result is `Ok(vec![])`, not an error.
pub async fn list_entities_db(
    db: &Database,
    entity: &str,
    filter: &str,
) -> Result<Vec<Value>, Error> {
    let filters = if let Some((field, value)) = filter.split_once('=') {
        vec![Filter::new(
            field.to_string(),
            FilterOp::Eq,
            Value::String(value.to_string()),
        )]
    } else {
        vec![]
    };
    db.list(entity.to_string(), filters, vec![], None, vec![], None)
        .await
}

fn resp_topic() -> String {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes).expect("system RNG");
    format!(
        "_mqdb/vault_resp/{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]
    )
}

async fn mqtt_rr(
    client: &MqttClient,
    topic: &str,
    payload: Vec<u8>,
    timeout: std::time::Duration,
) -> Option<Vec<u8>> {
    let response_topic = resp_topic();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));
    if client
        .subscribe(&response_topic, move |msg: Message| {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Some(tx) = tx.lock().await.take() {
                    let _ = tx.send(msg.payload.clone());
                }
            });
        })
        .await
        .is_err()
    {
        return None;
    }
    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(topic, payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }
    let result = tokio::time::timeout(timeout, rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    result.ok()?.ok()
}

/// # Errors
/// Returns `Error::Internal` when the MQTT round-trip fails or the broker returns a non-ok status.
pub async fn create_entity_mqtt(
    client: &MqttClient,
    entity: &str,
    data: &Value,
) -> Result<Value, Error> {
    let topic = format!("$DB/{entity}/create");
    let payload = serde_json::to_vec(data).unwrap_or_default();
    let Some(resp) = mqtt_rr(client, &topic, payload, std::time::Duration::from_secs(5)).await
    else {
        return Err(Error::Internal(format!(
            "mqtt create failed: no response for {entity}"
        )));
    };
    let response: Value = serde_json::from_slice(&resp)
        .map_err(|e| Error::Internal(format!("mqtt create response decode failed: {e}")))?;
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status == "ok" {
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    } else {
        let message = response
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("mqtt create returned non-ok status");
        Err(Error::Internal(format!(
            "mqtt create failed for {entity}: {message}"
        )))
    }
}

/// # Errors
/// Returns `Error::Internal` when the MQTT round-trip fails or the broker returns a non-ok status without a `not_found` code; `Ok(None)` when the record does not exist.
pub async fn read_entity_mqtt(
    client: &MqttClient,
    entity: &str,
    id: &str,
) -> Result<Option<Value>, Error> {
    let topic = format!("$DB/{entity}/{id}");
    let Some(payload) = mqtt_rr(client, &topic, vec![], std::time::Duration::from_secs(5)).await
    else {
        return Err(Error::Internal(format!(
            "mqtt read failed: no response for {entity}/{id}"
        )));
    };
    let response: Value = serde_json::from_slice(&payload)
        .map_err(|e| Error::Internal(format!("mqtt read response decode failed: {e}")))?;
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status == "ok" {
        Ok(response.get("data").cloned())
    } else {
        let code = response
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if code == "not_found" {
            Ok(None)
        } else {
            let message = response
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("mqtt read returned non-ok status");
            Err(Error::Internal(format!(
                "mqtt read failed for {entity}/{id}: {message}"
            )))
        }
    }
}

/// # Errors
/// Returns `Error::NotFound` if the broker reports `not_found`, `Error::Internal` for any other MQTT or status failure.
pub async fn update_entity_mqtt(
    client: &MqttClient,
    entity: &str,
    id: &str,
    data: &Value,
) -> Result<Value, Error> {
    let topic = format!("$DB/{entity}/{id}/update");
    let payload = serde_json::to_vec(data).unwrap_or_default();
    let Some(resp) = mqtt_rr(client, &topic, payload, std::time::Duration::from_secs(5)).await
    else {
        return Err(Error::Internal(format!(
            "mqtt update failed: no response for {entity}/{id}"
        )));
    };
    let response: Value = serde_json::from_slice(&resp)
        .map_err(|e| Error::Internal(format!("mqtt update response decode failed: {e}")))?;
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status == "ok" {
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    } else {
        let code = response
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if code == "not_found" {
            Err(Error::NotFound {
                entity: entity.to_string(),
                id: id.to_string(),
            })
        } else {
            let message = response
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("mqtt update returned non-ok status");
            Err(Error::Internal(format!(
                "mqtt update failed for {entity}/{id}: {message}"
            )))
        }
    }
}

/// # Errors
/// Returns `Error::Internal` when the MQTT round-trip fails, the response is undecodable, or the broker returns a non-ok status.
pub async fn list_entities_mqtt(
    client: &MqttClient,
    entity: &str,
    filter: &str,
) -> Result<Vec<Value>, Error> {
    let topic = format!("$DB/{entity}/list");
    let list_payload = if let Some((field, value)) = filter.split_once('=') {
        serde_json::to_vec(&json!({
            "filters": [{"field": field, "op": "eq", "value": value}]
        }))
        .unwrap_or_default()
    } else {
        vec![]
    };
    let Some(payload) = mqtt_rr(
        client,
        &topic,
        list_payload,
        std::time::Duration::from_secs(10),
    )
    .await
    else {
        return Err(Error::Internal(format!(
            "mqtt list failed: no response for {entity}"
        )));
    };
    let response: Value = serde_json::from_slice(&payload)
        .map_err(|e| Error::Internal(format!("mqtt list response decode failed: {e}")))?;
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status == "ok" {
        Ok(response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default())
    } else {
        let message = response
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("mqtt list returned non-ok status");
        Err(Error::Internal(format!(
            "mqtt list failed for {entity}: {message}"
        )))
    }
}

impl DbAccess for Database {
    fn read_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
    ) -> VaultFuture<'a, Result<Option<Value>, Error>> {
        Box::pin(read_entity_db(self, entity, id))
    }
    fn update_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
        data: Value,
    ) -> VaultFuture<'a, Result<Value, Error>> {
        Box::pin(async move { update_entity_db(self, entity, id, &data).await })
    }
    fn list_entities<'a>(
        &'a self,
        entity: &'a str,
        filter: &'a str,
    ) -> VaultFuture<'a, Result<Vec<Value>, Error>> {
        Box::pin(list_entities_db(self, entity, filter))
    }
    fn create_entity<'a>(
        &'a self,
        entity: &'a str,
        data: Value,
    ) -> VaultFuture<'a, Result<Value, Error>> {
        Box::pin(async move { create_entity_db(self, entity, &data).await })
    }
}

pub struct MqttDbAccess {
    client: Arc<MqttClient>,
}

impl MqttDbAccess {
    #[must_use]
    pub fn new(client: Arc<MqttClient>) -> Self {
        Self { client }
    }
}

impl DbAccess for MqttDbAccess {
    fn read_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
    ) -> VaultFuture<'a, Result<Option<Value>, Error>> {
        Box::pin(async move { read_entity_mqtt(&self.client, entity, id).await })
    }
    fn update_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
        data: Value,
    ) -> VaultFuture<'a, Result<Value, Error>> {
        Box::pin(async move { update_entity_mqtt(&self.client, entity, id, &data).await })
    }
    fn list_entities<'a>(
        &'a self,
        entity: &'a str,
        filter: &'a str,
    ) -> VaultFuture<'a, Result<Vec<Value>, Error>> {
        Box::pin(async move { list_entities_mqtt(&self.client, entity, filter).await })
    }
    fn create_entity<'a>(
        &'a self,
        entity: &'a str,
        data: Value,
    ) -> VaultFuture<'a, Result<Value, Error>> {
        Box::pin(async move { create_entity_mqtt(&self.client, entity, &data).await })
    }
}
