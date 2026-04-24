// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::database::{CallerContext, Database};
use crate::vault_backend::{DbAccess, VaultFuture};
use mqdb_core::types::ScopeConfig;
use mqdb_core::{Filter, FilterOp};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn create_entity_db(db: &Database, entity: &str, data: &Value) -> bool {
    let scope = ScopeConfig::default();
    db.create(entity.to_string(), data.clone(), None, None, None, &scope)
        .await
        .is_ok()
}

pub async fn read_entity_db(db: &Database, entity: &str, id: &str) -> Option<Value> {
    db.read(entity.to_string(), id.to_string(), vec![], None)
        .await
        .ok()
}

pub async fn update_entity_db(db: &Database, entity: &str, id: &str, data: &Value) -> bool {
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
    .is_ok()
}

pub async fn list_entities_db(db: &Database, entity: &str, filter: &str) -> Option<Vec<Value>> {
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
        .ok()
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

pub async fn create_entity_mqtt(client: &MqttClient, entity: &str, data: &Value) -> bool {
    let topic = format!("$DB/{entity}/create");
    let payload = serde_json::to_vec(data).unwrap_or_default();
    let Some(resp) = mqtt_rr(client, &topic, payload, std::time::Duration::from_secs(5)).await
    else {
        return false;
    };
    serde_json::from_slice::<Value>(&resp)
        .ok()
        .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "ok"))
        .unwrap_or(false)
}

pub async fn read_entity_mqtt(client: &MqttClient, entity: &str, id: &str) -> Option<Value> {
    let topic = format!("$DB/{entity}/{id}");
    let payload = mqtt_rr(client, &topic, vec![], std::time::Duration::from_secs(5)).await?;
    let response: Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").cloned()
}

pub async fn update_entity_mqtt(client: &MqttClient, entity: &str, id: &str, data: &Value) -> bool {
    let topic = format!("$DB/{entity}/{id}/update");
    let payload = serde_json::to_vec(data).unwrap_or_default();
    let Some(resp) = mqtt_rr(client, &topic, payload, std::time::Duration::from_secs(5)).await
    else {
        return false;
    };
    serde_json::from_slice::<Value>(&resp)
        .ok()
        .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "ok"))
        .unwrap_or(false)
}

pub async fn list_entities_mqtt(
    client: &MqttClient,
    entity: &str,
    filter: &str,
) -> Option<Vec<Value>> {
    let topic = format!("$DB/{entity}/list");
    let list_payload = if let Some((field, value)) = filter.split_once('=') {
        serde_json::to_vec(&json!({
            "filters": [{"field": field, "op": "eq", "value": value}]
        }))
        .unwrap_or_default()
    } else {
        vec![]
    };
    let payload = mqtt_rr(
        client,
        &topic,
        list_payload,
        std::time::Duration::from_secs(10),
    )
    .await?;
    let response: Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").and_then(|v| v.as_array()).cloned()
}

impl DbAccess for Database {
    fn read_entity<'a>(&'a self, entity: &'a str, id: &'a str) -> VaultFuture<'a, Option<Value>> {
        Box::pin(read_entity_db(self, entity, id))
    }
    fn update_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
        data: Value,
    ) -> VaultFuture<'a, bool> {
        Box::pin(async move { update_entity_db(self, entity, id, &data).await })
    }
    fn list_entities<'a>(
        &'a self,
        entity: &'a str,
        filter: &'a str,
    ) -> VaultFuture<'a, Option<Vec<Value>>> {
        Box::pin(list_entities_db(self, entity, filter))
    }
    fn create_entity<'a>(&'a self, entity: &'a str, data: Value) -> VaultFuture<'a, bool> {
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
    fn read_entity<'a>(&'a self, entity: &'a str, id: &'a str) -> VaultFuture<'a, Option<Value>> {
        Box::pin(async move { read_entity_mqtt(&self.client, entity, id).await })
    }
    fn update_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
        data: Value,
    ) -> VaultFuture<'a, bool> {
        Box::pin(async move { update_entity_mqtt(&self.client, entity, id, &data).await })
    }
    fn list_entities<'a>(
        &'a self,
        entity: &'a str,
        filter: &'a str,
    ) -> VaultFuture<'a, Option<Vec<Value>>> {
        Box::pin(async move { list_entities_mqtt(&self.client, entity, filter).await })
    }
    fn create_entity<'a>(&'a self, entity: &'a str, data: Value) -> VaultFuture<'a, bool> {
        Box::pin(async move { create_entity_mqtt(&self.client, entity, &data).await })
    }
}
