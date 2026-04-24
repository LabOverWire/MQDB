// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use mqdb_core::types::OwnershipConfig;
use mqtt5::client::MqttClient;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::warn;

use crate::crypto::VaultCrypto;
use crate::key_store::VaultKeyStore;
use mqdb_agent::Database;
use mqdb_agent::db_helpers::{list_entities_db, update_entity_db};
use mqdb_agent::http::SessionStore;

#[derive(Clone, Copy)]
pub enum VaultMode {
    Encrypt,
    Decrypt,
}

pub struct BatchResult {
    pub succeeded: usize,
    pub failed: usize,
    pub entities_skipped: Vec<String>,
}

pub struct MigrationResumeResult {
    pub mode: String,
    pub succeeded: usize,
    pub failed: usize,
}

fn uuid_v4() -> String {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes)
        .expect("system RNG unavailable — OS CSPRNG failure");
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

pub async fn read_entity(client: &MqttClient, entity: &str, id: &str) -> Option<Value> {
    let topic = format!("$DB/{entity}/{id}");
    let response_topic = format!("_mqdb/vault_resp/{}", uuid_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
        .subscribe(&response_topic, move |msg: mqtt5::types::Message| {
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
        .publish_with_options(&topic, vec![], options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    let payload = result.ok()?.ok()?;

    let response: Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").cloned()
}

pub async fn list_entities(client: &MqttClient, entity: &str, filter: &str) -> Option<Vec<Value>> {
    let topic = format!("$DB/{entity}/list");
    let response_topic = format!("_mqdb/vault_resp/{}", uuid_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
        .subscribe(&response_topic, move |msg: mqtt5::types::Message| {
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

    let list_payload = if let Some((field, value)) = filter.split_once('=') {
        serde_json::to_vec(&json!({
            "filters": [{"field": field, "op": "eq", "value": value}]
        }))
        .unwrap_or_default()
    } else {
        vec![]
    };

    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(&topic, list_payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    let payload = result.ok()?.ok()?;

    let response: Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").and_then(|v| v.as_array()).cloned()
}

pub async fn update_entity(client: &MqttClient, entity: &str, id: &str, data: &Value) -> bool {
    let topic = format!("$DB/{entity}/{id}/update");
    let response_topic = format!("_mqdb/vault_resp/{}", uuid_v4());
    let payload = serde_json::to_vec(data).unwrap_or_default();

    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
        .subscribe(&response_topic, move |msg: mqtt5::types::Message| {
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
        return false;
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
        .publish_with_options(&topic, payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return false;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    match result {
        Ok(Ok(resp_payload)) => serde_json::from_slice::<Value>(&resp_payload)
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "ok"))
            .unwrap_or(false),
        _ => false,
    }
}

#[must_use]
pub fn extract_record_data(record: &Value) -> Value {
    if let Some(inner) = record.get("data").filter(|v| v.is_object()) {
        return inner.clone();
    }
    let mut data = record.clone();
    if let Some(obj) = data.as_object_mut() {
        obj.remove("id");
    }
    data
}

pub async fn batch_vault_operation(
    client: &MqttClient,
    ownership: &OwnershipConfig,
    canonical_id: &str,
    crypto: &VaultCrypto,
    mode: VaultMode,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &ownership.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities(client, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            match mode {
                VaultMode::Encrypt => crypto.encrypt_record(entity, id, &mut data, &skip),
                VaultMode::Decrypt => crypto.decrypt_record(entity, id, &mut data, &skip),
            }
            if update_entity(client, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

pub async fn batch_vault_re_encrypt(
    client: &MqttClient,
    ownership: &OwnershipConfig,
    canonical_id: &str,
    old_crypto: &VaultCrypto,
    new_crypto: &VaultCrypto,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &ownership.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities(client, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            old_crypto.decrypt_record(entity, id, &mut data, &skip);
            new_crypto.encrypt_record(entity, id, &mut data, &skip);
            if update_entity(client, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_pending_migration(
    client: &MqttClient,
    ownership: &OwnershipConfig,
    vault_key_store: &VaultKeyStore,
    session_store: Option<&SessionStore>,
    canonical_id: &str,
    crypto: &VaultCrypto,
    identity: &Value,
    passphrase: &str,
) -> Option<MigrationResumeResult> {
    let status = identity
        .get("vault_migration_status")
        .and_then(|v| v.as_str())?;
    if status != "pending" {
        return None;
    }
    let mode = identity
        .get("vault_migration_mode")
        .and_then(|v| v.as_str())?;

    let batch = match mode {
        "encrypt" => {
            batch_vault_operation(client, ownership, canonical_id, crypto, VaultMode::Encrypt).await
        }
        "decrypt" => {
            batch_vault_operation(client, ownership, canonical_id, crypto, VaultMode::Decrypt).await
        }
        "re_encrypt" => {
            let Some(old_salt_b64) = identity.get("vault_old_salt").and_then(|v| v.as_str()) else {
                warn!("vault re_encrypt resume failed: old salt missing from identity");
                return None;
            };
            let Ok(old_salt) = BASE64.decode(old_salt_b64) else {
                warn!("vault re_encrypt resume failed: old salt decode error");
                return None;
            };
            let old_crypto = VaultCrypto::derive(passphrase, &old_salt);
            batch_vault_re_encrypt(client, ownership, canonical_id, &old_crypto, crypto).await
        }
        _ => return None,
    };

    let migration_done = json!({
        "vault_migration_status": "complete",
        "vault_migration_mode": null,
        "vault_old_check": null,
        "vault_old_salt": null,
    });
    update_entity(client, "_identities", canonical_id, &migration_done).await;

    if mode == "decrypt" {
        let disable_vault = json!({
            "vault_enabled": false,
            "vault_salt": null,
            "vault_check": null,
        });
        update_entity(client, "_identities", canonical_id, &disable_vault).await;
        if let Some(ss) = session_store {
            ss.set_vault_unlocked_by_canonical_id(canonical_id, false);
        }
        vault_key_store.remove(canonical_id);
    }

    Some(MigrationResumeResult {
        mode: mode.to_string(),
        succeeded: batch.succeeded,
        failed: batch.failed,
    })
}

pub async fn batch_vault_operation_db(
    db: &Database,
    ownership: &OwnershipConfig,
    canonical_id: &str,
    crypto: &VaultCrypto,
    mode: VaultMode,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &ownership.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities_db(db, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            match mode {
                VaultMode::Encrypt => crypto.encrypt_record(entity, id, &mut data, &skip),
                VaultMode::Decrypt => crypto.decrypt_record(entity, id, &mut data, &skip),
            }
            if update_entity_db(db, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

pub async fn batch_vault_re_encrypt_db(
    db: &Database,
    ownership: &OwnershipConfig,
    canonical_id: &str,
    old_crypto: &VaultCrypto,
    new_crypto: &VaultCrypto,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &ownership.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities_db(db, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            old_crypto.decrypt_record(entity, id, &mut data, &skip);
            new_crypto.encrypt_record(entity, id, &mut data, &skip);
            if update_entity_db(db, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_pending_migration_db(
    db: &Database,
    ownership: &OwnershipConfig,
    vault_key_store: &VaultKeyStore,
    canonical_id: &str,
    crypto: &VaultCrypto,
    identity: &Value,
    passphrase: &str,
) -> Option<MigrationResumeResult> {
    let status = identity
        .get("vault_migration_status")
        .and_then(|v| v.as_str())?;
    if status != "pending" {
        return None;
    }
    let mode = identity
        .get("vault_migration_mode")
        .and_then(|v| v.as_str())?;

    let batch = match mode {
        "encrypt" => {
            batch_vault_operation_db(db, ownership, canonical_id, crypto, VaultMode::Encrypt).await
        }
        "decrypt" => {
            batch_vault_operation_db(db, ownership, canonical_id, crypto, VaultMode::Decrypt).await
        }
        "re_encrypt" => {
            let Some(old_salt_b64) = identity.get("vault_old_salt").and_then(|v| v.as_str()) else {
                warn!("vault re_encrypt resume failed: old salt missing from identity");
                return None;
            };
            let Ok(old_salt) = BASE64.decode(old_salt_b64) else {
                warn!("vault re_encrypt resume failed: old salt decode error");
                return None;
            };
            let old_crypto = VaultCrypto::derive(passphrase, &old_salt);
            batch_vault_re_encrypt_db(db, ownership, canonical_id, &old_crypto, crypto).await
        }
        _ => return None,
    };

    let migration_done = json!({
        "vault_migration_status": "complete",
        "vault_migration_mode": null,
        "vault_old_check": null,
        "vault_old_salt": null,
    });
    update_entity_db(db, "_identities", canonical_id, &migration_done).await;

    if mode == "decrypt" {
        let disable_vault = json!({
            "vault_enabled": false,
            "vault_salt": null,
            "vault_check": null,
        });
        update_entity_db(db, "_identities", canonical_id, &disable_vault).await;
        vault_key_store.remove(canonical_id);
    }

    Some(MigrationResumeResult {
        mode: mode.to_string(),
        succeeded: batch.succeeded,
        failed: batch.failed,
    })
}
