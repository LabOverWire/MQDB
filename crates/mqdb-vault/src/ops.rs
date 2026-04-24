// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use mqdb_core::types::OwnershipConfig;
use serde_json::{Value, json};
use tracing::warn;

use crate::crypto::VaultCrypto;
use crate::key_store::VaultKeyStore;
use mqdb_agent::vault_backend::DbAccess;

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
    db: &dyn DbAccess,
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
        let Some(records) = db.list_entities(entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()).map(String::from) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            match mode {
                VaultMode::Encrypt => crypto.encrypt_record(entity, &id, &mut data, &skip),
                VaultMode::Decrypt => crypto.decrypt_record(entity, &id, &mut data, &skip),
            }
            if db.update_entity(entity, &id, data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

pub async fn batch_vault_re_encrypt(
    db: &dyn DbAccess,
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
        let Some(records) = db.list_entities(entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()).map(String::from) else {
                continue;
            };
            let mut data = extract_record_data(&record);
            let skip: Vec<&str> = vec![owner_field.as_str()];
            old_crypto.decrypt_record(entity, &id, &mut data, &skip);
            new_crypto.encrypt_record(entity, &id, &mut data, &skip);
            if db.update_entity(entity, &id, data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

pub async fn resume_pending_migration(
    db: &dyn DbAccess,
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
            batch_vault_operation(db, ownership, canonical_id, crypto, VaultMode::Encrypt).await
        }
        "decrypt" => {
            batch_vault_operation(db, ownership, canonical_id, crypto, VaultMode::Decrypt).await
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
            batch_vault_re_encrypt(db, ownership, canonical_id, &old_crypto, crypto).await
        }
        _ => return None,
    };

    let migration_done = json!({
        "vault_migration_status": "complete",
        "vault_migration_mode": null,
        "vault_old_check": null,
        "vault_old_salt": null,
    });
    db.update_entity("_identities", canonical_id, migration_done)
        .await;

    if mode == "decrypt" {
        let disable_vault = json!({
            "vault_enabled": false,
            "vault_salt": null,
            "vault_check": null,
        });
        db.update_entity("_identities", canonical_id, disable_vault)
            .await;
        vault_key_store.remove(canonical_id);
    }

    Some(MigrationResumeResult {
        mode: mode.to_string(),
        succeeded: batch.succeeded,
        failed: batch.failed,
    })
}
