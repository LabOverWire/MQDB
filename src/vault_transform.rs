// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::http::VaultCrypto;
use crate::types::OwnershipConfig;
use serde_json::Value;

#[must_use]
pub fn is_vault_eligible(entity: &str, ownership: &OwnershipConfig) -> bool {
    !entity.starts_with('_') && ownership.entity_owner_fields.contains_key(entity)
}

#[must_use]
pub fn build_vault_skip_fields(entity: &str, ownership: &OwnershipConfig) -> Vec<String> {
    let mut skip = vec!["id".to_string()];
    if let Some(owner_field) = ownership.entity_owner_fields.get(entity) {
        skip.push(owner_field.clone());
    }
    skip
}

#[must_use]
pub fn should_skip_field(key: &str, skip_fields: &[String]) -> bool {
    key.starts_with('_') || skip_fields.iter().any(|s| s == key)
}

pub fn ensure_id(data: &mut Value) -> String {
    if let Some(id) = data.get("id").and_then(|v| v.as_str()) {
        return id.to_string();
    }
    let id = uuid_v7();
    if let Some(obj) = data.as_object_mut() {
        obj.insert("id".to_string(), Value::String(id.clone()));
    }
    id
}

pub fn vault_encrypt_fields(
    crypto: &VaultCrypto,
    entity: &str,
    id: &str,
    data: &mut Value,
    skip_fields: &[String],
) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };
    let keys: Vec<String> = obj.keys().cloned().collect();
    for key in keys {
        if should_skip_field(&key, skip_fields) {
            continue;
        }
        if let Some(Value::String(val)) = obj.get(&key)
            && let Ok(encrypted) = encrypt_string(crypto, entity, id, val)
        {
            obj.insert(key, Value::String(encrypted));
        }
    }
}

pub fn vault_decrypt_fields(
    crypto: &VaultCrypto,
    entity: &str,
    id: &str,
    data: &mut Value,
    skip_fields: &[String],
) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };
    let keys: Vec<String> = obj.keys().cloned().collect();
    for key in keys {
        if should_skip_field(&key, skip_fields) {
            continue;
        }
        if let Some(Value::String(val)) = obj.get(&key)
            && let Some(decrypted) = decrypt_string(crypto, entity, id, val)
        {
            obj.insert(key, Value::String(decrypted));
        }
    }
}

/// # Errors
/// Returns `Err` if encryption fails to produce a ciphertext string.
pub fn encrypt_string(
    crypto: &VaultCrypto,
    entity: &str,
    id: &str,
    plaintext: &str,
) -> Result<String, String> {
    let mut wrapper = serde_json::json!({ "v": plaintext });
    crypto.encrypt_record(entity, id, &mut wrapper, &[]);
    wrapper
        .get("v")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| "encrypt failed".to_string())
}

#[must_use]
pub fn decrypt_string(
    crypto: &VaultCrypto,
    entity: &str,
    id: &str,
    ciphertext: &str,
) -> Option<String> {
    let mut wrapper = serde_json::json!({ "v": ciphertext });
    crypto.decrypt_record(entity, id, &mut wrapper, &[]);
    let decrypted = wrapper.get("v")?.as_str()?;
    if decrypted == ciphertext {
        None
    } else {
        Some(decrypted.to_string())
    }
}

#[allow(clippy::missing_panics_doc)]
#[must_use]
pub fn uuid_v7() -> String {
    use ring::rand::{SecureRandom, SystemRandom};

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());

    let mut bytes = [0u8; 16];
    bytes[0] = ((ts >> 40) & 0xFF) as u8;
    bytes[1] = ((ts >> 32) & 0xFF) as u8;
    bytes[2] = ((ts >> 24) & 0xFF) as u8;
    bytes[3] = ((ts >> 16) & 0xFF) as u8;
    bytes[4] = ((ts >> 8) & 0xFF) as u8;
    bytes[5] = (ts & 0xFF) as u8;

    let rng = SystemRandom::new();
    rng.fill(&mut bytes[6..])
        .expect("system RNG unavailable — OS CSPRNG failure");

    bytes[6] = (bytes[6] & 0x0F) | 0x70;
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
