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
        if key.starts_with('_') || skip_fields.iter().any(|s| s == &key) {
            continue;
        }
        if let Some(val) = obj.get_mut(&key) {
            encrypt_value_recursive(crypto, entity, id, val);
        }
    }
}

fn encrypt_value_recursive(crypto: &VaultCrypto, entity: &str, id: &str, value: &mut Value) {
    match value {
        Value::String(s) => {
            if let Ok(encrypted) = encrypt_string(crypto, entity, id, s) {
                *s = encrypted;
            }
        }
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if key.starts_with('_') {
                    continue;
                }
                if let Some(child) = map.get_mut(&key) {
                    encrypt_value_recursive(crypto, entity, id, child);
                }
            }
        }
        Value::Array(arr) => {
            for elem in arr {
                encrypt_value_recursive(crypto, entity, id, elem);
            }
        }
        Value::Number(_) | Value::Bool(_) | Value::Null => {}
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
        if key.starts_with('_') || skip_fields.iter().any(|s| s == &key) {
            continue;
        }
        if let Some(val) = obj.get_mut(&key) {
            decrypt_value_recursive(crypto, entity, id, val);
        }
    }
}

fn decrypt_value_recursive(crypto: &VaultCrypto, entity: &str, id: &str, value: &mut Value) {
    match value {
        Value::String(s) => {
            if let Some(decrypted) = decrypt_string(crypto, entity, id, s) {
                *s = decrypted;
            }
        }
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if key.starts_with('_') {
                    continue;
                }
                if let Some(child) = map.get_mut(&key) {
                    decrypt_value_recursive(crypto, entity, id, child);
                }
            }
        }
        Value::Array(arr) => {
            for elem in arr {
                decrypt_value_recursive(crypto, entity, id, elem);
            }
        }
        Value::Number(_) | Value::Bool(_) | Value::Null => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_crypto() -> VaultCrypto {
        let salt = VaultCrypto::generate_salt();
        VaultCrypto::derive("test-passphrase", &salt)
    }

    fn owned_config() -> OwnershipConfig {
        let mut config = OwnershipConfig::default();
        config
            .entity_owner_fields
            .insert("notes".to_string(), "userId".to_string());
        config
    }

    #[test]
    fn vault_eligible_owned_entity() {
        let config = owned_config();
        assert!(is_vault_eligible("notes", &config));
    }

    #[test]
    fn vault_eligible_system_entity() {
        let config = owned_config();
        assert!(!is_vault_eligible("_sessions", &config));
    }

    #[test]
    fn vault_eligible_no_ownership() {
        let config = OwnershipConfig::default();
        assert!(!is_vault_eligible("notes", &config));
    }

    #[test]
    fn skip_fields_includes_owner() {
        let config = owned_config();
        let skip = build_vault_skip_fields("notes", &config);
        assert_eq!(skip, vec!["id".to_string(), "userId".to_string()]);
    }

    #[test]
    fn skip_fields_without_ownership() {
        let config = OwnershipConfig::default();
        let skip = build_vault_skip_fields("notes", &config);
        assert_eq!(skip, vec!["id".to_string()]);
    }

    #[test]
    fn should_skip_system_fields() {
        let skip = vec!["id".to_string()];
        assert!(should_skip_field("_version", &skip));
        assert!(should_skip_field("_created_at", &skip));
        assert!(should_skip_field("id", &skip));
        assert!(!should_skip_field("title", &skip));
        assert!(!should_skip_field("body", &skip));
    }

    #[test]
    fn ensure_id_preserves_existing() {
        let mut data = serde_json::json!({"id": "my-id", "title": "test"});
        let id = ensure_id(&mut data);
        assert_eq!(id, "my-id");
        assert_eq!(data["id"], "my-id");
    }

    #[test]
    fn ensure_id_generates_when_missing() {
        let mut data = serde_json::json!({"title": "test"});
        let id = ensure_id(&mut data);
        assert!(!id.is_empty());
        assert_eq!(data["id"].as_str().unwrap(), id);
    }

    #[test]
    fn encrypt_decrypt_fields_roundtrip() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string(), "userId".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "userId": "user-abc",
            "title": "Secret Title",
            "body": "Secret Body",
            "count": 42,
            "active": true,
            "tags": null,
            "_version": 1
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["id"], "rec-1");
        assert_eq!(data["userId"], "user-abc");
        assert_ne!(data["title"].as_str().unwrap(), "Secret Title");
        assert_ne!(data["body"].as_str().unwrap(), "Secret Body");
        assert_eq!(data["count"], 42);
        assert_eq!(data["active"], true);
        assert!(data["tags"].is_null());
        assert_eq!(data["_version"], 1);

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["title"], "Secret Title");
        assert_eq!(data["body"], "Secret Body");
    }

    #[test]
    fn nested_object_roundtrip() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "profile": {"name": "Alice", "age": 30}
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["id"], "rec-1");
        assert_ne!(data["profile"]["name"].as_str().unwrap(), "Alice");
        assert_eq!(data["profile"]["age"], 30);

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["profile"]["name"], "Alice");
        assert_eq!(data["profile"]["age"], 30);
    }

    #[test]
    fn array_of_strings_roundtrip() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "tags": ["secret1", "secret2"]
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_ne!(data["tags"][0].as_str().unwrap(), "secret1");
        assert_ne!(data["tags"][1].as_str().unwrap(), "secret2");

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["tags"][0], "secret1");
        assert_eq!(data["tags"][1], "secret2");
    }

    #[test]
    fn mixed_deep_nesting_roundtrip() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "data": {"items": [{"label": "X", "count": 5}]}
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_ne!(data["data"]["items"][0]["label"].as_str().unwrap(), "X");
        assert_eq!(data["data"]["items"][0]["count"], 5);

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["data"]["items"][0]["label"], "X");
        assert_eq!(data["data"]["items"][0]["count"], 5);
    }

    #[test]
    fn underscore_keys_skipped_at_depth() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "meta": {"_internal": "skip-me", "visible": "encrypt-me"}
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["meta"]["_internal"], "skip-me");
        assert_ne!(data["meta"]["visible"].as_str().unwrap(), "encrypt-me");

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["meta"]["_internal"], "skip-me");
        assert_eq!(data["meta"]["visible"], "encrypt-me");
    }

    #[test]
    fn skip_fields_only_at_top_level() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "keep-clear",
            "nested": {"id": "encrypt-me"}
        });

        vault_encrypt_fields(&crypto, "notes", "keep-clear", &mut data, &skip);

        assert_eq!(data["id"], "keep-clear");
        assert_ne!(data["nested"]["id"].as_str().unwrap(), "encrypt-me");

        vault_decrypt_fields(&crypto, "notes", "keep-clear", &mut data, &skip);

        assert_eq!(data["id"], "keep-clear");
        assert_eq!(data["nested"]["id"], "encrypt-me");
    }

    #[test]
    fn empty_containers_unchanged() {
        let crypto = test_crypto();
        let skip = vec!["id".to_string()];
        let mut data = serde_json::json!({
            "id": "rec-1",
            "obj": {},
            "arr": []
        });

        vault_encrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["obj"], serde_json::json!({}));
        assert_eq!(data["arr"], serde_json::json!([]));

        vault_decrypt_fields(&crypto, "notes", "rec-1", &mut data, &skip);

        assert_eq!(data["obj"], serde_json::json!({}));
        assert_eq!(data["arr"], serde_json::json!([]));
    }

    #[test]
    fn encrypt_decrypt_string_roundtrip() {
        let crypto = test_crypto();
        let encrypted = encrypt_string(&crypto, "notes", "rec-1", "hello world").unwrap();
        assert_ne!(encrypted, "hello world");
        let decrypted = decrypt_string(&crypto, "notes", "rec-1", &encrypted).unwrap();
        assert_eq!(decrypted, "hello world");
    }

    #[test]
    fn decrypt_string_plaintext_returns_none() {
        let crypto = test_crypto();
        assert!(decrypt_string(&crypto, "notes", "rec-1", "not-ciphertext").is_none());
    }

    #[test]
    fn uuid_v7_format_and_uniqueness() {
        let id1 = uuid_v7();
        let id2 = uuid_v7();
        assert_ne!(id1, id2);
        assert_eq!(id1.len(), 36);
        assert_eq!(id1.chars().filter(|c| *c == '-').count(), 4);
        assert_eq!(&id1[14..15], "7");
    }
}
