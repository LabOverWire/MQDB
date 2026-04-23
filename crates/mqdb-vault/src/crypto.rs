// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use std::num::NonZeroU32;
use zeroize::Zeroizing;

const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const SALT_LEN: usize = 32;
const TAG_LEN: usize = 16;
const PBKDF2_ITERATIONS: u32 = 600_000;
const CHECK_PLAINTEXT: &[u8] = b"mqdb-vault-check-v1";

pub struct VaultCrypto {
    key: LessSafeKey,
}

impl VaultCrypto {
    #[allow(clippy::missing_panics_doc)]
    fn derive_raw(passphrase: &str, salt: &[u8]) -> Zeroizing<[u8; KEY_LEN]> {
        let iterations = NonZeroU32::new(PBKDF2_ITERATIONS).expect("PBKDF2_ITERATIONS is non-zero");
        let mut key_bytes = Zeroizing::new([0u8; KEY_LEN]);
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            salt,
            passphrase.as_bytes(),
            key_bytes.as_mut(),
        );
        key_bytes
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn derive(passphrase: &str, salt: &[u8]) -> Self {
        let key_bytes = Self::derive_raw(passphrase, salt);
        let unbound = UnboundKey::new(&AES_256_GCM, key_bytes.as_ref())
            .expect("AES-256-GCM accepts 32-byte keys");
        Self {
            key: LessSafeKey::new(unbound),
        }
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn derive_with_raw_key(passphrase: &str, salt: &[u8]) -> (Self, Zeroizing<Vec<u8>>) {
        let key_bytes = Self::derive_raw(passphrase, salt);
        let unbound = UnboundKey::new(&AES_256_GCM, key_bytes.as_ref())
            .expect("AES-256-GCM accepts 32-byte keys");
        (
            Self {
                key: LessSafeKey::new(unbound),
            },
            Zeroizing::new(key_bytes.to_vec()),
        )
    }

    #[must_use]
    pub fn from_key_bytes(key_bytes: &[u8]) -> Option<Self> {
        let unbound = UnboundKey::new(&AES_256_GCM, key_bytes).ok()?;
        Some(Self {
            key: LessSafeKey::new(unbound),
        })
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn generate_salt() -> [u8; SALT_LEN] {
        let rng = SystemRandom::new();
        let mut salt = [0u8; SALT_LEN];
        rng.fill(&mut salt).expect("system RNG should not fail");
        salt
    }

    #[must_use]
    pub fn raw_key_bytes(passphrase: &str, salt: &[u8]) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(Self::derive_raw(passphrase, salt).to_vec())
    }

    /// # Errors
    /// Returns error string if encryption of the check token fails.
    pub fn create_check_token(&self) -> Result<String, String> {
        self.encrypt_value("_vault_check", "check", CHECK_PLAINTEXT)
    }

    #[must_use]
    pub fn verify_check_token(&self, token: &str) -> bool {
        self.decrypt_value("_vault_check", "check", token)
            .is_ok_and(|plain| plain == CHECK_PLAINTEXT)
    }

    pub fn encrypt_record(
        &self,
        entity: &str,
        id: &str,
        data: &mut serde_json::Value,
        skip_fields: &[&str],
    ) {
        let Some(obj) = data.as_object_mut() else {
            return;
        };
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            if key.starts_with('_') || skip_fields.contains(&key.as_str()) {
                continue;
            }
            if let Some(val) = obj.get_mut(&key) {
                self.encrypt_value_recursive(entity, id, val);
            }
        }
    }

    pub fn decrypt_record(
        &self,
        entity: &str,
        id: &str,
        data: &mut serde_json::Value,
        skip_fields: &[&str],
    ) {
        let Some(obj) = data.as_object_mut() else {
            return;
        };
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            if key.starts_with('_') || skip_fields.contains(&key.as_str()) {
                continue;
            }
            if let Some(val) = obj.get_mut(&key) {
                self.decrypt_value_recursive(entity, id, val);
            }
        }
    }

    fn encrypt_value_recursive(&self, entity: &str, id: &str, value: &mut serde_json::Value) {
        match value {
            serde_json::Value::String(s) => {
                if let Ok(encrypted) = self.encrypt_value(entity, id, s.as_bytes()) {
                    *s = encrypted;
                }
            }
            serde_json::Value::Number(_) | serde_json::Value::Bool(_) | serde_json::Value::Null => {
                let text = format!("\x01{value}");
                if let Ok(encrypted) = self.encrypt_value(entity, id, text.as_bytes()) {
                    *value = serde_json::Value::String(encrypted);
                }
            }
            serde_json::Value::Object(map) => {
                let keys: Vec<String> = map.keys().cloned().collect();
                for key in keys {
                    if key.starts_with('_') {
                        continue;
                    }
                    if let Some(child) = map.get_mut(&key) {
                        self.encrypt_value_recursive(entity, id, child);
                    }
                }
            }
            serde_json::Value::Array(arr) => {
                for elem in arr {
                    self.encrypt_value_recursive(entity, id, elem);
                }
            }
        }
    }

    fn decrypt_value_recursive(&self, entity: &str, id: &str, value: &mut serde_json::Value) {
        if let serde_json::Value::String(s) = &*value {
            if let Ok(decrypted) = self.decrypt_value(entity, id, s)
                && let Ok(plain) = String::from_utf8(decrypted)
            {
                if let Some(json_text) = plain.strip_prefix('\x01') {
                    if let Ok(restored) = serde_json::from_str(json_text) {
                        *value = restored;
                    }
                } else {
                    *value = serde_json::Value::String(plain);
                }
            }
            return;
        }

        match value {
            serde_json::Value::Object(map) => {
                let keys: Vec<String> = map.keys().cloned().collect();
                for key in keys {
                    if key.starts_with('_') {
                        continue;
                    }
                    if let Some(child) = map.get_mut(&key) {
                        self.decrypt_value_recursive(entity, id, child);
                    }
                }
            }
            serde_json::Value::Array(arr) => {
                for elem in arr {
                    self.decrypt_value_recursive(entity, id, elem);
                }
            }
            _ => {}
        }
    }

    fn encrypt_value(&self, entity: &str, id: &str, plaintext: &[u8]) -> Result<String, String> {
        let rng = SystemRandom::new();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rng.fill(&mut nonce_bytes)
            .map_err(|_| "nonce generation failed".to_string())?;
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| "invalid nonce".to_string())?;

        let aad_str = format!("{entity}:{id}");
        let aad = Aad::from(aad_str.as_bytes());
        let mut in_out = Vec::with_capacity(plaintext.len() + TAG_LEN);
        in_out.extend_from_slice(plaintext);

        self.key
            .seal_in_place_append_tag(nonce, aad, &mut in_out)
            .map_err(|_| "seal failed".to_string())?;

        let mut output = Vec::with_capacity(NONCE_LEN + in_out.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&in_out);

        Ok(BASE64.encode(&output))
    }

    fn decrypt_value(&self, entity: &str, id: &str, encrypted: &str) -> Result<Vec<u8>, String> {
        let data = BASE64
            .decode(encrypted)
            .map_err(|_| "invalid base64".to_string())?;

        if data.len() < NONCE_LEN + TAG_LEN + 1 {
            return Err("ciphertext too short".to_string());
        }

        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
            .map_err(|_| "invalid nonce".to_string())?;

        let aad_str = format!("{entity}:{id}");
        let aad = Aad::from(aad_str.as_bytes());
        let mut in_out = ciphertext.to_vec();

        let plaintext = self
            .key
            .open_in_place(nonce, aad, &mut in_out)
            .map_err(|_| "decryption failed".to_string())?;

        Ok(plaintext.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_roundtrip() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("my-passphrase", &salt);
        let encrypted = crypto
            .encrypt_value("test_entity", "rec1", b"hello world")
            .unwrap();
        let decrypted = crypto
            .decrypt_value("test_entity", "rec1", &encrypted)
            .unwrap();
        assert_eq!(decrypted, b"hello world");
    }

    #[test]
    fn check_token_verify() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("my-passphrase", &salt);
        let token = crypto.create_check_token().unwrap();
        assert!(crypto.verify_check_token(&token));
    }

    #[test]
    fn wrong_passphrase_fails_check() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("correct-passphrase", &salt);
        let token = crypto.create_check_token().unwrap();

        let wrong = VaultCrypto::derive("wrong-passphrase", &salt);
        assert!(!wrong.verify_check_token(&token));
    }

    #[test]
    fn encrypt_decrypt_record() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "owner": "user-abc",
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30,
            "active": true,
            "notes": null
        });

        crypto.encrypt_record("people", "rec-1", &mut data, &["id", "owner"]);

        assert_eq!(data["id"], "rec-1");
        assert_eq!(data["owner"], "user-abc");
        assert_ne!(data["name"].as_str().unwrap(), "Alice");
        assert_ne!(data["email"].as_str().unwrap(), "alice@example.com");
        assert!(
            data["age"].is_string(),
            "number should be encrypted to string"
        );
        assert!(
            data["active"].is_string(),
            "bool should be encrypted to string"
        );
        assert!(
            data["notes"].is_string(),
            "null should be encrypted to string"
        );

        crypto.decrypt_record("people", "rec-1", &mut data, &["id", "owner"]);

        assert_eq!(data["name"], "Alice");
        assert_eq!(data["email"], "alice@example.com");
        assert_eq!(data["age"], 30);
        assert_eq!(data["active"], true);
        assert!(data["notes"].is_null());
    }

    #[test]
    fn nested_object_encrypt_decrypt() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "profile": {"name": "Alice", "city": "Paris"},
            "score": 99
        });

        crypto.encrypt_record("people", "rec-1", &mut data, &["id"]);

        assert_eq!(data["id"], "rec-1");
        assert_ne!(data["profile"]["name"].as_str().unwrap(), "Alice");
        assert_ne!(data["profile"]["city"].as_str().unwrap(), "Paris");
        assert!(data["score"].is_string());

        crypto.decrypt_record("people", "rec-1", &mut data, &["id"]);

        assert_eq!(data["profile"]["name"], "Alice");
        assert_eq!(data["profile"]["city"], "Paris");
        assert_eq!(data["score"], 99);
    }

    #[test]
    fn array_encrypt_decrypt() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "tags": ["alpha", "beta"],
            "counts": [1, 2, 3]
        });

        crypto.encrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_ne!(data["tags"][0].as_str().unwrap(), "alpha");
        assert_ne!(data["tags"][1].as_str().unwrap(), "beta");
        assert!(data["counts"][0].is_string());
        assert!(data["counts"][1].is_string());
        assert!(data["counts"][2].is_string());

        crypto.decrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_eq!(data["tags"][0], "alpha");
        assert_eq!(data["tags"][1], "beta");
        assert_eq!(data["counts"][0], 1);
        assert_eq!(data["counts"][1], 2);
        assert_eq!(data["counts"][2], 3);
    }

    #[test]
    fn deep_nesting_encrypt_decrypt() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "data": {
                "items": [
                    {"label": "X", "count": 5, "nested": {"secret": "deep"}}
                ]
            }
        });

        crypto.encrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_ne!(data["data"]["items"][0]["label"].as_str().unwrap(), "X");
        assert!(data["data"]["items"][0]["count"].is_string());
        assert_ne!(
            data["data"]["items"][0]["nested"]["secret"]
                .as_str()
                .unwrap(),
            "deep"
        );

        crypto.decrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_eq!(data["data"]["items"][0]["label"], "X");
        assert_eq!(data["data"]["items"][0]["count"], 5);
        assert_eq!(data["data"]["items"][0]["nested"]["secret"], "deep");
    }

    #[test]
    fn wrong_passphrase_nested_data() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("correct", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "profile": {"secret": "sensitive"}
        });

        crypto.encrypt_record("entity", "rec-1", &mut data, &["id"]);
        let encrypted = data["profile"]["secret"].as_str().unwrap().to_string();

        let wrong = VaultCrypto::derive("wrong", &salt);
        wrong.decrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_eq!(data["profile"]["secret"].as_str().unwrap(), encrypted);
    }

    #[test]
    fn wrong_passphrase_cannot_decrypt_record() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("correct", &salt);
        let mut data = serde_json::json!({
            "id": "rec-1",
            "secret": "sensitive-data"
        });

        crypto.encrypt_record("entity", "rec-1", &mut data, &["id"]);
        let encrypted_secret = data["secret"].as_str().unwrap().to_string();

        let wrong = VaultCrypto::derive("wrong", &salt);
        wrong.decrypt_record("entity", "rec-1", &mut data, &["id"]);

        assert_eq!(data["secret"].as_str().unwrap(), encrypted_secret);
    }

    #[test]
    fn from_key_bytes_roundtrip() {
        let salt = VaultCrypto::generate_salt();
        let key_bytes = VaultCrypto::raw_key_bytes("test-pass", &salt);
        let crypto = VaultCrypto::from_key_bytes(&key_bytes).unwrap();
        let encrypted = crypto
            .encrypt_value("entity", "id1", b"plaintext data")
            .unwrap();
        let decrypted = crypto.decrypt_value("entity", "id1", &encrypted).unwrap();
        assert_eq!(decrypted, b"plaintext data");
    }

    #[test]
    fn from_key_bytes_matches_derive() {
        let salt = VaultCrypto::generate_salt();
        let derived = VaultCrypto::derive("pass", &salt);
        let token = derived.create_check_token().unwrap();

        let key_bytes = VaultCrypto::raw_key_bytes("pass", &salt);
        let from_bytes = VaultCrypto::from_key_bytes(&key_bytes).unwrap();
        assert!(from_bytes.verify_check_token(&token));
    }

    #[test]
    fn from_key_bytes_wrong_length_returns_none() {
        assert!(VaultCrypto::from_key_bytes(&[0u8; 16]).is_none());
        assert!(VaultCrypto::from_key_bytes(&[0u8; 64]).is_none());
        assert!(VaultCrypto::from_key_bytes(&[]).is_none());
    }

    #[test]
    fn different_salt_produces_different_key() {
        let salt1 = VaultCrypto::generate_salt();
        let salt2 = VaultCrypto::generate_salt();
        let crypto1 = VaultCrypto::derive("same-pass", &salt1);
        let crypto2 = VaultCrypto::derive("same-pass", &salt2);

        let token = crypto1.create_check_token().unwrap();
        assert!(crypto1.verify_check_token(&token));
        assert!(!crypto2.verify_check_token(&token));
    }

    #[test]
    fn number_roundtrip_preserves_type() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({"id": "r1", "val": 42});

        crypto.encrypt_record("e", "r1", &mut data, &["id"]);
        assert!(data["val"].is_string());

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert_eq!(data["val"], 42);
    }

    #[test]
    fn bool_roundtrip_preserves_type() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({"id": "r1", "flag": true, "off": false});

        crypto.encrypt_record("e", "r1", &mut data, &["id"]);
        assert!(data["flag"].is_string());
        assert!(data["off"].is_string());

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert_eq!(data["flag"], true);
        assert_eq!(data["off"], false);
    }

    #[test]
    fn null_roundtrip_preserves_type() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({"id": "r1", "empty": null});

        crypto.encrypt_record("e", "r1", &mut data, &["id"]);
        assert!(data["empty"].is_string());

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert!(data["empty"].is_null());
    }

    #[test]
    fn float_roundtrip_preserves_type() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({"id": "r1", "price": 19.99});

        crypto.encrypt_record("e", "r1", &mut data, &["id"]);
        assert!(data["price"].is_string());

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert!((data["price"].as_f64().unwrap() - 19.99).abs() < f64::EPSILON);
    }

    #[test]
    fn mixed_array_roundtrip() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let mut data = serde_json::json!({
            "id": "r1",
            "mix": ["text", 42, true, null]
        });

        crypto.encrypt_record("e", "r1", &mut data, &["id"]);
        for i in 0..4 {
            assert!(
                data["mix"][i].is_string(),
                "element {i} should be encrypted to string"
            );
        }

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert_eq!(data["mix"][0], "text");
        assert_eq!(data["mix"][1], 42);
        assert_eq!(data["mix"][2], true);
        assert!(data["mix"][3].is_null());
    }

    #[test]
    fn backward_compat_string_without_prefix() {
        let salt = VaultCrypto::generate_salt();
        let crypto = VaultCrypto::derive("pass123", &salt);
        let encrypted = crypto.encrypt_value("e", "r1", b"plain string").unwrap();
        let mut data = serde_json::json!({"id": "r1", "val": encrypted});

        crypto.decrypt_record("e", "r1", &mut data, &["id"]);
        assert_eq!(data["val"], "plain string");
    }
}
