// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use std::num::NonZeroU32;

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
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn derive(passphrase: &str, salt: &[u8]) -> Self {
        let iterations = NonZeroU32::new(PBKDF2_ITERATIONS).expect("PBKDF2_ITERATIONS is non-zero");
        let mut key_bytes = [0u8; KEY_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            salt,
            passphrase.as_bytes(),
            &mut key_bytes,
        );
        let unbound =
            UnboundKey::new(&AES_256_GCM, &key_bytes).expect("AES-256-GCM accepts 32-byte keys");
        Self {
            key: LessSafeKey::new(unbound),
        }
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
    #[allow(clippy::missing_panics_doc)]
    pub fn raw_key_bytes(passphrase: &str, salt: &[u8]) -> Vec<u8> {
        let iterations = NonZeroU32::new(PBKDF2_ITERATIONS).expect("PBKDF2_ITERATIONS is non-zero");
        let mut key_bytes = [0u8; KEY_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            salt,
            passphrase.as_bytes(),
            &mut key_bytes,
        );
        key_bytes.to_vec()
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
            if skip_fields.contains(&key.as_str()) {
                continue;
            }
            if let Some(serde_json::Value::String(val)) = obj.get(&key)
                && let Ok(encrypted) = self.encrypt_value(entity, id, val.as_bytes())
            {
                obj.insert(key, serde_json::Value::String(encrypted));
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
            if skip_fields.contains(&key.as_str()) {
                continue;
            }
            if let Some(serde_json::Value::String(val)) = obj.get(&key)
                && let Ok(decrypted) = self.decrypt_value(entity, id, val)
                && let Ok(s) = String::from_utf8(decrypted)
            {
                obj.insert(key, serde_json::Value::String(s));
            }
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
        assert_eq!(data["age"], 30);
        assert_eq!(data["active"], true);
        assert!(data["notes"].is_null());

        crypto.decrypt_record("people", "rec-1", &mut data, &["id", "owner"]);

        assert_eq!(data["name"], "Alice");
        assert_eq!(data["email"], "alice@example.com");
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
}
