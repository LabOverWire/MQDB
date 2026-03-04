// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::hkdf;
use ring::hmac;
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use std::num::NonZeroU32;
use zeroize::Zeroizing;

const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const SALT_LEN: usize = 32;
const TAG_LEN: usize = 16;
const PBKDF2_ITERATIONS: u32 = 600_000;

const KEY_DERIVATION_INFO: &[u8] = b"mqdb-identity-encryption-seed-v1";

pub struct IdentityCrypto {
    key: LessSafeKey,
    hmac_key: hmac::Key,
}

#[derive(Debug)]
pub enum CryptoError {
    KeyGeneration(String),
    Encryption(String),
    Decryption(String),
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyGeneration(e) => write!(f, "key generation failed: {e}"),
            Self::Encryption(e) => write!(f, "encryption failed: {e}"),
            Self::Decryption(e) => write!(f, "decryption failed: {e}"),
        }
    }
}

impl std::error::Error for CryptoError {}

pub struct IdentityKeyMaterial {
    pub salt: Vec<u8>,
    pub wrapped_key: Vec<u8>,
}

impl IdentityCrypto {
    /// # Errors
    /// Returns `CryptoError` if key generation or wrapping fails.
    pub fn generate() -> Result<(Self, IdentityKeyMaterial), CryptoError> {
        let rng = SystemRandom::new();

        let mut identity_key = Zeroizing::new([0u8; KEY_LEN]);
        rng.fill(identity_key.as_mut())
            .map_err(|_| CryptoError::KeyGeneration("random fill failed".into()))?;

        let mut salt = [0u8; SALT_LEN];
        rng.fill(&mut salt)
            .map_err(|_| CryptoError::KeyGeneration("salt generation failed".into()))?;

        let wrapping_key = derive_wrapping_key(&salt)?;
        let wrapped_key = wrap_key(&wrapping_key, identity_key.as_ref())?;

        let crypto = Self::from_raw_key(identity_key.as_ref())?;

        Ok((
            crypto,
            IdentityKeyMaterial {
                salt: salt.to_vec(),
                wrapped_key,
            },
        ))
    }

    /// # Errors
    /// Returns `CryptoError` if unwrapping the stored key fails.
    pub fn from_stored(salt: &[u8], wrapped_key: &[u8]) -> Result<Self, CryptoError> {
        let wrapping_key = derive_wrapping_key(salt)?;
        let identity_key = Zeroizing::new(unwrap_key(&wrapping_key, wrapped_key)?);
        Self::from_raw_key(&identity_key)
    }

    /// # Errors
    /// Returns `CryptoError` if the key bytes are invalid.
    pub fn from_external_key(key_bytes: &[u8]) -> Result<Self, CryptoError> {
        if key_bytes.len() != KEY_LEN {
            return Err(CryptoError::KeyGeneration(format!(
                "key must be {KEY_LEN} bytes, got {}",
                key_bytes.len()
            )));
        }
        Self::from_raw_key(key_bytes)
    }

    fn from_raw_key(key_bytes: &[u8]) -> Result<Self, CryptoError> {
        let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]);
        let prk = salt.extract(key_bytes);

        let mut enc_key = Zeroizing::new([0u8; KEY_LEN]);
        let enc_okm = prk
            .expand(&[b"aes-encryption"], hkdf::HKDF_SHA256)
            .map_err(|_| {
                CryptoError::KeyGeneration("HKDF expand for encryption key failed".into())
            })?;
        enc_okm.fill(enc_key.as_mut()).map_err(|_| {
            CryptoError::KeyGeneration("HKDF fill for encryption key failed".into())
        })?;

        let mut hmac_bytes = Zeroizing::new([0u8; KEY_LEN]);
        let hmac_okm = prk
            .expand(&[b"hmac-blind-index"], hkdf::HKDF_SHA256)
            .map_err(|_| CryptoError::KeyGeneration("HKDF expand for HMAC key failed".into()))?;
        hmac_okm
            .fill(hmac_bytes.as_mut())
            .map_err(|_| CryptoError::KeyGeneration("HKDF fill for HMAC key failed".into()))?;

        let unbound = UnboundKey::new(&AES_256_GCM, enc_key.as_ref())
            .map_err(|_| CryptoError::KeyGeneration("invalid encryption key".into()))?;
        let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, hmac_bytes.as_ref());

        Ok(Self {
            key: LessSafeKey::new(unbound),
            hmac_key,
        })
    }

    /// # Errors
    /// Returns `CryptoError::Encryption` if encryption fails.
    pub fn encrypt_field(&self, entity: &str, value: &str) -> Result<String, CryptoError> {
        let rng = SystemRandom::new();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rng.fill(&mut nonce_bytes)
            .map_err(|_| CryptoError::Encryption("nonce generation failed".into()))?;
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| CryptoError::Encryption("invalid nonce".into()))?;

        let aad = Aad::from(entity.as_bytes());
        let mut in_out = Vec::with_capacity(value.len() + TAG_LEN);
        in_out.extend_from_slice(value.as_bytes());

        self.key
            .seal_in_place_append_tag(nonce, aad, &mut in_out)
            .map_err(|_| CryptoError::Encryption("seal failed".into()))?;

        let mut output = Vec::with_capacity(NONCE_LEN + in_out.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&in_out);

        Ok(BASE64.encode(&output))
    }

    /// # Errors
    /// Returns `CryptoError::Decryption` if decryption fails.
    pub fn decrypt_field(&self, entity: &str, encrypted: &str) -> Result<String, CryptoError> {
        let data = BASE64
            .decode(encrypted)
            .map_err(|_| CryptoError::Decryption("invalid base64".into()))?;

        if data.len() < NONCE_LEN + TAG_LEN + 1 {
            return Err(CryptoError::Decryption("ciphertext too short".into()));
        }

        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
            .map_err(|_| CryptoError::Decryption("invalid nonce".into()))?;

        let aad = Aad::from(entity.as_bytes());
        let mut in_out = ciphertext.to_vec();

        let plaintext = self
            .key
            .open_in_place(nonce, aad, &mut in_out)
            .map_err(|_| CryptoError::Decryption("decryption failed".into()))?;

        String::from_utf8(plaintext.to_vec())
            .map_err(|_| CryptoError::Decryption("invalid UTF-8".into()))
    }

    #[must_use]
    pub fn blind_index(&self, entity: &str, value: &str) -> String {
        let tag = hmac::sign(&self.hmac_key, format!("{entity}:{value}").as_bytes());
        hex_encode(tag.as_ref())
    }

    pub fn encrypt_json_fields(&self, entity: &str, data: &mut serde_json::Value, fields: &[&str]) {
        if let Some(obj) = data.as_object_mut() {
            for &field in fields {
                if let Some(serde_json::Value::String(val)) = obj.get(field)
                    && let Ok(encrypted) = self.encrypt_field(entity, val)
                {
                    obj.insert(field.to_string(), serde_json::Value::String(encrypted));
                }
            }
        }
    }

    pub fn decrypt_json_fields(&self, entity: &str, data: &mut serde_json::Value, fields: &[&str]) {
        if let Some(obj) = data.as_object_mut() {
            for &field in fields {
                if let Some(serde_json::Value::String(val)) = obj.get(field)
                    && let Ok(decrypted) = self.decrypt_field(entity, val)
                {
                    obj.insert(field.to_string(), serde_json::Value::String(decrypted));
                }
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        s.push(char::from(b"0123456789abcdef"[(b & 0x0F) as usize]));
    }
    s
}

fn derive_wrapping_key(salt: &[u8]) -> Result<LessSafeKey, CryptoError> {
    let iterations = NonZeroU32::new(PBKDF2_ITERATIONS)
        .ok_or_else(|| CryptoError::KeyGeneration("invalid iteration count".into()))?;
    let mut key_bytes = Zeroizing::new([0u8; KEY_LEN]);
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        KEY_DERIVATION_INFO,
        key_bytes.as_mut(),
    );
    let unbound = UnboundKey::new(&AES_256_GCM, key_bytes.as_ref())
        .map_err(|_| CryptoError::KeyGeneration("wrapping key creation failed".into()))?;
    Ok(LessSafeKey::new(unbound))
}

fn wrap_key(wrapping_key: &LessSafeKey, identity_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| CryptoError::KeyGeneration("nonce generation failed".into()))?;
    let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| CryptoError::KeyGeneration("invalid nonce".into()))?;

    let aad = Aad::from(b"identity_key" as &[u8]);
    let mut in_out = Vec::with_capacity(identity_key.len() + TAG_LEN);
    in_out.extend_from_slice(identity_key);

    wrapping_key
        .seal_in_place_append_tag(nonce, aad, &mut in_out)
        .map_err(|_| CryptoError::KeyGeneration("key wrapping failed".into()))?;

    let mut output = Vec::with_capacity(NONCE_LEN + in_out.len());
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&in_out);
    Ok(output)
}

fn unwrap_key(wrapping_key: &LessSafeKey, wrapped: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if wrapped.len() < NONCE_LEN + TAG_LEN + 1 {
        return Err(CryptoError::Decryption("wrapped key data too short".into()));
    }

    let (nonce_bytes, ciphertext) = wrapped.split_at(NONCE_LEN);
    let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
        .map_err(|_| CryptoError::Decryption("invalid nonce in wrapped key".into()))?;

    let aad = Aad::from(b"identity_key" as &[u8]);
    let mut in_out = ciphertext.to_vec();

    let plaintext = wrapping_key
        .open_in_place(nonce, aad, &mut in_out)
        .map_err(|_| CryptoError::Decryption("key unwrapping failed".into()))?;

    Ok(plaintext.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let (crypto, _material) = IdentityCrypto::generate().unwrap();
        let plaintext = "user@example.com";
        let encrypted = crypto.encrypt_field("_identities", plaintext).unwrap();
        assert_ne!(encrypted, plaintext);
        let decrypted = crypto.decrypt_field("_identities", &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn different_entities_produce_different_ciphertext() {
        let (crypto, _material) = IdentityCrypto::generate().unwrap();
        let plaintext = "test@example.com";
        let enc_a = crypto.encrypt_field("_identities", plaintext).unwrap();
        let enc_b = crypto.encrypt_field("_identity_links", plaintext).unwrap();
        assert_ne!(enc_a, enc_b);
    }

    #[test]
    fn wrong_entity_fails_decryption() {
        let (crypto, _material) = IdentityCrypto::generate().unwrap();
        let encrypted = crypto.encrypt_field("_identities", "secret").unwrap();
        assert!(crypto.decrypt_field("_identity_links", &encrypted).is_err());
    }

    #[test]
    fn stored_key_roundtrip() {
        let (original, material) = IdentityCrypto::generate().unwrap();
        let plaintext = "sensitive data";
        let encrypted = original.encrypt_field("test", plaintext).unwrap();

        let restored = IdentityCrypto::from_stored(&material.salt, &material.wrapped_key).unwrap();
        let decrypted = restored.decrypt_field("test", &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn external_key_works() {
        let key_bytes = [0x42u8; KEY_LEN];
        let crypto = IdentityCrypto::from_external_key(&key_bytes).unwrap();
        let encrypted = crypto.encrypt_field("test", "hello").unwrap();
        let decrypted = crypto.decrypt_field("test", &encrypted).unwrap();
        assert_eq!(decrypted, "hello");
    }

    #[test]
    fn external_key_wrong_size_rejected() {
        assert!(IdentityCrypto::from_external_key(&[0u8; 16]).is_err());
    }

    #[test]
    fn blind_index_deterministic() {
        let (crypto, _) = IdentityCrypto::generate().unwrap();
        let idx1 = crypto.blind_index("_identity_links", "user@example.com");
        let idx2 = crypto.blind_index("_identity_links", "user@example.com");
        assert_eq!(idx1, idx2);
        let idx3 = crypto.blind_index("_identity_links", "other@example.com");
        assert_ne!(idx1, idx3);
        let idx4 = crypto.blind_index("_identities", "user@example.com");
        assert_ne!(idx1, idx4);
    }

    #[test]
    fn json_field_encryption() {
        let (crypto, _) = IdentityCrypto::generate().unwrap();
        let mut data = serde_json::json!({
            "id": "google:123",
            "email": "user@example.com",
            "name": "Test User",
            "canonical_id": "some-uuid"
        });

        crypto.encrypt_json_fields("_identity_links", &mut data, &["email", "name"]);

        assert_ne!(data["email"].as_str().unwrap(), "user@example.com");
        assert_ne!(data["name"].as_str().unwrap(), "Test User");
        assert_eq!(data["id"].as_str().unwrap(), "google:123");
        assert_eq!(data["canonical_id"].as_str().unwrap(), "some-uuid");

        crypto.decrypt_json_fields("_identity_links", &mut data, &["email", "name"]);
        assert_eq!(data["email"].as_str().unwrap(), "user@example.com");
        assert_eq!(data["name"].as_str().unwrap(), "Test User");
    }
}
