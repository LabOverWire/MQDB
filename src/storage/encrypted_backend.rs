// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::backend::{BatchOperations, StorageBackend};
use crate::error::{Error, Result};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use std::num::NonZeroU32;
use std::sync::Arc;

const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const SALT_LEN: usize = 32;
const TAG_LEN: usize = 16;
const PBKDF2_ITERATIONS: u32 = 600_000;

const SALT_KEY: &[u8] = b"_crypto/salt";
const CHECK_KEY: &[u8] = b"_crypto/check";
const CHECK_PLAINTEXT: &[u8] = b"mqdb";

pub struct EncryptedBackend {
    inner: Arc<dyn StorageBackend>,
    key: Arc<LessSafeKey>,
}

impl EncryptedBackend {
    /// # Errors
    /// Returns an error if key derivation or passphrase verification fails.
    pub fn open(inner: Arc<dyn StorageBackend>, passphrase: &str) -> Result<Self> {
        let existing_salt = inner.get(SALT_KEY)?;

        let salt = if let Some(s) = existing_salt {
            if s.len() != SALT_LEN {
                return Err(Error::Internal("corrupt encryption salt".into()));
            }
            let mut arr = [0u8; SALT_LEN];
            arr.copy_from_slice(&s);
            arr
        } else {
            let new_salt = generate_salt()?;
            inner.insert(SALT_KEY, &new_salt)?;
            new_salt
        };

        let key = Arc::new(derive_key(passphrase, &salt)?);

        let existing_check = inner.get(CHECK_KEY)?;
        if let Some(encrypted_check) = existing_check {
            decrypt(&key, CHECK_KEY, &encrypted_check)
                .map_err(|_| Error::Internal("invalid passphrase".into()))?;
        } else {
            let encrypted = encrypt(&key, CHECK_KEY, CHECK_PLAINTEXT)?;
            inner.insert(CHECK_KEY, &encrypted)?;
        }

        Ok(Self { inner, key })
    }
}

impl StorageBackend for EncryptedBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.get(key)? {
            Some(encrypted) => Ok(Some(decrypt(&self.key, key, &encrypted)?)),
            None => Ok(None),
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let encrypted = encrypt(&self.key, key, value)?;
        self.inner.insert(key, &encrypted)
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.inner.remove(key)
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let raw = self.inner.prefix_scan(prefix)?;
        decrypt_pairs(&self.key, raw)
    }

    fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        self.inner.prefix_count(prefix)
    }

    fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.inner.prefix_scan_keys(prefix)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let raw = self.inner.range_scan(start, end)?;
        decrypt_pairs(&self.key, raw)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(EncryptedBatch {
            inner: self.inner.batch(),
            backend: Arc::clone(&self.inner),
            key: Arc::clone(&self.key),
            pending_expects: Vec::new(),
        })
    }

    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

struct EncryptedBatch {
    inner: Box<dyn BatchOperations>,
    backend: Arc<dyn StorageBackend>,
    key: Arc<LessSafeKey>,
    pending_expects: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BatchOperations for EncryptedBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match encrypt(&self.key, &key, &value) {
            Ok(encrypted) => self.inner.insert(key, encrypted),
            Err(_) => self.inner.insert(key, value),
        }
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.inner.remove(key);
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.pending_expects.push((key, expected_value));
    }

    fn commit(mut self: Box<Self>) -> Result<()> {
        for (key, expected_plaintext) in &self.pending_expects {
            let stored = self.backend.get(key)?;
            match stored {
                Some(encrypted) => {
                    let decrypted = decrypt(&self.key, key, &encrypted)?;
                    if decrypted != *expected_plaintext {
                        return Err(Error::Conflict(
                            "optimistic lock failed: value was modified".into(),
                        ));
                    }
                    self.inner.expect_value(key.clone(), encrypted);
                }
                None => {
                    return Err(Error::Conflict(
                        "optimistic lock failed: value was modified".into(),
                    ));
                }
            }
        }
        self.inner.commit()
    }
}

fn generate_salt() -> Result<[u8; SALT_LEN]> {
    let rng = SystemRandom::new();
    let mut salt = [0u8; SALT_LEN];
    rng.fill(&mut salt)
        .map_err(|_| Error::Internal("random generation failed".into()))?;
    Ok(salt)
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<LessSafeKey> {
    let iterations =
        NonZeroU32::new(PBKDF2_ITERATIONS).expect("PBKDF2_ITERATIONS is non-zero constant");
    let mut key_bytes = [0u8; KEY_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        passphrase.as_bytes(),
        &mut key_bytes,
    );
    let unbound = UnboundKey::new(&AES_256_GCM, &key_bytes)
        .map_err(|_| Error::Internal("key construction failed".into()))?;
    Ok(LessSafeKey::new(unbound))
}

fn encrypt(key: &LessSafeKey, storage_key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| Error::Internal("nonce generation failed".into()))?;
    let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| Error::Internal("nonce construction failed".into()))?;
    let aad = Aad::from(storage_key);

    let mut in_out = Vec::with_capacity(plaintext.len() + TAG_LEN);
    in_out.extend_from_slice(plaintext);
    key.seal_in_place_append_tag(nonce, aad, &mut in_out)
        .map_err(|_| Error::Internal("encryption failed".into()))?;

    let mut output = Vec::with_capacity(NONCE_LEN + in_out.len());
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&in_out);
    Ok(output)
}

fn decrypt(key: &LessSafeKey, storage_key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < NONCE_LEN + TAG_LEN + 1 {
        return Err(Error::Internal("ciphertext too short".into()));
    }
    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
        .map_err(|_| Error::Internal("nonce construction failed".into()))?;
    let aad = Aad::from(storage_key);

    let mut in_out = ciphertext.to_vec();
    let plaintext = key
        .open_in_place(nonce, aad, &mut in_out)
        .map_err(|_| Error::Internal("decryption failed".into()))?;
    Ok(plaintext.to_vec())
}

fn decrypt_pairs(
    key: &LessSafeKey,
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut decrypted = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        let plaintext = decrypt(key, &k, &v)?;
        decrypted.push((k, plaintext));
    }
    Ok(decrypted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    fn make_encrypted(passphrase: &str) -> (Arc<MemoryBackend>, EncryptedBackend) {
        let memory = Arc::new(MemoryBackend::new());
        let encrypted =
            EncryptedBackend::open(Arc::clone(&memory) as Arc<dyn StorageBackend>, passphrase)
                .unwrap();
        (memory, encrypted)
    }

    #[test]
    fn roundtrip() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"users/1", b"alice").unwrap();
        let val = enc.get(b"users/1").unwrap().unwrap();
        assert_eq!(val, b"alice");
    }

    #[test]
    fn stored_values_are_encrypted() {
        let (mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"users/1", b"alice").unwrap();
        let raw = mem.get(b"users/1").unwrap().unwrap();
        assert_ne!(raw, b"alice");
        assert!(raw.len() > b"alice".len());
    }

    #[test]
    fn get_missing_key() {
        let (_mem, enc) = make_encrypted("test-passphrase");
        assert_eq!(enc.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn remove_key() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"key", b"value").unwrap();
        enc.remove(b"key").unwrap();
        assert_eq!(enc.get(b"key").unwrap(), None);
    }

    #[test]
    fn prefix_scan_decrypts() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"users/1", b"alice").unwrap();
        enc.insert(b"users/2", b"bob").unwrap();
        enc.insert(b"posts/1", b"hello").unwrap();

        let results = enc.prefix_scan(b"users/").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"users/1".to_vec(), b"alice".to_vec()));
        assert_eq!(results[1], (b"users/2".to_vec(), b"bob".to_vec()));
    }

    #[test]
    fn range_scan_decrypts() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"a", b"1").unwrap();
        enc.insert(b"b", b"2").unwrap();
        enc.insert(b"c", b"3").unwrap();
        enc.insert(b"d", b"4").unwrap();

        let results = enc.range_scan(b"b", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn batch_insert_and_get() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        let mut batch = enc.batch();
        batch.insert(b"k1".to_vec(), b"v1".to_vec());
        batch.insert(b"k2".to_vec(), b"v2".to_vec());
        batch.commit().unwrap();

        assert_eq!(enc.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(enc.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn batch_expect_value_success() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"key", b"original").unwrap();

        let mut batch = enc.batch();
        batch.expect_value(b"key".to_vec(), b"original".to_vec());
        batch.insert(b"key".to_vec(), b"updated".to_vec());
        batch.commit().unwrap();

        assert_eq!(enc.get(b"key").unwrap(), Some(b"updated".to_vec()));
    }

    #[test]
    fn batch_expect_value_failure() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"key", b"original").unwrap();

        let mut batch = enc.batch();
        batch.expect_value(b"key".to_vec(), b"wrong".to_vec());
        batch.insert(b"key".to_vec(), b"updated".to_vec());

        let result = batch.commit();
        assert!(result.is_err());
        assert_eq!(enc.get(b"key").unwrap(), Some(b"original".to_vec()));
    }

    #[test]
    fn wrong_passphrase_rejected() {
        let memory = Arc::new(MemoryBackend::new());
        let _enc = EncryptedBackend::open(
            Arc::clone(&memory) as Arc<dyn StorageBackend>,
            "correct-passphrase",
        )
        .unwrap();

        let result = EncryptedBackend::open(memory as Arc<dyn StorageBackend>, "wrong-passphrase");
        assert!(result.is_err());
    }

    #[test]
    fn correct_passphrase_reopens() {
        let memory = Arc::new(MemoryBackend::new());

        {
            let enc = EncryptedBackend::open(
                Arc::clone(&memory) as Arc<dyn StorageBackend>,
                "my-passphrase",
            )
            .unwrap();
            enc.insert(b"secret", b"data").unwrap();
        }

        let enc =
            EncryptedBackend::open(memory as Arc<dyn StorageBackend>, "my-passphrase").unwrap();
        assert_eq!(enc.get(b"secret").unwrap(), Some(b"data".to_vec()));
    }

    #[test]
    fn batch_remove() {
        let (_mem, enc) = make_encrypted("test-passphrase");

        enc.insert(b"key", b"value").unwrap();

        let mut batch = enc.batch();
        batch.remove(b"key".to_vec());
        batch.commit().unwrap();

        assert_eq!(enc.get(b"key").unwrap(), None);
    }
}
