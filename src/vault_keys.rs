// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::RwLock;
use zeroize::Zeroizing;

pub struct VaultKeyStore {
    keys: RwLock<HashMap<String, Zeroizing<Vec<u8>>>>,
}

impl Default for VaultKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VaultKeyStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, canonical_id: &str, key: Vec<u8>) {
        if let Ok(mut map) = self.keys.write() {
            map.insert(canonical_id.to_string(), Zeroizing::new(key));
        }
    }

    pub fn remove(&self, canonical_id: &str) {
        if let Ok(mut map) = self.keys.write() {
            map.remove(canonical_id);
        }
    }

    #[must_use]
    pub fn get(&self, canonical_id: &str) -> Option<Zeroizing<Vec<u8>>> {
        let map = self.keys.read().ok()?;
        map.get(canonical_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_roundtrip() {
        let store = VaultKeyStore::new();
        let key = vec![1u8; 32];
        store.set("user-1", key.clone());
        let retrieved = store.get("user-1").expect("key should exist");
        assert_eq!(&*retrieved, &key);
    }

    #[test]
    fn remove_clears_key() {
        let store = VaultKeyStore::new();
        store.set("user-2", vec![2u8; 32]);
        assert!(store.get("user-2").is_some());
        store.remove("user-2");
        assert!(store.get("user-2").is_none());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = VaultKeyStore::new();
        assert!(store.get("nonexistent").is_none());
    }
}
