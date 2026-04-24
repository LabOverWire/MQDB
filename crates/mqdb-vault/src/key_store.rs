// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::OwnedRwLockWriteGuard;
use zeroize::Zeroizing;

pub struct VaultKeyStore {
    keys: RwLock<HashMap<String, Zeroizing<Vec<u8>>>>,
    fences: RwLock<HashMap<String, Arc<tokio::sync::RwLock<()>>>>,
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
            fences: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, canonical_id: &str, key: Zeroizing<Vec<u8>>) {
        if let Ok(mut map) = self.keys.write() {
            map.insert(canonical_id.to_string(), key);
        }
    }

    pub fn remove(&self, canonical_id: &str) {
        if let Ok(mut map) = self.keys.write() {
            map.remove(canonical_id);
        }
        if let Ok(mut map) = self.fences.write() {
            map.remove(canonical_id);
        }
    }

    #[must_use]
    pub fn get(&self, canonical_id: &str) -> Option<Zeroizing<Vec<u8>>> {
        let map = self.keys.read().ok()?;
        map.get(canonical_id).cloned()
    }

    pub async fn acquire_fence(&self, canonical_id: &str) -> OwnedRwLockWriteGuard<()> {
        let lock = {
            let mut map = self
                .fences
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.entry(canonical_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())))
                .clone()
        };
        lock.write_owned().await
    }

    pub async fn read_fence(&self, canonical_id: &str) {
        let lock = {
            let map = self
                .fences
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.get(canonical_id).cloned()
        };
        if let Some(lock) = lock {
            let _guard = lock.read().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_roundtrip() {
        let store = VaultKeyStore::new();
        let key = vec![1u8; 32];
        store.set("user-1", Zeroizing::new(key.clone()));
        let retrieved = store.get("user-1").expect("key should exist");
        assert_eq!(&*retrieved, &key);
    }

    #[test]
    fn remove_clears_key() {
        let store = VaultKeyStore::new();
        store.set("user-2", Zeroizing::new(vec![2u8; 32]));
        assert!(store.get("user-2").is_some());
        store.remove("user-2");
        assert!(store.get("user-2").is_none());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = VaultKeyStore::new();
        assert!(store.get("nonexistent").is_none());
    }

    #[tokio::test]
    async fn fence_blocks_concurrent_reads() {
        let store = Arc::new(VaultKeyStore::new());
        let fence_guard = store.acquire_fence("user-a").await;

        let read_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let read_completed_clone = read_completed.clone();
        let store_clone = store.clone();

        let handle = tokio::spawn(async move {
            store_clone.read_fence("user-a").await;
            read_completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!read_completed.load(std::sync::atomic::Ordering::SeqCst));

        drop(fence_guard);
        let _ = handle.await;
        assert!(read_completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn read_fence_noop_without_active_batch() {
        let store = VaultKeyStore::new();
        store.read_fence("no-fence-user").await;
    }
}
