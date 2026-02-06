// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::error::Result;
use crate::keys;
use crate::storage::Storage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResponse {
    pub correlation_id: String,
    pub response: serde_json::Value,
    pub timestamp: u64,
}

impl CachedResponse {
    #[allow(clippy::must_use_candidate)]
    pub fn new(correlation_id: String, response: serde_json::Value) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        Self {
            correlation_id,
            response,
            timestamp,
        }
    }

    #[must_use]
    pub fn is_expired(&self, ttl_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        now - self.timestamp > ttl_secs
    }
}

pub struct DedupStore {
    storage: Arc<Storage>,
    ttl_secs: u64,
}

impl DedupStore {
    #[allow(clippy::must_use_candidate)]
    pub fn new(storage: Arc<Storage>, ttl_secs: u64) -> Self {
        Self { storage, ttl_secs }
    }

    /// # Errors
    /// Returns an error if reading or deserializing fails.
    pub fn get(&self, correlation_id: &str) -> Result<Option<serde_json::Value>> {
        let key = keys::encode_dedup_key(correlation_id);

        if let Some(data) = self.storage.get(&key)? {
            let cached: CachedResponse = serde_json::from_slice(&data)?;

            if cached.is_expired(self.ttl_secs) {
                self.storage.remove(&key)?;
                return Ok(None);
            }

            return Ok(Some(cached.response));
        }

        Ok(None)
    }

    /// # Errors
    /// Returns an error if serializing or storing fails.
    pub fn set(&self, correlation_id: &str, response: serde_json::Value) -> Result<()> {
        let cached = CachedResponse::new(correlation_id.to_string(), response);
        let key = keys::encode_dedup_key(correlation_id);
        let value = serde_json::to_vec(&cached)?;

        self.storage.insert(&key, &value)?;
        Ok(())
    }

    /// # Errors
    /// Returns an error if scanning or removing entries fails.
    pub fn cleanup_expired(&self) -> Result<usize> {
        let prefix = b"dedup/";
        let items = self.storage.prefix_scan(prefix)?;

        let mut removed = 0;

        for (key, value) in items {
            if let Ok(cached) = serde_json::from_slice::<CachedResponse>(&value)
                && cached.is_expired(self.ttl_secs)
            {
                self.storage.remove(&key)?;
                removed += 1;
            }
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_cached_response_expiry() {
        let response = CachedResponse::new("test-id".into(), json!({"status": "ok"}));
        assert!(!response.is_expired(3600));

        let old_response = CachedResponse {
            correlation_id: "old-id".into(),
            response: json!({"status": "ok"}),
            timestamp: 0,
        };
        assert!(old_response.is_expired(3600));
    }

    #[test]
    fn test_dedup_store() {
        use crate::config::DurabilityMode;

        let tmp = TempDir::new().unwrap();
        let storage = Arc::new(Storage::open(tmp.path(), DurabilityMode::None).unwrap());
        let dedup = DedupStore::new(storage, 3600);

        let response = json!({"status": "ok", "id": 123});
        dedup.set("req-1", response.clone()).unwrap();

        let cached = dedup.get("req-1").unwrap();
        assert_eq!(cached, Some(response));

        let missing = dedup.get("req-2").unwrap();
        assert_eq!(missing, None);
    }
}
