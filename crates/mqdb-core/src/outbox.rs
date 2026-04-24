// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
use crate::events::ChangeEvent;
use crate::storage::{BatchWriter, Storage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const OUTBOX_PREFIX: &[u8] = b"_outbox/";
const DEAD_LETTER_PREFIX: &[u8] = b"_dead_letter/";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOutboxEntry {
    events: Vec<ChangeEvent>,
    retry_count: u32,
    created_at: u64,
    #[serde(default)]
    dispatched_count: usize,
}

pub struct Outbox {
    storage: Arc<Storage>,
}

impl Outbox {
    #[allow(clippy::must_use_candidate)]
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn enqueue_event(&self, batch: &mut BatchWriter, operation_id: &str, event: &ChangeEvent) {
        self.enqueue_events(batch, operation_id, std::slice::from_ref(event));
    }

    pub fn enqueue_events(
        &self,
        batch: &mut BatchWriter,
        operation_id: &str,
        events: &[ChangeEvent],
    ) {
        let key = format!("_outbox/{operation_id}");
        let stored = StoredOutboxEntry {
            events: events.to_vec(),
            retry_count: 0,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            dispatched_count: 0,
        };
        let value = serde_json::to_vec(&stored).unwrap_or_default();
        batch.insert(key.into_bytes(), value);
    }

    /// # Errors
    /// Returns an error if reading from storage fails.
    pub fn pending_events(&self) -> Result<Vec<OutboxEntry>> {
        self.scan_entries(OUTBOX_PREFIX, "_outbox/")
    }

    fn scan_entries(&self, prefix: &[u8], strip_prefix: &str) -> Result<Vec<OutboxEntry>> {
        let items = self.storage.prefix_scan(prefix)?;
        let mut entries = Vec::new();

        for (key, value) in items {
            let key_str = String::from_utf8_lossy(&key);
            let operation_id = key_str
                .strip_prefix(strip_prefix)
                .unwrap_or(&key_str)
                .to_string();

            if let Ok(stored) = serde_json::from_slice::<StoredOutboxEntry>(&value) {
                entries.push(OutboxEntry {
                    operation_id,
                    events: stored.events,
                    retry_count: stored.retry_count,
                    created_at: stored.created_at,
                    dispatched_count: stored.dispatched_count,
                });
            } else if let Ok(event) = serde_json::from_slice::<ChangeEvent>(&value) {
                entries.push(OutboxEntry {
                    operation_id,
                    events: vec![event],
                    retry_count: 0,
                    created_at: 0,
                    dispatched_count: 0,
                });
            } else if let Ok(events) = serde_json::from_slice::<Vec<ChangeEvent>>(&value) {
                entries.push(OutboxEntry {
                    operation_id,
                    events,
                    retry_count: 0,
                    created_at: 0,
                    dispatched_count: 0,
                });
            }
        }

        Ok(entries)
    }

    /// # Errors
    /// Returns an error if removing from storage fails.
    pub fn mark_delivered(&self, operation_id: &str) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        self.storage.remove(key.as_bytes())
    }

    /// # Errors
    /// Returns an error if reading from storage fails.
    pub fn pending_count(&self) -> Result<usize> {
        let items = self.storage.prefix_scan(OUTBOX_PREFIX)?;
        Ok(items.len())
    }

    /// # Errors
    /// Returns an error if reading or writing to storage fails.
    pub fn increment_retry(&self, operation_id: &str) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        if let Some(value) = self.storage.get(key.as_bytes())?
            && let Ok(mut stored) = serde_json::from_slice::<StoredOutboxEntry>(&value)
        {
            stored.retry_count += 1;
            let new_value = serde_json::to_vec(&stored).unwrap_or_default();
            self.storage.insert(key.as_bytes(), &new_value)?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading or writing to storage fails.
    pub fn update_dispatched_count(&self, operation_id: &str, count: usize) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        if let Some(value) = self.storage.get(key.as_bytes())?
            && let Ok(mut stored) = serde_json::from_slice::<StoredOutboxEntry>(&value)
        {
            stored.dispatched_count = count;
            stored.retry_count += 1;
            let new_value = serde_json::to_vec(&stored).unwrap_or_default();
            self.storage.insert(key.as_bytes(), &new_value)?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading or writing to storage fails.
    pub fn move_to_dead_letter(&self, operation_id: &str) -> Result<()> {
        let outbox_key = format!("_outbox/{operation_id}");
        if let Some(value) = self.storage.get(outbox_key.as_bytes())? {
            let dead_letter_key = format!("_dead_letter/{operation_id}");
            let mut batch = self.storage.batch();
            batch.remove(outbox_key.into_bytes());
            batch.insert(dead_letter_key.into_bytes(), value);
            batch.commit()?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading from storage fails.
    pub fn dead_letter_entries(&self) -> Result<Vec<OutboxEntry>> {
        self.scan_entries(DEAD_LETTER_PREFIX, "_dead_letter/")
    }

    /// # Errors
    /// Returns an error if reading from storage fails.
    pub fn dead_letter_count(&self) -> Result<usize> {
        let items = self.storage.prefix_scan(DEAD_LETTER_PREFIX)?;
        Ok(items.len())
    }

    /// # Errors
    /// Returns an error if removing from storage fails.
    pub fn remove_dead_letter(&self, operation_id: &str) -> Result<()> {
        let key = format!("_dead_letter/{operation_id}");
        self.storage.remove(key.as_bytes())
    }
}

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub operation_id: String,
    pub events: Vec<ChangeEvent>,
    pub retry_count: u32,
    pub created_at: u64,
    pub dispatched_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[test]
    fn test_outbox_enqueue_single_event() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation_id, "op-123");
        assert_eq!(pending[0].events.len(), 1);
        assert_eq!(pending[0].events[0].entity, "users");
    }

    #[test]
    fn test_outbox_enqueue_multiple_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let events = vec![
            ChangeEvent::delete("users".to_string(), "1".to_string(), serde_json::json!({})),
            ChangeEvent::delete("posts".to_string(), "10".to_string(), serde_json::json!({})),
            ChangeEvent::delete("posts".to_string(), "11".to_string(), serde_json::json!({})),
        ];

        let mut batch = storage.batch();
        outbox.enqueue_events(&mut batch, "cascade-456", &events);
        batch.commit().unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].events.len(), 3);
    }

    #[test]
    fn test_outbox_mark_delivered() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        assert_eq!(outbox.pending_count().unwrap(), 1);

        outbox.mark_delivered("op-123").unwrap();

        assert_eq!(outbox.pending_count().unwrap(), 0);
    }

    #[test]
    fn test_outbox_atomic_with_data() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        batch.insert(b"data/users/1".to_vec(), b"user data".to_vec());
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        assert!(storage.get(b"data/users/1").unwrap().is_some());
        assert_eq!(outbox.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_outbox_retry_count() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending[0].retry_count, 0);

        outbox.increment_retry("op-123").unwrap();
        outbox.increment_retry("op-123").unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending[0].retry_count, 2);
    }

    #[test]
    fn test_outbox_dead_letter() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        assert_eq!(outbox.pending_count().unwrap(), 1);
        assert_eq!(outbox.dead_letter_count().unwrap(), 0);

        outbox.move_to_dead_letter("op-123").unwrap();

        assert_eq!(outbox.pending_count().unwrap(), 0);
        assert_eq!(outbox.dead_letter_count().unwrap(), 1);

        let dead = outbox.dead_letter_entries().unwrap();
        assert_eq!(dead[0].operation_id, "op-123");
        assert_eq!(dead[0].events[0].entity, "users");
    }

    #[test]
    fn test_outbox_created_at() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let event = ChangeEvent::create(
            "users".to_string(),
            "1".to_string(),
            serde_json::json!({"name": "Alice"}),
        );

        let mut batch = storage.batch();
        outbox.enqueue_event(&mut batch, "op-123", &event);
        batch.commit().unwrap();

        let pending = outbox.pending_events().unwrap();
        assert!(pending[0].created_at > 0);
    }

    #[test]
    fn test_outbox_update_dispatched_count() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let events = vec![
            ChangeEvent::create("users".to_string(), "1".to_string(), serde_json::json!({})),
            ChangeEvent::create("users".to_string(), "2".to_string(), serde_json::json!({})),
            ChangeEvent::create("users".to_string(), "3".to_string(), serde_json::json!({})),
        ];

        let mut batch = storage.batch();
        outbox.enqueue_events(&mut batch, "op-partial", &events);
        batch.commit().unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending[0].dispatched_count, 0);
        assert_eq!(pending[0].retry_count, 0);

        outbox.update_dispatched_count("op-partial", 1).unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending[0].dispatched_count, 1);
        assert_eq!(pending[0].retry_count, 1);
        assert_eq!(pending[0].events.len(), 3);

        outbox.update_dispatched_count("op-partial", 2).unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending[0].dispatched_count, 2);
        assert_eq!(pending[0].retry_count, 2);
    }

    #[test]
    fn test_outbox_backward_compat_missing_dispatched_count() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = Arc::new(Storage::with_backend(backend));
        let outbox = Outbox::new(Arc::clone(&storage));

        let legacy = serde_json::json!({
            "events": [{"sequence": 0, "entity": "users", "id": "1", "operation": "Create", "data": {}}],
            "retry_count": 3,
            "created_at": 1000
        });
        storage
            .insert(b"_outbox/legacy-op", &serde_json::to_vec(&legacy).unwrap())
            .unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].dispatched_count, 0);
        assert_eq!(pending[0].retry_count, 3);
    }
}
