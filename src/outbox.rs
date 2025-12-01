use crate::error::Result;
use crate::events::ChangeEvent;
use crate::storage::{BatchWriter, Storage};
use std::sync::Arc;

const OUTBOX_PREFIX: &[u8] = b"_outbox/";

pub struct Outbox {
    storage: Arc<Storage>,
}

impl Outbox {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn enqueue_event(&self, batch: &mut BatchWriter, operation_id: &str, event: &ChangeEvent) {
        let key = format!("_outbox/{operation_id}");
        let value = serde_json::to_vec(event).unwrap_or_default();
        batch.insert(key.into_bytes(), value);
    }

    pub fn enqueue_events(
        &self,
        batch: &mut BatchWriter,
        operation_id: &str,
        events: &[ChangeEvent],
    ) {
        let key = format!("_outbox/{operation_id}");
        let value = serde_json::to_vec(events).unwrap_or_default();
        batch.insert(key.into_bytes(), value);
    }

    pub fn pending_events(&self) -> Result<Vec<OutboxEntry>> {
        let items = self.storage.prefix_scan(OUTBOX_PREFIX)?;
        let mut entries = Vec::new();

        for (key, value) in items {
            let key_str = String::from_utf8_lossy(&key);
            let operation_id = key_str
                .strip_prefix("_outbox/")
                .unwrap_or(&key_str)
                .to_string();

            if let Ok(event) = serde_json::from_slice::<ChangeEvent>(&value) {
                entries.push(OutboxEntry {
                    operation_id,
                    events: vec![event],
                });
            } else if let Ok(events) = serde_json::from_slice::<Vec<ChangeEvent>>(&value) {
                entries.push(OutboxEntry {
                    operation_id,
                    events,
                });
            }
        }

        Ok(entries)
    }

    pub fn mark_delivered(&self, operation_id: &str) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        self.storage.remove(key.as_bytes())
    }

    pub fn pending_count(&self) -> Result<usize> {
        let items = self.storage.prefix_scan(OUTBOX_PREFIX)?;
        Ok(items.len())
    }
}

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub operation_id: String,
    pub events: Vec<ChangeEvent>,
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
            ChangeEvent::delete("users".to_string(), "1".to_string()),
            ChangeEvent::delete("posts".to_string(), "10".to_string()),
            ChangeEvent::delete("posts".to_string(), "11".to_string()),
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
}
