// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::PartitionId;
use crate::cluster::protocol::{ChangeEventRecord, EventId};
use mqdb_core::error::Result;
use mqdb_core::storage::StorageBackend;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const OUTBOX_PREFIX: &[u8] = b"_outbox/";
const CASCADE_PREFIX: &[u8] = b"_cascade/";
const CHANGE_EVENT_PREFIX: &[u8] = b"_evt/";
const CHANGE_EVENT_KEY_LEN: usize = CHANGE_EVENT_PREFIX.len() + 2 + 8 + 8;

fn change_event_partition_prefix(partition: PartitionId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(CHANGE_EVENT_PREFIX.len() + 2);
    prefix.extend_from_slice(CHANGE_EVENT_PREFIX);
    prefix.extend_from_slice(&partition.get().to_be_bytes());
    prefix
}

fn change_event_key(partition: PartitionId, id: EventId) -> Vec<u8> {
    let mut key = change_event_partition_prefix(partition);
    key.extend_from_slice(&id.epoch.to_be_bytes());
    key.extend_from_slice(&id.sequence.to_be_bytes());
    key
}

fn parse_change_event_key(key: &[u8]) -> Option<(PartitionId, EventId)> {
    if key.len() != CHANGE_EVENT_KEY_LEN {
        return None;
    }
    let rest = key.strip_prefix(CHANGE_EVENT_PREFIX)?;
    let (partition, rest) = rest.split_first_chunk::<2>()?;
    let (epoch, rest) = rest.split_first_chunk::<8>()?;
    let sequence: &[u8; 8] = rest.try_into().ok()?;
    Some((
        PartitionId::new(u16::from_be_bytes(*partition))?,
        EventId::new(u64::from_be_bytes(*epoch), u64::from_be_bytes(*sequence)),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredChangeEvent {
    pub partition: PartitionId,
    pub id: EventId,
    pub record: ChangeEventRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEntry {
    topic: String,
    payload: Vec<u8>,
    user_properties: Vec<(String, String)>,
    created_at: u64,
}

#[derive(Clone)]
pub struct ClusterOutbox {
    backend: Arc<dyn StorageBackend>,
}

impl ClusterOutbox {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn change_event_entry(
        partition: PartitionId,
        id: EventId,
        record: &ChangeEventRecord,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        Some((change_event_key(partition, id), record.to_bytes()?))
    }

    /// # Errors
    /// Returns an error if the storage scan fails.
    pub fn stored_change_events(&self) -> Result<Vec<StoredChangeEvent>> {
        Ok(Self::stored_from_items(
            self.backend.prefix_scan(CHANGE_EVENT_PREFIX)?,
        ))
    }

    /// # Errors
    /// Returns an error if the storage scan fails.
    pub fn stored_change_events_for(
        &self,
        partition: PartitionId,
    ) -> Result<Vec<StoredChangeEvent>> {
        Ok(Self::stored_from_items(
            self.backend
                .prefix_scan(&change_event_partition_prefix(partition))?,
        ))
    }

    fn stored_from_items(items: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<StoredChangeEvent> {
        items
            .into_iter()
            .filter_map(|(key, value)| {
                let (partition, id) = parse_change_event_key(&key)?;
                Some(StoredChangeEvent {
                    partition,
                    id,
                    record: ChangeEventRecord::from_bytes(&value)?,
                })
            })
            .collect()
    }

    /// # Errors
    /// Returns an error if the storage scan or the batch commit fails.
    pub fn remove_change_events_through(
        &self,
        partition: PartitionId,
        through: EventId,
    ) -> Result<()> {
        let keys = self
            .backend
            .prefix_scan_keys(&change_event_partition_prefix(partition))?;
        let mut batch = self.backend.batch();
        for key in keys {
            if parse_change_event_key(&key).is_some_and(|(_, id)| id <= through) {
                batch.remove(key);
            }
        }
        batch.commit()
    }

    /// # Errors
    /// Returns an error if the storage scan or the batch commit fails.
    pub fn remove_change_events(&self, partition: PartitionId) -> Result<()> {
        let keys = self
            .backend
            .prefix_scan_keys(&change_event_partition_prefix(partition))?;
        let mut batch = self.backend.batch();
        for key in keys {
            batch.remove(key);
        }
        batch.commit()
    }

    /// # Errors
    /// Returns an error if the batch commit fails.
    pub fn store_change_events(&self, events: &[StoredChangeEvent]) -> Result<()> {
        let mut batch = self.backend.batch();
        for event in events {
            if let Some((key, value)) =
                Self::change_event_entry(event.partition, event.id, &event.record)
            {
                batch.insert(key, value);
            }
        }
        batch.commit()
    }

    #[must_use]
    pub fn encode_change_events(events: &[StoredChangeEvent]) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::try_from(events.len()).ok()?.to_be_bytes());
        for event in events {
            let record = event.record.to_bytes()?;
            buf.extend_from_slice(&event.partition.get().to_be_bytes());
            buf.extend_from_slice(&event.id.epoch.to_be_bytes());
            buf.extend_from_slice(&event.id.sequence.to_be_bytes());
            buf.extend_from_slice(&u32::try_from(record.len()).ok()?.to_be_bytes());
            buf.extend_from_slice(&record);
        }
        Some(buf)
    }

    #[must_use]
    pub fn decode_change_events(bytes: &[u8]) -> Option<Vec<StoredChangeEvent>> {
        let (count, mut rest) = bytes.split_first_chunk::<4>()?;
        let events = (0..u32::from_be_bytes(*count))
            .map(|_| {
                let (partition, after) = rest.split_first_chunk::<2>()?;
                let (epoch, after) = after.split_first_chunk::<8>()?;
                let (sequence, after) = after.split_first_chunk::<8>()?;
                let (len, after) = after.split_first_chunk::<4>()?;
                let len = usize::try_from(u32::from_be_bytes(*len)).ok()?;
                let record = after.get(..len)?;
                rest = after.get(len..)?;
                Some(StoredChangeEvent {
                    partition: PartitionId::new(u16::from_be_bytes(*partition))?,
                    id: EventId::new(u64::from_be_bytes(*epoch), u64::from_be_bytes(*sequence)),
                    record: ChangeEventRecord::from_bytes(record)?,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        rest.is_empty().then_some(events)
    }

    /// # Errors
    /// Returns an error if the storage remove fails.
    pub fn mark_delivered(&self, operation_id: &str) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        self.backend.remove(key.as_bytes())
    }

    /// # Errors
    /// Returns an error if the storage scan fails.
    pub fn pending_events(&self) -> Result<Vec<ClusterOutboxEntry>> {
        let items = self.backend.prefix_scan(OUTBOX_PREFIX)?;
        let mut entries = Vec::new();

        for (key, value) in items {
            let key_str = String::from_utf8_lossy(&key);
            let operation_id = key_str
                .strip_prefix("_outbox/")
                .unwrap_or(&key_str)
                .to_string();

            if let Ok(stored) = serde_json::from_slice::<StoredEntry>(&value) {
                entries.push(ClusterOutboxEntry {
                    operation_id,
                    topic: stored.topic,
                    payload: stored.payload,
                    user_properties: stored.user_properties,
                });
            }
        }

        Ok(entries)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCascadeEntry {
    pub operations: Vec<CascadeRemoteOp>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CascadeRemoteOp {
    Delete {
        entity: String,
        id: String,
        #[serde(default)]
        sender: Option<String>,
    },
    SetNull {
        entity: String,
        id: String,
        field: String,
        expected_value: String,
    },
    GuardedDelete {
        entity: String,
        id: String,
        field: String,
        expected_value: String,
        #[serde(default)]
        sender: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct CascadePendingEntry {
    pub operation_id: String,
    pub operations: Vec<CascadeRemoteOp>,
}

impl ClusterOutbox {
    #[must_use]
    pub fn build_cascade_entry(operation_id: &str, ops: &[CascadeRemoteOp]) -> (Vec<u8>, Vec<u8>) {
        let key = format!("_cascade/{operation_id}");
        let stored = StoredCascadeEntry {
            operations: ops.to_vec(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        };
        let value = serde_json::to_vec(&stored).unwrap_or_default();
        (key.into_bytes(), value)
    }

    /// # Errors
    /// Returns an error if the storage scan fails.
    pub fn pending_cascade_ops(&self) -> Result<Vec<CascadePendingEntry>> {
        let items = self.backend.prefix_scan(CASCADE_PREFIX)?;
        let mut entries = Vec::new();
        for (key, value) in items {
            let key_str = String::from_utf8_lossy(&key);
            let operation_id = key_str
                .strip_prefix("_cascade/")
                .unwrap_or(&key_str)
                .to_string();
            if let Ok(stored) = serde_json::from_slice::<StoredCascadeEntry>(&value) {
                entries.push(CascadePendingEntry {
                    operation_id,
                    operations: stored.operations,
                });
            }
        }
        Ok(entries)
    }

    /// # Errors
    /// Returns an error if the storage remove fails.
    pub fn mark_cascade_delivered(&self, operation_id: &str) -> Result<()> {
        let key = format!("_cascade/{operation_id}");
        self.backend.remove(key.as_bytes())
    }
}

impl std::fmt::Debug for ClusterOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterOutbox").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct ClusterOutboxEntry {
    pub operation_id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct CascadeOutboxPayload {
    pub operation_id: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl CascadeOutboxPayload {
    #[must_use]
    pub fn new(operation_id: String, ops: &[CascadeRemoteOp]) -> Self {
        let (key, value) = ClusterOutbox::build_cascade_entry(&operation_id, ops);
        Self {
            operation_id,
            key,
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqdb_core::storage::MemoryBackend;

    fn legacy_entry(
        operation_id: &str,
        topic: &str,
        payload: &[u8],
        user_properties: &[(String, String)],
    ) -> (Vec<u8>, Vec<u8>) {
        let stored = StoredEntry {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            user_properties: user_properties.to_vec(),
            created_at: 0,
        };
        (
            format!("_outbox/{operation_id}").into_bytes(),
            serde_json::to_vec(&stored).unwrap(),
        )
    }

    fn stored(partition: u16, epoch: u64, sequence: u64) -> StoredChangeEvent {
        StoredChangeEvent {
            partition: PartitionId::new(partition).unwrap(),
            id: EventId::new(epoch, sequence),
            record: ChangeEventRecord {
                topic: format!("$DB/item/events/{sequence}"),
                payload: b"{}".to_vec(),
                user_properties: vec![("x-origin-client-id".to_string(), "c".to_string())],
                order_key: format!("item/{sequence}"),
            },
        }
    }

    #[test]
    fn change_events_are_read_back_per_partition_in_id_order() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));
        let events = [
            stored(3, 2, 1),
            stored(3, 1, 9),
            stored(4, 1, 1),
            stored(3, 1, 10),
        ];
        outbox.store_change_events(&events).unwrap();

        let for_three = outbox
            .stored_change_events_for(PartitionId::new(3).unwrap())
            .unwrap();
        assert_eq!(
            for_three.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![EventId::new(1, 9), EventId::new(1, 10), EventId::new(2, 1)]
        );
        assert_eq!(for_three[0], stored(3, 1, 9));
        assert_eq!(outbox.stored_change_events().unwrap().len(), 4);
    }

    #[test]
    fn removing_change_events_through_an_id_keeps_later_ones_and_other_partitions() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));
        outbox
            .store_change_events(&[
                stored(3, 1, 5),
                stored(3, 1, 6),
                stored(3, 2, 1),
                stored(5, 1, 1),
            ])
            .unwrap();

        outbox
            .remove_change_events_through(PartitionId::new(3).unwrap(), EventId::new(1, 6))
            .unwrap();
        let remaining: Vec<_> = outbox
            .stored_change_events()
            .unwrap()
            .into_iter()
            .map(|e| (e.partition.get(), e.id))
            .collect();
        assert_eq!(
            remaining,
            vec![(3, EventId::new(2, 1)), (5, EventId::new(1, 1))]
        );

        outbox
            .remove_change_events(PartitionId::new(5).unwrap())
            .unwrap();
        assert_eq!(outbox.stored_change_events().unwrap().len(), 1);
    }

    #[test]
    fn change_events_do_not_show_up_as_legacy_outbox_entries() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));
        outbox.store_change_events(&[stored(1, 1, 1)]).unwrap();
        assert!(outbox.pending_events().unwrap().is_empty());
    }

    #[test]
    fn build_and_recover() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (key, value) = legacy_entry(
            "op-1",
            "$DB/entity/users/abc/created",
            b"{\"id\":\"abc\"}",
            &[("x-origin-client-id".to_string(), "client-1".to_string())],
        );

        backend.insert(&key, &value).unwrap();

        let pending = outbox.pending_events().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation_id, "op-1");
        assert_eq!(pending[0].topic, "$DB/entity/users/abc/created");
        assert_eq!(pending[0].payload, b"{\"id\":\"abc\"}");
        assert_eq!(pending[0].user_properties.len(), 1);
        assert_eq!(pending[0].user_properties[0].0, "x-origin-client-id");
    }

    #[test]
    fn mark_delivered_removes_entry() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (key, value) = legacy_entry("op-2", "$DB/entity/users/abc/deleted", b"{}", &[]);
        backend.insert(&key, &value).unwrap();

        assert_eq!(outbox.pending_events().unwrap().len(), 1);
        outbox.mark_delivered("op-2").unwrap();
        assert!(outbox.pending_events().unwrap().is_empty());
    }

    #[test]
    fn multiple_entries() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        for i in 0..5 {
            let (key, value) = legacy_entry(
                &format!("op-{i}"),
                &format!("$DB/entity/users/{i}/created"),
                b"{}",
                &[],
            );
            backend.insert(&key, &value).unwrap();
        }

        assert_eq!(outbox.pending_events().unwrap().len(), 5);
        outbox.mark_delivered("op-2").unwrap();
        assert_eq!(outbox.pending_events().unwrap().len(), 4);
    }

    #[test]
    fn cascade_entry_roundtrip() {
        let ops = vec![
            CascadeRemoteOp::Delete {
                entity: "orders".to_string(),
                id: "o-1".to_string(),
                sender: None,
            },
            CascadeRemoteOp::SetNull {
                entity: "invoices".to_string(),
                id: "inv-2".to_string(),
                field: "order_id".to_string(),
                expected_value: "o-1".to_string(),
            },
        ];

        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (key, value) = ClusterOutbox::build_cascade_entry("cas-1", &ops);
        backend.insert(&key, &value).unwrap();

        let pending = outbox.pending_cascade_ops().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation_id, "cas-1");
        assert_eq!(pending[0].operations.len(), 2);
    }

    #[test]
    fn cascade_mark_delivered() {
        let ops = vec![CascadeRemoteOp::Delete {
            entity: "items".to_string(),
            id: "i-1".to_string(),
            sender: None,
        }];

        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (key, value) = ClusterOutbox::build_cascade_entry("cas-2", &ops);
        backend.insert(&key, &value).unwrap();

        assert_eq!(outbox.pending_cascade_ops().unwrap().len(), 1);
        outbox.mark_cascade_delivered("cas-2").unwrap();
        assert!(outbox.pending_cascade_ops().unwrap().is_empty());
    }

    #[test]
    fn cascade_does_not_interfere_with_outbox() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (ok, ov) = legacy_entry("ev-1", "topic", b"data", &[]);
        backend.insert(&ok, &ov).unwrap();

        let ops = vec![CascadeRemoteOp::Delete {
            entity: "x".to_string(),
            id: "1".to_string(),
            sender: None,
        }];
        let (ck, cv) = ClusterOutbox::build_cascade_entry("cas-3", &ops);
        backend.insert(&ck, &cv).unwrap();

        assert_eq!(outbox.pending_events().unwrap().len(), 1);
        assert_eq!(outbox.pending_cascade_ops().unwrap().len(), 1);
    }

    #[test]
    fn cascade_outbox_payload_builds_correctly() {
        let ops = vec![CascadeRemoteOp::Delete {
            entity: "users".to_string(),
            id: "u-1".to_string(),
            sender: None,
        }];
        let payload = CascadeOutboxPayload::new("cas-4".to_string(), &ops);
        assert!(payload.key.starts_with(b"_cascade/cas-4"));
        assert!(!payload.value.is_empty());
    }
}
