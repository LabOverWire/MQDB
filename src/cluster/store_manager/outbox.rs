// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::error::Result;
use crate::storage::StorageBackend;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const OUTBOX_PREFIX: &[u8] = b"_outbox/";
const CASCADE_PREFIX: &[u8] = b"_cascade/";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEntry {
    topic: String,
    payload: Vec<u8>,
    user_properties: Vec<(String, String)>,
    created_at: u64,
}

pub struct ClusterOutbox {
    backend: Arc<dyn StorageBackend>,
}

impl ClusterOutbox {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    pub fn build_entry(
        operation_id: &str,
        topic: &str,
        payload: &[u8],
        user_properties: &[(String, String)],
    ) -> (Vec<u8>, Vec<u8>) {
        let key = format!("_outbox/{operation_id}");
        let stored = StoredEntry {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            user_properties: user_properties.to_vec(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let value = serde_json::to_vec(&stored).unwrap_or_default();
        (key.into_bytes(), value)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn mark_delivered(&self, operation_id: &str) -> Result<()> {
        let key = format!("_outbox/{operation_id}");
        self.backend.remove(key.as_bytes())
    }

    #[allow(clippy::missing_errors_doc)]
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
    },
    SetNull {
        entity: String,
        id: String,
        field: String,
    },
}

#[derive(Debug, Clone)]
pub struct CascadePendingEntry {
    pub operation_id: String,
    pub operations: Vec<CascadeRemoteOp>,
}

impl ClusterOutbox {
    pub fn build_cascade_entry(operation_id: &str, ops: &[CascadeRemoteOp]) -> (Vec<u8>, Vec<u8>) {
        let key = format!("_cascade/{operation_id}");
        let stored = StoredCascadeEntry {
            operations: ops.to_vec(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let value = serde_json::to_vec(&stored).unwrap_or_default();
        (key.into_bytes(), value)
    }

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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
pub struct OutboxPayload {
    pub operation_id: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl OutboxPayload {
    #[must_use]
    pub fn new(
        operation_id: String,
        topic: &str,
        payload: &[u8],
        user_properties: &[(String, String)],
    ) -> Self {
        let (key, value) =
            ClusterOutbox::build_entry(&operation_id, topic, payload, user_properties);
        Self {
            operation_id,
            key,
            value,
        }
    }
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
    use crate::storage::MemoryBackend;

    #[test]
    fn build_and_recover() {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let outbox = ClusterOutbox::new(Arc::clone(&backend));

        let (key, value) = ClusterOutbox::build_entry(
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

        let (key, value) =
            ClusterOutbox::build_entry("op-2", "$DB/entity/users/abc/deleted", b"{}", &[]);
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
            let (key, value) = ClusterOutbox::build_entry(
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
            },
            CascadeRemoteOp::SetNull {
                entity: "invoices".to_string(),
                id: "inv-2".to_string(),
                field: "order_id".to_string(),
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

        let (ok, ov) = ClusterOutbox::build_entry("ev-1", "topic", b"data", &[]);
        backend.insert(&ok, &ov).unwrap();

        let ops = vec![CascadeRemoteOp::Delete {
            entity: "x".to_string(),
            id: "1".to_string(),
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
        }];
        let payload = CascadeOutboxPayload::new("cas-4".to_string(), &ops);
        assert!(payload.key.starts_with(b"_cascade/cas-4"));
        assert!(!payload.value.is_empty());
    }
}
