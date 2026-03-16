// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::Operation;
use super::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

const TTL_MS: u64 = 24 * 60 * 60 * 1000;
const GRACE_PERIOD_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const MAX_PER_PARTITION: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, BeBytes, Default)]
#[repr(u8)]
pub enum IdempotencyStatus {
    #[default]
    Processing = 0,
    Committed = 1,
    Expired = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct IdempotencyRecord {
    pub version: u8,
    pub status: IdempotencyStatus,
    pub partition: u16,
    pub epoch: u64,
    pub timestamp: u64,
    pub entity_len: u8,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub id_len: u16,
    #[FromField(id_len)]
    pub id: Vec<u8>,
    pub key_len: u16,
    #[FromField(key_len)]
    pub idempotency_key: Vec<u8>,
    pub response_len: u32,
    #[FromField(response_len)]
    pub response: Vec<u8>,
}

impl IdempotencyRecord {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn processing(
        idempotency_key: &str,
        partition: PartitionId,
        epoch: Epoch,
        entity: &str,
        id: &str,
        timestamp: u64,
    ) -> Self {
        let key_bytes = idempotency_key.as_bytes().to_vec();
        let entity_bytes = entity.as_bytes().to_vec();
        let id_bytes = id.as_bytes().to_vec();
        Self {
            version: 1,
            status: IdempotencyStatus::Processing,
            partition: partition.get(),
            epoch: epoch.get(),
            timestamp,
            entity_len: entity_bytes.len() as u8,
            entity: entity_bytes,
            id_len: id_bytes.len() as u16,
            id: id_bytes,
            key_len: key_bytes.len() as u16,
            idempotency_key: key_bytes,
            response_len: 0,
            response: Vec::new(),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn mark_committed(&mut self, response: Vec<u8>) {
        self.status = IdempotencyStatus::Committed;
        self.response_len = response.len() as u32;
        self.response = response;
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn id_str(&self) -> &str {
        std::str::from_utf8(&self.id).unwrap_or("")
    }

    #[must_use]
    pub fn key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }

    #[must_use]
    pub fn partition_id(&self) -> Option<PartitionId> {
        PartitionId::new(self.partition)
    }

    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.status == IdempotencyStatus::Committed
    }

    #[must_use]
    pub fn is_processing(&self) -> bool {
        self.status == IdempotencyStatus::Processing
    }

    #[must_use]
    pub fn should_mark_expired(&self, now: u64) -> bool {
        self.is_committed() && now.saturating_sub(self.timestamp) > TTL_MS
    }

    #[must_use]
    pub fn should_delete(&self, now: u64) -> bool {
        self.status == IdempotencyStatus::Expired
            && now.saturating_sub(self.timestamp) > TTL_MS + GRACE_PERIOD_MS
    }

    pub fn mark_expired(&mut self) {
        self.status = IdempotencyStatus::Expired;
    }

    #[must_use]
    pub fn is_expired_status(&self) -> bool {
        self.status == IdempotencyStatus::Expired
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyError {
    AlreadyProcessing,
    ParameterMismatch,
    PartitionFull,
    SerializationError,
    Expired,
}

impl std::fmt::Display for IdempotencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyProcessing => {
                write!(f, "write with this idempotency key is already processing")
            }
            Self::ParameterMismatch => {
                write!(f, "idempotency key was used with different entity/id")
            }
            Self::PartitionFull => write!(f, "partition has reached maximum idempotency records"),
            Self::SerializationError => write!(f, "serialization error"),
            Self::Expired => write!(f, "idempotency key has expired and cannot be retried"),
        }
    }
}

impl std::error::Error for IdempotencyError {}

#[derive(Debug, Clone)]
pub enum IdempotencyCheck {
    Proceed,
    ReturnCached(Vec<u8>),
}

#[must_use]
pub fn idempotency_storage_key(partition: PartitionId, key: &str) -> String {
    format!("_idemp/p{}/{key}", partition.get())
}

type StoreKey = (u16, String);

pub struct IdempotencyStore {
    node_id: NodeId,
    records: RwLock<HashMap<StoreKey, IdempotencyRecord>>,
}

impl IdempotencyStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            records: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyProcessing`, `ParameterMismatch`, or `PartitionFull`.
    pub fn check_or_insert_processing(
        &self,
        idempotency_key: &str,
        partition: PartitionId,
        epoch: Epoch,
        entity: &str,
        id: &str,
        timestamp: u64,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        let store_key = (partition.get(), idempotency_key.to_string());
        let mut records = self.records.write().unwrap();

        if let Some(existing) = records.get(&store_key) {
            if existing.entity_str() != entity || existing.id_str() != id {
                return Err(IdempotencyError::ParameterMismatch);
            }

            match existing.status {
                IdempotencyStatus::Committed => {
                    return Ok(IdempotencyCheck::ReturnCached(existing.response.clone()));
                }
                IdempotencyStatus::Processing => {
                    return Err(IdempotencyError::AlreadyProcessing);
                }
                IdempotencyStatus::Expired => {
                    return Err(IdempotencyError::Expired);
                }
            }
        }

        let partition_count = records
            .keys()
            .filter(|(p, _)| *p == partition.get())
            .count();
        if partition_count >= MAX_PER_PARTITION {
            return Err(IdempotencyError::PartitionFull);
        }

        let record =
            IdempotencyRecord::processing(idempotency_key, partition, epoch, entity, id, timestamp);
        records.insert(store_key, record);
        Ok(IdempotencyCheck::Proceed)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn mark_committed(&self, partition: PartitionId, idempotency_key: &str, response: Vec<u8>) {
        let store_key = (partition.get(), idempotency_key.to_string());
        let mut records = self.records.write().unwrap();
        if let Some(record) = records.get_mut(&store_key) {
            record.mark_committed(response);
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn remove_processing(&self, partition: PartitionId, idempotency_key: &str) {
        let store_key = (partition.get(), idempotency_key.to_string());
        let mut records = self.records.write().unwrap();
        if records
            .get(&store_key)
            .is_some_and(IdempotencyRecord::is_processing)
        {
            records.remove(&store_key);
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, partition: PartitionId, idempotency_key: &str) -> Option<IdempotencyRecord> {
        let store_key = (partition.get(), idempotency_key.to_string());
        self.records.read().unwrap().get(&store_key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired(&self, now: u64) -> usize {
        let mut records = self.records.write().unwrap();

        for record in records.values_mut() {
            if record.should_mark_expired(now) {
                record.mark_expired();
            }
        }

        let before = records.len();
        records.retain(|_, record| !record.should_delete(now));
        before - records.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.records.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count_for_partition(&self, partition: PartitionId) -> usize {
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|((p, _), _)| *p == partition.get())
            .count()
    }

    #[must_use]
    pub fn serialize(record: &IdempotencyRecord) -> Vec<u8> {
        record.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<IdempotencyRecord> {
        IdempotencyRecord::try_from_be_bytes(bytes)
            .ok()
            .map(|(r, _)| r)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if ID parsing or deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), IdempotencyError> {
        let parts: Vec<&str> = id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(IdempotencyError::SerializationError);
        }
        let partition_num: u16 = parts[0]
            .parse()
            .map_err(|_| IdempotencyError::SerializationError)?;
        let idemp_key = parts[1];

        match operation {
            Operation::Insert | Operation::Update => {
                let record = Self::deserialize(data).ok_or(IdempotencyError::SerializationError)?;
                let store_key = (partition_num, idemp_key.to_string());
                self.records.write().unwrap().insert(store_key, record);
                Ok(())
            }
            Operation::Delete => {
                let store_key = (partition_num, idemp_key.to_string());
                self.records.write().unwrap().remove(&store_key);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let records = self.records.read().unwrap();
        let partition_records: Vec<_> = records
            .iter()
            .filter(|((p, _), _)| *p == partition.get())
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_records.len() as u32).to_be_bytes());

        for ((part_num, idemp_key), record) in partition_records {
            let id = format!("{part_num}:{idemp_key}");
            let id_bytes = id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = record.to_be_bytes();
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if UTF-8 parsing fails.
    pub fn import_records(&self, data: &[u8]) -> Result<usize, IdempotencyError> {
        if data.len() < 4 {
            return Ok(0);
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut offset = 4;
        let mut imported = 0;

        for _ in 0..count {
            if offset + 2 > data.len() {
                break;
            }
            let id_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + id_len > data.len() {
                break;
            }
            let id = std::str::from_utf8(&data[offset..offset + id_len])
                .map_err(|_| IdempotencyError::SerializationError)?;
            offset += id_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let record_data = &data[offset..offset + data_len];
            offset += data_len;

            let parts: Vec<&str> = id.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let partition_num: u16 = match parts[0].parse() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let idemp_key = parts[1];

            if let Some(record) = Self::deserialize(record_data) {
                let store_key = (partition_num, idemp_key.to_string());
                self.records.write().unwrap().insert(store_key, record);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut records = self.records.write().unwrap();
        let before = records.len();
        records.retain(|(p, _), _| *p != partition.get());
        before - records.len()
    }
}

impl std::fmt::Debug for IdempotencyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdempotencyStore")
            .field("node_id", &self.node_id)
            .field("count", &self.count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    fn epoch(n: u64) -> Epoch {
        Epoch::new(n)
    }

    #[test]
    fn idempotency_record_bebytes_roundtrip() {
        let record =
            IdempotencyRecord::processing("req-123", partition(10), epoch(5), "users", "456", 1000);
        let bytes = IdempotencyStore::serialize(&record);
        let parsed = IdempotencyStore::deserialize(&bytes).unwrap();

        assert_eq!(record.key_str(), parsed.key_str());
        assert_eq!(record.entity_str(), parsed.entity_str());
        assert_eq!(record.id_str(), parsed.id_str());
        assert_eq!(record.partition, parsed.partition);
        assert_eq!(record.epoch, parsed.epoch);
        assert_eq!(record.status, parsed.status);
    }

    #[test]
    fn check_or_insert_processing_new_key() {
        let store = IdempotencyStore::new(node(1));

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );

        assert!(matches!(result, Ok(IdempotencyCheck::Proceed)));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn check_or_insert_processing_duplicate_key_processing() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1001,
        );

        assert!(matches!(result, Err(IdempotencyError::AlreadyProcessing)));
    }

    #[test]
    fn check_or_insert_processing_committed_returns_cached() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );
        store.mark_committed(partition(10), "req-123", b"response-data".to_vec());

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1001,
        );

        match result {
            Ok(IdempotencyCheck::ReturnCached(response)) => {
                assert_eq!(response, b"response-data");
            }
            _ => panic!("expected ReturnCached"),
        }
    }

    #[test]
    fn check_or_insert_processing_parameter_mismatch() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "posts",
            "456",
            1001,
        );

        assert!(matches!(result, Err(IdempotencyError::ParameterMismatch)));
    }

    #[test]
    fn remove_processing_only_removes_processing_status() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );
        store.remove_processing(partition(10), "req-123");
        assert_eq!(store.count(), 0);

        let _ = store.check_or_insert_processing(
            "req-456",
            partition(10),
            epoch(5),
            "users",
            "789",
            1000,
        );
        store.mark_committed(partition(10), "req-456", b"response".to_vec());
        store.remove_processing(partition(10), "req-456");
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn cleanup_marks_expired_after_ttl() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-old",
            partition(10),
            epoch(5),
            "users",
            "1",
            1000,
        );
        store.mark_committed(partition(10), "req-old", b"old".to_vec());

        let _ = store.check_or_insert_processing(
            "req-new",
            partition(10),
            epoch(5),
            "users",
            "2",
            TTL_MS + 2000,
        );
        store.mark_committed(partition(10), "req-new", b"new".to_vec());

        let _ = store.check_or_insert_processing(
            "req-processing",
            partition(10),
            epoch(5),
            "users",
            "3",
            500,
        );

        assert_eq!(store.count(), 3);

        let removed = store.cleanup_expired(TTL_MS + 2000);
        assert_eq!(removed, 0);
        assert_eq!(store.count(), 3);

        let old_record = store.get(partition(10), "req-old").unwrap();
        assert!(old_record.is_expired_status());

        let new_record = store.get(partition(10), "req-new").unwrap();
        assert!(new_record.is_committed());

        let processing_record = store.get(partition(10), "req-processing").unwrap();
        assert!(processing_record.is_processing());
    }

    #[test]
    fn cleanup_deletes_after_grace_period() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing(
            "req-old",
            partition(10),
            epoch(5),
            "users",
            "1",
            1000,
        );
        store.mark_committed(partition(10), "req-old", b"old".to_vec());

        store.cleanup_expired(TTL_MS + 2000);
        assert_eq!(store.count(), 1);

        let removed = store.cleanup_expired(TTL_MS + GRACE_PERIOD_MS + 2000);
        assert_eq!(removed, 1);
        assert_eq!(store.count(), 0);
        assert!(store.get(partition(10), "req-old").is_none());
    }

    #[test]
    fn retry_after_expiry_returns_error() {
        let store = IdempotencyStore::new(node(1));

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            1000,
        );
        assert!(matches!(result, Ok(IdempotencyCheck::Proceed)));

        store.mark_committed(partition(10), "req-123", b"response-data".to_vec());

        store.cleanup_expired(TTL_MS + 2000);

        let result = store.check_or_insert_processing(
            "req-123",
            partition(10),
            epoch(5),
            "users",
            "456",
            TTL_MS + 3000,
        );
        assert!(matches!(result, Err(IdempotencyError::Expired)));
    }

    #[test]
    fn export_import_roundtrip() {
        let store1 = IdempotencyStore::new(node(1));

        let _ =
            store1.check_or_insert_processing("req-1", partition(5), epoch(1), "users", "a", 1000);
        store1.mark_committed(partition(5), "req-1", b"resp1".to_vec());

        let _ =
            store1.check_or_insert_processing("req-2", partition(5), epoch(2), "posts", "b", 2000);
        store1.mark_committed(partition(5), "req-2", b"resp2".to_vec());

        let exported = store1.export_for_partition(partition(5));

        let store2 = IdempotencyStore::new(node(2));
        let imported = store2.import_records(&exported).unwrap();
        assert_eq!(imported, 2);

        let r1 = store2.get(partition(5), "req-1").unwrap();
        assert_eq!(r1.entity_str(), "users");
        assert_eq!(r1.response, b"resp1");

        let r2 = store2.get(partition(5), "req-2").unwrap();
        assert_eq!(r2.entity_str(), "posts");
        assert_eq!(r2.response, b"resp2");
    }

    #[test]
    fn clear_partition() {
        let store = IdempotencyStore::new(node(1));

        let _ = store.check_or_insert_processing("req-1", partition(5), epoch(1), "e", "1", 1000);
        let _ = store.check_or_insert_processing("req-2", partition(5), epoch(1), "e", "2", 1000);
        let _ = store.check_or_insert_processing("req-3", partition(10), epoch(1), "e", "3", 1000);

        assert_eq!(store.count(), 3);

        let cleared = store.clear_partition(partition(5));
        assert_eq!(cleared, 2);
        assert_eq!(store.count(), 1);
        assert!(store.get(partition(5), "req-1").is_none());
        assert!(store.get(partition(10), "req-3").is_some());
    }
}
