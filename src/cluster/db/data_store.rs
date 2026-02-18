// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::partition::data_partition;
use crate::cluster::protocol::Operation;
use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct DbEntity {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub id_len: u16,
    #[FromField(id_len)]
    pub id: Vec<u8>,
    pub timestamp_ms: u64,
    pub data_len: u32,
    #[FromField(data_len)]
    pub data: Vec<u8>,
}

impl DbEntity {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(entity: &str, id: &str, data: &[u8], timestamp_ms: u64) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let id_bytes = id.as_bytes().to_vec();
        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            id_len: id_bytes.len() as u16,
            id: id_bytes,
            timestamp_ms,
            data_len: data.len() as u32,
            data: data.to_vec(),
        }
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
    pub fn key(&self) -> String {
        format!("{}/{}", self.entity_str(), self.id_str())
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        data_partition(self.entity_str(), self.id_str())
    }
}

#[must_use]
pub fn db_data_key(entity: &str, id: &str) -> String {
    let partition = data_partition(entity, id);
    format!("_db_data/p{}/{}/{}", partition.get(), entity, id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbDataStoreError {
    NotFound,
    AlreadyExists,
    SerializationError,
}

impl std::fmt::Display for DbDataStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "entity not found"),
            Self::AlreadyExists => write!(f, "entity already exists"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for DbDataStoreError {}

pub struct DbDataStore {
    node_id: NodeId,
    entities: RwLock<HashMap<String, DbEntity>>,
}

impl DbDataStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            entities: RwLock::new(HashMap::new()),
        }
    }

    fn read_entities(&self) -> RwLockReadGuard<'_, HashMap<String, DbEntity>> {
        self.entities.read().expect("db entity lock poisoned")
    }

    fn write_entities(&self) -> RwLockWriteGuard<'_, HashMap<String, DbEntity>> {
        self.entities.write().expect("db entity lock poisoned")
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_entities().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if the entity already exists.
    pub fn create(
        &self,
        entity: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<DbEntity, DbDataStoreError> {
        let start = std::time::Instant::now();
        let key = format!("{entity}/{id}");
        let mut entities = self.write_entities();
        let lock_time = start.elapsed();

        if entities.contains_key(&key) {
            return Err(DbDataStoreError::AlreadyExists);
        }
        let db_entity = DbEntity::create(entity, id, data, timestamp_ms);
        entities.insert(key, db_entity.clone());

        let total_time = start.elapsed();
        if total_time.as_micros() > 100 {
            tracing::warn!(
                total_us = total_time.as_micros(),
                lock_us = lock_time.as_micros(),
                store_size = entities.len(),
                "slow DbDataStore::create"
            );
        }
        Ok(db_entity)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, entity: &str, id: &str) -> Option<DbEntity> {
        let key = format!("{entity}/{id}");
        self.read_entities().get(&key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the entity does not exist.
    pub fn update(
        &self,
        entity: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<DbEntity, DbDataStoreError> {
        let key = format!("{entity}/{id}");
        let mut entities = self.write_entities();
        if !entities.contains_key(&key) {
            return Err(DbDataStoreError::NotFound);
        }
        let db_entity = DbEntity::create(entity, id, data, timestamp_ms);
        entities.insert(key, db_entity.clone());
        Ok(db_entity)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the entity does not exist.
    pub fn delete(&self, entity: &str, id: &str) -> Result<DbEntity, DbDataStoreError> {
        let key = format!("{entity}/{id}");
        let mut entities = self.write_entities();
        entities.remove(&key).ok_or(DbDataStoreError::NotFound)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn upsert(&self, entity: &str, id: &str, data: &[u8], timestamp_ms: u64) -> DbEntity {
        let key = format!("{entity}/{id}");
        let mut entities = self.write_entities();
        let db_entity = DbEntity::create(entity, id, data, timestamp_ms);
        entities.insert(key, db_entity.clone());
        db_entity
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn list(&self, entity_type: &str) -> Vec<DbEntity> {
        let prefix = format!("{entity_type}/");
        let entities = self.read_entities();
        entities
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn list_for_partition(&self, entity_type: &str, partition: PartitionId) -> Vec<DbEntity> {
        let prefix = format!("{entity_type}/");
        let entities = self.read_entities();
        entities
            .iter()
            .filter(|(k, v)| k.starts_with(&prefix) && v.partition() == partition)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.read_entities().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut entities = self.write_entities();
        let before = entities.len();
        entities.retain(|_, v| v.partition() != partition);
        before - entities.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn query(
        &self,
        entity_type: Option<&str>,
        filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<DbEntity>, bool, Option<Vec<u8>>) {
        let entities = self.read_entities();

        let cursor_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = entities
            .iter()
            .filter(|(key, entity)| {
                if let Some(et) = entity_type
                    && entity.entity_str() != et
                {
                    return false;
                }

                if let Some(f) = filter
                    && !Self::matches_filter(entity, f)
                {
                    return false;
                }

                if let Some(ck) = cursor_key {
                    return key.as_str() > ck;
                }

                true
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        results.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        let limit_usize = limit as usize;
        let has_more = results.len() > limit_usize;
        let results: Vec<_> = results.into_iter().take(limit_usize).collect();

        let next_cursor = if has_more {
            results.last().map(|(k, _)| k.as_bytes().to_vec())
        } else {
            None
        };

        let entities: Vec<DbEntity> = results.into_iter().map(|(_, v)| v).collect();

        (entities, has_more, next_cursor)
    }

    fn matches_filter(entity: &DbEntity, filter: &str) -> bool {
        let filter = filter.trim();

        if let Some(value) = filter.strip_prefix("entity_type=") {
            let value = value.trim().trim_matches('"').trim_matches('\'');
            return entity.entity_str() == value;
        }

        if let Some(value) = filter.strip_prefix("id=") {
            let value = value.trim().trim_matches('"').trim_matches('\'');
            return entity.id_str() == value;
        }

        true
    }

    #[must_use]
    pub fn serialize(entity: &DbEntity) -> Vec<u8> {
        entity.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<DbEntity> {
        DbEntity::try_from_be_bytes(bytes).ok().map(|(e, _)| e)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), DbDataStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let entity = Self::deserialize(data).ok_or(DbDataStoreError::SerializationError)?;
                let key = entity.key();
                self.write_entities().insert(key, entity);
                Ok(())
            }
            Operation::Delete => {
                self.write_entities().remove(id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let entities = self.read_entities();
        let partition_entities: Vec<_> = entities
            .iter()
            .filter(|(_, v)| v.partition() == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_entities.len() as u32).to_be_bytes());

        for (key, entity) in partition_entities {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(key_bytes);

            let data = Self::serialize(entity);
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
    pub fn import_entities(&self, data: &[u8]) -> Result<usize, DbDataStoreError> {
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
            let key_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + key_len > data.len() {
                break;
            }
            let key = std::str::from_utf8(&data[offset..offset + key_len])
                .map_err(|_| DbDataStoreError::SerializationError)?;
            offset += key_len;

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
            let entity_data = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(entity) = Self::deserialize(entity_data) {
                self.write_entities().insert(key.to_string(), entity);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn cleanup_expired_ttl(&self, now_secs: u64) -> Vec<(String, String)> {
        let expired: Vec<(String, String, String)> = {
            let entities = self.read_entities();
            entities
                .iter()
                .filter_map(|(key, entity)| {
                    let data: serde_json::Value = serde_json::from_slice(&entity.data).ok()?;
                    let expires_at = data
                        .get("_expires_at")
                        .and_then(serde_json::Value::as_u64)?;
                    if expires_at <= now_secs {
                        Some((
                            key.clone(),
                            entity.entity_str().to_string(),
                            entity.id_str().to_string(),
                        ))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if expired.is_empty() {
            return Vec::new();
        }

        let mut entities = self.write_entities();
        let mut deleted = Vec::new();
        for (key, entity_name, entity_id) in expired {
            entities.remove(&key);
            deleted.push((entity_name, entity_id));
        }
        deleted
    }
}

impl std::fmt::Debug for DbDataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbDataStore")
            .field("node_id", &self.node_id)
            .field("entity_count", &self.count())
            .finish_non_exhaustive()
    }
}

type FkIndexKey = (String, String, String, String);

pub struct FkReverseIndex {
    index: Mutex<HashMap<FkIndexKey, HashSet<String>>>,
}

impl Default for FkReverseIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FkReverseIndex {
    #[must_use]
    pub fn new() -> Self {
        Self {
            index: Mutex::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn insert(
        &self,
        target_entity: &str,
        target_id: &str,
        source_entity: &str,
        source_field: &str,
        source_id: &str,
    ) {
        let key = (
            target_entity.to_string(),
            target_id.to_string(),
            source_entity.to_string(),
            source_field.to_string(),
        );
        self.index
            .lock()
            .expect("fk reverse index lock poisoned")
            .entry(key)
            .or_default()
            .insert(source_id.to_string());
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn remove(
        &self,
        target_entity: &str,
        target_id: &str,
        source_entity: &str,
        source_field: &str,
        source_id: &str,
    ) {
        let key = (
            target_entity.to_string(),
            target_id.to_string(),
            source_entity.to_string(),
            source_field.to_string(),
        );
        let mut map = self.index.lock().expect("fk reverse index lock poisoned");
        if let Some(set) = map.get_mut(&key) {
            set.remove(source_id);
            if set.is_empty() {
                map.remove(&key);
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn lookup(
        &self,
        target_entity: &str,
        target_id: &str,
        source_entity: &str,
        source_field: &str,
    ) -> Vec<String> {
        let key = (
            target_entity.to_string(),
            target_id.to_string(),
            source_entity.to_string(),
            source_field.to_string(),
        );
        self.index
            .lock()
            .expect("fk reverse index lock poisoned")
            .get(&key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for FkReverseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.index.lock().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("FkReverseIndex")
            .field("entries", &count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn db_entity_bebytes_roundtrip() {
        let entity = DbEntity::create("users", "123", b"{\"name\":\"Alice\"}", 1000);
        let bytes = DbDataStore::serialize(&entity);
        let parsed = DbDataStore::deserialize(&bytes).unwrap();

        assert_eq!(entity.entity, parsed.entity);
        assert_eq!(entity.id, parsed.id);
        assert_eq!(entity.data, parsed.data);
        assert_eq!(entity.timestamp_ms, parsed.timestamp_ms);
    }

    #[test]
    fn db_data_key_format() {
        let key = db_data_key("users", "123");
        assert!(key.starts_with("_db_data/p"));
        assert!(key.contains("/users/123"));
    }

    #[test]
    fn create_and_get() {
        let store = DbDataStore::new(node(1));

        store
            .create("users", "123", b"{\"name\":\"Alice\"}", 1000)
            .unwrap();
        let entity = store.get("users", "123").unwrap();

        assert_eq!(entity.entity_str(), "users");
        assert_eq!(entity.id_str(), "123");
        assert_eq!(entity.data, b"{\"name\":\"Alice\"}");
    }

    #[test]
    fn create_duplicate_fails() {
        let store = DbDataStore::new(node(1));

        store.create("users", "123", b"{}", 1000).unwrap();
        let result = store.create("users", "123", b"{}", 2000);

        assert_eq!(result, Err(DbDataStoreError::AlreadyExists));
    }

    #[test]
    fn update_existing() {
        let store = DbDataStore::new(node(1));

        store
            .create("users", "123", b"{\"name\":\"Alice\"}", 1000)
            .unwrap();
        store
            .update("users", "123", b"{\"name\":\"Bob\"}", 2000)
            .unwrap();

        let entity = store.get("users", "123").unwrap();
        assert_eq!(entity.data, b"{\"name\":\"Bob\"}");
        assert_eq!(entity.timestamp_ms, 2000);
    }

    #[test]
    fn update_missing_fails() {
        let store = DbDataStore::new(node(1));

        let result = store.update("users", "123", b"{}", 1000);
        assert_eq!(result, Err(DbDataStoreError::NotFound));
    }

    #[test]
    fn delete_existing() {
        let store = DbDataStore::new(node(1));

        store.create("users", "123", b"{}", 1000).unwrap();
        store.delete("users", "123").unwrap();

        assert!(store.get("users", "123").is_none());
    }

    #[test]
    fn delete_missing_fails() {
        let store = DbDataStore::new(node(1));

        let result = store.delete("users", "123");
        assert_eq!(result, Err(DbDataStoreError::NotFound));
    }

    #[test]
    fn list_by_entity_type() {
        let store = DbDataStore::new(node(1));

        store.create("users", "1", b"{}", 1000).unwrap();
        store.create("users", "2", b"{}", 1000).unwrap();
        store.create("orders", "1", b"{}", 1000).unwrap();

        let users = store.list("users");
        assert_eq!(users.len(), 2);

        let orders = store.list("orders");
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn export_import_roundtrip() {
        let store1 = DbDataStore::new(node(1));
        let store2 = DbDataStore::new(node(2));

        store1
            .create("users", "1", b"{\"name\":\"Alice\"}", 1000)
            .unwrap();
        store1
            .create("users", "2", b"{\"name\":\"Bob\"}", 2000)
            .unwrap();

        let entity1 = store1.get("users", "1").unwrap();
        let partition = entity1.partition();

        let exported = store1.export_for_partition(partition);
        let imported = store2.import_entities(&exported).unwrap();

        assert!(imported >= 1);
    }

    #[test]
    fn query_returns_all_entities() {
        let store = DbDataStore::new(node(1));

        store.create("users", "1", b"{}", 1000).unwrap();
        store.create("users", "2", b"{}", 1000).unwrap();
        store.create("orders", "1", b"{}", 1000).unwrap();

        let (results, has_more, cursor) = store.query(None, None, 100, None);
        assert_eq!(results.len(), 3);
        assert!(!has_more);
        assert!(cursor.is_none());
    }

    #[test]
    fn query_filters_by_entity_type() {
        let store = DbDataStore::new(node(1));

        store.create("users", "1", b"{}", 1000).unwrap();
        store.create("users", "2", b"{}", 1000).unwrap();
        store.create("orders", "1", b"{}", 1000).unwrap();

        let (results, _, _) = store.query(Some("users"), None, 100, None);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.entity_str() == "users"));
    }

    #[test]
    fn query_respects_limit_and_pagination() {
        let store = DbDataStore::new(node(1));

        for i in 0..5 {
            store
                .create("items", &format!("item{i}"), b"{}", 1000)
                .unwrap();
        }

        let (page1, has_more1, cursor1) = store.query(None, None, 2, None);
        assert_eq!(page1.len(), 2);
        assert!(has_more1);
        assert!(cursor1.is_some());

        let (page2, has_more2, cursor2) = store.query(None, None, 2, cursor1.as_deref());
        assert_eq!(page2.len(), 2);
        assert!(has_more2);
        assert!(cursor2.is_some());

        let (page3, has_more3, _) = store.query(None, None, 2, cursor2.as_deref());
        assert_eq!(page3.len(), 1);
        assert!(!has_more3);
    }

    #[test]
    fn query_with_id_filter() {
        let store = DbDataStore::new(node(1));

        store.create("users", "alice", b"{}", 1000).unwrap();
        store.create("users", "bob", b"{}", 1000).unwrap();

        let (results, _, _) = store.query(None, Some("id=alice"), 100, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id_str(), "alice");
    }
}
