// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::partition::schema_partition;
use crate::cluster::protocol::Operation;
use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct ClusterSchema {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub schema_version: u64,
    pub state: u8,
    pub data_len: u32,
    #[FromField(data_len)]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SchemaState {
    Active = 0,
    PendingAdd = 1,
    PendingDrop = 2,
    Dropped = 3,
}

impl From<u8> for SchemaState {
    fn from(v: u8) -> Self {
        match v {
            1 => Self::PendingAdd,
            2 => Self::PendingDrop,
            3 => Self::Dropped,
            _ => Self::Active,
        }
    }
}

impl ClusterSchema {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(entity: &str, schema_version: u64, state: SchemaState, data: &[u8]) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            schema_version,
            state: state as u8,
            data_len: data.len() as u32,
            data: data.to_vec(),
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn state(&self) -> SchemaState {
        SchemaState::from(self.state)
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        schema_partition(self.entity_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaStoreError {
    NotFound,
    AlreadyExists,
    SerializationError,
    InvalidState,
}

impl std::fmt::Display for SchemaStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "schema not found"),
            Self::AlreadyExists => write!(f, "schema already exists"),
            Self::SerializationError => write!(f, "serialization error"),
            Self::InvalidState => write!(f, "invalid schema state"),
        }
    }
}

impl std::error::Error for SchemaStoreError {}

pub struct SchemaStore {
    node_id: NodeId,
    schemas: RwLock<HashMap<String, ClusterSchema>>,
}

impl SchemaStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if the schema already exists.
    pub fn register(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<ClusterSchema, SchemaStoreError> {
        let mut schemas = self.schemas.write().unwrap();
        if schemas.contains_key(entity) {
            return Err(SchemaStoreError::AlreadyExists);
        }

        let schema = ClusterSchema::create(entity, 1, SchemaState::Active, schema_data);
        schemas.insert(entity.to_string(), schema.clone());
        Ok(schema)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, entity: &str) -> Option<ClusterSchema> {
        self.schemas.read().unwrap().get(entity).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the schema does not exist.
    pub fn update(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<ClusterSchema, SchemaStoreError> {
        let mut schemas = self.schemas.write().unwrap();
        let existing = schemas.get(entity).ok_or(SchemaStoreError::NotFound)?;

        let new_version = existing.schema_version + 1;
        let schema = ClusterSchema::create(entity, new_version, SchemaState::Active, schema_data);
        schemas.insert(entity.to_string(), schema.clone());
        Ok(schema)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the schema does not exist.
    pub fn drop_schema(&self, entity: &str) -> Result<ClusterSchema, SchemaStoreError> {
        let mut schemas = self.schemas.write().unwrap();
        let existing = schemas.get(entity).ok_or(SchemaStoreError::NotFound)?;

        let schema = ClusterSchema::create(
            entity,
            existing.schema_version,
            SchemaState::Dropped,
            &existing.data,
        );
        schemas.insert(entity.to_string(), schema.clone());
        Ok(schema)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn list(&self) -> Vec<ClusterSchema> {
        self.schemas
            .read()
            .unwrap()
            .values()
            .filter(|s| s.state() == SchemaState::Active)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.schemas.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize(schema: &ClusterSchema) -> Vec<u8> {
        schema.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<ClusterSchema> {
        ClusterSchema::try_from_be_bytes(bytes).ok().map(|(s, _)| s)
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
    ) -> Result<(), SchemaStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let schema = Self::deserialize(data).ok_or(SchemaStoreError::SerializationError)?;
                self.schemas.write().unwrap().insert(id.to_string(), schema);
                Ok(())
            }
            Operation::Delete => {
                self.schemas.write().unwrap().remove(id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn is_valid_for_write(&self, entity: &str) -> bool {
        self.schemas
            .read()
            .unwrap()
            .get(entity)
            .is_some_and(|s| s.state() == SchemaState::Active)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let schemas = self.schemas.read().unwrap();
        let matching: Vec<_> = schemas
            .iter()
            .filter(|(_, s)| s.partition() == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(matching.len() as u32).to_be_bytes());

        for (key, schema) in matching {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(key_bytes);

            let data = Self::serialize(schema);
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if a key or schema cannot be deserialized.
    pub fn import_schemas(&self, data: &[u8]) -> Result<usize, SchemaStoreError> {
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
                .map_err(|_| SchemaStoreError::SerializationError)?;
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
            let schema_bytes = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(schema) = Self::deserialize(schema_bytes) {
                self.schemas
                    .write()
                    .unwrap()
                    .insert(key.to_string(), schema);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut schemas = self.schemas.write().unwrap();
        let before = schemas.len();
        schemas.retain(|_, s| s.partition() != partition);
        before - schemas.len()
    }
}

impl std::fmt::Debug for SchemaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaStore")
            .field("node_id", &self.node_id)
            .field("schema_count", &self.count())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn schema_key(entity: &str) -> String {
    let partition = schema_partition(entity);
    format!("_db_schema/p{}/{entity}", partition.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn cluster_schema_bebytes_roundtrip() {
        let schema = ClusterSchema::create("users", 1, SchemaState::Active, b"{\"fields\":[]}");
        let bytes = SchemaStore::serialize(&schema);
        let parsed = SchemaStore::deserialize(&bytes).unwrap();

        assert_eq!(schema.entity, parsed.entity);
        assert_eq!(schema.schema_version, parsed.schema_version);
        assert_eq!(schema.data, parsed.data);
    }

    #[test]
    fn register_and_get() {
        let store = SchemaStore::new(node(1));

        store.register("users", b"{\"fields\":[]}").unwrap();
        let schema = store.get("users").unwrap();

        assert_eq!(schema.entity_str(), "users");
        assert_eq!(schema.schema_version, 1);
        assert_eq!(schema.state(), SchemaState::Active);
    }

    #[test]
    fn register_duplicate_fails() {
        let store = SchemaStore::new(node(1));

        store.register("users", b"{}").unwrap();
        let result = store.register("users", b"{}");

        assert_eq!(result, Err(SchemaStoreError::AlreadyExists));
    }

    #[test]
    fn update_increments_version() {
        let store = SchemaStore::new(node(1));

        store.register("users", b"{v1}").unwrap();
        let updated = store.update("users", b"{v2}").unwrap();

        assert_eq!(updated.schema_version, 2);
        assert_eq!(updated.data, b"{v2}");
    }

    #[test]
    fn drop_marks_as_dropped() {
        let store = SchemaStore::new(node(1));

        store.register("users", b"{}").unwrap();
        let dropped = store.drop_schema("users").unwrap();

        assert_eq!(dropped.state(), SchemaState::Dropped);

        let list = store.list();
        assert!(list.is_empty());
    }

    #[test]
    fn is_valid_for_write_checks_state() {
        let store = SchemaStore::new(node(1));

        assert!(!store.is_valid_for_write("users"));

        store.register("users", b"{}").unwrap();
        assert!(store.is_valid_for_write("users"));

        store.drop_schema("users").unwrap();
        assert!(!store.is_valid_for_write("users"));
    }

    #[test]
    fn apply_replicated_insert() {
        let store = SchemaStore::new(node(1));
        let schema = ClusterSchema::create("orders", 1, SchemaState::Active, b"{}");
        let data = SchemaStore::serialize(&schema);

        store
            .apply_replicated(Operation::Insert, "orders", &data)
            .unwrap();

        assert!(store.get("orders").is_some());
    }

    #[test]
    fn export_import_roundtrip_preserves_partition() {
        let src = SchemaStore::new(node(1));
        let dst = SchemaStore::new(node(2));

        src.register("alpha", b"{}").unwrap();
        src.register("beta", b"{}").unwrap();
        src.register("gamma", b"{}").unwrap();

        let target_partition = src.get("alpha").unwrap().partition();

        let other_partition_count = src
            .list()
            .iter()
            .filter(|s| s.partition() != target_partition)
            .count();
        assert!(
            other_partition_count > 0,
            "test prerequisite: schemas must span more than one partition"
        );

        let payload = src.export_for_partition(target_partition);
        assert!(!payload.is_empty());

        let imported = dst.import_schemas(&payload).unwrap();
        assert!(imported >= 1);

        for schema in src.list() {
            if schema.partition() == target_partition {
                let copy = dst.get(schema.entity_str()).expect("imported schema");
                assert_eq!(copy.data, schema.data);
            } else {
                assert!(
                    dst.get(schema.entity_str()).is_none(),
                    "schema {:?} from a different partition leaked into snapshot",
                    schema.entity_str()
                );
            }
        }
    }

    #[test]
    fn clear_partition_removes_only_target() {
        let store = SchemaStore::new(node(1));
        store.register("alpha", b"{}").unwrap();
        store.register("beta", b"{}").unwrap();
        store.register("gamma", b"{}").unwrap();

        let target = store.get("alpha").unwrap().partition();
        let removed = store.clear_partition(target);

        assert!(removed >= 1);
        assert!(store.get("alpha").is_none());
        for entity in ["beta", "gamma"] {
            if let Some(s) = store.get(entity) {
                assert_ne!(s.partition(), target);
            }
        }
    }
}
