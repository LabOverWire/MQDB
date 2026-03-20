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
}
