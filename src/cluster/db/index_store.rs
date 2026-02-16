// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::partition::index_partition;
use crate::cluster::protocol::Operation;
use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct IndexEntry {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub data_partition: u16,
    pub record_id_len: u16,
    #[FromField(record_id_len)]
    pub record_id: Vec<u8>,
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

impl IndexEntry {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        let record_id_bytes = record_id.as_bytes().to_vec();

        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            value_len: value.len() as u32,
            value: value.to_vec(),
            data_partition: data_partition.get(),
            record_id_len: record_id_bytes.len() as u16,
            record_id: record_id_bytes,
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn record_id_str(&self) -> &str {
        std::str::from_utf8(&self.record_id).unwrap_or("")
    }

    #[must_use]
    pub fn data_partition(&self) -> PartitionId {
        PartitionId::new(self.data_partition).unwrap_or(PartitionId::ZERO)
    }

    #[must_use]
    pub fn index_partition(&self) -> PartitionId {
        index_partition(self.entity_str(), self.field_str(), &self.value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStoreError {
    NotFound,
    AlreadyExists,
    SerializationError,
}

impl std::fmt::Display for IndexStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "index entry not found"),
            Self::AlreadyExists => write!(f, "index entry already exists"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for IndexStoreError {}

pub struct IndexStore {
    node_id: NodeId,
    entries: RwLock<HashMap<String, BTreeMap<String, IndexEntry>>>,
}

impl IndexStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if the index entry already exists.
    pub fn add_entry(&self, entry: IndexEntry) -> Result<(), IndexStoreError> {
        let prefix = Self::prefix_key(entry.entity_str(), entry.field_str(), &entry.value);
        let suffix = Self::suffix_key(entry.data_partition(), entry.record_id_str());

        let mut entries = self.entries.write().unwrap();
        let inner = entries.entry(prefix).or_default();

        if inner.contains_key(&suffix) {
            return Err(IndexStoreError::AlreadyExists);
        }

        inner.insert(suffix, entry);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the index entry does not exist.
    pub fn remove_entry(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Result<IndexEntry, IndexStoreError> {
        let prefix = Self::prefix_key(entity, field, value);
        let suffix = Self::suffix_key(data_partition, record_id);

        let mut entries = self.entries.write().unwrap();
        let inner = entries.get_mut(&prefix).ok_or(IndexStoreError::NotFound)?;

        let entry = inner.remove(&suffix).ok_or(IndexStoreError::NotFound)?;

        if inner.is_empty() {
            entries.remove(&prefix);
        }

        Ok(entry)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn lookup(&self, entity: &str, field: &str, value: &[u8]) -> Vec<IndexEntry> {
        let prefix = Self::prefix_key(entity, field, value);
        let entries = self.entries.read().unwrap();

        entries
            .get(&prefix)
            .map(|inner| inner.values().cloned().collect())
            .unwrap_or_default()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.entries
            .read()
            .unwrap()
            .values()
            .map(BTreeMap::len)
            .sum()
    }

    #[must_use]
    pub fn serialize(entry: &IndexEntry) -> Vec<u8> {
        entry.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<IndexEntry> {
        IndexEntry::try_from_be_bytes(bytes).ok().map(|(e, _)| e)
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
    ) -> Result<(), IndexStoreError> {
        match operation {
            Operation::Insert => {
                let entry = Self::deserialize(data).ok_or(IndexStoreError::SerializationError)?;
                self.add_entry(entry).or(Ok(()))
            }
            Operation::Delete => {
                let parts: Vec<&str> = id.split('/').collect();
                if parts.len() >= 4 {
                    let entity = parts[0];
                    let field = parts[1];
                    let value_hex = parts[2];
                    let rest = &parts[3..];

                    if rest.len() >= 2 {
                        let data_partition_str = rest[rest.len() - 2];
                        let record_id = rest[rest.len() - 1];

                        if let Ok(dp) = data_partition_str
                            .strip_prefix('p')
                            .unwrap_or(data_partition_str)
                            .parse::<u16>()
                            && let Some(p) = PartitionId::new(dp)
                        {
                            let value = decode_hex(value_hex).unwrap_or_default();
                            self.remove_entry(entity, field, &value, p, record_id).ok();
                        }
                    }
                }
                Ok(())
            }
            Operation::Update => {
                let entry = Self::deserialize(data).ok_or(IndexStoreError::SerializationError)?;
                let _ = self.remove_entry(
                    entry.entity_str(),
                    entry.field_str(),
                    &entry.value,
                    entry.data_partition(),
                    entry.record_id_str(),
                );
                self.add_entry(entry).or(Ok(()))
            }
        }
    }

    fn prefix_key(entity: &str, field: &str, value: &[u8]) -> String {
        format!("gidx/{entity}/{field}/{}", encode_hex(value))
    }

    fn suffix_key(data_partition: PartitionId, record_id: &str) -> String {
        format!("p{}/{record_id}", data_partition.get())
    }
}

impl std::fmt::Debug for IndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexStore")
            .field("node_id", &self.node_id)
            .field("entry_count", &self.count())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn index_key(entry: &IndexEntry) -> String {
    format!(
        "gidx/{}/{}/{}/p{}/{}",
        entry.entity_str(),
        entry.field_str(),
        encode_hex(&entry.value),
        entry.data_partition,
        entry.record_id_str()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    #[test]
    fn index_entry_bebytes_roundtrip() {
        let entry = IndexEntry::create(
            "users",
            "email",
            b"alice@example.com",
            partition(42),
            "user123",
        );
        let bytes = IndexStore::serialize(&entry);
        let parsed = IndexStore::deserialize(&bytes).unwrap();

        assert_eq!(entry.entity, parsed.entity);
        assert_eq!(entry.field, parsed.field);
        assert_eq!(entry.value, parsed.value);
        assert_eq!(entry.data_partition, parsed.data_partition);
        assert_eq!(entry.record_id, parsed.record_id);
    }

    #[test]
    fn add_and_lookup() {
        let store = IndexStore::new(node(1));
        let entry = IndexEntry::create(
            "users",
            "email",
            b"alice@example.com",
            partition(42),
            "user123",
        );

        store.add_entry(entry.clone()).unwrap();

        let results = store.lookup("users", "email", b"alice@example.com");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record_id_str(), "user123");
    }

    #[test]
    fn add_duplicate_fails() {
        let store = IndexStore::new(node(1));
        let entry = IndexEntry::create(
            "users",
            "email",
            b"alice@example.com",
            partition(42),
            "user123",
        );

        store.add_entry(entry.clone()).unwrap();
        let result = store.add_entry(entry);

        assert_eq!(result, Err(IndexStoreError::AlreadyExists));
    }

    #[test]
    fn remove_entry_succeeds() {
        let store = IndexStore::new(node(1));
        let entry = IndexEntry::create(
            "users",
            "email",
            b"alice@example.com",
            partition(42),
            "user123",
        );

        store.add_entry(entry).unwrap();

        let removed = store
            .remove_entry(
                "users",
                "email",
                b"alice@example.com",
                partition(42),
                "user123",
            )
            .unwrap();
        assert_eq!(removed.record_id_str(), "user123");

        let results = store.lookup("users", "email", b"alice@example.com");
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_entries_same_value() {
        let store = IndexStore::new(node(1));

        store
            .add_entry(IndexEntry::create(
                "products",
                "category",
                b"electronics",
                partition(10),
                "prod1",
            ))
            .unwrap();

        store
            .add_entry(IndexEntry::create(
                "products",
                "category",
                b"electronics",
                partition(20),
                "prod2",
            ))
            .unwrap();

        let results = store.lookup("products", "category", b"electronics");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn apply_replicated_insert() {
        let store = IndexStore::new(node(1));
        let entry = IndexEntry::create(
            "users",
            "email",
            b"bob@example.com",
            partition(5),
            "user456",
        );
        let data = IndexStore::serialize(&entry);

        store
            .apply_replicated(Operation::Insert, "", &data)
            .unwrap();

        let results = store.lookup("users", "email", b"bob@example.com");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn index_key_format() {
        let entry = IndexEntry::create(
            "users",
            "email",
            b"test@example.com",
            partition(42),
            "user789",
        );
        let key = index_key(&entry);

        assert!(key.starts_with("gidx/users/email/"));
        assert!(key.contains("/p42/user789"));
    }
}
