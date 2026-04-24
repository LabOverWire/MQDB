// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::entity::Entity;
use crate::error::Result;
use crate::keys;
use crate::storage::{BatchWriter, Storage};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct IndexDefinition {
    pub entity: String,
    pub fields: Vec<String>,
}

impl IndexDefinition {
    #[allow(clippy::must_use_candidate)]
    pub fn new(entity: String, fields: Vec<String>) -> Self {
        Self { entity, fields }
    }
}

pub struct IndexManager {
    indexes: HashMap<String, IndexDefinition>,
}

impl IndexManager {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn add_index(&mut self, definition: IndexDefinition) {
        self.indexes.insert(definition.entity.clone(), definition);
    }

    #[allow(clippy::must_use_candidate)]
    pub fn get_indexed_fields(&self, entity: &str) -> Option<&Vec<String>> {
        self.indexes.get(entity).map(|idx| &idx.fields)
    }

    pub fn update_indexes(
        &self,
        batch: &mut BatchWriter,
        entity: &Entity,
        old_entity: Option<&Entity>,
    ) {
        if let Some(fields) = self.get_indexed_fields(&entity.name) {
            if let Some(old) = old_entity {
                Self::remove_index_entries(batch, old, fields);
            }
            Self::add_index_entries(batch, entity, fields);
        }
    }

    pub fn remove_indexes(&self, batch: &mut BatchWriter, entity: &Entity) {
        if let Some(fields) = self.get_indexed_fields(&entity.name) {
            Self::remove_index_entries(batch, entity, fields);
        }
    }

    fn add_index_entries(batch: &mut BatchWriter, entity: &Entity, fields: &[String]) {
        let index_values = entity.extract_index_values(fields);

        for (field, value) in index_values {
            let key = keys::encode_index_key(&entity.name, &field, &value, &entity.id);
            batch.insert(key, Vec::new());
        }
    }

    fn remove_index_entries(batch: &mut BatchWriter, entity: &Entity, fields: &[String]) {
        let index_values = entity.extract_index_values(fields);

        for (field, value) in index_values {
            let key = keys::encode_index_key(&entity.name, &field, &value, &entity.id);
            batch.remove(key);
        }
    }

    /// # Errors
    /// Returns an error if the storage prefix scan fails.
    pub fn lookup_by_field(
        &self,
        storage: &crate::storage::Storage,
        entity: &str,
        field: &str,
        value: &[u8],
    ) -> Result<Vec<String>> {
        let prefix = keys::encode_index_prefix(entity, field, Some(value));
        let items = storage.prefix_scan(&prefix)?;

        Ok(Self::extract_ids_from_keys(&items))
    }

    #[allow(clippy::must_use_candidate)]
    pub fn is_field_indexed(&self, entity: &str, field: &str) -> bool {
        self.indexes
            .get(entity)
            .is_some_and(|idx| idx.fields.iter().any(|f| f == field))
    }

    /// # Errors
    /// Returns an error if serialization fails.
    pub fn persist_index(
        &self,
        batch: &mut BatchWriter,
        definition: &IndexDefinition,
    ) -> Result<()> {
        let key = keys::encode_index_definition_key(&definition.entity);
        let value = serde_json::to_vec(definition)?;
        batch.insert(key, value);
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading or deserializing index definitions fails.
    pub fn load_indexes(&mut self, storage: &Storage) -> Result<()> {
        let prefix = b"meta/index/";
        let items = storage.prefix_scan(prefix)?;

        for (_key, value) in items {
            let definition: IndexDefinition = serde_json::from_slice(&value)?;
            self.indexes.insert(definition.entity.clone(), definition);
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if the storage range scan fails.
    pub fn lookup_by_range(
        &self,
        storage: &crate::storage::Storage,
        entity: &str,
        field: &str,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
    ) -> Result<Vec<String>> {
        let field_prefix = keys::encode_index_prefix(entity, field, None);

        let start = if let Some((value, inclusive)) = lower {
            let mut key = field_prefix.clone();
            key.push(keys::SEPARATOR);
            key.extend_from_slice(value);
            key.push(keys::SEPARATOR);
            if !inclusive {
                key.push(0xFF);
            }
            key
        } else {
            let mut key = field_prefix.clone();
            key.push(keys::SEPARATOR);
            key
        };

        let end = if let Some((value, inclusive)) = upper {
            let mut key = field_prefix;
            key.push(keys::SEPARATOR);
            key.extend_from_slice(value);
            key.push(keys::SEPARATOR);
            if inclusive {
                key.push(0xFF);
            }
            key
        } else {
            let mut key = field_prefix;
            key.push(0xFF);
            key
        };

        let items = storage.range_scan(&start, &end)?;
        Ok(Self::extract_ids_from_keys(&items))
    }

    fn extract_ids_from_keys(items: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
        let mut ids = Vec::new();
        for (key, _) in items {
            if let Some(id_start) = key.iter().rposition(|&b| b == b'/')
                && let Ok(id) = String::from_utf8(key[id_start + 1..].to_vec())
            {
                ids.push(id);
            }
        }
        ids
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use serde_json::json;

    #[test]
    fn test_index_definition() {
        let idx = IndexDefinition::new("users".into(), vec!["email".into(), "status".into()]);
        assert_eq!(idx.entity, "users");
        assert_eq!(idx.fields.len(), 2);
    }

    #[test]
    fn test_extract_index_values() {
        let entity = Entity::new(
            "users".into(),
            "123".into(),
            json!({
                "email": "test@example.com",
                "status": "active"
            }),
        );

        let values = entity.extract_index_values(&["email".into(), "status".into()]);
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn is_field_indexed_returns_true_for_indexed_field() {
        let mut mgr = IndexManager::new();
        mgr.add_index(IndexDefinition::new(
            "users".into(),
            vec!["age".into(), "name".into()],
        ));
        assert!(mgr.is_field_indexed("users", "age"));
        assert!(mgr.is_field_indexed("users", "name"));
        assert!(!mgr.is_field_indexed("users", "email"));
        assert!(!mgr.is_field_indexed("posts", "age"));
    }

    fn setup_indexed_entities(ages: &[i64]) -> (Storage, IndexManager) {
        let storage = Storage::memory();
        let mut mgr = IndexManager::new();
        mgr.add_index(IndexDefinition::new("users".into(), vec!["age".into()]));

        for (i, &age) in ages.iter().enumerate() {
            let id = format!("u{i}");
            let entity = Entity::new("users".into(), id, json!({"age": age}));
            let mut batch = storage.batch();
            mgr.update_indexes(&mut batch, &entity, None);
            batch.commit().unwrap();
        }

        (storage, mgr)
    }

    #[test]
    fn range_lookup_returns_ids_in_range() {
        let (storage, mgr) = setup_indexed_entities(&[10, 20, 30, 40, 50]);

        let lower = keys::encode_value_for_index(&json!(20)).unwrap();
        let upper = keys::encode_value_for_index(&json!(40)).unwrap();
        let ids = mgr
            .lookup_by_range(
                &storage,
                "users",
                "age",
                Some((&lower, true)),
                Some((&upper, true)),
            )
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"u1".to_string()));
        assert!(ids.contains(&"u2".to_string()));
        assert!(ids.contains(&"u3".to_string()));
    }

    #[test]
    fn range_lookup_exclusive_bounds() {
        let (storage, mgr) = setup_indexed_entities(&[10, 20, 30, 40, 50]);

        let lower = keys::encode_value_for_index(&json!(20)).unwrap();
        let upper = keys::encode_value_for_index(&json!(40)).unwrap();
        let ids = mgr
            .lookup_by_range(
                &storage,
                "users",
                "age",
                Some((&lower, false)),
                Some((&upper, false)),
            )
            .unwrap();

        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"u2".to_string()));
    }

    #[test]
    fn range_lookup_open_ended_lower() {
        let (storage, mgr) = setup_indexed_entities(&[10, 20, 30, 40, 50]);

        let upper = keys::encode_value_for_index(&json!(30)).unwrap();
        let ids = mgr
            .lookup_by_range(&storage, "users", "age", None, Some((&upper, false)))
            .unwrap();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"u0".to_string()));
        assert!(ids.contains(&"u1".to_string()));
    }

    #[test]
    fn range_lookup_open_ended_upper() {
        let (storage, mgr) = setup_indexed_entities(&[10, 20, 30, 40, 50]);

        let lower = keys::encode_value_for_index(&json!(30)).unwrap();
        let ids = mgr
            .lookup_by_range(&storage, "users", "age", Some((&lower, true)), None)
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"u2".to_string()));
        assert!(ids.contains(&"u3".to_string()));
        assert!(ids.contains(&"u4".to_string()));
    }

    #[test]
    fn range_lookup_string_field() {
        let storage = Storage::memory();
        let mut mgr = IndexManager::new();
        mgr.add_index(IndexDefinition::new("users".into(), vec!["name".into()]));

        let names = ["alice", "bob", "carol", "dave", "eve"];
        for (i, name) in names.iter().enumerate() {
            let id = format!("u{i}");
            let entity = Entity::new("users".into(), id, json!({"name": name}));
            let mut batch = storage.batch();
            mgr.update_indexes(&mut batch, &entity, None);
            batch.commit().unwrap();
        }

        let lower = keys::encode_value_for_index(&json!("bob")).unwrap();
        let upper = keys::encode_value_for_index(&json!("dave")).unwrap();
        let ids = mgr
            .lookup_by_range(
                &storage,
                "users",
                "name",
                Some((&lower, true)),
                Some((&upper, true)),
            )
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"u1".to_string()));
        assert!(ids.contains(&"u2".to_string()));
        assert!(ids.contains(&"u3".to_string()));
    }

    #[test]
    fn range_lookup_negative_numbers() {
        let (storage, mgr) = setup_indexed_entities(&[-20, -10, 0, 10, 20]);

        let lower = keys::encode_value_for_index(&json!(-10)).unwrap();
        let upper = keys::encode_value_for_index(&json!(10)).unwrap();
        let ids = mgr
            .lookup_by_range(
                &storage,
                "users",
                "age",
                Some((&lower, true)),
                Some((&upper, true)),
            )
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"u1".to_string()));
        assert!(ids.contains(&"u2".to_string()));
        assert!(ids.contains(&"u3".to_string()));
    }

    #[test]
    fn persist_and_load_indexes_round_trip() {
        let storage = Storage::memory();
        let mut mgr = IndexManager::new();
        mgr.add_index(IndexDefinition::new(
            "users".into(),
            vec!["email".into(), "status".into()],
        ));
        mgr.add_index(IndexDefinition::new("posts".into(), vec!["title".into()]));

        for def in mgr.indexes.values() {
            let mut batch = storage.batch();
            mgr.persist_index(&mut batch, def).unwrap();
            batch.commit().unwrap();
        }

        let mut loaded = IndexManager::new();
        loaded.load_indexes(&storage).unwrap();

        assert_eq!(
            loaded.get_indexed_fields("users").unwrap(),
            &vec!["email".to_string(), "status".to_string()]
        );
        assert_eq!(
            loaded.get_indexed_fields("posts").unwrap(),
            &vec!["title".to_string()]
        );
        assert!(loaded.get_indexed_fields("nonexistent").is_none());
    }
}
