use crate::entity::Entity;
use crate::error::Result;
use crate::keys;
use crate::storage::BatchWriter;
use std::collections::HashMap;

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
                self.remove_index_entries(batch, old, fields);
            }
            self.add_index_entries(batch, entity, fields);
        }
    }

    pub fn remove_indexes(&self, batch: &mut BatchWriter, entity: &Entity) {
        if let Some(fields) = self.get_indexed_fields(&entity.name) {
            self.remove_index_entries(batch, entity, fields);
        }
    }

    #[allow(clippy::unused_self)]
    fn add_index_entries(&self, batch: &mut BatchWriter, entity: &Entity, fields: &[String]) {
        let index_values = entity.extract_index_values(fields);

        for (field, value) in index_values {
            let key = keys::encode_index_key(&entity.name, &field, &value, &entity.id);
            batch.insert(key, Vec::new());
        }
    }

    #[allow(clippy::unused_self)]
    fn remove_index_entries(&self, batch: &mut BatchWriter, entity: &Entity, fields: &[String]) {
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

        let mut ids = Vec::new();
        for (key, _) in items {
            if let Some(id_start) = key.iter().rposition(|&b| b == b'/')
                && let Ok(id) = String::from_utf8(key[id_start + 1..].to_vec())
            {
                ids.push(id);
            }
        }

        Ok(ids)
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
}
