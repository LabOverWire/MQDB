// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::NodeId;
use crate::cluster::protocol::Operation;
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConstraintType {
    Unique = 0,
}

impl From<u8> for ConstraintType {
    fn from(_v: u8) -> Self {
        Self::Unique
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct ClusterConstraint {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub name_len: u16,
    #[FromField(name_len)]
    pub name: Vec<u8>,
    pub constraint_type: u8,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
}

impl ClusterConstraint {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn unique(entity: &str, name: &str, field: &str) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let name_bytes = name.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            name_len: name_bytes.len() as u16,
            name: name_bytes,
            constraint_type: ConstraintType::Unique as u8,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn name_str(&self) -> &str {
        std::str::from_utf8(&self.name).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn constraint_type(&self) -> ConstraintType {
        ConstraintType::from(self.constraint_type)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintStoreError {
    NotFound,
    AlreadyExists,
    SerializationError,
}

impl std::fmt::Display for ConstraintStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "constraint not found"),
            Self::AlreadyExists => write!(f, "constraint already exists"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for ConstraintStoreError {}

pub struct ConstraintStore {
    node_id: NodeId,
    constraints: RwLock<HashMap<String, ClusterConstraint>>,
}

impl ConstraintStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            constraints: RwLock::new(HashMap::new()),
        }
    }

    fn constraint_key(entity: &str, name: &str) -> String {
        format!("{entity}:{name}")
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if the constraint already exists.
    pub fn add(&self, constraint: ClusterConstraint) -> Result<(), ConstraintStoreError> {
        let key = Self::constraint_key(constraint.entity_str(), constraint.name_str());
        let mut constraints = self.constraints.write().unwrap();
        if constraints.contains_key(&key) {
            return Err(ConstraintStoreError::AlreadyExists);
        }
        constraints.insert(key, constraint);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the constraint does not exist.
    pub fn remove(
        &self,
        entity: &str,
        name: &str,
    ) -> Result<ClusterConstraint, ConstraintStoreError> {
        let key = Self::constraint_key(entity, name);
        self.constraints
            .write()
            .unwrap()
            .remove(&key)
            .ok_or(ConstraintStoreError::NotFound)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, entity: &str, name: &str) -> Option<ClusterConstraint> {
        let key = Self::constraint_key(entity, name);
        self.constraints.read().unwrap().get(&key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn exists(&self, entity: &str, name: &str) -> bool {
        let key = Self::constraint_key(entity, name);
        self.constraints.read().unwrap().contains_key(&key)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn list(&self, entity: &str) -> Vec<ClusterConstraint> {
        self.constraints
            .read()
            .unwrap()
            .values()
            .filter(|c| c.entity_str() == entity)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn list_all(&self) -> Vec<ClusterConstraint> {
        self.constraints.read().unwrap().values().cloned().collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_unique_fields(&self, entity: &str) -> Vec<String> {
        self.constraints
            .read()
            .unwrap()
            .values()
            .filter(|c| c.entity_str() == entity && c.constraint_type() == ConstraintType::Unique)
            .map(|c| c.field_str().to_string())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.constraints.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize(constraint: &ClusterConstraint) -> Vec<u8> {
        constraint.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<ClusterConstraint> {
        ClusterConstraint::try_from_be_bytes(bytes)
            .ok()
            .map(|(c, _)| c)
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
    ) -> Result<(), ConstraintStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let constraint =
                    Self::deserialize(data).ok_or(ConstraintStoreError::SerializationError)?;
                self.constraints
                    .write()
                    .unwrap()
                    .insert(id.to_string(), constraint);
                Ok(())
            }
            Operation::Delete => {
                self.constraints.write().unwrap().remove(id);
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for ConstraintStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstraintStore")
            .field("node_id", &self.node_id)
            .field("constraint_count", &self.count())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn constraint_key(entity: &str, name: &str) -> String {
    format!("_db_constraint/{entity}/{name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn cluster_constraint_bebytes_roundtrip() {
        let constraint = ClusterConstraint::unique("users", "unique_email", "email");
        let bytes = ConstraintStore::serialize(&constraint);
        let parsed = ConstraintStore::deserialize(&bytes).unwrap();

        assert_eq!(constraint.entity, parsed.entity);
        assert_eq!(constraint.name, parsed.name);
        assert_eq!(constraint.field, parsed.field);
        assert_eq!(constraint.constraint_type, parsed.constraint_type);
    }

    #[test]
    fn add_and_get() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("users", "unique_email", "email");

        store.add(constraint).unwrap();
        let fetched = store.get("users", "unique_email").unwrap();

        assert_eq!(fetched.entity_str(), "users");
        assert_eq!(fetched.name_str(), "unique_email");
        assert_eq!(fetched.field_str(), "email");
    }

    #[test]
    fn add_duplicate_fails() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("users", "unique_email", "email");

        store.add(constraint.clone()).unwrap();
        let result = store.add(constraint);

        assert_eq!(result, Err(ConstraintStoreError::AlreadyExists));
    }

    #[test]
    fn remove_constraint() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("users", "unique_email", "email");

        store.add(constraint).unwrap();
        let removed = store.remove("users", "unique_email").unwrap();

        assert_eq!(removed.name_str(), "unique_email");
        assert!(store.get("users", "unique_email").is_none());
    }

    #[test]
    fn list_by_entity() {
        let store = ConstraintStore::new(node(1));

        store
            .add(ClusterConstraint::unique("users", "unique_email", "email"))
            .unwrap();
        store
            .add(ClusterConstraint::unique(
                "users",
                "unique_username",
                "username",
            ))
            .unwrap();
        store
            .add(ClusterConstraint::unique(
                "orders",
                "unique_order_id",
                "order_id",
            ))
            .unwrap();

        let user_constraints = store.list("users");
        assert_eq!(user_constraints.len(), 2);

        let order_constraints = store.list("orders");
        assert_eq!(order_constraints.len(), 1);
    }

    #[test]
    fn get_unique_fields() {
        let store = ConstraintStore::new(node(1));

        store
            .add(ClusterConstraint::unique("users", "unique_email", "email"))
            .unwrap();
        store
            .add(ClusterConstraint::unique(
                "users",
                "unique_username",
                "username",
            ))
            .unwrap();

        let fields = store.get_unique_fields("users");
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&"email".to_string()));
        assert!(fields.contains(&"username".to_string()));
    }

    #[test]
    fn apply_replicated_insert() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("products", "unique_sku", "sku");
        let data = ConstraintStore::serialize(&constraint);

        store
            .apply_replicated(Operation::Insert, "products:unique_sku", &data)
            .unwrap();

        assert_eq!(store.count(), 1);
    }

    #[test]
    fn apply_replicated_delete() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("products", "unique_sku", "sku");
        let data = ConstraintStore::serialize(&constraint);

        store
            .apply_replicated(Operation::Insert, "products:unique_sku", &data)
            .unwrap();
        store
            .apply_replicated(Operation::Delete, "products:unique_sku", &[])
            .unwrap();

        assert_eq!(store.count(), 0);
    }
}
