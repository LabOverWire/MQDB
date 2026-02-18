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
    ForeignKey = 1,
}

impl From<u8> for ConstraintType {
    fn from(v: u8) -> Self {
        match v {
            1 => Self::ForeignKey,
            _ => Self::Unique,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OnDeleteAction {
    Restrict = 0,
    Cascade = 1,
    SetNull = 2,
}

impl OnDeleteAction {
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Cascade,
            2 => Self::SetNull,
            _ => Self::Restrict,
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cascade" => Some(Self::Cascade),
            "set_null" | "setnull" => Some(Self::SetNull),
            "restrict" => Some(Self::Restrict),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Restrict => "restrict",
            Self::Cascade => "cascade",
            Self::SetNull => "set_null",
        }
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
    pub target_entity_len: u16,
    #[FromField(target_entity_len)]
    pub target_entity: Vec<u8>,
    pub target_field_len: u16,
    #[FromField(target_field_len)]
    pub target_field: Vec<u8>,
    pub on_delete: u8,
}

impl ClusterConstraint {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn unique(entity: &str, name: &str, field: &str) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let name_bytes = name.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        Self {
            version: 2,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            name_len: name_bytes.len() as u16,
            name: name_bytes,
            constraint_type: ConstraintType::Unique as u8,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            target_entity_len: 0,
            target_entity: Vec::new(),
            target_field_len: 0,
            target_field: Vec::new(),
            on_delete: 0,
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
    #[must_use]
    pub fn foreign_key(
        entity: &str,
        name: &str,
        field: &str,
        target_entity: &str,
        target_field: &str,
        on_delete: OnDeleteAction,
    ) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let name_bytes = name.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        let target_entity_bytes = target_entity.as_bytes().to_vec();
        let target_field_bytes = target_field.as_bytes().to_vec();
        Self {
            version: 2,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            name_len: name_bytes.len() as u16,
            name: name_bytes,
            constraint_type: ConstraintType::ForeignKey as u8,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            target_entity_len: target_entity_bytes.len() as u16,
            target_entity: target_entity_bytes,
            target_field_len: target_field_bytes.len() as u16,
            target_field: target_field_bytes,
            on_delete: on_delete as u8,
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
    pub fn target_entity_str(&self) -> &str {
        std::str::from_utf8(&self.target_entity).unwrap_or("")
    }

    #[must_use]
    pub fn target_field_str(&self) -> &str {
        std::str::from_utf8(&self.target_field).unwrap_or("")
    }

    #[must_use]
    pub fn on_delete_action(&self) -> OnDeleteAction {
        OnDeleteAction::from_u8(self.on_delete)
    }

    #[must_use]
    pub fn constraint_type(&self) -> ConstraintType {
        ConstraintType::from(self.constraint_type)
    }

    #[must_use]
    pub fn is_foreign_key(&self) -> bool {
        self.constraint_type() == ConstraintType::ForeignKey
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
    pub fn get_fk_constraints(&self, entity: &str) -> Vec<ClusterConstraint> {
        self.constraints
            .read()
            .unwrap()
            .values()
            .filter(|c| {
                c.entity_str() == entity && c.constraint_type() == ConstraintType::ForeignKey
            })
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn find_referencing_constraints(&self, target_entity: &str) -> Vec<ClusterConstraint> {
        self.constraints
            .read()
            .unwrap()
            .values()
            .filter(|c| {
                c.constraint_type() == ConstraintType::ForeignKey
                    && c.target_entity_str() == target_entity
            })
            .cloned()
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
        if bytes.is_empty() {
            return None;
        }
        match bytes[0] {
            1 => Self::deserialize_v1(bytes),
            2 => ClusterConstraint::try_from_be_bytes(bytes)
                .ok()
                .map(|(c, _)| c),
            _ => None,
        }
    }

    fn deserialize_v1(bytes: &[u8]) -> Option<ClusterConstraint> {
        if bytes.is_empty() || bytes[0] != 1 {
            return None;
        }
        let mut offset = 1;
        let entity_len = read_u16(bytes, &mut offset)?;
        let entity = read_vec(bytes, &mut offset, entity_len)?;
        let name_len = read_u16(bytes, &mut offset)?;
        let name = read_vec(bytes, &mut offset, name_len)?;
        let constraint_type = read_u8(bytes, &mut offset)?;
        let field_len = read_u16(bytes, &mut offset)?;
        let field = read_vec(bytes, &mut offset, field_len)?;
        Some(ClusterConstraint {
            version: 2,
            entity_len,
            entity,
            name_len,
            name,
            constraint_type,
            field_len,
            field,
            target_entity_len: 0,
            target_entity: Vec::new(),
            target_field_len: 0,
            target_field: Vec::new(),
            on_delete: 0,
        })
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
                let key = Self::constraint_key(constraint.entity_str(), constraint.name_str());
                self.constraints.write().unwrap().insert(key, constraint);
                Ok(())
            }
            Operation::Delete => {
                let key = Self::replication_id_to_key(id);
                self.constraints.write().unwrap().remove(&key);
                Ok(())
            }
        }
    }

    fn replication_id_to_key(id: &str) -> String {
        if let Some(rest) = id.strip_prefix("_db_constraint/")
            && let Some((entity, name)) = rest.split_once('/')
        {
            return Self::constraint_key(entity, name);
        }
        id.to_string()
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

fn read_u8(bytes: &[u8], offset: &mut usize) -> Option<u8> {
    if *offset >= bytes.len() {
        return None;
    }
    let v = bytes[*offset];
    *offset += 1;
    Some(v)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    if *offset + 2 > bytes.len() {
        return None;
    }
    let v = u16::from_be_bytes([bytes[*offset], bytes[*offset + 1]]);
    *offset += 2;
    Some(v)
}

fn read_vec(bytes: &[u8], offset: &mut usize, len: u16) -> Option<Vec<u8>> {
    let len = usize::from(len);
    if *offset + len > bytes.len() {
        return None;
    }
    let v = bytes[*offset..*offset + len].to_vec();
    *offset += len;
    Some(v)
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

    #[test]
    fn fk_constraint_bebytes_roundtrip() {
        let constraint = ClusterConstraint::foreign_key(
            "posts",
            "posts_author_fk",
            "author_id",
            "users",
            "id",
            OnDeleteAction::Cascade,
        );
        let bytes = ConstraintStore::serialize(&constraint);
        let parsed = ConstraintStore::deserialize(&bytes).unwrap();

        assert_eq!(parsed.entity_str(), "posts");
        assert_eq!(parsed.name_str(), "posts_author_fk");
        assert_eq!(parsed.field_str(), "author_id");
        assert_eq!(parsed.target_entity_str(), "users");
        assert_eq!(parsed.target_field_str(), "id");
        assert_eq!(parsed.on_delete_action(), OnDeleteAction::Cascade);
        assert_eq!(parsed.constraint_type(), ConstraintType::ForeignKey);
        assert!(parsed.is_foreign_key());
    }

    #[test]
    fn get_fk_constraints() {
        let store = ConstraintStore::new(node(1));

        store
            .add(ClusterConstraint::unique("posts", "unique_slug", "slug"))
            .unwrap();
        store
            .add(ClusterConstraint::foreign_key(
                "posts",
                "posts_author_fk",
                "author_id",
                "users",
                "id",
                OnDeleteAction::Restrict,
            ))
            .unwrap();
        store
            .add(ClusterConstraint::foreign_key(
                "posts",
                "posts_category_fk",
                "category_id",
                "categories",
                "id",
                OnDeleteAction::SetNull,
            ))
            .unwrap();

        let fks = store.get_fk_constraints("posts");
        assert_eq!(fks.len(), 2);

        let unique_fields = store.get_unique_fields("posts");
        assert_eq!(unique_fields.len(), 1);
    }

    #[test]
    fn find_referencing_constraints() {
        let store = ConstraintStore::new(node(1));

        store
            .add(ClusterConstraint::foreign_key(
                "posts",
                "posts_author_fk",
                "author_id",
                "users",
                "id",
                OnDeleteAction::Cascade,
            ))
            .unwrap();
        store
            .add(ClusterConstraint::foreign_key(
                "comments",
                "comments_author_fk",
                "author_id",
                "users",
                "id",
                OnDeleteAction::Restrict,
            ))
            .unwrap();
        store
            .add(ClusterConstraint::foreign_key(
                "comments",
                "comments_post_fk",
                "post_id",
                "posts",
                "id",
                OnDeleteAction::Cascade,
            ))
            .unwrap();

        let refs_to_users = store.find_referencing_constraints("users");
        assert_eq!(refs_to_users.len(), 2);

        let refs_to_posts = store.find_referencing_constraints("posts");
        assert_eq!(refs_to_posts.len(), 1);
    }

    #[test]
    fn on_delete_action_conversions() {
        assert_eq!(OnDeleteAction::from_u8(0), OnDeleteAction::Restrict);
        assert_eq!(OnDeleteAction::from_u8(1), OnDeleteAction::Cascade);
        assert_eq!(OnDeleteAction::from_u8(2), OnDeleteAction::SetNull);
        assert_eq!(OnDeleteAction::from_u8(255), OnDeleteAction::Restrict);

        assert_eq!(
            OnDeleteAction::parse("cascade"),
            Some(OnDeleteAction::Cascade)
        );
        assert_eq!(
            OnDeleteAction::parse("set_null"),
            Some(OnDeleteAction::SetNull)
        );
        assert_eq!(
            OnDeleteAction::parse("restrict"),
            Some(OnDeleteAction::Restrict)
        );
        assert_eq!(
            OnDeleteAction::parse("CASCADE"),
            Some(OnDeleteAction::Cascade)
        );
        assert_eq!(
            OnDeleteAction::parse("Restrict"),
            Some(OnDeleteAction::Restrict)
        );
        assert!(OnDeleteAction::parse("unknown").is_none());

        assert_eq!(OnDeleteAction::Restrict.as_str(), "restrict");
        assert_eq!(OnDeleteAction::Cascade.as_str(), "cascade");
        assert_eq!(OnDeleteAction::SetNull.as_str(), "set_null");
    }

    #[test]
    fn apply_replicated_uses_normalized_key() {
        let store = ConstraintStore::new(node(1));
        let constraint = ClusterConstraint::unique("products", "unique_sku", "sku");
        let data = ConstraintStore::serialize(&constraint);

        let replication_id = "_db_constraint/products/unique_sku";
        store
            .apply_replicated(Operation::Insert, replication_id, &data)
            .unwrap();

        assert!(store.exists("products", "unique_sku"));
        assert!(store.get("products", "unique_sku").is_some());

        store
            .apply_replicated(Operation::Delete, replication_id, &[])
            .unwrap();

        assert!(!store.exists("products", "unique_sku"));
        assert_eq!(store.count(), 0);
    }
}
