// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::entity::Entity;
use crate::error::{Error, Result};
use crate::keys;
use crate::storage::{BatchWriter, Storage};
use crate::types::OwnershipConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OnDeleteAction {
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub entity: String,
    pub fields: Vec<String>,
    pub name: String,
}

impl UniqueConstraint {
    pub fn new(entity: impl Into<String>, fields: Vec<String>) -> Self {
        let entity = entity.into();
        let name = format!("{}_{}_unique", entity, fields.join("_"));
        Self {
            entity,
            fields,
            name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub source_entity: String,
    pub source_field: String,
    pub target_entity: String,
    pub target_field: String,
    pub on_delete: OnDeleteAction,
    pub name: String,
}

impl ForeignKeyConstraint {
    pub fn new(
        source_entity: impl Into<String>,
        source_field: impl Into<String>,
        target_entity: impl Into<String>,
        target_field: impl Into<String>,
        on_delete: OnDeleteAction,
    ) -> Self {
        let source_entity = source_entity.into();
        let source_field = source_field.into();
        let target_entity = target_entity.into();
        let target_field = target_field.into();
        let name = format!("{source_entity}_{source_field}_{target_entity}_fk");
        Self {
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
            name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotNullConstraint {
    pub entity: String,
    pub field: String,
    pub name: String,
}

impl NotNullConstraint {
    pub fn new(entity: impl Into<String>, field: impl Into<String>) -> Self {
        let entity = entity.into();
        let field = field.into();
        let name = format!("{entity}_{field}_notnull");
        Self {
            entity,
            field,
            name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Constraint {
    Unique(UniqueConstraint),
    ForeignKey(ForeignKeyConstraint),
    NotNull(NotNullConstraint),
}

impl Constraint {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Constraint::Unique(c) => &c.name,
            Constraint::ForeignKey(c) => &c.name,
            Constraint::NotNull(c) => &c.name,
        }
    }

    #[must_use]
    pub fn entity(&self) -> &str {
        match self {
            Constraint::Unique(c) => &c.entity,
            Constraint::ForeignKey(c) => &c.source_entity,
            Constraint::NotNull(c) => &c.entity,
        }
    }

    #[must_use]
    fn constraint_type(&self) -> &str {
        match self {
            Constraint::Unique(_) => "unique",
            Constraint::ForeignKey(_) => "fk",
            Constraint::NotNull(_) => "notnull",
        }
    }

    #[must_use]
    pub fn to_api_value(&self) -> Value {
        match self {
            Constraint::Unique(c) => json!({
                "name": c.name,
                "type": "unique",
                "fields": c.fields,
            }),
            Constraint::ForeignKey(c) => json!({
                "name": c.name,
                "type": "fk",
                "field": c.source_field,
                "target_entity": c.target_entity,
                "target_field": c.target_field,
                "on_delete": match c.on_delete {
                    OnDeleteAction::Restrict => "restrict",
                    OnDeleteAction::Cascade => "cascade",
                    OnDeleteAction::SetNull => "set_null",
                }
            }),
            Constraint::NotNull(c) => json!({
                "name": c.name,
                "type": "notnull",
                "field": c.field,
            }),
        }
    }
}

pub struct CascadeOperation {
    pub entity: String,
    pub id: String,
}

pub struct SetNullOperation {
    pub entity: String,
    pub id: String,
    pub field: String,
}

pub enum DeleteOperation {
    Cascade(CascadeOperation),
    SetNull(SetNullOperation),
}

pub struct OwnershipContext<'a> {
    pub sender: &'a str,
    pub ownership: &'a OwnershipConfig,
}

struct CrossOwnedRef {
    entity: String,
    id: String,
    field: String,
    owner: String,
}

pub struct ConstraintManager {
    constraints: HashMap<String, Vec<Constraint>>,
}

impl ConstraintManager {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            constraints: HashMap::new(),
        }
    }

    pub fn add_constraint(&mut self, constraint: Constraint) {
        let entity = constraint.entity().to_string();
        self.constraints.entry(entity).or_default().push(constraint);
    }

    #[must_use]
    pub fn get_constraints(&self, entity: &str) -> &[Constraint] {
        self.constraints.get(entity).map_or(&[], Vec::as_slice)
    }

    /// # Errors
    /// Returns an error if any constraint is violated.
    pub fn validate_create(
        &self,
        entity: &Entity,
        _batch: &mut BatchWriter,
        storage: &Storage,
    ) -> Result<()> {
        let constraints = self.get_constraints(&entity.name);

        for constraint in constraints {
            match constraint {
                Constraint::NotNull(c) => Self::validate_not_null(entity, c)?,
                Constraint::Unique(c) => Self::validate_unique(entity, c, storage)?,
                Constraint::ForeignKey(c) => Self::validate_foreign_key(entity, c, storage)?,
            }
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if any constraint is violated.
    pub fn validate_update(
        &self,
        entity: &Entity,
        old_entity: &Entity,
        _batch: &mut BatchWriter,
        storage: &Storage,
    ) -> Result<()> {
        let constraints = self.get_constraints(&entity.name);

        for constraint in constraints {
            match constraint {
                Constraint::NotNull(c) => Self::validate_not_null(entity, c)?,
                Constraint::Unique(c) => {
                    Self::validate_unique_update(entity, old_entity, c, storage)?;
                }
                Constraint::ForeignKey(c) => Self::validate_foreign_key(entity, c, storage)?,
            }
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if a foreign key constraint prevents deletion.
    pub fn validate_delete(
        &self,
        entity: &Entity,
        storage: &Storage,
        ownership_ctx: Option<&OwnershipContext<'_>>,
    ) -> Result<Vec<DeleteOperation>> {
        use std::collections::HashSet;
        let mut all_operations = Vec::new();
        let mut visited = HashSet::new();
        let mut cross_owned = Vec::new();
        self.collect_delete_operations(
            entity,
            storage,
            &mut all_operations,
            &mut visited,
            &mut cross_owned,
            ownership_ctx,
        )?;

        self.classify_cross_owned_danglers(
            entity,
            &cross_owned,
            &mut all_operations,
            ownership_ctx,
        )?;

        Ok(all_operations)
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_delete_operations(
        &self,
        entity: &Entity,
        storage: &Storage,
        all_operations: &mut Vec<DeleteOperation>,
        visited: &mut std::collections::HashSet<String>,
        cross_owned: &mut Vec<CrossOwnedRef>,
        ownership_ctx: Option<&OwnershipContext<'_>>,
    ) -> Result<()> {
        let entity_key = format!("{}/{}", entity.name, entity.id);
        if visited.contains(&entity_key) {
            return Ok(());
        }
        visited.insert(entity_key);

        let all_constraints: Vec<&Constraint> = self.constraints.values().flatten().collect();

        for constraint in all_constraints {
            if let Constraint::ForeignKey(fk) = constraint
                && fk.target_entity == entity.name
            {
                match fk.on_delete {
                    OnDeleteAction::Restrict => {
                        let referencing = Self::find_referencing_entities(
                            storage,
                            &fk.source_entity,
                            &fk.source_field,
                            &entity.id,
                        )?;
                        if !referencing.is_empty() {
                            return Err(Error::ForeignKeyRestrict {
                                entity: entity.name.clone(),
                                id: entity.id.clone(),
                                referencing_entity: fk.source_entity.clone(),
                            });
                        }
                    }
                    OnDeleteAction::Cascade => {
                        let referencing = Self::find_referencing_entities(
                            storage,
                            &fk.source_entity,
                            &fk.source_field,
                            &entity.id,
                        )?;
                        for id in referencing {
                            if Self::is_cross_owned(
                                storage,
                                &fk.source_entity,
                                &fk.source_field,
                                &id,
                                ownership_ctx,
                                cross_owned,
                            )? {
                                continue;
                            }

                            let cascade_key = keys::encode_data_key(&fk.source_entity, &id);
                            if let Some(cascade_data) = storage.get(&cascade_key)? {
                                let cascade_entity = Entity::deserialize(
                                    fk.source_entity.clone(),
                                    id.clone(),
                                    &cascade_data,
                                )?;
                                self.collect_delete_operations(
                                    &cascade_entity,
                                    storage,
                                    all_operations,
                                    visited,
                                    cross_owned,
                                    ownership_ctx,
                                )?;
                            }
                            all_operations.push(DeleteOperation::Cascade(CascadeOperation {
                                entity: fk.source_entity.clone(),
                                id,
                            }));
                        }
                    }
                    OnDeleteAction::SetNull => {
                        let referencing = Self::find_referencing_entities(
                            storage,
                            &fk.source_entity,
                            &fk.source_field,
                            &entity.id,
                        )?;
                        for id in referencing {
                            all_operations.push(DeleteOperation::SetNull(SetNullOperation {
                                entity: fk.source_entity.clone(),
                                id,
                                field: fk.source_field.clone(),
                            }));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn is_cross_owned(
        storage: &Storage,
        source_entity: &str,
        source_field: &str,
        ref_id: &str,
        ownership_ctx: Option<&OwnershipContext<'_>>,
        cross_owned: &mut Vec<CrossOwnedRef>,
    ) -> Result<bool> {
        let Some(ctx) = ownership_ctx else {
            return Ok(false);
        };

        if ctx.ownership.is_admin(ctx.sender) {
            return Ok(false);
        }

        let Some(owner_field) = ctx.ownership.owner_field(source_entity) else {
            return Ok(false);
        };

        let key = keys::encode_data_key(source_entity, ref_id);
        let Some(data) = storage.get(&key)? else {
            return Ok(false);
        };
        let entity = Entity::deserialize(source_entity.to_string(), ref_id.to_string(), &data)?;
        let owner = entity
            .data
            .get(owner_field)
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if owner == ctx.sender {
            return Ok(false);
        }

        cross_owned.push(CrossOwnedRef {
            entity: source_entity.to_string(),
            id: ref_id.to_string(),
            field: source_field.to_string(),
            owner: owner.to_string(),
        });
        Ok(true)
    }

    fn classify_cross_owned_danglers(
        &self,
        root_entity: &Entity,
        cross_owned: &[CrossOwnedRef],
        all_operations: &mut Vec<DeleteOperation>,
        ownership_ctx: Option<&OwnershipContext<'_>>,
    ) -> Result<()> {
        if ownership_ctx.is_none() || cross_owned.is_empty() {
            return Ok(());
        }

        for co in cross_owned {
            if self.has_not_null_constraint(&co.entity, &co.field) {
                return Err(Error::CascadeBlocked(Box::new(
                    crate::error::CascadeBlockedInfo {
                        entity: root_entity.name.clone(),
                        id: root_entity.id.clone(),
                        blocked_entity: co.entity.clone(),
                        blocked_id: co.id.clone(),
                        blocked_field: co.field.clone(),
                        blocked_owner: co.owner.clone(),
                    },
                )));
            }

            all_operations.push(DeleteOperation::SetNull(SetNullOperation {
                entity: co.entity.clone(),
                id: co.id.clone(),
                field: co.field.clone(),
            }));
        }

        Ok(())
    }

    fn has_not_null_constraint(&self, entity: &str, field: &str) -> bool {
        let Some(constraints) = self.constraints.get(entity) else {
            return false;
        };
        constraints
            .iter()
            .any(|c| matches!(c, Constraint::NotNull(nn) if nn.field == field))
    }

    fn validate_not_null(entity: &Entity, constraint: &NotNullConstraint) -> Result<()> {
        match entity.get_field(&constraint.field) {
            Some(value) if !value.is_null() => Ok(()),
            _ => Err(Error::NotNullViolation {
                entity: entity.name.clone(),
                field: constraint.field.clone(),
            }),
        }
    }

    fn validate_unique(
        entity: &Entity,
        constraint: &UniqueConstraint,
        storage: &Storage,
    ) -> Result<()> {
        for field in &constraint.fields {
            if let Some(value) = entity.get_field(field) {
                let value_bytes = keys::encode_value_for_index(value)?;
                let prefix = keys::encode_index_prefix(&entity.name, field, Some(&value_bytes));
                let existing = storage.prefix_scan(&prefix)?;

                for (key, _) in existing {
                    if let Some(existing_id) = Self::extract_id_from_index_key(&key)
                        && existing_id != entity.id
                    {
                        return Err(Error::UniqueViolation {
                            entity: entity.name.clone(),
                            field: field.clone(),
                            value: String::from_utf8_lossy(&value_bytes).to_string(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_unique_update(
        entity: &Entity,
        _old_entity: &Entity,
        constraint: &UniqueConstraint,
        storage: &Storage,
    ) -> Result<()> {
        Self::validate_unique(entity, constraint, storage)
    }

    fn validate_foreign_key(
        entity: &Entity,
        constraint: &ForeignKeyConstraint,
        storage: &Storage,
    ) -> Result<()> {
        if let Some(fk_value) = entity.get_field(&constraint.source_field)
            && !fk_value.is_null()
        {
            let target_id = fk_value.as_str().ok_or(Error::InvalidForeignKey)?;

            let target_key = keys::encode_data_key(&constraint.target_entity, target_id);

            if storage.get(&target_key)?.is_none() {
                return Err(Error::ForeignKeyViolation {
                    entity: entity.name.clone(),
                    field: constraint.source_field.clone(),
                    target_entity: constraint.target_entity.clone(),
                    target_id: target_id.to_string(),
                });
            }
        }

        Ok(())
    }

    fn find_referencing_entities(
        storage: &Storage,
        source_entity: &str,
        source_field: &str,
        target_id: &str,
    ) -> Result<Vec<String>> {
        use crate::entity::Entity;

        let prefix = keys::encode_data_key(source_entity, "");
        let items = storage.prefix_scan(&prefix)?;

        let mut referencing_ids = Vec::new();

        for (key, value) in items {
            if let Ok((_entity, id)) = keys::decode_data_key(&key)
                && let Ok(entity) =
                    Entity::deserialize(source_entity.to_string(), id.clone(), &value)
                && let Some(fk_value) = entity.get_field(source_field)
                && let Some(fk_str) = fk_value.as_str()
                && fk_str == target_id
            {
                referencing_ids.push(id);
            }
        }

        Ok(referencing_ids)
    }

    fn extract_id_from_index_key(key: &[u8]) -> Option<String> {
        if let Some(last_slash) = key.iter().rposition(|&b| b == b'/') {
            String::from_utf8(key[last_slash + 1..].to_vec()).ok()
        } else {
            None
        }
    }

    /// # Errors
    /// Returns an error if serialization fails.
    pub fn persist_constraint(
        &self,
        batch: &mut BatchWriter,
        constraint: &Constraint,
    ) -> Result<()> {
        let key = keys::encode_constraint_key(
            constraint.constraint_type(),
            constraint.entity(),
            constraint.name(),
        );
        let value = serde_json::to_vec(constraint)?;
        batch.insert(key, value);
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading or deserializing constraints fails.
    pub fn load_constraints(&mut self, storage: &Storage) -> Result<()> {
        let prefix = b"meta/constraint/";
        let items = storage.prefix_scan(prefix)?;

        for (_key, value) in items {
            let constraint: Constraint = serde_json::from_slice(&value)?;
            self.add_constraint(constraint);
        }

        Ok(())
    }

    #[must_use]
    pub fn entity_names(&self) -> Vec<String> {
        self.constraints.keys().cloned().collect()
    }

    #[must_use]
    pub fn all_constraints(&self) -> &HashMap<String, Vec<Constraint>> {
        &self.constraints
    }

    pub fn remove_constraint(&mut self, batch: &mut BatchWriter, entity: &str, name: &str) {
        if let Some(constraints) = self.constraints.get_mut(entity)
            && let Some(pos) = constraints.iter().position(|c| c.name() == name)
        {
            let constraint = constraints.remove(pos);
            let key = keys::encode_constraint_key(
                constraint.constraint_type(),
                constraint.entity(),
                constraint.name(),
            );
            batch.remove(key);
        }
    }
}

impl Default for ConstraintManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_creation() {
        let unique = UniqueConstraint::new("users", vec!["email".to_string()]);
        assert_eq!(unique.entity, "users");
        assert_eq!(unique.fields, vec!["email"]);
        assert_eq!(unique.name, "users_email_unique");

        let fk =
            ForeignKeyConstraint::new("posts", "author_id", "users", "id", OnDeleteAction::Cascade);
        assert_eq!(fk.source_entity, "posts");
        assert_eq!(fk.source_field, "author_id");
        assert_eq!(fk.target_entity, "users");
        assert_eq!(fk.on_delete, OnDeleteAction::Cascade);

        let not_null = NotNullConstraint::new("users", "email");
        assert_eq!(not_null.entity, "users");
        assert_eq!(not_null.field, "email");
    }

    #[test]
    fn test_constraint_manager() {
        let mut manager = ConstraintManager::new();

        let constraint =
            Constraint::Unique(UniqueConstraint::new("users", vec!["email".to_string()]));
        manager.add_constraint(constraint);

        let constraints = manager.get_constraints("users");
        assert_eq!(constraints.len(), 1);
    }
}
