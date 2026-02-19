// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::{ClusterConstraint, ConstraintStore, ConstraintType};
use crate::cluster::node_controller::fk::extract_fk_string_value;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    /// # Errors
    /// Returns `ConstraintStoreError::AlreadyExists` if the constraint is already registered.
    pub fn constraint_add_replicated(
        &self,
        constraint: &ClusterConstraint,
    ) -> Result<ReplicationWrite, super::super::db::ConstraintStoreError> {
        let key = super::super::db::constraint_key(constraint.entity_str(), constraint.name_str());
        if self
            .db_constraints
            .exists(constraint.entity_str(), constraint.name_str())
        {
            return Err(super::super::db::ConstraintStoreError::AlreadyExists);
        }
        let serialized = ConstraintStore::serialize(constraint);

        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_CONSTRAINT.to_string(),
            key,
            serialized,
        );

        Ok(write)
    }

    /// # Errors
    /// Returns `ConstraintStoreError::NotFound` if the constraint does not exist.
    pub fn constraint_remove_replicated(
        &self,
        entity_type: &str,
        name: &str,
    ) -> Result<ReplicationWrite, super::super::db::ConstraintStoreError> {
        let key = super::super::db::constraint_key(entity_type, name);
        if !self.db_constraints.exists(entity_type, name) {
            return Err(super::super::db::ConstraintStoreError::NotFound);
        }

        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::DB_CONSTRAINT.to_string(),
            key,
            Vec::new(),
        );

        Ok(write)
    }

    #[must_use]
    pub fn constraint_list(&self, entity_type: &str) -> Vec<ClusterConstraint> {
        self.db_constraints.list(entity_type)
    }

    #[must_use]
    pub fn constraint_list_all(&self) -> Vec<ClusterConstraint> {
        self.db_constraints.list_all()
    }

    #[must_use]
    pub fn constraint_get_unique_fields(&self, entity_type: &str) -> Vec<String> {
        self.db_constraints.get_unique_fields(entity_type)
    }

    #[must_use]
    pub fn constraint_get_fk_constraints(
        &self,
        entity_type: &str,
    ) -> Vec<super::super::db::ClusterConstraint> {
        self.db_constraints.get_fk_constraints(entity_type)
    }

    #[must_use]
    pub fn constraint_find_referencing(
        &self,
        target_entity: &str,
    ) -> Vec<super::super::db::ClusterConstraint> {
        self.db_constraints
            .find_referencing_constraints(target_entity)
    }

    #[must_use]
    pub fn fk_reverse_lookup(
        &self,
        target_entity: &str,
        target_id: &str,
        source_entity: &str,
        source_field: &str,
    ) -> Vec<String> {
        self.fk_reverse_index
            .lookup(target_entity, target_id, source_entity, source_field)
    }

    pub fn rebuild_fk_index_for_constraint(&self, constraint: &ClusterConstraint) {
        if constraint.constraint_type() != ConstraintType::ForeignKey {
            return;
        }
        let source_entity = constraint.entity_str();
        let source_field = constraint.field_str();
        let target_entity = constraint.target_entity_str();

        for record in self.db_data.list(source_entity) {
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.data)
                && let Some(fk_val) = parsed.get(source_field).and_then(extract_fk_string_value)
            {
                self.fk_reverse_index.insert(
                    target_entity,
                    &fk_val,
                    source_entity,
                    source_field,
                    record.id_str(),
                );
            }
        }
    }

    pub fn update_fk_reverse_index(
        &self,
        op: Operation,
        source_entity: &str,
        source_id: &str,
        new_data: Option<&[u8]>,
        old_data: Option<&[u8]>,
    ) {
        let fk_constraints = self.db_constraints.get_fk_constraints(source_entity);

        if fk_constraints.is_empty() {
            return;
        }

        if matches!(op, Operation::Delete | Operation::Update)
            && let Some(raw) = old_data
            && let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(raw)
        {
            for c in &fk_constraints {
                if let Some(fk_val) = parsed.get(c.field_str()).and_then(extract_fk_string_value) {
                    self.fk_reverse_index.remove(
                        c.target_entity_str(),
                        &fk_val,
                        source_entity,
                        c.field_str(),
                        source_id,
                    );
                }
            }
        }

        if matches!(op, Operation::Insert | Operation::Update)
            && let Some(raw) = new_data
            && let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(raw)
        {
            for c in &fk_constraints {
                if let Some(fk_val) = parsed.get(c.field_str()).and_then(extract_fk_string_value) {
                    self.fk_reverse_index.insert(
                        c.target_entity_str(),
                        &fk_val,
                        source_entity,
                        c.field_str(),
                        source_id,
                    );
                }
            }
        }
    }
}
