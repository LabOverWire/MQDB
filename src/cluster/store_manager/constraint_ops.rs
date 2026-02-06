// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::{ClusterConstraint, ConstraintStore};
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
}
