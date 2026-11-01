// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::{DbDataStore, DbDataStoreError, DbEntity, data_partition};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, entity};

impl StoreManager {
    /// # Errors
    /// Returns `DbDataStoreError` if the record already exists or serialization fails.
    pub fn db_create_write(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        if self.db_data.contains(entity_type, id) {
            return Err(DbDataStoreError::AlreadyExists);
        }
        Self::record_write(entity_type, id, data, timestamp_ms, Operation::Insert)
    }

    /// # Errors
    /// Returns `DbDataStoreError` if the record does not exist or serialization fails.
    pub fn db_update_write(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        if !self.db_data.contains(entity_type, id) {
            return Err(DbDataStoreError::NotFound);
        }
        Self::record_write(entity_type, id, data, timestamp_ms, Operation::Update)
    }

    fn record_write(
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
        operation: Operation,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = DbEntity::create(entity_type, id, data, timestamp_ms)
            .ok_or(DbDataStoreError::SerializationError)?;
        let write = ReplicationWrite::new(
            data_partition(entity_type, id),
            operation,
            Epoch::ZERO,
            0,
            entity::DB_DATA.to_string(),
            db_entity.key(),
            DbDataStore::serialize(&db_entity),
        );
        Ok((db_entity, write))
    }

    /// # Errors
    /// Returns `DbDataStoreError` if the record does not exist.
    pub fn db_delete_write(
        &self,
        entity_type: &str,
        id: &str,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self
            .db_data
            .get(entity_type, id)
            .ok_or(DbDataStoreError::NotFound)?;
        let partition = data_partition(entity_type, id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::DB_DATA.to_string(),
            db_entity.key(),
            Vec::new(),
        );
        Ok((db_entity, write))
    }

    #[must_use]
    pub fn db_get(&self, entity_type: &str, id: &str) -> Option<DbEntity> {
        self.db_data.get(entity_type, id)
    }

    #[must_use]
    pub fn db_list(&self, entity_type: &str) -> Vec<DbEntity> {
        self.db_data.list(entity_type)
    }
}
