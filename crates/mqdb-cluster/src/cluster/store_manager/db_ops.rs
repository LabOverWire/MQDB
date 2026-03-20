// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::db::{DbDataStore, DbDataStoreError, DbEntity, data_partition};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, entity};

impl StoreManager {
    /// # Errors
    /// Returns `DbDataStoreError` if the record already exists or serialization fails.
    pub fn db_create_replicated(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self.db_data.create(entity_type, id, data, timestamp_ms)?;
        self.update_fk_reverse_index(Operation::Insert, entity_type, id, Some(data), None);
        let partition = data_partition(entity_type, id);
        let serialized = DbDataStore::serialize(&db_entity);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_DATA.to_string(),
            db_entity.key(),
            serialized,
        );
        Ok((db_entity, write))
    }

    /// # Errors
    /// Returns `DbDataStoreError` if the record does not exist.
    pub fn db_update_replicated(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let old = self.db_data.get(entity_type, id);
        let db_entity = self.db_data.update(entity_type, id, data, timestamp_ms)?;
        self.update_fk_reverse_index(
            Operation::Update,
            entity_type,
            id,
            Some(data),
            old.as_ref().map(|e| e.data.as_slice()),
        );
        let partition = data_partition(entity_type, id);
        let serialized = DbDataStore::serialize(&db_entity);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::DB_DATA.to_string(),
            db_entity.key(),
            serialized,
        );
        Ok((db_entity, write))
    }

    #[must_use]
    pub fn db_upsert_replicated(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> (DbEntity, ReplicationWrite) {
        let old = self.db_data.get(entity_type, id);
        let db_entity = self.db_data.upsert(entity_type, id, data, timestamp_ms);
        self.update_fk_reverse_index(
            Operation::Update,
            entity_type,
            id,
            Some(data),
            old.as_ref().map(|e| e.data.as_slice()),
        );
        let partition = data_partition(entity_type, id);
        let serialized = DbDataStore::serialize(&db_entity);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::DB_DATA.to_string(),
            db_entity.key(),
            serialized,
        );
        (db_entity, write)
    }

    /// # Errors
    /// Returns `DbDataStoreError` if the record does not exist.
    pub fn db_delete_replicated(
        &self,
        entity_type: &str,
        id: &str,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self.db_data.delete(entity_type, id)?;
        self.update_fk_reverse_index(
            Operation::Delete,
            entity_type,
            id,
            None,
            Some(&db_entity.data),
        );
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
