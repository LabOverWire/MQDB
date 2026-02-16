// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::idempotency_store::{IdempotencyCheck, IdempotencyError, IdempotencyStore};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::session::SessionData;
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    /// # Errors
    /// Returns `IdempotencyError` if the key is already committed or a conflict is detected.
    pub fn check_idempotency(
        &self,
        idempotency_key: &str,
        partition: PartitionId,
        epoch: Epoch,
        entity: &str,
        id: &str,
        timestamp: u64,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        self.idempotency.check_or_insert_processing(
            idempotency_key,
            partition,
            epoch,
            entity,
            id,
            timestamp,
        )
    }

    pub fn commit_idempotency(
        &self,
        partition: PartitionId,
        idempotency_key: &str,
        response: &[u8],
    ) -> ReplicationWrite {
        self.idempotency
            .mark_committed(partition, idempotency_key, response.to_vec());
        let record = self.idempotency.get(partition, idempotency_key);
        let data = record
            .map(|r| IdempotencyStore::serialize(&r))
            .unwrap_or_default();
        ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::IDEMPOTENCY.to_string(),
            format!("{}:{idempotency_key}", partition.get()),
            data,
        )
    }

    pub fn rollback_idempotency(&self, partition: PartitionId, idempotency_key: &str) {
        self.idempotency
            .remove_processing(partition, idempotency_key);
    }

    pub fn cleanup_expired_idempotency(&self, now: u64) -> usize {
        self.idempotency.cleanup_expired(now)
    }

    pub fn cleanup_expired_sessions(&self, now: u64) -> Vec<SessionData> {
        self.sessions.cleanup_expired_sessions(now)
    }

    #[must_use]
    pub fn expired_sessions(&self, now: u64) -> Vec<SessionData> {
        self.sessions.expired_sessions(now)
    }

    pub fn cleanup_stale_offsets(&self, now: u64) -> usize {
        self.offsets.cleanup_stale(now)
    }
}
