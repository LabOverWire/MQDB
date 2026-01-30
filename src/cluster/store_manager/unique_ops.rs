use super::StoreManager;
use crate::cluster::db::{ReserveResult, UniqueReservation, UniqueStore, unique_partition};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn unique_reserve_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        request_id: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
        now: u64,
    ) -> (ReserveResult, Option<ReplicationWrite>) {
        let result = self.db_unique.reserve(
            entity,
            field,
            value,
            record_id,
            request_id,
            data_partition,
            ttl_ms,
            now,
        );

        match result {
            ReserveResult::Reserved => {
                let reservation = self.db_unique.get(entity, field, value);
                if let Some(r) = reservation {
                    let serialized = UniqueStore::serialize(&r);
                    let partition = unique_partition(entity, field, value);
                    let key = super::super::db::unique_key(&r);
                    let write = ReplicationWrite::new(
                        partition,
                        Operation::Insert,
                        Epoch::ZERO,
                        0,
                        entity::DB_UNIQUE.to_string(),
                        key,
                        serialized,
                    );
                    (ReserveResult::Reserved, Some(write))
                } else {
                    (ReserveResult::Reserved, None)
                }
            }
            other => (other, None),
        }
    }

    pub fn unique_commit_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        request_id: &str,
    ) -> Result<(UniqueReservation, ReplicationWrite), super::super::db::UniqueStoreError> {
        let committed = self.db_unique.commit(entity, field, value, request_id)?;
        let serialized = UniqueStore::serialize(&committed);
        let partition = unique_partition(entity, field, value);
        let key = super::super::db::unique_key(&committed);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::DB_UNIQUE.to_string(),
            key,
            serialized,
        );
        Ok((committed, write))
    }

    #[must_use]
    pub fn unique_release_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        request_id: &str,
    ) -> Option<ReplicationWrite> {
        fn encode_hex(bytes: &[u8]) -> String {
            use std::fmt::Write;
            bytes
                .iter()
                .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                })
        }

        if self.db_unique.release(entity, field, value, request_id) {
            let partition = unique_partition(entity, field, value);
            let key = format!("_unique/{entity}/{field}/{}", encode_hex(value));
            Some(ReplicationWrite::new(
                partition,
                Operation::Delete,
                Epoch::ZERO,
                0,
                entity::DB_UNIQUE.to_string(),
                key,
                vec![],
            ))
        } else {
            None
        }
    }

    #[must_use]
    pub fn unique_check(&self, entity: &str, field: &str, value: &[u8], now: u64) -> bool {
        self.db_unique.check(entity, field, value, now)
    }

    #[must_use]
    pub fn unique_get(&self, entity: &str, field: &str, value: &[u8]) -> Option<UniqueReservation> {
        self.db_unique.get(entity, field, value)
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn unique_reserve(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        request_id: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
        now: u64,
    ) -> ReserveResult {
        self.db_unique.reserve(
            entity,
            field,
            value,
            record_id,
            request_id,
            data_partition,
            ttl_ms,
            now,
        )
    }

    pub fn unique_commit(&self, entity: &str, field: &str, value: &[u8], request_id: &str) -> bool {
        self.db_unique
            .commit(entity, field, value, request_id)
            .is_ok()
    }

    pub fn unique_release(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        request_id: &str,
    ) -> bool {
        self.db_unique.release(entity, field, value, request_id)
    }

    pub fn unique_cleanup_expired(&self, now: u64) -> usize {
        self.db_unique.cleanup_expired(now)
    }
}
