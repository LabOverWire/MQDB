use super::StoreManager;
use crate::cluster::db::{IndexEntry, IndexStore, index_partition};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    pub fn index_add_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Result<(IndexEntry, ReplicationWrite), super::super::db::IndexStoreError> {
        let entry = IndexEntry::create(entity, field, value, data_partition, record_id);
        self.db_index.add_entry(entry.clone())?;

        let serialized = IndexStore::serialize(&entry);
        let idx_partition = index_partition(entity, field, value);
        let key = super::super::db::index_key(&entry);

        let write = ReplicationWrite::new(
            idx_partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_INDEX.to_string(),
            key,
            serialized,
        );
        Ok((entry, write))
    }

    pub fn index_remove_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Result<(IndexEntry, ReplicationWrite), super::super::db::IndexStoreError> {
        let entry = self
            .db_index
            .remove_entry(entity, field, value, data_partition, record_id)?;

        let idx_partition = index_partition(entity, field, value);
        let key = super::super::db::index_key(&entry);

        let write = ReplicationWrite::new(
            idx_partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::DB_INDEX.to_string(),
            key,
            vec![],
        );
        Ok((entry, write))
    }

    #[must_use]
    pub fn index_lookup(&self, entity: &str, field: &str, value: &[u8]) -> Vec<IndexEntry> {
        self.db_index.lookup(entity, field, value)
    }
}
