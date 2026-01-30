use super::StoreManager;
use crate::cluster::db::{ClusterSchema, SchemaStore};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, PartitionId, entity};

impl StoreManager {
    pub fn schema_register_replicated(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<(ClusterSchema, Vec<ReplicationWrite>), super::super::db::SchemaStoreError> {
        let schema = self.db_schema.register(entity, schema_data)?;
        let serialized = SchemaStore::serialize(&schema);
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|p| {
                ReplicationWrite::new(
                    p,
                    Operation::Insert,
                    Epoch::ZERO,
                    0,
                    entity::DB_SCHEMA.to_string(),
                    entity.to_string(),
                    serialized.clone(),
                )
            })
            .collect();

        self.persist_broadcast_batch(&writes, "schema_register");

        Ok((schema, writes))
    }

    pub fn schema_update_replicated(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<(ClusterSchema, Vec<ReplicationWrite>), super::super::db::SchemaStoreError> {
        let schema = self.db_schema.update(entity, schema_data)?;
        let serialized = SchemaStore::serialize(&schema);
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|p| {
                ReplicationWrite::new(
                    p,
                    Operation::Update,
                    Epoch::ZERO,
                    0,
                    entity::DB_SCHEMA.to_string(),
                    entity.to_string(),
                    serialized.clone(),
                )
            })
            .collect();

        self.persist_broadcast_batch(&writes, "schema_update");

        Ok((schema, writes))
    }

    #[must_use]
    pub fn schema_get(&self, entity: &str) -> Option<ClusterSchema> {
        self.db_schema.get(entity)
    }

    #[must_use]
    pub fn schema_list(&self) -> Vec<ClusterSchema> {
        self.db_schema.list()
    }

    #[must_use]
    pub fn schema_is_valid_for_write(&self, entity: &str) -> bool {
        self.db_schema.is_valid_for_write(entity)
    }
}
