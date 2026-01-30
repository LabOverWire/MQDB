use super::StoreManager;
use crate::cluster::db::{FkValidationRequest, FkValidationStore};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, entity};

impl StoreManager {
    pub fn fk_add_request_replicated(
        &self,
        request: FkValidationRequest,
    ) -> Result<ReplicationWrite, super::super::db::FkStoreError> {
        let partition = request.target_partition();
        let key = request.request_id_str().to_string();
        let serialized = FkValidationStore::serialize_request(&request);
        self.db_fk.add_request(request)?;
        Ok(ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_FK.to_string(),
            key,
            serialized,
        ))
    }

    pub fn fk_complete_request_replicated(
        &self,
        request_id: &str,
    ) -> Result<(FkValidationRequest, ReplicationWrite), super::super::db::FkStoreError> {
        let request = self.db_fk.complete_request(request_id)?;
        let partition = request.target_partition();
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::DB_FK.to_string(),
            request_id.to_string(),
            vec![],
        );
        Ok((request, write))
    }

    #[must_use]
    pub fn fk_get_request(&self, request_id: &str) -> Option<FkValidationRequest> {
        self.db_fk.get_request(request_id)
    }

    pub fn fk_cleanup_expired(&self, now: u64) -> usize {
        self.db_fk.cleanup_expired(now)
    }
}
