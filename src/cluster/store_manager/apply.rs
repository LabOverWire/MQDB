use super::{StoreApplyError, StoreManager};
use crate::cluster::entity;
use crate::cluster::protocol::ReplicationWrite;

impl StoreManager {
    pub fn persist_writes_batch(&self, writes: &[ReplicationWrite]) -> Result<(), StoreApplyError> {
        if let Some(storage) = &self.storage {
            storage
                .write_batch(writes)
                .map_err(|_| StoreApplyError::PersistenceError)?;
        }
        Ok(())
    }

    pub(super) fn persist_broadcast_batch(&self, writes: &[ReplicationWrite], context: &str) {
        if let Some(storage) = &self.storage
            && let Err(e) = storage.write_batch(writes)
        {
            tracing::warn!(context, error = ?e, "failed to persist broadcast batch");
        }
    }

    pub fn apply_write(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        if let Some(storage) = &self.storage {
            storage
                .write(write)
                .map_err(|_| StoreApplyError::PersistenceError)?;
        }

        self.apply_to_memory(write)
    }

    pub(super) fn apply_to_memory(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        match write.entity.as_str() {
            entity::SESSIONS => self.apply_session(write),
            entity::QOS2 => self.apply_qos2(write),
            entity::SUBSCRIPTIONS => self.apply_subscription(write),
            entity::RETAINED => self.apply_retained(write),
            entity::TOPIC_INDEX => self.apply_topic_index(write),
            entity::WILDCARDS => self.apply_wildcard(write),
            entity::INFLIGHT => self.apply_inflight(write),
            entity::OFFSETS => self.apply_offset(write),
            entity::IDEMPOTENCY => self.apply_idempotency(write),
            entity::DB_DATA => self.apply_db_data(write),
            entity::DB_SCHEMA => self.apply_db_schema(write),
            entity::DB_INDEX => self.apply_db_index(write),
            entity::DB_UNIQUE => self.apply_db_unique(write),
            entity::DB_FK => self.apply_db_fk(write),
            entity::DB_CONSTRAINT => self.apply_db_constraint(write),
            entity::CLIENT_LOCATIONS => self.apply_client_location(write),
            _ => Err(StoreApplyError::UnknownEntity),
        }
    }

    fn apply_client_location(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.client_locations
            .apply_replicated(write.operation, &write.data)
            .map_err(|_| StoreApplyError::ClientLocationError)
    }

    fn apply_session(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.sessions
            .apply_replicated_by_id(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::SessionError)
    }

    fn apply_qos2(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.qos2
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::Qos2Error)
    }

    fn apply_subscription(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.subscriptions
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::SubscriptionError)
    }

    fn apply_retained(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.retained
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::RetainedError)
    }

    fn apply_topic_index(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.topics
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::TopicIndexError)
    }

    fn apply_wildcard(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.wildcards
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::WildcardError)
    }

    fn apply_inflight(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.inflight
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::InflightError)
    }

    fn apply_offset(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.offsets
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::OffsetError)
    }

    fn apply_idempotency(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.idempotency
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::IdempotencyError)
    }

    fn apply_db_data(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_data
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbDataError)
    }

    fn apply_db_schema(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_schema
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbSchemaError)
    }

    fn apply_db_index(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_index
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbIndexError)
    }

    fn apply_db_unique(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_unique
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbUniqueError)
    }

    fn apply_db_fk(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_fk
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbFkError)
    }

    fn apply_db_constraint(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
        self.db_constraints
            .apply_replicated(write.operation, &write.id, &write.data)
            .map_err(|_| StoreApplyError::DbConstraintError)
    }
}
