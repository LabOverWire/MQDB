use bebytes::BeBytes;

use super::SubscriptionType;
use super::db::{
    ClusterSchema, DbDataStore, DbDataStoreError, DbEntity, FkValidationRequest, FkValidationStore,
    IndexEntry, IndexStore, ReserveResult, SchemaStore, UniqueReservation, UniqueStore,
    data_partition, index_partition, unique_partition,
};
use super::entity;
use super::idempotency_store::{IdempotencyCheck, IdempotencyError, IdempotencyStore};
use super::inflight_store::{InflightMessage, InflightStore, InflightStoreError};
use super::offset_store::{ConsumerOffset, OffsetStore};
use super::protocol::{Operation, ReplicationWrite};
use super::qos2_store::{Qos2Phase, Qos2State, Qos2Store, Qos2StoreError};
use super::retained_store::{RetainedMessage, RetainedStore};
use super::session::{SessionData, SessionError, SessionStore, session_partition};
use super::subscription_cache::{
    MqttSubscriptionSnapshot, SubscriptionCache, SubscriptionCacheError,
};
use super::topic_index::{TopicIndex, TopicIndexEntry, TopicIndexError, topic_partition};
use super::wildcard_pending::WildcardPendingStore;
use super::wildcard_store::{WildcardEntry, WildcardStore, WildcardStoreError};
use super::{Epoch, NodeId, PartitionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreApplyError {
    UnknownEntity,
    SessionError,
    Qos2Error,
    SubscriptionError,
    RetainedError,
    TopicIndexError,
    WildcardError,
    InflightError,
    OffsetError,
    IdempotencyError,
    DbDataError,
    DbSchemaError,
    DbIndexError,
    DbUniqueError,
    DbFkError,
}

impl std::fmt::Display for StoreApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEntity => write!(f, "unknown entity type"),
            Self::SessionError => write!(f, "session store error"),
            Self::Qos2Error => write!(f, "qos2 store error"),
            Self::SubscriptionError => write!(f, "subscription cache error"),
            Self::RetainedError => write!(f, "retained store error"),
            Self::TopicIndexError => write!(f, "topic index error"),
            Self::WildcardError => write!(f, "wildcard store error"),
            Self::InflightError => write!(f, "inflight store error"),
            Self::OffsetError => write!(f, "offset store error"),
            Self::IdempotencyError => write!(f, "idempotency store error"),
            Self::DbDataError => write!(f, "db data store error"),
            Self::DbSchemaError => write!(f, "db schema store error"),
            Self::DbIndexError => write!(f, "db index store error"),
            Self::DbUniqueError => write!(f, "db unique store error"),
            Self::DbFkError => write!(f, "db fk store error"),
        }
    }
}

impl std::error::Error for StoreApplyError {}

pub struct StoreManager {
    pub sessions: SessionStore,
    pub qos2: Qos2Store,
    pub subscriptions: SubscriptionCache,
    pub retained: RetainedStore,
    pub topics: TopicIndex,
    pub wildcards: WildcardStore,
    pub wildcard_pending: WildcardPendingStore,
    pub inflight: InflightStore,
    pub offsets: OffsetStore,
    pub idempotency: IdempotencyStore,
    pub db_data: DbDataStore,
    pub db_schema: SchemaStore,
    pub db_index: IndexStore,
    pub db_unique: UniqueStore,
    pub db_fk: FkValidationStore,
}

impl StoreManager {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            sessions: SessionStore::new(node_id),
            qos2: Qos2Store::new(node_id),
            subscriptions: SubscriptionCache::new(node_id),
            retained: RetainedStore::new(node_id),
            topics: TopicIndex::new(node_id),
            wildcards: WildcardStore::new(node_id),
            wildcard_pending: WildcardPendingStore::new(node_id),
            inflight: InflightStore::new(node_id),
            offsets: OffsetStore::new(node_id),
            idempotency: IdempotencyStore::new(node_id),
            db_data: DbDataStore::new(node_id),
            db_schema: SchemaStore::new(node_id),
            db_index: IndexStore::new(node_id),
            db_unique: UniqueStore::new(node_id),
            db_fk: FkValidationStore::new(node_id),
        }
    }

    /// # Errors
    /// Returns an error if the entity type is unknown or store application fails.
    pub fn apply_write(&self, write: &ReplicationWrite) -> Result<(), StoreApplyError> {
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
            _ => Err(StoreApplyError::UnknownEntity),
        }
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

    /// # Errors
    /// Returns `DbDataStoreError::AlreadyExists` if the entity already exists.
    pub fn db_create_replicated(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self.db_data.create(entity_type, id, data, timestamp_ms)?;
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
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub fn db_update_replicated(
        &self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self.db_data.update(entity_type, id, data, timestamp_ms)?;
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
        let db_entity = self.db_data.upsert(entity_type, id, data, timestamp_ms);
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
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub fn db_delete_replicated(
        &self,
        entity_type: &str,
        id: &str,
    ) -> Result<(DbEntity, ReplicationWrite), DbDataStoreError> {
        let db_entity = self.db_data.delete(entity_type, id)?;
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

    /// # Errors
    /// Returns `SchemaStoreError::AlreadyExists` if the schema already exists.
    pub fn schema_register_replicated(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<(ClusterSchema, Vec<ReplicationWrite>), super::db::SchemaStoreError> {
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
        Ok((schema, writes))
    }

    /// # Errors
    /// Returns `SchemaStoreError::NotFound` if the schema does not exist.
    pub fn schema_update_replicated(
        &self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<(ClusterSchema, Vec<ReplicationWrite>), super::db::SchemaStoreError> {
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

    /// # Errors
    /// Returns `IndexStoreError::AlreadyExists` if the entry already exists.
    pub fn index_add_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Result<(IndexEntry, ReplicationWrite), super::db::IndexStoreError> {
        let entry = IndexEntry::create(entity, field, value, data_partition, record_id);
        self.db_index.add_entry(entry.clone())?;

        let serialized = IndexStore::serialize(&entry);
        let idx_partition = index_partition(entity, field, value);
        let key = super::db::index_key(&entry);

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

    /// # Errors
    /// Returns `IndexStoreError::NotFound` if the entry does not exist.
    pub fn index_remove_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: PartitionId,
        record_id: &str,
    ) -> Result<(IndexEntry, ReplicationWrite), super::db::IndexStoreError> {
        let entry = self
            .db_index
            .remove_entry(entity, field, value, data_partition, record_id)?;

        let idx_partition = index_partition(entity, field, value);
        let key = super::db::index_key(&entry);

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
                    let key = super::db::unique_key(&r);
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

    /// # Errors
    /// Returns `UniqueStoreError` if reservation not found, wrong `request_id`, or already committed.
    pub fn unique_commit_replicated(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        request_id: &str,
    ) -> Result<(UniqueReservation, ReplicationWrite), super::db::UniqueStoreError> {
        let committed = self.db_unique.commit(entity, field, value, request_id)?;
        let serialized = UniqueStore::serialize(&committed);
        let partition = unique_partition(entity, field, value);
        let key = super::db::unique_key(&committed);
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

    /// # Errors
    /// Returns `FkStoreError::AlreadyExists` if request with same ID exists.
    pub fn fk_add_request_replicated(
        &self,
        request: FkValidationRequest,
    ) -> Result<ReplicationWrite, super::db::FkStoreError> {
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

    /// # Errors
    /// Returns `FkStoreError::NotFound` if request does not exist.
    pub fn fk_complete_request_replicated(
        &self,
        request_id: &str,
    ) -> Result<(FkValidationRequest, ReplicationWrite), super::db::FkStoreError> {
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

    pub fn unique_cleanup_expired(&self, now: u64) -> usize {
        self.db_unique.cleanup_expired(now)
    }

    /// # Errors
    /// Returns `SessionError::AlreadyExists` if the session already exists.
    pub fn create_session_replicated(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, ReplicationWrite), SessionError> {
        let (session, data) = self.sessions.create_session_with_data(client_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }

    /// # Errors
    /// Returns `SessionError::NotFound` if the session does not exist.
    pub fn update_session_replicated<F>(
        &self,
        client_id: &str,
        f: F,
    ) -> Result<(SessionData, ReplicationWrite), SessionError>
    where
        F: FnOnce(&mut SessionData),
    {
        let (session, data) = self.sessions.update_with_data(client_id, f)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }

    /// # Errors
    /// Returns `SessionError::NotFound` if the session does not exist.
    pub fn remove_session_replicated(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, ReplicationWrite), SessionError> {
        let (session, data) = self.sessions.remove_with_data(client_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::SESSIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((session, write))
    }

    /// # Errors
    /// Returns `Qos2StoreError::AlreadyExists` if the state already exists.
    pub fn start_qos2_inbound_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(Qos2State, ReplicationWrite), Qos2StoreError> {
        let (state, data) = self
            .qos2
            .start_inbound_with_data(client_id, packet_id, topic, payload, timestamp)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::QOS2.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((state, write))
    }

    /// # Errors
    /// Returns `Qos2StoreError::AlreadyExists` if the state already exists.
    pub fn start_qos2_outbound_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(Qos2State, ReplicationWrite), Qos2StoreError> {
        let (state, data) = self
            .qos2
            .start_outbound_with_data(client_id, packet_id, topic, payload, timestamp)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::QOS2.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((state, write))
    }

    /// # Errors
    /// Returns `Qos2StoreError::NotFound` or `InvalidState` if state is invalid.
    pub fn advance_qos2_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<(Qos2Phase, ReplicationWrite), Qos2StoreError> {
        let (phase, data) = self.qos2.advance_with_data(client_id, packet_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::QOS2.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((phase, write))
    }

    /// # Errors
    /// Returns `Qos2StoreError::NotFound` if state does not exist.
    pub fn complete_qos2_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<(Qos2State, ReplicationWrite), Qos2StoreError> {
        let (state, data) = self.qos2.complete_with_data(client_id, packet_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::QOS2.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((state, write))
    }

    #[must_use]
    pub fn add_subscription_replicated(
        &self,
        client_id: &str,
        topic: &str,
        qos: u8,
    ) -> (MqttSubscriptionSnapshot, ReplicationWrite) {
        let (snapshot, data) = self
            .subscriptions
            .add_subscription_with_data(client_id, topic, qos);
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::SUBSCRIPTIONS.to_string(),
            client_id.to_string(),
            data,
        );
        (snapshot, write)
    }

    /// # Errors
    /// Returns `SubscriptionCacheError::NotFound` if client has no subscriptions.
    pub fn remove_subscription_replicated(
        &self,
        client_id: &str,
        topic: &str,
    ) -> Result<(MqttSubscriptionSnapshot, ReplicationWrite), SubscriptionCacheError> {
        let (snapshot, data) = self
            .subscriptions
            .remove_subscription_with_data(client_id, topic)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::SUBSCRIPTIONS.to_string(),
            client_id.to_string(),
            data,
        );
        Ok((snapshot, write))
    }

    #[must_use]
    pub fn set_retained_replicated(
        &self,
        topic: &str,
        qos: u8,
        payload: &[u8],
        timestamp_ms: u64,
    ) -> (Option<RetainedMessage>, ReplicationWrite) {
        let (msg, data) = self
            .retained
            .set_with_data(topic, qos, payload, timestamp_ms);
        let partition = topic_partition(topic);
        let operation = if payload.is_empty() {
            Operation::Delete
        } else {
            Operation::Insert
        };
        let write = ReplicationWrite::new(
            partition,
            operation,
            Epoch::ZERO,
            0,
            entity::RETAINED.to_string(),
            topic.to_string(),
            data,
        );
        (msg, write)
    }

    /// Returns writes for ALL 64 partitions since topic index must be available
    /// on every node for correct publish routing.
    #[must_use]
    pub fn subscribe_topic_replicated(
        &self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> (TopicIndexEntry, Vec<ReplicationWrite>) {
        let (entry, data) =
            self.topics
                .subscribe_with_data(topic, client_id, client_partition, qos);
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|partition| {
                ReplicationWrite::new(
                    partition,
                    Operation::Update,
                    Epoch::ZERO,
                    0,
                    entity::TOPIC_INDEX.to_string(),
                    topic.to_string(),
                    data.clone(),
                )
            })
            .collect();
        (entry, writes)
    }

    /// # Errors
    /// Returns `TopicIndexError::NotFound` if topic does not exist.
    ///
    /// Returns writes for ALL 64 partitions since topic index must be available
    /// on every node for correct publish routing.
    pub fn unsubscribe_topic_replicated(
        &self,
        topic: &str,
        client_id: &str,
    ) -> Result<(TopicIndexEntry, Vec<ReplicationWrite>), TopicIndexError> {
        let (entry, data) = self.topics.unsubscribe_with_data(topic, client_id)?;
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|partition| {
                ReplicationWrite::new(
                    partition,
                    Operation::Update,
                    Epoch::ZERO,
                    0,
                    entity::TOPIC_INDEX.to_string(),
                    topic.to_string(),
                    data.clone(),
                )
            })
            .collect();
        Ok((entry, writes))
    }

    /// # Errors
    /// Returns `WildcardStoreError::InvalidPattern` or `NotWildcard` for invalid patterns.
    ///
    /// Returns writes for ALL 64 partitions since wildcards must be broadcast
    /// to every partition for correct publish routing.
    pub fn subscribe_wildcard_replicated(
        &self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> Result<(WildcardEntry, Vec<ReplicationWrite>), WildcardStoreError> {
        let (entry, data) = self.wildcards.subscribe_with_data(
            pattern,
            client_id,
            client_partition,
            qos,
            subscription_type,
        )?;
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|partition| {
                ReplicationWrite::new(
                    partition,
                    Operation::Insert,
                    Epoch::ZERO,
                    0,
                    entity::WILDCARDS.to_string(),
                    format!("{pattern}:{client_id}"),
                    data.clone(),
                )
            })
            .collect();
        Ok((entry, writes))
    }

    /// # Errors
    /// Returns `WildcardStoreError::InvalidPattern` or `NotFound` for invalid/missing patterns.
    ///
    /// Returns writes for ALL 64 partitions since wildcards must be broadcast.
    pub fn unsubscribe_wildcard_replicated(
        &self,
        pattern: &str,
        client_id: &str,
    ) -> Result<Vec<ReplicationWrite>, WildcardStoreError> {
        let _ = self.wildcards.unsubscribe_with_data(pattern, client_id)?;
        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|partition| {
                ReplicationWrite::new(
                    partition,
                    Operation::Delete,
                    Epoch::ZERO,
                    0,
                    entity::WILDCARDS.to_string(),
                    format!("{pattern}:{client_id}"),
                    Vec::new(),
                )
            })
            .collect();
        Ok(writes)
    }

    /// # Errors
    /// Returns `InflightStoreError::AlreadyExists` if message already exists.
    pub fn add_inflight_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        qos: u8,
        timestamp: u64,
    ) -> Result<(InflightMessage, ReplicationWrite), InflightStoreError> {
        let (msg, data) = self
            .inflight
            .add_with_data(client_id, packet_id, topic, payload, qos, timestamp)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::INFLIGHT.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((msg, write))
    }

    /// # Errors
    /// Returns `InflightStoreError::NotFound` if message does not exist.
    pub fn acknowledge_inflight_replicated(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<(InflightMessage, ReplicationWrite), InflightStoreError> {
        let (msg, data) = self.inflight.acknowledge_with_data(client_id, packet_id)?;
        let partition = session_partition(client_id);
        let write = ReplicationWrite::new(
            partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::INFLIGHT.to_string(),
            format!("{client_id}:{packet_id}"),
            data,
        );
        Ok((msg, write))
    }

    #[must_use]
    pub fn commit_offset_replicated(
        &self,
        consumer_id: &str,
        partition: PartitionId,
        sequence: u64,
        timestamp: u64,
    ) -> (ConsumerOffset, ReplicationWrite) {
        let (offset, data) =
            self.offsets
                .commit_with_data(consumer_id, partition, sequence, timestamp);
        let consumer_partition = session_partition(consumer_id);
        let write = ReplicationWrite::new(
            consumer_partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::OFFSETS.to_string(),
            format!("{consumer_id}:{}", partition.get()),
            data,
        );
        (offset, write)
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_partition(&self, partition: PartitionId) -> Vec<u8> {
        let mut buf = Vec::new();

        let store_data = [
            (
                entity::SESSIONS,
                self.sessions.export_for_partition(partition),
            ),
            (entity::QOS2, self.qos2.export_for_partition(partition)),
            (
                entity::SUBSCRIPTIONS,
                self.subscriptions.export_for_partition(partition),
            ),
            (
                entity::RETAINED,
                self.retained.export_for_partition(partition),
            ),
            (
                entity::TOPIC_INDEX,
                self.topics.export_for_partition(partition),
            ),
            (
                entity::WILDCARDS,
                self.wildcards.export_for_partition(partition),
            ),
            (
                entity::INFLIGHT,
                self.inflight.export_for_partition(partition),
            ),
            (
                entity::OFFSETS,
                self.offsets.export_for_partition(partition),
            ),
            (
                entity::IDEMPOTENCY,
                self.idempotency.export_for_partition(partition),
            ),
        ];

        buf.extend_from_slice(&(store_data.len() as u8).to_be_bytes());

        for (entity_name, data) in store_data {
            let name_bytes = entity_name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Errors
    /// Returns an error if UTF-8 parsing or store import fails.
    pub fn import_partition(&self, data: &[u8]) -> Result<usize, StoreApplyError> {
        if data.is_empty() {
            return Ok(0);
        }

        let store_count = data[0] as usize;
        let mut offset = 1;
        let mut total_imported = 0;

        for _ in 0..store_count {
            if offset >= data.len() {
                break;
            }

            let name_len = data[offset] as usize;
            offset += 1;

            if offset + name_len > data.len() {
                break;
            }
            let entity_name = std::str::from_utf8(&data[offset..offset + name_len])
                .map_err(|_| StoreApplyError::UnknownEntity)?;
            offset += name_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let store_data = &data[offset..offset + data_len];
            offset += data_len;

            let imported = match entity_name {
                entity::SESSIONS => self
                    .sessions
                    .import_sessions(store_data)
                    .map_err(|_| StoreApplyError::SessionError)?,
                entity::QOS2 => self
                    .qos2
                    .import_states(store_data)
                    .map_err(|_| StoreApplyError::Qos2Error)?,
                entity::SUBSCRIPTIONS => self
                    .subscriptions
                    .import_subscriptions(store_data)
                    .map_err(|_| StoreApplyError::SubscriptionError)?,
                entity::RETAINED => self
                    .retained
                    .import_retained(store_data)
                    .map_err(|_| StoreApplyError::RetainedError)?,
                entity::TOPIC_INDEX => self
                    .topics
                    .import_entries(store_data)
                    .map_err(|_| StoreApplyError::TopicIndexError)?,
                entity::WILDCARDS => self
                    .wildcards
                    .import_wildcards(store_data)
                    .map_err(|_| StoreApplyError::WildcardError)?,
                entity::INFLIGHT => self
                    .inflight
                    .import_inflight(store_data)
                    .map_err(|_| StoreApplyError::InflightError)?,
                entity::OFFSETS => self
                    .offsets
                    .import_offsets(store_data)
                    .map_err(|_| StoreApplyError::OffsetError)?,
                entity::IDEMPOTENCY => self
                    .idempotency
                    .import_records(store_data)
                    .map_err(|_| StoreApplyError::IdempotencyError)?,
                _ => continue,
            };

            total_imported += imported;
        }

        Ok(total_imported)
    }

    /// # Panics
    /// Panics if any internal store lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut total_cleared = 0;
        total_cleared += self.sessions.clear_partition(partition);
        total_cleared += self.qos2.clear_partition(partition);
        total_cleared += self.subscriptions.clear_partition(partition);
        total_cleared += self.retained.clear_partition(partition);
        total_cleared += self.topics.clear_partition(partition);
        total_cleared += self.wildcards.clear_partition(partition);
        total_cleared += self.inflight.clear_partition(partition);
        total_cleared += self.offsets.clear_partition(partition);
        total_cleared += self.idempotency.clear_partition(partition);
        total_cleared
    }

    /// # Errors
    /// Returns an error if the entity type is unknown.
    #[allow(clippy::type_complexity)]
    pub fn query_entity(
        &self,
        entity: &str,
        filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> Result<(Vec<u8>, bool, Option<Vec<u8>>), StoreApplyError> {
        match entity {
            entity::SESSIONS => {
                let (sessions, has_more, next_cursor) = self.sessions.query(filter, limit, cursor);
                let data = sessions.into_iter().flat_map(|s| s.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::RETAINED => {
                let (messages, has_more, next_cursor) = self.retained.query(filter, limit, cursor);
                let data = messages.into_iter().flat_map(|m| m.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::SUBSCRIPTIONS => {
                let (subs, has_more, next_cursor) = self.subscriptions.query(filter, limit, cursor);
                let data = subs.into_iter().flat_map(|s| s.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::TOPIC_INDEX => {
                let (entries, has_more, next_cursor) = self.topics.query(filter, limit, cursor);
                let data = entries.into_iter().flat_map(|e| e.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::WILDCARDS => {
                let (entries, has_more, next_cursor) = self.wildcards.query(filter, limit, cursor);
                let data = entries.into_iter().flat_map(|e| e.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::INFLIGHT => {
                let (messages, has_more, next_cursor) = self.inflight.query(filter, limit, cursor);
                let data = messages.into_iter().flat_map(|m| m.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::OFFSETS => {
                let (offsets, has_more, next_cursor) = self.offsets.query(filter, limit, cursor);
                let data = offsets.into_iter().flat_map(|o| o.to_be_bytes()).collect();
                Ok((data, has_more, next_cursor))
            }
            entity::DB_DATA => {
                let entity_type = Self::extract_db_entity_type(filter);
                let (entities, has_more, next_cursor) =
                    self.db_data.query(entity_type, filter, limit, cursor);
                let data = entities
                    .into_iter()
                    .flat_map(|e| DbDataStore::serialize(&e))
                    .collect();
                Ok((data, has_more, next_cursor))
            }
            _ if entity.starts_with("$DB/") => {
                let entity_type = entity.strip_prefix("$DB/");
                let (entities, has_more, next_cursor) =
                    self.db_data.query(entity_type, filter, limit, cursor);
                let data = entities
                    .into_iter()
                    .flat_map(|e| DbDataStore::serialize(&e))
                    .collect();
                Ok((data, has_more, next_cursor))
            }
            _ => Err(StoreApplyError::UnknownEntity),
        }
    }

    fn extract_db_entity_type(filter: Option<&str>) -> Option<&str> {
        filter.and_then(|f| {
            f.strip_prefix("entity_type=")
                .map(|v| v.trim().trim_matches('"').trim_matches('\''))
        })
    }

    #[must_use]
    pub fn get_entity(&self, entity: &str, id: &str) -> Option<Vec<u8>> {
        match entity {
            entity::SESSIONS => self.sessions.get(id).map(|s| s.to_be_bytes()),
            entity::RETAINED => self.retained.get(id).map(|m| m.to_be_bytes()),
            entity::SUBSCRIPTIONS => self.subscriptions.get_snapshot(id).map(|s| s.to_be_bytes()),
            entity::TOPIC_INDEX => self.topics.get_entry(id).map(|e| e.to_be_bytes()),
            entity::DB_DATA => {
                let parts: Vec<&str> = id.splitn(2, '/').collect();
                if parts.len() == 2 {
                    self.db_data
                        .get(parts[0], parts[1])
                        .map(|e| DbDataStore::serialize(&e))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `IdempotencyError` if the key exists and is processing or has mismatched parameters.
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn rollback_idempotency(&self, partition: PartitionId, idempotency_key: &str) {
        self.idempotency
            .remove_processing(partition, idempotency_key);
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired_idempotency(&self, now: u64) -> usize {
        self.idempotency.cleanup_expired(now)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired_sessions(&self, now: u64) -> Vec<SessionData> {
        self.sessions.cleanup_expired_sessions(now)
    }

    #[must_use]
    pub fn expired_sessions(&self, now: u64) -> Vec<SessionData> {
        self.sessions.expired_sessions(now)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_stale_offsets(&self, now: u64) -> usize {
        self.offsets.cleanup_stale(now)
    }
}

impl std::fmt::Debug for StoreManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreManager")
            .field("sessions", &self.sessions.session_count())
            .field("qos2", &self.qos2.count())
            .field("subscriptions", &self.subscriptions.client_count())
            .field("retained", &self.retained.message_count())
            .field("topics", &self.topics.topic_count())
            .field("wildcards", &self.wildcards.pattern_count())
            .field("wildcard_pending", &self.wildcard_pending.count())
            .field("inflight", &self.inflight.count())
            .field("offsets", &self.offsets.count())
            .field("idempotency", &self.idempotency.count())
            .field("db_data", &self.db_data.count())
            .field("db_schema", &self.db_schema.count())
            .field("db_index", &self.db_index.count())
            .field("db_unique", &self.db_unique.count())
            .field("db_fk", &self.db_fk.count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Epoch, PartitionId, protocol::Operation};

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition() -> PartitionId {
        PartitionId::new(0).unwrap()
    }

    #[test]
    fn apply_session_insert() {
        let manager = StoreManager::new(node(1));

        let session = crate::cluster::SessionData::create("client1", node(1));
        let data = crate::cluster::SessionStore::serialize(&session);

        let write = ReplicationWrite::new(
            partition(),
            Operation::Insert,
            Epoch::new(1),
            1,
            entity::SESSIONS.to_string(),
            "client1".to_string(),
            data,
        );

        manager.apply_write(&write).unwrap();
        assert!(manager.sessions.get("client1").is_some());
    }

    #[test]
    fn apply_session_delete() {
        let manager = StoreManager::new(node(1));
        manager.sessions.create_session("client1").unwrap();

        let session = crate::cluster::SessionData::create("client1", node(1));
        let data = crate::cluster::SessionStore::serialize(&session);

        let write = ReplicationWrite::new(
            partition(),
            Operation::Delete,
            Epoch::new(1),
            1,
            entity::SESSIONS.to_string(),
            "client1".to_string(),
            data,
        );

        manager.apply_write(&write).unwrap();
        assert!(manager.sessions.get("client1").is_none());
    }

    #[test]
    fn apply_unknown_entity_fails() {
        let manager = StoreManager::new(node(1));

        let write = ReplicationWrite::new(
            partition(),
            Operation::Insert,
            Epoch::new(1),
            1,
            "_unknown".to_string(),
            "id".to_string(),
            vec![],
        );

        let result = manager.apply_write(&write);
        assert!(matches!(result, Err(StoreApplyError::UnknownEntity)));
    }
}
