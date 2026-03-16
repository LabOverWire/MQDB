// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod apply;
mod cleanup;
mod constraint_ops;
mod db_ops;
mod fk_ops;
mod index_ops;
mod mqtt_state;
pub(crate) mod outbox;
mod partition_io;
mod query;
mod recovery;
mod schema_ops;
mod session_ops;
#[cfg(test)]
mod tests;
mod unique_ops;

use super::NodeId;
use super::client_location::ClientLocationStore;
use super::db::{
    ConstraintStore, DbDataStore, FkReverseIndex, FkValidationStore, IndexStore, SchemaStore,
    UniqueStore,
};
use super::idempotency_store::IdempotencyStore;
use super::inflight_store::InflightStore;
use super::offset_store::OffsetStore;
use super::partition_storage::PartitionStorage;
use super::qos2_store::Qos2Store;
use super::retained_store::RetainedStore;
use super::session::SessionStore;
use super::subscription_cache::SubscriptionCache;
use super::topic_index::TopicIndex;
use super::wildcard_pending::WildcardPendingStore;
use super::wildcard_store::WildcardStore;
use mqdb_core::storage::StorageBackend;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    pub sessions: usize,
    pub qos2: usize,
    pub subscriptions: usize,
    pub retained: usize,
    pub inflight: usize,
    pub offsets: usize,
    pub idempotency: usize,
    pub db_data: usize,
    pub db_schema: usize,
    pub db_index: usize,
    pub db_unique: usize,
    pub db_fk: usize,
    pub topic_index_rebuilt: usize,
    pub wildcards_rebuilt: usize,
}

#[derive(Debug, Clone, Copy)]
enum RecoveryField {
    Sessions,
    Qos2,
    Subscriptions,
    Retained,
    Inflight,
    Offsets,
    Idempotency,
    DbData,
    DbSchema,
    DbIndex,
    DbUnique,
    DbFk,
}

impl RecoveryStats {
    fn increment(&mut self, field: RecoveryField) {
        match field {
            RecoveryField::Sessions => self.sessions += 1,
            RecoveryField::Qos2 => self.qos2 += 1,
            RecoveryField::Subscriptions => self.subscriptions += 1,
            RecoveryField::Retained => self.retained += 1,
            RecoveryField::Inflight => self.inflight += 1,
            RecoveryField::Offsets => self.offsets += 1,
            RecoveryField::Idempotency => self.idempotency += 1,
            RecoveryField::DbData => self.db_data += 1,
            RecoveryField::DbSchema => self.db_schema += 1,
            RecoveryField::DbIndex => self.db_index += 1,
            RecoveryField::DbUnique => self.db_unique += 1,
            RecoveryField::DbFk => self.db_fk += 1,
        }
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.sessions
            + self.qos2
            + self.subscriptions
            + self.retained
            + self.inflight
            + self.offsets
            + self.idempotency
            + self.db_data
            + self.db_schema
            + self.db_index
            + self.db_unique
            + self.db_fk
    }
}

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
    DbConstraintError,
    ClientLocationError,
    PersistenceError,
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
            Self::DbConstraintError => write!(f, "db constraint store error"),
            Self::ClientLocationError => write!(f, "client location store error"),
            Self::PersistenceError => write!(f, "persistence error"),
        }
    }
}

impl std::error::Error for StoreApplyError {}

pub struct StoreManager {
    storage: Option<PartitionStorage>,
    cluster_outbox: Option<outbox::ClusterOutbox>,
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
    pub db_constraints: ConstraintStore,
    pub client_locations: ClientLocationStore,
    pub fk_reverse_index: FkReverseIndex,
}

impl StoreManager {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self::new_with_storage(node_id, None)
    }

    #[must_use]
    pub fn new_with_storage(node_id: NodeId, backend: Option<Arc<dyn StorageBackend>>) -> Self {
        Self {
            cluster_outbox: backend
                .as_ref()
                .map(|b| outbox::ClusterOutbox::new(Arc::clone(b))),
            storage: backend.map(PartitionStorage::new),
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
            db_constraints: ConstraintStore::new(node_id),
            client_locations: ClientLocationStore::new(),
            fk_reverse_index: FkReverseIndex::new(),
        }
    }

    #[must_use]
    pub fn has_persistence(&self) -> bool {
        self.storage.is_some()
    }

    #[must_use]
    pub fn cluster_outbox(&self) -> Option<&outbox::ClusterOutbox> {
        self.cluster_outbox.as_ref()
    }
}

impl std::fmt::Debug for StoreManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreManager")
            .field("persistence", &self.storage.is_some())
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
            .field("db_constraints", &self.db_constraints.count())
            .field("client_locations", &self.client_locations.count())
            .field("fk_reverse_index", &self.fk_reverse_index)
            .field("cluster_outbox", &self.cluster_outbox.is_some())
            .finish()
    }
}
