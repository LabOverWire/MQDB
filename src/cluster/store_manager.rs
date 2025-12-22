use super::SubscriptionType;
use super::entity;
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
    pub inflight: InflightStore,
    pub offsets: OffsetStore,
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
            inflight: InflightStore::new(node_id),
            offsets: OffsetStore::new(node_id),
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

    #[must_use]
    pub fn subscribe_topic_replicated(
        &self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> (TopicIndexEntry, ReplicationWrite) {
        let (entry, data) =
            self.topics
                .subscribe_with_data(topic, client_id, client_partition, qos);
        let partition = topic_partition(topic);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::TOPIC_INDEX.to_string(),
            topic.to_string(),
            data,
        );
        (entry, write)
    }

    /// # Errors
    /// Returns `TopicIndexError::NotFound` if topic does not exist.
    pub fn unsubscribe_topic_replicated(
        &self,
        topic: &str,
        client_id: &str,
    ) -> Result<(TopicIndexEntry, ReplicationWrite), TopicIndexError> {
        let (entry, data) = self.topics.unsubscribe_with_data(topic, client_id)?;
        let partition = topic_partition(topic);
        let write = ReplicationWrite::new(
            partition,
            Operation::Update,
            Epoch::ZERO,
            0,
            entity::TOPIC_INDEX.to_string(),
            topic.to_string(),
            data,
        );
        Ok((entry, write))
    }

    /// # Errors
    /// Returns `WildcardStoreError::InvalidPattern` or `NotWildcard` for invalid patterns.
    pub fn subscribe_wildcard_replicated(
        &self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> Result<(WildcardEntry, ReplicationWrite), WildcardStoreError> {
        let (entry, data) = self.wildcards.subscribe_with_data(
            pattern,
            client_id,
            client_partition,
            qos,
            subscription_type,
        )?;
        let partition = client_partition;
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::ZERO,
            0,
            entity::WILDCARDS.to_string(),
            format!("{pattern}:{client_id}"),
            data,
        );
        Ok((entry, write))
    }

    /// # Errors
    /// Returns `WildcardStoreError::InvalidPattern` or `NotFound` for invalid/missing patterns.
    pub fn unsubscribe_wildcard_replicated(
        &self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
    ) -> Result<ReplicationWrite, WildcardStoreError> {
        let _ = self.wildcards.unsubscribe_with_data(pattern, client_id)?;
        let write = ReplicationWrite::new(
            client_partition,
            Operation::Delete,
            Epoch::ZERO,
            0,
            entity::WILDCARDS.to_string(),
            format!("{pattern}:{client_id}"),
            Vec::new(),
        );
        Ok(write)
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
            .field("inflight", &self.inflight.count())
            .field("offsets", &self.offsets.count())
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
