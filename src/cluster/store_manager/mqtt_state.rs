use super::StoreManager;
use crate::cluster::inflight_store::{InflightMessage, InflightStoreError};
use crate::cluster::offset_store::ConsumerOffset;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::qos2_store::{Qos2Phase, Qos2State, Qos2StoreError};
use crate::cluster::retained_store::RetainedMessage;
use crate::cluster::session::session_partition;
use crate::cluster::subscription_cache::{MqttSubscriptionSnapshot, SubscriptionCacheError};
use crate::cluster::topic_index::{TopicIndexEntry, TopicIndexError, topic_partition};
use crate::cluster::wildcard_store::{WildcardEntry, WildcardStoreError};
use crate::cluster::{Epoch, PartitionId, SubscriptionType, entity};

impl StoreManager {
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
    pub fn clear_qos2_client_replicated(&self, client_id: &str) -> Vec<ReplicationWrite> {
        let removed = self.qos2.clear_client_with_data(client_id);
        let partition = session_partition(client_id);
        removed
            .into_iter()
            .map(|(packet_id, data)| {
                ReplicationWrite::new(
                    partition,
                    Operation::Delete,
                    Epoch::ZERO,
                    0,
                    entity::QOS2.to_string(),
                    format!("{client_id}:{packet_id}"),
                    data,
                )
            })
            .collect()
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

        self.persist_broadcast_batch(&writes, "topic_subscribe");

        (entry, writes)
    }

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

        self.persist_broadcast_batch(&writes, "topic_unsubscribe");

        Ok((entry, writes))
    }

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

        self.persist_broadcast_batch(&writes, "wildcard_subscribe");

        Ok((entry, writes))
    }

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

        self.persist_broadcast_batch(&writes, "wildcard_unsubscribe");

        Ok(writes)
    }

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
    pub fn clear_inflight_client_replicated(&self, client_id: &str) -> Vec<ReplicationWrite> {
        let removed = self.inflight.clear_client_with_data(client_id);
        let partition = session_partition(client_id);
        removed
            .into_iter()
            .map(|(packet_id, data)| {
                ReplicationWrite::new(
                    partition,
                    Operation::Delete,
                    Epoch::ZERO,
                    0,
                    entity::INFLIGHT.to_string(),
                    format!("{client_id}:{packet_id}"),
                    data,
                )
            })
            .collect()
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
