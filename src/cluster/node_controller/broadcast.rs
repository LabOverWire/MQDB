use super::super::entity;
use super::super::protocol::{
    Operation, ReplicationWrite, TopicSubscriptionBroadcast, WildcardBroadcast, WildcardOp,
};
use super::super::topic_trie::SubscriptionType as TopicTrieSubscriptionType;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionId, SubscriptionType, WildcardStoreError};
use super::NodeController;

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) fn handle_wildcard_broadcast(
        &mut self,
        from: NodeId,
        broadcast: &WildcardBroadcast,
    ) {
        let pattern = broadcast.pattern_str();
        let client_id = broadcast.client_id_str();

        match broadcast.operation() {
            Some(WildcardOp::Subscribe) => {
                let subscription_type = if broadcast.subscription_type() == 1 {
                    TopicTrieSubscriptionType::Db
                } else {
                    TopicTrieSubscriptionType::Mqtt
                };
                let result = self.stores.wildcards.subscribe(
                    pattern,
                    client_id,
                    broadcast.qos(),
                    subscription_type,
                );
                match result {
                    Ok(()) => {
                        tracing::debug!(
                            pattern,
                            client_id,
                            from = from.get(),
                            "applied wildcard subscription from broadcast"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            pattern,
                            client_id,
                            ?e,
                            "failed to apply wildcard subscription from broadcast"
                        );
                    }
                }
            }
            Some(WildcardOp::Unsubscribe) => {
                let _ = self.stores.wildcards.unsubscribe(pattern, client_id);
                tracing::debug!(
                    pattern,
                    client_id,
                    from = from.get(),
                    "applied wildcard unsubscription from broadcast"
                );
            }
            None => {
                tracing::warn!("unknown wildcard broadcast operation");
            }
        }
    }

    pub(crate) fn handle_topic_subscription_broadcast(
        &mut self,
        from: NodeId,
        broadcast: &TopicSubscriptionBroadcast,
    ) {
        let topic = broadcast.topic_str();
        let client_id = broadcast.client_id_str();

        match broadcast.operation() {
            Some(WildcardOp::Subscribe) => {
                if let Some(client_partition) = broadcast.client_partition() {
                    let result = self.stores.topics.subscribe(
                        topic,
                        client_id,
                        client_partition,
                        broadcast.qos(),
                    );
                    match result {
                        Ok(()) => {
                            tracing::debug!(
                                topic,
                                client_id,
                                from = from.get(),
                                "applied topic subscription from broadcast"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic,
                                client_id,
                                ?e,
                                "failed to apply topic subscription from broadcast"
                            );
                        }
                    }
                }
            }
            Some(WildcardOp::Unsubscribe) => {
                let _ = self.stores.topics.unsubscribe(topic, client_id);
                tracing::debug!(
                    topic,
                    client_id,
                    from = from.get(),
                    "applied topic unsubscription from broadcast"
                );
            }
            None => {
                tracing::warn!("unknown topic subscription broadcast operation");
            }
        }
    }

    /// # Errors
    /// Returns error if the wildcard store operation fails.
    pub fn subscribe_wildcard_broadcast(
        &mut self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> Result<Vec<ReplicationWrite>, WildcardStoreError> {
        let (entry, data) = self.stores.wildcards.subscribe_with_data(
            pattern,
            client_id,
            client_partition,
            qos,
            SubscriptionType::Mqtt,
        )?;

        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|p| {
                ReplicationWrite::new(
                    p,
                    Operation::Insert,
                    Epoch::ZERO,
                    0,
                    entity::WILDCARDS.to_string(),
                    format!("{}:{}", entry.pattern_str(), entry.client_id_str()),
                    data.clone(),
                )
            })
            .collect();

        Ok(writes)
    }

    pub async fn broadcast_wildcard_writes(&mut self, writes: Vec<ReplicationWrite>) {
        for write in writes {
            let partition = write.partition;
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();

            for &replica in &replicas {
                let _ = self
                    .transport
                    .send(replica, ClusterMessage::Write(write.clone()))
                    .await;
            }
        }
    }
}
