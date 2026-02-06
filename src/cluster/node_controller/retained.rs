use super::super::PartitionId;
use super::super::entity;
use super::super::protocol::{Operation, QueryRequest, QueryResponse, ReplicationWrite};
use super::super::query_coordinator::QueryCoordinator;
use super::super::retained_store::RetainedMessage;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::NodeController;
use bebytes::BeBytes;
use std::time::Instant;
use tokio::sync::oneshot;

impl<T: ClusterTransport> NodeController<T> {
    #[must_use]
    pub fn query_local_retained_exact(&self, topic: &str) -> Option<RetainedMessage> {
        self.stores.retained.get(topic)
    }

    #[must_use]
    pub fn query_local_retained_pattern(&self, pattern: &str) -> Vec<RetainedMessage> {
        self.stores.retained.query_matching_pattern(pattern)
    }

    pub fn start_retained_query(
        &mut self,
        topic_filter: &str,
        timeout_ms: u32,
        now: u64,
    ) -> (u64, Vec<(PartitionId, QueryRequest)>) {
        let is_wildcard = topic_filter.contains('+') || topic_filter.contains('#');

        let partitions = if is_wildcard {
            QueryCoordinator::all_partitions()
        } else {
            vec![super::topic_partition(topic_filter)]
        };

        let filter = if is_wildcard {
            None
        } else {
            Some(format!("topic={topic_filter}"))
        };

        let (query_id, requests) = self.query_coordinator.start_query(
            "retained",
            filter.as_deref(),
            1000,
            None,
            Some(timeout_ms),
            partitions.clone(),
            now,
        );

        let requests_with_partitions: Vec<_> = partitions.into_iter().zip(requests).collect();

        (query_id, requests_with_partitions)
    }

    pub async fn send_retained_query(&self, partition: PartitionId, request: QueryRequest) {
        if let Some(primary) = self.partition_map.primary(partition) {
            if primary == self.node_id {
                return;
            }
            let msg = ClusterMessage::QueryRequest { partition, request };
            let _ = self.transport.send(primary, msg).await;
        }
    }

    #[must_use]
    pub fn is_partition_primary(&self, partition: PartitionId) -> bool {
        self.partition_map.primary(partition) == Some(self.node_id)
    }

    pub async fn start_async_retained_query(
        &mut self,
        topic: &str,
    ) -> Option<oneshot::Receiver<Vec<RetainedMessage>>> {
        let partition = super::topic_partition(topic);
        let primary = self.partition_map.primary(partition)?;

        if primary == self.node_id {
            return None;
        }

        let query_id = self.current_time;
        let request = QueryRequest::new(
            query_id,
            5000,
            entity::RETAINED.to_string(),
            Some(format!("topic={topic}")),
            10,
            None,
        );

        let (tx, rx) = oneshot::channel();
        self.pending_retained_queries.insert(query_id, tx);

        let msg = ClusterMessage::QueryRequest { partition, request };
        if self.transport.send(primary, msg).await.is_err() {
            self.pending_retained_queries.remove(&query_id);
            return None;
        }

        tracing::debug!(
            topic,
            ?partition,
            ?primary,
            query_id,
            "started async retained query"
        );

        Some(rx)
    }

    pub fn complete_retained_query(&mut self, query_id: u64, results: Vec<RetainedMessage>) {
        if let Some(sender) = self.pending_retained_queries.remove(&query_id) {
            let _ = sender.send(results);
        }
    }

    pub(crate) fn parse_retained_query_response(response: &QueryResponse) -> Vec<RetainedMessage> {
        if !response.status.is_ok() || response.results.is_empty() {
            return Vec::new();
        }

        let mut messages = Vec::new();
        let mut offset = 0;
        while offset < response.results.len() {
            match RetainedMessage::try_from_be_bytes(&response.results[offset..]) {
                Ok((msg, consumed)) => {
                    messages.push(msg);
                    offset += consumed;
                }
                Err(_) => break,
            }
        }
        messages
    }

    pub async fn sync_retained_to_broker(&self, write: &ReplicationWrite) {
        if write.entity != entity::RETAINED {
            return;
        }

        let topic = write.id.clone();

        if let Some(ref synced_topics) = self.synced_retained_topics {
            let synced = synced_topics.read().await;
            if let Some(&insert_time) = synced.get(&topic)
                && insert_time.elapsed() < std::time::Duration::from_secs(5)
            {
                tracing::trace!(topic, "skipping retained sync within TTL window");
                return;
            }
        }

        if write.operation == Operation::Delete {
            tracing::debug!(topic, "clearing retained message from local broker");
            self.transport
                .queue_local_publish_retained(topic, Vec::new(), 0)
                .await;
            return;
        }

        if let Ok((msg, _)) = RetainedMessage::try_from_be_bytes(&write.data) {
            let payload = msg.payload.clone();
            let qos = msg.qos;

            if let Some(ref synced_topics) = self.synced_retained_topics {
                synced_topics
                    .write()
                    .await
                    .insert(topic.clone(), Instant::now());
            }

            tracing::debug!(
                topic,
                qos,
                payload_len = payload.len(),
                "syncing retained message to local broker"
            );
            self.transport
                .queue_local_publish_retained(topic, payload, qos)
                .await;
        }
    }
}
