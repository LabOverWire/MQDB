use super::super::idempotency_store::{IdempotencyCheck, IdempotencyError};
use super::super::offset_store::ConsumerOffset;
use super::super::protocol::ReplicationWrite;
use super::super::quorum::{QuorumResult, QuorumTracker};
use super::super::replication::ReplicaRole;
use super::super::session::session_partition;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionId};
use super::{NodeController, ReplicationError};

impl<T: ClusterTransport> NodeController<T> {
    /// # Errors
    /// Returns an error if this node is not the primary for the session partition or if replication fails.
    pub async fn create_session_replicated(
        &mut self,
        client_id: &str,
    ) -> Result<super::SessionData, ReplicationError> {
        let partition = session_partition(client_id);

        let state = self
            .replicas
            .get(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let (session, write) = self
            .stores
            .create_session_replicated(client_id)
            .map_err(|_| ReplicationError::QuorumFailed)?;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        let required_acks = replicas.len();

        if !replicas.is_empty() {
            self.replicate_write(write, &replicas, required_acks)
                .await?;
        }

        Ok(session)
    }

    /// # Errors
    /// Returns an error if this node is not the primary for the session partition or if there are too many pending writes.
    #[cfg(feature = "cluster")]
    pub async fn create_session_quorum(
        &mut self,
        client_id: &str,
    ) -> Result<
        (
            super::SessionData,
            tokio::sync::oneshot::Receiver<QuorumResult>,
        ),
        ReplicationError,
    > {
        let partition = session_partition(client_id);

        let state = self
            .replicas
            .get_mut(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let sequence = state.advance_sequence();
        let epoch = state.epoch();

        let (session, write_data) = self
            .stores
            .create_session_replicated(client_id)
            .map_err(|_| ReplicationError::QuorumFailed)?;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        let required_acks = replicas.len();

        if replicas.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QuorumResult::Success);
            return Ok((session, rx));
        }

        let write_msg = ReplicationWrite::new(
            partition,
            write_data.operation,
            epoch,
            sequence,
            write_data.entity,
            write_data.id,
            write_data.data,
        );

        let (tracker, rx) =
            QuorumTracker::with_completion(sequence, epoch, &replicas, required_acks);

        if !self.pending.add(tracker) {
            return Err(ReplicationError::TooManyPending);
        }

        self.write_log
            .append(partition, sequence, write_msg.clone());

        for &replica in &replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }

        Ok((session, rx))
    }

    /// # Errors
    /// Returns an error if this node is not the primary for the partition.
    pub async fn replicate_write_async(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
    ) -> Result<u64, ReplicationError> {
        let start = std::time::Instant::now();
        let partition = write.partition;

        let state = self
            .replicas
            .get_mut(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let sequence = state.advance_sequence();
        let t_state = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            state.epoch(),
            sequence,
            write.entity,
            write.id,
            write.data,
        );

        self.write_log
            .append(partition, sequence, write_msg.clone());
        let t_writelog = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        let _ = self.stores.apply_write(&write_msg);
        let t_apply = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }
        let t_send = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);

        tracing::trace!(
            node = self.node_id.get(),
            t_state,
            t_writelog,
            t_apply,
            t_send,
            replica_count = replicas.len(),
            "replicate_write_async_timing"
        );

        Ok(sequence)
    }

    pub async fn commit_offset(
        &mut self,
        consumer_id: &str,
        partition: PartitionId,
        sequence: u64,
        timestamp: u64,
    ) -> ConsumerOffset {
        let (offset, write) =
            self.stores
                .commit_offset_replicated(consumer_id, partition, sequence, timestamp);
        self.write_or_forward(write).await;
        offset
    }

    #[must_use]
    pub fn get_offset(&self, consumer_id: &str, partition: PartitionId) -> Option<u64> {
        self.stores.offsets.get_sequence(consumer_id, partition)
    }

    /// # Errors
    /// Returns an error if the idempotency check fails due to key expiry or other idempotency violations.
    pub fn check_idempotency(
        &self,
        idempotency_key: &str,
        partition: PartitionId,
        entity: &str,
        id: &str,
        timestamp: u64,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        let epoch = self
            .replicas
            .get(&partition.get())
            .map_or(Epoch::ZERO, super::replication::ReplicaState::epoch);
        self.stores
            .check_idempotency(idempotency_key, partition, epoch, entity, id, timestamp)
    }

    pub async fn commit_idempotency(
        &mut self,
        partition: PartitionId,
        idempotency_key: &str,
        response: &[u8],
    ) {
        let write = self
            .stores
            .commit_idempotency(partition, idempotency_key, response);
        self.write_or_forward(write).await;
    }

    pub fn rollback_idempotency(&self, partition: PartitionId, idempotency_key: &str) {
        self.stores.rollback_idempotency(partition, idempotency_key);
    }
}
