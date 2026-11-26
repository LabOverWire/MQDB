// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::idempotency_store::{IdempotencyCheck, IdempotencyError};
use super::super::offset_store::ConsumerOffset;
use super::super::protocol::{ChangeEventRecord, ReplicationWrite};
use super::super::quorum::{QuorumResult, QuorumTracker};
use super::super::replication::ReplicaRole;
use super::super::session::session_partition;
use super::super::store_manager::StoreApplyError;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionId};
use super::{NodeController, RefusedWrite, ReplicationError};

struct StoredSession {
    session: super::SessionData,
    write: ReplicationWrite,
    replicas: Vec<NodeId>,
}

impl<T: ClusterTransport> NodeController<T> {
    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the session partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    /// Returns `StoreFailed` if this node cannot store the session.
    /// Returns `QuorumFailed` if a session already exists for this client ID.
    pub async fn create_session_replicated(
        &mut self,
        client_id: &str,
    ) -> Result<super::SessionData, ReplicationError> {
        let stored = self.store_new_session(client_id)?;

        if !stored.replicas.is_empty() {
            let required_acks = stored.replicas.len();
            self.pending
                .add(QuorumTracker::new(
                    stored.write.partition,
                    stored.write.sequence,
                    stored.write.epoch,
                    &stored.replicas,
                    required_acks,
                ))
                .map_err(|_| ReplicationError::TooManyPending)?;
            self.send_to_replicas(&stored.write, &stored.replicas).await;
        }

        Ok(stored.session)
    }

    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the session partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    /// Returns `StoreFailed` if this node cannot store the session.
    /// Returns `QuorumFailed` if a session already exists for this client ID.
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
        let stored = self.store_new_session(client_id)?;

        if stored.replicas.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QuorumResult::Success);
            return Ok((stored.session, rx));
        }

        let required_acks = stored.replicas.len();
        let (tracker, rx) = QuorumTracker::with_completion(
            stored.write.partition,
            stored.write.sequence,
            stored.write.epoch,
            &stored.replicas,
            required_acks,
        );
        self.pending
            .add(tracker)
            .map_err(|_| ReplicationError::TooManyPending)?;
        self.send_to_replicas(&stored.write, &stored.replicas).await;

        Ok((stored.session, rx))
    }

    fn store_new_session(&mut self, client_id: &str) -> Result<StoredSession, ReplicationError> {
        let partition = session_partition(client_id);

        let state = self
            .replicas
            .get(&partition.get())
            .filter(|state| state.role() == ReplicaRole::Primary)
            .ok_or(ReplicationError::NotPrimary)?;
        let sequence = state.sequence() + 1;
        let epoch = state.epoch();

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        if !replicas.is_empty() && self.pending.is_full() {
            return Err(ReplicationError::TooManyPending);
        }

        let (session, write_data) = self
            .stores
            .create_session_replicated(client_id)
            .map_err(|_| ReplicationError::QuorumFailed)?;

        let write = ReplicationWrite::new(
            partition,
            write_data.operation,
            epoch,
            sequence,
            write_data.entity,
            write_data.id,
            write_data.data,
        );

        if let Err(error) = self.apply_primary_write(&write, None, &[]) {
            let removed = self.stores.sessions.remove(client_id);
            tracing::warn!(
                client_id,
                removed = removed.is_some(),
                %error,
                "session not created"
            );
            return Err(error);
        }

        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.advance_sequence();
        }
        self.write_log.append(partition, sequence, write.clone());

        Ok(StoredSession {
            session,
            write,
            replicas,
        })
    }

    async fn send_to_replicas(&self, write: &ReplicationWrite, replicas: &[NodeId]) {
        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write.clone()))
                .await;
        }
    }

    pub(super) fn apply_primary_write(
        &self,
        write_msg: &ReplicationWrite,
        change_event: Option<&super::change_events::WriteChangeEvent>,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ReplicationError> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(extra_outbox.len() + 1);
        if let Some(event) = change_event {
            entries.push(event.storage_entry.clone());
        }
        entries.extend_from_slice(extra_outbox);

        let applied = if entries.is_empty() {
            self.stores.apply_write(write_msg)
        } else {
            self.stores
                .apply_write_with_outbox_entries(write_msg, &entries)
        };
        match applied {
            Ok(()) => Ok(()),
            Err(StoreApplyError::PersistenceError) => {
                tracing::error!(
                    partition = ?write_msg.partition,
                    entity = %write_msg.entity,
                    "failed to persist primary write"
                );
                Err(ReplicationError::StoreFailed)
            }
            Err(e) => {
                tracing::warn!(
                    partition = ?write_msg.partition,
                    entity = %write_msg.entity,
                    ?e,
                    "primary write persisted but not applied in memory"
                );
                Ok(())
            }
        }
    }

    pub(crate) async fn replicate_primary_write(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
        event: Option<ChangeEventRecord>,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<u64, RefusedWrite> {
        let start = std::time::Instant::now();
        let partition = write.partition;

        let Some(state) = self
            .replicas
            .get(&partition.get())
            .filter(|state| state.role() == ReplicaRole::Primary)
        else {
            return Err(RefusedWrite {
                error: ReplicationError::NotPrimary,
                event,
            });
        };

        let sequence = state.sequence() + 1;
        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            state.epoch(),
            sequence,
            write.entity,
            write.id,
            write.data,
        )
        .with_event(event);
        let t_state = super::elapsed_micros(start);

        let change_event = Self::change_event_entry(&write_msg);
        if let Err(error) =
            self.apply_primary_write(&write_msg, change_event.as_ref(), extra_outbox)
        {
            return Err(RefusedWrite {
                error,
                event: write_msg.event,
            });
        }
        let t_apply = super::elapsed_micros(start);

        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.advance_sequence();
        }
        self.write_log
            .append(partition, sequence, write_msg.clone());
        let t_writelog = super::elapsed_micros(start);

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }
        let t_send = super::elapsed_micros(start);

        tracing::trace!(
            node = self.node_id.get(),
            t_state,
            t_apply,
            t_writelog,
            t_send,
            replica_count = replicas.len(),
            "replicate_primary_write_timing"
        );

        if let Some(event) = change_event {
            self.take_on_change_event(partition, event.id, event.record)
                .await;
        }

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
