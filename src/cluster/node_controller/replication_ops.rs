// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::entity;
use super::super::protocol::{self, CatchupResponse, ReplicationAck, ReplicationWrite};
use super::super::quorum::{QuorumResult, QuorumTracker};
use super::super::replication::{ReplicaRole, ReplicaState};
use super::super::store_manager::StoreManager;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionId, PartitionMap};
use super::{
    NodeController, NodeStatus, ReplicationError, WRITE_COUNTER, WRITE_RECEIVED, WRITE_REQUEST_SENT,
};
use std::sync::atomic::Ordering as AtomicOrdering;

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) async fn handle_write(&mut self, from: NodeId, write: &ReplicationWrite) {
        let start = std::time::Instant::now();
        let partition = write.partition;

        tracing::debug!(
            ?partition,
            from = from.get(),
            sequence = write.sequence,
            entity = ?write.entity,
            "received replication write"
        );

        let ack = if let Some(state) = self.replicas.get_mut(&partition.get()) {
            let ack = state.handle_write(write);
            let t_state = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
            tracing::debug!(
                ?partition,
                sequence = write.sequence,
                ack_ok = ack.is_ok(),
                ack_status = ?ack.status(),
                "handle_write result"
            );
            if ack.is_ok() {
                if let Err(e) = self.stores.apply_write(write) {
                    tracing::error!(?partition, ?e, "failed to apply write to stores");
                } else {
                    tracing::debug!(
                        ?partition,
                        sequence = write.sequence,
                        entity = ?write.entity,
                        id = ?write.id,
                        "applied write to stores"
                    );
                    self.sync_retained_to_broker(write).await;
                }
                let t_apply = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                self.write_log
                    .append(partition, write.sequence, write.clone());
                let t_log = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                tracing::trace!(
                    node = self.node_id.get(),
                    from = from.get(),
                    t_state,
                    t_apply,
                    t_log,
                    "replica_handle_write_timing"
                );
            }
            ack
        } else {
            tracing::warn!(
                ?partition,
                from = from.get(),
                "not a replica for partition, rejecting write"
            );
            ReplicationAck::not_replica(partition, self.node_id)
        };

        let _ = self.transport.send(from, ClusterMessage::Ack(ack)).await;
    }

    pub(crate) async fn handle_write_request(&mut self, from: NodeId, write: ReplicationWrite) {
        WRITE_RECEIVED.fetch_add(1, AtomicOrdering::Relaxed);
        let partition = write.partition;
        let is_broadcast = write.entity == entity::TOPIC_INDEX
            || write.entity == entity::WILDCARDS
            || write.entity == entity::CLIENT_LOCATIONS
            || write.entity == entity::DB_SCHEMA
            || write.entity == entity::DB_CONSTRAINT;

        tracing::debug!(
            ?partition,
            from = from.get(),
            entity = ?write.entity,
            is_broadcast,
            "received write request"
        );

        if is_broadcast && let Err(e) = self.stores.apply_write(&write) {
            tracing::error!(?partition, ?e, "failed to apply broadcast write to stores");
        }

        if !self.is_primary_for_partition(partition) {
            if !is_broadcast {
                tracing::warn!(
                    ?partition,
                    from = from.get(),
                    "received write request but not primary"
                );
            }
            return;
        }

        if !is_broadcast && let Err(e) = self.stores.apply_write(&write) {
            tracing::error!(?partition, ?e, "failed to apply write request to stores");
            return;
        }

        tracing::debug!(
            ?partition,
            entity = ?write.entity,
            id = ?write.id,
            "applied write request to stores"
        );
        self.sync_retained_to_broker(&write).await;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        if let Err(e) = self.replicate_write_async(write, &replicas, None).await {
            tracing::warn!(?partition, ?e, "failed to replicate write request");
        }
    }

    pub(crate) async fn handle_ack(&mut self, ack: ReplicationAck) {
        self.pending.record_ack(&ack);

        if ack.status() == Some(protocol::AckStatus::SequenceGap) {
            self.handle_sequence_gap_ack(&ack).await;
        }

        for (seq, result) in self.pending.drain_completed() {
            self.on_write_complete(ack.partition(), seq, result);
        }
    }

    async fn handle_sequence_gap_ack(&self, ack: &ReplicationAck) {
        let partition = ack.partition();
        let from_seq = ack.sequence();
        let replica_id = ack.node_id();

        let Some(state) = self.replicas.get(&partition.get()) else {
            return;
        };

        if state.role() != ReplicaRole::Primary {
            return;
        }

        let current_seq = state.sequence();
        if from_seq >= current_seq {
            return;
        }

        tracing::debug!(
            ?partition,
            from_seq,
            current_seq,
            ?replica_id,
            "replica has sequence gap, sending catchup"
        );

        let writes = self.write_log.get_range(partition, from_seq, current_seq);
        if writes.is_empty() {
            tracing::warn!(
                ?partition,
                from_seq,
                current_seq,
                "cannot serve catchup - data evicted from log"
            );
            return;
        }

        let Some(replica_node) = NodeId::validated(replica_id) else {
            return;
        };

        let resp = CatchupResponse::create(partition, self.node_id, writes);
        let _ = self
            .transport
            .send(replica_node, ClusterMessage::CatchupResponse(resp))
            .await;
    }

    fn on_write_complete(&mut self, partition: PartitionId, seq: u64, result: QuorumResult) {
        match result {
            QuorumResult::Success => {
                tracing::debug!(?partition, seq, "quorum write succeeded");
            }
            QuorumResult::Failed => {
                tracing::warn!(?partition, seq, "quorum write failed");
                if let Some(state) = self.replicas.get(&partition.get()) {
                    tracing::debug!(
                        ?partition,
                        epoch = ?state.epoch(),
                        "write failed for partition"
                    );
                }
            }
            QuorumResult::Pending => {
                tracing::trace!(?partition, seq, "quorum write still pending");
            }
        }
    }

    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    pub async fn replicate_write(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
        required_acks: usize,
    ) -> Result<u64, ReplicationError> {
        let partition = write.partition;

        let (sequence, epoch) = {
            let state = self
                .replicas
                .get_mut(&partition.get())
                .ok_or(ReplicationError::NotPrimary)?;

            if state.role() != ReplicaRole::Primary {
                return Err(ReplicationError::NotPrimary);
            }

            (state.advance_sequence(), state.epoch())
        };

        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            epoch,
            sequence,
            write.entity,
            write.id,
            write.data,
        );

        self.write_log
            .append(partition, sequence, write_msg.clone());
        let _ = self.stores.apply_write(&write_msg);

        let tracker = QuorumTracker::new(sequence, epoch, replicas, required_acks);

        if !self.pending.add(tracker) {
            return Err(ReplicationError::TooManyPending);
        }

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }

        Ok(sequence)
    }

    #[must_use]
    pub fn node_status(&self, node: NodeId) -> NodeStatus {
        self.heartbeat.node_status(node)
    }

    #[must_use]
    pub fn alive_peers(&self) -> Vec<NodeId> {
        self.heartbeat.alive_nodes()
    }

    #[must_use]
    pub fn has_alive_peers(&self) -> bool {
        self.heartbeat.has_alive_peers()
    }

    #[must_use]
    pub fn sequence(&self, partition: PartitionId) -> Option<u64> {
        self.replicas
            .get(&partition.get())
            .map(ReplicaState::sequence)
    }

    #[must_use]
    pub fn epoch(&self, partition: PartitionId) -> Option<Epoch> {
        self.replicas.get(&partition.get()).map(ReplicaState::epoch)
    }

    pub fn stores_mut(&mut self) -> &mut StoreManager {
        &mut self.stores
    }

    #[must_use]
    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    pub async fn write_or_forward(&mut self, write: ReplicationWrite) {
        self.write_or_forward_impl(write, None, &[]).await;
    }

    pub(crate) async fn write_or_forward_with_outbox(
        &mut self,
        write: ReplicationWrite,
        outbox: super::super::store_manager::outbox::OutboxPayload,
    ) {
        self.write_or_forward_impl(write, Some(outbox), &[]).await;
    }

    pub(crate) async fn write_or_forward_with_outbox_entries(
        &mut self,
        write: ReplicationWrite,
        outbox: super::super::store_manager::outbox::OutboxPayload,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) {
        self.write_or_forward_impl(write, Some(outbox), extra_outbox)
            .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn write_or_forward_impl(
        &mut self,
        write: ReplicationWrite,
        outbox: Option<super::super::store_manager::outbox::OutboxPayload>,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) {
        let write_count = WRITE_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        if write_count.is_multiple_of(10000) {
            let wr_sent = WRITE_REQUEST_SENT.load(AtomicOrdering::Relaxed);
            let wr_recv = WRITE_RECEIVED.load(AtomicOrdering::Relaxed);
            tracing::warn!(
                node = self.node_id.get(),
                writes = write_count,
                wr_sent,
                wr_recv,
                entity = %write.entity,
                "WRITE_COUNTER milestone"
            );
        }
        let start = std::time::Instant::now();
        let partition = write.partition;

        let is_broadcast_entity = write.entity == entity::TOPIC_INDEX
            || write.entity == entity::WILDCARDS
            || write.entity == entity::CLIENT_LOCATIONS
            || write.entity == entity::DB_SCHEMA
            || write.entity == entity::DB_CONSTRAINT;
        if is_broadcast_entity {
            let _ = self.stores.apply_write(&write);
            let alive = self.heartbeat.alive_nodes();
            let targets: Vec<_> = alive.iter().filter(|&n| *n != self.node_id).collect();
            tracing::debug!(
                entity = write.entity,
                id = write.id,
                alive_count = alive.len(),
                target_count = targets.len(),
                targets = ?targets.iter().map(|n| n.get()).collect::<Vec<_>>(),
                "write_or_forward: broadcasting to alive nodes"
            );
            for node in targets {
                WRITE_REQUEST_SENT.fetch_add(1, AtomicOrdering::Relaxed);
                let _ = self
                    .transport
                    .send(*node, ClusterMessage::WriteRequest(write.clone()))
                    .await;
            }
            return;
        }

        if self.is_primary_for_partition(partition) {
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            WRITE_REQUEST_SENT.fetch_add(replicas.len() as u64, AtomicOrdering::Relaxed);
            let t_lookup = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
            let _ = self
                .replicate_write_async_with_extra(write, &replicas, outbox, extra_outbox)
                .await;
            let t_replicate = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
            tracing::trace!(
                node = self.node_id.get(),
                ?partition,
                replica_count = replicas.len(),
                t_lookup,
                t_replicate,
                "write_or_forward_timing"
            );
        } else if let Some(primary) = self
            .partition_map
            .primary(partition)
            .or_else(|| self.heartbeat.partition_map().primary(partition))
        {
            tracing::debug!(
                ?partition,
                primary = primary.get(),
                entity = ?write.entity,
                "write_or_forward: forwarding to primary"
            );
            let _ = self
                .transport
                .send(primary, ClusterMessage::WriteRequest(write))
                .await;
        } else {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: no primary found for partition"
            );
        }
    }
}
