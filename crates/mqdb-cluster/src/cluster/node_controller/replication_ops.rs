// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::entity;
use super::super::protocol::{
    self, CatchupResponse, ChangeEventRecord, ReplicationAck, ReplicationWrite,
};
use super::super::quorum::{QuorumResult, QuorumTracker};
use super::super::replication::{ReplicaRole, ReplicaState};
use super::super::store_manager::{HeldPrimaries, StoreManager};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionAssignment, PartitionId, PartitionMap};
use super::{
    HELD_WRITE_CAPACITY, HELD_WRITE_TIMEOUT, HeldWrite, NodeController, NodeStatus,
    ReplicationError, WRITE_COUNTER, WRITE_RECEIVED, WRITE_REQUEST_SENT,
};
use std::sync::atomic::Ordering as AtomicOrdering;

fn is_broadcast_entity(entity_name: &str) -> bool {
    [
        entity::TOPIC_INDEX,
        entity::WILDCARDS,
        entity::CLIENT_LOCATIONS,
        entity::DB_SCHEMA,
        entity::DB_CONSTRAINT,
    ]
    .contains(&entity_name)
}

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
                if let Err(e) = self.apply_replicated_write(write) {
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
        let is_broadcast = is_broadcast_entity(&write.entity);

        tracing::debug!(
            ?partition,
            from = from.get(),
            entity = ?write.entity,
            is_broadcast,
            "received write request"
        );

        if is_broadcast {
            if self.role(partition) == ReplicaRole::Primary {
                self.apply_write_request(write).await;
                return;
            }
            if let Err(e) = self.stores.apply_write(&write) {
                tracing::error!(?partition, ?e, "failed to apply broadcast write to stores");
            }
            if let Some(record) = write.event {
                self.publish_unowned_change_event(partition, record).await;
            }
            return;
        }

        self.route_write_request(from, write).await;
    }

    async fn route_write_request(&mut self, from: NodeId, write: ReplicationWrite) {
        let partition = write.partition;
        let assignment_epoch = self.partition_map.get(partition).epoch;
        if write.epoch > assignment_epoch || self.waiting_to_take(partition) {
            self.hold_write_request(from, write).await;
            return;
        }
        if self.is_primary_for_partition(partition) {
            self.apply_write_request(write).await;
            return;
        }

        if write.epoch < assignment_epoch {
            tracing::warn!(
                ?partition,
                from = from.get(),
                epoch = ?write.epoch,
                "received write request for an older assignment"
            );
        } else {
            tracing::warn!(
                ?partition,
                from = from.get(),
                "received write request but not primary"
            );
        }
        if let Some(record) = write.event {
            self.publish_unowned_change_event(partition, record).await;
        }
    }

    async fn hold_write_request(&mut self, from: NodeId, write: ReplicationWrite) {
        if self.held_writes.len() >= HELD_WRITE_CAPACITY
            && let Some(dropped) = self.held_writes.pop_front()
        {
            tracing::warn!(
                partition = ?dropped.write.partition,
                from = dropped.from.get(),
                "held write request dropped: too many held"
            );
            if let Some(record) = dropped.write.event {
                self.publish_unowned_change_event(dropped.write.partition, record)
                    .await;
            }
        }
        tracing::debug!(
            partition = ?write.partition,
            from = from.get(),
            epoch = ?write.epoch,
            "holding write request until the assignment is applied"
        );
        self.held_writes.push_back(HeldWrite {
            from,
            write,
            since: std::time::Instant::now(),
        });
    }

    pub(super) async fn release_held_writes(&mut self) {
        let held = std::mem::take(&mut self.held_writes);
        for entry in held {
            let assignment_epoch = self.partition_map.get(entry.write.partition).epoch;
            if entry.write.epoch <= assignment_epoch && !self.waiting_to_take(entry.write.partition)
            {
                self.route_write_request(entry.from, entry.write).await;
            } else if entry.since.elapsed() >= HELD_WRITE_TIMEOUT {
                tracing::warn!(
                    partition = ?entry.write.partition,
                    from = entry.from.get(),
                    epoch = ?entry.write.epoch,
                    "held write request dropped: the assignment was not applied or the snapshot never arrived"
                );
                if let Some(record) = entry.write.event {
                    self.publish_unowned_change_event(entry.write.partition, record)
                        .await;
                }
            } else {
                self.held_writes.push_back(entry);
            }
        }
    }

    async fn apply_write_request(&mut self, mut write: ReplicationWrite) {
        let partition = write.partition;
        if self.role(partition) != ReplicaRole::Primary {
            tracing::warn!(
                ?partition,
                "write request dropped: primary in the map but not in the primary role"
            );
            if let Some(record) = write.event {
                self.publish_unowned_change_event(partition, record).await;
            }
            return;
        }

        let retained_topic = (write.entity == entity::RETAINED).then(|| write.id.clone());
        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        let event = write.event.take();
        if let Err(e) = self.replicate_write_async(write, &replicas, event).await {
            tracing::warn!(?partition, ?e, "failed to replicate write request");
            return;
        }
        if let Some(topic) = retained_topic {
            self.transport.queue_retained_refresh(topic).await;
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

    pub async fn apply_raft_partition_map(&mut self, raft_map: PartitionMap) -> bool {
        if self.partition_map == raft_map {
            self.retry_stalled_snapshots().await;
            self.release_held_writes().await;
            return false;
        }
        let held = *self
            .held_primaries
            .get_or_insert_with(|| self.stores.held_primaries());
        let current_map = self.partition_map.clone();
        let mut became_primary = false;
        let mut changes = 0u32;
        for partition in PartitionId::all() {
            let new_assignment = raft_map.get(partition);
            let old_assignment = current_map.get(partition);
            if new_assignment == old_assignment {
                continue;
            }
            if new_assignment.primary == Some(self.node_id) {
                match self.source_needed_before_primary(partition, new_assignment, &held) {
                    Some(source) => {
                        self.await_snapshot(partition, new_assignment.epoch, source)
                            .await;
                    }
                    None => {
                        self.become_primary(partition, new_assignment.epoch);
                        became_primary |= old_assignment.primary != Some(self.node_id);
                    }
                }
            } else if new_assignment.replicas.contains(&self.node_id) {
                if matches!(
                    self.role(partition),
                    ReplicaRole::Primary | ReplicaRole::Replica
                ) {
                    let sequence = self.sequence(partition).unwrap_or(0);
                    self.become_replica(partition, new_assignment.epoch, sequence);
                } else if let Some(primary) = new_assignment.primary {
                    self.become_replica_with_snapshot(partition, new_assignment.epoch, primary)
                        .await;
                }
            } else {
                self.step_down(partition);
                self.forget_change_events(partition);
            }
            changes += 1;
            if changes.is_multiple_of(8) {
                tokio::task::yield_now().await;
            }
        }
        self.held_primaries = Some(held.without_legacy());
        self.update_partition_map(raft_map);
        self.retry_stalled_snapshots().await;
        self.release_held_writes().await;
        became_primary
    }

    fn waiting_to_take(&self, partition: PartitionId) -> bool {
        self.partition_map.primary(partition) == Some(self.node_id)
            && self.role(partition) == ReplicaRole::AwaitingSnapshot
    }

    fn source_needed_before_primary(
        &self,
        partition: PartitionId,
        assignment: &PartitionAssignment,
        held: &HeldPrimaries,
    ) -> Option<NodeId> {
        let holds_data = matches!(
            self.role(partition),
            ReplicaRole::Primary | ReplicaRole::Replica
        ) || (!self.held_since_boot[usize::from(partition.get())]
            && held.trusts(partition, assignment.epoch));
        if holds_data {
            return None;
        }
        assignment
            .replicas
            .iter()
            .copied()
            .find(|node| *node != self.node_id)
    }

    pub async fn write_or_forward(&mut self, write: ReplicationWrite) {
        self.write_or_forward_impl(write, None, &[]).await;
    }

    pub(crate) async fn write_or_forward_with_event(
        &mut self,
        write: ReplicationWrite,
        event: ChangeEventRecord,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) {
        self.write_or_forward_impl(write, Some(event), extra_outbox)
            .await;
    }

    async fn broadcast_write(&self, write: &ReplicationWrite) {
        let _ = self.stores.apply_write(write);
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
    }

    fn believed_primary(&self, partition: PartitionId) -> Option<(NodeId, Epoch)> {
        [&self.partition_map, self.heartbeat.partition_map()]
            .into_iter()
            .map(|map| map.get(partition))
            .find_map(|assignment| {
                assignment
                    .primary
                    .map(|primary| (primary, assignment.epoch))
            })
    }

    async fn write_or_forward_impl(
        &mut self,
        mut write: ReplicationWrite,
        event: Option<ChangeEventRecord>,
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

        if write.entity == entity::DB_UNIQUE {
            self.replicate_unique_to_group(write).await;
            return;
        }

        if is_broadcast_entity(&write.entity) {
            self.broadcast_write(&write).await;
            return;
        }

        if self.waiting_to_take(partition) && extra_outbox.is_empty() {
            write.event = event;
            let own = self.node_id;
            self.hold_write_request(own, write).await;
        } else if self.is_primary_for_partition(partition)
            && self.role(partition) != ReplicaRole::Primary
        {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: primary in the map but not in the primary role"
            );
            if let Some(record) = event {
                self.publish_unowned_change_event(partition, record).await;
            }
        } else if self.is_primary_for_partition(partition) {
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            WRITE_REQUEST_SENT.fetch_add(replicas.len() as u64, AtomicOrdering::Relaxed);
            let t_lookup = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
            let _ = self
                .replicate_write_async_with_extra(write, &replicas, event, extra_outbox)
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
        } else if let Some((primary, epoch)) = self.believed_primary(partition) {
            write.epoch = epoch;
            tracing::debug!(
                ?partition,
                primary = primary.get(),
                entity = ?write.entity,
                "write_or_forward: forwarding to primary"
            );
            self.forward_write_with_change_event(primary, write, event)
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
