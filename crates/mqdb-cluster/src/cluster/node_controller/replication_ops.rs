// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::entity;
use super::super::protocol::{self, ChangeEventRecord, ReplicationAck, ReplicationWrite};
use super::super::quorum::{QuorumResult, QuorumTracker};
use super::super::replication::{ReplicaRole, ReplicaState};
use super::super::store_manager::{HeldPrimaries, StoreApplyError, StoreManager};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NodeId, PartitionAssignment, PartitionId, PartitionMap};
use super::{
    HELD_WRITE_CAPACITY, HELD_WRITE_TIMEOUT, HeldWrite, NodeController, NodeStatus,
    ReplicationError, SupersededPrimaryWrite, UnansweredWrite, WRITE_COUNTER, WRITE_RECEIVED,
    WRITE_REQUEST_SENT,
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
            let t_state = super::elapsed_micros(start);
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
                let t_apply = super::elapsed_micros(start);
                self.write_log
                    .append(partition, write.sequence, write.clone());
                let t_log = super::elapsed_micros(start);
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
            ReplicationAck::not_replica(partition, write.sequence, self.node_id)
        };
        if ack.status() == Some(protocol::AckStatus::NotReplica)
            && self.role(partition) == ReplicaRole::Primary
        {
            tracing::warn!(
                ?partition,
                from = from.get(),
                epoch = ?write.epoch,
                "refused a replication write while primary for the partition"
            );
            self.refused_while_primary.insert(partition);
        }

        if let Err(e) = self.transport.send(from, ClusterMessage::Ack(ack)).await {
            tracing::debug!(to = from.get(), %e, "replication ack not sent");
        }
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
            if let Err(e) = self.stores.apply_write_keeping_unrecovered(&write) {
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
        if write.epoch > assignment_epoch {
            self.hold_write_request(from, write).await;
            return;
        }
        if self.waiting_to_take(partition) {
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
            let partition = entry.write.partition;
            if entry.write.epoch <= assignment_epoch && !self.waiting_to_take(partition) {
                if entry.from == self.node_id && !self.is_primary_for_partition(partition) {
                    let mut write = entry.write;
                    let event = write.event.take();
                    if let Err(e) = self.write_or_forward_with_event(write, event, &[]).await {
                        tracing::warn!(?partition, %e, "held write not forwarded");
                    }
                } else {
                    self.route_write_request(entry.from, entry.write).await;
                }
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
        if let Err(refused) = self
            .replicate_primary_write(write, &replicas, event, &[])
            .await
        {
            tracing::warn!(?partition, error = ?refused.error, "failed to replicate write request");
            if let Some(record) = refused.event {
                self.publish_unowned_change_event(partition, record).await;
            }
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

        self.settle_unanswered(&ack);

        for (partition, seq, result) in self.pending.drain_completed() {
            self.on_write_complete(partition, seq, result);
        }
        if self.has_handoff_work() {
            Box::pin(self.settle_handoff()).await;
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

        self.send_catchup_writes(replica_node, partition, writes)
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
    /// Returns `StoreFailed` if this node cannot store the write.
    pub async fn replicate_write(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
        required_acks: usize,
    ) -> Result<u64, ReplicationError> {
        let partition = write.partition;

        let state = self
            .replicas
            .get(&partition.get())
            .filter(|state| state.role() == ReplicaRole::Primary)
            .ok_or(ReplicationError::NotPrimary)?;
        let sequence = state.sequence() + 1;
        let epoch = state.epoch();

        if !replicas.is_empty() && self.pending.is_full() {
            return Err(ReplicationError::TooManyPending);
        }

        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            epoch,
            sequence,
            write.entity,
            write.id,
            write.data,
        );

        self.apply_primary_write(&write_msg, None, &[])?;

        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.advance_sequence();
        }
        self.write_log
            .append(partition, sequence, write_msg.clone());

        if !replicas.is_empty() {
            self.pending
                .add(QuorumTracker::new(
                    partition,
                    sequence,
                    epoch,
                    replicas,
                    required_acks,
                ))
                .map_err(|_| ReplicationError::TooManyPending)?;
        }

        for &replica in replicas {
            if let Err(e) = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await
            {
                tracing::debug!(replica = replica.get(), %e, "replication write not sent");
            }
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
            self.release_held_writes().await;
            self.serve_deferred_snapshots().await;
            return std::mem::take(&mut self.primary_taken_after_wait);
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
                if let Some(source) = self.source_needed_before_primary(
                    partition,
                    new_assignment,
                    old_assignment,
                    &held,
                ) {
                    self.await_snapshot(partition, new_assignment.epoch, source, true)
                        .await;
                } else {
                    self.become_primary(partition, new_assignment.epoch);
                    became_primary |= old_assignment.primary != Some(self.node_id);
                }
            } else if new_assignment.replicas.contains(&self.node_id) {
                if matches!(
                    self.role(partition),
                    ReplicaRole::Primary | ReplicaRole::Replica
                ) {
                    let sequence = self.sequence(partition).unwrap_or(0);
                    let demoted = self.role(partition) == ReplicaRole::Primary;
                    self.become_replica(partition, new_assignment.epoch, sequence);
                    if demoted && self.holds_unconfirmed_writes(partition) {
                        self.resync_after_demotion.insert(partition);
                    }
                    self.refused_while_primary.remove(&partition);
                } else if let Some(primary) = new_assignment.primary {
                    self.become_replica_with_snapshot(partition, new_assignment.epoch, primary)
                        .await;
                    self.remember_replaced_replica(partition, self.other_replica(old_assignment));
                }
            } else {
                self.step_down(partition);
                self.forget_change_events(partition);
                self.refused_while_primary.remove(&partition);
            }
            changes += 1;
            if changes.is_multiple_of(8) {
                tokio::task::yield_now().await;
            }
        }
        self.held_primaries = Some(held.without_legacy());
        self.update_partition_map(raft_map);
        self.send_owed_refusals().await;
        self.release_held_writes().await;
        self.serve_deferred_snapshots().await;
        if self.has_handoff_work() {
            Box::pin(self.settle_handoff()).await;
        }
        became_primary | std::mem::take(&mut self.primary_taken_after_wait)
    }

    fn holds_unconfirmed_writes(&self, partition: PartitionId) -> bool {
        self.refused_while_primary.contains(&partition)
            || self.holds_unanswered(partition)
            || self.holds_superseded(partition)
    }

    pub(super) fn holds_unanswered(&self, partition: PartitionId) -> bool {
        self.unanswered.keys().any(|(held, _)| *held == partition)
    }

    fn holds_superseded(&self, partition: PartitionId) -> bool {
        self.superseded_writes
            .iter()
            .any(|entry| entry.write.partition == partition)
    }

    pub(super) fn remember_unanswered(&mut self, write: &ReplicationWrite, replicas: &[NodeId]) {
        if replicas.is_empty() {
            return;
        }
        if self.unanswered_order.len() >= HELD_WRITE_CAPACITY
            && let Some((partition, sequence, epoch)) = self.unanswered_order.pop_front()
            && self
                .unanswered
                .get(&(partition, sequence))
                .is_some_and(|held| held.write.epoch == epoch)
            && let Some(dropped) = self.unanswered.remove(&(partition, sequence))
        {
            tracing::warn!(
                ?partition,
                entity = %dropped.write.entity,
                id = %dropped.write.id,
                "forgetting a write no replica answered: too many waiting"
            );
        }
        let mut held = write.clone();
        held.event = None;
        self.unanswered_order
            .push_back((held.partition, held.sequence, held.epoch));
        self.unanswered.insert(
            (held.partition, held.sequence),
            UnansweredWrite {
                write: held,
                awaiting: replicas.to_vec(),
            },
        );
    }

    fn settle_unanswered(&mut self, ack: &ReplicationAck) {
        let key = (ack.partition(), ack.sequence());
        let Some(held) = self.unanswered.get_mut(&key) else {
            return;
        };
        let answers = match ack.status() {
            Some(protocol::AckStatus::Ok) => held.write.epoch == ack.epoch(),
            Some(protocol::AckStatus::StaleEpoch) => held.write.epoch < ack.epoch(),
            Some(protocol::AckStatus::NotReplica) => true,
            Some(protocol::AckStatus::SequenceGap) | None => false,
        };
        if !answers {
            return;
        }
        if ack.status() == Some(protocol::AckStatus::StaleEpoch) {
            if let Some(refused) = self.unanswered.remove(&key) {
                self.keep_superseded_write(refused.write, ack.epoch());
            }
            return;
        }
        held.awaiting
            .retain(|replica| replica.get() != ack.node_id());
        if held.awaiting.is_empty() {
            self.unanswered.remove(&key);
        }
    }

    fn keep_superseded_write(&mut self, write: ReplicationWrite, superseded_at: Epoch) {
        if self.superseded_writes.len() >= HELD_WRITE_CAPACITY
            && let Some(dropped) = self.superseded_writes.pop_front()
        {
            tracing::warn!(
                partition = ?dropped.write.partition,
                entity = %dropped.write.entity,
                id = %dropped.write.id,
                "dropping a write refused by a newer primary: too many waiting"
            );
        }
        self.superseded_writes.push_back(SupersededPrimaryWrite {
            write,
            superseded_at,
        });
    }

    pub(super) async fn send_owed_refusals(&mut self) {
        for (previous, refusal) in std::mem::take(&mut self.refusals_owed) {
            tracing::info!(
                partition = ?refusal.partition(),
                sequence = refusal.sequence(),
                to = previous.get(),
                "refusing a write held for a gap when this node took the primary"
            );
            if let Err(e) = self
                .transport
                .send(previous, ClusterMessage::Ack(refusal))
                .await
            {
                tracing::debug!(to = previous.get(), %e, "held-write refusal not sent");
            }
        }
    }

    fn has_handoff_work(&self) -> bool {
        !self.superseded_writes.is_empty() || !self.resync_after_demotion.is_empty()
    }

    async fn settle_handoff(&mut self) {
        self.forward_superseded_writes().await;
        self.start_due_resyncs().await;
    }

    async fn forward_superseded_writes(&mut self) {
        let waiting = std::mem::take(&mut self.superseded_writes);
        for entry in waiting {
            let partition = entry.write.partition;
            if !self.is_primary_for_partition(partition) {
                tracing::info!(
                    ?partition,
                    entity = %entry.write.entity,
                    id = %entry.write.id,
                    "forwarding a write a newer primary refused to the current primary"
                );
                if let Err(e) = self
                    .write_or_forward_with_event(entry.write, None, &[])
                    .await
                {
                    tracing::warn!(?partition, %e, "refused write not forwarded");
                }
            } else if self.partition_map.get(partition).epoch >= entry.superseded_at {
                tracing::warn!(
                    ?partition,
                    entity = %entry.write.entity,
                    id = %entry.write.id,
                    "a replica refused a write at an epoch this node is still primary for"
                );
            } else {
                self.superseded_writes.push_back(entry);
            }
        }
    }

    async fn start_due_resyncs(&mut self) {
        let waiting = std::mem::take(&mut self.resync_after_demotion);
        for partition in waiting {
            if self.holds_unanswered(partition) || self.holds_superseded(partition) {
                self.resync_after_demotion.insert(partition);
                continue;
            }
            if self.role(partition) != ReplicaRole::Replica {
                continue;
            }
            let Some(primary) = self
                .partition_map
                .primary(partition)
                .filter(|primary| *primary != self.node_id)
            else {
                continue;
            };
            let epoch = self.partition_map.get(partition).epoch;
            tracing::info!(
                ?partition,
                primary = primary.get(),
                "resyncing a demoted primary that held unconfirmed writes"
            );
            self.become_replica_with_snapshot(partition, epoch, primary)
                .await;
        }
    }

    fn waiting_to_take(&self, partition: PartitionId) -> bool {
        self.partition_map.primary(partition) == Some(self.node_id)
            && self.role(partition) == ReplicaRole::AwaitingSnapshot
    }

    fn source_needed_before_primary(
        &self,
        partition: PartitionId,
        assignment: &PartitionAssignment,
        old_assignment: &PartitionAssignment,
        held: &HeldPrimaries,
    ) -> Option<NodeId> {
        let holds_data = matches!(
            self.role(partition),
            ReplicaRole::Primary | ReplicaRole::Replica
        ) || held.trusts(partition, assignment);
        if holds_data {
            return None;
        }
        let current = self.other_replica(assignment);
        if current.is_some_and(|node| old_assignment.all_nodes().contains(&node)) {
            return current;
        }
        old_assignment
            .primary
            .filter(|node| {
                *node != self.node_id
                    && matches!(
                        self.node_status(*node),
                        NodeStatus::Alive | NodeStatus::Suspected
                    )
            })
            .or_else(|| self.replaced_replica(partition))
            .or_else(|| self.other_replica(old_assignment))
            .filter(|node| self.source_is_alive(*node))
            .or(current)
    }

    fn other_replica(&self, assignment: &PartitionAssignment) -> Option<NodeId> {
        assignment
            .replicas
            .iter()
            .copied()
            .find(|node| *node != self.node_id)
    }

    pub async fn write_or_forward(&mut self, write: ReplicationWrite) {
        if let Err(e) = self.write_or_forward_with_event(write, None, &[]).await {
            tracing::debug!(%e, "write_or_forward: write not applied");
        }
    }

    pub(crate) async fn broadcast_write(
        &self,
        write: &ReplicationWrite,
    ) -> Result<(), ReplicationError> {
        match self.stores.apply_write_keeping_unrecovered(write) {
            Ok(()) => {}
            Err(StoreApplyError::PersistenceError) => {
                tracing::error!(
                    entity = write.entity,
                    id = write.id,
                    "broadcast write not stored on this node, not sent"
                );
                return Err(ReplicationError::StoreFailed);
            }
            Err(error) => {
                tracing::warn!(
                    entity = write.entity,
                    id = write.id,
                    %error,
                    "broadcast write not applied in memory"
                );
            }
        }
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
            if let Err(error) = self
                .transport
                .send(*node, ClusterMessage::WriteRequest(write.clone()))
                .await
            {
                tracing::warn!(to = node.get(), %error, entity = write.entity, "broadcast write not sent");
            }
        }
        Ok(())
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

    pub(crate) async fn write_or_forward_with_event(
        &mut self,
        mut write: ReplicationWrite,
        event: Option<ChangeEventRecord>,
        extra_outbox: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ReplicationError> {
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
            return Ok(());
        }

        if is_broadcast_entity(&write.entity) {
            return self.broadcast_write(&write).await;
        }

        if self.waiting_to_take(partition) && extra_outbox.is_empty() {
            write.event = event;
            let own = self.node_id;
            self.hold_write_request(own, write).await;
            Ok(())
        } else if self.is_primary_for_partition(partition)
            && self.role(partition) != ReplicaRole::Primary
        {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: primary in the map but not in the primary role"
            );
            Err(ReplicationError::NotPrimary)
        } else if self.is_primary_for_partition(partition) {
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            WRITE_REQUEST_SENT.fetch_add(replicas.len() as u64, AtomicOrdering::Relaxed);
            let t_lookup = super::elapsed_micros(start);
            self.replicate_primary_write(write, &replicas, event, extra_outbox)
                .await
                .map_err(|refused| refused.error)?;
            let t_replicate = super::elapsed_micros(start);
            tracing::trace!(
                node = self.node_id.get(),
                ?partition,
                replica_count = replicas.len(),
                t_lookup,
                t_replicate,
                "write_or_forward_timing"
            );
            Ok(())
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
            Ok(())
        } else {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: no primary found for partition"
            );
            Err(ReplicationError::NotPrimary)
        }
    }
}
