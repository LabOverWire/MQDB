// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::StoreApplyError;
use super::super::migration::MigrationPhase;
use super::super::protocol::{CatchupRequest, JsonDbRequest, JsonDbResponse};
use super::super::replication::ReplicaRole;
use super::super::snapshot::{
    ChunkOutcome, SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
};
use super::super::transport::{ClusterMessage, ClusterTransport, TransportError};
use super::super::{Epoch, NodeId, PartitionId};
use super::NodeController;
use crate::cluster::heartbeat::NodeStatus;
use std::collections::{HashMap, HashSet};

pub(crate) const SNAPSHOT_CHUNKS_PER_PUMP: u32 = 8;
pub(crate) const SNAPSHOT_STALL_TIMEOUT_MS: u64 = 30_000;
pub(crate) const SNAPSHOT_RETRY_BACKOFF_MS: u64 = 5_000;
pub(super) const DEFERRED_SNAPSHOT_CAPACITY: usize = 1024;
const SILENT_SOURCE_GRACE_MS: u64 = 90_000;

#[derive(Debug)]
pub(crate) struct OutgoingSnapshot {
    to: NodeId,
    partition: PartitionId,
    pub(super) sender: Option<SnapshotSender>,
}

#[derive(Debug)]
enum IncomingState {
    Requested {
        source: NodeId,
        builder: Option<SnapshotBuilder>,
    },
    BackingOff,
    CatchingUp {
        asked_from: u64,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SnapshotWait {
    source: Option<NodeId>,
    first_asked_ms: Option<u64>,
    taking_primary: bool,
    replaced_replica: Option<NodeId>,
}

#[derive(Debug)]
pub(crate) struct IncomingSnapshot {
    state: IncomingState,
    since_ms: Option<u64>,
    wait: SnapshotWait,
}

impl IncomingSnapshot {
    fn requested_from(&self) -> Option<NodeId> {
        match self.state {
            IncomingState::Requested { source, .. } => Some(source),
            IncomingState::BackingOff | IncomingState::CatchingUp { .. } => None,
        }
    }

    fn catching_up(&self) -> bool {
        matches!(self.state, IncomingState::CatchingUp { .. })
    }

    #[cfg(test)]
    fn building(&self) -> bool {
        matches!(
            self.state,
            IncomingState::Requested {
                builder: Some(_),
                ..
            }
        )
    }

    fn expired(&self, now: u64) -> bool {
        let Some(since) = self.since_ms else {
            return false;
        };
        let limit = match self.state {
            IncomingState::Requested { .. } => SNAPSHOT_STALL_TIMEOUT_MS,
            IncomingState::BackingOff | IncomingState::CatchingUp { .. } => {
                SNAPSHOT_RETRY_BACKOFF_MS
            }
        };
        now.saturating_sub(since) >= limit
    }
}

impl<T: ClusterTransport> NodeController<T> {
    fn snapshot_stamp(&self) -> Option<u64> {
        (self.snapshot_clock_ms > 0).then_some(self.snapshot_clock_ms)
    }

    pub(crate) async fn handle_snapshot_request(&mut self, from: NodeId, req: &SnapshotRequest) {
        let Some(partition) = req.partition() else {
            tracing::warn!(?from, "received snapshot request with invalid partition");
            return;
        };

        let waits_on_requester = self
            .incoming_snapshots
            .get(&partition)
            .is_some_and(|incoming| incoming.wait.source == Some(from));
        if self.role(partition) == ReplicaRole::AwaitingSnapshot && !waits_on_requester {
            self.defer_snapshot_request(from, partition);
            return;
        }

        self.queue_snapshot_send(from, partition);
        self.pump_outgoing_snapshots().await;
    }

    fn defer_snapshot_request(&mut self, from: NodeId, partition: PartitionId) {
        if self.deferred_snapshot_requests.contains(&(from, partition)) {
            return;
        }
        if self.deferred_snapshot_requests.len() >= DEFERRED_SNAPSHOT_CAPACITY
            && let Some((dropped_from, dropped_partition)) =
                self.deferred_snapshot_requests.pop_front()
        {
            tracing::warn!(
                partition = ?dropped_partition,
                requester = dropped_from.get(),
                "deferred snapshot request dropped: too many deferred"
            );
        }
        tracing::info!(
            ?partition,
            requester = from.get(),
            "deferring a snapshot request until this node holds the partition"
        );
        self.deferred_snapshot_requests.push_back((from, partition));
    }

    pub(super) async fn serve_deferred_snapshots(&mut self) {
        if self.deferred_snapshot_requests.is_empty() {
            return;
        }
        let deferred = std::mem::take(&mut self.deferred_snapshot_requests);
        let mut queued = false;
        for (from, partition) in deferred {
            match self.role(partition) {
                ReplicaRole::AwaitingSnapshot => {
                    self.deferred_snapshot_requests.push_back((from, partition));
                }
                ReplicaRole::Primary | ReplicaRole::Replica => {
                    self.queue_snapshot_send(from, partition);
                    queued = true;
                }
                ReplicaRole::None => {
                    tracing::info!(
                        ?partition,
                        requester = from.get(),
                        "deferred snapshot request dropped: left the partition without its data"
                    );
                }
            }
        }
        if queued {
            self.pump_outgoing_snapshots().await;
        }
    }

    fn queue_snapshot_send(&mut self, from: NodeId, partition: PartitionId) {
        tracing::info!(
            ?partition,
            requester = from.get(),
            "received snapshot request, queueing snapshot send"
        );

        match self
            .outgoing_snapshots
            .iter_mut()
            .find(|outgoing| outgoing.to == from && outgoing.partition == partition)
        {
            Some(existing) => existing.sender = None,
            None => self.outgoing_snapshots.push_back(OutgoingSnapshot {
                to: from,
                partition,
                sender: None,
            }),
        }
    }

    fn export_snapshot(
        &self,
        partition: PartitionId,
    ) -> Result<Option<SnapshotSender>, StoreApplyError> {
        let data = self.stores.export_partition(partition)?;
        let sequence = self.sequence(partition).unwrap_or(0);
        let transfer_id = uuid::Uuid::new_v4().as_u64_pair().0;
        let Some(sender) = SnapshotSender::new(partition, data, sequence) else {
            return Ok(None);
        };
        let sender = sender.with_transfer_id(transfer_id);
        tracing::info!(
            ?partition,
            total_chunks = sender.total_chunks(),
            transfer_id,
            "exported partition snapshot"
        );
        Ok(Some(sender))
    }

    async fn pump_outgoing_snapshots(&mut self) {
        let mut budget = SNAPSHOT_CHUNKS_PER_PUMP;
        let mut blocked = HashSet::new();
        let mut index = 0;
        while budget > 0 && index < self.outgoing_snapshots.len() {
            let to = self.outgoing_snapshots[index].to;
            if blocked.contains(&to) {
                index += 1;
                continue;
            }
            if self.outgoing_snapshots[index].sender.is_none() {
                let partition = self.outgoing_snapshots[index].partition;
                match self.export_snapshot(partition) {
                    Ok(Some(sender)) => self.outgoing_snapshots[index].sender = Some(sender),
                    Ok(None) => {
                        tracing::warn!(?partition, to = to.get(), "partition too large to send");
                        self.outgoing_snapshots.remove(index);
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(?partition, to = to.get(), %error, "partition snapshot not exported");
                        self.outgoing_snapshots.remove(index);
                        self.send_snapshot_failed(to, partition).await;
                        continue;
                    }
                }
            }
            let outgoing = &mut self.outgoing_snapshots[index];
            let Some(sender) = outgoing.sender.as_mut() else {
                index += 1;
                continue;
            };
            let keep = loop {
                if budget == 0 {
                    break true;
                }
                let Some(chunk) = sender.current_chunk() else {
                    break false;
                };
                match self
                    .transport
                    .send(to, ClusterMessage::SnapshotChunk(chunk))
                    .await
                {
                    Ok(()) => {
                        sender.advance();
                        budget -= 1;
                        if sender.is_complete() {
                            tracing::info!(
                                partition = ?outgoing.partition,
                                to = to.get(),
                                total_chunks = sender.total_chunks(),
                                "snapshot send complete"
                            );
                            break false;
                        }
                    }
                    Err(TransportError::SendQueueFull(_)) => {
                        blocked.insert(to);
                        break true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            partition = ?outgoing.partition,
                            to = to.get(),
                            %error,
                            "abandoning snapshot send"
                        );
                        break false;
                    }
                }
            };
            if keep {
                index += 1;
            } else {
                self.outgoing_snapshots.remove(index);
            }
        }
    }

    pub async fn drive_snapshots(&mut self, now: u64) {
        self.snapshot_clock_ms = now;
        for incoming in self.incoming_snapshots.values_mut() {
            incoming.since_ms.get_or_insert(now);
            if incoming.wait.source.is_some() {
                incoming.wait.first_asked_ms.get_or_insert(now);
            }
        }
        let unanswered_catchups: Vec<PartitionId> = self
            .incoming_snapshots
            .iter()
            .filter(|(_, incoming)| incoming.catching_up() && incoming.expired(now))
            .map(|(partition, _)| *partition)
            .collect();
        let mut retries: HashMap<PartitionId, SnapshotWait> = HashMap::new();
        let replicas = &self.replicas;
        let partition_map = &self.partition_map;
        self.incoming_snapshots.retain(|partition, incoming| {
            if incoming.catching_up() {
                return true;
            }
            if incoming.expired(now) {
                if incoming.requested_from().is_some() {
                    tracing::warn!(?partition, "snapshot transfer stalled, asking again");
                }
                retries.insert(*partition, incoming.wait);
                return false;
            }
            let awaiting = replicas
                .get(&partition.get())
                .is_some_and(|state| state.role() == ReplicaRole::AwaitingSnapshot);
            let moved = !incoming.wait.taking_primary
                && incoming
                    .requested_from()
                    .zip(partition_map.primary(*partition))
                    .is_some_and(|(source, primary)| source != primary);
            if awaiting && moved {
                tracing::info!(
                    ?partition,
                    "partition primary moved, abandoning snapshot request"
                );
                retries.insert(*partition, incoming.wait);
                return false;
            }
            true
        });

        for partition in unanswered_catchups {
            self.request_catchup(partition).await;
        }

        for partition in PartitionId::all() {
            if self.role(partition) != ReplicaRole::AwaitingSnapshot
                || self.incoming_snapshots.contains_key(&partition)
            {
                continue;
            }
            let wait = retries.get(&partition).copied().unwrap_or_default();
            self.retry_snapshot(partition, wait, now).await;
        }

        self.pump_outgoing_snapshots().await;
    }

    async fn retry_snapshot(&mut self, partition: PartitionId, wait: SnapshotWait, now: u64) {
        let waiting_to_take = self.partition_map.primary(partition) == Some(self.node_id);
        let silent_source = wait
            .source
            .is_some_and(|source| self.node_status(source) == NodeStatus::Unknown);
        let grace_over = wait
            .first_asked_ms
            .is_some_and(|asked| now.saturating_sub(asked) >= SILENT_SOURCE_GRACE_MS);
        if waiting_to_take && silent_source && grace_over {
            tracing::warn!(
                ?partition,
                source = ?wait.source,
                "taking the primary role: the snapshot source was never heard from"
            );
            self.take_primary_without_snapshot(partition);
            return;
        }
        let kept_source = wait
            .source
            .filter(|source| wait.taking_primary && self.source_is_alive(*source));
        let source = kept_source.or_else(|| {
            wait.replaced_replica
                .filter(|node| waiting_to_take && self.source_is_alive(*node))
                .or_else(|| self.snapshot_source(partition))
        });
        if let Some(source) = source {
            tracing::info!(
                ?partition,
                source = source.get(),
                "retrying snapshot request while waiting for the data"
            );
            let next = SnapshotWait {
                source: Some(source),
                first_asked_ms: wait.first_asked_ms.filter(|_| wait.source == Some(source)),
                taking_primary: waiting_to_take || kept_source.is_some(),
                replaced_replica: wait.replaced_replica,
            };
            self.send_snapshot_request(partition, next).await;
        } else if waiting_to_take {
            tracing::warn!(
                ?partition,
                source = ?wait.source,
                "taking the primary role: no node is left to ask for the data"
            );
            self.take_primary_without_snapshot(partition);
        }
    }

    fn take_primary_without_snapshot(&mut self, partition: PartitionId) {
        self.become_primary(partition, self.partition_map.epoch(partition));
        self.primary_taken_after_wait = true;
    }

    fn back_off_snapshot(&mut self, partition: PartitionId) {
        let wait = self
            .incoming_snapshots
            .get(&partition)
            .map(|incoming| incoming.wait)
            .unwrap_or_default();
        self.back_off_snapshot_with(partition, wait);
    }

    fn back_off_snapshot_with(&mut self, partition: PartitionId, wait: SnapshotWait) {
        let since_ms = self.snapshot_stamp();
        self.incoming_snapshots.insert(
            partition,
            IncomingSnapshot {
                state: IncomingState::BackingOff,
                since_ms,
                wait,
            },
        );
    }

    async fn refuse_snapshot(&mut self, from: NodeId, partition: PartitionId) {
        self.back_off_snapshot(partition);
        self.send_snapshot_failed(from, partition).await;
    }

    async fn send_snapshot_failed(&self, to: NodeId, partition: PartitionId) {
        let complete = SnapshotComplete::failed(partition);
        if let Err(error) = self
            .transport
            .send(to, ClusterMessage::SnapshotComplete(complete))
            .await
        {
            tracing::debug!(?partition, to = to.get(), %error, "snapshot failure notice not sent");
        }
    }

    pub(crate) fn snapshots_node_gone(&mut self, node: NodeId) {
        self.outgoing_snapshots
            .retain(|outgoing| outgoing.to != node);
        self.deferred_snapshot_requests
            .retain(|(from, _)| *from != node);
        for incoming in self.incoming_snapshots.values_mut() {
            if incoming.requested_from() == Some(node) {
                incoming.state = IncomingState::BackingOff;
                incoming.since_ms = Some(0);
            } else if incoming.catching_up() && incoming.wait.source == Some(node) {
                incoming.since_ms = Some(0);
            }
        }
    }

    pub(crate) async fn handle_snapshot_chunk(&mut self, from: NodeId, chunk: &SnapshotChunk) {
        let partition = chunk.partition;
        let stamp = self.snapshot_stamp();

        tracing::debug!(
            ?partition,
            from = from.get(),
            chunk_index = chunk.chunk_index,
            total_chunks = chunk.total_chunks,
            "received snapshot chunk"
        );

        let Some(incoming) = self.incoming_snapshots.get_mut(&partition) else {
            tracing::debug!(
                ?partition,
                from = from.get(),
                "ignoring unrequested snapshot chunk"
            );
            return;
        };
        let IncomingState::Requested { source, builder } = &mut incoming.state else {
            tracing::debug!(?partition, "ignoring snapshot chunk while backing off");
            return;
        };
        if *source != from {
            tracing::debug!(
                ?partition,
                from = from.get(),
                source = source.get(),
                "ignoring snapshot chunk from a node that was not asked"
            );
            return;
        }

        let outcome = if chunk.chunk_index == 0 {
            match SnapshotBuilder::start(chunk) {
                Some(started) => {
                    *builder = Some(started);
                    ChunkOutcome::Accepted
                }
                None => ChunkOutcome::Gap,
            }
        } else {
            builder
                .as_mut()
                .map_or(ChunkOutcome::Stale, |current| current.offer(chunk))
        };

        match outcome {
            ChunkOutcome::Stale => {
                tracing::debug!(
                    ?partition,
                    chunk_index = chunk.chunk_index,
                    "ignoring stale snapshot chunk"
                );
                return;
            }
            ChunkOutcome::Gap => {
                tracing::warn!(
                    ?partition,
                    chunk_index = chunk.chunk_index,
                    total_chunks = chunk.total_chunks,
                    "snapshot chunk out of order, abandoning transfer"
                );
                self.refuse_snapshot(from, partition).await;
                return;
            }
            ChunkOutcome::Accepted => {}
        }

        incoming.since_ms = stamp;
        let Some(finished) = builder.take_if(|current| current.is_complete()) else {
            return;
        };
        let wait = incoming.wait;
        self.incoming_snapshots.remove(&partition);

        let sequence = finished.sequence_at_snapshot();
        let data = finished.into_data();

        let saved_events = self.set_aside_change_events(partition);
        match self.stores.import_partition_counted(partition, &data) {
            Ok(counts) => {
                self.write_log.clear_partition(partition);
                if counts.partition_records > 0 {
                    self.imported_partitions[usize::from(partition.get())] = true;
                }
                tracing::info!(
                    ?partition,
                    imported = counts.total,
                    partition_records = counts.partition_records,
                    sequence,
                    "snapshot import complete"
                );

                if let Err(e) = self
                    .migration_manager
                    .mark_snapshot_complete(partition, sequence)
                {
                    tracing::debug!(?partition, ?e, "no active migration for snapshot");
                }

                self.reload_change_events(partition);
                if let Some(epoch) = self.epoch(partition) {
                    self.take_role_after_import(partition, epoch, sequence, from, wait)
                        .await;
                }
                self.release_held_writes().await;
                self.serve_deferred_snapshots().await;

                let complete = SnapshotComplete::ok(partition, sequence);
                if let Err(error) = self
                    .transport
                    .send(from, ClusterMessage::SnapshotComplete(complete))
                    .await
                {
                    tracing::debug!(?partition, to = from.get(), %error, "snapshot completion notice not sent");
                }

                if self.role(partition) == ReplicaRole::Replica {
                    self.request_catchup(partition).await;
                }
            }
            Err(e) => {
                tracing::error!(?partition, ?e, "failed to import snapshot");
                if e == StoreApplyError::TransferWindowFull {
                    self.stores.close_transfer_window(partition);
                }
                self.restore_change_events(partition, &saved_events);
                self.back_off_snapshot_with(partition, wait);
                self.send_snapshot_failed(from, partition).await;
            }
        }
    }

    async fn take_role_after_import(
        &mut self,
        partition: PartitionId,
        epoch: Epoch,
        sequence: u64,
        source: NodeId,
        wait: SnapshotWait,
    ) {
        if self.role(partition) != ReplicaRole::AwaitingSnapshot {
            return;
        }
        if self.partition_map.primary(partition) == Some(self.node_id) {
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                state.set_sequence(sequence);
            }
            let since_ms = self.snapshot_stamp();
            self.incoming_snapshots.insert(
                partition,
                IncomingSnapshot {
                    state: IncomingState::CatchingUp {
                        asked_from: sequence + 1,
                    },
                    since_ms,
                    wait: SnapshotWait {
                        source: Some(source),
                        first_asked_ms: since_ms,
                        taking_primary: true,
                        replaced_replica: wait.replaced_replica,
                    },
                },
            );
            self.request_catchup(partition).await;
        } else {
            self.become_replica(partition, epoch, sequence);
            tracing::info!(
                ?partition,
                sequence,
                "replica now active after snapshot import"
            );
        }
    }

    pub(super) fn source_is_alive(&self, source: NodeId) -> bool {
        self.node_status(source) != NodeStatus::Dead
    }

    pub(super) fn snapshot_source(&self, partition: PartitionId) -> Option<NodeId> {
        if self.partition_map.primary(partition) == Some(self.node_id) {
            self.partition_map
                .replicas(partition)
                .iter()
                .copied()
                .find(|node| *node != self.node_id)
        } else {
            self.partition_map
                .primary(partition)
                .filter(|node| *node != self.node_id)
        }
    }

    pub(super) async fn await_snapshot(
        &mut self,
        partition: PartitionId,
        epoch: Epoch,
        source: NodeId,
        taking_primary: bool,
    ) {
        let wait = self
            .incoming_snapshots
            .get(&partition)
            .map(|incoming| incoming.wait)
            .unwrap_or_default();
        let kept_source = wait
            .source
            .filter(|asked| wait.taking_primary && self.source_is_alive(*asked));
        let source = kept_source.unwrap_or(source);
        self.replicas
            .entry(partition.get())
            .or_insert_with(|| {
                super::super::replication::ReplicaState::new(partition, self.node_id)
            })
            .become_awaiting_snapshot(epoch);
        self.stop_sending_change_events(partition);
        tracing::info!(
            ?partition,
            epoch = epoch.get(),
            source = source.get(),
            "waiting for a snapshot before taking the partition"
        );
        let next = SnapshotWait {
            source: Some(source),
            first_asked_ms: if wait.source == Some(source) {
                wait.first_asked_ms
            } else {
                None
            },
            taking_primary: taking_primary || kept_source.is_some(),
            replaced_replica: wait.replaced_replica,
        };
        self.send_snapshot_request(partition, next).await;
    }

    pub(super) fn remember_replaced_replica(
        &mut self,
        partition: PartitionId,
        replaced: Option<NodeId>,
    ) {
        if let Some(incoming) = self.incoming_snapshots.get_mut(&partition) {
            incoming.wait.replaced_replica = incoming.wait.replaced_replica.or(replaced);
        }
    }

    pub(super) fn replaced_replica(&self, partition: PartitionId) -> Option<NodeId> {
        self.incoming_snapshots
            .get(&partition)
            .and_then(|incoming| incoming.wait.replaced_replica)
    }

    pub(super) async fn refuse_unready_read(&self, from: NodeId, request: &JsonDbRequest) {
        tracing::info!(
            entity = %request.entity,
            requester = from.get(),
            "refusing a forwarded read for a partition this node does not hold yet"
        );
        let response = JsonDbResponse::new(
            request.request_id,
            Self::json_error(503, "partition is not ready on this node"),
            request.response_topic.clone(),
            request.correlation_data.clone(),
        );
        let _ = self
            .transport
            .send(from, ClusterMessage::JsonDbResponse(response))
            .await;
    }

    pub(super) async fn take_role_after_catchup(&mut self, partition: PartitionId) {
        let Some(epoch) = self.epoch(partition) else {
            return;
        };
        let sequence = self.sequence(partition).unwrap_or(0);
        self.become_primary(partition, epoch);
        self.primary_taken_after_wait = true;
        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.set_sequence(sequence);
        }
        tracing::info!(
            ?partition,
            sequence,
            "primary now active after snapshot import"
        );
        self.release_held_writes().await;
        self.serve_deferred_snapshots().await;
    }

    pub(super) fn catching_up_to_take_from(&self, partition: PartitionId) -> Option<NodeId> {
        if !self.waits_for_primary_data(partition) {
            return None;
        }
        let incoming = self.incoming_snapshots.get(&partition)?;
        incoming.wait.source.filter(|_| incoming.catching_up())
    }

    pub(crate) fn handle_snapshot_complete(&mut self, from: NodeId, complete: &SnapshotComplete) {
        let Some(partition) = complete.partition() else {
            tracing::warn!(?from, "received snapshot complete with invalid partition");
            return;
        };

        let status = complete.status();
        let final_sequence = complete.final_sequence();

        tracing::info!(
            ?partition,
            from = from.get(),
            ?status,
            final_sequence,
            "received snapshot complete"
        );

        if complete.is_ok() {
            if self.migration_manager.is_sending(partition)
                && let Err(e) = self
                    .migration_manager
                    .advance_phase(partition, MigrationPhase::Overlapping)
            {
                tracing::debug!(?partition, ?e, "could not advance migration phase");
            }
        } else {
            tracing::warn!(?partition, ?status, "snapshot transfer failed");
        }
    }

    pub(super) async fn request_catchup(&mut self, partition: PartitionId) {
        let taking_from = self.catching_up_to_take_from(partition);
        let target = if self.waits_for_primary_data(partition) {
            taking_from.filter(|source| self.source_is_alive(*source))
        } else {
            self.partition_map
                .primary(partition)
                .filter(|primary| *primary != self.node_id)
        };
        let wait = self
            .incoming_snapshots
            .get(&partition)
            .map(|incoming| incoming.wait)
            .filter(|_| taking_from.is_some())
            .unwrap_or_default();
        let overdue = wait.first_asked_ms.is_some_and(|since| {
            self.snapshot_clock_ms.saturating_sub(since) >= SNAPSHOT_STALL_TIMEOUT_MS
        });
        let Some(primary) = target.filter(|_| !overdue) else {
            if taking_from.is_some() {
                tracing::warn!(
                    ?partition,
                    source = ?taking_from,
                    "taking the primary role: the source did not finish catching this node up"
                );
                self.take_role_after_catchup(partition).await;
            } else {
                self.incoming_snapshots.remove(&partition);
            }
            return;
        };
        let since_ms = self.snapshot_stamp();
        let from_seq = self.sequence(partition).unwrap_or(0) + 1;
        self.incoming_snapshots.insert(
            partition,
            IncomingSnapshot {
                state: IncomingState::CatchingUp {
                    asked_from: from_seq,
                },
                since_ms,
                wait,
            },
        );
        let request = CatchupRequest::create(partition, from_seq, u64::MAX, self.node_id);
        if let Err(error) = self
            .transport
            .send(primary, ClusterMessage::CatchupRequest(request))
            .await
        {
            tracing::debug!(?partition, to = primary.get(), %error, "catchup request not sent");
        }
    }

    pub(crate) fn abandon_snapshot_request(&mut self, partition: PartitionId) {
        if !self
            .incoming_snapshots
            .get(&partition)
            .is_some_and(IncomingSnapshot::catching_up)
        {
            self.incoming_snapshots.remove(&partition);
        }
    }

    pub(super) fn catchup_asked_from(&self, partition: PartitionId) -> Option<u64> {
        match self.incoming_snapshots.get(&partition)?.state {
            IncomingState::CatchingUp { asked_from } => Some(asked_from),
            _ => None,
        }
    }

    pub(super) fn catchup_finished(&mut self, partition: PartitionId) {
        if self
            .incoming_snapshots
            .get(&partition)
            .is_some_and(IncomingSnapshot::catching_up)
        {
            self.incoming_snapshots.remove(&partition);
        }
    }

    pub(super) fn catchup_progressed(&mut self, partition: PartitionId) {
        let stamp = self.snapshot_stamp();
        if let Some(incoming) = self
            .incoming_snapshots
            .get_mut(&partition)
            .filter(|incoming| incoming.catching_up())
        {
            incoming.since_ms = stamp;
        }
    }

    pub async fn request_snapshot(&mut self, partition: PartitionId, from: NodeId) {
        let earlier = self
            .incoming_snapshots
            .get(&partition)
            .map(|incoming| incoming.wait)
            .unwrap_or_default();
        let next = SnapshotWait {
            source: Some(from),
            first_asked_ms: if earlier.source == Some(from) {
                earlier.first_asked_ms
            } else {
                None
            },
            taking_primary: self.partition_map.primary(partition) == Some(self.node_id),
            replaced_replica: earlier.replaced_replica,
        };
        self.send_snapshot_request(partition, next).await;
    }

    async fn send_snapshot_request(&mut self, partition: PartitionId, wait: SnapshotWait) {
        let Some(from) = wait.source else {
            return;
        };
        if self
            .incoming_snapshots
            .get(&partition)
            .is_some_and(IncomingSnapshot::catching_up)
        {
            self.incoming_snapshots.remove(&partition);
        }
        if let Some(existing) = self.incoming_snapshots.get_mut(&partition) {
            if existing.wait.source == Some(from) {
                tracing::debug!(?partition, "snapshot already requested or pending");
                existing.wait = wait;
                return;
            }
            tracing::info!(
                ?partition,
                old_source = ?existing.wait.source,
                source = from.get(),
                "snapshot source changed, abandoning earlier request"
            );
            self.incoming_snapshots.remove(&partition);
        }

        tracing::info!(?partition, source = from.get(), "requesting snapshot");

        let request = SnapshotRequest::create(partition, self.node_id);
        if let Err(error) = self
            .transport
            .send(from, ClusterMessage::SnapshotRequest(request))
            .await
        {
            tracing::warn!(?partition, source = from.get(), %error, "snapshot request not sent");
            self.back_off_snapshot_with(partition, wait);
            return;
        }

        self.stores.open_transfer_window(partition);
        let since_ms = self.snapshot_stamp();
        let first_asked_ms = wait.first_asked_ms.or(since_ms);
        self.incoming_snapshots.insert(
            partition,
            IncomingSnapshot {
                state: IncomingState::Requested {
                    source: from,
                    builder: None,
                },
                since_ms,
                wait: SnapshotWait {
                    first_asked_ms,
                    ..wait
                },
            },
        );
    }

    #[cfg(test)]
    pub(super) fn snapshot_asked_from(&self, partition: PartitionId) -> Option<NodeId> {
        self.incoming_snapshots.get(&partition)?.wait.source
    }

    #[cfg(test)]
    pub(super) fn snapshot_transfer_running(&self, partition: PartitionId) -> bool {
        self.incoming_snapshots
            .get(&partition)
            .is_some_and(|incoming| incoming.requested_from().is_some())
    }

    #[cfg(test)]
    pub(super) fn snapshot_in_progress(&self, partition: PartitionId) -> bool {
        self.incoming_snapshots
            .get(&partition)
            .is_some_and(IncomingSnapshot::building)
    }

    #[cfg(test)]
    pub(super) fn snapshot_request_stalled(&mut self, partition: PartitionId) {
        if let Some(incoming) = self.incoming_snapshots.get_mut(&partition) {
            incoming.since_ms = Some(0);
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_waited_since(&mut self, partition: PartitionId, asked_ms: u64) {
        if let Some(incoming) = self.incoming_snapshots.get_mut(&partition) {
            incoming.wait.first_asked_ms = Some(asked_ms);
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_first_asked(&self, partition: PartitionId) -> Option<u64> {
        self.incoming_snapshots.get(&partition)?.wait.first_asked_ms
    }
}
