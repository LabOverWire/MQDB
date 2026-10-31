// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::migration::MigrationPhase;
use super::super::replication::ReplicaRole;
use super::super::snapshot::{
    ChunkOutcome, SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
};
use super::super::transport::{ClusterMessage, ClusterTransport, TransportError};
use super::super::{NodeId, PartitionId};
use super::NodeController;
use std::collections::HashSet;

pub(crate) const SNAPSHOT_CHUNKS_PER_PUMP: u32 = 8;
pub(crate) const SNAPSHOT_STALL_TIMEOUT_MS: u64 = 30_000;
pub(crate) const SNAPSHOT_RETRY_BACKOFF_MS: u64 = 5_000;

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
}

#[derive(Debug)]
pub(crate) struct IncomingSnapshot {
    state: IncomingState,
    since_ms: Option<u64>,
}

impl IncomingSnapshot {
    fn requested_from(&self) -> Option<NodeId> {
        match self.state {
            IncomingState::Requested { source, .. } => Some(source),
            IncomingState::BackingOff => None,
        }
    }

    fn expired(&self, now: u64) -> bool {
        let Some(since) = self.since_ms else {
            return false;
        };
        let limit = match self.state {
            IncomingState::Requested { .. } => SNAPSHOT_STALL_TIMEOUT_MS,
            IncomingState::BackingOff => SNAPSHOT_RETRY_BACKOFF_MS,
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
        self.pump_outgoing_snapshots().await;
    }

    fn export_snapshot(&self, partition: PartitionId) -> Option<SnapshotSender> {
        let data = self.stores.export_partition(partition);
        let sequence = self.sequence(partition).unwrap_or(0);
        let transfer_id = uuid::Uuid::new_v4().as_u64_pair().0;
        let sender = SnapshotSender::new(partition, data, sequence)?.with_transfer_id(transfer_id);
        tracing::info!(
            ?partition,
            total_chunks = sender.total_chunks(),
            transfer_id,
            "exported partition snapshot"
        );
        Some(sender)
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
                let exported = self.export_snapshot(partition);
                if exported.is_none() {
                    tracing::warn!(?partition, to = to.get(), "partition too large to send");
                    self.outgoing_snapshots.remove(index);
                    continue;
                }
                self.outgoing_snapshots[index].sender = exported;
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
        }
        let replicas = &self.replicas;
        let partition_map = &self.partition_map;
        self.incoming_snapshots.retain(|partition, incoming| {
            if incoming.expired(now) {
                if incoming.requested_from().is_some() {
                    tracing::warn!(?partition, "snapshot transfer stalled, abandoning request");
                }
                return false;
            }
            let awaiting = replicas
                .get(&partition.get())
                .is_some_and(|state| state.role() == ReplicaRole::AwaitingSnapshot);
            let moved = incoming
                .requested_from()
                .zip(partition_map.primary(*partition))
                .is_some_and(|(source, primary)| source != primary);
            if awaiting && moved {
                tracing::info!(
                    ?partition,
                    "partition primary moved, abandoning snapshot request"
                );
                return false;
            }
            true
        });

        for partition in PartitionId::all() {
            if self.role(partition) != ReplicaRole::AwaitingSnapshot
                || self.incoming_snapshots.contains_key(&partition)
            {
                continue;
            }
            let Some(primary) = self.partition_map.primary(partition) else {
                continue;
            };
            if primary == self.node_id {
                continue;
            }
            tracing::info!(
                ?partition,
                primary = primary.get(),
                "retrying snapshot request for awaiting replica"
            );
            self.request_snapshot(partition, primary).await;
        }

        self.pump_outgoing_snapshots().await;
    }

    fn back_off_snapshot(&mut self, partition: PartitionId) {
        let since_ms = self.snapshot_stamp();
        self.incoming_snapshots.insert(
            partition,
            IncomingSnapshot {
                state: IncomingState::BackingOff,
                since_ms,
            },
        );
    }

    async fn refuse_snapshot(&mut self, from: NodeId, partition: PartitionId) {
        self.back_off_snapshot(partition);
        let complete = SnapshotComplete::failed(partition);
        if let Err(error) = self
            .transport
            .send(from, ClusterMessage::SnapshotComplete(complete))
            .await
        {
            tracing::debug!(?partition, to = from.get(), %error, "snapshot failure notice not sent");
        }
    }

    pub(crate) fn snapshots_node_gone(&mut self, node: NodeId) {
        self.outgoing_snapshots
            .retain(|outgoing| outgoing.to != node);
        self.incoming_snapshots
            .retain(|_, incoming| incoming.requested_from() != Some(node));
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
        self.incoming_snapshots.remove(&partition);

        let sequence = finished.sequence_at_snapshot();
        let data = finished.into_data();

        let saved_events = self.set_aside_change_events(partition);
        match self.stores.import_partition(&data) {
            Ok(count) => {
                tracing::info!(
                    ?partition,
                    imported = count,
                    sequence,
                    "snapshot import complete"
                );

                if let Err(e) = self
                    .migration_manager
                    .mark_snapshot_complete(partition, sequence)
                {
                    tracing::debug!(?partition, ?e, "no active migration for snapshot");
                }

                if let Some(state) = self.replicas.get_mut(&partition.get()) {
                    let current_epoch = state.epoch();
                    let is_primary = self.partition_map.primary(partition) == Some(self.node_id);
                    if is_primary {
                        state.become_primary(current_epoch);
                        state.set_sequence(sequence);
                        tracing::info!(
                            ?partition,
                            sequence,
                            "primary now active after snapshot import"
                        );
                    } else {
                        state.become_replica(current_epoch, sequence);
                        tracing::info!(
                            ?partition,
                            sequence,
                            "replica now active after snapshot import"
                        );
                    }
                }

                self.reload_change_events(partition);
                if self.role(partition) == ReplicaRole::Primary {
                    self.start_sending_change_events(partition);
                }

                let complete = SnapshotComplete::ok(partition, sequence);
                if let Err(error) = self
                    .transport
                    .send(from, ClusterMessage::SnapshotComplete(complete))
                    .await
                {
                    tracing::debug!(?partition, to = from.get(), %error, "snapshot completion notice not sent");
                }
            }
            Err(e) => {
                tracing::error!(?partition, ?e, "failed to import snapshot");
                self.restore_change_events(partition, &saved_events);
                self.refuse_snapshot(from, partition).await;
            }
        }
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

    pub async fn request_snapshot(&mut self, partition: PartitionId, from: NodeId) {
        if let Some(existing) = self.incoming_snapshots.get(&partition) {
            match existing.requested_from() {
                Some(source) if source != from => {
                    tracing::info!(
                        ?partition,
                        old_source = source.get(),
                        source = from.get(),
                        "snapshot source changed, abandoning earlier request"
                    );
                    self.incoming_snapshots.remove(&partition);
                }
                _ => {
                    tracing::debug!(?partition, "snapshot already requested or pending");
                    return;
                }
            }
        }

        tracing::info!(?partition, source = from.get(), "requesting snapshot");

        let request = SnapshotRequest::create(partition, self.node_id);
        if let Err(error) = self
            .transport
            .send(from, ClusterMessage::SnapshotRequest(request))
            .await
        {
            tracing::warn!(?partition, source = from.get(), %error, "snapshot request not sent");
            self.back_off_snapshot(partition);
            return;
        }

        let since_ms = self.snapshot_stamp();
        self.incoming_snapshots.insert(
            partition,
            IncomingSnapshot {
                state: IncomingState::Requested {
                    source: from,
                    builder: None,
                },
                since_ms,
            },
        );
    }
}
