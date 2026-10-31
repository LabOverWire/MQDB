// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::migration::MigrationPhase;
use super::super::protocol::{JsonDbRequest, JsonDbResponse};
use super::super::replication::ReplicaRole;
use super::super::snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{Epoch, NUM_PARTITIONS, NodeId, PartitionId};
use super::NodeController;
use std::time::{Duration, Instant};

pub(super) const SNAPSHOT_RETRY_AFTER: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotRequestState {
    pub(crate) source: NodeId,
    pub(crate) last_activity: Instant,
}

#[derive(Debug)]
pub(crate) struct SnapshotRequests([Option<SnapshotRequestState>; NUM_PARTITIONS as usize]);

impl SnapshotRequests {
    pub(crate) fn new() -> Self {
        Self([None; NUM_PARTITIONS as usize])
    }

    pub(crate) fn get(&self, partition: &PartitionId) -> Option<&SnapshotRequestState> {
        self.0[usize::from(partition.get())].as_ref()
    }

    pub(crate) fn get_mut(&mut self, partition: &PartitionId) -> Option<&mut SnapshotRequestState> {
        self.0[usize::from(partition.get())].as_mut()
    }

    pub(crate) fn contains_key(&self, partition: &PartitionId) -> bool {
        self.get(partition).is_some()
    }

    pub(crate) fn insert(&mut self, partition: PartitionId, request: SnapshotRequestState) {
        self.0[usize::from(partition.get())] = Some(request);
    }

    pub(crate) fn remove(&mut self, partition: &PartitionId) {
        self.0[usize::from(partition.get())] = None;
    }
}

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) async fn handle_snapshot_request(&mut self, from: NodeId, req: &SnapshotRequest) {
        let Some(partition) = req.partition() else {
            tracing::warn!(?from, "received snapshot request with invalid partition");
            return;
        };

        tracing::info!(
            ?partition,
            requester = from.get(),
            "received snapshot request, starting snapshot send"
        );

        let data = self.stores.export_partition(partition);
        let sequence = self.sequence(partition).unwrap_or(0);
        let sender = SnapshotSender::new(partition, data, sequence);

        self.outgoing_snapshots.insert((from, partition), sender);
        self.send_next_snapshot_chunk(from, partition).await;
    }

    async fn send_next_snapshot_chunk(&mut self, to: NodeId, partition: PartitionId) {
        let Some(sender) = self.outgoing_snapshots.get_mut(&(to, partition)) else {
            return;
        };

        if let Some(chunk) = sender.next_chunk() {
            let is_last = chunk.is_last();
            let _ = self
                .transport
                .send(to, ClusterMessage::SnapshotChunk(chunk))
                .await;

            if is_last {
                self.outgoing_snapshots.remove(&(to, partition));
            }
        }
    }

    pub(crate) async fn handle_snapshot_chunk(&mut self, from: NodeId, chunk: &SnapshotChunk) {
        let partition = chunk.partition;

        tracing::debug!(
            ?partition,
            from = from.get(),
            chunk_index = chunk.chunk_index,
            total_chunks = chunk.total_chunks,
            "received snapshot chunk"
        );

        if let Some(request) = self.requested_snapshots.get_mut(&partition) {
            if request.source != from {
                tracing::debug!(
                    ?partition,
                    from = from.get(),
                    source = request.source.get(),
                    "ignoring snapshot chunk from a node this snapshot was not requested from"
                );
                return;
            }
            request.last_activity = Instant::now();
        }

        let builder = self.pending_snapshots.entry(partition).or_insert_with(|| {
            SnapshotBuilder::new(partition, chunk.total_chunks, chunk.sequence_at_snapshot)
        });

        if !builder.add_chunk(chunk) {
            tracing::warn!(?partition, "failed to add snapshot chunk");
            let complete = SnapshotComplete::failed(partition);
            let _ = self
                .transport
                .send(from, ClusterMessage::SnapshotComplete(complete))
                .await;
            self.pending_snapshots.remove(&partition);
            return;
        }

        if builder.is_complete() {
            let sequence = builder.sequence_at_snapshot();
            let Some(data) = self
                .pending_snapshots
                .remove(&partition)
                .and_then(SnapshotBuilder::assemble)
            else {
                tracing::error!(?partition, "failed to assemble snapshot");
                let complete = SnapshotComplete::failed(partition);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::SnapshotComplete(complete))
                    .await;
                return;
            };

            let saved_events = self.set_aside_change_events(partition);
            match self.stores.import_partition(&data) {
                Ok(count) => {
                    self.imported_partitions[usize::from(partition.get())] = true;
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

                    self.requested_snapshots.remove(&partition);
                    self.reload_change_events(partition);
                    if let Some(epoch) = self.epoch(partition) {
                        self.take_role_after_import(partition, epoch, sequence);
                    }

                    let complete = SnapshotComplete::ok(partition, sequence);
                    let _ = self
                        .transport
                        .send(from, ClusterMessage::SnapshotComplete(complete))
                        .await;
                }
                Err(e) => {
                    tracing::error!(?partition, ?e, "failed to import snapshot");
                    self.restore_change_events(partition, &saved_events);
                    let complete = SnapshotComplete::failed(partition);
                    let _ = self
                        .transport
                        .send(from, ClusterMessage::SnapshotComplete(complete))
                        .await;
                }
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

    fn take_role_after_import(&mut self, partition: PartitionId, epoch: Epoch, sequence: u64) {
        if self.role(partition) != ReplicaRole::AwaitingSnapshot {
            return;
        }
        if self.partition_map.primary(partition) == Some(self.node_id) {
            self.become_primary(partition, epoch);
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                state.set_sequence(sequence);
            }
            tracing::info!(
                ?partition,
                sequence,
                "primary now active after snapshot import"
            );
        } else {
            self.become_replica(partition, epoch, sequence);
            tracing::info!(
                ?partition,
                sequence,
                "replica now active after snapshot import"
            );
        }
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
    ) {
        self.replicas
            .entry(partition.get())
            .or_insert_with(|| {
                super::super::replication::ReplicaState::new(partition, self.node_id)
            })
            .become_awaiting_snapshot(epoch);
        self.stop_sending_change_events(partition);
        if self
            .requested_snapshots
            .get(&partition)
            .is_some_and(|request| request.source != source)
        {
            self.requested_snapshots.remove(&partition);
            self.pending_snapshots.remove(&partition);
        }
        tracing::info!(
            ?partition,
            epoch = epoch.get(),
            source = source.get(),
            "waiting for a snapshot before taking the partition"
        );
        self.request_snapshot(partition, source).await;
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

    pub(super) async fn retry_stalled_snapshots(&mut self) {
        for partition in PartitionId::all() {
            let stalled = self
                .requested_snapshots
                .get(&partition)
                .is_some_and(|request| request.last_activity.elapsed() >= SNAPSHOT_RETRY_AFTER);
            if !stalled {
                continue;
            }
            self.requested_snapshots.remove(&partition);
            self.pending_snapshots.remove(&partition);
            if self.role(partition) != ReplicaRole::AwaitingSnapshot {
                continue;
            }
            if let Some(source) = self.snapshot_source(partition) {
                tracing::info!(
                    ?partition,
                    source = source.get(),
                    "snapshot stalled, requesting again"
                );
                self.request_snapshot(partition, source).await;
            }
        }
    }

    pub async fn request_snapshot(&mut self, partition: PartitionId, from: NodeId) {
        if self.requested_snapshots.contains_key(&partition)
            || self.pending_snapshots.contains_key(&partition)
        {
            tracing::debug!(?partition, "snapshot already requested or pending");
            return;
        }

        tracing::info!(?partition, source = from.get(), "requesting snapshot");
        self.requested_snapshots.insert(
            partition,
            SnapshotRequestState {
                source: from,
                last_activity: Instant::now(),
            },
        );

        let request = SnapshotRequest::create(partition, self.node_id);
        let _ = self
            .transport
            .send(from, ClusterMessage::SnapshotRequest(request))
            .await;
    }
}
