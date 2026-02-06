use super::super::migration::MigrationPhase;
use super::super::snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{NodeId, PartitionId};
use super::NodeController;

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
            self.requested_snapshots.remove(&partition);
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
                self.requested_snapshots.remove(&partition);
                let complete = SnapshotComplete::failed(partition);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::SnapshotComplete(complete))
                    .await;
                return;
            };

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
                        state.become_replica(current_epoch, sequence);
                        tracing::info!(
                            ?partition,
                            sequence,
                            "replica now active after snapshot import"
                        );
                    }

                    self.requested_snapshots.remove(&partition);

                    let complete = SnapshotComplete::ok(partition, sequence);
                    let _ = self
                        .transport
                        .send(from, ClusterMessage::SnapshotComplete(complete))
                        .await;
                }
                Err(e) => {
                    tracing::error!(?partition, ?e, "failed to import snapshot");
                    self.requested_snapshots.remove(&partition);
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

    pub async fn request_snapshot(&mut self, partition: PartitionId, from: NodeId) {
        if self.requested_snapshots.contains(&partition)
            || self.pending_snapshots.contains_key(&partition)
        {
            tracing::debug!(?partition, "snapshot already requested or pending");
            return;
        }

        tracing::info!(?partition, source = from.get(), "requesting snapshot");
        self.requested_snapshots.insert(partition);

        let request = SnapshotRequest::create(partition, self.node_id);
        let _ = self
            .transport
            .send(from, ClusterMessage::SnapshotRequest(request))
            .await;
    }
}
