// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeController;
use crate::cluster::protocol::{CatchupResponse, ReplicationWrite};
use crate::cluster::replication::{ReplicaRole, ReplicaState};
use crate::cluster::transport::{ClusterMessage, ClusterTransport};
use crate::cluster::{NodeId, PartitionId};

const CATCHUP_RESPONSE_MAX_BYTES: usize = 8 * 1024 * 1024;
const WRITE_FRAMING_BYTES: usize = 16;

impl<T: ClusterTransport> NodeController<T> {
    pub(super) async fn handle_catchup_request(
        &self,
        from: NodeId,
        partition: PartitionId,
        from_seq: u64,
        to_seq: u64,
    ) {
        let oldest_seq = self.write_log.oldest_sequence(partition);
        let newest_seq = self.write_log.newest_sequence(partition);
        tracing::info!(
            ?partition,
            from_seq,
            to_seq,
            ?oldest_seq,
            ?newest_seq,
            log_entries = self.write_log.entry_count(partition),
            requester = from.get(),
            "received catchup request"
        );

        let newest_held = to_seq.min(self.sequence(partition).unwrap_or(0));
        let writes = self.write_log.get_range(partition, from_seq, newest_held);

        tracing::info!(
            ?partition,
            write_count = writes.len(),
            first_seq = writes.first().map(|w| w.sequence),
            last_seq = writes.last().map(|w| w.sequence),
            "sending catchup response"
        );

        let nothing_to_send = writes.is_empty();
        let all_sent = self.send_catchup_writes(from, partition, writes).await;
        if nothing_to_send || (all_sent && to_seq == u64::MAX) {
            self.send_catchup_piece(from, partition, Vec::new()).await;
        }
    }

    pub(super) async fn send_catchup_writes(
        &self,
        to: NodeId,
        partition: PartitionId,
        writes: Vec<ReplicationWrite>,
    ) -> bool {
        for piece in catchup_pieces(writes) {
            if !self.send_catchup_piece(to, partition, piece).await {
                return false;
            }
        }
        true
    }

    async fn send_catchup_piece(
        &self,
        to: NodeId,
        partition: PartitionId,
        writes: Vec<ReplicationWrite>,
    ) -> bool {
        let resp = CatchupResponse::create(partition, self.node_id, writes);
        match self
            .transport
            .send(to, ClusterMessage::CatchupResponse(resp))
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::debug!(?partition, to = to.get(), %error, "catchup response not sent");
                false
            }
        }
    }

    pub(super) async fn handle_catchup_response(&mut self, resp: CatchupResponse) {
        let partition = resp.partition;
        let responder = resp.responder_id;

        if self.role(partition) != ReplicaRole::Replica {
            tracing::debug!(
                ?partition,
                role = ?self.role(partition),
                "ignoring catchup response - not a replica"
            );
            return;
        }

        tracing::debug!(
            ?partition,
            write_count = resp.writes.len(),
            "received catchup response"
        );

        if resp.writes.is_empty() {
            let needs_snapshot = self
                .replicas
                .get(&partition.get())
                .is_some_and(ReplicaState::has_gap);
            if needs_snapshot {
                tracing::info!(
                    ?partition,
                    "catchup response empty but replica has gap - switching to awaiting snapshot"
                );
                self.catch_up_by_snapshot(partition, responder).await;
            } else {
                self.catchup_finished(partition);
            }
            return;
        }

        let mut writes: Vec<_> = resp.writes;
        writes.sort_by_key(|w| w.sequence);

        let mut applied_count = 0;
        let mut rejected_count = 0;
        for write in &writes {
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                let sequence_before = state.sequence();
                if write.epoch <= state.epoch() && write.sequence <= sequence_before {
                    continue;
                }
                let ack = state.handle_write(write);
                if ack.is_ok() {
                    if let Err(e) = self.apply_replicated_write(write) {
                        tracing::error!(?partition, ?e, "failed to apply catchup write to stores");
                        if let Some(state) = self.replicas.get_mut(&partition.get()) {
                            state.set_sequence(sequence_before);
                        }
                        rejected_count += 1;
                        break;
                    }
                    self.sync_retained_to_broker(write).await;
                    self.write_log
                        .append(partition, write.sequence, write.clone());
                    applied_count += 1;
                } else {
                    rejected_count += 1;
                }
            }
        }

        tracing::debug!(
            ?partition,
            applied_count,
            rejected_count,
            "catchup response processed"
        );

        if rejected_count > 0 {
            tracing::info!(
                ?partition,
                rejected_count,
                "catchup writes rejected - switching to awaiting snapshot"
            );
            self.catch_up_by_snapshot(partition, responder).await;
        } else {
            self.catchup_progressed(partition);
        }
    }

    async fn catch_up_by_snapshot(&mut self, partition: PartitionId, source: NodeId) {
        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            let epoch = state.epoch();
            state.become_awaiting_snapshot(epoch);
        }
        self.request_snapshot(partition, source).await;
    }
}

pub(super) fn catchup_pieces(writes: Vec<ReplicationWrite>) -> Vec<Vec<ReplicationWrite>> {
    let mut pieces = Vec::new();
    let mut piece = Vec::new();
    let mut piece_bytes = 0;
    for write in writes {
        let write_bytes = write.to_bytes().map_or(0, |bytes| bytes.len()) + WRITE_FRAMING_BYTES;
        if !piece.is_empty() && piece_bytes + write_bytes > CATCHUP_RESPONSE_MAX_BYTES {
            pieces.push(std::mem::take(&mut piece));
            piece_bytes = 0;
        }
        piece_bytes += write_bytes;
        piece.push(write);
    }
    if !piece.is_empty() {
        pieces.push(piece);
    }
    pieces
}
