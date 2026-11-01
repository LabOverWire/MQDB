// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeController;
use crate::cluster::protocol::{CatchupResponse, ReplicationWrite};
use crate::cluster::replication::{ReplicaRole, ReplicaState};
use crate::cluster::transport::{ClusterMessage, ClusterTransport};
use crate::cluster::{NodeId, PartitionId};

const CATCHUP_RESPONSE_MAX_BYTES: usize = 8 * 1024 * 1024;
const WRITE_OVERHEAD_BYTES: usize = 64;

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

        let writes = limit_catchup_bytes(self.write_log.get_range(partition, from_seq, to_seq));

        tracing::info!(
            ?partition,
            write_count = writes.len(),
            first_seq = writes.first().map(|w| w.sequence),
            last_seq = writes.last().map(|w| w.sequence),
            "sending catchup response"
        );

        let resp = CatchupResponse::create(partition, self.node_id, writes);
        if let Err(error) = self
            .transport
            .send(from, ClusterMessage::CatchupResponse(resp))
            .await
        {
            tracing::debug!(?partition, to = from.get(), %error, "catchup response not sent");
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
                self.catchup_answered(partition, true).await;
            }
            return;
        }

        let mut writes: Vec<_> = resp.writes;
        writes.sort_by_key(|w| w.sequence);

        let mut applied_count = 0;
        let mut rejected_count = 0;
        for write in &writes {
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                let ack = state.handle_write(write);
                if ack.is_ok() {
                    if let Err(e) = self.apply_replicated_write(write) {
                        tracing::error!(?partition, ?e, "failed to apply catchup write to stores");
                    } else {
                        self.sync_retained_to_broker(write).await;
                    }
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
            self.catchup_answered(partition, false).await;
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

pub(super) fn limit_catchup_bytes(mut writes: Vec<ReplicationWrite>) -> Vec<ReplicationWrite> {
    let mut total = 0;
    let within_limit = writes
        .iter()
        .take_while(|write| {
            total += write.data.len() + write.entity.len() + write.id.len() + WRITE_OVERHEAD_BYTES;
            total <= CATCHUP_RESPONSE_MAX_BYTES
        })
        .count();
    writes.truncate(within_limit.max(1));
    writes
}
