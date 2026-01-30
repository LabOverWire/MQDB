use super::NodeController;
use crate::cluster::protocol::CatchupResponse;
use crate::cluster::replication::{ReplicaRole, ReplicaState};
use crate::cluster::transport::{ClusterMessage, ClusterTransport};
use crate::cluster::{NodeId, PartitionId};

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

        let writes = if self.write_log.can_catchup(partition, from_seq) {
            self.write_log.get_range(partition, from_seq, to_seq)
        } else {
            tracing::warn!(
                ?partition,
                from_seq,
                "cannot serve catchup - data evicted from log"
            );
            Vec::new()
        };

        tracing::info!(
            ?partition,
            write_count = writes.len(),
            first_seq = writes.first().map(|w| w.sequence),
            last_seq = writes.last().map(|w| w.sequence),
            "sending catchup response"
        );

        let resp = CatchupResponse::create(partition, self.node_id, writes);
        let _ = self
            .transport
            .send(from, ClusterMessage::CatchupResponse(resp))
            .await;
    }

    pub(super) async fn handle_catchup_response(&mut self, resp: CatchupResponse) {
        let partition = resp.partition;
        let responder = resp.responder_id;

        if self
            .replicas
            .get(&partition.get())
            .is_some_and(|s| s.role() == ReplicaRole::AwaitingSnapshot)
        {
            tracing::debug!(
                ?partition,
                "ignoring catchup response - replica awaiting snapshot"
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
                if let Some(state) = self.replicas.get_mut(&partition.get()) {
                    let epoch = state.epoch();
                    state.become_awaiting_snapshot(epoch);
                }
                self.request_snapshot(partition, responder).await;
            }
            return;
        }

        let mut writes: Vec<_> = resp.writes;
        writes.sort_by_key(|w| w.sequence);

        let replica_seq = self
            .replicas
            .get(&partition.get())
            .map_or(0, ReplicaState::sequence);
        let min_write_seq = writes.first().map_or(0, |w| w.sequence);

        if min_write_seq > replica_seq + 1000 {
            tracing::info!(
                ?partition,
                replica_seq,
                min_write_seq,
                "catchup data too far ahead of replica - switching to awaiting snapshot"
            );
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                let epoch = state.epoch();
                state.become_awaiting_snapshot(epoch);
            }
            self.request_snapshot(partition, responder).await;
            return;
        }

        let mut applied_count = 0;
        let mut rejected_count = 0;
        for write in &writes {
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                let ack = state.handle_write(write);
                if ack.is_ok() {
                    if let Err(e) = self.stores.apply_write(write) {
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
    }
}
