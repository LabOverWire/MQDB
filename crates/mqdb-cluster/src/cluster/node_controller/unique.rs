// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId, PendingUniqueReserve,
    ReserveFailure, UniqueCheckPhase1Result, UniqueCommitRequest, UniqueReassertRequest,
    UniqueReleaseRequest, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
    UniqueSealRequest, UniqueSealResponse,
};
use crate::cluster::Epoch;
use tokio::sync::oneshot;

const UNIQUE_RESERVE_TIMEOUT_SECS: u64 = 5;
const UNIQUE_QUORUM_TIMEOUT_MS: u64 = 5000;
const UNIQUE_SEAL_TIMEOUT_MS: u64 = 5000;
/// Minimum spacing between single-node unique-voter changes. Longer than the reserve quorum timeout
/// so any reservation outstanding across one voter change has committed (or failed) before the next,
/// keeping every single add/remove within the "one uncarried change preserves the majority" bound.
pub(crate) const UNIQUE_VOTER_CHANGE_INTERVAL_MS: u64 = 2 * UNIQUE_QUORUM_TIMEOUT_MS;

/// One record/field reassert the reconciler will apply: enough to re-check the durable record and
/// route the reassert to the value-partition primary.
pub(crate) struct UniqueReassertWork {
    pub entity: String,
    pub field: String,
    pub value: Vec<u8>,
    pub record_id: String,
    pub data_partition: PartitionId,
}

/// A durable record this node owns, gathered by the reconciler's first pass WITHOUT parsing it.
/// The JSON parse (the scan's dominant cost) is deferred to `project_unique_reconcile_work` so it
/// runs in bounded chunks instead of under one long read-lock hold.
pub(crate) struct UniqueReassertKey {
    pub entity: String,
    pub record_id: String,
    pub data_partition: PartitionId,
}

pub async fn await_unique_reserves(
    pending: Vec<PendingUniqueReserve>,
) -> Result<Vec<(String, Vec<u8>, NodeId)>, (ReserveFailure, Vec<(String, Vec<u8>, NodeId)>)> {
    let mut confirmed: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

    for pending_reserve in pending {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_RESERVE_TIMEOUT_SECS);

        let result = tokio::time::timeout_at(deadline, pending_reserve.receiver).await;

        match result {
            Ok(Ok(UniqueReserveStatus::Reserved | UniqueReserveStatus::AlreadyReserved)) => {
                confirmed.push((
                    pending_reserve.field,
                    pending_reserve.value,
                    pending_reserve.target_node,
                ));
            }
            // A remote conflict is a permanent 409; a remote error, timeout, or dropped channel is a
            // transient not-durable failure the client should retry (503).
            Ok(Ok(UniqueReserveStatus::Conflict)) => {
                return Err((ReserveFailure::Conflict(pending_reserve.field), confirmed));
            }
            _ => {
                return Err((ReserveFailure::NotDurable(pending_reserve.field), confirmed));
            }
        }
    }

    Ok(confirmed)
}

/// Await majority-durability for every locally reserved unique value. Returns the fields now
/// confirmed majority-durable on success, or `Err(field)` for the first reserve that failed to
/// reach a group majority before its deadline (fail-closed).
pub async fn await_unique_quorum(
    pending: Vec<(String, oneshot::Receiver<bool>)>,
) -> Result<Vec<String>, String> {
    let mut durable = Vec::with_capacity(pending.len());
    for (field, receiver) in pending {
        match receiver.await {
            Ok(true) => durable.push(field),
            _ => return Err(field),
        }
    }
    Ok(durable)
}

impl<T: ClusterTransport> NodeController<T> {
    #[allow(clippy::missing_errors_doc)]
    pub async fn start_unique_constraint_check(
        &mut self,
        entity: &str,
        id: &str,
        data: &serde_json::Value,
        data_partition: PartitionId,
        request_id: &str,
        now_ms: u64,
    ) -> Result<UniqueCheckPhase1Result, ReserveFailure> {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let mut pending_remote: Vec<PendingUniqueReserve> = Vec::new();
        let mut pending_quorum: Vec<(String, oneshot::Receiver<bool>)> = Vec::new();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            let reserve_params = super::super::db::UniqueReserveParams {
                entity,
                field,
                value: &value,
                record_id: id,
                request_id,
                data_partition,
                ttl_ms: 30_000,
            };

            let failure: Option<ReserveFailure> = if primary == Some(self.node_id) {
                if self.unique_partition_sealed(unique_part) {
                    let (result, write) = self
                        .stores
                        .unique_reserve_replicated(&reserve_params, now_ms);

                    match result {
                        super::super::db::ReserveResult::Reserved => {
                            if let Some(w) = write {
                                self.sync_unique_write();
                                let rx = self.replicate_unique_reserve(w).await;
                                pending_quorum.push((field.clone(), rx));
                            }
                            local_reserved.push((field.clone(), value));
                            None
                        }
                        super::super::db::ReserveResult::AlreadyReservedBySameRequest => {
                            local_reserved.push((field.clone(), value));
                            None
                        }
                        super::super::db::ReserveResult::Conflict => {
                            Some(ReserveFailure::Conflict(field.clone()))
                        }
                    }
                } else {
                    tracing::warn!(
                        field,
                        partition = unique_part.get(),
                        "unique partition not sealed at the current epoch; failing reserve closed"
                    );
                    Some(ReserveFailure::NotDurable(field.clone()))
                }
            } else if let Some(target_node) = primary {
                let receiver = self
                    .send_unique_reserve_request_async(target_node, &reserve_params)
                    .await;

                match receiver {
                    Ok(rx) => {
                        pending_remote.push(PendingUniqueReserve {
                            field: field.clone(),
                            value,
                            target_node,
                            receiver: rx,
                        });
                        None
                    }
                    Err(_) => Some(ReserveFailure::NotDurable(field.clone())),
                }
            } else {
                Some(ReserveFailure::NotDurable(field.clone()))
            };

            if let Some(failure) = failure {
                for (f, v) in &local_reserved {
                    if let Some(w) = self
                        .stores
                        .unique_release_replicated(entity, f, v, request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for pending in pending_remote {
                    drop(pending.receiver);
                }
                return Err(failure);
            }
        }

        Ok(UniqueCheckPhase1Result {
            local_reserved,
            pending_remote,
            pending_quorum,
        })
    }

    pub async fn release_unique_check_reservations(
        &mut self,
        entity: &str,
        request_id: &str,
        local_reserved: &[(String, Vec<u8>)],
        confirmed_remotes: &[(String, Vec<u8>, NodeId)],
    ) {
        for (f, v) in local_reserved {
            if let Some(w) = self
                .stores
                .unique_release_replicated(entity, f, v, request_id)
            {
                self.write_or_forward(w).await;
            }
        }
        for (f, v, target) in confirmed_remotes {
            self.send_unique_release_fire_and_forget(*target, entity, f, v, request_id, None)
                .await;
        }
    }

    #[allow(clippy::too_many_arguments, clippy::missing_errors_doc)]
    pub async fn check_unique_constraints(
        &mut self,
        entity: &str,
        id: &str,
        data: &serde_json::Value,
        data_partition: PartitionId,
        request_id: &str,
        now_ms: u64,
    ) -> Result<(), String> {
        let phase1 = self
            .start_unique_constraint_check(entity, id, data, data_partition, request_id, now_ms)
            .await
            .map_err(|f| f.field().to_string())?;

        if !phase1.pending_remote.is_empty() {
            let remote_results = await_unique_reserves(phase1.pending_remote).await;
            match remote_results {
                Ok(confirmed_remotes) => {
                    for (f, v, target) in &confirmed_remotes {
                        self.send_unique_commit_fire_and_forget(*target, entity, f, v, request_id)
                            .await;
                    }
                }
                Err((failure, confirmed)) => {
                    self.release_unique_check_reservations(
                        entity,
                        request_id,
                        &phase1.local_reserved,
                        &confirmed,
                    )
                    .await;
                    return Err(failure.field().to_string());
                }
            }
        }

        Ok(())
    }

    pub async fn commit_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        _data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Ok((_, w)) = self
                    .stores
                    .unique_commit_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                    self.sync_unique_write();
                }
            } else if let Some(target_node) = primary {
                self.send_unique_commit_fire_and_forget(
                    target_node,
                    entity,
                    field,
                    &value,
                    request_id,
                )
                .await;
            }
        }
    }

    /// Release every unique claim held by a record being deleted, so the value is reusable. The
    /// data-partition is derived from the record id and the record id is the release key (committed
    /// claims are keyed by `record_id`). A no-op if the entity has no unique fields. Call at every
    /// record-delete site — direct, binary, cascade parent, and cascade child.
    pub(crate) async fn release_unique_for_deleted_record(
        &mut self,
        entity: &str,
        id: &str,
        data: &serde_json::Value,
    ) {
        let partition = super::super::db::data_partition(entity, id);
        self.release_unique_constraints(entity, id, data, partition, id, Self::current_time_ms())
            .await;
    }

    pub async fn release_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let epoch = self.partition_map.epoch(data_partition).get();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Some(w) = self
                    .stores
                    .unique_release_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
                // Releasing a committed claim raises the value site's fence so a stale reassert from
                // a superseded data-partition primary is rejected (see ClusterUniqueReconcilerFence).
                self.stores.db_unique.fence_bump(data_partition, epoch);
            } else if let Some(target_node) = primary {
                self.send_unique_release_fire_and_forget(
                    target_node,
                    entity,
                    field,
                    &value,
                    request_id,
                    Some(data_partition),
                )
                .await;
            }
        }
    }

    pub(crate) async fn handle_unique_reserve_request(
        &mut self,
        from: NodeId,
        req: &UniqueReserveRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let record_id = req.record_id_str();
        let idempotency_key = req.idempotency_key_str();

        let unique_part = super::super::db::unique_partition(entity, field, &req.value);
        let status = if !self.unique_partition_sealed(unique_part) {
            tracing::warn!(
                field,
                partition = unique_part.get(),
                "unique partition not sealed at the current epoch; rejecting remote reserve"
            );
            UniqueReserveStatus::Error
        } else if let Some(data_partition) = req.data_partition() {
            let reserve_params = super::super::db::UniqueReserveParams {
                entity,
                field,
                value: &req.value,
                record_id,
                request_id: idempotency_key,
                data_partition,
                ttl_ms: req.ttl_ms,
            };
            let (result, write) = self
                .stores
                .unique_reserve_replicated(&reserve_params, Self::current_time_ms());
            match result {
                super::super::db::ReserveResult::Reserved => {
                    if let Some(w) = write {
                        self.sync_unique_write();
                        // Defer the response until a majority of the group holds the reservation;
                        // the tracker sends the UniqueReserveResponse when quorum is reached.
                        self.replicate_unique_reserve_remote(
                            w,
                            from,
                            req.request_id,
                            super::UniqueClaimId {
                                entity: entity.to_owned(),
                                field: field.to_owned(),
                                value: req.value.clone(),
                                idempotency_key: idempotency_key.to_owned(),
                            },
                        )
                        .await;
                        return;
                    }
                    UniqueReserveStatus::Reserved
                }
                super::super::db::ReserveResult::AlreadyReservedBySameRequest => {
                    UniqueReserveStatus::AlreadyReserved
                }
                super::super::db::ReserveResult::Conflict => UniqueReserveStatus::Conflict,
            }
        } else {
            UniqueReserveStatus::Error
        };

        let response = UniqueReserveResponse::create(req.request_id, status);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReserveResponse(response))
            .await;
    }

    pub(crate) async fn handle_unique_commit_request(
        &mut self,
        from: NodeId,
        req: &UniqueCommitRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let committed =
            match self
                .stores
                .unique_commit_replicated(entity, field, &req.value, idempotency_key)
            {
                Ok((_, w)) => {
                    self.write_or_forward(w).await;
                    self.sync_unique_write();
                    true
                }
                Err(super::super::db::UniqueStoreError::AlreadyCommitted) => true,
                Err(_) => false,
            };

        let response = super::UniqueCommitResponse::create(req.request_id, committed);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueCommitResponse(response))
            .await;
    }

    pub(crate) async fn handle_unique_release_request(
        &mut self,
        from: NodeId,
        req: &UniqueReleaseRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        if let Some(w) =
            self.stores
                .unique_release_replicated(entity, field, &req.value, idempotency_key)
        {
            self.write_or_forward(w).await;
        }
        // A committed-claim release (epoch > 0) raises the fence so a later stale reassert from a
        // superseded data-partition primary is rejected; uncommitted releases stamp epoch 0.
        if req.data_partition_epoch > 0
            && let Some(dp) = req.data_partition()
        {
            self.stores
                .db_unique
                .fence_bump(dp, req.data_partition_epoch);
        }

        let response = super::UniqueReleaseResponse::create(req.request_id, true);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReleaseResponse(response))
            .await;
    }

    pub(crate) async fn handle_unique_reassert_request(&mut self, req: &UniqueReassertRequest) {
        let Some(data_partition) = req.data_partition() else {
            return;
        };
        self.reassert_unique_claim(
            req.entity_str(),
            req.field_str(),
            &req.value,
            req.record_id_str(),
            data_partition,
            req.data_partition_epoch,
        )
        .await;
    }

    async fn reassert_unique_claim(
        &mut self,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        data_partition: PartitionId,
        epoch: u64,
    ) {
        let (result, write) = self.stores.reassert_replicated(
            entity,
            field,
            value,
            record_id,
            data_partition,
            epoch,
            Self::current_time_ms(),
        );
        if let Some(w) = write {
            self.write_or_forward(w).await;
            self.sync_unique_write();
        }
        if matches!(result, super::super::db::ReassertResult::Conflict) {
            tracing::error!(
                entity,
                field,
                record_id,
                "unique oversell detected: committed claim held by a different record"
            );
        }
    }

    fn allocate_unique_request_id(&self) -> u64 {
        self.pending_constraints.allocate_unique_id()
    }

    /// Durably fsync a just-written unique reservation/commit so it survives a crash
    /// before the operation is acknowledged.
    pub(crate) fn sync_unique_write(&self) {
        if let Err(e) = self.stores.sync_storage() {
            tracing::warn!(error = ?e, "failed to sync unique write");
        }
    }

    pub(crate) async fn send_unique_reserve_request_async(
        &mut self,
        target_node: NodeId,
        params: &super::super::db::UniqueReserveParams<'_>,
    ) -> Result<oneshot::Receiver<UniqueReserveStatus>, super::super::transport::TransportError>
    {
        let request_id = self.allocate_unique_request_id();
        let (tx, rx) = oneshot::channel();

        self.pending_constraints.insert_unique(request_id, tx);

        let request = UniqueReserveRequest::create(
            request_id,
            params.entity,
            params.field,
            params.value,
            params.record_id,
            params.request_id,
            params.data_partition,
            params.ttl_ms,
        );

        self.transport
            .send(target_node, ClusterMessage::UniqueReserveRequest(request))
            .await?;

        Ok(rx)
    }

    pub(crate) async fn send_unique_commit_fire_and_forget(
        &self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) {
        let request = UniqueCommitRequest::create(0, entity, field, value, idempotency_key);

        let _ = self
            .transport
            .send(target_node, ClusterMessage::UniqueCommitRequest(request))
            .await;
    }

    pub(crate) async fn send_unique_release_fire_and_forget(
        &self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
        fence: Option<PartitionId>,
    ) {
        // A committed-claim release carries its data-partition + epoch so the value site raises its
        // fence high-water mark; an uncommitted-reservation release (fence = None) stamps epoch 0,
        // which never bumps the fence and needs no reassert protection.
        let (data_partition, epoch) = match fence {
            Some(dp) => (dp.get(), self.partition_map.epoch(dp).get()),
            None => (0, 0),
        };
        let request = UniqueReleaseRequest::create(
            0,
            entity,
            field,
            value,
            idempotency_key,
            PartitionId::new(data_partition).unwrap_or(PartitionId::ZERO),
            epoch,
        );

        let _ = self
            .transport
            .send(target_node, ClusterMessage::UniqueReleaseRequest(request))
            .await;
    }

    /// Every node that must RECEIVE unique writes (reserve/replicate/commit/release/seal): the full
    /// partition-map membership (consensus-agreed, so all nodes derive the same set) plus self.
    /// Learners are included here so they stay caught up and can be promoted — but they do NOT count
    /// toward the majority/seal (see `unique_voter_group`). An uninitialised map yields just this node.
    #[must_use]
    pub(crate) fn unique_fanout_group(&self) -> Vec<NodeId> {
        let mut group = self.partition_map.all_nodes();
        if !group.contains(&self.node_id) {
            group.push(self.node_id);
            group.sort_unstable_by_key(|n| n.get());
        }
        group
    }

    /// The unique VOTER set — the denominator for durability and seal quorums. A reservation is
    /// majority-durable and a seal learns it only against these nodes, so a newly-joined node is
    /// excluded until the leader has carried outstanding reservations to it and promoted it. The set
    /// is the leader-managed voter set verbatim: it is NOT filtered to current membership, because a
    /// departed voter must stay in the denominator until the leader paces it out one node per
    /// interval. Filtering it out on departure would shrink the denominator by more than one node at
    /// once under correlated loss, defeating the single-step safety invariant (see
    /// `ClusterUniqueReconfigV6.tla`: unpaced removal oversells). A departed voter cannot ack a
    /// reserve or seal (it is absent from `unique_fanout_group`), so keeping it counted only raises
    /// the quorum bar — a correlated loss fails closed rather than overselling.
    /// Before the leader establishes a voter set (empty), falls back to the full fan-out group —
    /// pre-founding there are no reservations to protect, and the leader installs the set within a tick.
    #[must_use]
    pub(crate) fn unique_voter_group(&self) -> Vec<NodeId> {
        let voters = self.heartbeat.voters();
        if voters.is_empty() {
            return self.unique_fanout_group();
        }
        let mut group: Vec<NodeId> = voters.iter().copied().collect();
        group.sort_unstable_by_key(|n| n.get());
        group
    }

    /// Whether `node` counts toward unique quorums — a voter, or (before the voter set is
    /// established) any fan-out member.
    #[must_use]
    pub(crate) fn is_unique_voter(&self, node: NodeId) -> bool {
        self.unique_voter_group().contains(&node)
    }

    /// Majority size for the current voter group.
    #[must_use]
    pub(crate) fn unique_majority(&self) -> usize {
        self.unique_voter_group().len() / 2 + 1
    }

    /// Seed the unique-voter set directly at a fresh timestamp — test setup that bypasses the leader
    /// tick which normally establishes voters via `manage_unique_voters`.
    #[cfg(test)]
    pub(crate) fn seed_unique_voters(&mut self, voters: std::collections::BTreeSet<NodeId>) {
        let (term, seq) = self.heartbeat.voter_stamp();
        self.heartbeat.set_voters(term.max(1), seq + 1, voters);
    }

    /// Leader-driven unique-voter management, called each tick with the current raft status. The
    /// leader converges the voter set toward the current membership ONE node per interval: a single
    /// add or remove keeps every outstanding reservation a majority of the voter set (so a seal still
    /// intersects the holders), and the interval exceeds the reserve quorum timeout, so a reservation
    /// from before a change has resolved before the next. The set is stamped `(raft_term, seq)` and
    /// gossiped by heartbeat; followers ignore this and adopt the leader's set. Founding takes the
    /// whole membership at once — a fresh cluster has no outstanding reservations to carry.
    pub(crate) fn manage_unique_voters(&mut self, is_leader: bool, raft_term: u64, now: u64) {
        if !is_leader {
            return;
        }
        let members: std::collections::BTreeSet<NodeId> =
            self.partition_map.all_nodes().into_iter().collect();
        if members.is_empty() {
            return;
        }
        let (_, stamp_seq) = self.heartbeat.voter_stamp();
        let current: std::collections::BTreeSet<NodeId> = self.heartbeat.voters().clone();

        if current.is_empty() {
            let founding: Vec<u16> = members.iter().map(|n| n.get()).collect();
            self.heartbeat.set_voters(raft_term, stamp_seq + 1, members);
            self.last_voter_change_ms = now;
            tracing::info!(term = raft_term, voters = ?founding, "founded unique voters");
            return;
        }

        if now.saturating_sub(self.last_voter_change_ms) < UNIQUE_VOTER_CHANGE_INTERVAL_MS {
            return;
        }

        if let Some(&departed) = current.difference(&members).next() {
            let mut next = current;
            next.remove(&departed);
            self.heartbeat.set_voters(raft_term, stamp_seq + 1, next);
            self.last_voter_change_ms = now;
            tracing::info!(
                term = raft_term,
                node = departed.get(),
                "removed unique voter"
            );
            return;
        }

        if let Some(&learner) = members.difference(&current).next() {
            let mut next = current;
            next.insert(learner);
            self.heartbeat.set_voters(raft_term, stamp_seq + 1, next);
            self.last_voter_change_ms = now;
            tracing::info!(
                term = raft_term,
                node = learner.get(),
                "promoted unique voter"
            );
        }
    }

    /// Stamp `write` with the value-partition epoch, bump this node's promised epoch, and fan the
    /// decision out to every other group member under `request_id` (0 = untracked/fire-and-forget).
    async fn send_unique_replicate(
        &mut self,
        write: crate::cluster::protocol::ReplicationWrite,
        request_id: u64,
    ) {
        let partition = write.partition;
        let epoch = self.epoch(partition).unwrap_or(crate::cluster::Epoch::ZERO);
        let stamped = crate::cluster::protocol::ReplicationWrite::new(
            partition,
            write.operation,
            epoch,
            0,
            write.entity,
            write.id,
            write.data,
        );

        let promised = self
            .unique_promised
            .entry(partition.get())
            .or_insert(crate::cluster::Epoch::ZERO);
        if epoch > *promised {
            *promised = epoch;
        }

        for node in self.unique_fanout_group() {
            if node != self.node_id {
                let _ = self
                    .transport
                    .send(
                        node,
                        ClusterMessage::UniqueReplicate {
                            request_id,
                            write: stamped.clone(),
                        },
                    )
                    .await;
            }
        }
    }

    /// Replicate a `DB_UNIQUE` write to the whole quorum group, fire-and-forget (commit/release/
    /// reassert): the Phase-1 reconciler repairs any member that misses it.
    pub(crate) async fn replicate_unique_to_group(
        &mut self,
        write: crate::cluster::protocol::ReplicationWrite,
    ) {
        self.send_unique_replicate(write, 0).await;
    }

    /// Replicate a reserve `DB_UNIQUE` write to the group and return a receiver that resolves
    /// `true` once a **majority** of the group (including this node) holds it, or `false` on
    /// timeout. The local store already holds the reservation, so this node counts as one ack.
    pub(crate) async fn replicate_unique_reserve(
        &mut self,
        write: crate::cluster::protocol::ReplicationWrite,
    ) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        let needed = self.unique_majority();
        let request_id = self.next_unique_quorum_id();

        self.send_unique_replicate(write, request_id).await;

        let mut acks = std::collections::HashSet::new();
        if self.is_unique_voter(self.node_id) {
            acks.insert(self.node_id);
        }
        if acks.len() >= needed {
            let _ = tx.send(true);
        } else {
            let deadline_ms = Self::current_time_ms() + UNIQUE_QUORUM_TIMEOUT_MS;
            self.pending_unique_quorum.insert(
                request_id,
                super::UniqueQuorumTracker {
                    acks,
                    needed,
                    deadline_ms,
                    completion: super::UniqueQuorumCompletion::Local(tx),
                },
            );
        }
        rx
    }

    /// Replicate a remote-coordinated reserve to the group and, once a **majority** holds it (or the
    /// deadline passes), send the deferred `UniqueReserveResponse` back to `from`. This makes a
    /// reserve routed from a non-primary coordinator majority-durable before it is acknowledged.
    pub(crate) async fn replicate_unique_reserve_remote(
        &mut self,
        write: crate::cluster::protocol::ReplicationWrite,
        from: NodeId,
        response_request_id: u64,
        claim: super::UniqueClaimId,
    ) {
        let needed = self.unique_majority();
        let request_id = self.next_unique_quorum_id();

        self.send_unique_replicate(write, request_id).await;

        let completion = super::UniqueQuorumCompletion::RemoteReserve {
            from,
            response_request_id,
            claim,
        };
        let mut acks = std::collections::HashSet::new();
        if self.is_unique_voter(self.node_id) {
            acks.insert(self.node_id);
        }
        if acks.len() >= needed {
            self.fire_unique_quorum_completion(completion, true).await;
        } else {
            let deadline_ms = Self::current_time_ms() + UNIQUE_QUORUM_TIMEOUT_MS;
            self.pending_unique_quorum.insert(
                request_id,
                super::UniqueQuorumTracker {
                    acks,
                    needed,
                    deadline_ms,
                    completion,
                },
            );
        }
    }

    async fn fire_unique_quorum_completion(
        &mut self,
        completion: super::UniqueQuorumCompletion,
        success: bool,
    ) {
        match completion {
            super::UniqueQuorumCompletion::Local(tx) => {
                let _ = tx.send(success);
            }
            super::UniqueQuorumCompletion::RemoteReserve {
                from,
                response_request_id,
                claim,
            } => {
                let status = if success {
                    UniqueReserveStatus::Reserved
                } else {
                    // The reserve never reached a majority: release the local reservation this node
                    // holds so a transient quorum timeout does not wedge the value until its TTL.
                    if let Some(w) = self.stores.unique_release_replicated(
                        &claim.entity,
                        &claim.field,
                        &claim.value,
                        &claim.idempotency_key,
                    ) {
                        self.write_or_forward(w).await;
                        self.sync_unique_write();
                    }
                    UniqueReserveStatus::Error
                };
                let response = UniqueReserveResponse::create(response_request_id, status);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::UniqueReserveResponse(response))
                    .await;
            }
        }
    }

    fn next_unique_quorum_id(&mut self) -> u64 {
        self.unique_quorum_counter += 1;
        if self.unique_quorum_counter == 0 {
            self.unique_quorum_counter = 1;
        }
        self.unique_quorum_counter
    }

    /// Receive a group `DB_UNIQUE` replication write: accept iff the write's epoch is not below
    /// this node's promised epoch for the partition (the fence), then apply last-writer-wins and
    /// ack the coordinator if the write is quorum-tracked (`request_id != 0`).
    pub(crate) async fn handle_unique_replicate(
        &mut self,
        from: NodeId,
        request_id: u64,
        write: &crate::cluster::protocol::ReplicationWrite,
    ) {
        let promised = self
            .unique_promised
            .entry(write.partition.get())
            .or_insert(crate::cluster::Epoch::ZERO);
        if write.epoch < *promised {
            tracing::debug!(
                partition = write.partition.get(),
                write_epoch = write.epoch.get(),
                promised = promised.get(),
                "rejecting stale-epoch unique replicate"
            );
            return;
        }
        *promised = write.epoch;
        if let Err(e) = self.stores.apply_write(write) {
            tracing::error!(?e, "failed to apply unique replicate write");
            return;
        }
        self.sync_unique_write();

        if request_id != 0 {
            let ack = crate::cluster::protocol::UniqueReplicateAck::create(request_id);
            let _ = self
                .transport
                .send(from, ClusterMessage::UniqueReplicateAck(ack))
                .await;
        }
    }

    /// Record a group member's ack for a tracked reserve; complete it once a majority
    /// (including this node) holds the reservation.
    pub(crate) async fn record_unique_quorum_ack(&mut self, from: NodeId, request_id: u64) {
        let from_is_voter = self.is_unique_voter(from);
        let reached = match self.pending_unique_quorum.get_mut(&request_id) {
            Some(tracker) => {
                if from_is_voter {
                    tracker.acks.insert(from);
                }
                tracker.acks.len() >= tracker.needed
            }
            None => false,
        };
        if reached && let Some(tracker) = self.pending_unique_quorum.remove(&request_id) {
            self.fire_unique_quorum_completion(tracker.completion, true)
                .await;
        }
    }

    /// Fail any reserve whose quorum was not reached before its deadline (fail-closed).
    pub(crate) async fn sweep_unique_quorum(&mut self, now_ms: u64) {
        let expired: Vec<u64> = self
            .pending_unique_quorum
            .iter()
            .filter(|(_, t)| t.deadline_ms <= now_ms)
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            if let Some(tracker) = self.pending_unique_quorum.remove(&id) {
                self.fire_unique_quorum_completion(tracker.completion, false)
                    .await;
            }
        }
    }

    /// Seal a unique partition immediately if this node is its own majority (single-node group),
    /// else queue a seal round. Called from `become_primary` (stays synchronous).
    pub(crate) fn seal_or_queue_unique(&mut self, partition: PartitionId, epoch: Epoch) {
        if self.unique_majority() == 1 {
            let promised = self
                .unique_promised
                .entry(partition.get())
                .or_insert(Epoch::ZERO);
            if epoch > *promised {
                *promised = epoch;
            }
            self.mark_unique_sealed(partition, epoch);
        } else {
            self.queue_unique_seal(partition, epoch);
        }
    }

    /// Whether this node has sealed the partition at its current acting (primary) epoch — the
    /// precondition for serving reserves (a superseded primary is not sealed at its stale epoch).
    #[must_use]
    pub(crate) fn unique_partition_sealed(&self, partition: PartitionId) -> bool {
        match self.epoch(partition) {
            Some(epoch) => self.unique_sealed.get(&partition.get()) == Some(&epoch),
            None => false,
        }
    }

    /// Queue a promotion seal for a unique partition (deduped; skipped if already sealed ≥ epoch).
    /// Drained by the event-loop tick so `become_primary` stays synchronous.
    pub(crate) fn queue_unique_seal(&mut self, partition: PartitionId, epoch: Epoch) {
        if self
            .unique_sealed
            .get(&partition.get())
            .is_some_and(|&s| s >= epoch)
        {
            return;
        }
        if !self
            .pending_seal_queue
            .iter()
            .any(|&(p, e)| p == partition && e >= epoch)
        {
            self.pending_seal_queue.push((partition, epoch));
        }
    }

    /// Initiate any queued promotion seals by sending seal requests directly (used by the cluster
    /// agent's async tick). Promises this node's epoch at a majority before it serves reserves.
    pub(crate) async fn drain_pending_seals(&mut self) {
        let queue = std::mem::take(&mut self.pending_seal_queue);
        for (partition, epoch) in queue {
            for (node, msg) in self.prepare_unique_seal(partition, epoch) {
                let _ = self.transport.send(node, msg).await;
            }
        }
    }

    /// Collect seal requests for queued promotions into the tick output (used by the synchronous
    /// `tick`/`send_tick_output` path).
    pub(crate) fn collect_pending_seals(&mut self, output: &mut super::TickOutput) {
        let queue = std::mem::take(&mut self.pending_seal_queue);
        for (partition, epoch) in queue {
            output
                .seal_requests
                .extend(self.prepare_unique_seal(partition, epoch));
        }
    }

    /// Promise this node's epoch for the partition and register a seal tracker; returns the seal
    /// requests to send to the group (empty if already sealed/in-flight/superseded or self-majority).
    fn prepare_unique_seal(
        &mut self,
        partition: PartitionId,
        epoch: Epoch,
    ) -> Vec<(NodeId, ClusterMessage)> {
        if self
            .unique_sealed
            .get(&partition.get())
            .is_some_and(|&s| s >= epoch)
        {
            return Vec::new();
        }
        if self
            .pending_unique_seals
            .values()
            .any(|t| t.partition == partition && t.epoch >= epoch)
        {
            return Vec::new();
        }
        if self.epoch(partition) != Some(epoch) {
            return Vec::new();
        }

        let promised = self
            .unique_promised
            .entry(partition.get())
            .or_insert(Epoch::ZERO);
        if epoch > *promised {
            *promised = epoch;
        }

        let needed = self.unique_majority();
        let request_id = self.next_unique_quorum_id();
        let mut requests = Vec::new();
        for node in self.unique_fanout_group() {
            if node != self.node_id {
                let req = UniqueSealRequest::create(request_id, partition, epoch.get());
                requests.push((node, ClusterMessage::UniqueSealRequest(req)));
            }
        }

        // Only VOTER acceptances count toward the seal quorum; self counts iff it is a voter.
        let mut responses = std::collections::HashSet::new();
        if self.is_unique_voter(self.node_id) {
            responses.insert(self.node_id);
        }
        if responses.len() >= needed {
            self.mark_unique_sealed(partition, epoch);
        } else {
            let deadline_ms = Self::current_time_ms() + UNIQUE_SEAL_TIMEOUT_MS;
            self.pending_unique_seals.insert(
                request_id,
                super::UniqueSealTracker {
                    partition,
                    epoch,
                    responses,
                    needed,
                    deadline_ms,
                },
            );
        }
        requests
    }

    pub(crate) async fn handle_unique_seal_request(
        &mut self,
        from: NodeId,
        req: &UniqueSealRequest,
    ) {
        let Some(partition) = req.partition_id() else {
            return;
        };
        let epoch = Epoch::new(req.epoch);
        let promised = self
            .unique_promised
            .entry(partition.get())
            .or_insert(Epoch::ZERO);
        let accepted = if epoch >= *promised {
            *promised = epoch;
            true
        } else {
            false
        };
        // An accepting member returns its reservations for the partition so the promoting primary
        // learns any claim it missed (the leaderView step) before it serves reserves.
        let reservations = if accepted {
            self.stores.db_unique.export_for_partition(partition)
        } else {
            Vec::new()
        };
        let resp = UniqueSealResponse::create(
            req.request_id,
            partition,
            req.epoch,
            accepted,
            reservations,
        );
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueSealResponse(resp))
            .await;
    }

    pub(crate) fn handle_unique_seal_response(&mut self, from: NodeId, resp: &UniqueSealResponse) {
        if !resp.is_accepted() {
            // A group member has promised a higher epoch — this node is superseded; abandon.
            self.pending_unique_seals.remove(&resp.request_id);
            return;
        }
        // Learn any reservation this promoting primary was missing before it is allowed to serve.
        // Merge from any responder (learners may hold reservations too); only voters count to quorum.
        if !resp.reservations.is_empty()
            && let Err(e) = self.stores.db_unique.merge_for_seal(&resp.reservations)
        {
            tracing::warn!(error = ?e, "failed to merge reservations from seal response");
        }
        let from_is_voter = self.is_unique_voter(from);
        let reached = match self.pending_unique_seals.get_mut(&resp.request_id) {
            Some(tracker) => {
                if from_is_voter {
                    tracker.responses.insert(from);
                }
                tracker.responses.len() >= tracker.needed
            }
            None => false,
        };
        if reached && let Some(tracker) = self.pending_unique_seals.remove(&resp.request_id) {
            self.mark_unique_sealed(tracker.partition, tracker.epoch);
        }
    }

    fn mark_unique_sealed(&mut self, partition: PartitionId, epoch: Epoch) {
        let entry = self
            .unique_sealed
            .entry(partition.get())
            .or_insert(Epoch::ZERO);
        if epoch > *entry {
            *entry = epoch;
        }
        tracing::debug!(
            partition = partition.get(),
            epoch = epoch.get(),
            "unique partition sealed"
        );
    }

    /// Retry seals that did not reach a majority before their deadline (liveness).
    pub(crate) fn sweep_unique_seals(&mut self, now_ms: u64) {
        let expired: Vec<(u64, PartitionId, Epoch)> = self
            .pending_unique_seals
            .iter()
            .filter(|(_, t)| t.deadline_ms <= now_ms)
            .map(|(&id, t)| (id, t.partition, t.epoch))
            .collect();
        for (id, partition, epoch) in expired {
            self.pending_unique_seals.remove(&id);
            if self.epoch(partition) == Some(epoch) {
                self.queue_unique_seal(partition, epoch);
            }
        }
    }

    async fn send_unique_reassert_fire_and_forget(
        &self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        data_partition: PartitionId,
    ) {
        let request = UniqueReassertRequest::create(
            0,
            entity,
            field,
            value,
            record_id,
            data_partition,
            self.partition_map.epoch(data_partition).get(),
        );

        let _ = self
            .transport
            .send(target_node, ClusterMessage::UniqueReassertRequest(request))
            .await;
    }

    /// Reconcile every durable record this node owns (data-partition primary) against its unique
    /// claim, repairing a reservation lost to failover/restart or a lost commit. Collects the work
    /// read-only, then applies it — see `collect_unique_reconcile_work` / `apply_unique_reconcile_chunk`
    /// for the split that lets the agent event loop release the controller lock between chunks.
    pub async fn reconcile_unique_claims(&mut self) {
        let work = self.collect_unique_reconcile_work();
        self.apply_unique_reconcile_chunk(&work).await;
    }

    /// The set of entities carrying a unique constraint, sorted and deduped.
    fn unique_constrained_entities(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for constraint in self.stores.constraint_list_all() {
            if constraint.constraint_type() == super::super::db::ConstraintType::Unique {
                set.insert(constraint.entity_str().to_string());
            }
        }
        set.into_iter().collect()
    }

    /// First reconcile pass: gather the keys of durable records this node owns (data-partition
    /// primary) for unique-constrained entities, WITHOUT parsing them. Cheap per record (no JSON
    /// parse — the scan's dominant cost), so the whole-store read-lock hold is brief; the parse is
    /// deferred to `project_unique_reconcile_work` in bounded chunks.
    #[must_use]
    pub(crate) fn collect_unique_reconcile_keys(&self) -> Vec<UniqueReassertKey> {
        let mut keys = Vec::new();
        for entity in &self.unique_constrained_entities() {
            if self.stores.constraint_get_unique_fields(entity).is_empty() {
                continue;
            }
            self.stores.db_data.for_each_record(entity, |record| {
                let data_partition = record.partition();
                if self.partition_map.primary(data_partition) != Some(self.node_id) {
                    return;
                }
                keys.push(UniqueReassertKey {
                    entity: entity.clone(),
                    record_id: record.id_str().to_string(),
                    data_partition,
                });
            });
        }
        keys
    }

    /// Second reconcile pass: parse a chunk of gathered keys into reassert work (the JSON parse
    /// deferred from key-gather). A record deleted since key-gather is skipped. Read-only.
    #[must_use]
    pub(crate) fn project_unique_reconcile_work(
        &self,
        keys: &[UniqueReassertKey],
    ) -> Vec<UniqueReassertWork> {
        let mut work = Vec::new();
        // `collect_unique_reconcile_keys` emits keys grouped by entity, so cache the unique-field
        // list per entity and refetch only when the entity changes, rather than once per record.
        let mut cached_entity = String::new();
        let mut unique_fields: Vec<String> = Vec::new();
        for key in keys {
            if key.entity != cached_entity {
                unique_fields = self.stores.constraint_get_unique_fields(&key.entity);
                cached_entity.clone_from(&key.entity);
            }
            let Some(record) = self.stores.db_data.get(&key.entity, &key.record_id) else {
                continue;
            };
            let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.data) else {
                continue;
            };
            for field in &unique_fields {
                let value = match parsed.get(field) {
                    Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                    None => continue,
                };
                work.push(UniqueReassertWork {
                    entity: key.entity.clone(),
                    field: field.clone(),
                    value,
                    record_id: key.record_id.clone(),
                    data_partition: key.data_partition,
                });
            }
        }
        work
    }

    /// Scan this node's durable records (read-only) and project the unique-claim reassert work for
    /// records whose *data* partition this node is primary of, so each record is reconciled by
    /// exactly one node. Non-chunked convenience over `collect_unique_reconcile_keys` +
    /// `project_unique_reconcile_work`; the event loop chunks the two passes to bound its lock hold.
    #[must_use]
    pub(crate) fn collect_unique_reconcile_work(&self) -> Vec<UniqueReassertWork> {
        self.project_unique_reconcile_work(&self.collect_unique_reconcile_keys())
    }

    /// Apply a batch of reassert work. The controller lock may have been released since the work was
    /// collected, so each item is re-checked against the current durable record: reasserting a
    /// committed claim for a record that was since deleted or changed would wedge the value (TTL only
    /// reclaims *uncommitted* claims), so a stale item is skipped.
    pub(crate) async fn apply_unique_reconcile_chunk(&mut self, work: &[UniqueReassertWork]) {
        for item in work {
            if !self.record_owns_value(&item.entity, &item.record_id, &item.field, &item.value) {
                continue;
            }
            let unique_part =
                super::super::db::unique_partition(&item.entity, &item.field, &item.value);
            match self.partition_map.primary(unique_part) {
                Some(primary) if primary == self.node_id => {
                    let epoch = self.partition_map.epoch(item.data_partition).get();
                    self.reassert_unique_claim(
                        &item.entity,
                        &item.field,
                        &item.value,
                        &item.record_id,
                        item.data_partition,
                        epoch,
                    )
                    .await;
                }
                Some(target) => {
                    self.send_unique_reassert_fire_and_forget(
                        target,
                        &item.entity,
                        &item.field,
                        &item.value,
                        &item.record_id,
                        item.data_partition,
                    )
                    .await;
                }
                None => {}
            }
        }
    }

    /// Whether the durable record `entity/record_id` currently exists and its `field` still encodes
    /// to `value` — the reconcile precondition that keeps a stale reassert from resurrecting a claim.
    fn record_owns_value(&self, entity: &str, record_id: &str, field: &str, value: &[u8]) -> bool {
        let Some(record) = self.stores.db_data.get(entity, record_id) else {
            return false;
        };
        let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.data) else {
            return false;
        };
        match parsed.get(field) {
            Some(v) => serde_json::to_vec(v).is_ok_and(|b| b == value),
            None => false,
        }
    }
}
