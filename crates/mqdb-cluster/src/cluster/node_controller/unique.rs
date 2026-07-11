// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId, PendingUniqueReserve,
    UniqueCheckPhase1Result, UniqueCommitRequest, UniqueReassertRequest, UniqueReleaseRequest,
    UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
};
use tokio::sync::oneshot;

const UNIQUE_RESERVE_TIMEOUT_SECS: u64 = 5;

pub async fn await_unique_reserves(
    pending: Vec<PendingUniqueReserve>,
) -> Result<Vec<(String, Vec<u8>, NodeId)>, (String, Vec<(String, Vec<u8>, NodeId)>)> {
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
            _ => {
                return Err((pending_reserve.field, confirmed));
            }
        }
    }

    Ok(confirmed)
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
    ) -> Result<UniqueCheckPhase1Result, String> {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let mut pending_remote: Vec<PendingUniqueReserve> = Vec::new();

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

            let is_conflict = if primary == Some(self.node_id) {
                let (result, write) = self
                    .stores
                    .unique_reserve_replicated(&reserve_params, now_ms);

                match result {
                    super::super::db::ReserveResult::Reserved => {
                        if let Some(w) = write {
                            self.write_or_forward(w).await;
                            self.sync_unique_write();
                        }
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::super::db::ReserveResult::AlreadyReservedBySameRequest => {
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::super::db::ReserveResult::Conflict => true,
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
                        false
                    }
                    Err(_) => true,
                }
            } else {
                true
            };

            if is_conflict {
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
                return Err(field.clone());
            }
        }

        Ok(UniqueCheckPhase1Result {
            local_reserved,
            pending_remote,
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
            self.send_unique_release_fire_and_forget(*target, entity, f, v, request_id)
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
            .await?;

        if !phase1.pending_remote.is_empty() {
            let remote_results = await_unique_reserves(phase1.pending_remote).await;
            match remote_results {
                Ok(confirmed_remotes) => {
                    for (f, v, target) in &confirmed_remotes {
                        self.send_unique_commit_fire_and_forget(*target, entity, f, v, request_id)
                            .await;
                    }
                }
                Err((conflict_field, confirmed)) => {
                    self.release_unique_check_reservations(
                        entity,
                        request_id,
                        &phase1.local_reserved,
                        &confirmed,
                    )
                    .await;
                    return Err(conflict_field);
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

    pub async fn release_unique_constraints(
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
                if let Some(w) = self
                    .stores
                    .unique_release_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
            } else if let Some(target_node) = primary {
                self.send_unique_release_fire_and_forget(
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

    pub(crate) async fn handle_unique_reserve_request(
        &mut self,
        from: NodeId,
        req: &UniqueReserveRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let record_id = req.record_id_str();
        let idempotency_key = req.idempotency_key_str();

        let status = if let Some(data_partition) = req.data_partition() {
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
            if let Some(w) = write {
                self.write_or_forward(w).await;
                self.sync_unique_write();
            }
            match result {
                super::super::db::ReserveResult::Reserved => UniqueReserveStatus::Reserved,
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
    ) {
        let (result, write) = self.stores.reassert_replicated(
            entity,
            field,
            value,
            record_id,
            data_partition,
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
    fn sync_unique_write(&self) {
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
    ) {
        let request = UniqueReleaseRequest::create(0, entity, field, value, idempotency_key);

        let _ = self
            .transport
            .send(target_node, ClusterMessage::UniqueReleaseRequest(request))
            .await;
    }

    /// The fixed quorum group for the `DB_UNIQUE` keyspace: the registered cluster membership
    /// (stable across heartbeat timeouts), decoupled from data replication factor.
    #[must_use]
    pub(crate) fn unique_quorum_group(&self) -> Vec<NodeId> {
        self.heartbeat.registered_nodes()
    }

    /// Replicate a `DB_UNIQUE` write to the whole quorum group (sequence-free, epoch-fenced).
    /// The local store is already updated by the store op that produced `write`; this fans the
    /// decision out to the group members, who accept it idempotently under the epoch fence.
    pub(crate) async fn replicate_unique_to_group(
        &mut self,
        write: crate::cluster::protocol::ReplicationWrite,
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

        for node in self.unique_quorum_group() {
            if node != self.node_id {
                let _ = self
                    .transport
                    .send(node, ClusterMessage::UniqueReplicate(stamped.clone()))
                    .await;
            }
        }
    }

    /// Receive a group `DB_UNIQUE` replication write: accept iff the write's epoch is not below
    /// this node's promised epoch for the partition (the fence), then apply last-writer-wins.
    pub(crate) fn handle_unique_replicate(
        &mut self,
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
        let request =
            UniqueReassertRequest::create(0, entity, field, value, record_id, data_partition);

        let _ = self
            .transport
            .send(target_node, ClusterMessage::UniqueReassertRequest(request))
            .await;
    }

    /// Reconcile every durable record this node owns (data-partition primary) against its
    /// unique claim, repairing a reservation lost to failover/restart or a lost commit. The
    /// durable record is the source of truth; each record is reconciled by exactly one node.
    pub async fn reconcile_unique_claims(&mut self) {
        let entities: Vec<String> = {
            let mut set = std::collections::BTreeSet::new();
            for constraint in self.stores.constraint_list_all() {
                if constraint.constraint_type() == super::super::db::ConstraintType::Unique {
                    set.insert(constraint.entity_str().to_string());
                }
            }
            set.into_iter().collect()
        };

        for entity in &entities {
            let unique_fields = self.stores.constraint_get_unique_fields(entity);
            if unique_fields.is_empty() {
                continue;
            }
            let records = self.stores.db_data.list(entity);
            for record in records {
                let data_partition = record.partition();
                if self.partition_map.primary(data_partition) != Some(self.node_id) {
                    continue;
                }
                let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.data) else {
                    continue;
                };
                let record_id = record.id_str().to_string();
                for field in &unique_fields {
                    let value = match parsed.get(field) {
                        Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                        None => continue,
                    };

                    let unique_part = super::super::db::unique_partition(entity, field, &value);
                    match self.partition_map.primary(unique_part) {
                        Some(primary) if primary == self.node_id => {
                            self.reassert_unique_claim(
                                entity,
                                field,
                                &value,
                                &record_id,
                                data_partition,
                            )
                            .await;
                        }
                        Some(target) => {
                            self.send_unique_reassert_fire_and_forget(
                                target,
                                entity,
                                field,
                                &value,
                                &record_id,
                                data_partition,
                            )
                            .await;
                        }
                        None => {}
                    }
                }
            }
        }
    }
}
