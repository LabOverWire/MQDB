// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId, PendingUniqueReserve,
    UniqueCheckPhase1Result, UniqueCommitRequest, UniqueReleaseRequest, UniqueReserveRequest,
    UniqueReserveResponse, UniqueReserveStatus,
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

            let is_conflict = if primary == Some(self.node_id) {
                let (result, write) = self.stores.unique_reserve_replicated(
                    entity,
                    field,
                    &value,
                    id,
                    request_id,
                    data_partition,
                    30_000,
                    now_ms,
                );

                match result {
                    super::super::db::ReserveResult::Reserved => {
                        if let Some(w) = write {
                            self.write_or_forward(w).await;
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
                    .send_unique_reserve_request_async(
                        target_node,
                        entity,
                        field,
                        &value,
                        id,
                        request_id,
                        data_partition,
                        30_000,
                    )
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
            let result = self.stores.unique_reserve(
                entity,
                field,
                &req.value,
                record_id,
                idempotency_key,
                data_partition,
                req.ttl_ms,
                Self::current_time_ms(),
            );
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

    pub(crate) fn handle_unique_reserve_response(&mut self, resp: &UniqueReserveResponse) {
        if let Some(tx) = self.pending_unique_requests.remove(&resp.request_id) {
            let _ = tx.send(resp.status());
        }
    }

    pub(crate) async fn handle_unique_commit_request(
        &mut self,
        from: NodeId,
        req: &UniqueCommitRequest,
    ) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let _ = self
            .stores
            .unique_commit(entity, field, &req.value, idempotency_key);

        let response = super::UniqueCommitResponse::create(req.request_id, true);
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

        let _ = self
            .stores
            .unique_release(entity, field, &req.value, idempotency_key);

        let response = super::UniqueReleaseResponse::create(req.request_id, true);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReleaseResponse(response))
            .await;
    }

    fn allocate_unique_request_id(&mut self) -> u64 {
        let id = self.next_unique_request_id;
        self.next_unique_request_id += 1;
        id
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_unique_reserve_request_async(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        idempotency_key: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> Result<oneshot::Receiver<UniqueReserveStatus>, super::super::transport::TransportError>
    {
        let request_id = self.allocate_unique_request_id();
        let (tx, rx) = oneshot::channel();

        self.pending_unique_requests.insert(request_id, tx);

        let request = UniqueReserveRequest::create(
            request_id,
            entity,
            field,
            value,
            record_id,
            idempotency_key,
            data_partition,
            ttl_ms,
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
}
