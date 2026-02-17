// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, Epoch, JsonDbOp, JsonDbRequest, JsonDbResponse,
    NodeController, NodeId, PartitionId, PendingUniqueWork, ReplicationWrite,
    UniqueCheckContinuation, UniqueReservationParams, entity,
};
use crate::types::MAX_LIST_RESULTS;
use serde_json::Value;

fn unique_field_diffs(
    unique_fields: &[String],
    old_data: &Value,
    new_data: &Value,
) -> (Value, Value) {
    let mut new_diff = serde_json::Map::new();
    let mut old_diff = serde_json::Map::new();
    for field in unique_fields {
        let old_val = old_data.get(field);
        let new_val = new_data.get(field);
        if old_val != new_val {
            if let Some(nv) = new_val {
                new_diff.insert(field.clone(), nv.clone());
            }
            if let Some(ov) = old_val {
                old_diff.insert(field.clone(), ov.clone());
            }
        }
    }
    (Value::Object(new_diff), Value::Object(old_diff))
}

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) fn compute_unique_field_diffs(
        &self,
        entity: &str,
        old_data: &Value,
        new_data: &Value,
    ) -> (Value, Value) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        unique_field_diffs(&unique_fields, old_data, new_data)
    }

    /// # Errors
    /// Returns `DbDataStoreError::AlreadyExists` if an entity with the given ID already exists.
    pub async fn db_create(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let start = std::time::Instant::now();
        let (db_entity, write) =
            self.stores
                .db_create_replicated(entity_type, id, data, timestamp_ms)?;
        let t_store = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.write_or_forward(write).await;
        let t_forward = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        tracing::info!(
            node = self.node_id.get(),
            t_store,
            t_forward,
            "db_create_timing"
        );
        Ok(db_entity)
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub async fn db_update(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) =
            self.stores
                .db_update_replicated(entity_type, id, data, timestamp_ms)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    pub async fn db_upsert(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> super::db::DbEntity {
        let (db_entity, write) =
            self.stores
                .db_upsert_replicated(entity_type, id, data, timestamp_ms);
        self.write_or_forward(write).await;
        db_entity
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub async fn db_delete(
        &mut self,
        entity_type: &str,
        id: &str,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) = self.stores.db_delete_replicated(entity_type, id)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    #[must_use]
    pub fn db_get(&self, entity_type: &str, id: &str) -> Option<super::db::DbEntity> {
        self.stores.db_get(entity_type, id)
    }

    #[must_use]
    pub fn db_list(&self, entity_type: &str) -> Vec<super::db::DbEntity> {
        self.stores.db_list(entity_type)
    }

    /// # Errors
    /// Returns `SchemaStoreError::AlreadyExists` if the schema already exists.
    ///
    /// # Panics
    /// Panics if partition 0 is invalid (should never happen as 0-63 are valid).
    pub async fn schema_register(
        &mut self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<super::db::ClusterSchema, super::db::SchemaStoreError> {
        let schema = self.stores.db_schema.register(entity, schema_data)?;
        let serialized = super::db::SchemaStore::serialize(&schema);
        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            super::protocol::Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_SCHEMA.to_string(),
            entity.to_string(),
            serialized,
        );
        self.write_or_forward(write).await;
        Ok(schema)
    }

    /// # Errors
    /// Returns `SchemaStoreError::NotFound` if the schema does not exist.
    ///
    /// # Panics
    /// Panics if partition 0 is invalid (should never happen as 0-63 are valid).
    pub async fn schema_update(
        &mut self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<super::db::ClusterSchema, super::db::SchemaStoreError> {
        let schema = self.stores.db_schema.update(entity, schema_data)?;
        let serialized = super::db::SchemaStore::serialize(&schema);
        let write = ReplicationWrite::new(
            PartitionId::ZERO,
            super::protocol::Operation::Update,
            Epoch::ZERO,
            0,
            entity::DB_SCHEMA.to_string(),
            entity.to_string(),
            serialized,
        );
        self.write_or_forward(write).await;
        Ok(schema)
    }

    #[must_use]
    pub fn schema_get(&self, entity: &str) -> Option<super::db::ClusterSchema> {
        self.stores.schema_get(entity)
    }

    #[must_use]
    pub fn schema_list(&self) -> Vec<super::db::ClusterSchema> {
        self.stores.schema_list()
    }

    #[must_use]
    pub fn schema_is_valid_for_write(&self, entity: &str) -> bool {
        self.stores.schema_is_valid_for_write(entity)
    }

    /// # Errors
    /// Returns `ConstraintStoreError::AlreadyExists` if the constraint already exists.
    pub async fn constraint_add(
        &mut self,
        constraint: &super::db::ClusterConstraint,
    ) -> Result<(), super::db::ConstraintStoreError> {
        let write = self.stores.constraint_add_replicated(constraint)?;
        self.write_or_forward(write).await;
        Ok(())
    }

    /// # Errors
    /// Returns `ConstraintStoreError::NotFound` if the constraint does not exist.
    pub async fn constraint_remove(
        &mut self,
        entity: &str,
        name: &str,
    ) -> Result<(), super::db::ConstraintStoreError> {
        let write = self.stores.constraint_remove_replicated(entity, name)?;
        self.write_or_forward(write).await;
        Ok(())
    }

    #[must_use]
    pub fn constraint_list(&self, entity: &str) -> Vec<super::db::ClusterConstraint> {
        self.stores.constraint_list(entity)
    }

    #[must_use]
    pub fn constraint_list_all(&self) -> Vec<super::db::ClusterConstraint> {
        self.stores.constraint_list_all()
    }

    #[must_use]
    pub fn constraint_get_unique_fields(&self, entity: &str) -> Vec<String> {
        self.stores.constraint_get_unique_fields(entity)
    }

    pub(crate) async fn handle_json_db_request(
        &mut self,
        from: NodeId,
        partition: PartitionId,
        request: &JsonDbRequest,
    ) -> Option<PendingUniqueWork> {
        if let Some(err) = self.check_forwarded_ownership(request) {
            let response = JsonDbResponse::new(
                request.request_id,
                err,
                request.response_topic.clone(),
                request.correlation_data.clone(),
            );
            let _ = self
                .transport
                .send(from, ClusterMessage::JsonDbResponse(response))
                .await;
            return None;
        }

        let (response_payload, pending) = match request.op {
            JsonDbOp::Read => {
                let id = request.id.as_deref().unwrap_or("");
                (self.handle_json_read_local(&request.entity, id), None)
            }
            JsonDbOp::Update => {
                let id = request.id.as_deref().unwrap_or("");
                let (payload, pending) = self
                    .handle_json_update_local(&request.entity, id, &request.payload, request, from)
                    .await;
                (payload, pending)
            }
            JsonDbOp::Delete => {
                let id = request.id.as_deref().unwrap_or("");
                (
                    self.handle_json_delete_local(&request.entity, id).await,
                    None,
                )
            }
            JsonDbOp::Create => {
                let (payload, pending) = self
                    .handle_json_create_local(
                        partition,
                        &request.entity,
                        &request.payload,
                        request,
                        from,
                    )
                    .await;
                (payload, pending)
            }
            JsonDbOp::List => (
                self.handle_json_list_local(&request.entity, &request.payload),
                None,
            ),
        };

        if let Some(pending) = pending {
            return Some(pending);
        }

        let response = JsonDbResponse::new(
            request.request_id,
            response_payload,
            request.response_topic.clone(),
            request.correlation_data.clone(),
        );

        let _ = self
            .transport
            .send(from, ClusterMessage::JsonDbResponse(response))
            .await;

        None
    }

    pub(crate) async fn handle_json_db_response(&mut self, response: &JsonDbResponse) {
        let scatter_prefix = format!("_mqdb/scatter/{}/", self.node_id.get());

        tracing::debug!(
            response_topic = %response.response_topic,
            scatter_prefix = %scatter_prefix,
            pending_count = self.pending_scatter_requests.len(),
            "received JsonDbResponse"
        );

        if let Some(request_id_str) = response.response_topic.strip_prefix(&scatter_prefix)
            && let Ok(request_id) = request_id_str.parse::<u64>()
        {
            tracing::debug!(request_id, "matched scatter response");
            let items: Vec<serde_json::Value> =
                serde_json::from_slice::<serde_json::Value>(&response.payload)
                    .ok()
                    .and_then(|parsed| parsed.get("data").and_then(|d| d.as_array()).cloned())
                    .unwrap_or_default();
            self.handle_scatter_list_response(request_id, items).await;
            return;
        }

        self.transport
            .queue_local_publish(response.response_topic.clone(), response.payload.clone(), 0)
            .await;
    }

    fn check_forwarded_ownership(&self, request: &JsonDbRequest) -> Option<Vec<u8>> {
        let sender = request.sender.as_deref()?;
        if self.ownership.is_admin(sender) {
            return None;
        }
        let owner_field = self.ownership.owner_field(&request.entity)?;
        let id = request.id.as_deref()?;
        if !matches!(
            request.op,
            JsonDbOp::Read | JsonDbOp::Update | JsonDbOp::Delete
        ) {
            return None;
        }
        let existing = self.stores.db_get(&request.entity, id)?;
        let Ok(data) = serde_json::from_slice::<serde_json::Value>(&existing.data) else {
            return Some(Self::json_error(403, "permission denied"));
        };
        let owner_value = data.get(owner_field).and_then(serde_json::Value::as_str);
        if owner_value != Some(sender) {
            return Some(Self::json_error(403, "permission denied"));
        }
        None
    }

    fn handle_json_read_local(&self, entity: &str, id: &str) -> Vec<u8> {
        match self.stores.db_get(entity, id) {
            Some(db_entity) => {
                let data_json: serde_json::Value =
                    serde_json::from_slice(&db_entity.data).unwrap_or(serde_json::Value::Null);
                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": data_json
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            None => Self::json_error(404, &format!("entity not found: {entity} id={id}")),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_json_update_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
        request: &JsonDbRequest,
        from: NodeId,
    ) -> (Vec<u8>, Option<PendingUniqueWork>) {
        let data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return (Self::json_error(400, "invalid JSON payload"), None),
        };

        let (old_data, merged_data) = if let Some(existing) = self.db_get(entity, id) {
            let existing_data: Value = serde_json::from_slice(&existing.data)
                .unwrap_or(Value::Object(serde_json::Map::new()));
            let old_data = existing_data.clone();
            let mut merged = existing_data;

            if let (Value::Object(existing_obj), Value::Object(updates)) = (&mut merged, data) {
                for (key, value) in updates {
                    existing_obj.insert(key, value);
                }
            }

            let existing_version = old_data
                .get("_version")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if let Value::Object(ref mut obj) = merged {
                obj.insert(
                    "_version".to_string(),
                    Value::Number((existing_version + 1).into()),
                );
            }

            (old_data, merged)
        } else {
            return (
                Self::json_error(404, &format!("entity not found: {entity} id={id}")),
                None,
            );
        };

        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();
        let partition = super::super::db::data_partition(entity, id);

        let (new_diff, old_diff) = self.compute_unique_field_diffs(entity, &old_data, &merged_data);
        let has_unique_changes = new_diff.as_object().is_some_and(|m| !m.is_empty());

        if has_unique_changes {
            match self
                .start_unique_constraint_check(
                    entity,
                    id,
                    &new_diff,
                    partition,
                    &request_id,
                    now_ms,
                )
                .await
            {
                Err(conflict_field) => {
                    return (
                        Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{conflict_field}'"),
                        ),
                        None,
                    );
                }
                Ok(phase1) => {
                    if !phase1.pending_remote.is_empty() {
                        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
                        return (
                            Vec::new(),
                            Some(PendingUniqueWork {
                                phase1,
                                continuation: UniqueCheckContinuation::UpdateFromNodeController {
                                    from,
                                    entity: entity.to_string(),
                                    id: id.to_string(),
                                    merged_data,
                                    data_bytes,
                                    partition,
                                    request_id,
                                    now_ms,
                                    new_diff,
                                    old_diff,
                                    response_topic: request.response_topic.clone(),
                                    correlation_data: request.correlation_data.clone(),
                                },
                            }),
                        );
                    }
                }
            }
        }

        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();

        let result = match self.db_update(entity, id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                if has_unique_changes {
                    self.commit_unique_constraints(
                        entity,
                        id,
                        &new_diff,
                        partition,
                        &request_id,
                        now_ms,
                    )
                    .await;
                    self.release_unique_constraints(entity, id, &old_diff, partition, id, now_ms)
                        .await;
                }
                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": merged_data
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                if has_unique_changes {
                    self.release_unique_constraints(
                        entity,
                        id,
                        &new_diff,
                        partition,
                        &request_id,
                        now_ms,
                    )
                    .await;
                }
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => {
                if has_unique_changes {
                    self.release_unique_constraints(
                        entity,
                        id,
                        &new_diff,
                        partition,
                        &request_id,
                        now_ms,
                    )
                    .await;
                }
                Self::json_error(500, "internal error")
            }
        };

        (result, None)
    }

    async fn handle_json_delete_local(&mut self, entity: &str, id: &str) -> Vec<u8> {
        match self.db_delete(entity, id).await {
            Ok(_) => {
                let result = serde_json::json!({
                    "status": "ok",
                    "id": id,
                    "entity": entity,
                    "deleted": true
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_json_create_local(
        &mut self,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
        request: &JsonDbRequest,
        from: NodeId,
    ) -> (Vec<u8>, Option<PendingUniqueWork>) {
        let mut data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return (Self::json_error(400, "invalid JSON payload"), None),
        };

        Self::apply_ttl_expiry(&mut data);

        if let serde_json::Value::Object(ref mut obj) = data {
            obj.remove("_version");
            obj.insert("_version".to_string(), serde_json::Value::Number(1.into()));
        }

        let id = if let Some(client_id) = data.get("id").and_then(serde_json::Value::as_str) {
            client_id.to_string()
        } else {
            self.generate_id_for_partition(entity, partition, payload)
        };
        let request_id = uuid::Uuid::new_v4().to_string();
        let now_ms = Self::current_time_ms();

        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();
        let mut pending_remote_reserves = Vec::new();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let params = UniqueReservationParams {
                entity,
                field,
                value: &value,
                id: &id,
                request_id: &request_id,
                partition,
                now_ms,
            };

            let unique_part =
                super::db::unique_partition(params.entity, params.field, params.value);
            let primary = self.partition_map.primary(unique_part);

            let is_conflict = if primary == Some(self.node_id) {
                self.reserve_unique_local(&params, &mut local_reserved)
                    .await
            } else if let Some(target_node) = primary {
                match self
                    .send_unique_reserve_request_async(
                        target_node,
                        params.entity,
                        params.field,
                        params.value,
                        params.id,
                        params.request_id,
                        params.partition,
                        30_000,
                    )
                    .await
                {
                    Ok(rx) => {
                        pending_remote_reserves.push(super::PendingUniqueReserve {
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
                self.release_all_reservations(
                    entity,
                    &request_id,
                    &local_reserved,
                    &remote_reserved,
                )
                .await;
                for pending in pending_remote_reserves {
                    drop(pending.receiver);
                }
                return (
                    Self::json_error(
                        409,
                        &format!("unique constraint violation on field '{field}'"),
                    ),
                    None,
                );
            }
        }

        if !pending_remote_reserves.is_empty() {
            let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
            return (
                Vec::new(),
                Some(PendingUniqueWork {
                    phase1: super::UniqueCheckPhase1Result {
                        local_reserved,
                        pending_remote: pending_remote_reserves,
                    },
                    continuation: UniqueCheckContinuation::CreateFromNodeController {
                        from,
                        entity: entity.to_string(),
                        id,
                        data,
                        data_bytes,
                        partition,
                        request_id,
                        now_ms,
                        response_topic: request.response_topic.clone(),
                        correlation_data: request.correlation_data.clone(),
                    },
                }),
            );
        }

        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();

        let result = match self.db_create(entity, &id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                self.commit_all_reservations(
                    entity,
                    &request_id,
                    &local_reserved,
                    &remote_reserved,
                )
                .await;
                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": data
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::AlreadyExists) => {
                self.release_all_reservations(
                    entity,
                    &request_id,
                    &local_reserved,
                    &remote_reserved,
                )
                .await;
                Self::json_error(409, "entity already exists")
            }
            Err(_) => {
                self.release_all_reservations(
                    entity,
                    &request_id,
                    &local_reserved,
                    &remote_reserved,
                )
                .await;
                Self::json_error(500, "internal error")
            }
        };

        (result, None)
    }

    fn apply_ttl_expiry(data: &mut serde_json::Value) {
        if let serde_json::Value::Object(obj) = data
            && let Some(ttl_secs) = obj.get("ttl_secs").and_then(serde_json::Value::as_u64)
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let expires_at = now + ttl_secs;
            obj.insert(
                "_expires_at".to_string(),
                serde_json::Value::Number(expires_at.into()),
            );
            obj.remove("ttl_secs");
        }
    }

    async fn reserve_unique_local(
        &mut self,
        params: &UniqueReservationParams<'_>,
        local_reserved: &mut Vec<(String, Vec<u8>)>,
    ) -> bool {
        let (result, write) = self.stores.unique_reserve_replicated(
            params.entity,
            params.field,
            params.value,
            params.id,
            params.request_id,
            params.partition,
            30_000,
            params.now_ms,
        );

        match result {
            super::db::ReserveResult::Reserved => {
                if let Some(w) = write {
                    self.write_or_forward(w).await;
                }
                local_reserved.push((params.field.to_owned(), params.value.to_vec()));
                false
            }
            super::db::ReserveResult::AlreadyReservedBySameRequest => {
                local_reserved.push((params.field.to_owned(), params.value.to_vec()));
                false
            }
            super::db::ReserveResult::Conflict => true,
        }
    }

    async fn release_all_reservations(
        &mut self,
        entity: &str,
        request_id: &str,
        local_reserved: &[(String, Vec<u8>)],
        remote_reserved: &[(String, Vec<u8>, NodeId)],
    ) {
        for (f, v) in local_reserved {
            if let Some(w) = self
                .stores
                .unique_release_replicated(entity, f, v, request_id)
            {
                self.write_or_forward(w).await;
            }
        }
        for (f, v, target) in remote_reserved {
            self.send_unique_release_fire_and_forget(*target, entity, f, v, request_id)
                .await;
        }
    }

    async fn commit_all_reservations(
        &mut self,
        entity: &str,
        request_id: &str,
        local_reserved: &[(String, Vec<u8>)],
        remote_reserved: &[(String, Vec<u8>, NodeId)],
    ) {
        for (field, value) in local_reserved {
            if let Ok((_, w)) = self
                .stores
                .unique_commit_replicated(entity, field, value, request_id)
            {
                self.write_or_forward(w).await;
            }
        }
        for (field, value, target) in remote_reserved {
            self.send_unique_commit_fire_and_forget(*target, entity, field, value, request_id)
                .await;
        }
    }

    pub(crate) fn handle_json_list_local(&self, entity: &str, payload: &[u8]) -> Vec<u8> {
        let filters: Vec<crate::Filter> = if payload.is_empty() {
            Vec::new()
        } else if let Ok(data) = serde_json::from_slice::<serde_json::Value>(payload) {
            data.get("filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let entities = self.db_list(entity);
        let mut items: Vec<serde_json::Value> = entities
            .iter()
            .filter_map(|e| {
                let data: serde_json::Value = serde_json::from_slice(&e.data).ok()?;
                if Self::matches_filters(&data, &filters) {
                    Some(serde_json::json!({
                        "id": e.id_str(),
                        "data": data
                    }))
                } else {
                    None
                }
            })
            .collect();
        items.truncate(MAX_LIST_RESULTS);

        let result = serde_json::json!({
            "status": "ok",
            "data": items
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    pub(crate) fn matches_filters(entity: &serde_json::Value, filters: &[crate::Filter]) -> bool {
        for filter in filters {
            if let Some(field_value) = entity.get(&filter.field) {
                if !filter.matches(field_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub(crate) fn json_error(code: u16, message: &str) -> Vec<u8> {
        let result = serde_json::json!({
            "status": "error",
            "code": code,
            "message": message
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    pub(crate) fn current_time_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX)
    }

    fn generate_id_for_partition(
        &self,
        entity: &str,
        partition: PartitionId,
        data: &[u8],
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entity.hash(&mut hasher);
        data.hash(&mut hasher);
        self.node_id.get().hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
            .hash(&mut hasher);

        let base_id = hasher.finish();

        for suffix in 0..1000_u16 {
            let id = format!("{base_id:016x}-{suffix:04x}");
            if super::db::data_partition(entity, &id) == partition {
                return id;
            }
        }

        format!("{base_id:016x}-p{}", partition.get())
    }

    #[allow(clippy::too_many_arguments, clippy::cast_possible_truncation)]
    pub async fn forward_json_db_request(
        &self,
        partition: PartitionId,
        op: JsonDbOp,
        entity: &str,
        id: Option<&str>,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
    ) -> bool {
        let start = std::time::Instant::now();
        let node_id = self.node_id.get();

        let Some(primary) = self.partition_map.primary(partition) else {
            tracing::warn!(?partition, "cannot forward JSON request: no primary known");
            return false;
        };

        if primary == self.node_id {
            return false;
        }

        #[allow(clippy::cast_possible_truncation)]
        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);

        let request = JsonDbRequest::new(
            request_id,
            op,
            entity.to_string(),
            id.map(String::from),
            payload.to_vec(),
            response_topic.to_string(),
            correlation_data.map(<[u8]>::to_vec),
            sender.map(String::from),
        );

        let msg = ClusterMessage::JsonDbRequest { partition, request };
        if let Err(e) = self.transport.send(primary, msg).await {
            tracing::warn!(
                node = node_id,
                partition = partition.get(),
                primary = primary.get(),
                elapsed_us = start.elapsed().as_micros() as u64,
                ?e,
                "forward_json_request_failed"
            );
            return false;
        }

        tracing::info!(
            node = node_id,
            partition = partition.get(),
            primary = primary.get(),
            elapsed_us = start.elapsed().as_micros() as u64,
            ?op,
            entity,
            "forward_json_request_sent"
        );
        true
    }

    #[cfg(test)]
    pub async fn handle_json_update_local_for_test(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let request = JsonDbRequest::new(
            0,
            JsonDbOp::Update,
            entity.to_string(),
            Some(id.to_string()),
            payload.to_vec(),
            String::new(),
            None,
            None,
        );
        let (result, _) = self
            .handle_json_update_local(entity, id, payload, &request, self.node_id)
            .await;
        result
    }
}
