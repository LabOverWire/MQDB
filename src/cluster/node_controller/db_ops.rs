// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ClusterMessage, ClusterTransport, Epoch, FkCheckContinuation, FkDeleteContinuation, JsonDbOp,
    JsonDbRequest, JsonDbResponse, NodeController, NodeId, PartitionId, PendingFkDeleteWork,
    PendingFkWork, PendingUniqueWork, ReplicationWrite, UniqueCheckContinuation,
    UniqueReservationParams, UniqueReserveStatus, entity,
};
use crate::cluster::store_manager::outbox::{CascadeOutboxPayload, CascadeRemoteOp, OutboxPayload};
use crate::events::ChangeEvent;
use crate::types::MAX_LIST_RESULTS;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::cluster::db_handler::helpers::{parse_projection, validate_projection_against_schema};

const CASCADE_ACK_TIMEOUT_SECS: u64 = 5;

pub(crate) fn spawn_cascade_ack_waiter(
    outbox: Option<crate::cluster::store_manager::outbox::ClusterOutbox>,
    operation_id: String,
    receivers: Vec<oneshot::Receiver<bool>>,
    mark_on_empty: bool,
) {
    if receivers.is_empty() {
        if mark_on_empty {
            if let Some(ob) = outbox {
                let _ = ob.mark_cascade_delivered(&operation_id);
            }
        } else {
            tracing::debug!(
                operation_id,
                "cascade has no ack receivers, leaving for retry"
            );
        }
        return;
    }
    tokio::spawn(async move {
        let timeout = tokio::time::Duration::from_secs(CASCADE_ACK_TIMEOUT_SECS);
        let mut all_ok = true;
        for rx in receivers {
            if !matches!(tokio::time::timeout(timeout, rx).await, Ok(Ok(true))) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            if let Some(ob) = outbox {
                let _ = ob.mark_cascade_delivered(&operation_id);
            }
        } else {
            tracing::warn!(operation_id, "cascade ack timeout, leaving entry for retry");
        }
    });
}

pub(crate) fn build_change_event_outbox(event: &ChangeEvent) -> OutboxPayload {
    let topic = event.event_topic(0);
    let user_properties: Vec<(String, String)> = event
        .client_id
        .as_ref()
        .map(|cid| vec![("x-origin-client-id".to_string(), cid.clone())])
        .unwrap_or_default();
    let payload = serde_json::to_vec(event).unwrap_or_default();
    let operation_id = uuid::Uuid::new_v4().to_string();
    OutboxPayload::new(operation_id, &topic, &payload, &user_properties)
}

pub(crate) enum CascadeSideEffect {
    LocalDelete {
        entity: String,
        id: String,
    },
    LocalSetNull {
        entity: String,
        id: String,
        updated_data: Vec<u8>,
    },
    RemoteDelete {
        partition: PartitionId,
        entity: String,
        id: String,
    },
    RemoteSetNull {
        partition: PartitionId,
        entity: String,
        id: String,
        field: String,
        expected_value: String,
    },
}

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
    pub fn db_create_prepare(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(super::db::DbEntity, ReplicationWrite), super::db::DbDataStoreError> {
        self.stores
            .db_create_replicated(entity_type, id, data, timestamp_ms)
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
        let (db_entity, write) = self.db_create_prepare(entity_type, id, data, timestamp_ms)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub fn db_update_prepare(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<(super::db::DbEntity, ReplicationWrite), super::db::DbDataStoreError> {
        self.stores
            .db_update_replicated(entity_type, id, data, timestamp_ms)
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
        let (db_entity, write) = self.db_update_prepare(entity_type, id, data, timestamp_ms)?;
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
    pub fn db_delete_prepare(
        &mut self,
        entity_type: &str,
        id: &str,
    ) -> Result<(super::db::DbEntity, ReplicationWrite), super::db::DbDataStoreError> {
        self.stores.db_delete_replicated(entity_type, id)
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub async fn db_delete(
        &mut self,
        entity_type: &str,
        id: &str,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) = self.db_delete_prepare(entity_type, id)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    pub async fn db_commit(
        &mut self,
        write: ReplicationWrite,
        outbox: super::super::store_manager::outbox::OutboxPayload,
    ) {
        self.write_or_forward_with_outbox(write, outbox).await;
    }

    pub(crate) async fn db_commit_with_cascade(
        &mut self,
        write: ReplicationWrite,
        outbox: OutboxPayload,
        cascade: &CascadeOutboxPayload,
    ) {
        self.write_or_forward_with_outbox_entries(
            write,
            outbox,
            &[(cascade.key.clone(), cascade.value.clone())],
        )
        .await;
    }

    #[must_use]
    pub fn db_get(&self, entity_type: &str, id: &str) -> Option<super::db::DbEntity> {
        self.stores.db_get(entity_type, id)
    }

    #[must_use]
    pub fn db_list(&self, entity_type: &str) -> Vec<super::db::DbEntity> {
        self.stores.db_list(entity_type)
    }

    #[must_use]
    pub fn db_list_primary_only(&self, entity_type: &str) -> Vec<super::db::DbEntity> {
        self.stores
            .db_list(entity_type)
            .into_iter()
            .filter(|e| self.is_primary_for_partition(e.partition()))
            .collect()
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
    ) -> Option<super::PendingConstraintWork> {
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
                (
                    self.handle_json_read_local(&request.entity, id, &request.payload),
                    None,
                )
            }
            JsonDbOp::Update => {
                let id = request.id.as_deref().unwrap_or("");
                self.handle_json_update_local(&request.entity, id, &request.payload, request, from)
                    .await
            }
            JsonDbOp::Delete => {
                let id = request.id.as_deref().unwrap_or("");
                self.handle_json_delete_local(&request.entity, id, request, from)
                    .await
            }
            JsonDbOp::Create => {
                self.handle_json_create_local(
                    partition,
                    &request.entity,
                    &request.payload,
                    request,
                    from,
                )
                .await
            }
            JsonDbOp::List => (
                self.handle_json_list_local(&request.entity, &request.payload),
                None,
            ),
        };

        if let Some(pending) = pending {
            return Some(pending);
        }

        if !request.response_topic.is_empty() {
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
        }

        None
    }

    pub(crate) async fn handle_json_db_response(&mut self, response: &JsonDbResponse) {
        let scatter_prefix = format!("_mqdb/scatter/{}/", self.node_id.get());
        let cascade_prefix = format!("_mqdb/cascade_ack/{}/", self.node_id.get());

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

        if let Some(request_id_str) = response.response_topic.strip_prefix(&cascade_prefix)
            && let Ok(request_id) = request_id_str.parse::<u64>()
        {
            tracing::debug!(request_id, "matched cascade ack response");
            self.pending_constraints.resolve_cascade_ack(request_id);
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

    fn handle_json_read_local(&self, entity: &str, id: &str, payload: &[u8]) -> Vec<u8> {
        let projection = parse_projection(payload);

        if let Some(ref fields) = projection
            && let Some(cluster_schema) = self.stores.schema_get(entity)
            && let Some(msg) =
                validate_projection_against_schema(&cluster_schema.data, entity, fields)
        {
            return Self::json_error(400, &msg);
        }

        match self.stores.db_get(entity, id) {
            Some(db_entity) => {
                let data_json: serde_json::Value =
                    serde_json::from_slice(&db_entity.data).unwrap_or(serde_json::Value::Null);
                let data_json = if let Some(ref fields) = projection {
                    crate::Database::project_fields(data_json, fields)
                } else {
                    data_json
                };
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
    ) -> (Vec<u8>, Option<super::PendingConstraintWork>) {
        let mut data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return (Self::json_error(400, "invalid JSON payload"), None),
        };

        let fk_expected = data
            .as_object_mut()
            .and_then(|obj| obj.remove("__mqdb_fk_expected"))
            .and_then(|v| v.as_str().map(String::from));

        let (old_data, merged_data) = if let Some(existing) = self.db_get(entity, id) {
            let existing_data: Value = serde_json::from_slice(&existing.data)
                .unwrap_or(Value::Object(serde_json::Map::new()));

            if let Some(ref expected) = fk_expected
                && let Some((field, value)) = expected.split_once('=')
            {
                let current_fk = existing_data
                    .get(field)
                    .and_then(super::fk::extract_fk_string_value);
                if current_fk.as_deref() != Some(value) {
                    tracing::debug!(
                        entity,
                        id,
                        field,
                        expected = value,
                        actual = ?current_fk,
                        "skipping set-null: FK field was updated since cascade"
                    );
                    let result = serde_json::json!({
                        "status": "ok",
                        "id": id,
                        "entity": entity,
                        "data": existing_data
                    });
                    return (serde_json::to_vec(&result).unwrap_or_default(), None);
                }
            }

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

        let fk_result = match self.start_fk_existence_check(entity, &merged_data).await {
            Ok(r) => r,
            Err(msg) => return (Self::json_error(409, &msg), None),
        };

        if !fk_result.pending_remote.is_empty() {
            let (new_diff, old_diff) =
                self.compute_unique_field_diffs(entity, &old_data, &merged_data);
            return (
                Vec::new(),
                Some(super::PendingConstraintWork::Fk(PendingFkWork {
                    pending_checks: fk_result.pending_remote,
                    continuation: FkCheckContinuation::UpdateFromNodeController {
                        from,
                        entity: entity.to_string(),
                        id: id.to_string(),
                        merged_data,
                        partition,
                        request_id,
                        new_diff,
                        old_diff,
                        response_topic: request.response_topic.clone(),
                        correlation_data: request.correlation_data.clone(),
                    },
                })),
            );
        }

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
                            Some(super::PendingConstraintWork::Unique(PendingUniqueWork {
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
                            })),
                        );
                    }
                }
            }
        }

        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();

        let result = match self.db_update_prepare(entity, id, &data_bytes, now_ms) {
            Ok((db_entity, write)) => {
                let event =
                    ChangeEvent::update(entity.to_string(), id.to_string(), merged_data.clone());
                let outbox = build_change_event_outbox(&event);
                self.db_commit(write, outbox.clone()).await;
                self.publish_and_deliver_change_event(event, &outbox.operation_id)
                    .await;
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

    async fn handle_json_delete_local(
        &mut self,
        entity: &str,
        id: &str,
        request: &JsonDbRequest,
        from: NodeId,
    ) -> (Vec<u8>, Option<super::PendingConstraintWork>) {
        let (local_results, pending_remote) = match self.start_fk_reverse_lookup(entity, id).await {
            Ok(pair) => pair,
            Err(msg) => return (Self::json_error(409, &msg), None),
        };

        if !pending_remote.is_empty() {
            return (
                Vec::new(),
                Some(super::PendingConstraintWork::FkDelete(
                    PendingFkDeleteWork {
                        local_results,
                        pending_lookups: pending_remote,
                        continuation: FkDeleteContinuation::DeleteFromNodeController {
                            from,
                            entity: entity.to_string(),
                            id: id.to_string(),
                            response_topic: request.response_topic.clone(),
                            correlation_data: request.correlation_data.clone(),
                        },
                    },
                )),
            );
        }

        let all_results = match self.collect_local_cascade(entity, id, local_results) {
            Ok(results) => results,
            Err(msg) => return (Self::json_error(409, &msg), None),
        };
        let effects = self.prepare_fk_side_effects(&all_results);
        let (cascade, ack_receivers) = self.execute_fk_side_effects(&effects).await;
        (
            self.execute_json_delete(entity, id, cascade.as_ref(), ack_receivers)
                .await,
            None,
        )
    }

    async fn execute_json_delete(
        &mut self,
        entity: &str,
        id: &str,
        cascade: Option<&CascadeOutboxPayload>,
        ack_receivers: Vec<oneshot::Receiver<bool>>,
    ) -> Vec<u8> {
        match self.db_delete_prepare(entity, id) {
            Ok((db_entity, write)) => {
                let data: Value = serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                let event = ChangeEvent::delete(entity.to_string(), id.to_string(), data);
                let outbox = build_change_event_outbox(&event);
                if let Some(cas) = cascade {
                    self.db_commit_with_cascade(write, outbox.clone(), cas)
                        .await;
                } else {
                    self.db_commit(write, outbox.clone()).await;
                }
                self.publish_and_deliver_change_event(event, &outbox.operation_id)
                    .await;
                if let Some(cas) = cascade {
                    spawn_cascade_ack_waiter(
                        self.stores.cluster_outbox().cloned(),
                        cas.operation_id.clone(),
                        ack_receivers,
                        true,
                    );
                }
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

    pub(crate) fn prepare_fk_side_effects(
        &self,
        results: &[super::fk::FkReverseLookupResult],
    ) -> Vec<CascadeSideEffect> {
        use crate::cluster::db::{OnDeleteAction, data_partition};
        let mut effects = Vec::new();
        for r in results {
            match r.on_delete {
                OnDeleteAction::Restrict => {}
                OnDeleteAction::Cascade => {
                    for child_id in &r.referencing_ids {
                        let partition = data_partition(&r.source_entity, child_id);
                        if self.is_primary_for_partition(partition) {
                            effects.push(CascadeSideEffect::LocalDelete {
                                entity: r.source_entity.clone(),
                                id: child_id.clone(),
                            });
                        } else {
                            effects.push(CascadeSideEffect::RemoteDelete {
                                partition,
                                entity: r.source_entity.clone(),
                                id: child_id.clone(),
                            });
                        }
                    }
                }
                OnDeleteAction::SetNull => {
                    for child_id in &r.referencing_ids {
                        let partition = data_partition(&r.source_entity, child_id);
                        if self.is_primary_for_partition(partition) {
                            let Some(existing) = self.db_get(&r.source_entity, child_id) else {
                                continue;
                            };
                            let Ok(mut data) =
                                serde_json::from_slice::<serde_json::Value>(&existing.data)
                            else {
                                continue;
                            };
                            if let Some(obj) = data.as_object_mut() {
                                obj.insert(r.source_field.clone(), serde_json::Value::Null);
                                let v = obj
                                    .get("_version")
                                    .and_then(serde_json::Value::as_u64)
                                    .unwrap_or(0);
                                obj.insert(
                                    "_version".to_string(),
                                    serde_json::Value::Number((v + 1).into()),
                                );
                            }
                            let updated_data = serde_json::to_vec(&data).unwrap_or_default();
                            effects.push(CascadeSideEffect::LocalSetNull {
                                entity: r.source_entity.clone(),
                                id: child_id.clone(),
                                updated_data,
                            });
                        } else {
                            effects.push(CascadeSideEffect::RemoteSetNull {
                                partition,
                                entity: r.source_entity.clone(),
                                id: child_id.clone(),
                                field: r.source_field.clone(),
                                expected_value: r.target_id.clone(),
                            });
                        }
                    }
                }
            }
        }
        effects
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn execute_fk_side_effects(
        &mut self,
        effects: &[CascadeSideEffect],
    ) -> (Option<CascadeOutboxPayload>, Vec<oneshot::Receiver<bool>>) {
        let now_ms = Self::current_time_ms();
        let mut remote_ops = Vec::new();

        for effect in effects {
            match effect {
                CascadeSideEffect::LocalDelete { entity, id } => {
                    if let Ok((db_entity, write)) = self.db_delete_prepare(entity, id) {
                        let data: Value =
                            serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                        let event = ChangeEvent::delete(entity.clone(), id.clone(), data);
                        let outbox = build_change_event_outbox(&event);
                        self.db_commit(write, outbox.clone()).await;
                        self.publish_and_deliver_change_event(event, &outbox.operation_id)
                            .await;
                    }
                }
                CascadeSideEffect::LocalSetNull {
                    entity,
                    id,
                    updated_data,
                } => {
                    if let Ok((_, write)) = self.db_update_prepare(entity, id, updated_data, now_ms)
                    {
                        let data_json: serde_json::Value =
                            serde_json::from_slice(updated_data).unwrap_or(Value::Null);
                        let event = ChangeEvent::update(entity.clone(), id.clone(), data_json);
                        let outbox = build_change_event_outbox(&event);
                        self.db_commit(write, outbox.clone()).await;
                        self.publish_and_deliver_change_event(event, &outbox.operation_id)
                            .await;
                    }
                }
                CascadeSideEffect::RemoteDelete { entity, id, .. } => {
                    remote_ops.push(CascadeRemoteOp::Delete {
                        entity: entity.clone(),
                        id: id.clone(),
                    });
                }
                CascadeSideEffect::RemoteSetNull {
                    entity,
                    id,
                    field,
                    expected_value,
                    ..
                } => {
                    remote_ops.push(CascadeRemoteOp::SetNull {
                        entity: entity.clone(),
                        id: id.clone(),
                        field: field.clone(),
                        expected_value: expected_value.clone(),
                    });
                }
            }
        }

        if remote_ops.is_empty() {
            return (None, Vec::new());
        }

        let cascade_outbox =
            CascadeOutboxPayload::new(uuid::Uuid::new_v4().to_string(), &remote_ops);

        let mut ack_receivers = Vec::new();

        for effect in effects {
            match effect {
                CascadeSideEffect::RemoteDelete {
                    partition,
                    entity,
                    id,
                } => {
                    if self.is_primary_for_partition(*partition) {
                        if let Ok((db_entity, write)) = self.db_delete_prepare(entity, id) {
                            let data: Value =
                                serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                            let event = ChangeEvent::delete(entity.clone(), id.clone(), data);
                            let outbox = build_change_event_outbox(&event);
                            self.db_commit(write, outbox.clone()).await;
                            self.publish_and_deliver_change_event(event, &outbox.operation_id)
                                .await;
                        }
                    } else if let Some(rx) = self
                        .send_cascade_request(*partition, JsonDbOp::Delete, entity, id, &[])
                        .await
                    {
                        ack_receivers.push(rx);
                    }
                }
                CascadeSideEffect::RemoteSetNull {
                    partition,
                    entity,
                    id,
                    field,
                    expected_value,
                } => {
                    if self.is_primary_for_partition(*partition) {
                        self.execute_local_cascade_set_null(
                            entity,
                            id,
                            field,
                            expected_value,
                            now_ms,
                        )
                        .await;
                    } else if let Some(rx) = self
                        .send_cascade_set_null_request(
                            *partition,
                            entity,
                            id,
                            field,
                            expected_value,
                        )
                        .await
                    {
                        ack_receivers.push(rx);
                    }
                }
                CascadeSideEffect::LocalDelete { .. } | CascadeSideEffect::LocalSetNull { .. } => {}
            }
        }

        (Some(cascade_outbox), ack_receivers)
    }

    pub(crate) async fn send_cascade_request(
        &self,
        partition: PartitionId,
        op: JsonDbOp,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Option<oneshot::Receiver<bool>> {
        let Some(primary) = self.partition_map.primary(partition) else {
            tracing::debug!(
                partition = partition.get(),
                entity,
                id,
                "cascade request skipped: no primary for partition"
            );
            return None;
        };
        if primary == self.node_id {
            tracing::debug!(
                partition = partition.get(),
                entity,
                id,
                "cascade request skipped: primary is self"
            );
            return None;
        }
        let request_id = self.pending_constraints.allocate_cascade_id();
        let (tx, rx) = oneshot::channel();
        self.pending_constraints.insert_cascade_ack(request_id, tx);

        let response_topic = format!("_mqdb/cascade_ack/{}/{request_id}", self.node_id.get());
        let request = JsonDbRequest::new(
            request_id,
            op,
            entity.to_string(),
            Some(id.to_string()),
            payload.to_vec(),
            response_topic,
            None,
            None,
        );
        let _ = self
            .transport
            .send(
                primary,
                ClusterMessage::JsonDbRequest { partition, request },
            )
            .await;
        Some(rx)
    }

    pub(crate) async fn send_cascade_set_null_request(
        &self,
        partition: PartitionId,
        entity: &str,
        id: &str,
        field: &str,
        expected_value: &str,
    ) -> Option<oneshot::Receiver<bool>> {
        let Some(primary) = self.partition_map.primary(partition) else {
            tracing::debug!(
                partition = partition.get(),
                entity,
                id,
                "cascade set-null skipped: no primary for partition"
            );
            return None;
        };
        if primary == self.node_id {
            tracing::debug!(
                partition = partition.get(),
                entity,
                id,
                "cascade set-null skipped: primary is self"
            );
            return None;
        }
        let request_id = self.pending_constraints.allocate_cascade_id();
        let (tx, rx) = oneshot::channel();
        self.pending_constraints.insert_cascade_ack(request_id, tx);

        let response_topic = format!("_mqdb/cascade_ack/{}/{request_id}", self.node_id.get());
        let payload = serde_json::json!({
            field: serde_json::Value::Null,
            "__mqdb_fk_expected": format!("{field}={expected_value}"),
        });
        let bytes = serde_json::to_vec(&payload).unwrap_or_default();
        let request = JsonDbRequest::new(
            request_id,
            JsonDbOp::Update,
            entity.to_string(),
            Some(id.to_string()),
            bytes,
            response_topic,
            None,
            None,
        );
        let _ = self
            .transport
            .send(
                primary,
                ClusterMessage::JsonDbRequest { partition, request },
            )
            .await;
        Some(rx)
    }

    pub(crate) async fn publish_and_deliver_change_event(
        &self,
        event: ChangeEvent,
        operation_id: &str,
    ) {
        let topic = event.event_topic(0);
        let user_properties: Vec<(String, String)> = event
            .client_id
            .as_ref()
            .map(|cid| vec![("x-origin-client-id".to_string(), cid.clone())])
            .unwrap_or_default();
        if let Ok(payload) = serde_json::to_vec(&event) {
            self.transport
                .queue_local_publish_with_properties(topic, payload, 1, user_properties)
                .await;
        }
        if let Some(outbox) = self.stores.cluster_outbox() {
            let _ = outbox.mark_delivered(operation_id);
        }
    }

    pub(crate) async fn execute_local_cascade_set_null(
        &mut self,
        entity: &str,
        id: &str,
        field: &str,
        expected_value: &str,
        now_ms: u64,
    ) {
        let Some(existing) = self.stores.db_get(entity, id) else {
            return;
        };
        let Ok(mut record) = serde_json::from_slice::<Value>(&existing.data) else {
            return;
        };
        let Some(obj) = record.as_object_mut() else {
            return;
        };
        let current = obj.get(field).and_then(|v| {
            if v.is_string() {
                v.as_str().map(String::from)
            } else {
                Some(v.to_string())
            }
        });
        if current.as_deref() != Some(expected_value) {
            return;
        }
        obj.insert(field.to_string(), Value::Null);
        let updated_bytes = serde_json::to_vec(&record).unwrap_or_default();
        if let Ok((_, write)) = self.db_update_prepare(entity, id, &updated_bytes, now_ms) {
            let event = ChangeEvent::update(entity.to_string(), id.to_string(), record);
            let outbox = build_change_event_outbox(&event);
            self.db_commit(write, outbox.clone()).await;
            self.publish_and_deliver_change_event(event, &outbox.operation_id)
                .await;
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
    ) -> (Vec<u8>, Option<super::PendingConstraintWork>) {
        let mut data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return (Self::json_error(400, "invalid JSON payload"), None),
        };

        Self::apply_ttl_expiry(&mut data);

        if let serde_json::Value::Object(ref mut obj) = data {
            obj.remove("_version");
            obj.remove("__mqdb_fk_expected");
            obj.insert("_version".to_string(), serde_json::Value::Number(1.into()));
        }

        let id = if let Some(client_id) = data.get("id").and_then(serde_json::Value::as_str) {
            client_id.to_string()
        } else {
            self.generate_id_for_partition(entity, partition, payload)
        };
        let request_id = uuid::Uuid::new_v4().to_string();
        let now_ms = Self::current_time_ms();

        let fk_result = match self.start_fk_existence_check(entity, &data).await {
            Ok(r) => r,
            Err(msg) => return (Self::json_error(409, &msg), None),
        };

        if !fk_result.pending_remote.is_empty() {
            return (
                Vec::new(),
                Some(super::PendingConstraintWork::Fk(PendingFkWork {
                    pending_checks: fk_result.pending_remote,
                    continuation: FkCheckContinuation::CreateFromNodeController {
                        from,
                        entity: entity.to_string(),
                        id,
                        data,
                        partition,
                        request_id,
                        response_topic: request.response_topic.clone(),
                        correlation_data: request.correlation_data.clone(),
                    },
                })),
            );
        }

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
                Some(super::PendingConstraintWork::Unique(PendingUniqueWork {
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
                })),
            );
        }

        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();

        let result = match self.db_create_prepare(entity, &id, &data_bytes, now_ms) {
            Ok((db_entity, write)) => {
                self.commit_all_reservations(
                    entity,
                    &request_id,
                    &local_reserved,
                    &remote_reserved,
                )
                .await;
                let event = ChangeEvent::create(entity.to_string(), id.clone(), data.clone());
                let outbox = build_change_event_outbox(&event);
                self.db_commit(write, outbox.clone()).await;
                self.publish_and_deliver_change_event(event, &outbox.operation_id)
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

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn complete_pending_fk_work(
        &mut self,
        fk_ok: bool,
        fk_error: Option<String>,
        continuation: FkCheckContinuation,
    ) {
        if !fk_ok {
            let msg = fk_error.unwrap_or_else(|| "FK constraint violation".to_string());
            let payload = Self::json_error(409, &msg);
            let (FkCheckContinuation::CreateFromNodeController {
                from,
                response_topic,
                correlation_data,
                ..
            }
            | FkCheckContinuation::UpdateFromNodeController {
                from,
                response_topic,
                correlation_data,
                ..
            }) = continuation
            else {
                return;
            };
            let response = JsonDbResponse::new(0, payload, response_topic, correlation_data);
            let _ = self
                .transport
                .send(from, ClusterMessage::JsonDbResponse(response))
                .await;
            return;
        }

        match continuation {
            FkCheckContinuation::CreateFromNodeController {
                from,
                entity,
                id,
                data,
                partition,
                request_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let now_ms = Self::current_time_ms();
                let unique_fields = self.stores.constraint_get_unique_fields(&entity);
                let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
                let mut remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

                for field in &unique_fields {
                    let value = match data.get(field) {
                        Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                        None => continue,
                    };

                    let params = UniqueReservationParams {
                        entity: &entity,
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
                                match tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                                    .await
                                {
                                    Ok(Ok(
                                        UniqueReserveStatus::Reserved
                                        | UniqueReserveStatus::AlreadyReserved,
                                    )) => {
                                        remote_reserved.push((
                                            field.clone(),
                                            value.clone(),
                                            target_node,
                                        ));
                                        false
                                    }
                                    _ => true,
                                }
                            }
                            Err(_) => true,
                        }
                    } else {
                        true
                    };

                    if is_conflict {
                        self.release_all_reservations(
                            &entity,
                            &request_id,
                            &local_reserved,
                            &remote_reserved,
                        )
                        .await;
                        let payload = Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{field}'"),
                        );
                        let response =
                            JsonDbResponse::new(0, payload, response_topic, correlation_data);
                        let _ = self
                            .transport
                            .send(from, ClusterMessage::JsonDbResponse(response))
                            .await;
                        return;
                    }
                }

                let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
                let response_payload =
                    match self.db_create_prepare(&entity, &id, &data_bytes, now_ms) {
                        Ok((db_entity, write)) => {
                            self.commit_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            let event =
                                ChangeEvent::create(entity.clone(), id.clone(), data.clone());
                            let outbox = build_change_event_outbox(&event);
                            self.db_commit(write, outbox.clone()).await;
                            self.publish_and_deliver_change_event(event, &outbox.operation_id)
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
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            Self::json_error(409, "entity already exists")
                        }
                        Err(_) => {
                            self.release_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            Self::json_error(500, "internal error")
                        }
                    };

                let response =
                    JsonDbResponse::new(0, response_payload, response_topic, correlation_data);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::JsonDbResponse(response))
                    .await;
            }
            FkCheckContinuation::UpdateFromNodeController {
                from,
                entity,
                id,
                merged_data,
                partition,
                request_id,
                new_diff,
                old_diff,
                response_topic,
                correlation_data,
                ..
            } => {
                let now_ms = Self::current_time_ms();
                let has_unique_changes = new_diff.as_object().is_some_and(|m| !m.is_empty());

                let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
                let mut remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

                if has_unique_changes {
                    let unique_fields = self.stores.constraint_get_unique_fields(&entity);
                    for field in &unique_fields {
                        let value = match new_diff.get(field) {
                            Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                            None => continue,
                        };

                        let params = UniqueReservationParams {
                            entity: &entity,
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
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        rx,
                                    )
                                    .await
                                    {
                                        Ok(Ok(
                                            UniqueReserveStatus::Reserved
                                            | UniqueReserveStatus::AlreadyReserved,
                                        )) => {
                                            remote_reserved.push((
                                                field.clone(),
                                                value.clone(),
                                                target_node,
                                            ));
                                            false
                                        }
                                        _ => true,
                                    }
                                }
                                Err(_) => true,
                            }
                        } else {
                            true
                        };

                        if is_conflict {
                            self.release_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            let payload = Self::json_error(
                                409,
                                &format!("unique constraint violation on field '{field}'"),
                            );
                            let response =
                                JsonDbResponse::new(0, payload, response_topic, correlation_data);
                            let _ = self
                                .transport
                                .send(from, ClusterMessage::JsonDbResponse(response))
                                .await;
                            return;
                        }
                    }
                }

                let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
                let response_payload =
                    match self.db_update_prepare(&entity, &id, &data_bytes, now_ms) {
                        Ok((db_entity, write)) => {
                            self.commit_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            self.release_unique_constraints(
                                &entity, &id, &old_diff, partition, &id, now_ms,
                            )
                            .await;
                            let event = ChangeEvent::update(
                                entity.clone(),
                                id.clone(),
                                merged_data.clone(),
                            );
                            let outbox = build_change_event_outbox(&event);
                            self.db_commit(write, outbox.clone()).await;
                            self.publish_and_deliver_change_event(event, &outbox.operation_id)
                                .await;
                            let result = serde_json::json!({
                                "status": "ok",
                                "id": db_entity.id_str(),
                                "entity": entity,
                                "data": merged_data
                            });
                            serde_json::to_vec(&result).unwrap_or_default()
                        }
                        Err(super::db::DbDataStoreError::NotFound) => {
                            self.release_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            Self::json_error(404, &format!("entity not found: {entity} id={id}"))
                        }
                        Err(_) => {
                            self.release_all_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &remote_reserved,
                            )
                            .await;
                            Self::json_error(500, "internal error")
                        }
                    };

                let response =
                    JsonDbResponse::new(0, response_payload, response_topic, correlation_data);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::JsonDbResponse(response))
                    .await;
            }
            FkCheckContinuation::CreateFromDbHandler { .. }
            | FkCheckContinuation::UpdateFromDbHandler { .. } => {
                tracing::error!(
                    "complete_pending_fk_work received misrouted continuation (expected *FromNodeController)"
                );
            }
        }
    }

    pub(crate) async fn complete_pending_fk_delete_work(
        &mut self,
        restrict_error: Option<String>,
        side_effects: Vec<super::fk::FkReverseLookupResult>,
        continuation: FkDeleteContinuation,
    ) {
        if let Some(msg) = restrict_error {
            let payload = Self::json_error(409, &msg);
            let FkDeleteContinuation::DeleteFromNodeController {
                from,
                response_topic,
                correlation_data,
                ..
            } = continuation
            else {
                return;
            };
            if !response_topic.is_empty() {
                let response = JsonDbResponse::new(0, payload, response_topic, correlation_data);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::JsonDbResponse(response))
                    .await;
            }
            return;
        }

        let FkDeleteContinuation::DeleteFromNodeController {
            from,
            entity,
            id,
            response_topic,
            correlation_data,
        } = continuation
        else {
            return;
        };

        let effects = self.prepare_fk_side_effects(&side_effects);
        let (cascade, ack_receivers) = self.execute_fk_side_effects(&effects).await;
        let response_payload = self
            .execute_json_delete(&entity, &id, cascade.as_ref(), ack_receivers)
            .await;
        if !response_topic.is_empty() {
            let response =
                JsonDbResponse::new(0, response_payload, response_topic, correlation_data);
            let _ = self
                .transport
                .send(from, ClusterMessage::JsonDbResponse(response))
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
        super::db::generate_id_for_partition(self.node_id.get(), entity, partition, data)
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
