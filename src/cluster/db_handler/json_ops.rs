// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::PartitionId;
use super::super::db::{self, data_partition};
use super::super::db_topic::DbTopicOperation;
use super::super::node_controller::{NodeController, PendingUniqueWork, UniqueCheckContinuation};
use super::super::protocol::JsonDbOp;
use super::super::transport::ClusterTransport;
use super::DbRequestHandler;
use crate::events::ChangeEvent;
use serde_json::{Value, json};

pub(super) enum JsonOpResult {
    Response(Vec<u8>),
    NoResponse,
    PendingUniqueCheck(Box<PendingUniqueWork>),
}

#[allow(clippy::unused_self, clippy::too_many_arguments)]
impl DbRequestHandler {
    fn validate_against_schema<T: ClusterTransport>(
        controller: &NodeController<T>,
        entity: &str,
        data: &Value,
    ) -> Option<Vec<u8>> {
        let cluster_schema = controller.stores().schema_get(entity)?;
        let schema: crate::schema::Schema = match serde_json::from_slice(&cluster_schema.data) {
            Ok(s) => s,
            Err(_) => return None,
        };
        if let Err(e) = schema.validate(data) {
            return Some(Self::json_error(400, &e.to_string()));
        }
        None
    }

    fn validate_filter_fields<T: ClusterTransport>(
        controller: &NodeController<T>,
        entity: &str,
        filters: &[crate::Filter],
    ) -> Option<Vec<u8>> {
        let cluster_schema = controller.stores().schema_get(entity)?;
        let schema: crate::schema::Schema = match serde_json::from_slice(&cluster_schema.data) {
            Ok(s) => s,
            Err(_) => return None,
        };
        for filter in filters {
            if filter.field != "id" && !schema.fields.contains_key(&filter.field) {
                return Some(Self::json_error(
                    400,
                    &format!(
                        "schema violation for '{}': filter field '{}' does not exist in schema",
                        entity, filter.field
                    ),
                ));
            }
        }
        None
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn handle_json_operation<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        operation: &DbTopicOperation,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
        client_id: Option<&str>,
    ) -> JsonOpResult {
        match operation {
            DbTopicOperation::JsonCreate { entity } => {
                self.handle_json_create(
                    controller,
                    entity,
                    payload,
                    response_topic,
                    correlation_data,
                    sender,
                    client_id,
                )
                .await
            }
            DbTopicOperation::JsonRead { entity, id } => {
                if let Some(uid) = sender
                    && !self.ownership.is_admin(uid)
                    && let Some(owner_field) = self.ownership.owner_field(entity)
                    && let Some(err) =
                        self.check_cluster_ownership(controller, entity, id, owner_field, uid)
                {
                    return JsonOpResult::Response(err);
                }
                self.handle_json_read(
                    controller,
                    entity,
                    id,
                    response_topic,
                    correlation_data,
                    sender,
                )
                .await
            }
            DbTopicOperation::JsonUpdate { entity, id } => {
                let stripped_payload;
                let effective_payload = if let Some(uid) = sender
                    && !self.ownership.is_admin(uid)
                    && let Some(owner_field) = self.ownership.owner_field(entity)
                {
                    if let Some(err) =
                        self.check_cluster_ownership(controller, entity, id, owner_field, uid)
                    {
                        return JsonOpResult::Response(err);
                    }
                    stripped_payload = Self::strip_field_from_payload(payload, owner_field);
                    stripped_payload.as_deref().unwrap_or(payload)
                } else {
                    payload
                };
                let partition = data_partition(entity, id);
                if !controller.is_local_partition(partition) {
                    let forwarded = controller
                        .forward_json_db_request(
                            partition,
                            JsonDbOp::Update,
                            entity,
                            Some(id),
                            effective_payload,
                            response_topic,
                            correlation_data,
                            sender,
                        )
                        .await;
                    return if forwarded {
                        JsonOpResult::NoResponse
                    } else {
                        JsonOpResult::Response(Self::json_error(
                            503,
                            "partition not local and forwarding failed",
                        ))
                    };
                }
                self.handle_json_update(
                    controller,
                    entity,
                    id,
                    effective_payload,
                    sender,
                    client_id,
                    response_topic,
                    correlation_data,
                )
                .await
            }
            DbTopicOperation::JsonDelete { entity, id } => {
                if let Some(uid) = sender
                    && !self.ownership.is_admin(uid)
                    && let Some(owner_field) = self.ownership.owner_field(entity)
                    && let Some(err) =
                        self.check_cluster_ownership(controller, entity, id, owner_field, uid)
                {
                    return JsonOpResult::Response(err);
                }
                let partition = data_partition(entity, id);
                if !controller.is_local_partition(partition) {
                    let forwarded = controller
                        .forward_json_db_request(
                            partition,
                            JsonDbOp::Delete,
                            entity,
                            Some(id),
                            &[],
                            response_topic,
                            correlation_data,
                            sender,
                        )
                        .await;
                    return if forwarded {
                        JsonOpResult::NoResponse
                    } else {
                        JsonOpResult::Response(Self::json_error(
                            503,
                            "partition not local and forwarding failed",
                        ))
                    };
                }
                match self
                    .handle_json_delete(controller, entity, id, sender, client_id)
                    .await
                {
                    Some(payload) => JsonOpResult::Response(payload),
                    None => JsonOpResult::NoResponse,
                }
            }
            DbTopicOperation::JsonList { entity } => {
                match self
                    .handle_json_list(controller, entity, payload, response_topic, sender)
                    .await
                {
                    Some(payload) => JsonOpResult::Response(payload),
                    None => JsonOpResult::NoResponse,
                }
            }
            _ => JsonOpResult::NoResponse,
        }
    }

    fn strip_field_from_payload(payload: &[u8], field: &str) -> Option<Vec<u8>> {
        let mut data: Value = serde_json::from_slice(payload).ok()?;
        if let Value::Object(ref mut map) = data
            && map.remove(field).is_some()
        {
            return serde_json::to_vec(&data).ok();
        }
        None
    }

    fn check_cluster_ownership<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        owner_field: &str,
        sender: &str,
    ) -> Option<Vec<u8>> {
        let existing = controller.db_get(entity, id)?;
        let data: Value = serde_json::from_slice(&existing.data).ok()?;
        if let Some(owner_value) = data.get(owner_field)
            && owner_value.as_str() != Some(sender)
        {
            return Some(Self::json_error(
                403,
                &format!("user '{sender}' does not own {entity}/{id}"),
            ));
        }
        None
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn handle_json_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
        client_id: Option<&str>,
    ) -> JsonOpResult {
        let data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return JsonOpResult::Response(Self::json_error(400, "invalid JSON payload")),
        };

        if let Some(err) = Self::validate_against_schema(controller, entity, &data) {
            return JsonOpResult::Response(err);
        }

        let (partition, id) = if let Some(client_id) = data.get("id").and_then(Value::as_str) {
            (data_partition(entity, client_id), client_id.to_string())
        } else {
            let p = controller.pick_partition_for_create();
            (p, self.generate_id_for_partition(entity, p, payload))
        };

        if !controller.is_local_partition(partition) {
            return match self
                .try_forward_create(
                    controller,
                    partition,
                    entity,
                    payload,
                    response_topic,
                    correlation_data,
                    sender,
                )
                .await
            {
                Some(payload) => JsonOpResult::Response(payload),
                None => JsonOpResult::NoResponse,
            };
        }

        self.execute_local_create(
            controller,
            entity,
            &id,
            &data,
            partition,
            sender,
            client_id,
            response_topic,
            correlation_data,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_forward_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
    ) -> Option<Vec<u8>> {
        let forwarded = controller
            .forward_json_db_request(
                partition,
                JsonDbOp::Create,
                entity,
                None,
                payload,
                response_topic,
                correlation_data,
                sender,
            )
            .await;
        if forwarded {
            tracing::debug!(partition = partition.get(), "json_create_forwarded");
            None
        } else {
            Some(Self::json_error(
                503,
                "partition not local and forwarding failed",
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_local_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        data: &Value,
        partition: PartitionId,
        sender: Option<&str>,
        client_id: Option<&str>,
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> JsonOpResult {
        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();

        let phase1 = match controller
            .start_unique_constraint_check(entity, id, data, partition, &request_id, now_ms)
            .await
        {
            Ok(p) => p,
            Err(conflict_field) => {
                return JsonOpResult::Response(Self::json_error(
                    409,
                    &format!("unique constraint violation on field '{conflict_field}'"),
                ));
            }
        };

        if !phase1.pending_remote.is_empty() {
            let data_bytes = serde_json::to_vec(data).unwrap_or_default();
            return JsonOpResult::PendingUniqueCheck(Box::new(PendingUniqueWork {
                phase1,
                continuation: UniqueCheckContinuation::CreateFromDbHandler {
                    entity: entity.to_string(),
                    id: id.to_string(),
                    data: data.clone(),
                    data_bytes,
                    partition,
                    request_id,
                    now_ms,
                    sender: sender.map(str::to_string),
                    client_id: client_id.map(str::to_string),
                    response_topic: response_topic.to_string(),
                    correlation_data: correlation_data.map(<[u8]>::to_vec),
                },
            }));
        }

        self.complete_create(
            controller,
            entity,
            id,
            data,
            partition,
            &request_id,
            now_ms,
            sender,
            client_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        data: &Value,
        partition: PartitionId,
        request_id: &str,
        now_ms: u64,
        sender: Option<&str>,
        client_id: Option<&str>,
    ) -> JsonOpResult {
        let data_bytes = serde_json::to_vec(data).unwrap_or_default();

        JsonOpResult::Response(
            match controller.db_create(entity, id, &data_bytes, now_ms).await {
                Ok(db_entity) => {
                    controller
                        .commit_unique_constraints(entity, id, data, partition, request_id, now_ms)
                        .await;
                    let scope = self.scope_config.resolve_scope(entity, data);
                    let event = ChangeEvent::create(
                        entity.to_string(),
                        db_entity.id_str().to_string(),
                        data.clone(),
                    )
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(scope);
                    self.publish_change_event(controller, event).await;
                    Self::json_success(entity, db_entity.id_str(), data)
                }
                Err(db::DbDataStoreError::AlreadyExists) => {
                    controller
                        .release_unique_constraints(entity, id, data, partition, request_id, now_ms)
                        .await;
                    Self::json_error(409, "entity already exists")
                }
                Err(_) => {
                    controller
                        .release_unique_constraints(entity, id, data, partition, request_id, now_ms)
                        .await;
                    Self::json_error(500, "internal error")
                }
            },
        )
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn handle_json_read<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
    ) -> JsonOpResult {
        let start = std::time::Instant::now();
        let partition = data_partition(entity, id);
        let can_serve = controller.can_serve_reads(partition);
        let node_id = controller.node_id().get();

        tracing::info!(
            node = node_id,
            partition = partition.get(),
            can_serve,
            entity,
            "json_read_start"
        );

        if !can_serve {
            let forwarded = controller
                .forward_json_db_request(
                    partition,
                    JsonDbOp::Read,
                    entity,
                    Some(id),
                    &[],
                    response_topic,
                    correlation_data,
                    sender,
                )
                .await;
            if forwarded {
                tracing::info!(
                    node = node_id,
                    partition = partition.get(),
                    elapsed_us = start.elapsed().as_micros() as u64,
                    forwarded = true,
                    "json_read_forwarded"
                );
                return JsonOpResult::NoResponse;
            }
            tracing::warn!(
                node = node_id,
                partition = partition.get(),
                elapsed_us = start.elapsed().as_micros() as u64,
                "json_read_forward_failed"
            );
            return JsonOpResult::Response(Self::json_error(
                503,
                "partition not local and forwarding failed",
            ));
        }

        let result = match controller.db_get(entity, id) {
            Some(db_entity) => {
                let data: Value = serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                let result = json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": data
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            None => Self::json_error(404, &format!("entity not found: {entity} id={id}")),
        };

        tracing::info!(
            node = node_id,
            partition = partition.get(),
            elapsed_us = start.elapsed().as_micros() as u64,
            forwarded = false,
            "json_read_complete"
        );

        JsonOpResult::Response(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_json_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
        client_id: Option<&str>,
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> JsonOpResult {
        let updates: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return JsonOpResult::Response(Self::json_error(400, "invalid JSON payload")),
        };

        self.execute_local_update(
            controller,
            entity,
            id,
            updates,
            sender,
            client_id,
            response_topic,
            correlation_data,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_local_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        updates: Value,
        sender: Option<&str>,
        client_id: Option<&str>,
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> JsonOpResult {
        let (old_data, merged_data) =
            match self.merge_with_existing(controller, entity, id, updates) {
                Ok(pair) => pair,
                Err(response) => return JsonOpResult::Response(response),
            };

        if let Some(err) = Self::validate_against_schema(controller, entity, &merged_data) {
            return JsonOpResult::Response(err);
        }

        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();
        let partition = data_partition(entity, id);

        let (new_diff, old_diff) =
            controller.compute_unique_field_diffs(entity, &old_data, &merged_data);
        let has_unique_changes = new_diff.as_object().is_some_and(|m| !m.is_empty());

        if has_unique_changes {
            let phase1 = match controller
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
                Ok(p) => p,
                Err(conflict_field) => {
                    return JsonOpResult::Response(Self::json_error(
                        409,
                        &format!("unique constraint violation on field '{conflict_field}'"),
                    ));
                }
            };

            if !phase1.pending_remote.is_empty() {
                let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
                return JsonOpResult::PendingUniqueCheck(Box::new(PendingUniqueWork {
                    phase1,
                    continuation: UniqueCheckContinuation::UpdateFromDbHandler {
                        entity: entity.to_string(),
                        id: id.to_string(),
                        merged_data,
                        data_bytes,
                        partition,
                        request_id,
                        now_ms,
                        new_diff,
                        old_diff,
                        sender: sender.map(str::to_string),
                        client_id: client_id.map(str::to_string),
                        response_topic: response_topic.to_string(),
                        correlation_data: correlation_data.map(<[u8]>::to_vec),
                    },
                }));
            }
        }

        self.complete_update(
            controller,
            entity,
            id,
            &merged_data,
            partition,
            &request_id,
            now_ms,
            has_unique_changes,
            &new_diff,
            &old_diff,
            sender,
            client_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        merged_data: &Value,
        partition: PartitionId,
        request_id: &str,
        now_ms: u64,
        has_unique_changes: bool,
        new_diff: &Value,
        old_diff: &Value,
        sender: Option<&str>,
        client_id: Option<&str>,
    ) -> JsonOpResult {
        let data_bytes = serde_json::to_vec(merged_data).unwrap_or_default();

        JsonOpResult::Response(
            match controller.db_update(entity, id, &data_bytes, now_ms).await {
                Ok(db_entity) => {
                    if has_unique_changes {
                        controller
                            .commit_unique_constraints(
                                entity, id, new_diff, partition, request_id, now_ms,
                            )
                            .await;
                        controller
                            .release_unique_constraints(entity, id, old_diff, partition, id, now_ms)
                            .await;
                    }
                    let scope = self.scope_config.resolve_scope(entity, merged_data);
                    let event = ChangeEvent::update(
                        entity.to_string(),
                        db_entity.id_str().to_string(),
                        merged_data.clone(),
                    )
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(scope);
                    self.publish_change_event(controller, event).await;
                    Self::json_success(entity, db_entity.id_str(), merged_data)
                }
                Err(db::DbDataStoreError::NotFound) => {
                    if has_unique_changes {
                        controller
                            .release_unique_constraints(
                                entity, id, new_diff, partition, request_id, now_ms,
                            )
                            .await;
                    }
                    Self::json_error(404, &format!("entity not found: {entity} id={id}"))
                }
                Err(_) => {
                    if has_unique_changes {
                        controller
                            .release_unique_constraints(
                                entity, id, new_diff, partition, request_id, now_ms,
                            )
                            .await;
                    }
                    Self::json_error(500, "internal error")
                }
            },
        )
    }

    fn merge_with_existing<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        updates: Value,
    ) -> Result<(Value, Value), Vec<u8>> {
        let Some(existing) = controller.db_get(entity, id) else {
            return Err(Self::json_error(
                404,
                &format!("entity not found: {entity} id={id}"),
            ));
        };

        let existing_data: Value =
            serde_json::from_slice(&existing.data).unwrap_or(Value::Object(serde_json::Map::new()));
        let old_data = existing_data.clone();
        let mut merged = existing_data;

        if let (Value::Object(existing_obj), Value::Object(update_obj)) = (&mut merged, updates) {
            for (key, value) in update_obj {
                existing_obj.insert(key, value);
            }
        }

        Ok((old_data, merged))
    }

    async fn handle_json_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        sender: Option<&str>,
        client_id: Option<&str>,
    ) -> Option<Vec<u8>> {
        let pre_delete_scope = if self.scope_config.is_empty() {
            None
        } else {
            controller
                .db_get(entity, id)
                .and_then(|e| serde_json::from_slice::<Value>(&e.data).ok())
                .and_then(|data| self.scope_config.resolve_scope(entity, &data))
        };

        match controller.db_delete(entity, id).await {
            Ok(_) => {
                let event = ChangeEvent::delete(entity.to_string(), id.to_string())
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(pre_delete_scope);
                self.publish_change_event(controller, event).await;
                let result = json!({
                    "status": "ok",
                    "id": id,
                    "entity": entity,
                    "deleted": true
                });
                Some(serde_json::to_vec(&result).unwrap_or_default())
            }
            Err(db::DbDataStoreError::NotFound) => Some(Self::json_error(
                404,
                &format!("entity not found: {entity} id={id}"),
            )),
            Err(_) => Some(Self::json_error(500, "internal error")),
        }
    }

    async fn handle_json_list<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
        response_topic: &str,
        sender: Option<&str>,
    ) -> Option<Vec<u8>> {
        let mut filters: Vec<crate::Filter> = if payload.is_empty() {
            Vec::new()
        } else if let Ok(data) = serde_json::from_slice::<Value>(payload) {
            data.get("filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        if !filters.is_empty()
            && let Some(err) = Self::validate_filter_fields(controller, entity, &filters)
        {
            return Some(err);
        }

        if let Some(uid) = sender
            && !self.ownership.is_admin(uid)
            && let Some(owner_field) = self.ownership.owner_field(entity)
        {
            filters.push(crate::Filter::new(
                owner_field.to_string(),
                crate::FilterOp::Eq,
                Value::String(uid.to_string()),
            ));
        }

        let sender_needs_filter = sender.is_some_and(|uid| !self.ownership.is_admin(uid))
            && self.ownership.owner_field(entity).is_some();
        let scatter_payload = if sender_needs_filter {
            let mut data: Value = if payload.is_empty() {
                json!({})
            } else {
                serde_json::from_slice(payload).unwrap_or(json!({}))
            };
            data["filters"] = serde_json::to_value(&filters).unwrap_or(json!([]));
            serde_json::to_vec(&data).unwrap_or_default()
        } else {
            payload.to_vec()
        };

        let has_remote_nodes = !controller.alive_nodes().is_empty();

        if has_remote_nodes {
            let started = controller
                .start_scatter_list_query(
                    entity,
                    &scatter_payload,
                    response_topic.to_string(),
                    filters.clone(),
                )
                .await;

            if started {
                return None;
            }
        }

        let entities = controller.db_list(entity);
        let items: Vec<Value> = entities
            .iter()
            .filter_map(|e| {
                let data: Value = serde_json::from_slice(&e.data).ok()?;
                if !filters.is_empty() && !NodeController::<T>::matches_filters(&data, &filters) {
                    return None;
                }
                Some(json!({
                    "id": e.id_str(),
                    "data": data
                }))
            })
            .collect();

        let result = json!({
            "status": "ok",
            "data": items
        });

        Some(serde_json::to_vec(&result).unwrap_or_default())
    }

    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    pub async fn complete_pending_unique_check<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        local_reserved: Vec<(String, Vec<u8>)>,
        remote_results: Result<
            Vec<(String, Vec<u8>, super::NodeId)>,
            (String, Vec<(String, Vec<u8>, super::NodeId)>),
        >,
        continuation: UniqueCheckContinuation,
    ) -> Option<super::super::db_handler::DbPublishResponse> {
        match continuation {
            UniqueCheckContinuation::CreateFromDbHandler {
                entity,
                id,
                data,
                partition,
                request_id,
                now_ms,
                sender,
                client_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let response_payload = match remote_results {
                    Err((conflict_field, confirmed)) => {
                        controller
                            .release_unique_check_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &confirmed,
                            )
                            .await;
                        Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{conflict_field}'"),
                        )
                    }
                    Ok(confirmed_remotes) => {
                        for (f, v, target) in &confirmed_remotes {
                            controller
                                .send_unique_commit_fire_and_forget(
                                    *target,
                                    &entity,
                                    f,
                                    v,
                                    &request_id,
                                )
                                .await;
                        }
                        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
                        match controller
                            .db_create(&entity, &id, &data_bytes, now_ms)
                            .await
                        {
                            Ok(db_entity) => {
                                controller
                                    .commit_unique_constraints(
                                        &entity,
                                        &id,
                                        &data,
                                        partition,
                                        &request_id,
                                        now_ms,
                                    )
                                    .await;
                                let scope = self.scope_config.resolve_scope(&entity, &data);
                                let event = ChangeEvent::create(
                                    entity.clone(),
                                    db_entity.id_str().to_string(),
                                    data.clone(),
                                )
                                .with_sender(sender)
                                .with_client_id(client_id)
                                .with_scope(scope);
                                self.publish_change_event(controller, event).await;
                                Self::json_success(&entity, db_entity.id_str(), &data)
                            }
                            Err(db::DbDataStoreError::AlreadyExists) => {
                                controller
                                    .release_unique_check_reservations(
                                        &entity,
                                        &request_id,
                                        &local_reserved,
                                        &confirmed_remotes,
                                    )
                                    .await;
                                Self::json_error(409, "entity already exists")
                            }
                            Err(_) => {
                                controller
                                    .release_unique_check_reservations(
                                        &entity,
                                        &request_id,
                                        &local_reserved,
                                        &confirmed_remotes,
                                    )
                                    .await;
                                Self::json_error(500, "internal error")
                            }
                        }
                    }
                };
                Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload: response_payload,
                    correlation_data,
                })
            }
            UniqueCheckContinuation::UpdateFromDbHandler {
                entity,
                id,
                merged_data,
                partition,
                request_id,
                now_ms,
                new_diff,
                old_diff,
                sender,
                client_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let response_payload = match remote_results {
                    Err((conflict_field, confirmed)) => {
                        controller
                            .release_unique_check_reservations(
                                &entity,
                                &request_id,
                                &local_reserved,
                                &confirmed,
                            )
                            .await;
                        Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{conflict_field}'"),
                        )
                    }
                    Ok(confirmed_remotes) => {
                        for (f, v, target) in &confirmed_remotes {
                            controller
                                .send_unique_commit_fire_and_forget(
                                    *target,
                                    &entity,
                                    f,
                                    v,
                                    &request_id,
                                )
                                .await;
                        }
                        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
                        match controller
                            .db_update(&entity, &id, &data_bytes, now_ms)
                            .await
                        {
                            Ok(db_entity) => {
                                controller
                                    .commit_unique_constraints(
                                        &entity,
                                        &id,
                                        &new_diff,
                                        partition,
                                        &request_id,
                                        now_ms,
                                    )
                                    .await;
                                controller
                                    .release_unique_constraints(
                                        &entity, &id, &old_diff, partition, &id, now_ms,
                                    )
                                    .await;
                                let scope = self.scope_config.resolve_scope(&entity, &merged_data);
                                let event = ChangeEvent::update(
                                    entity.clone(),
                                    db_entity.id_str().to_string(),
                                    merged_data.clone(),
                                )
                                .with_sender(sender)
                                .with_client_id(client_id)
                                .with_scope(scope);
                                self.publish_change_event(controller, event).await;
                                Self::json_success(&entity, db_entity.id_str(), &merged_data)
                            }
                            Err(db::DbDataStoreError::NotFound) => {
                                controller
                                    .release_unique_check_reservations(
                                        &entity,
                                        &request_id,
                                        &local_reserved,
                                        &confirmed_remotes,
                                    )
                                    .await;
                                Self::json_error(
                                    404,
                                    &format!("entity not found: {entity} id={id}"),
                                )
                            }
                            Err(_) => {
                                controller
                                    .release_unique_check_reservations(
                                        &entity,
                                        &request_id,
                                        &local_reserved,
                                        &confirmed_remotes,
                                    )
                                    .await;
                                Self::json_error(500, "internal error")
                            }
                        }
                    }
                };
                Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload: response_payload,
                    correlation_data,
                })
            }
            _ => None,
        }
    }

    async fn publish_change_event<T: ClusterTransport>(
        &self,
        controller: &NodeController<T>,
        event: ChangeEvent,
    ) {
        let topic = event.event_topic(0);
        let user_properties: Vec<(String, String)> = event
            .client_id
            .as_ref()
            .map(|s| vec![("x-origin-client-id".to_string(), s.clone())])
            .unwrap_or_default();
        match serde_json::to_vec(&event) {
            Ok(payload) => {
                tracing::debug!(topic, "publishing change event");
                controller
                    .transport()
                    .queue_local_publish_with_properties(topic, payload, 1, user_properties)
                    .await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize change event");
            }
        }
    }
}
