// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::PartitionId;
use super::super::db::{self, data_partition};
use super::super::db_topic::DbTopicOperation;
use super::super::node_controller::{
    FkCheckContinuation, FkDeleteContinuation, NodeController, PendingFkDeleteWork, PendingFkWork,
    PendingUniqueWork, UniqueCheckContinuation,
};
use super::super::protocol::JsonDbOp;
use super::super::transport::ClusterTransport;
use super::DbRequestHandler;
use super::helpers::{self, parse_projection};
use mqdb_core::events::ChangeEvent;
use mqdb_core::types::{MAX_FILTERS, MAX_LIST_RESULTS, MAX_SORT_FIELDS};
use serde_json::{Value, json};

pub(super) enum JsonOpResult {
    Response(Vec<u8>),
    NoResponse,
    PendingUniqueCheck(Box<PendingUniqueWork>),
    PendingFkCheck(Box<PendingFkWork>),
    PendingFkDelete(Box<PendingFkDeleteWork>),
}

struct JsonOpContext<'a> {
    entity: &'a str,
    id: Option<&'a str>,
    partition: PartitionId,
    sender: Option<&'a str>,
    client_id: Option<&'a str>,
    response_topic: &'a str,
    correlation_data: Option<&'a [u8]>,
}

struct UniqueDiffs<'a> {
    has_changes: bool,
    new_diff: &'a Value,
    old_diff: &'a Value,
}

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    fn validate_against_schema<T: ClusterTransport>(
        controller: &NodeController<T>,
        entity: &str,
        data: &Value,
    ) -> Option<Vec<u8>> {
        let cluster_schema = controller.stores().schema_get(entity)?;
        let schema: mqdb_core::schema::Schema = match serde_json::from_slice(&cluster_schema.data) {
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
        filters: &[mqdb_core::Filter],
    ) -> Option<Vec<u8>> {
        let cluster_schema = controller.stores().schema_get(entity)?;
        let schema: mqdb_core::schema::Schema = match serde_json::from_slice(&cluster_schema.data) {
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

    fn validate_projection_fields<T: ClusterTransport>(
        controller: &NodeController<T>,
        entity: &str,
        fields: &[String],
    ) -> Option<Vec<u8>> {
        let cluster_schema = controller.stores().schema_get(entity)?;
        let msg =
            helpers::validate_projection_against_schema(&cluster_schema.data, entity, fields)?;
        Some(Self::json_error(400, &msg))
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn handle_json_operation<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        operation: &DbTopicOperation,
        payload: &[u8],
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> JsonOpResult {
        let response_topic = mqtt_ctx.response_topic.unwrap_or("");
        let correlation_data = mqtt_ctx.correlation_data;
        let sender = mqtt_ctx.sender;

        match operation {
            DbTopicOperation::JsonCreate { entity } => {
                let result = self
                    .handle_json_create(controller, entity, payload, mqtt_ctx)
                    .await;
                match result {
                    JsonOpResult::Response(payload) => JsonOpResult::Response(
                        self.vault_decrypt_response_payload(payload, entity, "create", sender),
                    ),
                    other => other,
                }
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
                self.handle_json_read(controller, entity, id, payload, mqtt_ctx)
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
                if !controller.is_primary_for_partition(partition) {
                    let forward_payload =
                        self.vault_encrypt_update_delta(entity, id, effective_payload, sender);
                    let forwarded = controller
                        .forward_json_db_request(
                            partition,
                            JsonDbOp::Update,
                            entity,
                            Some(id),
                            &forward_payload,
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
                let result = self
                    .handle_json_update(controller, entity, id, effective_payload, mqtt_ctx)
                    .await;
                match result {
                    JsonOpResult::Response(payload) => JsonOpResult::Response(
                        self.vault_decrypt_response_payload(payload, entity, "update", sender),
                    ),
                    other => other,
                }
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
                if !controller.is_primary_for_partition(partition) {
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
                self.handle_json_delete(controller, entity, id, mqtt_ctx)
                    .await
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
        let Ok(data) = serde_json::from_slice::<Value>(&existing.data) else {
            return Some(Self::json_error(403, "permission denied"));
        };
        let owner_value = data.get(owner_field).and_then(Value::as_str);
        if owner_value != Some(sender) {
            return Some(Self::json_error(403, "permission denied"));
        }
        None
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn handle_json_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> JsonOpResult {
        let response_topic = mqtt_ctx.response_topic.unwrap_or("");
        let correlation_data = mqtt_ctx.correlation_data;
        let sender = mqtt_ctx.sender;
        let client_id = mqtt_ctx.client_id;

        let mut data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return JsonOpResult::Response(Self::json_error(400, "invalid JSON payload")),
        };

        if let Value::Object(ref mut obj) = data {
            obj.remove("_version");
            obj.insert("_version".to_string(), Value::Number(1.into()));
        }

        if let Some(err) = Self::validate_against_schema(controller, entity, &data) {
            return JsonOpResult::Response(err);
        }

        let (partition, id) = if let Some(client_id) = data.get("id").and_then(Value::as_str) {
            (data_partition(entity, client_id), client_id.to_string())
        } else {
            let p = controller.pick_partition_for_create();
            (p, self.generate_id_for_partition(entity, p, payload))
        };

        let vault_crypto = self.resolve_vault_crypto(entity, sender);
        if vault_crypto.is_some() && !data.as_object().is_some_and(|o| o.contains_key("id")) {
            data.as_object_mut()
                .map(|o| o.insert("id".to_string(), Value::String(id.clone())));
        }

        if !controller.is_primary_for_partition(partition) {
            let forward_data =
                self.vault_encrypt_if_needed(vault_crypto.as_ref(), entity, &id, data);
            let encrypted_payload = serde_json::to_vec(&forward_data).unwrap_or_default();
            return match self
                .try_forward_create(controller, partition, entity, &encrypted_payload, mqtt_ctx)
                .await
            {
                Some(payload) => JsonOpResult::Response(payload),
                None => JsonOpResult::NoResponse,
            };
        }

        let ctx = JsonOpContext {
            entity,
            id: Some(&id),
            partition,
            sender,
            client_id,
            response_topic,
            correlation_data,
        };
        self.execute_local_create(controller, &ctx, &data, vault_crypto.as_ref())
            .await
    }

    async fn try_forward_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> Option<Vec<u8>> {
        let forwarded = controller
            .forward_json_db_request(
                partition,
                JsonDbOp::Create,
                entity,
                None,
                payload,
                mqtt_ctx.response_topic.unwrap_or(""),
                mqtt_ctx.correlation_data,
                mqtt_ctx.sender,
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

    async fn execute_local_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        ctx: &JsonOpContext<'_>,
        data: &Value,
        vault_crypto: Option<&mqdb_agent::http::VaultCrypto>,
    ) -> JsonOpResult {
        let entity = ctx.entity;
        let id = ctx.id.unwrap_or("");
        let partition = ctx.partition;
        let sender = ctx.sender;
        let client_id = ctx.client_id;
        let response_topic = ctx.response_topic;
        let correlation_data = ctx.correlation_data;

        let mut data = data.clone();
        if let Some(obj) = data.as_object_mut() {
            obj.remove("__mqdb_fk_expected");
        }
        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();

        let fk_result = match controller.start_fk_existence_check(entity, &data).await {
            Ok(r) => r,
            Err(msg) => return JsonOpResult::Response(Self::json_error(409, &msg)),
        };

        if !fk_result.pending_remote.is_empty() {
            return JsonOpResult::PendingFkCheck(Box::new(PendingFkWork {
                pending_checks: fk_result.pending_remote,
                continuation: FkCheckContinuation::CreateFromDbHandler {
                    entity: entity.to_string(),
                    id: id.to_string(),
                    data: data.clone(),
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

        let phase1 = match controller
            .start_unique_constraint_check(entity, id, &data, partition, &request_id, now_ms)
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

        let constraint_data = data.clone();
        let data = self.vault_encrypt_if_needed(vault_crypto, entity, id, data);

        if !phase1.pending_remote.is_empty() {
            let data_bytes = serde_json::to_vec(&data).unwrap_or_default();
            return JsonOpResult::PendingUniqueCheck(Box::new(PendingUniqueWork {
                phase1,
                continuation: UniqueCheckContinuation::CreateFromDbHandler {
                    entity: entity.to_string(),
                    id: id.to_string(),
                    data: data.clone(),
                    constraint_data,
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

        let create_ctx = JsonOpContext {
            entity,
            id: Some(id),
            partition,
            sender,
            client_id,
            response_topic,
            correlation_data,
        };
        self.complete_create(
            controller,
            &create_ctx,
            &data,
            &constraint_data,
            &request_id,
            now_ms,
        )
        .await
    }

    async fn complete_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        ctx: &JsonOpContext<'_>,
        data: &Value,
        constraint_data: &Value,
        request_id: &str,
        now_ms: u64,
    ) -> JsonOpResult {
        let entity = ctx.entity;
        let id = ctx.id.unwrap_or("");
        let partition = ctx.partition;
        let sender = ctx.sender;
        let client_id = ctx.client_id;

        let data_bytes = serde_json::to_vec(data).unwrap_or_default();

        JsonOpResult::Response(
            match controller.db_create_prepare(entity, id, &data_bytes, now_ms) {
                Ok((db_entity, write)) => {
                    let scope = self.scope_config.resolve_scope(entity, data);
                    let event = ChangeEvent::create(
                        entity.to_string(),
                        db_entity.id_str().to_string(),
                        data.clone(),
                    )
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(scope);
                    let outbox = Self::build_outbox(&event);
                    controller.db_commit(write, outbox.clone()).await;
                    controller
                        .commit_unique_constraints(
                            entity,
                            id,
                            constraint_data,
                            partition,
                            request_id,
                            now_ms,
                        )
                        .await;
                    self.publish_change_event_and_deliver(controller, event, &outbox.operation_id)
                        .await;
                    helpers::json_ok_with_id(db_entity.id_str(), data)
                }
                Err(db::DbDataStoreError::AlreadyExists) => {
                    controller
                        .release_unique_constraints(
                            entity,
                            id,
                            constraint_data,
                            partition,
                            request_id,
                            now_ms,
                        )
                        .await;
                    Self::json_error(409, "entity already exists")
                }
                Err(_) => {
                    controller
                        .release_unique_constraints(
                            entity,
                            id,
                            constraint_data,
                            partition,
                            request_id,
                            now_ms,
                        )
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
        payload: &[u8],
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> JsonOpResult {
        let response_topic = mqtt_ctx.response_topic.unwrap_or("");
        let correlation_data = mqtt_ctx.correlation_data;
        let sender = mqtt_ctx.sender;
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
                    payload,
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

        let projection = parse_projection(payload);

        if let Some(ref fields) = projection
            && let Some(err) = Self::validate_projection_fields(controller, entity, fields)
        {
            return JsonOpResult::Response(err);
        }

        let result = match controller.db_get(entity, id) {
            Some(db_entity) => {
                let mut data: Value =
                    serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                if let Some(crypto) = self.resolve_vault_crypto(entity, sender) {
                    let skip = mqdb_agent::vault_transform::build_vault_skip_fields(
                        entity,
                        &self.ownership,
                    );
                    mqdb_agent::vault_transform::vault_decrypt_fields(
                        &crypto, entity, id, &mut data, &skip,
                    );
                }
                let data = if let Some(ref fields) = projection {
                    mqdb_core::types::project_fields(data, fields)
                } else {
                    data
                };
                helpers::json_ok_with_id(db_entity.id_str(), &data)
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

    async fn handle_json_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        payload: &[u8],
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> JsonOpResult {
        let updates: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return JsonOpResult::Response(Self::json_error(400, "invalid JSON payload")),
        };

        let ctx = JsonOpContext {
            entity,
            id: Some(id),
            partition: data_partition(entity, id),
            sender: mqtt_ctx.sender,
            client_id: mqtt_ctx.client_id,
            response_topic: mqtt_ctx.response_topic.unwrap_or(""),
            correlation_data: mqtt_ctx.correlation_data,
        };
        self.execute_local_update(controller, &ctx, updates).await
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_local_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        ctx: &JsonOpContext<'_>,
        mut updates: Value,
    ) -> JsonOpResult {
        let entity = ctx.entity;
        let id = ctx.id.unwrap_or("");
        let sender = ctx.sender;
        let client_id = ctx.client_id;
        let response_topic = ctx.response_topic;
        let correlation_data = ctx.correlation_data;

        if let Some(obj) = updates.as_object_mut() {
            obj.remove("__mqdb_fk_expected");
        }
        let vault_crypto = self.resolve_vault_crypto(entity, sender);
        let (old_data, merged_data) = match self.vault_merge_with_existing(
            controller,
            entity,
            id,
            updates,
            vault_crypto.as_ref(),
        ) {
            Ok(pair) => pair,
            Err(response) => return JsonOpResult::Response(response),
        };

        if let Some(err) = Self::validate_against_schema(controller, entity, &merged_data) {
            return JsonOpResult::Response(err);
        }

        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();
        let partition = data_partition(entity, id);

        let fk_result = match controller
            .start_fk_existence_check(entity, &merged_data)
            .await
        {
            Ok(r) => r,
            Err(msg) => return JsonOpResult::Response(Self::json_error(409, &msg)),
        };

        let (new_diff, old_diff) =
            controller.compute_unique_field_diffs(entity, &old_data, &merged_data);

        let merged_data =
            self.vault_encrypt_if_needed(vault_crypto.as_ref(), entity, id, merged_data);

        if !fk_result.pending_remote.is_empty() {
            return JsonOpResult::PendingFkCheck(Box::new(PendingFkWork {
                pending_checks: fk_result.pending_remote,
                continuation: FkCheckContinuation::UpdateFromDbHandler {
                    entity: entity.to_string(),
                    id: id.to_string(),
                    merged_data,
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

        let update_ctx = JsonOpContext {
            entity,
            id: Some(id),
            partition,
            sender,
            client_id,
            response_topic,
            correlation_data,
        };
        let unique_diffs = UniqueDiffs {
            has_changes: has_unique_changes,
            new_diff: &new_diff,
            old_diff: &old_diff,
        };
        self.complete_update(
            controller,
            &update_ctx,
            &merged_data,
            &request_id,
            now_ms,
            &unique_diffs,
        )
        .await
    }

    async fn complete_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        ctx: &JsonOpContext<'_>,
        merged_data: &Value,
        request_id: &str,
        now_ms: u64,
        unique_diffs: &UniqueDiffs<'_>,
    ) -> JsonOpResult {
        let entity = ctx.entity;
        let id = ctx.id.unwrap_or("");
        let partition = ctx.partition;
        let sender = ctx.sender;
        let client_id = ctx.client_id;
        let has_unique_changes = unique_diffs.has_changes;
        let new_diff = unique_diffs.new_diff;
        let old_diff = unique_diffs.old_diff;

        let data_bytes = serde_json::to_vec(merged_data).unwrap_or_default();

        JsonOpResult::Response(
            match controller.db_update_prepare(entity, id, &data_bytes, now_ms) {
                Ok((db_entity, write)) => {
                    let scope = self.scope_config.resolve_scope(entity, merged_data);
                    let event = ChangeEvent::update(
                        entity.to_string(),
                        db_entity.id_str().to_string(),
                        merged_data.clone(),
                    )
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(scope);
                    let outbox = Self::build_outbox(&event);
                    controller.db_commit(write, outbox.clone()).await;
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
                    self.publish_change_event_and_deliver(controller, event, &outbox.operation_id)
                        .await;
                    helpers::json_ok_with_id(db_entity.id_str(), merged_data)
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

        Ok((old_data, merged))
    }

    async fn handle_json_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        mqtt_ctx: &super::MqttRequestContext<'_>,
    ) -> JsonOpResult {
        let sender = mqtt_ctx.sender;
        let client_id = mqtt_ctx.client_id;
        let response_topic = mqtt_ctx.response_topic.unwrap_or("");
        let correlation_data = mqtt_ctx.correlation_data;

        let (local_results, pending_remote) =
            match controller.start_fk_reverse_lookup(entity, id, sender).await {
                Ok(pair) => pair,
                Err(msg) => return JsonOpResult::Response(Self::json_error(409, &msg)),
            };

        if !pending_remote.is_empty() {
            return JsonOpResult::PendingFkDelete(Box::new(PendingFkDeleteWork {
                local_results,
                pending_lookups: pending_remote,
                continuation: FkDeleteContinuation::DeleteFromDbHandler {
                    entity: entity.to_string(),
                    id: id.to_string(),
                    sender: sender.map(str::to_string),
                    client_id: client_id.map(str::to_string),
                    response_topic: response_topic.to_string(),
                    correlation_data: correlation_data.map(<[u8]>::to_vec),
                },
            }));
        }

        let all_results = match controller.collect_local_cascade(entity, id, local_results) {
            Ok(results) => results,
            Err(msg) => return JsonOpResult::Response(Self::json_error(409, &msg)),
        };
        let effects = controller.prepare_fk_side_effects(&all_results);
        let (cascade, ack_receivers) = controller.execute_fk_side_effects(&effects).await;
        let ctx = JsonOpContext {
            entity,
            id: Some(id),
            partition: data_partition(entity, id),
            sender,
            client_id,
            response_topic,
            correlation_data,
        };
        JsonOpResult::Response(
            self.execute_delete(controller, &ctx, cascade.as_ref(), ack_receivers)
                .await,
        )
    }

    async fn execute_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        ctx: &JsonOpContext<'_>,
        cascade: Option<&crate::cluster::store_manager::outbox::CascadeOutboxPayload>,
        ack_receivers: Vec<tokio::sync::oneshot::Receiver<bool>>,
    ) -> Vec<u8> {
        let entity = ctx.entity;
        let id = ctx.id.unwrap_or("");
        let sender = ctx.sender;
        let client_id = ctx.client_id;

        let pre_delete_scope = if self.scope_config.is_empty() {
            None
        } else {
            controller
                .db_get(entity, id)
                .and_then(|e| serde_json::from_slice::<Value>(&e.data).ok())
                .and_then(|data| self.scope_config.resolve_scope(entity, &data))
        };

        match controller.db_delete_prepare(entity, id) {
            Ok((db_entity, write)) => {
                let data: Value = serde_json::from_slice(&db_entity.data).unwrap_or(Value::Null);
                let event = ChangeEvent::delete(entity.to_string(), id.to_string(), data)
                    .with_sender(sender.map(str::to_string))
                    .with_client_id(client_id.map(str::to_string))
                    .with_scope(pre_delete_scope);
                let outbox = Self::build_outbox(&event);
                if let Some(cas) = cascade {
                    controller
                        .db_commit_with_cascade(write, outbox.clone(), cas)
                        .await;
                } else {
                    controller.db_commit(write, outbox.clone()).await;
                }
                self.publish_change_event_and_deliver(controller, event, &outbox.operation_id)
                    .await;
                if let Some(cas) = cascade {
                    crate::cluster::node_controller::db_ops::spawn_cascade_ack_waiter(
                        controller.stores().cluster_outbox().cloned(),
                        cas.operation_id.clone(),
                        ack_receivers,
                        true,
                    );
                }
                let result = json!({
                    "status": "ok",
                    "data": {"id": id, "deleted": true}
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_json_list<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        payload: &[u8],
        response_topic: &str,
        sender: Option<&str>,
    ) -> Option<Vec<u8>> {
        let parsed_data: Option<Value> = if payload.is_empty() {
            None
        } else {
            serde_json::from_slice::<Value>(payload).ok()
        };

        let mut filters: Vec<mqdb_core::Filter> = parsed_data
            .as_ref()
            .and_then(|d| d.get("filters"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        if filters.len() > MAX_FILTERS {
            return Some(Self::json_error(
                400,
                &format!(
                    "validation error: too many filters: {} exceeds maximum of {MAX_FILTERS}",
                    filters.len()
                ),
            ));
        }

        let sorts: Vec<mqdb_core::SortOrder> = parsed_data
            .as_ref()
            .and_then(|d| d.get("sort"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        if sorts.len() > MAX_SORT_FIELDS {
            return Some(Self::json_error(
                400,
                &format!(
                    "validation error: too many sort fields: {} exceeds maximum of {MAX_SORT_FIELDS}",
                    sorts.len()
                ),
            ));
        }

        #[allow(clippy::cast_possible_truncation)]
        let pagination: Option<mqdb_core::Pagination> = parsed_data.as_ref().and_then(|d| {
            d.get("pagination")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .or_else(|| {
                    let limit = d.get("limit").and_then(Value::as_u64).map(|v| v as usize);
                    let offset = d.get("offset").and_then(Value::as_u64).map(|v| v as usize);
                    match (limit, offset) {
                        (Some(l), Some(o)) => Some(mqdb_core::Pagination::new(l, o)),
                        (Some(l), None) => Some(mqdb_core::Pagination::new(l, 0)),
                        (None, Some(o)) => Some(mqdb_core::Pagination::new(usize::MAX, o)),
                        (None, None) => None,
                    }
                })
        });

        if !filters.is_empty()
            && let Some(err) = Self::validate_filter_fields(controller, entity, &filters)
        {
            return Some(err);
        }

        if let Some(uid) = sender
            && !self.ownership.is_admin(uid)
            && let Some(owner_field) = self.ownership.owner_field(entity)
        {
            filters.push(mqdb_core::Filter::new(
                owner_field.to_string(),
                mqdb_core::FilterOp::Eq,
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

        let projection = parse_projection(payload);

        if let Some(ref fields) = projection
            && let Some(err) = Self::validate_projection_fields(controller, entity, fields)
        {
            return Some(err);
        }

        let has_remote_nodes = !controller.alive_nodes().is_empty();

        if has_remote_nodes {
            let started = controller
                .start_scatter_list_query(
                    entity,
                    &scatter_payload,
                    response_topic.to_string(),
                    filters.clone(),
                    sorts,
                    projection.clone(),
                    pagination.clone(),
                    sender,
                )
                .await;

            if started {
                return None;
            }
        }

        let vault_crypto = self.resolve_vault_crypto(entity, sender);
        let vault_skip = vault_crypto
            .as_ref()
            .map(|_| mqdb_agent::vault_transform::build_vault_skip_fields(entity, &self.ownership));

        let entities = controller.db_list(entity);
        let mut items: Vec<Value> = entities
            .iter()
            .filter_map(|e| {
                let mut data: Value = serde_json::from_slice(&e.data).ok()?;
                if let (Some(crypto), Some(skip)) = (&vault_crypto, &vault_skip) {
                    mqdb_agent::vault_transform::vault_decrypt_fields(
                        crypto,
                        entity,
                        e.id_str(),
                        &mut data,
                        skip,
                    );
                }
                if !filters.is_empty() && !NodeController::<T>::matches_filters(&data, &filters) {
                    return None;
                }
                let data = if let Some(ref fields) = projection {
                    mqdb_core::types::project_fields(data, fields)
                } else {
                    data
                };
                Some(json!({
                    "id": e.id_str(),
                    "data": data
                }))
            })
            .collect();
        if let Some(ref pagination) = pagination {
            items = items
                .into_iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .collect();
        }
        items.truncate(MAX_LIST_RESULTS);
        let flattened = helpers::flatten_list_items(items);

        let result = json!({
            "status": "ok",
            "data": flattened
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
                constraint_data,
                partition,
                request_id,
                now_ms,
                sender,
                client_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let vault_sender = sender.clone();
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
                        match controller.db_create_prepare(&entity, &id, &data_bytes, now_ms) {
                            Ok((db_entity, write)) => {
                                let scope = self.scope_config.resolve_scope(&entity, &data);
                                let event = ChangeEvent::create(
                                    entity.clone(),
                                    db_entity.id_str().to_string(),
                                    data.clone(),
                                )
                                .with_sender(sender)
                                .with_client_id(client_id)
                                .with_scope(scope);
                                let outbox = Self::build_outbox(&event);
                                controller.db_commit(write, outbox.clone()).await;
                                controller
                                    .commit_unique_constraints(
                                        &entity,
                                        &id,
                                        &constraint_data,
                                        partition,
                                        &request_id,
                                        now_ms,
                                    )
                                    .await;
                                self.publish_change_event_and_deliver(
                                    controller,
                                    event,
                                    &outbox.operation_id,
                                )
                                .await;
                                helpers::json_ok_with_id(db_entity.id_str(), &data)
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
                let response_payload = self.vault_decrypt_response_payload(
                    response_payload,
                    &entity,
                    "create",
                    vault_sender.as_deref(),
                );
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
                let vault_sender = sender.clone();
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
                        match controller.db_update_prepare(&entity, &id, &data_bytes, now_ms) {
                            Ok((db_entity, write)) => {
                                let scope = self.scope_config.resolve_scope(&entity, &merged_data);
                                let event = ChangeEvent::update(
                                    entity.clone(),
                                    db_entity.id_str().to_string(),
                                    merged_data.clone(),
                                )
                                .with_sender(sender)
                                .with_client_id(client_id)
                                .with_scope(scope);
                                let outbox = Self::build_outbox(&event);
                                controller.db_commit(write, outbox.clone()).await;
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
                                self.publish_change_event_and_deliver(
                                    controller,
                                    event,
                                    &outbox.operation_id,
                                )
                                .await;
                                helpers::json_ok_with_id(db_entity.id_str(), &merged_data)
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
                let response_payload = self.vault_decrypt_response_payload(
                    response_payload,
                    &entity,
                    "update",
                    vault_sender.as_deref(),
                );
                Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload: response_payload,
                    correlation_data,
                })
            }
            _ => {
                tracing::error!(
                    "complete_pending_unique_check received misrouted continuation (expected *FromDbHandler)"
                );
                None
            }
        }
    }

    pub async fn complete_pending_fk_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        restrict_error: Option<String>,
        side_effects: Vec<crate::cluster::node_controller::fk::FkReverseLookupResult>,
        continuation: FkDeleteContinuation,
    ) -> Option<super::super::db_handler::DbPublishResponse> {
        if let Some(msg) = restrict_error {
            let payload = Self::json_error(409, &msg);
            return match continuation {
                FkDeleteContinuation::DeleteFromDbHandler {
                    response_topic,
                    correlation_data,
                    ..
                } => Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload,
                    correlation_data,
                }),
                FkDeleteContinuation::DeleteFromNodeController { .. } => None,
            };
        }

        match continuation {
            FkDeleteContinuation::DeleteFromDbHandler {
                entity,
                id,
                sender,
                client_id,
                response_topic,
                correlation_data,
            } => {
                let effects = controller.prepare_fk_side_effects(&side_effects);
                let (cascade, ack_receivers) = controller.execute_fk_side_effects(&effects).await;
                let ctx = JsonOpContext {
                    entity: &entity,
                    id: Some(&id),
                    partition: data_partition(&entity, &id),
                    sender: sender.as_deref(),
                    client_id: client_id.as_deref(),
                    response_topic: &response_topic,
                    correlation_data: correlation_data.as_deref(),
                };
                let payload = self
                    .execute_delete(controller, &ctx, cascade.as_ref(), ack_receivers)
                    .await;
                Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload,
                    correlation_data,
                })
            }
            FkDeleteContinuation::DeleteFromNodeController { .. } => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn complete_pending_fk_check<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        fk_ok: bool,
        fk_error: Option<String>,
        continuation: FkCheckContinuation,
    ) -> super::FkCheckCompletion {
        use super::FkCheckCompletion;

        if !fk_ok {
            let msg = fk_error.unwrap_or_else(|| "FK constraint violation".to_string());
            let payload = Self::json_error(409, &msg);
            return match continuation {
                FkCheckContinuation::CreateFromDbHandler {
                    response_topic,
                    correlation_data,
                    ..
                }
                | FkCheckContinuation::UpdateFromDbHandler {
                    response_topic,
                    correlation_data,
                    ..
                } => FkCheckCompletion::Done(Some(super::DbPublishResponse {
                    topic: response_topic,
                    payload,
                    correlation_data,
                })),
                _ => FkCheckCompletion::Done(None),
            };
        }

        match continuation {
            FkCheckContinuation::CreateFromDbHandler {
                entity,
                id,
                data,
                partition,
                request_id,
                sender,
                client_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let now_ms = Self::current_time_ms();
                let constraint_data = data.clone();
                let crypto = self.resolve_vault_crypto(&entity, sender.as_deref());
                let data = self.vault_encrypt_if_needed(crypto.as_ref(), &entity, &id, data);
                let ctx = JsonOpContext {
                    entity: &entity,
                    id: Some(&id),
                    partition,
                    sender: sender.as_deref(),
                    client_id: client_id.as_deref(),
                    response_topic: &response_topic,
                    correlation_data: correlation_data.as_deref(),
                };
                let result = self
                    .complete_create(
                        controller,
                        &ctx,
                        &data,
                        &constraint_data,
                        &request_id,
                        now_ms,
                    )
                    .await;
                match result {
                    JsonOpResult::Response(payload) => {
                        let payload = self.vault_decrypt_response_payload(
                            payload,
                            &entity,
                            "create",
                            sender.as_deref(),
                        );
                        FkCheckCompletion::Done(Some(super::DbPublishResponse {
                            topic: response_topic,
                            payload,
                            correlation_data,
                        }))
                    }
                    JsonOpResult::PendingUniqueCheck(pending) => {
                        FkCheckCompletion::NeedUniqueCheck(pending)
                    }
                    JsonOpResult::PendingFkCheck(_)
                    | JsonOpResult::PendingFkDelete(_)
                    | JsonOpResult::NoResponse => FkCheckCompletion::Done(None),
                }
            }
            FkCheckContinuation::UpdateFromDbHandler {
                entity,
                id,
                merged_data,
                partition,
                request_id,
                new_diff,
                old_diff,
                sender,
                client_id,
                response_topic,
                correlation_data,
                ..
            } => {
                let now_ms = Self::current_time_ms();
                let has_unique_changes = new_diff.as_object().is_some_and(|m| !m.is_empty());
                let ctx = JsonOpContext {
                    entity: &entity,
                    id: Some(&id),
                    partition,
                    sender: sender.as_deref(),
                    client_id: client_id.as_deref(),
                    response_topic: &response_topic,
                    correlation_data: correlation_data.as_deref(),
                };
                let unique_diffs = UniqueDiffs {
                    has_changes: has_unique_changes,
                    new_diff: &new_diff,
                    old_diff: &old_diff,
                };
                let result = self
                    .complete_update(
                        controller,
                        &ctx,
                        &merged_data,
                        &request_id,
                        now_ms,
                        &unique_diffs,
                    )
                    .await;
                match result {
                    JsonOpResult::Response(payload) => {
                        let payload = self.vault_decrypt_response_payload(
                            payload,
                            &entity,
                            "update",
                            sender.as_deref(),
                        );
                        FkCheckCompletion::Done(Some(super::DbPublishResponse {
                            topic: response_topic,
                            payload,
                            correlation_data,
                        }))
                    }
                    JsonOpResult::PendingUniqueCheck(pending) => {
                        FkCheckCompletion::NeedUniqueCheck(pending)
                    }
                    JsonOpResult::PendingFkCheck(_)
                    | JsonOpResult::PendingFkDelete(_)
                    | JsonOpResult::NoResponse => FkCheckCompletion::Done(None),
                }
            }
            _ => {
                tracing::error!(
                    "complete_pending_fk_check received misrouted continuation (expected *FromDbHandler)"
                );
                FkCheckCompletion::Done(None)
            }
        }
    }

    fn vault_merge_with_existing<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        updates: Value,
        vault_crypto: Option<&mqdb_agent::http::VaultCrypto>,
    ) -> Result<(Value, Value), Vec<u8>> {
        if let Some(crypto) = vault_crypto {
            let skip =
                mqdb_agent::vault_transform::build_vault_skip_fields(entity, &self.ownership);
            let Some(existing) = controller.db_get(entity, id) else {
                return Err(Self::json_error(
                    404,
                    &format!("entity not found: {entity} id={id}"),
                ));
            };
            let mut existing_data: Value = serde_json::from_slice(&existing.data)
                .unwrap_or(Value::Object(serde_json::Map::new()));
            mqdb_agent::vault_transform::vault_decrypt_fields(
                crypto,
                entity,
                id,
                &mut existing_data,
                &skip,
            );
            let old_data = existing_data.clone();
            if let (Value::Object(base), Value::Object(patch)) = (&mut existing_data, updates) {
                for (key, value) in patch {
                    base.insert(key, value);
                }
            }
            let existing_version = old_data
                .get("_version")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if let Value::Object(ref mut obj) = existing_data {
                obj.insert(
                    "_version".to_string(),
                    Value::Number((existing_version + 1).into()),
                );
            }
            Ok((old_data, existing_data))
        } else {
            self.merge_with_existing(controller, entity, id, updates)
        }
    }

    fn vault_encrypt_update_delta(
        &self,
        entity: &str,
        id: &str,
        payload: &[u8],
        sender: Option<&str>,
    ) -> Vec<u8> {
        let Some(crypto) = self.resolve_vault_crypto(entity, sender) else {
            return payload.to_vec();
        };
        let Ok(mut delta) = serde_json::from_slice::<Value>(payload) else {
            return payload.to_vec();
        };
        let skip = mqdb_agent::vault_transform::build_vault_skip_fields(entity, &self.ownership);
        mqdb_agent::vault_transform::vault_encrypt_fields(&crypto, entity, id, &mut delta, &skip);
        serde_json::to_vec(&delta).unwrap_or_else(|_| payload.to_vec())
    }

    fn build_outbox(event: &ChangeEvent) -> crate::cluster::store_manager::outbox::OutboxPayload {
        crate::cluster::node_controller::db_ops::build_change_event_outbox(event)
    }

    async fn publish_change_event_and_deliver<T: ClusterTransport>(
        &self,
        controller: &NodeController<T>,
        event: ChangeEvent,
        operation_id: &str,
    ) {
        controller
            .publish_and_deliver_change_event(event, operation_id)
            .await;
    }
}
