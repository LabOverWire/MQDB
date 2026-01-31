use super::super::PartitionId;
use super::super::db::{self, data_partition};
use super::super::db_topic::DbTopicOperation;
use super::super::node_controller::NodeController;
use super::super::protocol::JsonDbOp;
use super::super::transport::ClusterTransport;
use super::DbRequestHandler;
use serde_json::{Value, json};

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    pub(super) async fn handle_json_operation<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        operation: &DbTopicOperation,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
    ) -> Option<Vec<u8>> {
        match operation {
            DbTopicOperation::JsonCreate { entity } => {
                self.handle_json_create(
                    controller,
                    entity,
                    payload,
                    response_topic,
                    correlation_data,
                )
                .await
            }
            DbTopicOperation::JsonRead { entity, id } => {
                self.handle_json_read(controller, entity, id, response_topic, correlation_data)
                    .await
            }
            DbTopicOperation::JsonUpdate { entity, id } => {
                if let Some(uid) = sender
                    && !self.ownership.is_admin(uid)
                    && let Some(owner_field) = self.ownership.owner_field(entity)
                    && let Some(err) =
                        self.check_cluster_ownership(controller, entity, id, owner_field, uid)
                {
                    return Some(err);
                }
                self.handle_json_update(
                    controller,
                    entity,
                    id,
                    payload,
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
                    return Some(err);
                }
                self.handle_json_delete(controller, entity, id, response_topic, correlation_data)
                    .await
            }
            DbTopicOperation::JsonList { entity } => {
                self.handle_json_list(controller, entity, payload, response_topic, sender)
                    .await
            }
            _ => None,
        }
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
    ) -> Option<Vec<u8>> {
        let data: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Some(Self::json_error(400, "invalid JSON payload")),
        };

        let (partition, id) = if let Some(client_id) = data.get("id").and_then(Value::as_str) {
            (data_partition(entity, client_id), client_id.to_string())
        } else {
            let p = controller.pick_partition_for_create();
            (p, self.generate_id_for_partition(entity, p, payload))
        };

        if !controller.is_local_partition(partition) {
            return self
                .try_forward_create(
                    controller,
                    partition,
                    entity,
                    payload,
                    response_topic,
                    correlation_data,
                )
                .await;
        }

        self.execute_local_create(controller, entity, &id, &data, partition)
            .await
    }

    async fn try_forward_create<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
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
        entity: &str,
        id: &str,
        data: &Value,
        partition: PartitionId,
    ) -> Option<Vec<u8>> {
        let data_bytes = serde_json::to_vec(data).unwrap_or_default();
        let now_ms = Self::current_time_ms();
        let request_id = uuid::Uuid::new_v4().to_string();

        if let Err(conflict_field) = controller
            .check_unique_constraints(entity, id, data, partition, &request_id, now_ms)
            .await
        {
            return Some(Self::json_error(
                409,
                &format!("unique constraint violation on field '{conflict_field}'"),
            ));
        }

        Some(
            match controller.db_create(entity, id, &data_bytes, now_ms).await {
                Ok(db_entity) => {
                    controller
                        .commit_unique_constraints(entity, id, data, partition, &request_id, now_ms)
                        .await;
                    Self::json_success(entity, db_entity.id_str(), data)
                }
                Err(db::DbDataStoreError::AlreadyExists) => {
                    controller
                        .release_unique_constraints(
                            entity,
                            id,
                            data,
                            partition,
                            &request_id,
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
                            data,
                            partition,
                            &request_id,
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
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
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
                return None;
            }
            tracing::warn!(
                node = node_id,
                partition = partition.get(),
                elapsed_us = start.elapsed().as_micros() as u64,
                "json_read_forward_failed"
            );
            return Some(Self::json_error(
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

        Some(result)
    }

    async fn handle_json_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        let updates: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Some(Self::json_error(400, "invalid JSON payload")),
        };

        let partition = data_partition(entity, id);

        if !controller.is_local_partition(partition) {
            let forwarded = controller
                .forward_json_db_request(
                    partition,
                    JsonDbOp::Update,
                    entity,
                    Some(id),
                    payload,
                    response_topic,
                    correlation_data,
                )
                .await;
            return if forwarded {
                None
            } else {
                Some(Self::json_error(
                    503,
                    "partition not local and forwarding failed",
                ))
            };
        }

        self.execute_local_update(controller, entity, id, updates)
            .await
    }

    async fn execute_local_update<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        updates: Value,
    ) -> Option<Vec<u8>> {
        let merged_data = match self.merge_with_existing(controller, entity, id, updates) {
            Ok(data) => data,
            Err(response) => return Some(response),
        };

        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
        let now_ms = Self::current_time_ms();

        Some(
            match controller.db_update(entity, id, &data_bytes, now_ms).await {
                Ok(db_entity) => Self::json_success(entity, db_entity.id_str(), &merged_data),
                Err(db::DbDataStoreError::NotFound) => {
                    Self::json_error(404, &format!("entity not found: {entity} id={id}"))
                }
                Err(_) => Self::json_error(500, "internal error"),
            },
        )
    }

    fn merge_with_existing<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        updates: Value,
    ) -> Result<Value, Vec<u8>> {
        let Some(existing) = controller.db_get(entity, id) else {
            return Err(Self::json_error(
                404,
                &format!("entity not found: {entity} id={id}"),
            ));
        };

        let mut existing_data: Value =
            serde_json::from_slice(&existing.data).unwrap_or(Value::Object(serde_json::Map::new()));

        if let (Value::Object(existing_obj), Value::Object(update_obj)) =
            (&mut existing_data, updates)
        {
            for (key, value) in update_obj {
                existing_obj.insert(key, value);
            }
        }

        Ok(existing_data)
    }

    async fn handle_json_delete<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        entity: &str,
        id: &str,
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
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
                )
                .await;
            if forwarded {
                return None;
            }
            return Some(Self::json_error(
                503,
                "partition not local and forwarding failed",
            ));
        }

        match controller.db_delete(entity, id).await {
            Ok(_) => {
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
}
