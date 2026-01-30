use super::{AdminRequest, ClusteredAgent};
use crate::cluster::{NUM_PARTITIONS, NodeId, PartitionId};
use crate::transport::{ErrorCode, Response};
use serde_json::json;
use tokio::sync::oneshot;
use tracing::warn;

impl ClusteredAgent {
    pub(super) async fn handle_admin_request(
        &self,
        client: &mqtt5::client::MqttClient,
        req: AdminRequest,
    ) {
        let Some(response_topic) = req.response_topic else {
            warn!(topic = %req.topic, "admin request without response topic");
            return;
        };

        let response = if req.topic == "$DB/_health" {
            self.build_health_response().await
        } else if req.topic.ends_with("/status") {
            self.build_status_response().await
        } else if req.topic.ends_with("/rebalance") {
            self.handle_rebalance_request().await
        } else if let Some(entity) = req.topic.strip_prefix("$DB/_admin/schema/") {
            if let Some(entity) = entity.strip_suffix("/set") {
                self.handle_schema_set(entity, &req.payload).await
            } else if let Some(entity) = entity.strip_suffix("/get") {
                self.handle_schema_get(entity).await
            } else {
                Response::error(ErrorCode::BadRequest, "invalid schema operation")
            }
        } else if let Some(rest) = req.topic.strip_prefix("$DB/_admin/constraint/") {
            if let Some(entity) = rest.strip_suffix("/add") {
                self.handle_constraint_add(entity, &req.payload).await
            } else if let Some(entity) = rest.strip_suffix("/list") {
                self.handle_constraint_list(entity).await
            } else if let Some(entity) = rest.strip_suffix("/remove") {
                self.handle_constraint_remove(entity, &req.payload).await
            } else {
                Response::error(ErrorCode::BadRequest, "invalid constraint operation")
            }
        } else if req.topic == "$DB/_admin/backup" {
            self.handle_backup_create(&req.payload)
        } else if req.topic == "$DB/_admin/backup/list" {
            self.handle_backup_list()
        } else if req.topic == "$DB/_admin/restore" {
            Response::error(
                ErrorCode::BadRequest,
                "restore requires agent restart - use CLI with --restore flag",
            )
        } else if req.topic == "$DB/_admin/consumer-groups" {
            Self::handle_consumer_group_list()
        } else if let Some(name) = req.topic.strip_prefix("$DB/_admin/consumer-groups/") {
            Self::handle_consumer_group_show(name)
        } else {
            Response::error(
                ErrorCode::BadRequest,
                format!("unknown admin command: {}", req.topic),
            )
        };

        let payload = serde_json::to_vec(&response).unwrap_or_default();
        if let Err(e) = client.publish(&response_topic, payload).await {
            warn!(error = %e, "failed to send admin response");
        }
    }

    async fn build_status_response(&self) -> Response {
        let ctrl = self.controller.read().await;
        let raft_status = self.rx_raft_status.borrow().clone();

        let alive_nodes: Vec<u16> = ctrl.alive_nodes().iter().map(|n| n.get()).collect();

        let mut partitions = Vec::new();
        for partition in PartitionId::all() {
            let assignment = ctrl.partition_map().get(partition);
            let replicas: Vec<_> = assignment
                .replicas
                .iter()
                .copied()
                .map(NodeId::get)
                .collect();
            partitions.push(json!({
                "id": partition.get(),
                "primary": assignment.primary.map(NodeId::get),
                "replicas": replicas,
                "epoch": assignment.epoch.get()
            }));
        }

        let stores = ctrl.stores();
        Response::ok(json!({
            "node_id": self.node_id.get(),
            "node_name": self.node_name,
            "is_raft_leader": raft_status.is_leader,
            "raft_term": raft_status.current_term,
            "raft_log_len": raft_status.log_len,
            "raft_commit_index": raft_status.commit_index,
            "raft_last_applied": raft_status.last_applied,
            "raft_last_log_index": raft_status.last_log_index,
            "store_subscriptions": stores.subscriptions.len(),
            "store_topics": stores.topics.len(),
            "store_client_locations": stores.client_locations.len(),
            "store_db_data": stores.db_data.len(),
            "alive_nodes": alive_nodes,
            "partition_count": NUM_PARTITIONS,
            "partitions": partitions
        }))
    }

    async fn handle_rebalance_request(&self) -> Response {
        let raft_status = self.rx_raft_status.borrow().clone();

        if !raft_status.is_leader {
            return Response::error(ErrorCode::Forbidden, "not the Raft leader");
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx_raft_admin
            .send(crate::cluster::RaftAdminCommand::ForceRebalance(tx))
            .is_err()
        {
            return Response::error(ErrorCode::Internal, "raft task not available");
        }

        match rx.await {
            Ok(proposals) => Response::ok(json!({
                "proposed_changes": proposals
            })),
            Err(_) => Response::error(ErrorCode::Internal, "failed to get rebalance response"),
        }
    }

    async fn handle_schema_set(&self, entity: &str, payload: &[u8]) -> Response {
        let schema_data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let schema_bytes = serde_json::to_vec(&schema_data).unwrap_or_default();
        let mut ctrl = self.controller.write().await;

        if ctrl.schema_get(entity).is_some() {
            match ctrl.schema_update(entity, &schema_bytes).await {
                Ok(_) => Response::ok(json!({"message": "schema updated"})),
                Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
            }
        } else {
            match ctrl.schema_register(entity, &schema_bytes).await {
                Ok(_) => Response::ok(json!({"message": "schema set"})),
                Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
            }
        }
    }

    async fn handle_schema_get(&self, entity: &str) -> Response {
        let ctrl = self.controller.read().await;
        match ctrl.schema_get(entity) {
            Some(schema) => {
                let data: serde_json::Value =
                    serde_json::from_slice(&schema.data).unwrap_or(serde_json::Value::Null);
                Response::ok(json!({
                    "entity": entity,
                    "version": schema.schema_version,
                    "schema": data
                }))
            }
            None => Response::error(
                ErrorCode::NotFound,
                format!("no schema for entity: {entity}"),
            ),
        }
    }

    async fn handle_constraint_add(&self, entity: &str, payload: &[u8]) -> Response {
        let constraint_def: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let constraint_type = constraint_def
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unique");
        let name = constraint_def
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let field = constraint_def
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if name.is_empty() || field.is_empty() {
            return Response::error(
                ErrorCode::BadRequest,
                "constraint requires 'name' and 'field' parameters",
            );
        }

        match constraint_type {
            "unique" => {
                let constraint = crate::cluster::db::ClusterConstraint::unique(entity, name, field);
                let mut ctrl = self.controller.write().await;
                match ctrl.constraint_add(&constraint).await {
                    Ok(()) => Response::ok(json!({"message": "constraint added"})),
                    Err(crate::cluster::db::ConstraintStoreError::AlreadyExists) => {
                        Response::error(ErrorCode::Conflict, "constraint already exists")
                    }
                    Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
                }
            }
            _ => Response::error(
                ErrorCode::BadRequest,
                format!("unsupported constraint type: {constraint_type}"),
            ),
        }
    }

    async fn handle_constraint_remove(&self, entity: &str, payload: &[u8]) -> Response {
        let remove_def: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let Some(name) = remove_def.get("name").and_then(|v| v.as_str()) else {
            return Response::error(
                ErrorCode::BadRequest,
                "constraint removal requires 'name' parameter",
            );
        };

        let mut ctrl = self.controller.write().await;
        match ctrl.constraint_remove(entity, name).await {
            Ok(()) => Response::ok(json!({"message": "constraint removed"})),
            Err(crate::cluster::db::ConstraintStoreError::NotFound) => {
                Response::error(ErrorCode::NotFound, "constraint not found")
            }
            Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
        }
    }

    async fn handle_constraint_list(&self, entity: &str) -> Response {
        let ctrl = self.controller.read().await;
        let constraints = ctrl.constraint_list(entity);
        let data: Vec<serde_json::Value> = constraints
            .iter()
            .map(|c| {
                json!({
                    "name": c.name_str(),
                    "type": "unique",
                    "field": c.field_str()
                })
            })
            .collect();
        Response::ok(json!(data))
    }

    fn handle_backup_create(&self, payload: &[u8]) -> Response {
        let name = serde_json::from_slice::<serde_json::Value>(payload)
            .ok()
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
            .unwrap_or_else(|| "backup".to_string());

        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Response::error(
                ErrorCode::BadRequest,
                "invalid backup name: must be alphanumeric, underscore, or hyphen only",
            );
        }

        let backup_dir = self.db_path.join("backups").join(&name);
        let stores_dir = self.db_path.join("stores");

        if backup_dir.exists() {
            return Response::error(
                ErrorCode::BadRequest,
                format!("backup already exists: {name}"),
            );
        }

        if !stores_dir.exists() {
            return Response::error(ErrorCode::Internal, "no stores directory to backup");
        }

        match copy_dir_recursive(&stores_dir, &backup_dir) {
            Ok(()) => Response::ok(json!({
                "message": "backup created",
                "name": name,
                "path": backup_dir.display().to_string()
            })),
            Err(e) => Response::error(ErrorCode::Internal, format!("backup failed: {e}")),
        }
    }

    fn handle_backup_list(&self) -> Response {
        let backup_dir = self.db_path.join("backups");
        if !backup_dir.exists() {
            return Response::ok(json!([]));
        }

        match std::fs::read_dir(&backup_dir) {
            Ok(entries) => {
                let backups: Vec<String> = entries
                    .filter_map(std::result::Result::ok)
                    .filter(|e| e.path().is_dir())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect();
                Response::ok(json!(backups))
            }
            Err(e) => Response::error(ErrorCode::Internal, format!("failed to list backups: {e}")),
        }
    }

    fn handle_consumer_group_list() -> Response {
        Response::ok(json!([]))
    }

    fn handle_consumer_group_show(name: &str) -> Response {
        Response::error(
            ErrorCode::NotFound,
            format!("consumer group not found: {name}"),
        )
    }

    async fn build_health_response(&self) -> Response {
        let ctrl = self.controller.read().await;
        let raft_status = self.rx_raft_status.borrow().clone();

        let is_leader = raft_status.is_leader;
        let leader_exists = is_leader || raft_status.leader_id.is_some();
        let alive_nodes: Vec<u16> = ctrl.alive_nodes().iter().map(|n| n.get()).collect();

        let mut partitions_ready = 0u16;
        for partition in PartitionId::all() {
            if ctrl.partition_map().get(partition).primary.is_some() {
                partitions_ready += 1;
            }
        }

        let ready = leader_exists && partitions_ready == NUM_PARTITIONS;
        let health_status = if partitions_ready == NUM_PARTITIONS && leader_exists {
            "healthy"
        } else if partitions_ready > 0 && leader_exists {
            "degraded"
        } else {
            "unhealthy"
        };

        Response::ok(json!({
            "health_status": health_status,
            "ready": ready,
            "mode": "cluster",
            "details": {
                "node_id": self.node_id.get(),
                "is_leader": is_leader,
                "raft_term": raft_status.current_term,
                "partitions_ready": partitions_ready,
                "partitions_total": NUM_PARTITIONS,
                "alive_nodes": alive_nodes
            }
        }))
    }
}

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
