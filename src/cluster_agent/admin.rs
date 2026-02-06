use super::{AdminRequest, ClusteredAgent};
use crate::cluster::{NUM_PARTITIONS, NodeId, PartitionId};
use crate::transport::{ErrorCode, Response};
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::broker::{AclRule, Permission};
use serde_json::{Value, json};
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
        } else if let Some(rest) = req.topic.strip_prefix("$DB/_admin/") {
            let payload: Value = if req.payload.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&req.payload).unwrap_or(Value::Null)
            };
            match rest {
                "users/add" => handle_user_add(self.auth_providers.as_deref(), &payload),
                "users/delete" => handle_user_delete(self.auth_providers.as_deref(), &payload),
                "users/list" => handle_user_list(self.auth_providers.as_deref()),
                "acl/rules/add" => {
                    handle_acl_rule_add(self.auth_providers.as_deref(), &payload).await
                }
                "acl/rules/remove" => {
                    handle_acl_rule_remove(self.auth_providers.as_deref(), &payload).await
                }
                "acl/rules/list" => {
                    handle_acl_rule_list(self.auth_providers.as_deref(), &payload).await
                }
                "acl/roles/add" => {
                    handle_acl_role_add(self.auth_providers.as_deref(), &payload).await
                }
                "acl/roles/delete" => {
                    handle_acl_role_delete(self.auth_providers.as_deref(), &payload).await
                }
                "acl/roles/list" => handle_acl_role_list(self.auth_providers.as_deref()).await,
                "acl/assignments/assign" => {
                    handle_acl_assignment_assign(self.auth_providers.as_deref(), &payload).await
                }
                "acl/assignments/unassign" => {
                    handle_acl_assignment_unassign(self.auth_providers.as_deref(), &payload).await
                }
                "acl/assignments/list" => {
                    handle_acl_assignment_list(self.auth_providers.as_deref(), &payload).await
                }
                _ => Response::error(
                    ErrorCode::BadRequest,
                    format!("unknown admin command: {}", req.topic),
                ),
            }
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

fn no_auth_response() -> Response {
    Response::error(ErrorCode::Forbidden, "authentication not configured")
}

fn parse_permission(s: &str) -> Option<Permission> {
    match s {
        "read" | "subscribe" => Some(Permission::Read),
        "write" | "publish" => Some(Permission::Write),
        "readwrite" | "rw" | "all" => Some(Permission::ReadWrite),
        "deny" | "none" => Some(Permission::Deny),
        _ => None,
    }
}

fn permission_str(p: Permission) -> &'static str {
    match p {
        Permission::Read => "read",
        Permission::Write => "write",
        Permission::ReadWrite => "readwrite",
        Permission::Deny => "deny",
    }
}

fn handle_user_add(auth: Option<&ComprehensiveAuthProvider>, payload: &Value) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(username) = payload.get("username").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: username");
    };
    let Some(password) = payload.get("password").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: password");
    };
    if auth.password_provider().has_user(username) {
        return Response::error(ErrorCode::Conflict, "user already exists");
    }
    match auth
        .password_provider()
        .add_user(username.to_string(), password)
    {
        Ok(()) => Response::ok(json!({"message": "user added"})),
        Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
    }
}

fn handle_user_delete(auth: Option<&ComprehensiveAuthProvider>, payload: &Value) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(username) = payload.get("username").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: username");
    };
    if !auth.password_provider().has_user(username) {
        return Response::error(ErrorCode::NotFound, "user not found");
    }
    let _ = auth.password_provider().remove_user(username);
    Response::ok(json!({"message": "user deleted"}))
}

fn handle_user_list(auth: Option<&ComprehensiveAuthProvider>) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let users = auth.password_provider().list_users();
    Response::ok(json!(users))
}

async fn handle_acl_rule_add(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: topic");
    };
    let Some(access_str) = payload.get("access").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: access");
    };
    let Some(permission) = parse_permission(access_str) else {
        return Response::error(
            ErrorCode::BadRequest,
            format!("invalid access value: {access_str}"),
        );
    };
    let rule = AclRule::new(user.to_string(), topic.to_string(), permission);
    auth.acl_manager().add_rule(rule).await;
    Response::ok(json!({"message": "rule added"}))
}

async fn handle_acl_rule_remove(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: topic");
    };
    auth.acl_manager().remove_rule(user, topic).await;
    Response::ok(json!({"message": "rule removed"}))
}

async fn handle_acl_rule_list(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let user_filter = payload.get("user").and_then(|v| v.as_str());
    let rules = if let Some(user) = user_filter {
        auth.acl_manager().list_user_rules(user).await
    } else {
        auth.acl_manager().list_rules().await
    };
    let data: Vec<Value> = rules
        .iter()
        .map(|r| {
            json!({
                "user": r.username,
                "topic": r.topic_pattern,
                "access": permission_str(r.permission)
            })
        })
        .collect();
    Response::ok(json!(data))
}

async fn handle_acl_role_add(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(role_name) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: role");
    };
    auth.acl_manager().add_role(role_name.to_string()).await;
    if let Some(rules) = payload.get("rules").and_then(|v| v.as_array()) {
        for rule_val in rules {
            let Some(topic) = rule_val.get("topic").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(access_str) = rule_val.get("access").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(permission) = parse_permission(access_str) else {
                continue;
            };
            let _ = auth
                .acl_manager()
                .add_role_rule(role_name, topic.to_string(), permission)
                .await;
        }
    }
    Response::ok(json!({"message": "role added"}))
}

async fn handle_acl_role_delete(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(role_name) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().remove_role(role_name).await {
        return Response::error(ErrorCode::NotFound, "role not found");
    }
    Response::ok(json!({"message": "role deleted"}))
}

async fn handle_acl_role_list(auth: Option<&ComprehensiveAuthProvider>) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let role_names = auth.acl_manager().list_roles().await;
    let mut roles = serde_json::Map::new();
    for name in &role_names {
        if let Some(role) = auth.acl_manager().get_role(name).await {
            let role_entries: Vec<Value> = role
                .rules
                .iter()
                .map(|r| {
                    json!({
                        "topic": r.topic_pattern,
                        "access": permission_str(r.permission)
                    })
                })
                .collect();
            roles.insert(name.clone(), json!(role_entries));
        }
    }
    Response::ok(json!(roles))
}

async fn handle_acl_assignment_assign(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: role");
    };
    match auth.acl_manager().assign_role(user, role).await {
        Ok(()) => Response::ok(json!({"message": "role assigned"})),
        Err(e) => Response::error(ErrorCode::BadRequest, e.to_string()),
    }
}

async fn handle_acl_assignment_unassign(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().unassign_role(user, role).await {
        return Response::error(ErrorCode::NotFound, "assignment not found");
    }
    Response::ok(json!({"message": "role unassigned"}))
}

async fn handle_acl_assignment_list(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(ErrorCode::BadRequest, "missing field: user");
    };
    let roles = auth.acl_manager().get_user_roles(user).await;
    Response::ok(json!(roles))
}
