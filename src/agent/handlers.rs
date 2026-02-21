// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::protocol::{AdminOperation, build_request, parse_admin_topic, parse_db_topic};
use crate::types::{OwnershipConfig, ScopeConfig};
use crate::{Database, Response};
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::broker::{AclRule, Permission};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use serde_json::Value;
use std::path::Path;
use tracing::{error, info_span, warn};

#[cfg(feature = "opentelemetry")]
use mqtt5::telemetry::propagation;

use tracing::Instrument;

pub(super) async fn handle_message(
    db: &Database,
    client: &MqttClient,
    message: Message,
    backup_dir: &Path,
    ownership: &OwnershipConfig,
    scope_config: &ScopeConfig,
    auth_providers: Option<&ComprehensiveAuthProvider>,
) {
    let topic = &message.topic;

    if topic.contains("/events") {
        return;
    }

    if let Some(admin_op) = parse_admin_topic(topic) {
        handle_admin_operation(db, client, &message, admin_op, backup_dir, auth_providers).await;
        return;
    }

    let Some(op) = parse_db_topic(topic) else {
        warn!("Invalid $DB topic format: {}", topic);
        return;
    };

    let request = match build_request(op.clone(), &message.payload) {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to build request from {}: {}", topic, e);
            if let Some(response_topic) = &message.properties.response_topic {
                let response = Response::error(crate::ErrorCode::BadRequest, e.to_string());
                if let Ok(payload) = serde_json::to_vec(&response) {
                    let _ = client.publish_qos1(response_topic, payload).await;
                }
            }
            return;
        }
    };

    let sender_uid = message
        .properties
        .user_properties
        .iter()
        .find(|(k, _)| k == "x-mqtt-sender")
        .map(|(_, v)| v.as_str());

    let mqtt_client_id = message
        .properties
        .user_properties
        .iter()
        .find(|(k, _)| k == "x-mqtt-client-id")
        .map(|(_, v)| v.as_str());

    let span = info_span!(
        "database_operation",
        entity = %op.entity,
        operation = %op.operation,
        id = ?op.id
    );

    #[cfg(feature = "opentelemetry")]
    let span = {
        use opentelemetry::Context;
        use opentelemetry::trace::{SpanContext, TraceContextExt, TraceState};
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let user_props: Vec<(String, String)> = message.properties.user_properties.clone();

        if let Some(parent_cx) = propagation::extract_trace_context(&user_props) {
            let parent = SpanContext::new(
                parent_cx.trace_id(),
                parent_cx.span_id(),
                parent_cx.trace_flags(),
                false,
                TraceState::default(),
            );
            let _ = span.set_parent(Context::current().with_remote_span_context(parent));
        }
        span
    };

    let response = db
        .execute_with_sender(request, sender_uid, mqtt_client_id, ownership, scope_config)
        .instrument(span)
        .await;

    if let Some(response_topic) = &message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                if let Err(e) = client.publish_qos1(response_topic, payload).await {
                    error!("Failed to publish response to {}: {}", response_topic, e);
                }
            }
            Err(e) => {
                error!("Failed to serialize response: {}", e);
            }
        }
    }
}

async fn handle_admin_operation(
    db: &Database,
    client: &MqttClient,
    message: &Message,
    op: AdminOperation,
    backup_dir: &Path,
    auth_providers: Option<&ComprehensiveAuthProvider>,
) {
    let payload: Value = if message.payload.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&message.payload) {
            Ok(v) => v,
            Err(e) => {
                if let Some(response_topic) = &message.properties.response_topic {
                    let response = Response::error(crate::ErrorCode::BadRequest, e.to_string());
                    if let Ok(payload) = serde_json::to_vec(&response) {
                        let _ = client.publish_qos1(response_topic, payload).await;
                    }
                }
                return;
            }
        }
    };

    let response = match op {
        AdminOperation::SchemaSet { entity } => handle_schema_set(db, entity, payload).await,
        AdminOperation::SchemaGet { entity } => handle_schema_get(db, &entity).await,
        AdminOperation::ConstraintAdd { entity } => {
            handle_constraint_add(db, entity, &payload).await
        }
        AdminOperation::ConstraintList { entity } => handle_constraint_list(db, &entity).await,
        AdminOperation::Backup => handle_backup(db, &payload, backup_dir).await,
        AdminOperation::Restore => Response::error(
            crate::ErrorCode::Internal,
            "restore requires agent restart - use CLI with --restore flag",
        ),
        AdminOperation::BackupList => handle_backup_list(backup_dir).await,
        AdminOperation::Subscribe => handle_subscribe(db, &payload).await,
        AdminOperation::Heartbeat { sub_id } => handle_heartbeat(db, &sub_id).await,
        AdminOperation::Unsubscribe { sub_id } => handle_unsubscribe(db, &sub_id).await,
        AdminOperation::ConsumerGroupList => handle_consumer_group_list(db).await,
        AdminOperation::ConsumerGroupShow { name } => handle_consumer_group_show(db, &name).await,
        AdminOperation::Health => handle_health(db),
        AdminOperation::UserAdd => handle_user_add(auth_providers, &payload),
        AdminOperation::UserDelete => handle_user_delete(auth_providers, &payload),
        AdminOperation::UserList => handle_user_list(auth_providers),
        AdminOperation::AclRuleAdd => handle_acl_rule_add(auth_providers, &payload).await,
        AdminOperation::AclRuleRemove => handle_acl_rule_remove(auth_providers, &payload).await,
        AdminOperation::AclRuleList => handle_acl_rule_list(auth_providers, &payload).await,
        AdminOperation::AclRoleAdd => handle_acl_role_add(auth_providers, &payload).await,
        AdminOperation::AclRoleDelete => handle_acl_role_delete(auth_providers, &payload).await,
        AdminOperation::AclRoleList => handle_acl_role_list(auth_providers).await,
        AdminOperation::AclAssignmentAssign => {
            handle_acl_assignment_assign(auth_providers, &payload).await
        }
        AdminOperation::AclAssignmentUnassign => {
            handle_acl_assignment_unassign(auth_providers, &payload).await
        }
        AdminOperation::AclAssignmentList => {
            handle_acl_assignment_list(auth_providers, &payload).await
        }
        AdminOperation::IndexAdd { entity } => handle_index_add(db, entity, &payload).await,
    };

    if let Some(response_topic) = &message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                if let Err(e) = client.publish_qos1(response_topic, payload).await {
                    error!("Failed to publish admin response to {response_topic}: {e}");
                }
            }
            Err(e) => {
                error!("Failed to serialize admin response: {e}");
            }
        }
    }
}

async fn handle_schema_set(db: &Database, entity: String, payload: Value) -> Response {
    use serde_json::json;
    match serde_json::from_value::<crate::schema::Schema>(payload) {
        Ok(mut schema) => {
            schema.entity = entity;
            match db.add_schema(schema).await {
                Ok(()) => Response::ok(json!({"message": "schema set"})),
                Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
            }
        }
        Err(e) => Response::error(crate::ErrorCode::BadRequest, e.to_string()),
    }
}

async fn handle_schema_get(db: &Database, entity: &str) -> Response {
    match db.get_schema(entity).await {
        Some(schema) => Response::ok(serde_json::to_value(schema).unwrap_or(Value::Null)),
        None => Response::error(
            crate::ErrorCode::NotFound,
            format!("no schema for entity: {entity}"),
        ),
    }
}

async fn handle_index_add(db: &Database, entity: String, payload: &Value) -> Response {
    use serde_json::json;

    let fields: Vec<String> = payload
        .get("fields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    if fields.is_empty() {
        return Response::error(crate::ErrorCode::BadRequest, "index requires fields array");
    }

    match db.add_index(entity, fields).await {
        Ok(()) => Response::ok(json!({"message": "index added"})),
        Err(e) => Response::error(crate::ErrorCode::BadRequest, e.to_string()),
    }
}

async fn handle_constraint_add(db: &Database, entity: String, payload: &Value) -> Response {
    use serde_json::json;

    let constraint_type = payload.get("type").and_then(|v| v.as_str());

    let result = match constraint_type {
        Some("unique") => {
            let fields: Vec<String> = payload
                .get("fields")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            if fields.is_empty() {
                Err("unique constraint requires fields array".to_string())
            } else {
                db.add_unique_constraint(entity, fields)
                    .await
                    .map_err(|e| e.to_string())
            }
        }
        Some("not_null") => {
            let field = payload
                .get("field")
                .and_then(|v| v.as_str())
                .map(String::from);

            match field {
                Some(f) => db.add_not_null(entity, f).await.map_err(|e| e.to_string()),
                None => Err("not_null constraint requires field".to_string()),
            }
        }
        Some("foreign_key") => handle_foreign_key_constraint(db, entity, payload).await,
        _ => Err(format!("unknown constraint type: {constraint_type:?}")),
    };

    match result {
        Ok(()) => Response::ok(json!({"message": "constraint added"})),
        Err(e) => Response::error(crate::ErrorCode::BadRequest, e),
    }
}

async fn handle_foreign_key_constraint(
    db: &Database,
    entity: String,
    payload: &Value,
) -> Result<(), String> {
    use crate::constraint::OnDeleteAction;

    let source_field = payload.get("field").and_then(|v| v.as_str());
    let target_entity = payload.get("target_entity").and_then(|v| v.as_str());
    let target_field = payload.get("target_field").and_then(|v| v.as_str());
    let on_delete = payload
        .get("on_delete")
        .and_then(|v| v.as_str())
        .unwrap_or("restrict");

    let action = match on_delete {
        "cascade" => OnDeleteAction::Cascade,
        "set_null" => OnDeleteAction::SetNull,
        _ => OnDeleteAction::Restrict,
    };

    match (source_field, target_entity, target_field) {
        (Some(sf), Some(te), Some(tf)) => db
            .add_foreign_key(
                entity,
                sf.to_string(),
                te.to_string(),
                tf.to_string(),
                action,
            )
            .await
            .map_err(|e| e.to_string()),
        _ => Err("foreign_key requires field, target_entity, target_field".to_string()),
    }
}

async fn handle_constraint_list(db: &Database, entity: &str) -> Response {
    use serde_json::json;
    let constraints = db.list_constraints(entity).await;
    let data: Vec<Value> = constraints
        .into_iter()
        .map(|c| serde_json::to_value(c).unwrap_or(Value::Null))
        .collect();
    Response::ok(json!(data))
}

async fn handle_backup(db: &Database, payload: &Value, backup_dir: &Path) -> Response {
    use serde_json::json;
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("backup");

    if !is_valid_backup_name(name) {
        return Response::error(
            crate::ErrorCode::BadRequest,
            "invalid backup name: must be alphanumeric, underscore, or hyphen only",
        );
    }

    if let Err(e) = tokio::fs::create_dir_all(backup_dir).await {
        return Response::error(
            crate::ErrorCode::Internal,
            format!("failed to create backup directory: {e}"),
        );
    }

    let backup_path = backup_dir.join(name);
    match db.backup(&backup_path) {
        Ok(()) => Response::ok(json!({"message": format!("backup created: {name}")})),
        Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
    }
}

async fn handle_backup_list(backup_dir: &Path) -> Response {
    use serde_json::json;
    if !backup_dir.exists() {
        return Response::ok(json!(Vec::<String>::new()));
    }

    match tokio::fs::read_dir(backup_dir).await {
        Ok(mut entries) => {
            let mut backups = Vec::new();
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.path().is_dir()
                    && let Ok(name) = entry.file_name().into_string()
                {
                    backups.push(name);
                }
            }
            Response::ok(json!(backups))
        }
        Err(e) => Response::error(
            crate::ErrorCode::Internal,
            format!("failed to read backup directory: {e}"),
        ),
    }
}

async fn handle_subscribe(db: &Database, payload: &Value) -> Response {
    use crate::subscription::SubscriptionMode;
    use serde_json::json;

    let pattern = payload
        .get("pattern")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let entity = payload
        .get("entity")
        .and_then(|v| v.as_str())
        .map(String::from);
    let group = payload
        .get("group")
        .and_then(|v| v.as_str())
        .map(String::from);
    let mode_str = payload
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("broadcast");

    let mode = match mode_str {
        "load-balanced" | "load_balanced" => SubscriptionMode::LoadBalanced,
        "ordered" => SubscriptionMode::Ordered,
        _ => SubscriptionMode::Broadcast,
    };

    if let Some(group) = group {
        match db.subscribe_shared(pattern, entity, group, mode).await {
            Ok(result) => Response::ok(json!({
                "id": result.id,
                "partitions": result.assigned_partitions
            })),
            Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
        }
    } else {
        match db.subscribe(pattern, entity).await {
            Ok(id) => Response::ok(json!({"id": id})),
            Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
        }
    }
}

async fn handle_heartbeat(db: &Database, sub_id: &str) -> Response {
    use serde_json::json;
    match db.heartbeat(sub_id).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => Response::error(crate::ErrorCode::NotFound, e.to_string()),
    }
}

async fn handle_unsubscribe(db: &Database, sub_id: &str) -> Response {
    use serde_json::json;
    match db.unsubscribe(sub_id).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
    }
}

async fn handle_consumer_group_list(db: &Database) -> Response {
    let groups = db.list_consumer_groups().await;
    Response::ok(serde_json::to_value(groups).unwrap_or(Value::Null))
}

async fn handle_consumer_group_show(db: &Database, name: &str) -> Response {
    match db.get_consumer_group(name).await {
        Some(details) => Response::ok(serde_json::to_value(details).unwrap_or(Value::Null)),
        None => Response::error(
            crate::ErrorCode::NotFound,
            format!("consumer group not found: {name}"),
        ),
    }
}

fn handle_health(db: &Database) -> Response {
    use serde_json::json;
    Response::ok(json!({
        "status": "healthy",
        "ready": true,
        "mode": "agent",
        "details": {
            "partitions": db.num_partitions()
        }
    }))
}

fn is_valid_backup_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn no_auth_response() -> Response {
    Response::error(crate::ErrorCode::Forbidden, "authentication not configured")
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(username) = payload.get("username").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: username");
    };
    let Some(password) = payload.get("password").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: password");
    };
    if auth.password_provider().has_user(username) {
        return Response::error(crate::ErrorCode::Conflict, "user already exists");
    }
    match auth
        .password_provider()
        .add_user(username.to_string(), password)
    {
        Ok(()) => Response::ok(json!({"message": "user added"})),
        Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
    }
}

fn handle_user_delete(auth: Option<&ComprehensiveAuthProvider>, payload: &Value) -> Response {
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(username) = payload.get("username").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: username");
    };
    if !auth.password_provider().has_user(username) {
        return Response::error(crate::ErrorCode::NotFound, "user not found");
    }
    let _ = auth.password_provider().remove_user(username);
    Response::ok(json!({"message": "user deleted"}))
}

fn handle_user_list(auth: Option<&ComprehensiveAuthProvider>) -> Response {
    use serde_json::json;
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: topic");
    };
    let Some(access_str) = payload.get("access").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: access");
    };
    let Some(permission) = parse_permission(access_str) else {
        return Response::error(
            crate::ErrorCode::BadRequest,
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: topic");
    };
    auth.acl_manager().remove_rule(user, topic).await;
    Response::ok(json!({"message": "rule removed"}))
}

async fn handle_acl_rule_list(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    use serde_json::json;
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(role_name) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: role");
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(role_name) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().remove_role(role_name).await {
        return Response::error(crate::ErrorCode::NotFound, "role not found");
    }
    Response::ok(json!({"message": "role deleted"}))
}

async fn handle_acl_role_list(auth: Option<&ComprehensiveAuthProvider>) -> Response {
    use serde_json::json;
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
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: role");
    };
    match auth.acl_manager().assign_role(user, role).await {
        Ok(()) => Response::ok(json!({"message": "role assigned"})),
        Err(e) => Response::error(crate::ErrorCode::BadRequest, e.to_string()),
    }
}

async fn handle_acl_assignment_unassign(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().unassign_role(user, role).await {
        return Response::error(crate::ErrorCode::NotFound, "assignment not found");
    }
    Response::ok(json!({"message": "role unassigned"}))
}

async fn handle_acl_assignment_list(
    auth: Option<&ComprehensiveAuthProvider>,
    payload: &Value,
) -> Response {
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(user) = payload.get("user").and_then(|v| v.as_str()) else {
        return Response::error(crate::ErrorCode::BadRequest, "missing field: user");
    };
    let roles = auth.acl_manager().get_user_roles(user).await;
    Response::ok(json!(roles))
}
