// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use crate::database::Database;
use crate::vault_backend::{VaultBackend, VaultError};
use mqdb_core::constraint::Constraint;
use mqdb_core::protocol::{AdminOperation, build_request, parse_admin_topic, parse_db_topic};
use mqdb_core::transport::Response;
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqtt5::QoS;
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::broker::{AclRule, Permission};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use serde_json::Value;
use std::path::Path;
use tracing::{error, info_span, warn};

#[cfg(feature = "http-api")]
use crate::http::rate_limiter::RateLimiter;

#[cfg(feature = "opentelemetry")]
use mqtt5::telemetry::propagation;

use tracing::Instrument;

async fn publish_response(
    client: &MqttClient,
    response_topic: &str,
    correlation_data: Option<&[u8]>,
    payload: Vec<u8>,
) {
    let props = mqtt5::types::PublishProperties {
        correlation_data: correlation_data.map(Vec::from),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        qos: QoS::AtLeastOnce,
        properties: props,
        ..Default::default()
    };
    if let Err(e) = client
        .publish_with_options(response_topic, payload, options)
        .await
    {
        error!("Failed to publish response to {response_topic}: {e}");
    }
}

pub(super) struct MessageContext<'a> {
    pub db: &'a Database,
    pub client: &'a MqttClient,
    pub backup_dir: &'a Path,
    pub ownership: &'a OwnershipConfig,
    pub scope_config: &'a ScopeConfig,
    pub auth_providers: Option<&'a ComprehensiveAuthProvider>,
    pub vault_backend: &'a Arc<dyn VaultBackend>,
    #[cfg(feature = "http-api")]
    pub auth_rate_limiter: &'a RateLimiter,
    #[cfg(feature = "http-api")]
    pub identity_crypto: Option<&'a Arc<crate::http::IdentityCrypto>>,
}

#[allow(clippy::too_many_lines)]
pub(super) async fn handle_message(ctx: &MessageContext<'_>, message: Message) {
    let db = ctx.db;
    let client = ctx.client;
    let ownership = ctx.ownership;
    let scope_config = ctx.scope_config;
    let vault_backend = ctx.vault_backend;
    let topic = &message.topic;

    if topic.contains("/events") {
        return;
    }

    if let Some(admin_op) = parse_admin_topic(topic) {
        let admin_ctx = AdminContext {
            db,
            client,
            message: &message,
            backup_dir: ctx.backup_dir,
            auth_providers: ctx.auth_providers,
            ownership,
            scope_config,
            vault_backend,
            #[cfg(feature = "http-api")]
            auth_rate_limiter: ctx.auth_rate_limiter,
            #[cfg(feature = "http-api")]
            identity_crypto: ctx.identity_crypto,
        };
        handle_admin_operation(&admin_ctx, admin_op).await;
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
                let response = Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string());
                if let Ok(payload) = serde_json::to_vec(&response) {
                    publish_response(
                        client,
                        response_topic,
                        message.properties.correlation_data.as_deref(),
                        payload,
                    )
                    .await;
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

    if vault_backend.is_eligible(&op.entity, ownership, sender_uid)
        && let Some(uid) = sender_uid
    {
        vault_backend.await_read_fence(uid).await;
    }

    let (request, vault_constraint) = match vault_backend
        .encrypt_request(db, &op.entity, ownership, sender_uid, request)
        .await
    {
        Ok((r, vc)) => (r, vc),
        Err(vault_err) => {
            let err_response = vault_error_to_response(&vault_err);
            if let Some(response_topic) = &message.properties.response_topic
                && let Ok(payload) = serde_json::to_vec(&err_response)
            {
                publish_response(
                    client,
                    response_topic,
                    message.properties.correlation_data.as_deref(),
                    payload,
                )
                .await;
            }
            return;
        }
    };

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

    let mut response = db
        .execute_with_sender(
            request,
            sender_uid,
            mqtt_client_id,
            ownership,
            scope_config,
            vault_constraint,
        )
        .instrument(span)
        .await;

    vault_backend
        .decrypt_response(&op.entity, op.operation, ownership, sender_uid, &mut response)
        .await;

    if let Some(response_topic) = &message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                publish_response(
                    client,
                    response_topic,
                    message.properties.correlation_data.as_deref(),
                    payload,
                )
                .await;
            }
            Err(e) => {
                error!("Failed to serialize response: {}", e);
            }
        }
    }
}

fn vault_error_to_response(err: &VaultError) -> Response {
    match err {
        VaultError::NotEnabled => {
            Response::error(mqdb_core::ErrorCode::BadRequest, "vault not enabled")
        }
        VaultError::AlreadyEnabled => {
            Response::error(mqdb_core::ErrorCode::Conflict, "vault already enabled")
        }
        VaultError::InvalidPassphrase => {
            Response::error(mqdb_core::ErrorCode::Unauthorized, "invalid passphrase")
        }
        VaultError::NotUnlocked => {
            Response::error(mqdb_core::ErrorCode::Unauthorized, "vault not unlocked")
        }
        VaultError::RateLimited => {
            Response::error(mqdb_core::ErrorCode::Conflict, "rate limited")
        }
        VaultError::PassphraseTooShort(n) => Response::error(
            mqdb_core::ErrorCode::BadRequest,
            format!("passphrase must be at least {n} characters"),
        ),
        VaultError::Unavailable => {
            Response::error(mqdb_core::ErrorCode::BadRequest, "vault not available")
        }
        VaultError::BadRequest(m) => Response::error(mqdb_core::ErrorCode::BadRequest, m.as_str()),
        VaultError::Internal(m) => Response::error(mqdb_core::ErrorCode::Internal, m.as_str()),
    }
}

struct AdminContext<'a> {
    db: &'a Database,
    client: &'a MqttClient,
    message: &'a Message,
    backup_dir: &'a Path,
    auth_providers: Option<&'a ComprehensiveAuthProvider>,
    ownership: &'a OwnershipConfig,
    scope_config: &'a ScopeConfig,
    vault_backend: &'a Arc<dyn VaultBackend>,
    #[cfg(feature = "http-api")]
    auth_rate_limiter: &'a RateLimiter,
    #[cfg(feature = "http-api")]
    identity_crypto: Option<&'a Arc<crate::http::IdentityCrypto>>,
}

#[allow(clippy::too_many_lines)]
async fn handle_admin_operation(ctx: &AdminContext<'_>, op: AdminOperation) {
    let payload: Value = if ctx.message.payload.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&ctx.message.payload) {
            Ok(v) => v,
            Err(e) => {
                if let Some(response_topic) = &ctx.message.properties.response_topic {
                    let response = Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string());
                    if let Ok(payload) = serde_json::to_vec(&response) {
                        publish_response(
                            ctx.client,
                            response_topic,
                            ctx.message.properties.correlation_data.as_deref(),
                            payload,
                        )
                        .await;
                    }
                }
                return;
            }
        }
    };

    let response = match op {
        AdminOperation::SchemaSet { entity } => handle_schema_set(ctx.db, entity, payload).await,
        AdminOperation::SchemaGet { entity } => handle_schema_get(ctx.db, &entity).await,
        AdminOperation::ConstraintAdd { entity } => {
            handle_constraint_add(ctx.db, entity, &payload).await
        }
        AdminOperation::ConstraintList { entity } => handle_constraint_list(ctx.db, &entity).await,
        AdminOperation::Backup => handle_backup(ctx.db, &payload, ctx.backup_dir).await,
        AdminOperation::Restore => Response::error(
            mqdb_core::ErrorCode::Internal,
            "restore requires agent restart - use CLI with --restore flag",
        ),
        AdminOperation::BackupList => handle_backup_list(ctx.backup_dir).await,
        AdminOperation::Subscribe => handle_subscribe(ctx.db, &payload).await,
        AdminOperation::Heartbeat { sub_id } => handle_heartbeat(ctx.db, &sub_id).await,
        AdminOperation::Unsubscribe { sub_id } => handle_unsubscribe(ctx.db, &sub_id).await,
        AdminOperation::ConsumerGroupList => handle_consumer_group_list(ctx.db).await,
        AdminOperation::ConsumerGroupShow { name } => {
            handle_consumer_group_show(ctx.db, &name).await
        }
        AdminOperation::Health => handle_health(ctx.db),
        AdminOperation::UserAdd => handle_user_add(ctx.auth_providers, &payload),
        AdminOperation::UserDelete => handle_user_delete(ctx.auth_providers, &payload),
        AdminOperation::UserList => handle_user_list(ctx.auth_providers),
        AdminOperation::AclRuleAdd => handle_acl_rule_add(ctx.auth_providers, &payload).await,
        AdminOperation::AclRuleRemove => handle_acl_rule_remove(ctx.auth_providers, &payload).await,
        AdminOperation::AclRuleList => handle_acl_rule_list(ctx.auth_providers, &payload).await,
        AdminOperation::AclRoleAdd => handle_acl_role_add(ctx.auth_providers, &payload).await,
        AdminOperation::AclRoleDelete => handle_acl_role_delete(ctx.auth_providers, &payload).await,
        AdminOperation::AclRoleList => handle_acl_role_list(ctx.auth_providers).await,
        AdminOperation::AclAssignmentAssign => {
            handle_acl_assignment_assign(ctx.auth_providers, &payload).await
        }
        AdminOperation::AclAssignmentUnassign => {
            handle_acl_assignment_unassign(ctx.auth_providers, &payload).await
        }
        AdminOperation::AclAssignmentList => {
            handle_acl_assignment_list(ctx.auth_providers, &payload).await
        }
        AdminOperation::IndexAdd { entity } => handle_index_add(ctx.db, entity, &payload).await,
        AdminOperation::Catalog => handle_catalog(ctx.db, ctx.ownership, ctx.scope_config).await,
        AdminOperation::VaultEnable
        | AdminOperation::VaultUnlock
        | AdminOperation::VaultLock
        | AdminOperation::VaultDisable
        | AdminOperation::VaultChange
        | AdminOperation::VaultStatus => dispatch_vault_admin_mqtt(ctx, &op, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::PasswordChange => handle_password_change_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::PasswordResetStart => handle_password_reset_start_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::PasswordResetSubmit => {
            handle_password_reset_submit_mqtt(ctx, &payload).await
        }
        #[cfg(not(feature = "http-api"))]
        AdminOperation::PasswordChange
        | AdminOperation::PasswordResetStart
        | AdminOperation::PasswordResetSubmit => {
            Response::error(mqdb_core::ErrorCode::Forbidden, "requires http-api feature")
        }
    };

    if let Some(response_topic) = &ctx.message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                publish_response(
                    ctx.client,
                    response_topic,
                    ctx.message.properties.correlation_data.as_deref(),
                    payload,
                )
                .await;
            }
            Err(e) => {
                error!("Failed to serialize admin response: {e}");
            }
        }
    }
}

async fn handle_schema_set(db: &Database, entity: String, payload: Value) -> Response {
    use serde_json::json;
    let mut fields: std::collections::HashMap<String, mqdb_core::schema::FieldDefinition> =
        match serde_json::from_value(payload) {
            Ok(f) => f,
            Err(e) => return Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string()),
        };
    for (key, def) in &mut fields {
        if def.name.is_empty() {
            def.name.clone_from(key);
        }
    }
    let schema = mqdb_core::schema::Schema::with_fields(entity, fields);
    match db.add_schema(schema).await {
        Ok(()) => Response::ok(json!({"message": "schema set"})),
        Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
    }
}

async fn handle_schema_get(db: &Database, entity: &str) -> Response {
    match db.get_schema(entity).await {
        Some(schema) => Response::ok(serde_json::to_value(schema).unwrap_or(Value::Null)),
        None => Response::error(
            mqdb_core::ErrorCode::NotFound,
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
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "index requires fields array",
        );
    }

    match db.add_index(entity, fields).await {
        Ok(()) => Response::ok(json!({"message": "index added"})),
        Err(e) => Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string()),
    }
}

async fn handle_constraint_add(db: &Database, entity: String, payload: &Value) -> Response {
    use serde_json::json;

    let constraint_type = payload.get("type").and_then(|v| v.as_str());

    let result = match constraint_type {
        Some("unique") => {
            let mut fields: Vec<String> = payload
                .get("fields")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            if fields.is_empty()
                && let Some(f) = payload.get("field").and_then(|v| v.as_str())
            {
                fields.push(f.to_string());
            }

            if fields.is_empty() {
                Err("unique constraint requires 'fields' or 'field'".to_string())
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
        Err(e) => Response::error(mqdb_core::ErrorCode::BadRequest, e),
    }
}

async fn handle_foreign_key_constraint(
    db: &Database,
    entity: String,
    payload: &Value,
) -> Result<(), String> {
    use mqdb_core::constraint::OnDeleteAction;

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
    let data: Vec<Value> = constraints.iter().map(Constraint::to_api_value).collect();
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
            mqdb_core::ErrorCode::BadRequest,
            "invalid backup name: must be alphanumeric, underscore, or hyphen only",
        );
    }

    if let Err(e) = tokio::fs::create_dir_all(backup_dir).await {
        return Response::error(
            mqdb_core::ErrorCode::Internal,
            format!("failed to create backup directory: {e}"),
        );
    }

    let backup_path = backup_dir.join(name);
    match db.backup(&backup_path) {
        Ok(()) => Response::ok(json!({"message": format!("backup created: {name}")})),
        Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
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
            mqdb_core::ErrorCode::Internal,
            format!("failed to read backup directory: {e}"),
        ),
    }
}

async fn handle_subscribe(db: &Database, payload: &Value) -> Response {
    use mqdb_core::subscription::SubscriptionMode;
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
            Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
        }
    } else {
        match db.subscribe(pattern, entity).await {
            Ok(id) => Response::ok(json!({"id": id})),
            Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
        }
    }
}

async fn handle_heartbeat(db: &Database, sub_id: &str) -> Response {
    use serde_json::json;
    match db.heartbeat(sub_id).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => Response::error(mqdb_core::ErrorCode::NotFound, e.to_string()),
    }
}

async fn handle_unsubscribe(db: &Database, sub_id: &str) -> Response {
    use serde_json::json;
    match db.unsubscribe(sub_id).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
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
            mqdb_core::ErrorCode::NotFound,
            format!("consumer group not found: {name}"),
        ),
    }
}

async fn handle_catalog(
    db: &Database,
    ownership: &OwnershipConfig,
    scope_config: &ScopeConfig,
) -> Response {
    use serde_json::json;

    let entity_names = db.entity_names().await;
    let all_constraints = db.all_constraints().await;

    let mut entities = Vec::new();
    for name in &entity_names {
        let record_count = db.entity_record_count(name);
        let schema = db.get_schema(name).await;
        let constraints = all_constraints.get(name).cloned().unwrap_or_default();
        let constraint_data: Vec<Value> =
            constraints.iter().map(Constraint::to_api_value).collect();

        let ownership_info = ownership
            .owner_field(name)
            .map(|field| json!({"field": field}));
        let scope_info = if scope_config.is_empty() {
            None
        } else {
            Some(json!({
                "entity": scope_config.scope_entity(),
                "field": scope_config.scope_field()
            }))
        };

        let schema_info = schema.map(|s| {
            let fields = serde_json::to_value(&s.fields).unwrap_or(Value::Null);
            json!({
                "entity": s.entity,
                "version": s.version,
                "schema": fields
            })
        });

        entities.push(json!({
            "name": name,
            "record_count": record_count,
            "schema": schema_info,
            "constraints": constraint_data,
            "ownership": ownership_info,
            "scope": scope_info,
            "vault_eligible": !name.starts_with('_') && ownership.entity_owner_fields.contains_key(name),
        }));
    }

    Response::ok(json!({
        "entities": entities,
        "server": {
            "mode": "agent",
        }
    }))
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
    Response::error(
        mqdb_core::ErrorCode::Forbidden,
        "authentication not configured",
    )
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: username");
    };
    let Some(password) = payload.get("password").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: password");
    };
    if auth.password_provider().has_user(username) {
        return Response::error(mqdb_core::ErrorCode::Conflict, "user already exists");
    }
    match auth
        .password_provider()
        .add_user(username.to_string(), password)
    {
        Ok(()) => Response::ok(json!({"message": "user added"})),
        Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
    }
}

fn handle_user_delete(auth: Option<&ComprehensiveAuthProvider>, payload: &Value) -> Response {
    use serde_json::json;
    let Some(auth) = auth else {
        return no_auth_response();
    };
    let Some(username) = payload.get("username").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: username");
    };
    if !auth.password_provider().has_user(username) {
        return Response::error(mqdb_core::ErrorCode::NotFound, "user not found");
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: topic");
    };
    let Some(access_str) = payload.get("access").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: access");
    };
    let Some(permission) = parse_permission(access_str) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(topic) = payload.get("topic").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: topic");
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: role");
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().remove_role(role_name).await {
        return Response::error(mqdb_core::ErrorCode::NotFound, "role not found");
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: role");
    };
    match auth.acl_manager().assign_role(user, role).await {
        Ok(()) => Response::ok(json!({"message": "role assigned"})),
        Err(e) => Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string()),
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: user");
    };
    let Some(role) = payload.get("role").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: role");
    };
    if !auth.acl_manager().unassign_role(user, role).await {
        return Response::error(mqdb_core::ErrorCode::NotFound, "assignment not found");
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
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing field: user");
    };
    let roles = auth.acl_manager().get_user_roles(user).await;
    Response::ok(json!(roles))
}

#[cfg(feature = "http-api")]
fn extract_sender(message: &Message) -> Option<&str> {
    message
        .properties
        .user_properties
        .iter()
        .find(|(k, _)| k == "x-mqtt-sender")
        .map(|(_, v)| v.as_str())
}

async fn dispatch_vault_admin_mqtt(
    ctx: &AdminContext<'_>,
    op: &AdminOperation,
    payload: &Value,
) -> Response {
    let Some(canonical_id) = ctx
        .message
        .properties
        .user_properties
        .iter()
        .find(|(k, _)| k == "x-mqtt-sender")
        .map(|(_, v)| v.as_str())
    else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let result = match op {
        AdminOperation::VaultEnable => {
            let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
                return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase");
            };
            ctx.vault_backend
                .admin_enable(ctx.db, ctx.ownership, canonical_id, passphrase)
                .await
        }
        AdminOperation::VaultUnlock => {
            let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
                return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase");
            };
            ctx.vault_backend
                .admin_unlock(ctx.db, ctx.ownership, canonical_id, passphrase)
                .await
        }
        AdminOperation::VaultLock => ctx.vault_backend.admin_lock(canonical_id).await,
        AdminOperation::VaultDisable => {
            let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
                return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase");
            };
            ctx.vault_backend
                .admin_disable(ctx.db, ctx.ownership, canonical_id, passphrase)
                .await
        }
        AdminOperation::VaultChange => {
            let Some(old_passphrase) = payload.get("current_passphrase").and_then(|v| v.as_str())
            else {
                return Response::error(
                    mqdb_core::ErrorCode::BadRequest,
                    "missing current_passphrase",
                );
            };
            let Some(new_passphrase) = payload.get("new_passphrase").and_then(|v| v.as_str()) else {
                return Response::error(
                    mqdb_core::ErrorCode::BadRequest,
                    "missing new_passphrase",
                );
            };
            ctx.vault_backend
                .admin_change(
                    ctx.db,
                    ctx.ownership,
                    canonical_id,
                    old_passphrase,
                    new_passphrase,
                )
                .await
        }
        AdminOperation::VaultStatus => ctx.vault_backend.admin_status(ctx.db, canonical_id).await,
        _ => unreachable!("dispatch_vault_admin_mqtt received non-vault op"),
    };

    match result {
        Ok(outcome) => Response::ok(outcome.body),
        Err(err) => vault_error_to_response(&err),
    }
}

#[cfg(feature = "http-api")]
async fn handle_password_change_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(current_password) = payload.get("current_password").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing current_password field",
        );
    };

    let Some(new_password) = payload.get("new_password").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing new_password field",
        );
    };

    if let Err(e) = crate::http::credentials::validate_password(new_password) {
        return Response::error(mqdb_core::ErrorCode::BadRequest, e);
    }

    if !ctx.auth_rate_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many attempts, try again later",
        );
    }

    let Some(identity) =
        crate::db_helpers::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    let email_verified = identity
        .get("email_verified")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !email_verified {
        return Response::error(
            mqdb_core::ErrorCode::Forbidden,
            "email must be verified before changing password",
        );
    }

    let Some(cred) = crate::db_helpers::read_entity_db(ctx.db, "_credentials", canonical_id).await
    else {
        return Response::error(
            mqdb_core::ErrorCode::NotFound,
            "no credentials found (OAuth-only account)",
        );
    };

    let Some(stored_hash) = cred.get("password_hash").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::Internal,
            "credential record is corrupt",
        );
    };

    if !crate::http::credentials::verify_password(stored_hash, current_password) {
        return Response::error(
            mqdb_core::ErrorCode::Unauthorized,
            "incorrect current password",
        );
    }

    let new_hash = match crate::http::credentials::hash_password(new_password) {
        Ok(h) => h,
        Err(e) => {
            error!("password hashing failed: {e}");
            return Response::error(mqdb_core::ErrorCode::Internal, "internal error");
        }
    };

    crate::db_helpers::update_entity_db(
        ctx.db,
        "_credentials",
        canonical_id,
        &json!({"password_hash": new_hash}),
    )
    .await;

    Response::ok(json!({"status": "password changed"}))
}

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_password_reset_start_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(email) = payload.get("email").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing email field");
    };

    if !ctx.auth_rate_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many attempts, try again later",
        );
    }

    let Some(identity) =
        crate::db_helpers::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    let email_lower = email.to_lowercase();
    let email_matches = if let Some(crypto) = ctx.identity_crypto {
        let computed = crypto.blind_index("_identities", &email_lower);
        identity
            .get("email_hash")
            .and_then(|v| v.as_str())
            .is_some_and(|stored| stored == computed)
    } else {
        identity
            .get("primary_email")
            .and_then(|v| v.as_str())
            .is_some_and(|stored| stored.to_lowercase() == email_lower)
    };
    if !email_matches {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "email does not match identity",
        );
    }

    let target_hash = crate::http::credentials::compute_email_hash(
        ctx.identity_crypto.map(std::convert::AsRef::as_ref),
        email,
    );

    if let Some(challenges) = crate::db_helpers::list_entities_db(
        ctx.db,
        "_verification_challenges",
        &format!("target_hash={target_hash}"),
    )
    .await
    {
        for challenge in challenges {
            let status = challenge
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if status != "pending" && status != "delivered" {
                continue;
            }
            if let Some(id) = challenge.get("id").and_then(|v| v.as_str()) {
                crate::db_helpers::update_entity_db(
                    ctx.db,
                    "_verification_challenges",
                    id,
                    &json!({"status": "expired"}),
                )
                .await;
            }
        }
    }

    let challenge_id = crate::http::challenge_utils::uuid_v4();
    let Some(code) = crate::http::challenge_utils::generate_verification_code() else {
        return Response::error(mqdb_core::ErrorCode::Internal, "RNG failure");
    };
    let code_hash = crate::http::challenge_utils::hash_code(&code);
    let now = crate::http::challenge_utils::now_unix();
    let expires_at = now + 600;

    let challenge_data = json!({
        "id": challenge_id,
        "canonical_id": canonical_id,
        "method": "email",
        "target_hash": target_hash,
        "code_hash": code_hash,
        "status": "pending",
        "mode": "code",
        "purpose": "password_reset",
        "attempts": 0,
        "max_attempts": 5,
        "created_at": now.to_string(),
        "expires_at": expires_at.to_string(),
    });

    if !crate::db_helpers::create_entity_db(ctx.db, "_verification_challenges", &challenge_data)
        .await
    {
        return Response::error(
            mqdb_core::ErrorCode::Internal,
            "failed to create reset challenge",
        );
    }

    let notification = json!({
        "challenge_id": challenge_id,
        "method": "email",
        "mode": "code",
        "purpose": "password_reset",
        "target": email,
        "code": code,
        "expires_at": expires_at.to_string(),
    });
    let notify_payload = serde_json::to_vec(&notification).unwrap_or_default();
    if let Err(e) = ctx
        .client
        .publish("$DB/_verify/challenges/email", notify_payload)
        .await
    {
        warn!("failed to publish password reset notification: {e}");
    }

    Response::ok(json!({
        "status": "reset_started",
        "challenge_id": challenge_id,
        "expires_in": 600,
    }))
}

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_password_reset_submit_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(challenge_id) = payload.get("challenge_id").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing challenge_id field",
        );
    };

    let Some(submitted_code) = payload.get("code").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing code field");
    };

    let Some(new_password) = payload.get("new_password").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing new_password field",
        );
    };

    if let Err(e) = crate::http::credentials::validate_password(new_password) {
        return Response::error(mqdb_core::ErrorCode::BadRequest, e);
    }

    if !ctx.auth_rate_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many attempts, try again later",
        );
    }

    let Some(challenge) =
        crate::db_helpers::read_entity_db(ctx.db, "_verification_challenges", challenge_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "challenge not found");
    };

    let purpose = challenge
        .get("purpose")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if purpose != "password_reset" {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "invalid challenge type");
    }

    let challenge_canonical = challenge
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if challenge_canonical != canonical_id {
        return Response::error(
            mqdb_core::ErrorCode::Forbidden,
            "challenge belongs to another user",
        );
    }

    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" && status != "delivered" {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "invalid or expired challenge",
        );
    }

    let expires_at = challenge
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let now = crate::http::challenge_utils::now_unix();
    if expires_at > 0 && now >= expires_at {
        crate::db_helpers::update_entity_db(
            ctx.db,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "expired"}),
        )
        .await;
        return Response::error(mqdb_core::ErrorCode::BadRequest, "challenge has expired");
    }

    let attempts = challenge
        .get("attempts")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let max_attempts = challenge
        .get("max_attempts")
        .and_then(Value::as_u64)
        .unwrap_or(5);
    if attempts >= max_attempts {
        crate::db_helpers::update_entity_db(
            ctx.db,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "failed"}),
        )
        .await;
        return Response::error(mqdb_core::ErrorCode::BadRequest, "too many attempts");
    }

    let new_attempts = attempts + 1;
    let submitted_hash = crate::http::challenge_utils::hash_code(submitted_code);
    let stored_hash = challenge
        .get("code_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if submitted_hash != stored_hash {
        let new_status = if new_attempts >= max_attempts {
            "failed"
        } else {
            status
        };
        crate::db_helpers::update_entity_db(
            ctx.db,
            "_verification_challenges",
            challenge_id,
            &json!({"attempts": new_attempts, "status": new_status}),
        )
        .await;
        return Response::error(mqdb_core::ErrorCode::Unauthorized, "invalid code");
    }

    crate::db_helpers::update_entity_db(
        ctx.db,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "verified", "attempts": new_attempts}),
    )
    .await;

    let new_hash = match crate::http::credentials::hash_password(new_password) {
        Ok(h) => h,
        Err(e) => {
            error!("password hashing failed: {e}");
            return Response::error(mqdb_core::ErrorCode::Internal, "internal error");
        }
    };

    let target_hash = challenge
        .get("target_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let existing_creds =
        crate::db_helpers::read_entity_db(ctx.db, "_credentials", canonical_id).await;
    if existing_creds.is_some() {
        crate::db_helpers::update_entity_db(
            ctx.db,
            "_credentials",
            canonical_id,
            &json!({"password_hash": new_hash}),
        )
        .await;
    } else {
        crate::db_helpers::create_entity_db(
            ctx.db,
            "_credentials",
            &json!({
                "id": canonical_id,
                "password_hash": new_hash,
                "email_hash": target_hash,
            }),
        )
        .await;
    }

    crate::db_helpers::update_entity_db(
        ctx.db,
        "_identities",
        canonical_id,
        &json!({"email_verified": true}),
    )
    .await;

    Response::ok(json!({"status": "password_reset"}))
}
