// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::database::Database;
use crate::http::VaultCrypto;
use crate::vault_transform::{
    build_vault_skip_fields, ensure_id, is_vault_eligible, vault_decrypt_fields,
    vault_encrypt_fields,
};
use mqdb_core::VaultKeyStore;
use mqdb_core::constraint::Constraint;
use mqdb_core::protocol::{AdminOperation, DbOp, build_request, parse_admin_topic, parse_db_topic};
use mqdb_core::transport::{Request, Response};
use mqdb_core::types::{OwnershipConfig, OwnershipDecision, ScopeConfig};
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::broker::{AclRule, Permission};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use serde_json::Value;
use std::path::Path;
use tracing::{debug, error, info_span, warn};

#[cfg(feature = "http-api")]
use crate::http::rate_limiter::RateLimiter;

#[cfg(feature = "opentelemetry")]
use mqtt5::telemetry::propagation;

use tracing::Instrument;

pub(super) struct MessageContext<'a> {
    pub db: &'a Database,
    pub client: &'a MqttClient,
    pub backup_dir: &'a Path,
    pub ownership: &'a OwnershipConfig,
    pub scope_config: &'a ScopeConfig,
    pub auth_providers: Option<&'a ComprehensiveAuthProvider>,
    pub vault_key_store: &'a VaultKeyStore,
    #[cfg(feature = "http-api")]
    pub vault_unlock_limiter: &'a RateLimiter,
}

#[allow(clippy::too_many_lines)]
pub(super) async fn handle_message(ctx: &MessageContext<'_>, message: Message) {
    let db = ctx.db;
    let client = ctx.client;
    let ownership = ctx.ownership;
    let scope_config = ctx.scope_config;
    let vault_key_store = ctx.vault_key_store;
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
            vault_key_store,
            #[cfg(feature = "http-api")]
            vault_unlock_limiter: ctx.vault_unlock_limiter,
        };
        handle_admin_operation(&admin_ctx, admin_op).await;
        return;
    }

    let Some(op) = parse_db_topic(topic) else {
        warn!("Invalid $DB topic format: {}", topic);
        return;
    };

    let mut request = match build_request(op.clone(), &message.payload) {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to build request from {}: {}", topic, e);
            if let Some(response_topic) = &message.properties.response_topic {
                let response = Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string());
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

    let has_vault = sender_uid
        .filter(|_| is_vault_eligible(&op.entity, ownership))
        .is_some();

    if has_vault && let Some(uid) = sender_uid {
        vault_key_store.read_fence(uid).await;
    }

    let vault_crypto = sender_uid
        .filter(|_| is_vault_eligible(&op.entity, ownership))
        .and_then(|uid| vault_key_store.get(uid))
        .and_then(|key_bytes| VaultCrypto::from_key_bytes(&key_bytes));

    let create_constraint_data = if vault_crypto.is_some() {
        if let Request::Create { ref mut data, .. } = request {
            ensure_id(data);
            Some(data.clone())
        } else {
            None
        }
    } else {
        None
    };

    let (request, update_constraint_data) = if let Some(ref crypto) = vault_crypto {
        match vault_transform_request(db, crypto, &op.entity, ownership, request, sender_uid).await
        {
            Ok((r, ucd)) => (r, ucd),
            Err(err_response) => {
                if let Some(response_topic) = &message.properties.response_topic
                    && let Ok(payload) = serde_json::to_vec(&err_response)
                {
                    let _ = client.publish_qos1(response_topic, payload).await;
                }
                return;
            }
        }
    } else {
        (request, None)
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

    let vault_constraint = if let Some(cd) = create_constraint_data {
        Some(mqdb_core::VaultConstraintData::Create(cd))
    } else {
        update_constraint_data
            .map(|(new_data, old_data)| mqdb_core::VaultConstraintData::Update(new_data, old_data))
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

    if let Some(ref crypto) = vault_crypto {
        vault_decrypt_response(crypto, &op.entity, op.operation, ownership, &mut response);
    }

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

async fn vault_transform_request(
    db: &Database,
    crypto: &VaultCrypto,
    entity: &str,
    ownership: &OwnershipConfig,
    request: Request,
    sender_uid: Option<&str>,
) -> Result<(Request, Option<(Value, Value)>), Response> {
    let skip = build_vault_skip_fields(entity, ownership);
    match request {
        Request::Create { entity, mut data } => {
            let id = ensure_id(&mut data);
            vault_encrypt_fields(crypto, &entity, &id, &mut data, &skip);
            debug!(entity = %entity, id = %id, "vault-encrypted create");
            Ok((Request::Create { entity, data }, None))
        }
        Request::Update {
            entity,
            id,
            fields: delta,
        } => {
            let params = VaultUpdateParams {
                crypto,
                skip_fields: &skip,
                sender_uid,
                ownership,
            };
            let (encrypted_request, constraint_data) =
                vault_pre_update(db, &params, &entity, &id, delta).await?;
            debug!(entity = %entity, id = %id, "vault-encrypted update");
            Ok((encrypted_request, constraint_data))
        }
        other => Ok((other, None)),
    }
}

struct VaultUpdateParams<'a> {
    crypto: &'a VaultCrypto,
    skip_fields: &'a [String],
    sender_uid: Option<&'a str>,
    ownership: &'a OwnershipConfig,
}

async fn vault_pre_update(
    db: &Database,
    params: &VaultUpdateParams<'_>,
    entity: &str,
    id: &str,
    delta: Value,
) -> Result<(Request, Option<(Value, Value)>), Response> {
    if let OwnershipDecision::Check {
        owner_field,
        sender: uid,
    } = params.ownership.evaluate(entity, params.sender_uid)
        && let Err(e) = db.check_ownership(entity, id, owner_field, uid)
    {
        return Err(e.into());
    }

    let Ok(mut decrypted_existing) = db
        .read(entity.to_string(), id.to_string(), vec![], None)
        .await
    else {
        return Ok((
            Request::Update {
                entity: entity.to_string(),
                id: id.to_string(),
                fields: delta,
            },
            None,
        ));
    };

    vault_decrypt_fields(
        params.crypto,
        entity,
        id,
        &mut decrypted_existing,
        params.skip_fields,
    );

    let plaintext_existing = decrypted_existing.clone();

    if let (Some(base), Some(patch)) = (decrypted_existing.as_object_mut(), delta.as_object()) {
        for (k, v) in patch {
            base.insert(k.clone(), v.clone());
        }
    }

    let plaintext_merged = decrypted_existing.clone();

    if let Some(obj) = decrypted_existing.as_object_mut() {
        obj.remove("id");
        for sf in params.skip_fields {
            if sf != "id" {
                obj.remove(sf.as_str());
            }
        }
    }

    let mut merged = decrypted_existing;
    vault_encrypt_fields(params.crypto, entity, id, &mut merged, params.skip_fields);

    Ok((
        Request::Update {
            entity: entity.to_string(),
            id: id.to_string(),
            fields: merged,
        },
        Some((plaintext_merged, plaintext_existing)),
    ))
}

fn vault_decrypt_response(
    crypto: &VaultCrypto,
    entity: &str,
    operation: DbOp,
    ownership: &OwnershipConfig,
    response: &mut Response,
) {
    let Response::Ok { data } = response else {
        return;
    };

    let skip = build_vault_skip_fields(entity, ownership);

    match operation {
        DbOp::Create | DbOp::Read | DbOp::Update => {
            if let Some(id) = data.get("id").and_then(|v| v.as_str()).map(String::from) {
                vault_decrypt_fields(crypto, entity, &id, data, &skip);
            }
        }
        DbOp::List => {
            if let Some(items) = data.as_array_mut() {
                for item in items {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()).map(String::from) {
                        vault_decrypt_fields(crypto, entity, &id, item, &skip);
                    }
                }
            }
        }
        DbOp::Delete => {}
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
    vault_key_store: &'a VaultKeyStore,
    #[cfg(feature = "http-api")]
    vault_unlock_limiter: &'a RateLimiter,
}

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
                        let _ = ctx.client.publish_qos1(response_topic, payload).await;
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
        #[cfg(feature = "http-api")]
        AdminOperation::VaultEnable => handle_vault_enable_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::VaultUnlock => handle_vault_unlock_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::VaultLock => handle_vault_lock_mqtt(ctx),
        #[cfg(feature = "http-api")]
        AdminOperation::VaultDisable => handle_vault_disable_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::VaultChange => handle_vault_change_mqtt(ctx, &payload).await,
        #[cfg(feature = "http-api")]
        AdminOperation::VaultStatus => handle_vault_status_mqtt(ctx).await,
        #[cfg(not(feature = "http-api"))]
        AdminOperation::VaultEnable
        | AdminOperation::VaultUnlock
        | AdminOperation::VaultLock
        | AdminOperation::VaultDisable
        | AdminOperation::VaultChange
        | AdminOperation::VaultStatus => Response::error(
            mqdb_core::ErrorCode::Forbidden,
            "vault requires http-api feature",
        ),
    };

    if let Some(response_topic) = &ctx.message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                if let Err(e) = ctx.client.publish_qos1(response_topic, payload).await {
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
            "vault_eligible": is_vault_eligible(name, ownership),
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

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_vault_enable_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase field");
    };

    let Some(identity) =
        crate::vault_ops::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    if identity
        .get("vault_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Response::error(mqdb_core::ErrorCode::Conflict, "vault already enabled");
    }

    let salt = VaultCrypto::generate_salt();
    let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);

    let check_token = match crypto.create_check_token() {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "vault check token creation failed");
            return Response::error(mqdb_core::ErrorCode::Internal, "encryption failed");
        }
    };

    let _fence = ctx.vault_key_store.acquire_fence(canonical_id).await;
    ctx.vault_key_store.set(canonical_id, key_bytes);

    let salt_b64 = BASE64.encode(salt);
    let migration_start = json!({
        "vault_enabled": true,
        "vault_salt": salt_b64,
        "vault_check": check_token,
        "vault_migration_status": "pending",
        "vault_migration_mode": "encrypt",
    });
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &migration_start)
        .await;

    let batch = crate::vault_ops::batch_vault_operation_db(
        ctx.db,
        ctx.ownership,
        canonical_id,
        &crypto,
        crate::vault_ops::VaultMode::Encrypt,
    )
    .await;

    let migration_done = json!({"vault_migration_status": "complete"});
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &migration_done).await;

    let mut body = json!({"status": "enabled", "records_encrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    Response::ok(body)
}

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_vault_unlock_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase field");
    };

    if !ctx.vault_unlock_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many unlock attempts, try again later",
        );
    }

    let Some(identity) =
        crate::vault_ops::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    if !identity
        .get("vault_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "vault not enabled");
    }

    let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::Internal,
            "vault salt missing from identity",
        );
    };
    let Ok(salt) = BASE64.decode(salt_b64) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "invalid vault salt");
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "vault check token missing");
    };

    let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);
    if !crypto.verify_check_token(check_token) {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "incorrect passphrase");
    }

    let _fence = ctx.vault_key_store.acquire_fence(canonical_id).await;
    ctx.vault_key_store.set(canonical_id, key_bytes);

    let resume_result = crate::vault_ops::resume_pending_migration_db(
        ctx.db,
        ctx.ownership,
        ctx.vault_key_store,
        canonical_id,
        &crypto,
        &identity,
        passphrase,
    )
    .await;

    let status = if resume_result.as_ref().is_some_and(|r| r.mode == "decrypt") {
        "vault_disabled"
    } else {
        "unlocked"
    };
    let mut body = json!({"status": status});
    if let Some(migration) = resume_result {
        body["migration_resumed"] = json!(migration.mode);
        body["records_processed"] = json!(migration.succeeded);
        if migration.failed > 0 {
            body["migration_failed"] = json!(migration.failed);
        }
    }
    Response::ok(body)
}

#[cfg(feature = "http-api")]
fn handle_vault_lock_mqtt(ctx: &AdminContext<'_>) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    ctx.vault_key_store.remove(canonical_id);
    Response::ok(json!({"status": "locked"}))
}

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_vault_disable_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(passphrase) = payload.get("passphrase").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "missing passphrase field");
    };

    if !ctx.vault_unlock_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many unlock attempts, try again later",
        );
    }

    let Some(identity) =
        crate::vault_ops::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    if !identity
        .get("vault_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "vault not enabled");
    }

    let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "vault salt missing");
    };
    let Ok(salt) = BASE64.decode(salt_b64) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "invalid vault salt");
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "vault check token missing");
    };

    let crypto = VaultCrypto::derive(passphrase, &salt);
    if !crypto.verify_check_token(check_token) {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "incorrect passphrase");
    }

    let _fence = ctx.vault_key_store.acquire_fence(canonical_id).await;
    ctx.vault_key_store.remove(canonical_id);

    let migration_start = json!({
        "vault_migration_status": "pending",
        "vault_migration_mode": "decrypt",
    });
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &migration_start)
        .await;

    let batch = crate::vault_ops::batch_vault_operation_db(
        ctx.db,
        ctx.ownership,
        canonical_id,
        &crypto,
        crate::vault_ops::VaultMode::Decrypt,
    )
    .await;

    let identity_update = json!({
        "vault_enabled": false,
        "vault_salt": null,
        "vault_check": null,
        "vault_migration_status": "complete",
        "vault_migration_mode": null,
    });
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &identity_update)
        .await;

    let mut body = json!({"status": "disabled", "records_decrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    Response::ok(body)
}

#[cfg(feature = "http-api")]
#[allow(clippy::too_many_lines)]
async fn handle_vault_change_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let Some(old_passphrase) = payload.get("old_passphrase").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing old_passphrase field",
        );
    };
    let Some(new_passphrase) = payload.get("new_passphrase").and_then(|v| v.as_str()) else {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing new_passphrase field",
        );
    };

    if !ctx.vault_unlock_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many unlock attempts, try again later",
        );
    }

    let Some(identity) =
        crate::vault_ops::read_entity_db(ctx.db, "_identities", canonical_id).await
    else {
        return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
    };

    if !identity
        .get("vault_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Response::error(mqdb_core::ErrorCode::BadRequest, "vault not enabled");
    }

    let Some(old_salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "vault salt missing");
    };
    let Ok(old_salt) = BASE64.decode(old_salt_b64) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "invalid vault salt");
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return Response::error(mqdb_core::ErrorCode::Internal, "vault check token missing");
    };

    let old_crypto = VaultCrypto::derive(old_passphrase, &old_salt);
    if !old_crypto.verify_check_token(check_token) {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "incorrect old passphrase");
    }

    let new_salt = VaultCrypto::generate_salt();
    let (new_crypto, new_key_bytes) = VaultCrypto::derive_with_raw_key(new_passphrase, &new_salt);

    let new_check = match new_crypto.create_check_token() {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "new vault check token creation failed");
            return Response::error(mqdb_core::ErrorCode::Internal, "encryption failed");
        }
    };

    let _fence = ctx.vault_key_store.acquire_fence(canonical_id).await;
    ctx.vault_key_store.set(canonical_id, new_key_bytes);

    let new_salt_b64 = BASE64.encode(new_salt);
    let old_salt_b64_encoded = BASE64.encode(&old_salt);
    let migration_start = json!({
        "vault_salt": new_salt_b64,
        "vault_check": new_check,
        "vault_migration_status": "pending",
        "vault_migration_mode": "re_encrypt",
        "vault_old_check": check_token,
        "vault_old_salt": old_salt_b64_encoded,
    });
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &migration_start)
        .await;

    let batch = crate::vault_ops::batch_vault_re_encrypt_db(
        ctx.db,
        ctx.ownership,
        canonical_id,
        &old_crypto,
        &new_crypto,
    )
    .await;

    let migration_done = json!({
        "vault_migration_status": "complete",
        "vault_migration_mode": null,
        "vault_old_check": null,
        "vault_old_salt": null,
    });
    crate::vault_ops::update_entity_db(ctx.db, "_identities", canonical_id, &migration_done).await;

    let mut body = json!({"status": "changed", "records_re_encrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    Response::ok(body)
}

#[cfg(feature = "http-api")]
async fn handle_vault_status_mqtt(ctx: &AdminContext<'_>) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let identity = crate::vault_ops::read_entity_db(ctx.db, "_identities", canonical_id).await;
    let vault_enabled = identity
        .as_ref()
        .and_then(|i| i.get("vault_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let migration_pending = identity
        .as_ref()
        .and_then(|i| i.get("vault_migration_status"))
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "pending");

    let unlocked = ctx.vault_key_store.get(canonical_id).is_some();

    let mut body = json!({"vault_enabled": vault_enabled, "unlocked": unlocked});
    if migration_pending {
        body["migration_pending"] = json!(true);
    }
    Response::ok(body)
}
