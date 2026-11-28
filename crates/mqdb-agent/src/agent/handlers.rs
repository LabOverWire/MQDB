// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::database::{Database, is_valid_backup_name};
use crate::response_limit::{ReplyKind, publish_reply};
use crate::vault_backend::{VaultBackend, VaultError};
use mqdb_core::constraint::Constraint;
use mqdb_core::protocol::{AdminOperation, build_request, parse_admin_topic, parse_db_topic};
use mqdb_core::transport::Response;
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqtt5::QoS;
use mqtt5::broker::auth::{AuthProvider, ComprehensiveAuthProvider};
use mqtt5::broker::{AclRule, Permission};
use mqtt5::client::MqttClient;
use mqtt5::types::Message;
use serde_json::Value;
use std::path::Path;
use tracing::{error, info_span, warn};

#[cfg(feature = "http-api")]
use crate::rate_limiter::RateLimiter;

#[cfg(feature = "opentelemetry")]
use mqtt5::telemetry::propagation;

use tracing::Instrument;

pub(super) const RESPONSE_PUBLISHER_CLIENT_ID: &str = "mqdb-response-publisher";

const VERIFY_TOPIC_PREFIX: &str = "$DB/_verify/";

fn user_property<'a>(message: &'a Message, key: &str) -> Option<&'a str> {
    message
        .properties
        .user_properties
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn is_own_response(
    service_username: Option<&str>,
    sender: Option<&str>,
    client_id: Option<&str>,
) -> bool {
    client_id == Some(RESPONSE_PUBLISHER_CLIENT_ID) && sender == service_username
}

async fn publish_response(
    client: &MqttClient,
    response_topic: &str,
    correlation_data: Option<&[u8]>,
    payload: Vec<u8>,
    kind: ReplyKind,
) {
    let options = mqtt5::PublishOptions {
        qos: QoS::AtLeastOnce,
        properties: mqtt5::types::PublishProperties {
            correlation_data: correlation_data.map(Vec::from),
            ..Default::default()
        },
        ..Default::default()
    };
    publish_reply(client, response_topic, payload, options, kind).await;
}

pub(super) struct MessageContext<'a> {
    pub db: &'a Database,
    pub client: &'a MqttClient,
    pub authorizer: &'a dyn AuthProvider,
    pub service_username: Option<&'a str>,
    pub backup_dir: &'a Path,
    pub ownership: &'a OwnershipConfig,
    pub scope_config: &'a ScopeConfig,
    pub auth_providers: Option<&'a ComprehensiveAuthProvider>,
    pub vault_backend: &'a Arc<dyn VaultBackend>,
    #[cfg(feature = "http-api")]
    pub auth_rate_limiter: &'a RateLimiter,
    #[cfg(feature = "http-api")]
    pub identity_crypto: Option<&'a Arc<crate::http::IdentityCrypto>>,
    #[cfg(feature = "http-api")]
    pub session_store: Option<&'a Arc<crate::http::SessionStore>>,
    #[cfg(feature = "http-api")]
    pub jti_revocation: Option<&'a Arc<crate::http::JtiRevocationStore>>,
}

#[cfg(feature = "http-api")]
pub(crate) async fn resolve_grantee_email(
    db: &Database,
    crypto: &crate::http::IdentityCrypto,
    email: &str,
) -> Result<Option<String>, mqdb_core::error::Error> {
    let hash = crypto.blind_index("_identity_links", email);
    let records =
        crate::db_helpers::list_entities_db(db, "_identity_links", &format!("email_hash={hash}"))
            .await?;
    Ok(records
        .first()
        .and_then(|record| record.get("canonical_id"))
        .and_then(Value::as_str)
        .map(String::from))
}

#[cfg(feature = "http-api")]
async fn resolve_grantee_or_refuse(
    db: &Database,
    crypto: &crate::http::IdentityCrypto,
    email: &str,
) -> Result<Option<String>, Response> {
    resolve_grantee_email(db, crypto, email).await.map_err(|e| {
        warn!("share grantee lookup failed: {e}");
        Response::error(mqdb_core::ErrorCode::Internal, "failed to resolve grantee")
    })
}

#[cfg(feature = "http-api")]
async fn resolve_share_request(
    db: &Database,
    crypto: Option<&Arc<crate::http::IdentityCrypto>>,
    request: mqdb_core::transport::Request,
) -> Result<mqdb_core::transport::Request, Response> {
    use mqdb_core::transport::Request;
    let Some(crypto) = crypto else {
        return Ok(request);
    };
    match request {
        Request::Share {
            entity,
            id,
            grantee,
            permission,
            cascade,
            ..
        } => {
            let trimmed = grantee.trim();
            if trimmed.is_empty() {
                return Ok(Request::Share {
                    entity,
                    id,
                    grantee,
                    grantee_key: None,
                    grantee_email: None,
                    permission,
                    cascade,
                });
            }
            let email = trimmed.to_lowercase();
            let key = crypto.blind_index(mqdb_core::types::SHARES_ENTITY, &email);
            let encrypted_email = crypto
                .encrypt_field(mqdb_core::types::SHARES_ENTITY, &email)
                .map_err(|e| {
                    warn!("share grantee email not encrypted: {e}");
                    Response::error(mqdb_core::ErrorCode::Internal, "failed to encrypt grantee")
                })?;
            let resolved = resolve_grantee_or_refuse(db, crypto, trimmed).await?;
            Ok(Request::Share {
                entity,
                id,
                grantee: resolved.unwrap_or_default(),
                grantee_key: Some(key),
                grantee_email: Some(encrypted_email),
                permission,
                cascade,
            })
        }
        Request::Unshare {
            entity,
            id,
            grantee,
            cascade,
            ..
        } => {
            let email = grantee.trim().to_lowercase();
            let key = crypto.blind_index(mqdb_core::types::SHARES_ENTITY, &email);
            let resolved = resolve_grantee_or_refuse(db, crypto, grantee.trim()).await?;
            Ok(Request::Unshare {
                entity,
                id,
                grantee: key,
                grantee_key: resolved,
                cascade,
            })
        }
        other => Ok(other),
    }
}

#[cfg(feature = "http-api")]
fn decrypt_shares_grantee_email(
    crypto: &Arc<crate::http::IdentityCrypto>,
    response: &mut mqdb_core::transport::Response,
) {
    if let mqdb_core::transport::Response::Ok { data } = response
        && let Some(records) = data.as_array_mut()
    {
        for record in records {
            crypto.decrypt_json_fields(mqdb_core::types::SHARES_ENTITY, record, &["grantee_email"]);
        }
    }
}

async fn is_request_accepted(
    ctx: &MessageContext<'_>,
    message: &Message,
    sender_uid: Option<&str>,
    mqtt_client_id: Option<&str>,
) -> bool {
    let topic = &message.topic;

    if is_own_response(ctx.service_username, sender_uid, mqtt_client_id) {
        return false;
    }

    if crate::topic_rules::is_change_event_topic(topic)
        || topic.starts_with(crate::presence::PRESENCE_TOPIC_PREFIX)
        || topic.starts_with(VERIFY_TOPIC_PREFIX)
    {
        return false;
    }

    if let Some(response_topic) = &message.properties.response_topic
        && !ctx
            .authorizer
            .authorize_publish(
                mqtt_client_id.unwrap_or_default(),
                sender_uid,
                response_topic,
            )
            .await
    {
        warn!(
            topic,
            response_topic,
            client_id = mqtt_client_id,
            "request refused: the sender may not publish to its response topic"
        );
        return false;
    }

    true
}

fn admin_context<'a>(ctx: &MessageContext<'a>, message: &'a Message) -> AdminContext<'a> {
    AdminContext {
        db: ctx.db,
        client: ctx.client,
        message,
        backup_dir: ctx.backup_dir,
        auth_providers: ctx.auth_providers,
        ownership: ctx.ownership,
        scope_config: ctx.scope_config,
        vault_backend: ctx.vault_backend,
        #[cfg(feature = "http-api")]
        auth_rate_limiter: ctx.auth_rate_limiter,
        #[cfg(feature = "http-api")]
        identity_crypto: ctx.identity_crypto,
        #[cfg(feature = "http-api")]
        session_store: ctx.session_store,
        #[cfg(feature = "http-api")]
        jti_revocation: ctx.jti_revocation,
    }
}

async fn reply_read_error(client: &MqttClient, message: &Message, response: &Response) {
    if let Some(response_topic) = &message.properties.response_topic
        && let Ok(payload) = serde_json::to_vec(response)
    {
        publish_response(
            client,
            response_topic,
            message.properties.correlation_data.as_deref(),
            payload,
            ReplyKind::Read,
        )
        .await;
    }
}

#[cfg(feature = "opentelemetry")]
fn with_remote_parent(span: tracing::Span, message: &Message) -> tracing::Span {
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
        if let Err(e) = span.set_parent(Context::current().with_remote_span_context(parent)) {
            tracing::debug!(%e, "request trace parent not attached");
        }
    }
    span
}

pub(super) async fn handle_message(ctx: &MessageContext<'_>, message: Message) {
    let db = ctx.db;
    let client = ctx.client;
    let ownership = ctx.ownership;
    let vault_backend = ctx.vault_backend;
    let topic = &message.topic;
    let sender_uid = user_property(&message, "x-mqtt-sender");
    let mqtt_client_id = user_property(&message, "x-mqtt-client-id");

    if !is_request_accepted(ctx, &message, sender_uid, mqtt_client_id).await {
        return;
    }

    if let Some(admin_op) = parse_admin_topic(topic) {
        handle_admin_operation(&admin_context(ctx, &message), admin_op).await;
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
            let response = Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string());
            reply_read_error(client, &message, &response).await;
            return;
        }
    };

    let vault_eligible = vault_backend.is_eligible(&op.entity, ownership, sender_uid);

    if vault_eligible && let Some(uid) = sender_uid {
        vault_backend.await_read_fence(uid).await;
    }

    let (request, vault_constraint) = if vault_eligible {
        match vault_backend
            .encrypt_request(db, &op.entity, ownership, sender_uid, request)
            .await
        {
            Ok((r, vc)) => (r, vc),
            Err(vault_err) => {
                reply_read_error(client, &message, &vault_error_to_response(&vault_err)).await;
                return;
            }
        }
    } else {
        (request, None)
    };

    #[cfg(feature = "http-api")]
    let request = match resolve_share_request(db, ctx.identity_crypto, request).await {
        Ok(request) => request,
        Err(response) => {
            reply_read_error(client, &message, &response).await;
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
    let span = with_remote_parent(span, &message);

    let mut response = db
        .execute_with_sender(
            request,
            sender_uid,
            mqtt_client_id,
            ownership,
            ctx.scope_config,
            vault_constraint,
        )
        .instrument(span)
        .await;

    if vault_eligible {
        vault_backend
            .decrypt_response(
                &op.entity,
                op.operation,
                ownership,
                sender_uid,
                &mut response,
            )
            .await;
    }

    #[cfg(feature = "http-api")]
    if matches!(op.operation, mqdb_core::protocol::DbOp::Shares)
        && let Some(crypto) = ctx.identity_crypto
    {
        decrypt_shares_grantee_email(crypto, &mut response);
    }

    if let Some(response_topic) = &message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                publish_response(
                    client,
                    response_topic,
                    message.properties.correlation_data.as_deref(),
                    payload,
                    ReplyKind::from(op.operation),
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
            Response::error(mqdb_core::ErrorCode::Forbidden, "incorrect passphrase")
        }
        VaultError::RateLimited => Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many unlock attempts, try again later",
        ),
        VaultError::PassphraseTooShort(n) => Response::error(
            mqdb_core::ErrorCode::BadRequest,
            format!("passphrase must be at least {n} characters"),
        ),
        VaultError::Unavailable => {
            Response::error(mqdb_core::ErrorCode::BadRequest, "vault not available")
        }
        VaultError::NotFound(m) => Response::error(mqdb_core::ErrorCode::NotFound, m.as_str()),
        VaultError::BadRequest(m) => Response::error(mqdb_core::ErrorCode::BadRequest, m.as_str()),
        VaultError::Forbidden(m) => Response::error(mqdb_core::ErrorCode::Forbidden, m.as_str()),
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
    #[cfg(feature = "http-api")]
    session_store: Option<&'a Arc<crate::http::SessionStore>>,
    #[cfg(feature = "http-api")]
    jti_revocation: Option<&'a Arc<crate::http::JtiRevocationStore>>,
}

fn parse_admin_payload(message: &Message) -> Result<Value, Response> {
    if message.payload.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&message.payload)
        .map_err(|e| Response::error(mqdb_core::ErrorCode::BadRequest, e.to_string()))
}

async fn handle_admin_operation(ctx: &AdminContext<'_>, op: AdminOperation) {
    let reply_kind = ReplyKind::from(&op);
    let payload = match parse_admin_payload(ctx.message) {
        Ok(v) => v,
        Err(response) => {
            reply_read_error(ctx.client, ctx.message, &response).await;
            return;
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
            mqdb_core::ErrorCode::BadRequest,
            "restore runs offline: stop the agent, then run mqdb restore --db <path> --name <backup>",
        ),
        AdminOperation::BackupList => handle_backup_list(ctx.backup_dir).await,
        AdminOperation::Subscribe => {
            handle_subscribe(ctx.db, &payload, extract_sender(ctx.message)).await
        }
        AdminOperation::Heartbeat { sub_id } => {
            handle_heartbeat(ctx.db, &sub_id, extract_sender(ctx.message), ctx.ownership).await
        }
        AdminOperation::Unsubscribe { sub_id } => {
            handle_unsubscribe(ctx.db, &sub_id, extract_sender(ctx.message), ctx.ownership).await
        }
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
                    reply_kind,
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
                Err((
                    mqdb_core::ErrorCode::BadRequest,
                    "unique constraint requires 'fields' or 'field'".to_string(),
                ))
            } else {
                db.add_unique_constraint(entity, fields)
                    .await
                    .map_err(|e| admin_write_error(&e))
            }
        }
        Some("not_null") => {
            let field = payload
                .get("field")
                .and_then(|v| v.as_str())
                .map(String::from);

            match field {
                Some(f) => db
                    .add_not_null(entity, f)
                    .await
                    .map_err(|e| admin_write_error(&e)),
                None => Err((
                    mqdb_core::ErrorCode::BadRequest,
                    "not_null constraint requires field".to_string(),
                )),
            }
        }
        Some("foreign_key") => handle_foreign_key_constraint(db, entity, payload).await,
        _ => Err((
            mqdb_core::ErrorCode::BadRequest,
            format!("unknown constraint type: {constraint_type:?}"),
        )),
    };

    match result {
        Ok(()) => Response::ok(json!({"message": "constraint added"})),
        Err((code, message)) => Response::error(code, message),
    }
}

async fn handle_foreign_key_constraint(
    db: &Database,
    entity: String,
    payload: &Value,
) -> Result<(), (mqdb_core::ErrorCode, String)> {
    use mqdb_core::constraint::OnDeleteAction;

    let source_field = payload.get("field").and_then(|v| v.as_str());
    let target_entity = payload.get("target_entity").and_then(|v| v.as_str());
    let target_field = payload.get("target_field").and_then(|v| v.as_str());
    let on_delete = payload
        .get("on_delete")
        .and_then(|v| v.as_str())
        .unwrap_or("restrict");

    let action = match on_delete.to_ascii_lowercase().as_str() {
        "cascade" => OnDeleteAction::Cascade,
        "set_null" | "setnull" => OnDeleteAction::SetNull,
        "restrict" => OnDeleteAction::Restrict,
        _ => {
            return Err((
                mqdb_core::ErrorCode::BadRequest,
                format!("invalid on_delete value: {on_delete}"),
            ));
        }
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
            .map_err(|e| admin_write_error(&e)),
        _ => Err((
            mqdb_core::ErrorCode::BadRequest,
            "foreign_key requires field, target_entity, target_field".to_string(),
        )),
    }
}

fn admin_write_error(error: &mqdb_core::Error) -> (mqdb_core::ErrorCode, String) {
    use mqdb_core::Error;
    if matches!(error, Error::AlreadyExists { .. }) {
        return (
            mqdb_core::ErrorCode::Conflict,
            "constraint already exists".to_string(),
        );
    }
    let code = match error {
        Error::Validation(_)
        | Error::InvalidKey(_)
        | Error::KeyTooLarge(_)
        | Error::SchemaViolation { .. }
        | Error::ConstraintViolation(_)
        | Error::UniqueViolation { .. }
        | Error::ForeignKeyViolation { .. }
        | Error::NotNullViolation { .. }
        | Error::InvalidForeignKey => mqdb_core::ErrorCode::BadRequest,
        _ => mqdb_core::ErrorCode::Internal,
    };
    (code, error.to_string())
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

async fn handle_subscribe(db: &Database, payload: &Value, caller: Option<&str>) -> Response {
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
        match db
            .subscribe_shared(pattern, entity, group, mode, caller)
            .await
        {
            Ok(result) => Response::ok(json!({
                "id": result.id,
                "partitions": result.assigned_partitions
            })),
            Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
        }
    } else {
        match db.subscribe(pattern, entity, caller).await {
            Ok(id) => Response::ok(json!({"id": id})),
            Err(e) => Response::error(mqdb_core::ErrorCode::Internal, e.to_string()),
        }
    }
}

async fn handle_heartbeat(
    db: &Database,
    sub_id: &str,
    caller: Option<&str>,
    ownership: &OwnershipConfig,
) -> Response {
    use serde_json::json;
    match db.heartbeat(sub_id, caller, ownership).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => e.into(),
    }
}

async fn handle_unsubscribe(
    db: &Database,
    sub_id: &str,
    caller: Option<&str>,
    ownership: &OwnershipConfig,
) -> Response {
    use serde_json::json;
    match db.unsubscribe(sub_id, caller, ownership).await {
        Ok(()) => Response::ok(json!({"ok": true})),
        Err(e) => e.into(),
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
    if !auth.password_provider().remove_user(username) {
        return Response::error(mqdb_core::ErrorCode::NotFound, "user not found");
    }
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
    let rule_values = match payload.get("rules") {
        None => &[][..],
        Some(Value::Array(rules)) => rules.as_slice(),
        Some(_) => {
            return Response::error(mqdb_core::ErrorCode::BadRequest, "rules must be an array");
        }
    };
    let mut rules = Vec::new();
    for rule_val in rule_values {
        let topic = rule_val.get("topic").and_then(|v| v.as_str());
        let permission = rule_val
            .get("access")
            .and_then(|v| v.as_str())
            .and_then(parse_permission);
        let (Some(topic), Some(permission)) = (topic, permission) else {
            return Response::error(
                mqdb_core::ErrorCode::BadRequest,
                format!(
                    "invalid rule: {rule_val}: needs a topic and an access of read, write, readwrite or deny"
                ),
            );
        };
        rules.push((topic, permission));
    }
    auth.acl_manager().add_role(role_name.to_string()).await;
    for (topic, permission) in rules {
        if let Err(e) = auth
            .acl_manager()
            .add_role_rule(role_name, topic.to_string(), permission)
            .await
        {
            return Response::error(mqdb_core::ErrorCode::Internal, e.to_string());
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
            let Some(old_passphrase) = payload.get("old_passphrase").and_then(|v| v.as_str())
            else {
                return Response::error(
                    mqdb_core::ErrorCode::BadRequest,
                    "missing old_passphrase field",
                );
            };
            let Some(new_passphrase) = payload.get("new_passphrase").and_then(|v| v.as_str())
            else {
                return Response::error(
                    mqdb_core::ErrorCode::BadRequest,
                    "missing new_passphrase field",
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
fn invalidate_http_sessions(ctx: &AdminContext<'_>, canonical_id: &str) {
    let (Some(sessions), Some(jtis)) = (ctx.session_store, ctx.jti_revocation) else {
        return;
    };
    let revoked = sessions.destroy_others_by_canonical_id(canonical_id, None);
    jtis.revoke_many(&revoked);
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

    let identity =
        match crate::db_helpers::read_entity_db(ctx.db, "_identities", canonical_id).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
            }
            Err(e) => {
                warn!("password change: failed to read identity {canonical_id}: {e}");
                return Response::error(mqdb_core::ErrorCode::Internal, "failed to read identity");
            }
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

    let cred = match crate::db_helpers::read_entity_db(ctx.db, "_credentials", canonical_id).await {
        Ok(Some(v)) => v,
        Ok(None) => {
            return Response::error(
                mqdb_core::ErrorCode::NotFound,
                "no credentials found (OAuth-only account)",
            );
        }
        Err(e) => {
            warn!("password change: failed to read credentials {canonical_id}: {e}");
            return Response::error(mqdb_core::ErrorCode::Internal, "failed to read credentials");
        }
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

    if let Err(e) = crate::db_helpers::update_entity_db(
        ctx.db,
        "_credentials",
        canonical_id,
        &json!({"password_hash": new_hash}),
    )
    .await
    {
        error!("password change: failed to update credentials for {canonical_id}: {e}");
        return Response::error(mqdb_core::ErrorCode::Internal, "failed to update password");
    }

    invalidate_http_sessions(ctx, canonical_id);

    Response::ok(json!({"status": "password changed"}))
}

#[cfg(feature = "http-api")]
fn reset_email_matches(
    crypto: Option<&Arc<crate::http::IdentityCrypto>>,
    identity: &Value,
    email: &str,
) -> bool {
    let email_lower = email.to_lowercase();
    if let Some(crypto) = crypto {
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
    }
}

#[cfg(feature = "http-api")]
async fn expire_open_challenges(db: &Database, target_hash: &str) {
    match crate::db_helpers::list_entities_db(
        db,
        "_verification_challenges",
        &format!("target_hash={target_hash}"),
    )
    .await
    {
        Ok(challenges) => {
            for challenge in challenges {
                let status = challenge
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if status != "pending" && status != "delivered" {
                    continue;
                }
                if let Some(id) = challenge.get("id").and_then(|v| v.as_str())
                    && let Err(e) = crate::db_helpers::update_entity_db(
                        db,
                        "_verification_challenges",
                        id,
                        &serde_json::json!({"status": "expired"}),
                    )
                    .await
                {
                    warn!("password reset start: failed to expire stale challenge {id}: {e}");
                }
            }
        }
        Err(e) => {
            warn!("password reset start: failed to list existing challenges: {e}");
        }
    }
}

#[cfg(feature = "http-api")]
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

    let identity =
        match crate::db_helpers::read_entity_db(ctx.db, "_identities", canonical_id).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Response::error(mqdb_core::ErrorCode::NotFound, "identity not found");
            }
            Err(e) => {
                warn!("password reset start: failed to read identity {canonical_id}: {e}");
                return Response::error(mqdb_core::ErrorCode::Internal, "failed to read identity");
            }
        };

    if !reset_email_matches(ctx.identity_crypto, &identity, email) {
        return Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "email does not match identity",
        );
    }

    let target_hash = crate::http::credentials::compute_email_hash(
        ctx.identity_crypto.map(std::convert::AsRef::as_ref),
        email,
    );

    expire_open_challenges(ctx.db, &target_hash).await;

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

    if let Err(e) =
        crate::db_helpers::create_entity_db(ctx.db, "_verification_challenges", &challenge_data)
            .await
    {
        error!("password reset start: failed to create reset challenge for {canonical_id}: {e}");
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
struct ResetSubmission<'a> {
    challenge_id: &'a str,
    code: &'a str,
    new_password: &'a str,
}

#[cfg(feature = "http-api")]
fn parse_reset_submission(payload: &Value) -> Result<ResetSubmission<'_>, Response> {
    let Some(challenge_id) = payload.get("challenge_id").and_then(|v| v.as_str()) else {
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing challenge_id field",
        ));
    };

    let Some(code) = payload.get("code").and_then(|v| v.as_str()) else {
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing code field",
        ));
    };

    let Some(new_password) = payload.get("new_password").and_then(|v| v.as_str()) else {
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "missing new_password field",
        ));
    };

    if let Err(e) = crate::http::credentials::validate_password(new_password) {
        return Err(Response::error(mqdb_core::ErrorCode::BadRequest, e));
    }

    Ok(ResetSubmission {
        challenge_id,
        code,
        new_password,
    })
}

#[cfg(feature = "http-api")]
async fn read_reset_challenge(db: &Database, challenge_id: &str) -> Result<Value, Response> {
    match crate::db_helpers::read_entity_db(db, "_verification_challenges", challenge_id).await {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err(Response::error(
            mqdb_core::ErrorCode::NotFound,
            "challenge not found",
        )),
        Err(e) => {
            warn!("password reset submit: failed to read challenge {challenge_id}: {e}");
            Err(Response::error(
                mqdb_core::ErrorCode::Internal,
                "failed to read challenge",
            ))
        }
    }
}

#[cfg(feature = "http-api")]
struct OpenChallenge<'a> {
    status: &'a str,
    attempts: u64,
    max_attempts: u64,
    code_hash: &'a str,
}

#[cfg(feature = "http-api")]
async fn check_open_challenge<'a>(
    db: &Database,
    challenge: &'a Value,
    challenge_id: &str,
    canonical_id: &str,
) -> Result<OpenChallenge<'a>, Response> {
    let purpose = challenge
        .get("purpose")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if purpose != "password_reset" {
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "invalid challenge type",
        ));
    }

    let challenge_canonical = challenge
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if challenge_canonical != canonical_id {
        return Err(Response::error(
            mqdb_core::ErrorCode::Forbidden,
            "challenge belongs to another user",
        ));
    }

    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" && status != "delivered" {
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "invalid or expired challenge",
        ));
    }

    let expires_at = challenge
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let now = crate::http::challenge_utils::now_unix();
    if expires_at > 0 && now >= expires_at {
        if let Err(e) = crate::db_helpers::update_entity_db(
            db,
            "_verification_challenges",
            challenge_id,
            &serde_json::json!({"status": "expired"}),
        )
        .await
        {
            warn!("password reset submit: failed to mark challenge {challenge_id} expired: {e}");
        }
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "challenge has expired",
        ));
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
        if let Err(e) = crate::db_helpers::update_entity_db(
            db,
            "_verification_challenges",
            challenge_id,
            &serde_json::json!({"status": "failed"}),
        )
        .await
        {
            warn!("password reset submit: failed to mark challenge {challenge_id} failed: {e}");
        }
        return Err(Response::error(
            mqdb_core::ErrorCode::BadRequest,
            "too many attempts",
        ));
    }

    let code_hash = challenge
        .get("code_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    Ok(OpenChallenge {
        status,
        attempts,
        max_attempts,
        code_hash,
    })
}

#[cfg(feature = "http-api")]
async fn verify_reset_code(
    db: &Database,
    challenge_id: &str,
    submitted_code: &str,
    open: &OpenChallenge<'_>,
) -> Result<(), Response> {
    let new_attempts = open.attempts + 1;
    let submitted_hash = crate::http::challenge_utils::hash_code(submitted_code);

    if submitted_hash != open.code_hash {
        let new_status = if new_attempts >= open.max_attempts {
            "failed"
        } else {
            open.status
        };
        if let Err(e) = crate::db_helpers::update_entity_db(
            db,
            "_verification_challenges",
            challenge_id,
            &serde_json::json!({"attempts": new_attempts, "status": new_status}),
        )
        .await
        {
            warn!("password reset submit: failed to record failed attempt on {challenge_id}: {e}");
        }
        return Err(Response::error(
            mqdb_core::ErrorCode::Unauthorized,
            "invalid code",
        ));
    }

    if let Err(e) = crate::db_helpers::update_entity_db(
        db,
        "_verification_challenges",
        challenge_id,
        &serde_json::json!({"status": "verified", "attempts": new_attempts}),
    )
    .await
    {
        warn!("password reset submit: failed to mark challenge {challenge_id} verified: {e}");
    }

    Ok(())
}

#[cfg(feature = "http-api")]
async fn store_reset_password(
    db: &Database,
    canonical_id: &str,
    new_hash: &str,
    target_hash: &str,
) -> Result<(), Response> {
    let existing_creds =
        match crate::db_helpers::read_entity_db(db, "_credentials", canonical_id).await {
            Ok(opt) => opt,
            Err(e) => {
                warn!("password reset submit: failed to read credentials {canonical_id}: {e}");
                return Err(Response::error(
                    mqdb_core::ErrorCode::Internal,
                    "failed to read credentials",
                ));
            }
        };
    let stored = if existing_creds.is_some() {
        crate::db_helpers::update_entity_db(
            db,
            "_credentials",
            canonical_id,
            &serde_json::json!({"password_hash": new_hash}),
        )
        .await
        .map_err(|e| {
            error!("password reset submit: failed to update credentials for {canonical_id}: {e}");
            Response::error(mqdb_core::ErrorCode::Internal, "failed to update password")
        })
    } else {
        create_or_update_credentials(db, canonical_id, new_hash, target_hash)
            .await
            .map_err(|e| {
                error!(
                    "password reset submit: failed to create credentials for {canonical_id}: {e}"
                );
                Response::error(
                    mqdb_core::ErrorCode::Internal,
                    "failed to create credentials",
                )
            })
    };
    stored.map(drop)
}

#[cfg(feature = "http-api")]
async fn handle_password_reset_submit_mqtt(ctx: &AdminContext<'_>, payload: &Value) -> Response {
    use serde_json::json;

    let Some(canonical_id) = extract_sender(ctx.message) else {
        return Response::error(mqdb_core::ErrorCode::Forbidden, "missing sender identity");
    };

    let submission = match parse_reset_submission(payload) {
        Ok(s) => s,
        Err(response) => return response,
    };

    if !ctx.auth_rate_limiter.check_and_record(canonical_id) {
        return Response::error(
            mqdb_core::ErrorCode::RateLimited,
            "too many attempts, try again later",
        );
    }

    let challenge = match read_reset_challenge(ctx.db, submission.challenge_id).await {
        Ok(c) => c,
        Err(response) => return response,
    };

    let open = match check_open_challenge(ctx.db, &challenge, submission.challenge_id, canonical_id)
        .await
    {
        Ok(o) => o,
        Err(response) => return response,
    };

    if let Err(response) =
        verify_reset_code(ctx.db, submission.challenge_id, submission.code, &open).await
    {
        return response;
    }

    let new_hash = match crate::http::credentials::hash_password(submission.new_password) {
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

    if let Err(response) = store_reset_password(ctx.db, canonical_id, &new_hash, target_hash).await
    {
        return response;
    }

    if let Err(e) = crate::db_helpers::update_entity_db(
        ctx.db,
        "_identities",
        canonical_id,
        &json!({"email_verified": true}),
    )
    .await
    {
        error!("password reset submit: failed to mark email_verified for {canonical_id}: {e}");
        return Response::error(
            mqdb_core::ErrorCode::Internal,
            "failed to mark email verified",
        );
    }

    invalidate_http_sessions(ctx, canonical_id);

    Response::ok(json!({"status": "password_reset"}))
}

#[cfg(feature = "http-api")]
async fn create_or_update_credentials(
    db: &crate::database::Database,
    canonical_id: &str,
    new_hash: &str,
    target_hash: &str,
) -> Result<Value, mqdb_core::error::Error> {
    let created = crate::db_helpers::create_entity_db(
        db,
        "_credentials",
        &serde_json::json!({
            "id": canonical_id,
            "password_hash": new_hash,
            "email_hash": target_hash,
        }),
    )
    .await;
    match created {
        Err(mqdb_core::error::Error::AlreadyExists { .. }) => {
            crate::db_helpers::update_entity_db(
                db,
                "_credentials",
                canonical_id,
                &serde_json::json!({"password_hash": new_hash}),
            )
            .await
        }
        other => other,
    }
}

#[cfg(all(test, feature = "http-api"))]
mod tests {
    use super::{create_or_update_credentials, resolve_grantee_email};

    #[tokio::test]
    async fn a_role_with_a_malformed_rule_is_refused_and_not_created() {
        let auth = mqtt5::broker::auth::ComprehensiveAuthProvider::new();
        let response = super::handle_acl_role_add(
            Some(&auth),
            &json!({"role": "editors", "rules": [{"topic": "a/#", "access": "sometimes"}]}),
        )
        .await;
        assert!(matches!(
            response,
            mqdb_core::transport::Response::Error { code: 400, .. }
        ));
        let not_an_array =
            super::handle_acl_role_add(Some(&auth), &json!({"role": "editors", "rules": "a/#"}))
                .await;
        assert!(matches!(
            not_an_array,
            mqdb_core::transport::Response::Error { code: 400, .. }
        ));
        assert!(auth.acl_manager().get_role("editors").await.is_none());
    }

    #[test]
    fn deleting_an_unknown_user_answers_404() {
        let auth = mqtt5::broker::auth::ComprehensiveAuthProvider::new();
        let response = super::handle_user_delete(Some(&auth), &json!({"username": "ghost"}));
        assert!(matches!(
            response,
            mqdb_core::transport::Response::Error { code: 404, .. }
        ));
    }
    use crate::database::Database;
    use crate::http::IdentityCrypto;
    use serde_json::json;
    use tempfile::TempDir;

    #[tokio::test]
    async fn a_reset_that_lost_the_create_race_updates_the_password() {
        let tmp = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();

        create_or_update_credentials(&db, "cid", "first", "target")
            .await
            .unwrap();
        create_or_update_credentials(&db, "cid", "second", "target")
            .await
            .unwrap();

        let stored = crate::db_helpers::read_entity_db(&db, "_credentials", "cid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored["password_hash"], "second");
        assert_eq!(stored["email_hash"], "target");
    }

    #[tokio::test]
    async fn resolve_grantee_email_maps_known_email_to_canonical_id() {
        let tmp = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        let crypto = IdentityCrypto::generate().unwrap().0;

        let hash = crypto.blind_index("_identity_links", "bob@example.com");
        crate::db_helpers::create_entity_db(
            &db,
            "_identity_links",
            &json!({
                "id": "google:bob",
                "canonical_id": "cid-bob",
                "email_hash": hash,
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            resolve_grantee_email(&db, &crypto, "bob@example.com")
                .await
                .unwrap(),
            Some("cid-bob".to_string())
        );
        assert_eq!(
            resolve_grantee_email(&db, &crypto, "nobody@example.com")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn resolve_share_request_blind_indexes_and_encrypts_email() {
        use super::resolve_share_request;
        use mqdb_core::transport::Request;
        use mqdb_core::types::SHARES_ENTITY;
        use std::sync::Arc;

        let tmp = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        let crypto = IdentityCrypto::generate().unwrap().0;
        let crypto = Arc::new(crypto);

        let req = resolve_share_request(
            &db,
            Some(&crypto),
            Request::Share {
                entity: "diagrams".into(),
                id: "d1".into(),
                grantee: "Alice@X.com".into(),
                grantee_key: None,
                grantee_email: None,
                permission: "view".into(),
                cascade: false,
            },
        )
        .await
        .unwrap();

        let Request::Share {
            grantee,
            grantee_key,
            grantee_email,
            ..
        } = req
        else {
            panic!("expected Share");
        };
        assert_eq!(grantee, "", "an unregistered email is stored pending");
        assert_eq!(
            grantee_key.as_deref(),
            Some(crypto.blind_index(SHARES_ENTITY, "alice@x.com").as_str()),
            "grantee_key is the blind index of the lowercased email, never plaintext"
        );
        let enc = grantee_email.expect("encrypted email retained for display");
        assert_ne!(enc, "alice@x.com", "the email is not stored in plaintext");
        assert_eq!(
            crypto.decrypt_field(SHARES_ENTITY, &enc).unwrap(),
            "alice@x.com",
            "grantee_email decrypts back to the lowercased email"
        );
    }

    struct IdentityLookupFails {
        inner: mqdb_core::storage::MemoryBackend,
        failing: std::sync::atomic::AtomicBool,
    }

    impl IdentityLookupFails {
        fn refuse(&self, prefix: &[u8]) -> mqdb_core::error::Result<()> {
            let touches_identities = prefix
                .windows(b"_identity_links".len())
                .any(|w| w == b"_identity_links");
            if touches_identities && self.failing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(mqdb_core::error::Error::Internal("lookup failed".into()));
            }
            Ok(())
        }
    }

    impl mqdb_core::storage::StorageBackend for IdentityLookupFails {
        fn get(&self, key: &[u8]) -> mqdb_core::error::Result<Option<Vec<u8>>> {
            self.inner.get(key)
        }
        fn insert(&self, key: &[u8], value: &[u8]) -> mqdb_core::error::Result<()> {
            self.inner.insert(key, value)
        }
        fn remove(&self, key: &[u8]) -> mqdb_core::error::Result<()> {
            self.inner.remove(key)
        }
        fn prefix_scan(&self, prefix: &[u8]) -> mqdb_core::error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.refuse(prefix)?;
            self.inner.prefix_scan(prefix)
        }
        fn prefix_count(&self, prefix: &[u8]) -> mqdb_core::error::Result<usize> {
            self.refuse(prefix)?;
            self.inner.prefix_count(prefix)
        }
        fn prefix_scan_keys(&self, prefix: &[u8]) -> mqdb_core::error::Result<Vec<Vec<u8>>> {
            self.refuse(prefix)?;
            self.inner.prefix_scan_keys(prefix)
        }
        fn prefix_scan_batch(
            &self,
            prefix: &[u8],
            batch_size: usize,
            after_key: Option<&[u8]>,
        ) -> mqdb_core::error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.refuse(prefix)?;
            self.inner.prefix_scan_batch(prefix, batch_size, after_key)
        }
        fn range_scan(
            &self,
            start: &[u8],
            end: &[u8],
        ) -> mqdb_core::error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.refuse(start)?;
            self.inner.range_scan(start, end)
        }
        fn batch(&self) -> Box<dyn mqdb_core::storage::BatchOperations> {
            self.inner.batch()
        }
        fn flush(&self) -> mqdb_core::error::Result<()> {
            self.inner.flush()
        }
    }

    #[tokio::test]
    async fn a_share_or_unshare_whose_grantee_lookup_fails_is_refused() {
        use super::resolve_share_request;
        use mqdb_core::transport::{Request, Response};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let backend = Arc::new(IdentityLookupFails {
            inner: mqdb_core::storage::MemoryBackend::new(),
            failing: AtomicBool::new(false),
        });
        let config = mqdb_core::config::DatabaseConfig::new(std::path::PathBuf::from("mem"))
            .without_background_tasks();
        let db = Database::open_with_backend(Arc::clone(&backend) as _, config)
            .await
            .unwrap();
        let crypto = IdentityCrypto::generate().unwrap().0;
        let crypto = Arc::new(crypto);
        backend.failing.store(true, Ordering::SeqCst);

        let unshare = resolve_share_request(
            &db,
            Some(&crypto),
            Request::Unshare {
                entity: "diagrams".into(),
                id: "d1".into(),
                grantee: "bob@example.com".into(),
                grantee_key: None,
                cascade: false,
            },
        )
        .await;
        let share = resolve_share_request(
            &db,
            Some(&crypto),
            Request::Share {
                entity: "diagrams".into(),
                id: "d1".into(),
                grantee: "bob@example.com".into(),
                grantee_key: None,
                grantee_email: None,
                permission: "view".into(),
                cascade: false,
            },
        )
        .await;

        assert!(
            matches!(unshare, Err(Response::Error { code: 500, .. }))
                && matches!(share, Err(Response::Error { code: 500, .. })),
            "unshare {unshare:?}, share {share:?}"
        );
    }

    #[tokio::test]
    async fn shares_listing_decrypts_grantee_email_for_owner() {
        use super::decrypt_shares_grantee_email;
        use mqdb_core::transport::Response;
        use mqdb_core::types::SHARES_ENTITY;
        use std::sync::Arc;

        let crypto = IdentityCrypto::generate().unwrap().0;
        let crypto = Arc::new(crypto);
        let enc = crypto.encrypt_field(SHARES_ENTITY, "alice@x.com").unwrap();
        assert_ne!(enc, "alice@x.com");

        let mut response = Response::Ok {
            data: json!([
                {
                    "grantee_key": "some-blind-index",
                    "grantee": serde_json::Value::Null,
                    "grantee_email": enc,
                    "permission": "view",
                },
                {
                    "grantee_key": "u",
                    "grantee": "u",
                    "grantee_email": serde_json::Value::Null,
                    "permission": "edit",
                }
            ]),
        };
        decrypt_shares_grantee_email(&crypto, &mut response);

        let Response::Ok { data } = response else {
            panic!("expected ok");
        };
        assert_eq!(
            data[0]["grantee_email"], "alice@x.com",
            "owner listing decrypts the encrypted grantee_email back to plaintext"
        );
        assert!(
            data[1]["grantee_email"].is_null(),
            "a null grantee_email (password mode) is left untouched"
        );
    }
}

#[cfg(test)]
mod admin_store_failure {
    use crate::database::Database;
    use mqdb_core::config::DatabaseConfig;
    use mqdb_core::error::{Error, Result};
    use mqdb_core::storage::{BatchOperations, MemoryBackend, StorageBackend};
    use mqdb_core::transport::Response;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Failing {
        inner: MemoryBackend,
        fail: Arc<AtomicBool>,
    }
    impl StorageBackend for Failing {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get(key)
        }
        fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Internal("injected".into()));
            }
            self.inner.insert(key, value)
        }
        fn remove(&self, key: &[u8]) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Internal("injected".into()));
            }
            self.inner.remove(key)
        }
        fn prefix_scan(&self, p: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.prefix_scan(p)
        }
        fn prefix_count(&self, p: &[u8]) -> Result<usize> {
            self.inner.prefix_count(p)
        }
        fn prefix_scan_keys(&self, p: &[u8]) -> Result<Vec<Vec<u8>>> {
            self.inner.prefix_scan_keys(p)
        }
        fn prefix_scan_batch(
            &self,
            p: &[u8],
            n: usize,
            a: Option<&[u8]>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.prefix_scan_batch(p, n, a)
        }
        fn range_scan(&self, s: &[u8], e: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.range_scan(s, e)
        }
        fn batch(&self) -> Box<dyn BatchOperations> {
            Box::new(FailingBatch {
                inner: self.inner.batch(),
                fail: Arc::clone(&self.fail),
            })
        }
        fn flush(&self) -> Result<()> {
            self.inner.flush()
        }
    }
    struct FailingBatch {
        inner: Box<dyn BatchOperations>,
        fail: Arc<AtomicBool>,
    }
    impl BatchOperations for FailingBatch {
        fn insert(&mut self, k: Vec<u8>, v: Vec<u8>) {
            self.inner.insert(k, v);
        }
        fn remove(&mut self, k: Vec<u8>) {
            self.inner.remove(k);
        }
        fn expect_value(&mut self, k: Vec<u8>, v: Vec<u8>) {
            self.inner.expect_value(k, v);
        }
        fn expect_absent(&mut self, k: Vec<u8>) {
            self.inner.expect_absent(k);
        }
        fn commit(self: Box<Self>) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Internal("injected commit failure".into()));
            }
            self.inner.commit()
        }
    }

    fn code(r: &Response) -> u16 {
        match r {
            Response::Ok { .. } => 200,
            Response::Error { code, .. } => *code,
        }
    }

    #[tokio::test]
    async fn admin_writes_the_agent_cannot_store_answer_500() {
        let fail = Arc::new(AtomicBool::new(false));
        let backend = Arc::new(Failing {
            inner: MemoryBackend::new(),
            fail: Arc::clone(&fail),
        });
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        let db = Database::open_with_backend(backend, config).await.unwrap();
        fail.store(true, Ordering::SeqCst);
        let schema =
            super::handle_schema_set(&db, "items".into(), json!({"code": {"type": "string"}}))
                .await;
        let constraint = super::handle_constraint_add(
            &db,
            "items".into(),
            &json!({"type": "unique", "fields": ["code"]}),
        )
        .await;
        assert_eq!(
            (code(&schema), code(&constraint)),
            (500, 500),
            "agent codes differ from cluster's 500/500"
        );
    }

    #[tokio::test]
    async fn constraint_adds_the_agent_refuses_as_invalid_answer_400() {
        let fail = Arc::new(AtomicBool::new(false));
        let backend = Arc::new(Failing {
            inner: MemoryBackend::new(),
            fail: Arc::clone(&fail),
        });
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        let db = Database::open_with_backend(backend, config).await.unwrap();
        let missing_fields =
            super::handle_constraint_add(&db, "items".into(), &json!({"type": "unique"})).await;
        let schema =
            super::handle_schema_set(&db, "items".into(), json!({"code": {"type": "string"}}))
                .await;
        assert_eq!(code(&schema), 200);
        let unknown_after_schema = super::handle_constraint_add(
            &db,
            "items".into(),
            &json!({"type": "not_null", "field": "absent"}),
        )
        .await;
        assert_eq!(
            (code(&missing_fields), code(&unknown_after_schema)),
            (400, 400)
        );
    }

    async fn memory_db() -> Database {
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        Database::open_with_backend(Arc::new(MemoryBackend::new()), config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_constraint_added_twice_answers_409_and_is_stored_once() {
        let db = memory_db().await;
        let unique = json!({"type": "unique", "fields": ["code"]});
        let first = super::handle_constraint_add(&db, "items".into(), &unique).await;
        let again = super::handle_constraint_add(&db, "items".into(), &unique).await;
        assert_eq!(
            (
                code(&first),
                code(&again),
                db.list_constraints("items").await.len()
            ),
            (200, 409, 1)
        );
    }

    #[tokio::test]
    async fn a_foreign_key_with_an_unknown_on_delete_answers_400() {
        let db = memory_db().await;
        let foreign_key = json!({
            "type": "foreign_key",
            "field": "owner_id",
            "target_entity": "owners",
            "target_field": "id",
            "on_delete": "bogus"
        });
        let refused = super::handle_constraint_add(&db, "items".into(), &foreign_key).await;
        assert_eq!(
            (code(&refused), db.list_constraints("items").await.len()),
            (400, 0)
        );
    }
}
