use crate::{Database, Response};
use mqtt5::broker::config::QuicConfig;
use mqtt5::broker::{BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::client::MqttClient;
use mqtt5::time::Duration;
use mqtt5::types::Message;
use serde_json::Value;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{Instrument, debug, error, info, info_span, warn};

#[cfg(feature = "opentelemetry")]
use mqtt5::telemetry::propagation;

const BROKER_MAX_CLIENTS: usize = 10_000;
const BROKER_MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;
const SESSION_EXPIRY_SECS: u64 = 3600;

pub use crate::protocol::{build_request, parse_admin_topic, parse_db_topic, AdminOperation, DbOperation};

pub struct MqdbAgent {
    db: Arc<Database>,
    bind_address: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
    password_file: Option<PathBuf>,
    acl_file: Option<PathBuf>,
    allow_anonymous: bool,
    service_username: Option<String>,
    service_password: Option<String>,
    backup_dir: PathBuf,
    quic_cert_file: Option<PathBuf>,
    quic_key_file: Option<PathBuf>,
}

impl MqdbAgent {
    /// # Panics
    /// Panics if the default bind address cannot be parsed (should never happen).
    #[allow(clippy::must_use_candidate)]
    pub fn new(db: Database) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let backup_dir = db.path().join("backups");
        Self {
            db: Arc::new(db),
            bind_address: "127.0.0.1:1883".parse().unwrap(),
            shutdown_tx,
            password_file: None,
            acl_file: None,
            allow_anonymous: true,
            service_username: None,
            service_password: None,
            backup_dir,
            quic_cert_file: None,
            quic_key_file: None,
        }
    }

    #[must_use]
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }

    #[must_use]
    pub fn with_password_file(mut self, path: PathBuf) -> Self {
        self.password_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_acl_file(mut self, path: PathBuf) -> Self {
        self.acl_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_anonymous(mut self, allow: bool) -> Self {
        self.allow_anonymous = allow;
        self
    }

    #[must_use]
    pub fn with_service_credentials(mut self, username: String, password: String) -> Self {
        self.service_username = Some(username);
        self.service_password = Some(password);
        self
    }

    #[must_use]
    pub fn with_backup_dir(mut self, path: PathBuf) -> Self {
        self.backup_dir = path;
        self
    }

    #[must_use]
    pub fn with_quic_certs(mut self, cert_file: PathBuf, key_file: PathBuf) -> Self {
        self.quic_cert_file = Some(cert_file);
        self.quic_key_file = Some(key_file);
        self
    }

    /// # Errors
    /// Returns an error if the broker fails to start or encounters a runtime error.
    #[allow(clippy::too_many_lines)]
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut config = BrokerConfig {
            bind_addresses: vec![self.bind_address],
            max_clients: BROKER_MAX_CLIENTS,
            max_packet_size: BROKER_MAX_PACKET_SIZE,
            session_expiry_interval: Duration::from_secs(SESSION_EXPIRY_SECS),
            maximum_qos: 2,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifier_available: true,
            shared_subscription_available: true,
            topic_alias_maximum: 100,
            ..Default::default()
        };

        config.auth_config.allow_anonymous = self.allow_anonymous;
        if let Some(ref path) = self.acl_file {
            config.auth_config.acl_file = Some(path.clone());
        }

        let (service_username, service_password, custom_auth_provider) =
            if let Some(ref path) = self.password_file {
                if self.service_username.is_none() {
                    let svc_user = format!("mqdb-internal-{}", uuid::Uuid::new_v4());
                    let svc_pass = uuid::Uuid::new_v4().to_string();
                    let auth_provider = PasswordAuthProvider::from_file(path).await?;
                    auth_provider.add_user(svc_user.clone(), &svc_pass)?;
                    (
                        Some(svc_user),
                        Some(svc_pass),
                        Some(Arc::new(auth_provider)),
                    )
                } else {
                    config.auth_config.password_file = Some(path.clone());
                    (
                        self.service_username.clone(),
                        self.service_password.clone(),
                        None,
                    )
                }
            } else {
                (
                    self.service_username.clone(),
                    self.service_password.clone(),
                    None,
                )
            };

        if let (Some(cert_file), Some(key_file)) = (&self.quic_cert_file, &self.quic_key_file) {
            let quic_config = QuicConfig::new(cert_file.clone(), key_file.clone())
                .with_bind_address(self.bind_address);
            config = config.with_quic(quic_config);
            info!(quic_bind = %self.bind_address, "QUIC listener configured");
        }

        let mut broker = if let Some(provider) = custom_auth_provider {
            MqttBroker::with_config(config)
                .await?
                .with_auth_provider(provider)
        } else {
            MqttBroker::with_config(config).await?
        };

        info!("MQDB Agent listening on {}", self.bind_address);

        let db = Arc::clone(&self.db);
        let bind_addr = self.bind_address;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let backup_dir = self.backup_dir.clone();
        let handler_username = service_username.clone();
        let handler_password = service_password.clone();

        let handler_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let client = MqttClient::new("mqdb-internal-handler");
            let connect_ip = if bind_addr.ip().is_unspecified() {
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            } else {
                bind_addr.ip()
            };
            let addr = format!("{}:{}", connect_ip, bind_addr.port());

            let response_creds = (handler_username.clone(), handler_password.clone());
            let connect_result =
                if let (Some(user), Some(pass)) = (handler_username, handler_password) {
                    let options = mqtt5::types::ConnectOptions::new("mqdb-internal-handler")
                        .with_credentials(user, pass);
                    Box::pin(client.connect_with_options(&addr, options))
                        .await
                        .map(|_| ())
                } else {
                    client.connect(&addr).await
                };

            if let Err(e) = connect_result {
                error!("Failed to connect internal handler: {}", e);
                return;
            }

            let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(256);

            let callback_tx = msg_tx.clone();
            if let Err(e) = client
                .subscribe("$DB/#", move |message| {
                    let _ = callback_tx.try_send(message);
                })
                .await
            {
                error!("Failed to subscribe to $DB/#: {}", e);
                return;
            }

            info!("Internal handler subscribed to $DB/#");

            let response_client = MqttClient::new("mqdb-response-publisher");
            let response_connect = if let (Some(user), Some(pass)) = response_creds {
                let options = mqtt5::types::ConnectOptions::new("mqdb-response-publisher")
                    .with_credentials(user, pass);
                Box::pin(response_client.connect_with_options(&addr, options))
                    .await
                    .map(|_| ())
            } else {
                response_client.connect(&addr).await
            };

            if let Err(e) = response_connect {
                error!("Failed to connect response publisher: {}", e);
                return;
            }

            loop {
                tokio::select! {
                    msg = msg_rx.recv() => {
                        if let Some(message) = msg {
                            handle_message(&db, &response_client, message, &backup_dir).await;
                        } else {
                            debug!("Message channel closed");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Internal handler shutting down");
                        break;
                    }
                }
            }
        });

        let event_db = Arc::clone(&self.db);
        let event_addr = self.bind_address;
        let mut event_shutdown_rx = self.shutdown_tx.subscribe();
        let event_service_username = service_username.clone();
        let event_service_password = service_password.clone();
        let num_partitions = self.db.num_partitions();

        let event_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let client = MqttClient::new("mqdb-event-publisher");
            let addr = format!("{}:{}", event_addr.ip(), event_addr.port());

            let connect_result = if let (Some(user), Some(pass)) =
                (event_service_username, event_service_password)
            {
                let options = mqtt5::types::ConnectOptions::new("mqdb-event-publisher")
                    .with_credentials(user, pass);
                Box::pin(client.connect_with_options(&addr, options))
                    .await
                    .map(|_| ())
            } else {
                client.connect(&addr).await
            };

            if let Err(e) = connect_result {
                error!("Failed to connect event publisher: {}", e);
                return;
            }

            let mut event_rx = event_db.event_receiver();

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(change_event) => {
                                let topic = if num_partitions > 0 {
                                    let partition = change_event.partition(num_partitions);
                                    format!("$DB/{}/events/p{}/{}", change_event.entity, partition, change_event.id)
                                } else {
                                    format!("$DB/{}/events/{}", change_event.entity, change_event.id)
                                };
                                let payload = match serde_json::to_vec(&change_event) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("Failed to serialize event: {}", e);
                                        continue;
                                    }
                                };

                                if let Err(e) = client.publish_qos1(&topic, payload).await {
                                    warn!("Failed to publish event: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Event channel error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = event_shutdown_rx.recv() => {
                        debug!("Event publisher shutting down");
                        break;
                    }
                }
            }
        });

        broker.run().await?;

        let _ = self.shutdown_tx.send(());
        let _ = handler_task.await;
        let _ = event_task.await;

        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

async fn handle_message(db: &Database, client: &MqttClient, message: Message, backup_dir: &Path) {
    let topic = &message.topic;

    if topic.contains("/events") {
        return;
    }

    if let Some(admin_op) = parse_admin_topic(topic) {
        handle_admin_operation(db, client, &message, admin_op, backup_dir).await;
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
                let response = Response::error(crate::ErrorCode::BadRequest, e);
                if let Ok(payload) = serde_json::to_vec(&response) {
                    let _ = client.publish_qos1(response_topic, payload).await;
                }
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

    let response = db.execute(request).instrument(span).await;

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

#[allow(clippy::too_many_lines)]
async fn handle_admin_operation(
    db: &Database,
    client: &MqttClient,
    message: &Message,
    op: AdminOperation,
    backup_dir: &Path,
) {
    use crate::constraint::OnDeleteAction;
    use serde_json::json;

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
        AdminOperation::SchemaSet { entity } => {
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
        AdminOperation::SchemaGet { entity } => match db.get_schema(&entity).await {
            Some(schema) => Response::ok(serde_json::to_value(schema).unwrap_or(Value::Null)),
            None => Response::error(
                crate::ErrorCode::NotFound,
                format!("no schema for entity: {entity}"),
            ),
        },
        AdminOperation::ConstraintAdd { entity } => {
            let constraint_type = payload.get("type").and_then(|v| v.as_str());

            let result =
                match constraint_type {
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
                    Some("foreign_key") => {
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
                            _ => Err("foreign_key requires field, target_entity, target_field"
                                .to_string()),
                        }
                    }
                    _ => Err(format!("unknown constraint type: {constraint_type:?}")),
                };

            match result {
                Ok(()) => Response::ok(json!({"message": "constraint added"})),
                Err(e) => Response::error(crate::ErrorCode::BadRequest, e),
            }
        }
        AdminOperation::ConstraintList { entity } => {
            let constraints = db.list_constraints(&entity).await;
            let data: Vec<Value> = constraints
                .into_iter()
                .map(|c| serde_json::to_value(c).unwrap_or(Value::Null))
                .collect();
            Response::ok(json!(data))
        }
        AdminOperation::Backup => {
            let name = payload
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("backup");

            if !is_valid_backup_name(name) {
                Response::error(
                    crate::ErrorCode::BadRequest,
                    "invalid backup name: must be alphanumeric, underscore, or hyphen only",
                )
            } else if let Err(e) = tokio::fs::create_dir_all(backup_dir).await {
                Response::error(
                    crate::ErrorCode::Internal,
                    format!("failed to create backup directory: {e}"),
                )
            } else {
                let backup_path = backup_dir.join(name);
                match db.backup(&backup_path) {
                    Ok(()) => Response::ok(json!({"message": format!("backup created: {name}")})),
                    Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
                }
            }
        }
        AdminOperation::Restore => Response::error(
            crate::ErrorCode::Internal,
            "restore requires agent restart - use CLI with --restore flag",
        ),
        AdminOperation::BackupList => {
            if backup_dir.exists() {
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
            } else {
                Response::ok(json!(Vec::<String>::new()))
            }
        }
        AdminOperation::Subscribe => {
            use crate::subscription::SubscriptionMode;

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
        AdminOperation::Heartbeat { sub_id } => match db.heartbeat(&sub_id).await {
            Ok(()) => Response::ok(json!({"ok": true})),
            Err(e) => Response::error(crate::ErrorCode::NotFound, e.to_string()),
        },
        AdminOperation::Unsubscribe { sub_id } => match db.unsubscribe(&sub_id).await {
            Ok(()) => Response::ok(json!({"ok": true})),
            Err(e) => Response::error(crate::ErrorCode::Internal, e.to_string()),
        },
        AdminOperation::ConsumerGroupList => {
            let groups = db.list_consumer_groups().await;
            Response::ok(serde_json::to_value(groups).unwrap_or(Value::Null))
        }
        AdminOperation::ConsumerGroupShow { name } => match db.get_consumer_group(&name).await {
            Some(details) => Response::ok(serde_json::to_value(details).unwrap_or(Value::Null)),
            None => Response::error(
                crate::ErrorCode::NotFound,
                format!("consumer group not found: {name}"),
            ),
        },
        AdminOperation::Health => Response::ok(json!({
            "status": "healthy",
            "ready": true,
            "mode": "agent",
            "details": {
                "partitions": db.num_partitions()
            }
        })),
    };

    if let Some(response_topic) = &message.properties.response_topic {
        match serde_json::to_vec(&response) {
            Ok(payload) => {
                if let Err(e) = client.publish_qos1(response_topic, payload).await {
                    error!(
                        "Failed to publish admin response to {}: {}",
                        response_topic, e
                    );
                }
            }
            Err(e) => {
                error!("Failed to serialize admin response: {}", e);
            }
        }
    }
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

