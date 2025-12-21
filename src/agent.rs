use crate::{Database, Request, Response};
use mqtt5::broker::{BrokerConfig, MqttBroker};
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

#[derive(Debug, Clone)]
pub struct DbOperation {
    pub entity: String,
    pub operation: String,
    pub id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AdminOperation {
    SchemaSet { entity: String },
    SchemaGet { entity: String },
    ConstraintAdd { entity: String },
    ConstraintList { entity: String },
    Backup,
    Restore,
    BackupList,
    Subscribe,
    Heartbeat { sub_id: String },
    Unsubscribe { sub_id: String },
    ConsumerGroupList,
    ConsumerGroupShow { name: String },
}

type ListOptions = (
    Vec<crate::Filter>,
    Vec<crate::SortOrder>,
    Option<crate::Pagination>,
    Vec<String>,
    Option<Vec<String>>,
);

#[allow(clippy::must_use_candidate)]
pub fn parse_admin_topic(topic: &str) -> Option<AdminOperation> {
    if let Some(rest) = topic.strip_prefix("$DB/_sub/") {
        let parts: Vec<&str> = rest.split('/').collect();
        return match parts.as_slice() {
            ["subscribe"] => Some(AdminOperation::Subscribe),
            [id, "heartbeat"] => Some(AdminOperation::Heartbeat {
                sub_id: (*id).to_string(),
            }),
            [id, "unsubscribe"] => Some(AdminOperation::Unsubscribe {
                sub_id: (*id).to_string(),
            }),
            _ => None,
        };
    }

    let parts: Vec<&str> = topic.strip_prefix("$DB/_admin/")?.split('/').collect();

    match parts.as_slice() {
        ["schema", entity, "set"] => Some(AdminOperation::SchemaSet {
            entity: (*entity).to_string(),
        }),
        ["schema", entity, "get"] => Some(AdminOperation::SchemaGet {
            entity: (*entity).to_string(),
        }),
        ["constraint", entity, "add"] => Some(AdminOperation::ConstraintAdd {
            entity: (*entity).to_string(),
        }),
        ["constraint", entity, "list"] => Some(AdminOperation::ConstraintList {
            entity: (*entity).to_string(),
        }),
        ["backup"] => Some(AdminOperation::Backup),
        ["backup", "list"] => Some(AdminOperation::BackupList),
        ["restore"] => Some(AdminOperation::Restore),
        ["consumer-groups"] => Some(AdminOperation::ConsumerGroupList),
        ["consumer-groups", name] => Some(AdminOperation::ConsumerGroupShow {
            name: (*name).to_string(),
        }),
        _ => None,
    }
}

#[allow(clippy::must_use_candidate)]
pub fn parse_db_topic(topic: &str) -> Option<DbOperation> {
    let parts: Vec<&str> = topic.strip_prefix("$DB/")?.split('/').collect();

    match parts.as_slice() {
        [entity, op] if *op == "create" || *op == "list" => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: (*op).to_string(),
            id: None,
        }),
        [entity, id] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: "read".to_string(),
            id: Some((*id).to_string()),
        }),
        [entity, id, op] if *op == "update" || *op == "delete" => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: (*op).to_string(),
            id: Some((*id).to_string()),
        }),
        _ => None,
    }
}

pub fn build_request(op: DbOperation, payload: &[u8]) -> Result<Request, String> {
    let data: Value = if payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(payload).map_err(|e| e.to_string())?
    };

    match op.operation.as_str() {
        "create" => Ok(Request::Create {
            entity: op.entity,
            data,
        }),
        "read" => {
            let id = op.id.ok_or("read requires id in topic")?;
            let (includes, projection) = extract_read_options(&data);
            Ok(Request::Read {
                entity: op.entity,
                id,
                includes,
                projection,
            })
        }
        "update" => {
            let id = op.id.ok_or("update requires id in topic")?;
            Ok(Request::Update {
                entity: op.entity,
                id,
                fields: data,
            })
        }
        "delete" => {
            let id = op.id.ok_or("delete requires id in topic")?;
            Ok(Request::Delete {
                entity: op.entity,
                id,
            })
        }
        "list" => {
            let (filters, sort, pagination, includes, projection) = extract_list_options(&data);
            Ok(Request::List {
                entity: op.entity,
                filters,
                sort,
                pagination,
                includes,
                projection,
            })
        }
        _ => Err(format!("unknown operation: {}", op.operation)),
    }
}

fn extract_read_options(data: &Value) -> (Vec<String>, Option<Vec<String>>) {
    let includes = data
        .get("includes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let projection = data
        .get("projection")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    (includes, projection)
}

fn extract_list_options(data: &Value) -> ListOptions {
    let filters: Vec<crate::Filter> = data
        .get("filters")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let sort: Vec<crate::SortOrder> = data
        .get("sort")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let pagination: Option<crate::Pagination> = data
        .get("pagination")
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let includes = data
        .get("includes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let projection = data
        .get("projection")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    (filters, sort, pagination, includes, projection)
}

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
}

impl MqdbAgent {
    #[allow(clippy::must_use_candidate)]
    pub fn new(db: Database) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let backup_dir = db.path().join("backups");
        Self {
            db: Arc::new(db),
            bind_address: "127.0.0.1:1884".parse().unwrap(),
            shutdown_tx,
            password_file: None,
            acl_file: None,
            allow_anonymous: true,
            service_username: None,
            service_password: None,
            backup_dir,
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

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut config = BrokerConfig {
            bind_addresses: vec![self.bind_address],
            max_clients: 1000,
            max_packet_size: 10 * 1024 * 1024,
            session_expiry_interval: Duration::from_secs(3600),
            maximum_qos: 2,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifier_available: true,
            shared_subscription_available: true,
            topic_alias_maximum: 100,
            ..Default::default()
        };

        config.auth_config.allow_anonymous = self.allow_anonymous;
        if let Some(ref path) = self.password_file {
            config.auth_config.password_file = Some(path.clone());
        }
        if let Some(ref path) = self.acl_file {
            config.auth_config.acl_file = Some(path.clone());
        }

        let mut broker = MqttBroker::with_config(config).await?;

        info!("MQDB Agent listening on {}", self.bind_address);

        let db = Arc::clone(&self.db);
        let bind_addr = self.bind_address;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let service_username = self.service_username.clone();
        let service_password = self.service_password.clone();
        let backup_dir = self.backup_dir.clone();

        let handler_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let client = MqttClient::new("mqdb-internal-handler");
            let addr = format!("{}:{}", bind_addr.ip(), bind_addr.port());

            let response_creds = (service_username.clone(), service_password.clone());
            let connect_result =
                if let (Some(user), Some(pass)) = (service_username, service_password) {
                    let options = mqtt5::types::ConnectOptions::new("mqdb-internal-handler")
                        .with_credentials(user, pass);
                    client
                        .connect_with_options(&addr, options)
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
                response_client
                    .connect_with_options(&addr, options)
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
                        match msg {
                            Some(message) => {
                                handle_message(&db, &response_client, message, &backup_dir).await;
                            }
                            None => {
                                debug!("Message channel closed");
                                break;
                            }
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
        let event_service_username = self.service_username.clone();
        let event_service_password = self.service_password.clone();
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
                client
                    .connect_with_options(&addr, options)
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

    let op = match parse_db_topic(topic) {
        Some(op) => op,
        None => {
            warn!("Invalid $DB topic format: {}", topic);
            return;
        }
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
        use opentelemetry::trace::{SpanContext, TraceContextExt};
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let user_props: Vec<(String, String)> = message.properties.user_properties.to_vec();

        if let Some(parent_cx) = propagation::extract_trace_context(&user_props) {
            let parent = SpanContext::new(
                parent_cx.trace_id(),
                parent_cx.span_id(),
                parent_cx.trace_flags(),
                false,
                Default::default(),
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
            } else if let Err(e) = std::fs::create_dir_all(backup_dir) {
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
            if !backup_dir.exists() {
                Response::ok(json!(Vec::<String>::new()))
            } else {
                match std::fs::read_dir(backup_dir) {
                    Ok(entries) => {
                        let backups: Vec<_> = entries
                            .filter_map(|e| e.ok())
                            .filter(|e| e.path().is_dir())
                            .filter_map(|e| e.file_name().into_string().ok())
                            .collect();
                        Response::ok(json!(backups))
                    }
                    Err(e) => Response::error(
                        crate::ErrorCode::Internal,
                        format!("failed to read backup directory: {e}"),
                    ),
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_db_topic_create() {
        let op = parse_db_topic("$DB/users/create").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "create");
        assert!(op.id.is_none());
    }

    #[test]
    fn test_parse_db_topic_read() {
        let op = parse_db_topic("$DB/users/123").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "read");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_update() {
        let op = parse_db_topic("$DB/users/123/update").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "update");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_delete() {
        let op = parse_db_topic("$DB/users/123/delete").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "delete");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_list() {
        let op = parse_db_topic("$DB/users/list").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "list");
        assert!(op.id.is_none());
    }

    #[test]
    fn test_parse_db_topic_invalid() {
        assert!(parse_db_topic("invalid/topic").is_none());
        assert!(parse_db_topic("$DB").is_none());
        assert!(parse_db_topic("$DB/").is_none());
    }

    #[test]
    fn test_build_create_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: "create".to_string(),
            id: None,
        };
        let payload = br#"{"name": "Alice"}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::Create { entity, data } => {
                assert_eq!(entity, "users");
                assert_eq!(data["name"], "Alice");
            }
            _ => panic!("expected Create request"),
        }
    }

    #[test]
    fn test_build_read_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: "read".to_string(),
            id: Some("123".to_string()),
        };
        let payload = br#"{"projection": ["name", "email"]}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::Read {
                entity,
                id,
                projection,
                ..
            } => {
                assert_eq!(entity, "users");
                assert_eq!(id, "123");
                assert_eq!(
                    projection,
                    Some(vec!["name".to_string(), "email".to_string()])
                );
            }
            _ => panic!("expected Read request"),
        }
    }

    #[test]
    fn test_build_list_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: "list".to_string(),
            id: None,
        };
        let payload = br#"{"filters": [{"field": "age", "op": "gt", "value": 18}]}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::List {
                entity, filters, ..
            } => {
                assert_eq!(entity, "users");
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].field, "age");
            }
            _ => panic!("expected List request"),
        }
    }

    #[test]
    fn test_read_without_id_fails() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: "read".to_string(),
            id: None,
        };
        let result = build_request(op, &[]);
        assert!(result.is_err());
    }
}
