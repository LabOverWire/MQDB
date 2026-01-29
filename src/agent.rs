use crate::auth_config::AuthSetupConfig;
use crate::topic_protection::TopicProtectionAuthProvider;
use crate::{Database, Response};
use mqtt5::broker::auth::CompositeAuthProvider;
use mqtt5::broker::config::{
    ChangeOnlyDeliveryConfig, FederatedJwtConfig, JwtConfig, QuicConfig, RateLimitConfig,
    WebSocketConfig,
};
use mqtt5::broker::{BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::client::MqttClient;
use mqtt5::time::Duration;
use mqtt5::types::Message;
use serde_json::Value;
use std::collections::HashSet;
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

pub use crate::protocol::{
    AdminOperation, DbOperation, build_request, parse_admin_topic, parse_db_topic,
};

pub struct MqdbAgent {
    db: Arc<Database>,
    bind_address: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
    auth_setup: AuthSetupConfig,
    service_username: Option<String>,
    service_password: Option<String>,
    backup_dir: PathBuf,
    quic_cert_file: Option<PathBuf>,
    quic_key_file: Option<PathBuf>,
    ws_bind_address: Option<SocketAddr>,
    http_config: std::sync::Mutex<Option<crate::http::HttpServerConfig>>,
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
            auth_setup: AuthSetupConfig::default(),
            service_username: None,
            service_password: None,
            backup_dir,
            quic_cert_file: None,
            quic_key_file: None,
            ws_bind_address: None,
            http_config: std::sync::Mutex::new(None),
        }
    }

    #[must_use]
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }

    #[must_use]
    pub fn with_password_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.password_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_acl_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.acl_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_anonymous(mut self, allow: bool) -> Self {
        self.auth_setup.allow_anonymous = allow;
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

    #[must_use]
    pub fn with_ws_bind_address(mut self, addr: SocketAddr) -> Self {
        self.ws_bind_address = Some(addr);
        self
    }

    #[must_use]
    pub fn with_auth_setup(mut self, config: AuthSetupConfig) -> Self {
        self.auth_setup = config;
        self
    }

    #[must_use]
    pub fn with_scram_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.scram_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_jwt_config(mut self, config: JwtConfig) -> Self {
        self.auth_setup.jwt_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_federated_jwt_config(mut self, config: FederatedJwtConfig) -> Self {
        self.auth_setup.federated_jwt_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_cert_auth_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.cert_auth_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_rate_limit_config(mut self, config: RateLimitConfig) -> Self {
        self.auth_setup.rate_limit = Some(config);
        self
    }

    #[must_use]
    pub fn with_no_rate_limit(mut self) -> Self {
        self.auth_setup.no_rate_limit = true;
        self
    }

    #[must_use]
    pub fn with_admin_users(mut self, users: HashSet<String>) -> Self {
        self.auth_setup.admin_users = users;
        self
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn with_http_config(self, config: crate::http::HttpServerConfig) -> Self {
        *self.http_config.lock().expect("http_config lock") = Some(config);
        self
    }

    /// # Errors
    /// Returns an error if the broker fails to start or encounters a runtime error.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut config, service_username, service_password, needs_composite, admin_users) =
            self.build_broker_config().await?;

        self.apply_transport_config(&mut config);

        let broker = MqttBroker::with_config(config).await?;
        let mut broker = Self::apply_auth_providers(
            broker,
            needs_composite,
            service_username.as_ref(),
            service_password.as_ref(),
            &admin_users,
        )?;

        info!("MQDB Agent listening on {}", self.bind_address);

        let bind_addr = self.bind_address;
        let handler_task = self.spawn_handler_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
        );
        let event_task = self.spawn_event_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
        );
        let http_task = self.spawn_http_task(
            bind_addr,
            service_username.as_ref(),
            service_password.as_ref(),
        );

        broker.run().await?;

        let _ = self.shutdown_tx.send(());
        let _ = handler_task.await;
        let _ = event_task.await;
        if let Some(http) = http_task {
            let _ = http.await;
        }

        Ok(())
    }

    async fn build_broker_config(
        &self,
    ) -> Result<
        (
            BrokerConfig,
            Option<String>,
            Option<String>,
            bool,
            HashSet<String>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
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
            change_only_delivery_config: ChangeOnlyDeliveryConfig {
                enabled: true,
                topic_patterns: vec!["$DB/+/events/#".to_string()],
            },
            ..Default::default()
        };

        let (service_username, service_password, needs_composite, admin_users) =
            if self.service_username.is_some() {
                (
                    self.service_username.clone(),
                    self.service_password.clone(),
                    false,
                    self.auth_setup.admin_users.clone(),
                )
            } else {
                let result = crate::auth_config::configure_broker_auth(
                    &self.auth_setup,
                    &mut config.auth_config,
                )
                .await?;
                (
                    result.service_username,
                    result.service_password,
                    result.needs_composite,
                    result.admin_users,
                )
            };

        Ok((
            config,
            service_username,
            service_password,
            needs_composite,
            admin_users,
        ))
    }

    fn apply_transport_config(&self, config: &mut BrokerConfig) {
        if let (Some(cert_file), Some(key_file)) = (&self.quic_cert_file, &self.quic_key_file) {
            let quic_config = QuicConfig::new(cert_file.clone(), key_file.clone())
                .with_bind_address(self.bind_address);
            *config = std::mem::take(config).with_quic(quic_config);
            info!(quic_bind = %self.bind_address, "QUIC listener configured");
        }

        if let Some(ws_addr) = self.ws_bind_address {
            let ws_config = WebSocketConfig::new().with_bind_addresses(vec![ws_addr]);
            *config = std::mem::take(config).with_websocket(ws_config);
            info!(ws_bind = %ws_addr, "WebSocket listener configured");
        }
    }

    fn apply_auth_providers(
        mut broker: MqttBroker,
        needs_composite: bool,
        service_username: Option<&String>,
        service_password: Option<&String>,
        admin_users: &HashSet<String>,
    ) -> Result<MqttBroker, Box<dyn std::error::Error + Send + Sync>> {
        if needs_composite
            && let (Some(svc_user), Some(svc_pass)) = (service_username, service_password)
        {
            let primary = broker.auth_provider();
            let fallback = PasswordAuthProvider::new();
            fallback.add_user(svc_user.clone(), svc_pass)?;
            let composite = CompositeAuthProvider::new(primary, Arc::new(fallback));
            broker = broker.with_auth_provider(Arc::new(composite));
            info!("composite auth provider configured for internal service clients");
        }

        let current_provider = broker.auth_provider();
        let protected_provider =
            TopicProtectionAuthProvider::new(current_provider, admin_users.clone())
                .with_internal_service_username(service_username.cloned());
        broker = broker.with_auth_provider(Arc::new(protected_provider));
        if admin_users.is_empty() {
            info!("topic protection enabled (no admin users configured)");
        } else {
            info!(
                admin_users = ?admin_users,
                "topic protection enabled with admin users"
            );
        }

        Ok(broker)
    }

    fn spawn_handler_task(
        &self,
        bind_addr: SocketAddr,
        handler_username: Option<String>,
        handler_password: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let backup_dir = self.backup_dir.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let client = MqttClient::new("mqdb-internal-handler");
            let addr = resolve_connect_address(bind_addr);

            let response_creds = (handler_username.clone(), handler_password.clone());
            let connect_result = if let Some(user) = handler_username {
                let options = mqtt5::types::ConnectOptions::new("mqdb-internal-handler")
                    .with_credentials(user, handler_password.as_deref().unwrap_or(""));
                Box::pin(client.connect_with_options(&addr, options))
                    .await
                    .map(|_| ())
            } else {
                client.connect(&addr).await
            };

            if let Err(e) = connect_result {
                error!("Failed to connect internal handler: {e}");
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
                error!("Failed to subscribe to $DB/#: {e}");
                return;
            }

            info!("Internal handler subscribed to $DB/#");

            let response_client = MqttClient::new("mqdb-response-publisher");
            if let Err(e) = connect_mqtt_client(
                &response_client,
                "mqdb-response-publisher",
                &addr,
                response_creds.0,
                response_creds.1,
            )
            .await
            {
                error!("Failed to connect response publisher: {e}");
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
        })
    }

    fn spawn_event_task(
        &self,
        event_addr: SocketAddr,
        event_service_username: Option<String>,
        event_service_password: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        let event_db = Arc::clone(&self.db);
        let mut event_shutdown_rx = self.shutdown_tx.subscribe();
        let num_partitions = self.db.num_partitions();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let client = MqttClient::new("mqdb-event-publisher");
            let addr = format!("{}:{}", event_addr.ip(), event_addr.port());

            if let Err(e) = connect_mqtt_client(
                &client,
                "mqdb-event-publisher",
                &addr,
                event_service_username,
                event_service_password,
            )
            .await
            {
                error!("Failed to connect event publisher: {e}");
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
                                        error!("Failed to serialize event: {e}");
                                        continue;
                                    }
                                };

                                if let Err(e) = client.publish_qos1(&topic, payload).await {
                                    warn!("Failed to publish event: {e}");
                                }
                            }
                            Err(e) => {
                                error!("Event channel error: {e}");
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
        })
    }

    fn spawn_http_task(
        &self,
        bind_addr: SocketAddr,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let http_config = self
            .http_config
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())?;

        let http_bind = http_config.bind_address;
        let http_shutdown_rx = self.shutdown_tx.subscribe();
        let http_addr = resolve_connect_address(bind_addr);
        let http_creds = (service_username.cloned(), service_password.cloned());

        Some(tokio::spawn(async move {
            tokio::time::sleep(mqtt5::time::Duration::from_millis(300)).await;

            let http_mqtt_client = MqttClient::new("mqdb-http-oauth");
            if let Err(e) = connect_mqtt_client(
                &http_mqtt_client,
                "mqdb-http-oauth",
                &http_addr,
                http_creds.0,
                http_creds.1,
            )
            .await
            {
                error!("Failed to connect HTTP OAuth MQTT client: {e}");
                return;
            }

            info!(addr = %http_bind, "starting HTTP OAuth server");
            let server = crate::http::HttpServer::new(
                http_config,
                Arc::new(http_mqtt_client),
                http_shutdown_rx,
            );
            if let Err(e) = server.run().await {
                error!("HTTP server error: {e}");
            }
        }))
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

fn resolve_connect_address(bind_addr: SocketAddr) -> String {
    let connect_ip = if bind_addr.ip().is_unspecified() {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        bind_addr.ip()
    };
    format!("{connect_ip}:{}", bind_addr.port())
}

async fn connect_mqtt_client(
    client: &MqttClient,
    client_id: &str,
    addr: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<(), mqtt5::MqttError> {
    if let (Some(user), Some(pass)) = (username, password) {
        let options = mqtt5::types::ConnectOptions::new(client_id).with_credentials(user, pass);
        Box::pin(client.connect_with_options(addr, options))
            .await
            .map(|_| ())
    } else {
        client.connect(addr).await
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

async fn handle_admin_operation(
    db: &Database,
    client: &MqttClient,
    message: &Message,
    op: AdminOperation,
    backup_dir: &Path,
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
