// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod broker;
mod handlers;
mod tasks;
mod websocket;

use crate::auth_config::AuthSetupConfig;
use crate::database::Database;
use crate::vault_backend::{NoopVaultBackend, VaultBackend};
use mqtt5::broker::config::{FederatedJwtConfig, JwtConfig, RateLimitConfig};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::info;

#[cfg(feature = "http-api")]
use crate::rate_limiter::RateLimiter;

pub use mqdb_core::protocol::{
    AdminOperation, DbOperation, build_request, parse_admin_topic, parse_db_topic,
};

pub struct MqdbAgent {
    pub(super) db: Arc<Database>,
    pub(super) bind_address: SocketAddr,
    pub(super) shutdown_tx: broadcast::Sender<()>,
    pub(super) auth_setup: AuthSetupConfig,
    pub(super) service_username: Option<String>,
    pub(super) service_password: Option<String>,
    pub(super) backup_dir: PathBuf,
    pub(super) quic_cert_file: Option<PathBuf>,
    pub(super) quic_key_file: Option<PathBuf>,
    pub(super) ws_bind_address: Option<SocketAddr>,
    #[cfg(feature = "http-api")]
    pub(super) http_config: std::sync::Mutex<Option<crate::http::HttpServerConfig>>,
    pub(super) ownership_config: Arc<mqdb_core::types::OwnershipConfig>,
    pub(super) scope_config: Arc<mqdb_core::types::ScopeConfig>,
    pub(super) scoped_events: bool,
    pub(super) presence: bool,
    pub(super) sparkplug_aware: bool,
    pub(super) max_retained_messages: usize,
    pub(super) max_retained_message_size: usize,
    pub(super) vault_backend: Arc<dyn VaultBackend>,
    #[cfg(feature = "http-api")]
    pub(super) auth_rate_limiter: Arc<RateLimiter>,
    #[cfg(feature = "http-api")]
    pub(super) identity_crypto: Option<Arc<crate::http::IdentityCrypto>>,
    #[cfg(feature = "http-api")]
    pub(super) session_store: Option<Arc<crate::http::SessionStore>>,
    #[cfg(feature = "http-api")]
    pub(super) jti_revocation: Option<Arc<crate::http::JtiRevocationStore>>,
    pub(super) license_expires_at: Option<u64>,
    #[cfg(feature = "opentelemetry")]
    pub(super) telemetry_config: Option<mqtt5::telemetry::TelemetryConfig>,
}

impl MqdbAgent {
    #[must_use]
    pub fn new(db: Database) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let backup_dir = db.path().join("backups");
        Self {
            db: Arc::new(db),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 1883)),
            shutdown_tx,
            auth_setup: AuthSetupConfig::default(),
            service_username: None,
            service_password: None,
            backup_dir,
            quic_cert_file: None,
            quic_key_file: None,
            ws_bind_address: None,
            #[cfg(feature = "http-api")]
            http_config: std::sync::Mutex::new(None),
            ownership_config: Arc::new(mqdb_core::types::OwnershipConfig::default()),
            scope_config: Arc::new(mqdb_core::types::ScopeConfig::default()),
            scoped_events: false,
            presence: false,
            sparkplug_aware: false,
            max_retained_messages: 0,
            max_retained_message_size: 0,
            vault_backend: Arc::new(NoopVaultBackend),
            #[cfg(feature = "http-api")]
            auth_rate_limiter: Arc::new(RateLimiter::new(10)),
            #[cfg(feature = "http-api")]
            identity_crypto: None,
            #[cfg(feature = "http-api")]
            session_store: None,
            #[cfg(feature = "http-api")]
            jti_revocation: None,
            license_expires_at: None,
            #[cfg(feature = "opentelemetry")]
            telemetry_config: None,
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
    pub fn with_ownership_config(mut self, config: mqdb_core::types::OwnershipConfig) -> Self {
        self.ownership_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn with_scope_config(mut self, config: mqdb_core::types::ScopeConfig) -> Self {
        self.scope_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn with_scoped_events(mut self, enabled: bool) -> Self {
        self.scoped_events = enabled;
        self
    }

    #[must_use]
    pub fn with_presence(mut self, enabled: bool) -> Self {
        self.presence = enabled;
        self
    }

    #[must_use]
    pub fn with_sparkplug_aware(mut self, enabled: bool) -> Self {
        self.sparkplug_aware = enabled;
        self
    }

    #[must_use]
    pub fn with_max_retained_messages(mut self, max: usize) -> Self {
        self.max_retained_messages = max;
        self
    }

    #[must_use]
    pub fn with_max_retained_message_size(mut self, max: usize) -> Self {
        self.max_retained_message_size = max;
        self
    }

    #[must_use]
    pub fn with_license_expiry(mut self, expires_at: u64) -> Self {
        self.license_expires_at = Some(expires_at);
        self
    }

    #[must_use]
    pub fn with_vault_backend(mut self, backend: Arc<dyn VaultBackend>) -> Self {
        self.vault_backend = backend;
        self
    }

    #[cfg(feature = "opentelemetry")]
    #[must_use]
    pub fn with_telemetry_config(mut self, config: mqtt5::telemetry::TelemetryConfig) -> Self {
        self.telemetry_config = Some(config);
        self
    }

    #[must_use]
    pub fn ownership_config_arc(&self) -> Arc<mqdb_core::types::OwnershipConfig> {
        Arc::clone(&self.ownership_config)
    }

    #[must_use]
    pub fn vault_backend_arc(&self) -> Arc<dyn VaultBackend> {
        Arc::clone(&self.vault_backend)
    }

    #[cfg(feature = "http-api")]
    #[must_use]
    pub fn with_http_config(mut self, config: crate::http::HttpServerConfig) -> Self {
        self.identity_crypto.clone_from(&config.identity_crypto);
        self.session_store = Some(Arc::clone(&config.session_store));
        self.jti_revocation = Some(Arc::clone(&config.jti_revocation));
        self.http_config = std::sync::Mutex::new(Some(config));
        self
    }

    /// # Errors
    /// Returns an error if the broker fails to start or encounters a runtime error.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let (mut config, service_username, service_password, needs_composite, admin_users) =
            self.build_broker_config().await?;

        self.apply_transport_config(&mut config);
        let (presence_rx, certificates) = self.apply_event_handlers(&mut config);

        let auth_method = config.auth_config.auth_method;
        let (config, rate_limit) = crate::auth_config::take_auth_rate_limit(config);
        let ws_listener = self.bind_websocket(&config).await?;
        let broker = mqtt5::broker::MqttBroker::with_config(config).await?;
        let (mut broker, auth_providers) = Self::apply_auth_providers(
            broker,
            broker::AuthProviderConfig {
                needs_composite,
                auth_method,
                service_username: service_username.as_ref(),
                service_password: service_password.as_ref(),
                password_file: self.auth_setup.password_file.as_deref(),
                acl_file: self.auth_setup.acl_file.as_deref(),
                admin_users: &admin_users,
                allow_anonymous: self.auth_setup.allow_anonymous,
                scoped_events: self.scoped_events,
                event_scope_entity: self.scope_config.scope_entity(),
                sparkplug_aware: self.sparkplug_aware,
                rate_limit,
            },
        )
        .await?;
        if let Some(certificates) = &certificates {
            certificates.attach(broker.router());
        }

        info!("MQDB Agent listening on {}", self.bind_address);

        let bind_addr = self.bind_address;
        let (handler_ready_tx, publisher_ready_tx, agent_ready_rx) = match ws_listener
            .as_ref()
            .map(|_| ReadinessSignals::spawn(broker.ready_receiver()))
        {
            Some(signals) => (
                Some(signals.handler),
                Some(signals.publisher),
                Some(signals.ready),
            ),
            None => (None, None, None),
        };
        let handler_task = self.spawn_handler_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
            tasks::HandlerAuth {
                providers: auth_providers,
                authorizer: broker.auth_provider(),
            },
            broker.ready_receiver(),
            handler_ready_tx,
        );
        let event_task = self.spawn_event_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
            broker.ready_receiver(),
            publisher_ready_tx,
        );
        let presence_task = presence_rx.map(|rx| self.spawn_presence_task(broker.router(), rx));
        let http_task: Option<tokio::task::JoinHandle<()>> = {
            #[cfg(feature = "http-api")]
            {
                self.spawn_http_task(
                    bind_addr,
                    service_username.as_ref(),
                    service_password.as_ref(),
                    broker.ready_receiver(),
                )
            }
            #[cfg(not(feature = "http-api"))]
            {
                None
            }
        };
        let license_task = self.spawn_license_check_task();
        let ws_task = self.spawn_websocket_task(ws_listener, &broker, agent_ready_rx);

        tokio::select! {
            result = broker.run() => result?,
            _ = shutdown_rx.recv() => info!("MQDB Agent shutting down"),
        }

        let _ = self.shutdown_tx.send(());
        let _ = handler_task.await;
        let _ = event_task.await;
        if let Some(presence) = presence_task {
            let _ = presence.await;
        }
        if let Some(http) = http_task {
            let _ = http.await;
        }
        if let Some(lic) = license_task {
            let _ = lic.await;
        }
        if let Some(ws) = ws_task {
            let _ = ws.await;
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if the broker fails to start.
    pub async fn start(
        self,
    ) -> Result<
        (
            tokio::task::JoinHandle<()>,
            watch::Receiver<bool>,
            broadcast::Sender<()>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let shutdown_tx = self.shutdown_tx.clone();

        let (mut config, service_username, service_password, needs_composite, admin_users) =
            self.build_broker_config().await?;

        self.apply_transport_config(&mut config);
        let (presence_rx, certificates) = self.apply_event_handlers(&mut config);

        let auth_method = config.auth_config.auth_method;
        let (config, rate_limit) = crate::auth_config::take_auth_rate_limit(config);
        let ws_listener = self.bind_websocket(&config).await?;
        let broker = mqtt5::broker::MqttBroker::with_config(config).await?;
        let (mut broker, auth_providers) = Self::apply_auth_providers(
            broker,
            broker::AuthProviderConfig {
                needs_composite,
                auth_method,
                service_username: service_username.as_ref(),
                service_password: service_password.as_ref(),
                password_file: self.auth_setup.password_file.as_deref(),
                acl_file: self.auth_setup.acl_file.as_deref(),
                admin_users: &admin_users,
                allow_anonymous: self.auth_setup.allow_anonymous,
                scoped_events: self.scoped_events,
                event_scope_entity: self.scope_config.scope_entity(),
                sparkplug_aware: self.sparkplug_aware,
                rate_limit,
            },
        )
        .await?;
        if let Some(certificates) = &certificates {
            certificates.attach(broker.router());
        }

        info!("MQDB Agent listening on {}", self.bind_address);

        let bind_addr = self.bind_address;
        let readiness = ReadinessSignals::spawn(broker.ready_receiver());
        let ready_rx = readiness.ready.clone();
        let handler_task = self.spawn_handler_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
            tasks::HandlerAuth {
                providers: auth_providers,
                authorizer: broker.auth_provider(),
            },
            broker.ready_receiver(),
            Some(readiness.handler),
        );
        let event_task = self.spawn_event_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
            broker.ready_receiver(),
            Some(readiness.publisher),
        );
        let presence_task = presence_rx.map(|rx| self.spawn_presence_task(broker.router(), rx));
        let http_task: Option<tokio::task::JoinHandle<()>> = {
            #[cfg(feature = "http-api")]
            {
                self.spawn_http_task(
                    bind_addr,
                    service_username.as_ref(),
                    service_password.as_ref(),
                    broker.ready_receiver(),
                )
            }
            #[cfg(not(feature = "http-api"))]
            {
                None
            }
        };
        let license_task = self.spawn_license_check_task();
        let ws_task = self.spawn_websocket_task(ws_listener, &broker, Some(readiness.ready));

        let mut shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            tokio::select! {
                result = broker.run() => {
                    if let Err(e) = result {
                        tracing::error!("broker error: {e}");
                    }
                }
                _ = shutdown_rx.recv() => info!("MQDB Agent shutting down"),
            }
            let _ = shutdown_tx.send(());
            let _ = handler_task.await;
            let _ = event_task.await;
            if let Some(presence) = presence_task {
                let _ = presence.await;
            }
            if let Some(http) = http_task {
                let _ = http.await;
            }
            if let Some(lic) = license_task {
                let _ = lic.await;
            }
            if let Some(ws) = ws_task {
                let _ = ws.await;
            }
        });

        let caller_shutdown_tx = self.shutdown_tx.clone();
        Ok((handle, ready_rx, caller_shutdown_tx))
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

struct ReadinessSignals {
    handler: oneshot::Sender<()>,
    publisher: oneshot::Sender<()>,
    ready: watch::Receiver<bool>,
}

impl ReadinessSignals {
    fn spawn(mut broker_ready_rx: watch::Receiver<bool>) -> Self {
        let (handler, handler_ready_rx) = oneshot::channel();
        let (publisher, publisher_ready_rx) = oneshot::channel();
        let (ready_tx, ready) = watch::channel(false);
        tokio::spawn(async move {
            if broker_ready_rx.wait_for(|ready| *ready).await.is_err()
                || handler_ready_rx.await.is_err()
                || publisher_ready_rx.await.is_err()
            {
                return;
            }
            let _ = ready_tx.send(true);
        });
        Self {
            handler,
            publisher,
            ready,
        }
    }
}

pub(super) fn resolve_connect_address(bind_addr: SocketAddr) -> String {
    let connect_ip = if bind_addr.ip().is_unspecified() {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        bind_addr.ip()
    };
    format!("{connect_ip}:{}", bind_addr.port())
}

pub(super) async fn connect_mqtt_client(
    client: &mqtt5::client::MqttClient,
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
