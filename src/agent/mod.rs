// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod broker;
mod handlers;
mod tasks;

use crate::Database;
use crate::VaultKeyStore;
use crate::auth_config::AuthSetupConfig;
use mqtt5::broker::config::{FederatedJwtConfig, JwtConfig, RateLimitConfig};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub use crate::protocol::{
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
    pub(super) http_config: std::sync::Mutex<Option<crate::http::HttpServerConfig>>,
    pub(super) ownership_config: Arc<crate::types::OwnershipConfig>,
    pub(super) scope_config: Arc<crate::types::ScopeConfig>,
    pub(super) vault_key_store: Arc<VaultKeyStore>,
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
            ownership_config: Arc::new(crate::types::OwnershipConfig::default()),
            scope_config: Arc::new(crate::types::ScopeConfig::default()),
            vault_key_store: Arc::new(VaultKeyStore::new()),
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
    pub fn with_ownership_config(mut self, config: crate::types::OwnershipConfig) -> Self {
        self.ownership_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn with_scope_config(mut self, config: crate::types::ScopeConfig) -> Self {
        self.scope_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn ownership_config_arc(&self) -> Arc<crate::types::OwnershipConfig> {
        Arc::clone(&self.ownership_config)
    }

    #[must_use]
    pub fn vault_key_store_arc(&self) -> Arc<VaultKeyStore> {
        Arc::clone(&self.vault_key_store)
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

        let broker = mqtt5::broker::MqttBroker::with_config(config).await?;
        let (mut broker, auth_providers) = Self::apply_auth_providers(
            broker,
            broker::AuthProviderConfig {
                needs_composite,
                service_username: service_username.as_ref(),
                service_password: service_password.as_ref(),
                password_file: self.auth_setup.password_file.as_deref(),
                acl_file: self.auth_setup.acl_file.as_deref(),
                admin_users: &admin_users,
                allow_anonymous: self.auth_setup.allow_anonymous,
            },
        )
        .await?;

        info!("MQDB Agent listening on {}", self.bind_address);

        let bind_addr = self.bind_address;
        let handler_task = self.spawn_handler_task(
            bind_addr,
            service_username.clone(),
            service_password.clone(),
            auth_providers,
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

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
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
