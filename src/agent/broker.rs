// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::MqdbAgent;
use crate::broker_defaults::{BROKER_MAX_CLIENTS, BROKER_MAX_PACKET_SIZE, SESSION_EXPIRY_SECS};
use crate::topic_protection::TopicProtectionAuthProvider;
use mqtt5::broker::auth::{CompositeAuthProvider, ComprehensiveAuthProvider};
use mqtt5::broker::config::{ChangeOnlyDeliveryConfig, QuicConfig, WebSocketConfig};
use mqtt5::broker::{AclManager, BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

impl MqdbAgent {
    pub(super) async fn build_broker_config(
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
                topic_patterns: vec![
                    "$DB/+/events/#".to_string(),
                    "$DB/+/+/events/#".to_string(),
                    "$DB/+/+/+/events/#".to_string(),
                ],
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

    pub(super) fn apply_transport_config(&self, config: &mut BrokerConfig) {
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

    #[allow(clippy::type_complexity)]
    pub(super) async fn apply_auth_providers(
        mut broker: MqttBroker,
        needs_composite: bool,
        service_username: Option<&String>,
        service_password: Option<&String>,
        password_file: Option<&std::path::Path>,
        acl_file: Option<&std::path::Path>,
        admin_users: &HashSet<String>,
    ) -> Result<
        (MqttBroker, Option<Arc<ComprehensiveAuthProvider>>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let auth_providers = if password_file.is_some() || acl_file.is_some() {
            let pwd = if let Some(path) = password_file {
                PasswordAuthProvider::from_file(path).await?
            } else {
                PasswordAuthProvider::new()
            };
            let acl = if let Some(path) = acl_file {
                AclManager::from_file(path).await?
            } else {
                AclManager::allow_all()
            };
            Some(Arc::new(ComprehensiveAuthProvider::with_providers(
                pwd, acl,
            )))
        } else {
            None
        };

        if needs_composite
            && let (Some(svc_user), Some(svc_pass)) = (service_username, service_password)
        {
            let primary = broker.auth_provider();
            let fallback: Arc<dyn mqtt5::broker::auth::AuthProvider> =
                if let Some(ref comprehensive) = auth_providers {
                    comprehensive
                        .password_provider()
                        .add_user(svc_user.clone(), svc_pass)?;
                    comprehensive.clone()
                } else {
                    let fallback = PasswordAuthProvider::new();
                    fallback.add_user(svc_user.clone(), svc_pass)?;
                    Arc::new(fallback)
                };
            let composite = CompositeAuthProvider::new(primary, fallback);
            broker = broker.with_auth_provider(Arc::new(composite));
            info!("composite auth provider configured (password file + service account)");
        } else if let Some(ref comprehensive) = auth_providers {
            broker = broker.with_auth_provider(comprehensive.clone());
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

        Ok((broker, auth_providers))
    }
}
