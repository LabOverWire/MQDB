// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqtt5::broker::BrokerConfig;
use mqtt5::broker::auth::{
    AuthProvider, AuthRateLimiter, AuthResult, AuthorizationMode, CompositeAuthProvider,
    EnhancedAuthResult, RateLimitedAuthProvider,
};
use mqtt5::broker::config::{
    AuthConfig, AuthMethod, FederatedJwtConfig, JwtConfig, RateLimitConfig,
};
use mqtt5::packet::connect::ConnectPacket;
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, instrument};

pub struct AuthSetupConfig {
    pub password_file: Option<PathBuf>,
    pub acl_file: Option<PathBuf>,
    pub allow_anonymous: bool,
    pub scram_file: Option<PathBuf>,
    pub jwt_config: Option<JwtConfig>,
    pub federated_jwt_config: Option<FederatedJwtConfig>,
    pub cert_auth_file: Option<PathBuf>,
    pub rate_limit: Option<RateLimitConfig>,
    pub no_rate_limit: bool,
    pub admin_users: HashSet<String>,
}

impl Default for AuthSetupConfig {
    fn default() -> Self {
        Self {
            password_file: None,
            acl_file: None,
            allow_anonymous: true,
            scram_file: None,
            jwt_config: None,
            federated_jwt_config: None,
            cert_auth_file: None,
            rate_limit: None,
            no_rate_limit: false,
            admin_users: HashSet::new(),
        }
    }
}

pub struct AuthSetupResult {
    pub service_username: Option<String>,
    pub service_password: Option<String>,
    pub needs_composite: bool,
    pub admin_users: HashSet<String>,
}

/// # Errors
///
/// Returns an error if the auth configuration cannot be built.
pub async fn configure_broker_auth(
    config: &AuthSetupConfig,
    auth_config: &mut AuthConfig,
) -> Result<AuthSetupResult, Box<dyn std::error::Error + Send + Sync>> {
    let auth_method = if config.scram_file.is_some() {
        AuthMethod::ScramSha256
    } else if config.jwt_config.is_some() {
        AuthMethod::Jwt
    } else if config.federated_jwt_config.is_some() {
        AuthMethod::JwtFederated
    } else if config.password_file.is_some() {
        AuthMethod::Password
    } else {
        AuthMethod::None
    };

    let uses_enhanced_auth = matches!(
        auth_method,
        AuthMethod::ScramSha256 | AuthMethod::Jwt | AuthMethod::JwtFederated
    );

    let mut service_username = None;
    let mut service_password = None;

    if let Some(ref path) = config.password_file {
        let svc_user = format!("mqdb-internal-{}", uuid::Uuid::new_v4());
        let svc_pass = uuid::Uuid::new_v4().to_string();

        auth_config.password_file = Some(path.clone());
        auth_config.allow_anonymous = config.allow_anonymous;
        service_username = Some(svc_user);
        service_password = Some(svc_pass);
        info!("password authentication configured with service account");
    } else if uses_enhanced_auth {
        let svc_user = format!("mqdb-internal-{}", uuid::Uuid::new_v4());
        let svc_pass = uuid::Uuid::new_v4().to_string();
        service_username = Some(svc_user);
        service_password = Some(svc_pass);
        auth_config.allow_anonymous = false;
    } else {
        auth_config.allow_anonymous = config.allow_anonymous;
        if config.allow_anonymous {
            let svc_user = format!("mqdb-internal-{}", uuid::Uuid::new_v4());
            let svc_pass = uuid::Uuid::new_v4().to_string();
            service_username = Some(svc_user);
            service_password = Some(svc_pass);
        }
    }

    if let Some(ref path) = config.acl_file {
        auth_config.acl_file = Some(path.clone());
    }

    if let Some(ref path) = config.scram_file {
        auth_config.scram_file = Some(path.clone());
        info!("SCRAM-SHA-256 authentication configured");
    }

    if let Some(ref jwt) = config.jwt_config {
        auth_config.jwt_config = Some(jwt.clone());
        info!("JWT authentication configured");
    }

    if let Some(ref fed) = config.federated_jwt_config {
        auth_config.federated_jwt_config = Some(fed.clone());
        info!("federated JWT authentication configured");
    }

    auth_config.auth_method = auth_method;

    let has_auth = auth_method != AuthMethod::None;
    if config.no_rate_limit {
        auth_config.rate_limit.enabled = false;
    } else if let Some(ref rl) = config.rate_limit {
        auth_config.rate_limit = rl.clone();
    } else if has_auth {
        auth_config.rate_limit = RateLimitConfig::default();
    } else {
        auth_config.rate_limit.enabled = false;
    }

    Ok(AuthSetupResult {
        service_username,
        service_password,
        needs_composite: uses_enhanced_auth,
        admin_users: config.admin_users.clone(),
    })
}

pub struct AuthRateLimit {
    limiter: Arc<AuthRateLimiter>,
    cleanup_interval: Duration,
}

#[must_use]
#[instrument(skip_all)]
pub fn take_auth_rate_limit(mut config: BrokerConfig) -> (BrokerConfig, Option<AuthRateLimit>) {
    let limits = config.auth_config.rate_limit.clone();
    if !limits.enabled {
        return (config, None);
    }
    config.auth_config.rate_limit.enabled = false;
    info!(
        max_attempts = limits.max_attempts,
        window_secs = limits.window_secs,
        lockout_secs = limits.lockout_secs,
        "authentication rate limiting enabled"
    );
    let rate_limit = AuthRateLimit {
        limiter: Arc::new(AuthRateLimiter::new(
            limits.max_attempts,
            limits.window_secs,
            limits.lockout_secs,
        )),
        cleanup_interval: Duration::from_secs(limits.lockout_secs.max(1)),
    };
    (config, Some(rate_limit))
}

#[must_use]
#[instrument(skip_all)]
pub fn rate_limited(
    provider: Arc<dyn AuthProvider>,
    rate_limit: Option<AuthRateLimit>,
    service_username: Option<&String>,
) -> Arc<dyn AuthProvider> {
    match rate_limit {
        Some(rate_limit) => {
            spawn_rate_limit_cleanup(&rate_limit);
            Arc::new(ServiceExemptRateLimit {
                limited: RateLimitedAuthProvider::new(Arc::clone(&provider), rate_limit.limiter),
                inner: provider,
                service_username: service_username.cloned(),
            })
        }
        None => provider,
    }
}

fn spawn_rate_limit_cleanup(rate_limit: &AuthRateLimit) {
    let limiter = Arc::downgrade(&rate_limit.limiter);
    let mut ticks = tokio::time::interval(rate_limit.cleanup_interval);
    tokio::spawn(async move {
        ticks.tick().await;
        loop {
            ticks.tick().await;
            let Some(limiter) = limiter.upgrade() else {
                break;
            };
            limiter.cleanup_expired();
        }
    });
}

struct ServiceExemptRateLimit {
    inner: Arc<dyn AuthProvider>,
    limited: RateLimitedAuthProvider,
    service_username: Option<String>,
}

impl AuthProvider for ServiceExemptRateLimit {
    fn authenticate<'a>(
        &'a self,
        connect: &'a ConnectPacket,
        client_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = mqtt5::error::Result<AuthResult>> + Send + 'a>> {
        match (&self.service_username, &connect.username) {
            (Some(service), Some(username)) if service == username => {
                self.inner.authenticate(connect, client_addr)
            }
            _ => self.limited.authenticate(connect, client_addr),
        }
    }

    fn authorize_publish<'a>(
        &'a self,
        client_id: &str,
        user_id: Option<&'a str>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.inner.authorize_publish(client_id, user_id, topic)
    }

    fn authorize_subscribe<'a>(
        &'a self,
        client_id: &str,
        user_id: Option<&'a str>,
        topic_filter: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.inner
            .authorize_subscribe(client_id, user_id, topic_filter)
    }

    fn supports_enhanced_auth(&self) -> bool {
        self.inner.supports_enhanced_auth()
    }

    fn authenticate_enhanced<'a>(
        &'a self,
        auth_method: &'a str,
        auth_data: Option<&'a [u8]>,
        client_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = mqtt5::error::Result<EnhancedAuthResult>> + Send + 'a>> {
        self.inner
            .authenticate_enhanced(auth_method, auth_data, client_id)
    }

    fn reauthenticate<'a>(
        &'a self,
        auth_method: &'a str,
        auth_data: Option<&'a [u8]>,
        client_id: &'a str,
        user_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = mqtt5::error::Result<EnhancedAuthResult>> + Send + 'a>> {
        self.inner
            .reauthenticate(auth_method, auth_data, client_id, user_id)
    }

    fn cleanup_session<'a>(
        &'a self,
        user_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.inner.cleanup_session(user_id)
    }
}

#[must_use]
pub fn enhanced_auth_with_fallback(
    auth_method: AuthMethod,
    enhanced: Arc<dyn AuthProvider>,
    fallback: Arc<dyn AuthProvider>,
) -> CompositeAuthProvider {
    let mode = match auth_method {
        AuthMethod::JwtFederated => AuthorizationMode::PrimaryOnly,
        _ => AuthorizationMode::And,
    };
    CompositeAuthProvider::new(enhanced, fallback).with_authorization_mode(mode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt5::broker::acl::FederatedRoleEntry;
    use mqtt5::broker::auth::{AllowAllAuthProvider, ComprehensiveAuthProvider};
    use mqtt5::broker::auth_mechanisms::FederatedJwtAuthProvider;
    use mqtt5::broker::config::FederatedAuthMode;
    use mqtt5::broker::{AclManager, PasswordAuthProvider};
    use mqtt5::protocol::v5::reason_codes::ReasonCode;

    fn connect_as(username: &str, password: &str) -> ConnectPacket {
        let mut connect =
            ConnectPacket::new(mqtt5::types::ConnectOptions::new("rate-limit").protocol_options);
        connect.username = Some(username.to_string());
        connect.password = Some(password.as_bytes().to_vec());
        connect
    }

    #[tokio::test]
    async fn failed_logins_from_loopback_do_not_lock_out_the_service_user() {
        let service_username = "mqdb-internal-test".to_string();
        let service_password = uuid::Uuid::new_v4().to_string();
        let passwords = PasswordAuthProvider::new();
        passwords
            .add_user(service_username.clone(), &service_password)
            .expect("add service user");
        let mut config = BrokerConfig::default();
        config.auth_config.rate_limit = RateLimitConfig {
            enabled: true,
            max_attempts: 2,
            window_secs: 60,
            lockout_secs: 300,
        };
        let (config, rate_limit) = take_auth_rate_limit(config);
        assert!(!config.auth_config.rate_limit.enabled);
        let provider = rate_limited(Arc::new(passwords), rate_limit, Some(&service_username));
        let loopback = SocketAddr::from(([127, 0, 0, 1], 40000));
        let wrong = uuid::Uuid::new_v4().to_string();

        for _ in 0..2 {
            let result = provider
                .authenticate(&connect_as("paysvc", &wrong), loopback)
                .await
                .expect("authenticate");
            assert_eq!(result.reason_code, ReasonCode::BadUsernameOrPassword);
        }
        let result = provider
            .authenticate(&connect_as("paysvc", &wrong), loopback)
            .await
            .expect("authenticate");
        assert_eq!(result.reason_code, ReasonCode::ConnectionRateExceeded);

        let result = provider
            .authenticate(&connect_as(&service_username, &service_password), loopback)
            .await
            .expect("authenticate");
        assert!(
            result.authenticated,
            "the service user must log in from a locked-out address: {:?}",
            result.reason_code
        );
    }

    #[tokio::test]
    async fn federated_clients_keep_the_roles_from_their_token() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let acl_path = dir.path().join("acl");
        std::fs::write(
            &acl_path,
            "role writer topic sensors/# permission readwrite\n",
        )
        .expect("write acl file");
        let federated_acl = AclManager::from_file(&acl_path)
            .await
            .expect("load federated acl");
        federated_acl
            .set_federated_roles(
                "idp:alice",
                FederatedRoleEntry::new(
                    HashSet::from(["writer".to_string()]),
                    "idp".to_string(),
                    FederatedAuthMode::TrustedRoles,
                    true,
                ),
            )
            .await;
        let federated = FederatedJwtAuthProvider::new(Vec::new())
            .expect("federated provider")
            .with_acl_manager(Arc::new(tokio::sync::RwLock::new(federated_acl)));
        let fallback = Arc::new(ComprehensiveAuthProvider::with_providers(
            PasswordAuthProvider::new(),
            AclManager::from_file(&acl_path)
                .await
                .expect("load fallback acl"),
        ));
        let provider =
            enhanced_auth_with_fallback(AuthMethod::JwtFederated, Arc::new(federated), fallback);

        assert!(
            provider
                .authorize_publish("c1", Some("idp:alice"), "sensors/t1")
                .await
        );
        assert!(
            !provider
                .authorize_publish("c2", Some("idp:bob"), "sensors/t1")
                .await
        );
    }

    #[tokio::test]
    async fn the_acl_fallback_still_authorizes_enhanced_auth_clients() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let acl_path = dir.path().join("acl");
        std::fs::write(
            &acl_path,
            "user * topic # permission readwrite\n\
             user * topic $DB/# permission readwrite\n\
             user * topic $DB/sale/create permission deny\n\
             user * topic secret/# permission deny\n\
             user paysvc topic $DB/sale/# permission readwrite\n",
        )
        .expect("write acl file");
        let acl = AclManager::from_file(&acl_path).await.expect("load acl");
        let fallback = Arc::new(ComprehensiveAuthProvider::with_providers(
            PasswordAuthProvider::new(),
            acl,
        ));
        let provider =
            enhanced_auth_with_fallback(AuthMethod::Jwt, Arc::new(AllowAllAuthProvider), fallback);

        assert!(
            !provider
                .authorize_publish("c1", Some("mallory"), "$DB/sale/create")
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("c1", Some("mallory"), "secret/plans")
                .await
        );
        assert!(
            provider
                .authorize_publish("c1", Some("mallory"), "$DB/sale/list")
                .await
        );
        assert!(
            provider
                .authorize_publish("c2", Some("paysvc"), "$DB/sale/create")
                .await
        );
    }

    #[tokio::test]
    async fn password_mode_leaves_the_password_file_unchanged() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("passwd");
        let hash =
            mqtt5::broker::PasswordAuthProvider::hash_password("admin").expect("hash password");
        let original = format!("admin:{hash}\n");
        std::fs::write(&path, &original).expect("write password file");

        let config = AuthSetupConfig {
            password_file: Some(path.clone()),
            allow_anonymous: false,
            ..AuthSetupConfig::default()
        };
        let mut auth_config = AuthConfig::default();
        let result = configure_broker_auth(&config, &mut auth_config)
            .await
            .expect("configure broker auth");

        assert!(result.service_username.is_some());
        assert!(result.service_password.is_some());
        assert_eq!(
            std::fs::read_to_string(&path).expect("read password file"),
            original
        );
    }
}
