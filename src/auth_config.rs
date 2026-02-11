// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqtt5::broker::PasswordAuthProvider;
use mqtt5::broker::config::{
    AuthConfig, AuthMethod, FederatedJwtConfig, JwtConfig, RateLimitConfig,
};
use std::collections::HashSet;
use std::path::PathBuf;
use tracing::info;

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
/// Returns an error if password hashing or file I/O fails.
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
        let hash = PasswordAuthProvider::hash_password(&svc_pass)?;

        let prefix = format!("{svc_user}:");
        let mut contents = tokio::fs::read_to_string(path).await.unwrap_or_default();
        let has_user = contents.lines().any(|line| line.starts_with(&prefix));
        if !has_user {
            use std::fmt::Write;
            let _ = writeln!(contents, "{svc_user}:{hash}");
            tokio::fs::write(path, &contents).await?;
        }

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
