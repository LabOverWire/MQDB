// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mqdb_agent::{Database, MqdbAgent};

use crate::cli_types::{AuthArgs, ConnectionArgs, DurabilityArg, JwtAlgorithmArg, OAuthArgs};
use crate::common::connect_client;

pub(crate) struct AgentStartArgs {
    pub(crate) bind: SocketAddr,
    pub(crate) db_path: PathBuf,
    pub(crate) auth: AuthArgs,
    pub(crate) durability: DurabilityArg,
    pub(crate) durability_ms: u64,
    pub(crate) quic_cert: Option<PathBuf>,
    pub(crate) quic_key: Option<PathBuf>,
    pub(crate) ws_bind: Option<SocketAddr>,
    pub(crate) oauth: OAuthArgs,
    pub(crate) ownership: Option<String>,
    pub(crate) event_scope: Option<String>,
    pub(crate) passphrase_file: Option<PathBuf>,
    pub(crate) license: Option<PathBuf>,
}

pub(crate) async fn cmd_agent_start(
    args: AgentStartArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqdb_core::config::{DatabaseConfig, DurabilityMode};

    let durability_mode = match args.durability {
        DurabilityArg::Immediate => DurabilityMode::Immediate,
        DurabilityArg::Periodic => DurabilityMode::PeriodicMs(args.durability_ms),
        DurabilityArg::None => DurabilityMode::None,
    };

    let mut config = DatabaseConfig::new(&args.db_path).with_durability(durability_mode);
    if let Some(ref pf) = args.passphrase_file {
        let passphrase = std::fs::read_to_string(pf)
            .map_err(|e| format!("failed to read passphrase file: {e}"))?;
        config = config.with_passphrase(passphrase.trim().to_string());
    }
    let db = Database::open_with_config(config).await?;

    let license_info = if let Some(ref license_path) = args.license {
        match crate::license::verify_license_file(license_path) {
            Ok(info) => {
                tracing::info!(
                    customer = %info.customer,
                    tier = %info.tier,
                    trial = info.trial,
                    days_remaining = info.days_remaining(),
                    "license validated"
                );
                Some(info)
            }
            Err(e) => {
                tracing::warn!("license validation failed: {e} — running in free tier");
                None
            }
        }
    } else {
        None
    };

    let needs_vault = args.passphrase_file.is_some();
    crate::license::enforce_license(license_info.as_ref(), needs_vault, false)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let auth_setup = build_auth_setup_config(&args.auth)?;
    let mut agent = MqdbAgent::new(db)
        .with_bind_address(args.bind)
        .with_auth_setup(auth_setup);

    if let Some(ref info) = license_info {
        agent = agent.with_license_expiry(info.expires_at);
    }

    if let (Some(cert), Some(key)) = (args.quic_cert, args.quic_key) {
        agent = agent.with_quic_certs(cert, key);
    }
    if let Some(ws_addr) = args.ws_bind {
        agent = agent.with_ws_bind_address(ws_addr);
    }
    if let Some(ownership_spec) = args.ownership {
        let admin_set = args.auth.admin_users.iter().cloned().collect();
        let ownership = mqdb_core::types::OwnershipConfig::parse(&ownership_spec)
            .map_err(|e| format!("invalid --ownership: {e}"))?
            .with_admin_users(admin_set);
        agent = agent.with_ownership_config(ownership);
    }

    if let Some(event_scope_spec) = args.event_scope {
        let scope_config = mqdb_core::types::ScopeConfig::parse(&event_scope_spec)
            .map_err(|e| format!("invalid --event-scope: {e}"))?;
        agent = agent.with_scope_config(scope_config);
    }

    if let Some(http_bind) = args.oauth.http_bind {
        let ownership_for_http = agent.ownership_config_arc();
        let http_config = build_http_config(
            http_bind,
            &args.auth,
            &args.oauth,
            ownership_for_http,
            &args.db_path,
        )?;
        if !http_config.cookie_secure {
            tracing::warn!(
                "session cookies will be sent without Secure flag — use --cookie-secure in production"
            );
        }
        agent = agent.with_http_config(http_config);
    }

    let agent = Arc::new(agent);
    agent.run().await.map_err(|e| e.to_string())?;

    Ok(())
}

pub(crate) async fn cmd_agent_status(
    conn: ConnectionArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(&conn)).await?;
    println!("Connected to broker at {}", conn.broker);
    client.disconnect().await?;
    Ok(())
}

pub(crate) fn build_auth_setup_config(
    auth: &AuthArgs,
) -> Result<mqdb_agent::auth_config::AuthSetupConfig, Box<dyn std::error::Error>> {
    use mqtt5::broker::config::{JwtAlgorithm, JwtConfig, RateLimitConfig};

    let jwt_config = if let Some(alg) = auth.jwt_algorithm {
        let key_path = auth
            .jwt_key
            .clone()
            .ok_or("--jwt-key is required when --jwt-algorithm is set")?;
        let algorithm = match alg {
            JwtAlgorithmArg::Hs256 => JwtAlgorithm::HS256,
            JwtAlgorithmArg::Rs256 => JwtAlgorithm::RS256,
            JwtAlgorithmArg::Es256 => JwtAlgorithm::ES256,
        };
        if matches!(algorithm, JwtAlgorithm::HS256) {
            let key_bytes = std::fs::read(&key_path).map_err(|e| {
                format!("failed to read JWT key file '{}': {e}", key_path.display())
            })?;
            if key_bytes.len() < 32 {
                return Err(format!(
                    "JWT HMAC key must be at least 32 bytes (got {})",
                    key_bytes.len()
                )
                .into());
            }
        }
        let mut cfg = JwtConfig::new(algorithm, key_path).with_clock_skew(auth.jwt_clock_skew);
        if let Some(ref issuer) = auth.jwt_issuer {
            cfg = cfg.with_issuer(issuer);
        }
        if let Some(ref audience) = auth.jwt_audience {
            cfg = cfg.with_audience(audience);
        }
        Some(cfg)
    } else {
        None
    };

    let federated_jwt_config = if let Some(ref path) = auth.federated_jwt_config {
        let content = std::fs::read_to_string(path)?;
        let config: mqtt5::broker::config::FederatedJwtConfig = serde_json::from_str(&content)?;
        Some(config)
    } else {
        None
    };

    let rate_limit = if auth.no_rate_limit {
        None
    } else {
        Some(RateLimitConfig {
            enabled: true,
            max_attempts: auth.rate_limit_max_attempts,
            window_secs: auth.rate_limit_window_secs,
            lockout_secs: auth.rate_limit_lockout_secs,
        })
    };

    #[cfg(feature = "dev-insecure")]
    let allow_anonymous = auth.anonymous;
    #[cfg(not(feature = "dev-insecure"))]
    let allow_anonymous = false;

    if !auth.admin_users.is_empty()
        && auth.passwd.is_none()
        && auth.scram_file.is_none()
        && jwt_config.is_none()
        && federated_jwt_config.is_none()
        && auth.cert_auth_file.is_none()
    {
        return Err(
            "--admin-users requires an authentication method (--passwd, --scram-file, --jwt-algorithm, --federated-jwt-config, or --cert-auth-file)".into()
        );
    }

    Ok(mqdb_agent::auth_config::AuthSetupConfig {
        password_file: auth.passwd.clone(),
        acl_file: auth.acl.clone(),
        allow_anonymous,
        scram_file: auth.scram_file.clone(),
        jwt_config,
        federated_jwt_config,
        cert_auth_file: auth.cert_auth_file.clone(),
        rate_limit,
        no_rate_limit: auth.no_rate_limit,
        admin_users: auth.admin_users.iter().cloned().collect(),
    })
}

pub(crate) fn build_http_config(
    http_bind: SocketAddr,
    auth: &AuthArgs,
    oauth: &OAuthArgs,
    ownership_config: std::sync::Arc<mqdb_core::types::OwnershipConfig>,
    db_path: &Path,
) -> Result<mqdb_agent::http::HttpServerConfig, Box<dyn std::error::Error>> {
    let jwt_key_path = auth
        .jwt_key
        .as_ref()
        .ok_or("--jwt-key is required for OAuth JWT signing")?;
    let jwt_key_bytes = std::fs::read_to_string(jwt_key_path)?
        .trim()
        .as_bytes()
        .to_vec();

    if jwt_key_bytes.len() < 32 {
        return Err(format!(
            "JWT signing key is too short ({} bytes) — minimum 32 bytes required",
            jwt_key_bytes.len()
        )
        .into());
    }

    let client_id = if let Some(ref fed_config_path) = auth.federated_jwt_config {
        let content = std::fs::read_to_string(fed_config_path)?;
        let config: serde_json::Value = serde_json::from_str(&content)?;
        config
            .get("providers")
            .and_then(|p| p.as_array())
            .and_then(|arr| arr.first())
            .and_then(|p| p.get("audience"))
            .and_then(|a| a.as_str())
            .map(String::from)
            .ok_or("federated JWT config must have providers[0].audience for OAuth client_id")?
    } else if let Some(ref aud) = auth.jwt_audience {
        aud.clone()
    } else {
        return Err("--jwt-audience or --federated-jwt-config required for OAuth client_id".into());
    };

    let redirect_uri = oauth.oauth_redirect_uri.as_ref().map_or_else(
        || format!("http://localhost:{}/oauth/callback", http_bind.port()),
        String::from,
    );

    let mut registry = mqdb_agent::http::ProviderRegistry::new();

    let google_secret = match oauth.oauth_client_secret.as_ref() {
        Some(path) => std::fs::read_to_string(path)?.trim().to_string(),
        None => return Err("--oauth-client-secret is required when --http-bind is set".into()),
    };

    registry.register(mqdb_agent::http::Provider::Google(
        mqdb_agent::http::GoogleProvider::new(mqdb_agent::http::ProviderConfig {
            client_id: client_id.clone(),
            client_secret: google_secret,
            redirect_uri,
        }),
    ));

    let issuer = auth
        .jwt_issuer
        .clone()
        .unwrap_or_else(|| "mqdb".to_string());
    let audience = auth
        .jwt_audience
        .clone()
        .or_else(|| Some(client_id.clone()));

    let identity_crypto = build_identity_crypto(oauth, db_path)?;

    Ok(mqdb_agent::http::HttpServerConfig {
        bind_address: http_bind,
        provider_registry: registry,
        jwt_config: mqdb_agent::http::JwtSigningConfig {
            algorithm: mqdb_agent::http::JwtSigningAlgorithm::HS256,
            key_bytes: jwt_key_bytes,
            issuer,
            audience,
            expiry_secs: 3600,
        },
        frontend_redirect_uri: oauth.oauth_frontend_redirect.clone(),
        ticket_expiry_secs: oauth.ticket_expiry_secs,
        cookie_secure: oauth.cookie_secure,
        cors_origin: oauth.cors_origin.clone(),
        ticket_rate_limit: oauth.ticket_rate_limit,
        trust_proxy: oauth.trust_proxy,
        identity_crypto,
        ownership_config,
        vault_key_store: None,
        vault_unlock_rate_limit: if auth.no_rate_limit { u32::MAX } else { 5 },
    })
}

fn build_identity_crypto(
    oauth: &OAuthArgs,
    db_path: &Path,
) -> Result<Option<mqdb_agent::http::IdentityCrypto>, Box<dyn std::error::Error>> {
    if cfg!(feature = "dev-insecure") {
        let _ = oauth;
        tracing::info!("dev-insecure: identity encryption disabled");
        return Ok(None);
    }
    if let Some(ref key_path) = oauth.identity_key_file {
        let key_bytes = std::fs::read(key_path).map_err(|e| {
            format!(
                "failed to read identity key file '{}': {e}",
                key_path.display()
            )
        })?;
        let crypto = mqdb_agent::http::IdentityCrypto::from_external_key(&key_bytes)
            .map_err(|e| format!("invalid identity key: {e}"))?;
        tracing::info!("identity encryption enabled (external key)");
        Ok(Some(crypto))
    } else {
        let stored_key_path = db_path.join(".identity_key");
        if stored_key_path.exists() {
            let data = std::fs::read(&stored_key_path).map_err(|e| {
                format!(
                    "failed to read stored identity key '{}': {e}",
                    stored_key_path.display()
                )
            })?;
            let (salt, wrapped_key) = deserialize_key_material(&data)?;
            let crypto = mqdb_agent::http::IdentityCrypto::from_stored(salt, wrapped_key)
                .map_err(|e| format!("failed to load stored identity key: {e}"))?;
            tracing::info!("identity encryption enabled (loaded stored key)");
            Ok(Some(crypto))
        } else {
            let (crypto, material) = mqdb_agent::http::IdentityCrypto::generate()
                .map_err(|e| format!("identity key generation failed: {e}"))?;
            let serialized = serialize_key_material(&material.salt, &material.wrapped_key);
            std::fs::write(&stored_key_path, &serialized).map_err(|e| {
                format!(
                    "failed to persist identity key to '{}': {e}",
                    stored_key_path.display()
                )
            })?;
            tracing::info!("identity encryption enabled (generated and persisted key)");
            Ok(Some(crypto))
        }
    }
}

fn serialize_key_material(salt: &[u8], wrapped_key: &[u8]) -> Vec<u8> {
    let salt_len: u32 = salt.len().try_into().expect("salt length fits in u32");
    let mut out = Vec::with_capacity(4 + salt.len() + wrapped_key.len());
    out.extend_from_slice(&salt_len.to_le_bytes());
    out.extend_from_slice(salt);
    out.extend_from_slice(wrapped_key);
    out
}

fn deserialize_key_material(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if data.len() < 4 {
        return Err("identity key file too short".into());
    }
    let salt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + salt_len {
        return Err("identity key file truncated".into());
    }
    let salt = &data[4..4 + salt_len];
    let wrapped_key = &data[4 + salt_len..];
    Ok((salt, wrapped_key))
}
