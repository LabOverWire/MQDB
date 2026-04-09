// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mqdb_agent::{Database, MqdbAgent};

use crate::cli_types::{AuthArgs, ConnectionArgs, DurabilityArg, JwtAlgorithmArg, OAuthArgs};
use crate::commands::env_secret::{
    resolve_federated_jwt_content, resolve_passphrase, resolve_path_or_data,
};
use crate::common::connect_client;

pub(crate) struct AgentStartArgs {
    pub(crate) bind: SocketAddr,
    pub(crate) db_path: PathBuf,
    pub(crate) auth: AuthArgs,
    pub(crate) durability: DurabilityArg,
    pub(crate) durability_ms: u64,
    pub(crate) quic_cert: Option<PathBuf>,
    pub(crate) quic_key: Option<PathBuf>,
    pub(crate) quic_cert_data: Option<String>,
    pub(crate) quic_key_data: Option<String>,
    pub(crate) ws_bind: Option<SocketAddr>,
    pub(crate) oauth: OAuthArgs,
    pub(crate) ownership: Option<String>,
    pub(crate) event_scope: Option<String>,
    pub(crate) passphrase_file: Option<PathBuf>,
    pub(crate) passphrase_data: Option<String>,
    pub(crate) license: Option<PathBuf>,
    pub(crate) license_data: Option<String>,
    pub(crate) vault_min_passphrase_length: usize,
    pub(crate) otlp_endpoint: Option<String>,
    pub(crate) otel_service_name: String,
    pub(crate) otel_sampling_ratio: f64,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn cmd_agent_start(
    args: AgentStartArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqdb_core::config::{DatabaseConfig, DurabilityMode};

    #[cfg(feature = "opentelemetry")]
    let otel_enabled = args.otlp_endpoint.is_some();
    #[cfg(not(feature = "opentelemetry"))]
    let otel_enabled = false;

    if !otel_enabled {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    let durability_mode = match args.durability {
        DurabilityArg::Immediate => DurabilityMode::Immediate,
        DurabilityArg::Periodic => DurabilityMode::PeriodicMs(args.durability_ms),
        DurabilityArg::None => DurabilityMode::None,
    };

    let mut config = DatabaseConfig::new(&args.db_path).with_durability(durability_mode);
    let passphrase = resolve_passphrase(
        args.passphrase_file.as_ref(),
        args.passphrase_data.as_deref(),
    )?;
    if let Some(ref pp) = passphrase {
        config = config.with_passphrase(pp.clone());
    }
    let db = Database::open_with_config(config).await?;

    let license_info =
        verify_and_log_license(args.license.as_deref(), args.license_data.as_deref());

    let needs_vault = passphrase.is_some();
    crate::license::enforce_license(license_info.as_ref(), needs_vault, false)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let federated_content = resolve_federated_jwt_content(
        args.auth.federated_jwt_config_data.as_deref(),
        args.auth.federated_jwt_config.as_ref(),
    );
    let auth_setup = build_auth_setup_config(&args.auth, federated_content.as_deref())?;
    let mut agent = MqdbAgent::new(db)
        .with_bind_address(args.bind)
        .with_auth_setup(auth_setup)
        .with_vault_min_passphrase_length(args.vault_min_passphrase_length);

    if let Some(ref info) = license_info {
        agent = agent.with_license_expiry(info.expires_at);
    }

    let quic_cert = resolve_path_or_data(
        args.quic_cert,
        args.quic_cert_data.as_deref(),
        "quic_cert.pem",
    )?;
    let quic_key =
        resolve_path_or_data(args.quic_key, args.quic_key_data.as_deref(), "quic_key.pem")?;
    if let (Some(cert), Some(key)) = (quic_cert, quic_key) {
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

    #[cfg(feature = "opentelemetry")]
    if let Some(ref endpoint) = args.otlp_endpoint {
        let telemetry_config = mqtt5::telemetry::TelemetryConfig::new(&args.otel_service_name)
            .with_endpoint(endpoint)
            .with_sampling_ratio(args.otel_sampling_ratio);
        agent = agent.with_telemetry_config(telemetry_config);
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = (&args.otel_service_name, args.otel_sampling_ratio);
        if args.otlp_endpoint.is_some() {
            tracing::warn!(
                "--otlp-endpoint ignored: build with --features opentelemetry to enable OTel tracing"
            );
        }
    }

    if let Some(http_bind) = args.oauth.http_bind {
        let ownership_for_http = agent.ownership_config_arc();
        let mut http_config = build_http_config(
            http_bind,
            &args.auth,
            &args.oauth,
            federated_content.as_deref(),
            ownership_for_http,
            &args.db_path,
        )?;
        http_config.vault_min_passphrase_length = args.vault_min_passphrase_length;
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

#[allow(clippy::too_many_lines)]
pub(crate) fn build_auth_setup_config(
    auth: &AuthArgs,
    federated_content: Option<&str>,
) -> Result<mqdb_agent::auth_config::AuthSetupConfig, Box<dyn std::error::Error>> {
    use mqtt5::broker::config::{JwtAlgorithm, JwtConfig, RateLimitConfig};

    let passwd_path =
        resolve_path_or_data(auth.passwd.clone(), auth.passwd_data.as_deref(), "passwd")?;
    let acl_path = resolve_path_or_data(auth.acl.clone(), auth.acl_data.as_deref(), "acl")?;
    let scram_path =
        resolve_path_or_data(auth.scram_file.clone(), auth.scram_data.as_deref(), "scram")?;
    let cert_auth_path = resolve_path_or_data(
        auth.cert_auth_file.clone(),
        auth.cert_auth_data.as_deref(),
        "cert_auth",
    )?;

    let jwt_key_path = resolve_path_or_data(
        auth.jwt_key.clone(),
        auth.jwt_key_data.as_deref(),
        "jwt_key",
    )?;

    let jwt_config = if let Some(alg) = auth.jwt_algorithm {
        let key_path = jwt_key_path
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

    let federated_jwt_config = if let Some(content) = federated_content {
        let config: mqtt5::broker::config::FederatedJwtConfig = serde_json::from_str(content)?;
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
        && passwd_path.is_none()
        && scram_path.is_none()
        && jwt_config.is_none()
        && federated_jwt_config.is_none()
        && cert_auth_path.is_none()
    {
        return Err(
            "--admin-users requires an authentication method (--passwd, --scram-file, --jwt-algorithm, --federated-jwt-config, or --cert-auth-file)".into()
        );
    }

    Ok(mqdb_agent::auth_config::AuthSetupConfig {
        password_file: passwd_path,
        acl_file: acl_path,
        allow_anonymous,
        scram_file: scram_path,
        jwt_config,
        federated_jwt_config,
        cert_auth_file: cert_auth_path,
        rate_limit,
        no_rate_limit: auth.no_rate_limit,
        admin_users: auth.admin_users.iter().cloned().collect(),
    })
}

pub(crate) fn build_http_config(
    http_bind: SocketAddr,
    auth: &AuthArgs,
    oauth: &OAuthArgs,
    federated_content: Option<&str>,
    ownership_config: std::sync::Arc<mqdb_core::types::OwnershipConfig>,
    db_path: &Path,
) -> Result<mqdb_agent::http::HttpServerConfig, Box<dyn std::error::Error>> {
    let jwt_key_content = if let Some(ref data) = auth.jwt_key_data {
        data.trim().to_string()
    } else if let Some(ref path) = auth.jwt_key {
        std::fs::read_to_string(path)?.trim().to_string()
    } else {
        return Err("--jwt-key is required for OAuth JWT signing".into());
    };
    let jwt_key_bytes = jwt_key_content.as_bytes().to_vec();

    if jwt_key_bytes.len() < 32 {
        return Err(format!(
            "JWT signing key is too short ({} bytes) — minimum 32 bytes required",
            jwt_key_bytes.len()
        )
        .into());
    }

    let mut registry = mqdb_agent::http::ProviderRegistry::new();

    let google_secret_content = if let Some(ref data) = oauth.oauth_client_secret_data {
        Some(data.trim().to_string())
    } else if let Some(ref path) = oauth.oauth_client_secret {
        Some(std::fs::read_to_string(path)?.trim().to_string())
    } else {
        None
    };

    let client_id = if let Some(content) = federated_content {
        let config: serde_json::Value = serde_json::from_str(content)?;
        Some(
            config
                .get("providers")
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|p| p.get("audience"))
                .and_then(|a| a.as_str())
                .map(String::from)
                .ok_or(
                    "federated JWT config must have providers[0].audience for OAuth client_id",
                )?,
        )
    } else if let Some(ref aud) = auth.jwt_audience {
        Some(aud.clone())
    } else if google_secret_content.is_some() {
        return Err("--jwt-audience or --federated-jwt-config required for OAuth client_id".into());
    } else if oauth.email_auth {
        None
    } else {
        return Err(
            "--oauth-client-secret or --email-auth is required when --http-bind is set".into(),
        );
    };

    if let Some(ref secret) = google_secret_content {
        let redirect_uri = oauth.oauth_redirect_uri.as_ref().map_or_else(
            || format!("http://localhost:{}/oauth/callback", http_bind.port()),
            String::from,
        );
        registry.register(mqdb_agent::http::Provider::Google(
            mqdb_agent::http::GoogleProvider::new(mqdb_agent::http::ProviderConfig {
                client_id: client_id.clone().unwrap_or_default(),
                client_secret: secret.clone(),
                redirect_uri,
            }),
        ));
    }

    let issuer = auth
        .jwt_issuer
        .clone()
        .unwrap_or_else(|| "mqdb".to_string());
    let audience = auth.jwt_audience.clone().or(client_id);

    let identity_crypto = build_identity_crypto(oauth, db_path)?.map(std::sync::Arc::new);

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
        vault_min_passphrase_length: 0,
        email_auth: oauth.email_auth,
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

    if let Some(ref data) = oauth.identity_key_data {
        let key_bytes = data.as_bytes();
        let crypto = mqdb_agent::http::IdentityCrypto::from_external_key(key_bytes)
            .map_err(|e| format!("invalid identity key: {e}"))?;
        tracing::info!("identity encryption enabled (external key via env)");
        return Ok(Some(crypto));
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

fn verify_and_log_license(
    path: Option<&std::path::Path>,
    data: Option<&str>,
) -> Option<mqdb_core::license::LicenseInfo> {
    let result = if let Some(content) = data {
        crate::license::verify_license_token(content.trim())
    } else if let Some(p) = path {
        crate::license::verify_license_file(p)
    } else {
        return None;
    };

    match result {
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
