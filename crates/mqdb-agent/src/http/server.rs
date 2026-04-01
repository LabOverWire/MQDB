// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::handlers::{self, ServerState};
use super::identity_crypto::IdentityCrypto;
use super::jwt_signer::JwtSigningConfig;
use super::pkce::PkceCache;
use super::providers::ProviderRegistry;
use super::rate_limiter::RateLimiter;
use super::session_store::{JtiRevocationStore, SessionStore};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use mqdb_core::VaultKeyStore;
use mqdb_core::types::OwnershipConfig;
use mqtt5::client::MqttClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tracing::{error, info, warn};

pub struct HttpServerConfig {
    pub bind_address: SocketAddr,
    pub provider_registry: ProviderRegistry,
    pub jwt_config: JwtSigningConfig,
    pub frontend_redirect_uri: Option<String>,
    pub ticket_expiry_secs: u64,
    pub cookie_secure: bool,
    pub cors_origin: Option<String>,
    pub ticket_rate_limit: u32,
    pub trust_proxy: bool,
    pub identity_crypto: Option<IdentityCrypto>,
    pub ownership_config: Arc<OwnershipConfig>,
    pub vault_key_store: Option<Arc<VaultKeyStore>>,
    pub vault_unlock_rate_limit: u32,
    pub email_auth: bool,
}

pub struct HttpServer {
    config: HttpServerConfig,
    mqtt_client: Arc<MqttClient>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl HttpServer {
    #[must_use]
    pub fn new(
        config: HttpServerConfig,
        mqtt_client: Arc<MqttClient>,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> Self {
        Self {
            config,
            mqtt_client,
            shutdown_rx,
        }
    }

    /// # Errors
    /// Returns an error if the TCP listener fails to bind.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.config.bind_address).await?;
        info!(addr = %self.config.bind_address, "HTTP server listening");

        let vault_key_store = self
            .config
            .vault_key_store
            .unwrap_or_else(|| Arc::new(VaultKeyStore::new()));
        let state = Arc::new(ServerState {
            provider_registry: self.config.provider_registry,
            jwt_config: self.config.jwt_config,
            pkce_cache: Mutex::new(PkceCache::new()),
            mqtt_client: self.mqtt_client,
            frontend_redirect_uri: self.config.frontend_redirect_uri,
            session_store: SessionStore::new(),
            ticket_expiry_secs: self.config.ticket_expiry_secs,
            cookie_secure: self.config.cookie_secure,
            cors_origin: self.config.cors_origin,
            ticket_rate_limiter: RateLimiter::new(self.config.ticket_rate_limit),
            vault_unlock_limiter: RateLimiter::new(self.config.vault_unlock_rate_limit),
            login_rate_limiter: RateLimiter::new(10),
            register_rate_limiter: RateLimiter::new(5),
            jti_revocation: JtiRevocationStore::new(),
            trust_proxy: self.config.trust_proxy,
            identity_crypto: self.config.identity_crypto,
            ownership_config: self.config.ownership_config,
            vault_key_store,
            email_auth: self.config.email_auth,
            verify_rate_limiter: RateLimiter::new(3),
        });

        initialize_identity_constraints(&state).await;

        if state.email_auth {
            spawn_receipt_handler(Arc::clone(&state)).await;
            spawn_challenge_cleanup(Arc::clone(&state));
        }

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let state = Arc::clone(&state);
                            let io = hyper_util::rt::TokioIo::new(stream);
                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    let state = Arc::clone(&state);
                                    async move { handle_request(req, state, addr).await }
                                });
                                if let Err(e) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await
                                {
                                    error!(error = %e, "HTTP connection error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "TCP accept error");
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!("HTTP server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

fn client_ip(
    headers: &http::header::HeaderMap,
    peer_addr: SocketAddr,
    trust_proxy: bool,
) -> String {
    if trust_proxy
        && let Some(xff) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
    {
        return xff.trim().to_string();
    }
    peer_addr.ip().to_string()
}

#[allow(clippy::too_many_lines)]
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<ServerState>,
    peer_addr: SocketAddr,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let headers = req.headers().clone();

    #[cfg(feature = "dev-insecure")]
    if method == Method::OPTIONS && path == "/auth/dev-login" {
        return Ok(handlers::handle_options_with_credentials(
            state.cors_origin.as_deref(),
        ));
    }
    #[cfg(feature = "dev-insecure")]
    if method == Method::POST && path == "/auth/dev-login" {
        let body = req
            .collect()
            .await
            .map(http_body_util::Collected::to_bytes)
            .unwrap_or_default();
        return Ok(handlers::handle_dev_login(&state, &body).await);
    }

    let response = match (&method, path.as_str()) {
        (
            &Method::OPTIONS,
            "/auth/ticket"
            | "/auth/logout"
            | "/auth/session"
            | "/auth/unlink"
            | "/oauth/refresh"
            | "/auth/register"
            | "/auth/login"
            | "/auth/verify/start"
            | "/auth/verify/submit"
            | "/auth/verify/status"
            | "/vault/enable"
            | "/vault/unlock"
            | "/vault/lock"
            | "/vault/disable"
            | "/vault/change"
            | "/vault/status",
        ) => handlers::handle_options_with_credentials(state.cors_origin.as_deref()),
        (&Method::OPTIONS, _) => handlers::handle_options(),
        (&Method::GET, "/health") => handlers::handle_health(&state),
        (&Method::GET, "/oauth/authorize") => handlers::handle_authorize(&state, &query).await,
        (&Method::GET, "/oauth/callback") => handlers::handle_callback(&state, &query).await,
        (&Method::POST, "/oauth/refresh") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_refresh(&state, &body).await
        }
        (&Method::POST, "/auth/ticket") => {
            let ip = client_ip(&headers, peer_addr, state.trust_proxy);
            handlers::handle_ticket(&state, &headers, &ip)
        }
        (&Method::POST, "/auth/logout") => handlers::handle_logout(&state, &headers),
        (&Method::GET, "/auth/session") => handlers::handle_session_status(&state, &headers),
        (&Method::POST, "/auth/register") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            let ip = client_ip(&headers, peer_addr, state.trust_proxy);
            handlers::handle_register(&state, &body, &ip).await
        }
        (&Method::POST, "/auth/login") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            let ip = client_ip(&headers, peer_addr, state.trust_proxy);
            handlers::handle_login(&state, &body, &ip).await
        }
        (&Method::POST, "/auth/verify/start") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            let ip = client_ip(&headers, peer_addr, state.trust_proxy);
            handlers::handle_verify_start(&state, &headers, &body, &ip).await
        }
        (&Method::POST, "/auth/verify/submit") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_verify_submit(&state, &headers, &body).await
        }
        (&Method::GET, "/auth/verify/status") => {
            handlers::handle_verify_status(&state, &headers).await
        }
        (&Method::POST, "/auth/unlink") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_unlink(&state, &headers, &body).await
        }
        (&Method::POST, "/vault/enable") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_vault_enable(&state, &headers, &body).await
        }
        (&Method::POST, "/vault/unlock") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_vault_unlock(&state, &headers, &body).await
        }
        (&Method::POST, "/vault/lock") => handlers::handle_vault_lock(&state, &headers),
        (&Method::POST, "/vault/disable") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_vault_disable(&state, &headers, &body).await
        }
        (&Method::POST, "/vault/change") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_vault_change(&state, &headers, &body).await
        }
        (&Method::GET, "/vault/status") => handlers::handle_vault_status(&state, &headers).await,
        _ => {
            let body = serde_json::json!({"error": "not found"});
            let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
            Response::builder()
                .status(404)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body_bytes)))
                .expect("static response")
        }
    };

    Ok(response)
}

async fn initialize_identity_constraints(state: &ServerState) {
    let unique_field = if state.identity_crypto.is_some() {
        "email_hash"
    } else {
        "primary_email"
    };
    publish_unique_constraint(state, "_identities", unique_field).await;

    if state.email_auth {
        publish_unique_constraint(state, "_credentials", "email_hash").await;
        publish_unique_constraint(state, "_verification_challenges", "target_hash").await;
    }
}

async fn spawn_receipt_handler(state: Arc<ServerState>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>)>(64);

    let receipt_topic = "$DB/_verify/receipts/#";
    let sub_result = state
        .mqtt_client
        .subscribe(receipt_topic, move |msg: mqtt5::types::Message| {
            let tx = tx.clone();
            let topic = msg.topic.clone();
            let payload = msg.payload.clone();
            tokio::spawn(async move {
                let _ = tx.send((topic, payload)).await;
            });
        })
        .await;

    if sub_result.is_err() {
        warn!("failed to subscribe to verification receipt topic");
        return;
    }
    info!("verification receipt handler started");

    tokio::spawn(async move {
        while let Some((topic, payload)) = rx.recv().await {
            let challenge_id = topic.strip_prefix("$DB/_verify/receipts/").unwrap_or("");
            if challenge_id.is_empty() {
                continue;
            }
            let Ok(receipt) = serde_json::from_slice::<serde_json::Value>(&payload) else {
                warn!(challenge_id, "invalid receipt payload");
                continue;
            };
            let receipt_status = receipt.get("status").and_then(|v| v.as_str()).unwrap_or("");

            match receipt_status {
                "delivered" => {
                    handlers::process_receipt_delivered(&state, challenge_id).await;
                }
                "failed" => {
                    handlers::process_receipt_failed(&state, challenge_id).await;
                }
                "verified" => {
                    handlers::process_receipt_verified(&state, challenge_id).await;
                }
                other => {
                    warn!(challenge_id, status = other, "unknown receipt status");
                }
            }
        }
    });
}

fn spawn_challenge_cleanup(state: Arc<ServerState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            handlers::cleanup_expired_challenges(&state).await;
        }
    });
}

async fn publish_unique_constraint(state: &ServerState, entity: &str, field: &str) {
    let payload = serde_json::json!({
        "type": "unique",
        "fields": [field],
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let topic = format!("$DB/_admin/constraint/{entity}/add");
    let response_topic = format!(
        "_mqdb/http_resp/constraint_{}_{}",
        entity,
        std::process::id()
    );

    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if state
        .mqtt_client
        .subscribe(&response_topic, move |msg: mqtt5::types::Message| {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Some(tx) = tx.lock().await.take() {
                    let _ = tx.send(msg.payload.clone());
                }
            });
        })
        .await
        .is_err()
    {
        warn!(entity, "failed to subscribe for constraint init response");
        return;
    }

    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if state
        .mqtt_client
        .publish_with_options(&topic, payload_bytes, options)
        .await
        .is_err()
    {
        warn!(entity, "failed to publish unique constraint");
        let _ = state.mqtt_client.unsubscribe(&response_topic).await;
        return;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = state.mqtt_client.unsubscribe(&response_topic).await;

    match result {
        Ok(Ok(payload)) => {
            if let Ok(resp) = serde_json::from_slice::<serde_json::Value>(&payload) {
                let status = resp.get("status").and_then(|v| v.as_str()).unwrap_or("");
                if status == "ok" || status == "error" {
                    info!(entity, field, "initialized unique constraint");
                } else {
                    warn!(entity, response = %resp, "unexpected constraint init response");
                }
            }
        }
        _ => {
            warn!(entity, "timeout waiting for constraint init response");
        }
    }
}
