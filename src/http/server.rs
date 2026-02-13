// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::handlers::{self, ServerState};
use super::jwt_signer::JwtSigningConfig;
use super::oauth::OAuthConfig;
use super::pkce::PkceCache;
use super::rate_limiter::RateLimiter;
use super::session_store::SessionStore;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use mqtt5::client::MqttClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tracing::{error, info};

pub struct HttpServerConfig {
    pub bind_address: SocketAddr,
    pub oauth_config: OAuthConfig,
    pub jwt_config: JwtSigningConfig,
    pub frontend_redirect_uri: Option<String>,
    pub ticket_expiry_secs: u64,
    pub cookie_secure: bool,
    pub cors_origin: Option<String>,
    pub ticket_rate_limit: u32,
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

        let state = Arc::new(ServerState {
            oauth_config: self.config.oauth_config,
            jwt_config: self.config.jwt_config,
            pkce_cache: Mutex::new(PkceCache::new()),
            mqtt_client: self.mqtt_client,
            frontend_redirect_uri: self.config.frontend_redirect_uri,
            session_store: SessionStore::new(),
            ticket_expiry_secs: self.config.ticket_expiry_secs,
            cookie_secure: self.config.cookie_secure,
            cors_origin: self.config.cors_origin,
            ticket_rate_limiter: RateLimiter::new(self.config.ticket_rate_limit),
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            let state = Arc::clone(&state);
                            let io = hyper_util::rt::TokioIo::new(stream);
                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    let state = Arc::clone(&state);
                                    async move { handle_request(req, state).await }
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

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let headers = req.headers().clone();

    let response = match (&method, path.as_str()) {
        (
            &Method::OPTIONS,
            "/auth/ticket" | "/auth/logout" | "/auth/session" | "/oauth/refresh",
        ) => handlers::handle_options_with_credentials(state.cors_origin.as_deref()),
        (&Method::OPTIONS, _) => handlers::handle_options(),
        (&Method::GET, "/health") => handlers::handle_health(&state),
        (&Method::GET, "/oauth/authorize") => handlers::handle_authorize(&state).await,
        (&Method::GET, "/oauth/callback") => handlers::handle_callback(&state, &query).await,
        (&Method::POST, "/oauth/refresh") => {
            let body = req
                .collect()
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            handlers::handle_refresh(&state, &body).await
        }
        (&Method::POST, "/auth/ticket") => handlers::handle_ticket(&state, &headers),
        (&Method::POST, "/auth/logout") => handlers::handle_logout(&state, &headers),
        (&Method::GET, "/auth/session") => handlers::handle_session_status(&state, &headers),
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
