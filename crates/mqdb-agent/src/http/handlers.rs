// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::cookies::{build_delete_cookie_header, build_set_cookie_header, parse_session_id};
use super::credentials;
use super::identity_crypto::IdentityCrypto;
use super::jwt_signer::{JwtSigningConfig, sign_jwt, verify_jwt_ignore_expiry};
use super::pkce::PkceCache;
use super::providers::{ProviderIdentity, ProviderRegistry};
use super::rate_limiter::RateLimiter;
use super::session_store::{JtiRevocationStore, NewSession, SessionStore};
use crate::database::Database;
use crate::vault_backend::{VaultBackend, VaultError};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::Response;
use http::header::HeaderMap;
use http_body_util::Full;
use hyper::body::Bytes;
use mqdb_core::types::OwnershipConfig;
use mqtt5::client::MqttClient;
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, warn};

fn escape_html(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '"' => result.push_str("&quot;"),
            '\'' => result.push_str("&#39;"),
            _ => result.push(c),
        }
    }
    result
}

fn is_valid_provider_id(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '.')
}

pub struct ServerState {
    pub provider_registry: ProviderRegistry,
    pub jwt_config: JwtSigningConfig,
    pub pkce_cache: Mutex<PkceCache>,
    pub mqtt_client: Arc<MqttClient>,
    pub db: Option<Arc<Database>>,
    pub frontend_redirect_uri: Option<String>,
    pub session_store: SessionStore,
    pub ticket_expiry_secs: u64,
    pub cookie_secure: bool,
    pub cors_origin: Option<String>,
    pub ticket_rate_limiter: RateLimiter,
    pub login_rate_limiter: RateLimiter,
    pub register_rate_limiter: RateLimiter,
    pub jti_revocation: JtiRevocationStore,
    pub trust_proxy: bool,
    pub identity_crypto: Option<Arc<IdentityCrypto>>,
    pub ownership_config: Arc<OwnershipConfig>,
    pub vault_backend: Arc<dyn VaultBackend>,
    pub email_auth: bool,
    pub verify_rate_limiter: RateLimiter,
    pub password_change_rate_limiter: RateLimiter,
    pub password_reset_start_rate_limiter: RateLimiter,
    pub password_reset_submit_rate_limiter: RateLimiter,
    pub refresh_rate_limiter: RateLimiter,
}

type HttpResponse = Response<Full<Bytes>>;

fn json_response(status: u16, body: &serde_json::Value) -> HttpResponse {
    let body_bytes = serde_json::to_vec(body).unwrap_or_default();
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .body(Full::new(Bytes::from(body_bytes)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

fn html_response(status: u16, html: String) -> HttpResponse {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/html; charset=utf-8")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from(html)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

fn redirect_response(location: &str) -> HttpResponse {
    Response::builder()
        .status(302)
        .header("Location", location)
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

fn redirect_with_cookie(location: &str, cookie: &str) -> HttpResponse {
    Response::builder()
        .status(302)
        .header("Location", location)
        .header("Set-Cookie", cookie)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

fn json_response_with_credentials(
    status: u16,
    body: &serde_json::Value,
    cors_origin: Option<&str>,
) -> HttpResponse {
    let body_bytes = serde_json::to_vec(body).unwrap_or_default();
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Type", "application/json");

    if let Some(origin) = cors_origin {
        builder = builder
            .header("Access-Control-Allow-Origin", origin)
            .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type")
            .header("Access-Control-Allow-Credentials", "true");
    }

    builder
        .body(Full::new(Bytes::from(body_bytes)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

fn json_response_with_cookie(
    status: u16,
    body: &serde_json::Value,
    cookie: &str,
    cors_origin: Option<&str>,
) -> HttpResponse {
    let body_bytes = serde_json::to_vec(body).unwrap_or_default();
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Set-Cookie", cookie);

    if let Some(origin) = cors_origin {
        builder = builder
            .header("Access-Control-Allow-Origin", origin)
            .header("Access-Control-Allow-Credentials", "true");
    }

    builder
        .body(Full::new(Bytes::from(body_bytes)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from("internal error")))
                .expect("static response")
        })
}

pub fn handle_health(state: &ServerState) -> HttpResponse {
    let _ = state;
    json_response(
        200,
        &json!({
            "status": "ok",
            "oauth_enabled": true
        }),
    )
}

pub async fn handle_authorize(state: &ServerState, query: &str) -> HttpResponse {
    let params = parse_query(query);
    let provider_name = params.get("provider").map_or("", String::as_str);

    let provider = if provider_name.is_empty() {
        state.provider_registry.default_provider()
    } else {
        state.provider_registry.get(provider_name)
    };

    let Some(provider) = provider else {
        return json_response(400, &json!({"error": "unknown or unconfigured provider"}));
    };

    let rng = SystemRandom::new();
    let mut verifier_bytes = [0u8; 32];
    if rng.fill(&mut verifier_bytes).is_err() {
        return json_response(500, &json!({"error": "failed to generate random bytes"}));
    }
    let code_verifier = URL_SAFE_NO_PAD.encode(verifier_bytes);

    let challenge_hash = digest::digest(&digest::SHA256, code_verifier.as_bytes());
    let code_challenge = URL_SAFE_NO_PAD.encode(challenge_hash.as_ref());

    let mut state_bytes = [0u8; 32];
    if rng.fill(&mut state_bytes).is_err() {
        return json_response(500, &json!({"error": "failed to generate state"}));
    }
    let oauth_state = super::credentials::hex_encode(&state_bytes);

    {
        let mut cache = state.pkce_cache.lock().await;
        cache.insert(
            oauth_state.clone(),
            code_verifier,
            provider.name().to_string(),
        );
    }

    let url = provider.authorize_url(&oauth_state, &code_challenge);
    redirect_response(&url)
}

#[allow(clippy::too_many_lines)]
pub async fn handle_callback(state: &ServerState, query: &str) -> HttpResponse {
    let (identity, token_response, provider) =
        match exchange_and_verify_callback(state, query).await {
            Ok(result) => result,
            Err(resp) => return resp,
        };

    let link_key = format!("{}:{}", identity.provider, identity.provider_sub);

    let canonical_id = match resolve_or_create_identity(state, &identity, &link_key).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "failed to resolve identity");
            return json_response(500, &json!({"error": "identity resolution failed"}));
        }
    };

    if let Some(refresh_token) = &token_response.refresh_token {
        persist_oauth_tokens(state, &link_key, &canonical_id, refresh_token, &identity).await;
    }

    let jwt = mint_callback_jwt(state, &canonical_id, &identity);

    let Some(session_id) = state.session_store.create(NewSession {
        jwt,
        canonical_id,
        provider: provider.to_string(),
        provider_sub: identity.provider_sub.clone(),
        email: identity.email.clone(),
        name: identity.name.clone(),
        picture: identity.picture.clone(),
    }) else {
        return json_response(500, &json!({"error": "failed to create session"}));
    };

    let cookie = build_set_cookie_header(&session_id, state.cookie_secure, 86400);

    let redirect_uri = state.frontend_redirect_uri.as_deref().unwrap_or("/");
    let user_json = serde_json::to_string(&json!({
        "email": identity.email,
        "name": identity.name,
        "picture": identity.picture,
        "provider": identity.provider,
        "provider_sub": identity.provider_sub,
    }))
    .unwrap_or_else(|_| "{}".into());

    let user_b64 = URL_SAFE_NO_PAD.encode(user_json.as_bytes());
    let location = format!("{redirect_uri}#user={user_b64}");

    redirect_with_cookie(&location, &cookie)
}

async fn exchange_and_verify_callback(
    state: &ServerState,
    query: &str,
) -> Result<
    (
        ProviderIdentity,
        super::providers::ProviderTokenResponse,
        &'static str,
    ),
    HttpResponse,
> {
    let params = parse_query(query);

    let Some(code) = params.get("code") else {
        if let Some(err) = params.get("error") {
            let escaped = escape_html(err);
            return Err(html_response(
                400,
                format!("<html><body><h1>OAuth Error</h1><p>{escaped}</p></body></html>"),
            ));
        }
        return Err(json_response(
            400,
            &json!({"error": "missing code parameter"}),
        ));
    };

    let Some(oauth_state) = params.get("state") else {
        return Err(json_response(
            400,
            &json!({"error": "missing state parameter"}),
        ));
    };

    let (code_verifier, provider_name) = {
        let mut cache = state.pkce_cache.lock().await;
        cache.take(oauth_state).unzip()
    };

    let Some(code_verifier) = code_verifier else {
        return Err(json_response(
            400,
            &json!({"error": "invalid or expired state"}),
        ));
    };

    let provider_name_str = provider_name.unwrap_or_default();
    let Some(provider) = state.provider_registry.get(&provider_name_str) else {
        return Err(json_response(
            400,
            &json!({"error": "provider not found for this OAuth flow"}),
        ));
    };

    let token_response = match provider.exchange_code(code, &code_verifier).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "OAuth token exchange failed");
            return Err(json_response(
                502,
                &json!({"error": "token exchange failed"}),
            ));
        }
    };

    let Some(id_token) = &token_response.id_token else {
        return Err(json_response(
            502,
            &json!({"error": "no id_token in response"}),
        ));
    };

    let identity = match provider.verify_id_token(id_token).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "ID token verification failed");
            return Err(json_response(
                401,
                &json!({"error": "authentication failed"}),
            ));
        }
    };

    if !identity
        .provider_sub
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '.')
    {
        return Err(json_response(
            400,
            &json!({"error": "invalid subject identifier format"}),
        ));
    }

    Ok((identity, token_response, provider.name()))
}

async fn resolve_or_create_identity(
    state: &ServerState,
    identity: &ProviderIdentity,
    link_key: &str,
) -> Result<String, String> {
    if let Some(existing_link) = read_entity(&state.mqtt_client, "_identity_links", link_key).await
        && let Some(cid) = existing_link.get("canonical_id").and_then(|v| v.as_str())
    {
        if read_entity(&state.mqtt_client, "_identities", cid)
            .await
            .is_none()
        {
            create_identity(state, cid, identity).await;
        }
        update_identity_link(state, link_key, identity).await;
        return Ok(cid.to_string());
    }

    if identity.email_verified
        && let Some(ref email) = identity.email
        && let Some(existing_cid) = find_canonical_id_by_email(state, email).await
    {
        create_identity_link(state, link_key, &existing_cid, identity).await;
        return Ok(existing_cid);
    }

    let canonical_id = uuid_v4();
    if !create_identity(state, &canonical_id, identity).await {
        if identity.email_verified
            && let Some(ref email) = identity.email
        {
            if let Some(existing_cid) = find_canonical_id_by_email(state, email).await {
                create_identity_link(state, link_key, &existing_cid, identity).await;
                return Ok(existing_cid);
            }
            if let Some(existing_cid) = find_identity_by_email(state, email).await {
                create_identity_link(state, link_key, &existing_cid, identity).await;
                return Ok(existing_cid);
            }
        }
        return Err("failed to create or find identity".to_string());
    }
    create_identity_link(state, link_key, &canonical_id, identity).await;
    Ok(canonical_id)
}

async fn create_identity(
    state: &ServerState,
    canonical_id: &str,
    identity: &ProviderIdentity,
) -> bool {
    let mut data = json!({
        "id": canonical_id,
        "primary_email": identity.email,
        "display_name": identity.name,
        "created_at": chrono_now_iso(),
        "vault_enabled": false,
    });
    if let Some(ref crypto) = state.identity_crypto {
        if let Some(ref email) = identity.email {
            data["email_hash"] =
                serde_json::Value::String(crypto.blind_index("_identities", email));
        }
        crypto.encrypt_json_fields("_identities", &mut data, &["primary_email", "display_name"]);
    }
    let Some(response) =
        create_entity_with_response(&state.mqtt_client, "_identities", &data).await
    else {
        return false;
    };
    response
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "ok")
}

async fn create_identity_link(
    state: &ServerState,
    link_key: &str,
    canonical_id: &str,
    identity: &ProviderIdentity,
) {
    let mut data = json!({
        "id": link_key,
        "canonical_id": canonical_id,
        "provider": identity.provider,
        "provider_sub": identity.provider_sub,
        "email": identity.email,
        "name": identity.name,
        "picture": identity.picture,
    });
    if let Some(ref crypto) = state.identity_crypto {
        if let Some(ref email) = identity.email {
            data["email_hash"] =
                serde_json::Value::String(crypto.blind_index("_identity_links", email));
        }
        crypto.encrypt_json_fields(
            "_identity_links",
            &mut data,
            &["provider_sub", "email", "name", "picture"],
        );
    }
    let _ = create_entity_with_response(&state.mqtt_client, "_identity_links", &data).await;
}

async fn update_identity_link(state: &ServerState, link_key: &str, identity: &ProviderIdentity) {
    let mut data = json!({
        "email": identity.email,
        "name": identity.name,
        "picture": identity.picture,
    });
    if let Some(ref crypto) = state.identity_crypto {
        crypto.encrypt_json_fields("_identity_links", &mut data, &["email", "name", "picture"]);
    }
    let topic = format!("$DB/_identity_links/{link_key}/update");
    let payload = serde_json::to_vec(&data).unwrap_or_default();
    if let Err(e) = state.mqtt_client.publish(&topic, payload).await {
        warn!(error = %e, "failed to update identity link");
    }
}

async fn fetch_picture_from_links(state: &ServerState, canonical_id: &str) -> Option<String> {
    let filter = format!("canonical_id={canonical_id}");
    let links = list_entities(&state.mqtt_client, "_identity_links", &filter).await?;
    for mut link in links {
        if let Some(ref crypto) = state.identity_crypto {
            crypto.decrypt_json_fields("_identity_links", &mut link, &["picture"]);
        }
        if let Some(pic) = link.get("picture").and_then(|v| v.as_str())
            && !pic.is_empty()
        {
            return Some(pic.to_string());
        }
    }
    None
}

fn is_safe_filter_value(value: &str) -> bool {
    !value.is_empty() && !value.contains([',', '<', '>', '=', '!', '~', '?'])
}

async fn find_canonical_id_by_email(state: &ServerState, email: &str) -> Option<String> {
    let (field, value) = if let Some(ref crypto) = state.identity_crypto {
        let hash = crypto.blind_index("_identity_links", email);
        ("email_hash".to_string(), hash)
    } else {
        if !is_safe_filter_value(email) {
            return None;
        }
        ("email".to_string(), email.to_string())
    };
    let filter = format!("{field}={value}");
    let records = list_entities(&state.mqtt_client, "_identity_links", &filter).await?;
    records
        .first()?
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .map(String::from)
}

async fn find_identity_by_email(state: &ServerState, email: &str) -> Option<String> {
    let filter = if let Some(ref crypto) = state.identity_crypto {
        let hash = crypto.blind_index("_identities", email);
        format!("email_hash={hash}")
    } else {
        if !is_safe_filter_value(email) {
            return None;
        }
        format!("primary_email={email}")
    };
    let records = list_entities(&state.mqtt_client, "_identities", &filter).await?;
    records
        .first()?
        .get("id")
        .and_then(|v| v.as_str())
        .map(String::from)
}

async fn persist_oauth_tokens(
    state: &ServerState,
    link_key: &str,
    canonical_id: &str,
    refresh_token: &str,
    identity: &ProviderIdentity,
) {
    let mut token_data = json!({
        "id": link_key,
        "canonical_id": canonical_id,
        "refresh_token": refresh_token,
        "email": identity.email,
        "name": identity.name,
        "picture": identity.picture,
        "updated_at": chrono_now_iso()
    });
    if let Some(ref crypto) = state.identity_crypto {
        crypto.encrypt_json_fields(
            "_oauth_tokens",
            &mut token_data,
            &["refresh_token", "email", "name"],
        );
    }

    let create_response =
        create_entity_with_response(&state.mqtt_client, "_oauth_tokens", &token_data).await;
    let created = create_response
        .as_ref()
        .and_then(|r| r.get("status"))
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "ok");

    if !created {
        if let Some(obj) = token_data.as_object_mut() {
            obj.remove("id");
        }
        let topic = format!("$DB/_oauth_tokens/{link_key}/update");
        let payload = serde_json::to_vec(&token_data).unwrap_or_default();
        if let Err(e) = state.mqtt_client.publish(&topic, payload).await {
            warn!(error = %e, "failed to store OAuth tokens");
        }
    }
}

fn mint_callback_jwt(
    state: &ServerState,
    canonical_id: &str,
    identity: &ProviderIdentity,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let claims = json!({
        "sub": canonical_id,
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.jwt_config.expiry_secs,
        "iat": now,
        "jti": JtiRevocationStore::generate_jti(),
        "email": identity.email,
        "name": identity.name,
        "picture": identity.picture,
        "provider": identity.provider,
        "provider_sub": identity.provider_sub,
    });

    sign_jwt(&claims, &state.jwt_config)
}

pub async fn handle_refresh(state: &ServerState, body: &[u8]) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (canonical_id, provider_name, link_key, stored_data) =
        match validate_refresh_request(body, state).await {
            Ok(result) => result,
            Err(resp) => return resp,
        };

    if !state.refresh_rate_limiter.check_and_record(&canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many requests, try again later"}),
            cors,
        );
    }

    let Some(stored_refresh_token) = stored_data
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(String::from)
    else {
        return json_response_with_credentials(
            404,
            &json!({"error": "no refresh token stored"}),
            cors,
        );
    };

    let Some(provider) = state.provider_registry.get(&provider_name) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "provider not configured"}),
            cors,
        );
    };

    let token_response = match provider.refresh_token(&stored_refresh_token).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "token refresh failed");
            return json_response_with_credentials(
                502,
                &json!({"error": "token refresh failed"}),
                cors,
            );
        }
    };

    if let Some(new_refresh) = &token_response.refresh_token {
        persist_new_refresh_token(state, &link_key, new_refresh).await;
    }

    let new_jwt = mint_refresh_jwt(state, &canonical_id, &provider_name, &stored_data);

    json_response_with_credentials(
        200,
        &json!({
            "token": new_jwt,
            "expires_in": state.jwt_config.expiry_secs
        }),
        cors,
    )
}

async fn validate_refresh_request(
    body: &[u8],
    state: &ServerState,
) -> Result<(String, String, String, serde_json::Value), HttpResponse> {
    let cors = state.cors_origin.as_deref();

    let body_value: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        json_response_with_credentials(400, &json!({"error": "invalid JSON body"}), cors)
    })?;

    let expired_token = body_value
        .get("token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            json_response_with_credentials(400, &json!({"error": "missing token field"}), cors)
        })?;

    let payload = verify_jwt_ignore_expiry(expired_token, &state.jwt_config).ok_or_else(|| {
        json_response_with_credentials(401, &json!({"error": "invalid or tampered token"}), cors)
    })?;

    if let Some(jti) = payload.get("jti").and_then(|v| v.as_str())
        && state.jti_revocation.is_revoked(jti)
    {
        return Err(json_response_with_credentials(
            401,
            &json!({"error": "token has been revoked"}),
            cors,
        ));
    }

    let (canonical_id, provider_name, link_key) =
        extract_identity_from_jwt(&payload, cors).map_err(|b| *b)?;

    let mut stored_data = read_entity(&state.mqtt_client, "_oauth_tokens", &link_key)
        .await
        .ok_or_else(|| {
            json_response_with_credentials(404, &json!({"error": "user not found"}), cors)
        })?;

    if let Some(ref crypto) = state.identity_crypto {
        crypto.decrypt_json_fields(
            "_oauth_tokens",
            &mut stored_data,
            &["refresh_token", "email", "name"],
        );
    }

    Ok((canonical_id, provider_name, link_key, stored_data))
}

fn extract_identity_from_jwt(
    payload: &serde_json::Value,
    cors: Option<&str>,
) -> Result<(String, String, String), Box<HttpResponse>> {
    let canonical_id = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Box::new(json_response_with_credentials(
                400,
                &json!({"error": "token missing sub claim"}),
                cors,
            ))
        })?
        .to_string();
    let provider = payload
        .get("provider")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Box::new(json_response_with_credentials(
                400,
                &json!({"error": "token missing provider claim"}),
                cors,
            ))
        })?
        .to_string();
    let provider_sub = payload
        .get("provider_sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Box::new(json_response_with_credentials(
                400,
                &json!({"error": "token missing provider_sub claim"}),
                cors,
            ))
        })?
        .to_string();
    if !is_valid_provider_id(&provider) {
        return Err(Box::new(json_response_with_credentials(
            400,
            &json!({"error": "invalid provider format"}),
            cors,
        )));
    }
    if !is_valid_provider_id(&provider_sub) {
        return Err(Box::new(json_response_with_credentials(
            400,
            &json!({"error": "invalid provider_sub format"}),
            cors,
        )));
    }
    let link_key = format!("{provider}:{provider_sub}");
    Ok((canonical_id, provider, link_key))
}

async fn persist_new_refresh_token(state: &ServerState, link_key: &str, new_refresh: &str) {
    let mut update_data = json!({
        "refresh_token": new_refresh,
        "updated_at": chrono_now_iso()
    });
    if let Some(ref crypto) = state.identity_crypto {
        crypto.encrypt_json_fields("_oauth_tokens", &mut update_data, &["refresh_token"]);
    }
    let topic = format!("$DB/_oauth_tokens/{link_key}/update");
    let update_payload = serde_json::to_vec(&update_data).unwrap_or_default();
    if let Err(e) = state.mqtt_client.publish(&topic, update_payload).await {
        warn!(error = %e, "failed to update refresh token");
    }
}

fn mint_refresh_jwt(
    state: &ServerState,
    canonical_id: &str,
    provider: &str,
    stored_data: &serde_json::Value,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let provider_sub = stored_data
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|id| id.strip_prefix(&format!("{provider}:")))
        .or_else(|| stored_data.get("provider_sub").and_then(|v| v.as_str()))
        .unwrap_or("");

    let new_claims = json!({
        "sub": canonical_id,
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.jwt_config.expiry_secs,
        "iat": now,
        "jti": JtiRevocationStore::generate_jti(),
        "email": stored_data.get("email"),
        "name": stored_data.get("name"),
        "picture": stored_data.get("picture"),
        "provider": provider,
        "provider_sub": provider_sub,
    });

    sign_jwt(&new_claims, &state.jwt_config)
}

pub fn handle_options() -> HttpResponse {
    Response::builder()
        .status(204)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .header("Access-Control-Max-Age", "3600")
        .body(Full::new(Bytes::new()))
        .expect("static response")
}

pub fn handle_options_with_credentials(cors_origin: Option<&str>) -> HttpResponse {
    let mut builder = Response::builder().status(204);

    if let Some(origin) = cors_origin {
        builder = builder
            .header("Access-Control-Allow-Origin", origin)
            .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type")
            .header("Access-Control-Max-Age", "3600")
            .header("Access-Control-Allow-Credentials", "true");
    }

    builder
        .body(Full::new(Bytes::new()))
        .expect("static response")
}

pub fn handle_ticket(state: &ServerState, headers: &HeaderMap, client_ip: &str) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let cookie_header = headers
        .get(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let Some(session_id) = parse_session_id(cookie_header) else {
        return json_response_with_credentials(401, &json!({"error": "no session cookie"}), cors);
    };

    let Some(session) = state.session_store.get(session_id) else {
        return json_response_with_credentials(
            401,
            &json!({"error": "session not found or expired"}),
            cors,
        );
    };

    if !state.ticket_rate_limiter.check_and_record(client_ip) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many ticket requests, try again later"}),
            cors,
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let ticket_claims = json!({
        "sub": session.canonical_id,
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.ticket_expiry_secs,
        "iat": now,
        "jti": JtiRevocationStore::generate_jti(),
        "email": session.email,
        "name": session.name,
        "picture": session.picture,
        "provider": session.provider,
        "ticket": true
    });

    let ticket_jwt = sign_jwt(&ticket_claims, &state.jwt_config);

    json_response_with_credentials(
        200,
        &json!({
            "ticket": ticket_jwt,
            "expires_in": state.ticket_expiry_secs
        }),
        cors,
    )
}

pub fn handle_logout(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let cookie_header = headers
        .get(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(session_id) = parse_session_id(cookie_header) {
        if let Some(session) = state.session_store.get(session_id)
            && let Some(payload) = verify_jwt_ignore_expiry(&session.jwt, &state.jwt_config)
            && let Some(jti) = payload.get("jti").and_then(|v| v.as_str())
        {
            state.jti_revocation.revoke(jti);
        }
        state.session_store.destroy(session_id);
    }

    let delete_cookie = build_delete_cookie_header();
    json_response_with_cookie(200, &json!({"status": "logged_out"}), &delete_cookie, cors)
}

pub fn handle_session_status(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let cookie_header = headers
        .get(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let Some(session_id) = parse_session_id(cookie_header) else {
        return json_response_with_credentials(200, &json!({"authenticated": false}), cors);
    };

    match state.session_store.get(session_id) {
        Some(session) => json_response_with_credentials(
            200,
            &json!({
                "authenticated": true,
                "user": {
                    "canonical_id": session.canonical_id,
                    "email": session.email,
                    "name": session.name,
                    "picture": session.picture,
                    "provider": session.provider,
                    "provider_sub": session.provider_sub,
                }
            }),
            cors,
        ),
        None => json_response_with_credentials(200, &json!({"authenticated": false}), cors),
    }
}

pub async fn handle_unlink(state: &ServerState, headers: &HeaderMap, body: &[u8]) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let cookie_header = headers
        .get(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let Some(session_id) = parse_session_id(cookie_header) else {
        return json_response_with_credentials(401, &json!({"error": "no session"}), cors);
    };

    let Some(session) = state.session_store.get(session_id) else {
        return json_response_with_credentials(401, &json!({"error": "session expired"}), cors);
    };

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(provider) = body_value.get("provider").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing provider field"}),
            cors,
        );
    };
    let Some(provider_sub) = body_value.get("provider_sub").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing provider_sub field"}),
            cors,
        );
    };

    if !is_valid_provider_id(provider) || !is_valid_provider_id(provider_sub) {
        return json_response_with_credentials(
            400,
            &json!({"error": "invalid provider or provider_sub format"}),
            cors,
        );
    }

    let link_key = format!("{provider}:{provider_sub}");

    let link_record = read_entity(&state.mqtt_client, "_identity_links", &link_key).await;
    let Some(link_data) = link_record else {
        return json_response_with_credentials(
            404,
            &json!({"error": "identity link not found"}),
            cors,
        );
    };

    let link_canonical_id = link_data
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if link_canonical_id != session.canonical_id {
        return json_response_with_credentials(
            403,
            &json!({"error": "identity link belongs to another user"}),
            cors,
        );
    }

    let cid = &session.canonical_id;
    let link_filter = format!("canonical_id={cid}");
    let link_count = list_entities(&state.mqtt_client, "_identity_links", &link_filter)
        .await
        .map_or(0, |v| v.len());
    if link_count < 2 {
        return json_response_with_credentials(
            400,
            &json!({"error": "cannot unlink last remaining provider"}),
            cors,
        );
    }

    let topic = format!("$DB/_identity_links/{link_key}/delete");
    if let Err(e) = state.mqtt_client.publish(&topic, vec![]).await {
        warn!(error = %e, "failed to delete identity link");
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to unlink provider"}),
            cors,
        );
    }

    let token_topic = format!("$DB/_oauth_tokens/{link_key}/delete");
    if let Err(e) = state.mqtt_client.publish(&token_topic, vec![]).await {
        warn!(error = %e, "failed to delete oauth token for unlinked provider");
    }

    json_response_with_credentials(200, &json!({"status": "unlinked"}), cors)
}

async fn read_entity(client: &MqttClient, entity: &str, id: &str) -> Option<serde_json::Value> {
    let topic = format!("$DB/{entity}/{id}");
    let response_topic = format!("_mqdb/http_resp/{}", uuid_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
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
        return None;
    }

    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(&topic, vec![], options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    let payload = result.ok()?.ok()?;

    let response: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").cloned()
}

async fn create_entity_with_response(
    client: &MqttClient,
    entity: &str,
    data: &serde_json::Value,
) -> Option<serde_json::Value> {
    let topic = format!("$DB/{entity}/create");
    let response_topic = format!("_mqdb/http_resp/{}", uuid_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
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
        return None;
    }

    let payload = serde_json::to_vec(data).unwrap_or_default();
    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(&topic, payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    let resp_payload = result.ok()?.ok()?;

    serde_json::from_slice(&resp_payload).ok()
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((urldecode(key), urldecode(value)))
        })
        .collect()
}

fn urldecode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hi = bytes.next().and_then(hex_val);
            let lo = bytes.next().and_then(hex_val);
            if let (Some(h), Some(l)) = (hi, lo) {
                result.push(h << 4 | l);
            }
        } else if b == b'+' {
            result.push(b' ');
        } else {
            result.push(b);
        }
    }
    String::from_utf8(result).unwrap_or_default()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn chrono_now_iso() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format!("{now}")
}

fn uuid_v4() -> String {
    super::challenge_utils::uuid_v4()
}

async fn list_entities(
    client: &MqttClient,
    entity: &str,
    filter: &str,
) -> Option<Vec<serde_json::Value>> {
    let topic = format!("$DB/{entity}/list");
    let response_topic = format!("_mqdb/http_resp/{}", uuid_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
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
        return None;
    }

    let list_payload = if let Some((field, value)) = filter.split_once('=') {
        serde_json::to_vec(&json!({
            "filters": [{"field": field, "op": "eq", "value": value}]
        }))
        .unwrap_or_default()
    } else {
        vec![]
    };

    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(&topic, list_payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    let payload = result.ok()?.ok()?;

    let response: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    response.get("data").and_then(|v| v.as_array()).cloned()
}

async fn update_entity(
    client: &MqttClient,
    entity: &str,
    id: &str,
    data: &serde_json::Value,
) -> bool {
    let topic = format!("$DB/{entity}/{id}/update");
    let response_topic = format!("_mqdb/http_resp/{}", uuid_v4());
    let payload = serde_json::to_vec(data).unwrap_or_default();

    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    if client
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
        return false;
    }

    let props = mqtt5::types::PublishProperties {
        response_topic: Some(response_topic.clone()),
        ..Default::default()
    };
    let options = mqtt5::PublishOptions {
        properties: props,
        ..Default::default()
    };
    if client
        .publish_with_options(&topic, payload, options)
        .await
        .is_err()
    {
        let _ = client.unsubscribe(&response_topic).await;
        return false;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
    let _ = client.unsubscribe(&response_topic).await;
    match result {
        Ok(Ok(payload)) => serde_json::from_slice::<serde_json::Value>(&payload)
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "ok"))
            .unwrap_or(false),
        _ => false,
    }
}

fn require_session<'a>(
    state: &'a ServerState,
    headers: &'a HeaderMap,
) -> Result<(&'a str, super::session_store::SessionRef), Box<HttpResponse>> {
    let cors = state.cors_origin.as_deref();
    let cookie_header = headers
        .get(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let session_id = parse_session_id(cookie_header).ok_or_else(|| {
        Box::new(json_response_with_credentials(
            401,
            &json!({"error": "no session"}),
            cors,
        ))
    })?;

    let session = state.session_store.get(session_id).ok_or_else(|| {
        Box::new(json_response_with_credentials(
            401,
            &json!({"error": "session expired"}),
            cors,
        ))
    })?;

    Ok((session_id, session))
}

fn vault_error_to_http(err: &VaultError, cors: Option<&str>) -> HttpResponse {
    let (status, msg) = match err {
        VaultError::NotEnabled => (400, "vault not enabled".to_string()),
        VaultError::AlreadyEnabled => (409, "vault already enabled".to_string()),
        VaultError::InvalidPassphrase => (401, "invalid passphrase".to_string()),
        VaultError::NotUnlocked => (403, "vault not unlocked".to_string()),
        VaultError::RateLimited => (429, "rate limited".to_string()),
        VaultError::PassphraseTooShort(n) => {
            (400, format!("passphrase must be at least {n} characters"))
        }
        VaultError::Unavailable => (400, "vault not available".to_string()),
        VaultError::BadRequest(m) => (400, m.clone()),
        VaultError::Internal(m) => (500, m.clone()),
    };
    json_response_with_credentials(status, &json!({"error": msg}), cors)
}

fn apply_vault_session_update(state: &ServerState, session_id: &str, update: Option<bool>) {
    if let Some(unlocked) = update {
        state.session_store.set_vault_unlocked(session_id, unlocked);
    }
}

pub async fn handle_vault_enable(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(
                400,
                &json!({"error": "invalid json body"}),
                cors,
            );
        }
    };
    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing passphrase"}), cors);
    };
    let canonical_id = session.canonical_id.clone();
    let result = state
        .vault_backend
        .admin_enable(
            match state.db.as_deref() {
                Some(db) => db,
                None => return vault_error_to_http(&VaultError::Unavailable, cors),
            },
            &state.ownership_config,
            &canonical_id,
            passphrase,
        )
        .await;
    match result {
        Ok(outcome) => {
            apply_vault_session_update(state, session_id, outcome.session_update);
            json_response_with_credentials(200, &outcome.body, cors)
        }
        Err(err) => vault_error_to_http(&err, cors),
    }
}

pub async fn handle_vault_unlock(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(
                400,
                &json!({"error": "invalid json body"}),
                cors,
            );
        }
    };
    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing passphrase"}), cors);
    };
    let canonical_id = session.canonical_id.clone();
    let result = state
        .vault_backend
        .admin_unlock(
            match state.db.as_deref() {
                Some(db) => db,
                None => return vault_error_to_http(&VaultError::Unavailable, cors),
            },
            &state.ownership_config,
            &canonical_id,
            passphrase,
        )
        .await;
    match result {
        Ok(outcome) => {
            apply_vault_session_update(state, session_id, outcome.session_update);
            json_response_with_credentials(200, &outcome.body, cors)
        }
        Err(err) => vault_error_to_http(&err, cors),
    }
}

pub async fn handle_vault_lock(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let canonical_id = session.canonical_id.clone();
    let result = state.vault_backend.admin_lock(&canonical_id).await;
    match result {
        Ok(outcome) => {
            apply_vault_session_update(state, session_id, outcome.session_update);
            json_response_with_credentials(200, &outcome.body, cors)
        }
        Err(err) => vault_error_to_http(&err, cors),
    }
}

pub async fn handle_vault_disable(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(
                400,
                &json!({"error": "invalid json body"}),
                cors,
            );
        }
    };
    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing passphrase"}), cors);
    };
    let canonical_id = session.canonical_id.clone();
    let result = state
        .vault_backend
        .admin_disable(
            match state.db.as_deref() {
                Some(db) => db,
                None => return vault_error_to_http(&VaultError::Unavailable, cors),
            },
            &state.ownership_config,
            &canonical_id,
            passphrase,
        )
        .await;
    match result {
        Ok(outcome) => {
            apply_vault_session_update(state, session_id, outcome.session_update);
            json_response_with_credentials(200, &outcome.body, cors)
        }
        Err(err) => vault_error_to_http(&err, cors),
    }
}

pub async fn handle_vault_change(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(
                400,
                &json!({"error": "invalid json body"}),
                cors,
            );
        }
    };
    let Some(old_passphrase) = body_value
        .get("current_passphrase")
        .and_then(|v| v.as_str())
    else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing current_passphrase"}),
            cors,
        );
    };
    let Some(new_passphrase) = body_value.get("new_passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing new_passphrase"}),
            cors,
        );
    };
    let canonical_id = session.canonical_id.clone();
    let result = state
        .vault_backend
        .admin_change(
            match state.db.as_deref() {
                Some(db) => db,
                None => return vault_error_to_http(&VaultError::Unavailable, cors),
            },
            &state.ownership_config,
            &canonical_id,
            old_passphrase,
            new_passphrase,
        )
        .await;
    match result {
        Ok(outcome) => {
            apply_vault_session_update(state, session_id, outcome.session_update);
            json_response_with_credentials(200, &outcome.body, cors)
        }
        Err(err) => vault_error_to_http(&err, cors),
    }
}

pub async fn handle_vault_status(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();
    let (_session_id, session) = match require_session(state, headers) {
        Ok(pair) => pair,
        Err(resp) => return *resp,
    };
    let canonical_id = session.canonical_id.clone();
    let result = state
        .vault_backend
        .admin_status(
            match state.db.as_deref() {
                Some(db) => db,
                None => return vault_error_to_http(&VaultError::Unavailable, cors),
            },
            &canonical_id,
        )
        .await;
    match result {
        Ok(outcome) => json_response_with_credentials(200, &outcome.body, cors),
        Err(err) => vault_error_to_http(&err, cors),
    }
}

#[allow(clippy::too_many_lines)]
pub async fn handle_register(state: &ServerState, body: &[u8], client_ip: &str) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(email) = body_value.get("email").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing email field"}), cors);
    };
    let Some(password) = body_value.get("password").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing password field"}),
            cors,
        );
    };
    let name = body_value
        .get("name")
        .and_then(|v| v.as_str())
        .map(String::from);

    if !credentials::validate_email(email) {
        return json_response_with_credentials(
            400,
            &json!({"error": "invalid email format"}),
            cors,
        );
    }
    if let Err(msg) = credentials::validate_password(password) {
        return json_response_with_credentials(400, &json!({"error": msg}), cors);
    }

    if !state.register_rate_limiter.check_and_record(client_ip) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many registration attempts, try again later"}),
            cors,
        );
    }

    let email_hash = credentials::compute_email_hash(state.identity_crypto.as_deref(), email);
    let filter = format!("email_hash={email_hash}");
    if let Some(existing) = list_entities(&state.mqtt_client, "_credentials", &filter).await
        && !existing.is_empty()
    {
        return json_response_with_credentials(
            409,
            &json!({"error": "email already registered"}),
            cors,
        );
    }

    let password_hash = match credentials::hash_password(password) {
        Ok(h) => h,
        Err(e) => {
            error!(error = %e, "password hashing failed");
            return json_response_with_credentials(
                500,
                &json!({"error": "registration failed"}),
                cors,
            );
        }
    };

    let provider_sub = uuid_v4();
    let identity = ProviderIdentity {
        provider: "email",
        provider_sub: provider_sub.clone(),
        email: Some(email.to_string()),
        name: name.clone(),
        picture: None,
        email_verified: false,
    };
    let link_key = format!("email:{provider_sub}");

    let canonical_id = match resolve_or_create_identity(state, &identity, &link_key).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "identity resolution failed during registration");
            return json_response_with_credentials(
                500,
                &json!({"error": "registration failed"}),
                cors,
            );
        }
    };

    let cred_data = json!({
        "id": canonical_id,
        "password_hash": password_hash,
        "email_hash": email_hash,
        "created_at": chrono_now_iso(),
    });
    if create_entity_with_response(&state.mqtt_client, "_credentials", &cred_data)
        .await
        .is_none()
    {
        error!("failed to store credentials for {}", canonical_id);
        return json_response_with_credentials(500, &json!({"error": "registration failed"}), cors);
    }

    let jwt = mint_callback_jwt(state, &canonical_id, &identity);
    let Some(session_id) = state.session_store.create(NewSession {
        jwt,
        canonical_id: canonical_id.clone(),
        provider: "email".to_string(),
        provider_sub,
        email: Some(email.to_string()),
        name: name.clone(),
        picture: None,
    }) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "session creation failed"}),
            cors,
        );
    };

    let cookie = build_set_cookie_header(&session_id, state.cookie_secure, 86400);
    json_response_with_cookie(
        200,
        &json!({
            "canonical_id": canonical_id,
            "email": email,
            "name": name,
            "provider": "email",
        }),
        &cookie,
        cors,
    )
}

#[allow(clippy::too_many_lines)]
pub async fn handle_login(state: &ServerState, body: &[u8], client_ip: &str) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(email) = body_value.get("email").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing email field"}), cors);
    };
    let Some(password) = body_value.get("password").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing password field"}),
            cors,
        );
    };

    if !state.login_rate_limiter.check_and_record(client_ip) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many login attempts, try again later"}),
            cors,
        );
    }

    let email_hash = credentials::compute_email_hash(state.identity_crypto.as_deref(), email);
    let filter = format!("email_hash={email_hash}");
    let cred_record = match list_entities(&state.mqtt_client, "_credentials", &filter).await {
        Some(records) if !records.is_empty() => records.into_iter().next().unwrap_or_default(),
        _ => {
            return json_response_with_credentials(
                401,
                &json!({"error": "invalid credentials"}),
                cors,
            );
        }
    };

    let Some(stored_hash) = cred_record.get("password_hash").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(401, &json!({"error": "invalid credentials"}), cors);
    };

    if !credentials::verify_password(stored_hash, password) {
        return json_response_with_credentials(401, &json!({"error": "invalid credentials"}), cors);
    }

    let Some(canonical_id) = cred_record.get("id").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(500, &json!({"error": "login failed"}), cors);
    };

    let (display_name, display_email, verified) = if let Some(mut ident) =
        read_entity(&state.mqtt_client, "_identities", canonical_id).await
    {
        if let Some(ref crypto) = state.identity_crypto {
            crypto.decrypt_json_fields(
                "_identities",
                &mut ident,
                &["primary_email", "display_name"],
            );
        }
        let n = ident
            .get("display_name")
            .and_then(|v| v.as_str())
            .map(String::from);
        let e = ident
            .get("primary_email")
            .and_then(|v| v.as_str())
            .map(String::from);
        let ev = ident
            .get("email_verified")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        (n, e, ev)
    } else {
        (None, Some(email.to_string()), false)
    };

    let picture = fetch_picture_from_links(state, canonical_id).await;

    let identity = ProviderIdentity {
        provider: "email",
        provider_sub: canonical_id.to_string(),
        email: display_email.clone(),
        name: display_name.clone(),
        picture: picture.clone(),
        email_verified: verified,
    };
    let jwt = mint_callback_jwt(state, canonical_id, &identity);

    let Some(session_id) = state.session_store.create(NewSession {
        jwt,
        canonical_id: canonical_id.to_string(),
        provider: "email".to_string(),
        provider_sub: canonical_id.to_string(),
        email: display_email.clone(),
        name: display_name.clone(),
        picture: picture.clone(),
    }) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "session creation failed"}),
            cors,
        );
    };

    let cookie = build_set_cookie_header(&session_id, state.cookie_secure, 86400);
    json_response_with_cookie(
        200,
        &json!({
            "canonical_id": canonical_id,
            "email": display_email,
            "name": display_name,
            "picture": picture,
            "provider": "email",
        }),
        &cookie,
        cors,
    )
}

#[allow(clippy::too_many_lines)]
pub async fn handle_verify_start(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let (_, session) = match require_session(state, headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    let canonical_id = session.canonical_id.clone();

    if !state.verify_rate_limiter.check_and_record(&canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many verification requests, try again later"}),
            cors,
        );
    }

    let body_value: serde_json::Value = serde_json::from_slice(body).unwrap_or(json!({}));
    let method = body_value
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("email");

    let mode = body_value
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("code");
    if mode != "code" && mode != "attestation" {
        return json_response_with_credentials(
            400,
            &json!({"error": "mode must be 'code' or 'attestation'"}),
            cors,
        );
    }

    let Some(mut identity) = read_entity(&state.mqtt_client, "_identities", &canonical_id).await
    else {
        return json_response_with_credentials(404, &json!({"error": "identity not found"}), cors);
    };

    if let Some(ref crypto) = state.identity_crypto {
        crypto.decrypt_json_fields("_identities", &mut identity, &["primary_email"]);
    }

    let Some(email) = identity
        .get("primary_email")
        .and_then(|v| v.as_str())
        .map(String::from)
    else {
        return json_response_with_credentials(
            400,
            &json!({"error": "no email associated with identity"}),
            cors,
        );
    };

    let target_hash = credentials::compute_email_hash(state.identity_crypto.as_deref(), &email);

    expire_pending_challenges(state, &target_hash).await;

    let challenge_id = uuid_v4();
    let now = now_unix();
    let expires_at = now + 600;

    let (code, code_hash) = if mode == "code" {
        let Some(c) = generate_verification_code() else {
            return json_response_with_credentials(
                500,
                &json!({"error": "failed to generate verification code"}),
                cors,
            );
        };
        let h = hash_code(&c);
        (Some(c), Some(h))
    } else {
        (None, None)
    };

    let challenge_data = json!({
        "id": challenge_id,
        "canonical_id": canonical_id,
        "method": method,
        "target_hash": target_hash,
        "code_hash": code_hash,
        "status": "pending",
        "mode": mode,
        "attempts": 0,
        "max_attempts": 5,
        "created_at": now.to_string(),
        "expires_at": expires_at.to_string(),
    });

    if create_entity_with_response(
        &state.mqtt_client,
        "_verification_challenges",
        &challenge_data,
    )
    .await
    .is_none()
    {
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to create verification challenge"}),
            cors,
        );
    }

    let mut notification = json!({
        "challenge_id": challenge_id,
        "method": method,
        "mode": mode,
        "target": email,
        "expires_at": expires_at.to_string(),
    });
    if let Some(ref c) = code {
        notification["code"] = serde_json::Value::String(c.clone());
    }

    let notify_topic = format!("$DB/_verify/challenges/{method}");
    let notify_payload = serde_json::to_vec(&notification).unwrap_or_default();
    if let Err(e) = state
        .mqtt_client
        .publish(&notify_topic, notify_payload)
        .await
    {
        warn!(error = %e, "failed to publish verification challenge notification");
    }

    json_response_with_credentials(
        200,
        &json!({
            "status": "challenge_created",
            "challenge_id": challenge_id,
            "expires_in": 600,
            "method": method,
        }),
        cors,
    )
}

#[allow(clippy::too_many_lines)]
pub async fn handle_verify_submit(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let (_, session) = match require_session(state, headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    let canonical_id = session.canonical_id.clone();

    if !state.verify_rate_limiter.check_and_record(&canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many verification requests, try again later"}),
            cors,
        );
    }

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(challenge_id) = body_value.get("challenge_id").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing challenge_id field"}),
            cors,
        );
    };

    let Some(submitted_code) = body_value.get("code").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing code field"}), cors);
    };

    let Some(challenge) =
        read_entity(&state.mqtt_client, "_verification_challenges", challenge_id).await
    else {
        return json_response_with_credentials(404, &json!({"error": "challenge not found"}), cors);
    };

    let challenge_canonical = challenge
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if challenge_canonical != canonical_id {
        return json_response_with_credentials(
            403,
            &json!({"error": "challenge belongs to another user"}),
            cors,
        );
    }

    if challenge.get("purpose").and_then(|v| v.as_str()) == Some("password_reset") {
        return json_response_with_credentials(
            400,
            &json!({"error": "cannot verify a password reset challenge via this endpoint"}),
            cors,
        );
    }

    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" && status != "delivered" {
        return json_response_with_credentials(
            400,
            &json!({"error": "invalid or expired challenge"}),
            cors,
        );
    }

    let challenge_mode = challenge
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("code");
    if challenge_mode != "code" {
        return json_response_with_credentials(
            400,
            &json!({"error": "challenge is attestation mode, code submission not supported"}),
            cors,
        );
    }

    let expires_at = challenge
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if expires_at > 0 && now_unix() >= expires_at {
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "expired"}),
        )
        .await;
        return json_response_with_credentials(
            400,
            &json!({"error": "challenge has expired"}),
            cors,
        );
    }

    let attempts = challenge
        .get("attempts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let max_attempts = challenge
        .get("max_attempts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(5);
    if attempts >= max_attempts {
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "failed"}),
        )
        .await;
        return json_response_with_credentials(400, &json!({"error": "too many attempts"}), cors);
    }

    let new_attempts = attempts + 1;
    let submitted_hash = hash_code(submitted_code);
    let stored_hash = challenge
        .get("code_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if submitted_hash != stored_hash {
        let new_status = if new_attempts >= max_attempts {
            "failed"
        } else {
            status
        };
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"attempts": new_attempts, "status": new_status}),
        )
        .await;
        let remaining = max_attempts.saturating_sub(new_attempts);
        return json_response_with_credentials(
            401,
            &json!({"error": "invalid code", "attempts_remaining": remaining}),
            cors,
        );
    }

    update_entity(
        &state.mqtt_client,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "verified", "attempts": new_attempts}),
    )
    .await;

    update_entity(
        &state.mqtt_client,
        "_identities",
        &canonical_id,
        &json!({"email_verified": true}),
    )
    .await;

    json_response_with_credentials(200, &json!({"status": "verified"}), cors)
}

pub async fn handle_verify_status(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let (_, session) = match require_session(state, headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    let canonical_id = &session.canonical_id;

    let identity = read_entity(&state.mqtt_client, "_identities", canonical_id).await;
    let email_verified = identity
        .as_ref()
        .and_then(|i| i.get("email_verified"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let filter = format!("canonical_id={canonical_id}");
    let pending_challenge = if let Some(challenges) =
        list_entities(&state.mqtt_client, "_verification_challenges", &filter).await
    {
        let now = now_unix();
        challenges.into_iter().find(|c| {
            let s = c.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let exp = c
                .get("expires_at")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            (s == "pending" || s == "delivered") && (exp == 0 || exp > now)
        })
    } else {
        None
    };

    let challenge_info = pending_challenge.map(|c| {
        let expires_at = c
            .get("expires_at")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let expires_in = expires_at.saturating_sub(now_unix());
        json!({
            "challenge_id": c.get("id").and_then(|v| v.as_str()).unwrap_or(""),
            "method": c.get("method").and_then(|v| v.as_str()).unwrap_or(""),
            "status": c.get("status").and_then(|v| v.as_str()).unwrap_or(""),
            "expires_in": expires_in,
            "attempts": c.get("attempts").and_then(serde_json::Value::as_u64).unwrap_or(0),
            "max_attempts": c.get("max_attempts").and_then(serde_json::Value::as_u64).unwrap_or(5),
        })
    });

    json_response_with_credentials(
        200,
        &json!({
            "email_verified": email_verified,
            "pending_challenge": challenge_info,
        }),
        cors,
    )
}

pub async fn handle_password_change(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let (_, session) = match require_session(state, headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    let canonical_id = session.canonical_id.clone();

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(current_password) = body_value.get("current_password").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing current_password field"}),
            cors,
        );
    };

    let Some(new_password) = body_value.get("new_password").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing new_password field"}),
            cors,
        );
    };

    if let Err(e) = credentials::validate_password(new_password) {
        return json_response_with_credentials(400, &json!({"error": e}), cors);
    }

    if !state
        .password_change_rate_limiter
        .check_and_record(&canonical_id)
    {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many password change attempts, try again later"}),
            cors,
        );
    }

    let identity = read_entity(&state.mqtt_client, "_identities", &canonical_id).await;
    let email_verified = identity
        .as_ref()
        .and_then(|i| i.get("email_verified"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !email_verified {
        return json_response_with_credentials(
            403,
            &json!({"error": "email must be verified before changing password"}),
            cors,
        );
    }

    let Some(cred) = read_entity(&state.mqtt_client, "_credentials", &canonical_id).await else {
        return json_response_with_credentials(
            404,
            &json!({"error": "no credentials found (OAuth-only account)"}),
            cors,
        );
    };

    let Some(stored_hash) = cred.get("password_hash").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "credential record is corrupt"}),
            cors,
        );
    };

    if !credentials::verify_password(stored_hash, current_password) {
        return json_response_with_credentials(
            401,
            &json!({"error": "incorrect current password"}),
            cors,
        );
    }

    let new_hash = match credentials::hash_password(new_password) {
        Ok(h) => h,
        Err(e) => {
            error!(error = %e, "password hashing failed during password change");
            return json_response_with_credentials(500, &json!({"error": "internal error"}), cors);
        }
    };

    if !update_entity(
        &state.mqtt_client,
        "_credentials",
        &canonical_id,
        &json!({"password_hash": new_hash}),
    )
    .await
    {
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to update credentials"}),
            cors,
        );
    }

    json_response_with_credentials(200, &json!({"status": "password changed"}), cors)
}

#[allow(clippy::too_many_lines)]
pub async fn handle_password_reset_start(
    state: &ServerState,
    body: &[u8],
    client_ip: &str,
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(email) = body_value.get("email").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing email field"}), cors);
    };

    if !state
        .password_reset_start_rate_limiter
        .check_and_record(client_ip)
    {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many requests, try again later"}),
            cors,
        );
    }

    let email_hash = credentials::compute_email_hash(state.identity_crypto.as_deref(), email);

    let creds = list_entities(
        &state.mqtt_client,
        "_credentials",
        &format!("email_hash={email_hash}"),
    )
    .await;

    let canonical_id_from_creds = creds
        .as_ref()
        .and_then(|list| list.first())
        .and_then(|c| c.get("id").and_then(|v| v.as_str()))
        .map(String::from);

    let canonical_id = match canonical_id_from_creds {
        Some(id) => id,
        None => match find_identity_by_email(state, email).await {
            Some(id) => id,
            None => {
                return json_response_with_credentials(
                    200,
                    &json!({"status": "reset_started", "expires_in": 600}),
                    cors,
                );
            }
        },
    };

    expire_pending_challenges(state, &email_hash).await;

    let challenge_id = uuid_v4();
    let now = now_unix();
    let expires_at = now + 600;

    let Some(code) = generate_verification_code() else {
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to generate verification code"}),
            cors,
        );
    };
    let code_hash = hash_code(&code);

    let challenge_data = json!({
        "id": challenge_id,
        "canonical_id": canonical_id,
        "method": "email",
        "target_hash": email_hash,
        "code_hash": code_hash,
        "status": "pending",
        "mode": "code",
        "purpose": "password_reset",
        "attempts": 0,
        "max_attempts": 5,
        "created_at": now.to_string(),
        "expires_at": expires_at.to_string(),
    });

    if create_entity_with_response(
        &state.mqtt_client,
        "_verification_challenges",
        &challenge_data,
    )
    .await
    .is_none()
    {
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to create reset challenge"}),
            cors,
        );
    }

    let notification = json!({
        "challenge_id": challenge_id,
        "method": "email",
        "mode": "code",
        "purpose": "password_reset",
        "target": email,
        "code": code,
        "expires_at": expires_at.to_string(),
    });

    let notify_payload = serde_json::to_vec(&notification).unwrap_or_default();
    if let Err(e) = state
        .mqtt_client
        .publish("$DB/_verify/challenges/email", notify_payload)
        .await
    {
        warn!(error = %e, "failed to publish password reset challenge notification");
    }

    json_response_with_credentials(
        200,
        &json!({
            "status": "reset_started",
            "challenge_id": challenge_id,
            "expires_in": 600,
        }),
        cors,
    )
}

#[allow(clippy::too_many_lines)]
pub async fn handle_password_reset_submit(
    state: &ServerState,
    body: &[u8],
    client_ip: &str,
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    if !state.email_auth {
        return json_response_with_credentials(404, &json!({"error": "not found"}), cors);
    }

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(challenge_id) = body_value.get("challenge_id").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing challenge_id field"}),
            cors,
        );
    };

    let Some(submitted_code) = body_value.get("code").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing code field"}), cors);
    };

    let Some(new_password) = body_value.get("new_password").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing new_password field"}),
            cors,
        );
    };

    if let Err(e) = credentials::validate_password(new_password) {
        return json_response_with_credentials(400, &json!({"error": e}), cors);
    }

    if !state
        .password_reset_submit_rate_limiter
        .check_and_record(client_ip)
    {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many requests, try again later"}),
            cors,
        );
    }

    let Some(challenge) =
        read_entity(&state.mqtt_client, "_verification_challenges", challenge_id).await
    else {
        return json_response_with_credentials(404, &json!({"error": "challenge not found"}), cors);
    };

    let purpose = challenge
        .get("purpose")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if purpose != "password_reset" {
        return json_response_with_credentials(
            400,
            &json!({"error": "invalid challenge type"}),
            cors,
        );
    }

    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" && status != "delivered" {
        return json_response_with_credentials(
            400,
            &json!({"error": "invalid or expired challenge"}),
            cors,
        );
    }

    let expires_at = challenge
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if expires_at > 0 && now_unix() >= expires_at {
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "expired"}),
        )
        .await;
        return json_response_with_credentials(
            400,
            &json!({"error": "challenge has expired"}),
            cors,
        );
    }

    let attempts = challenge
        .get("attempts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let max_attempts = challenge
        .get("max_attempts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(5);
    if attempts >= max_attempts {
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"status": "failed"}),
        )
        .await;
        return json_response_with_credentials(400, &json!({"error": "too many attempts"}), cors);
    }

    let new_attempts = attempts + 1;
    let submitted_hash = hash_code(submitted_code);
    let stored_hash = challenge
        .get("code_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if submitted_hash != stored_hash {
        let new_status = if new_attempts >= max_attempts {
            "failed"
        } else {
            status
        };
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            challenge_id,
            &json!({"attempts": new_attempts, "status": new_status}),
        )
        .await;
        let remaining = max_attempts.saturating_sub(new_attempts);
        return json_response_with_credentials(
            401,
            &json!({"error": "invalid code", "attempts_remaining": remaining}),
            cors,
        );
    }

    update_entity(
        &state.mqtt_client,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "verified", "attempts": new_attempts}),
    )
    .await;

    let canonical_id = challenge
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let new_hash = match credentials::hash_password(new_password) {
        Ok(h) => h,
        Err(e) => {
            error!(error = %e, "password hashing failed during password reset");
            return json_response_with_credentials(500, &json!({"error": "internal error"}), cors);
        }
    };

    let target_hash = challenge
        .get("target_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let existing_creds = read_entity(&state.mqtt_client, "_credentials", canonical_id).await;
    if existing_creds.is_some() {
        update_entity(
            &state.mqtt_client,
            "_credentials",
            canonical_id,
            &json!({"password_hash": new_hash}),
        )
        .await;
    } else {
        create_entity_with_response(
            &state.mqtt_client,
            "_credentials",
            &json!({
                "id": canonical_id,
                "password_hash": new_hash,
                "email_hash": target_hash,
            }),
        )
        .await;
    }

    update_entity(
        &state.mqtt_client,
        "_identities",
        canonical_id,
        &json!({"email_verified": true}),
    )
    .await;

    json_response_with_credentials(200, &json!({"status": "password_reset"}), cors)
}

#[cfg(feature = "dev-insecure")]
pub async fn handle_dev_login(state: &ServerState, body: &[u8]) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(email) = body_value.get("email").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(400, &json!({"error": "missing email field"}), cors);
    };

    let name = body_value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("Dev User");

    let canonical_id = body_value
        .get("canonical_id")
        .and_then(|v| v.as_str())
        .map_or_else(uuid_v4, String::from);

    let existing = read_entity(&state.mqtt_client, "_identities", &canonical_id).await;
    if existing.is_none() {
        let data = json!({
            "id": canonical_id,
            "primary_email": email,
            "display_name": name,
            "created_at": chrono_now_iso(),
            "vault_enabled": false,
        });
        let _ = create_entity_with_response(&state.mqtt_client, "_identities", &data).await;
    }

    let Some(session_id) = state.session_store.create(NewSession {
        jwt: String::new(),
        canonical_id: canonical_id.clone(),
        provider: "dev".to_string(),
        provider_sub: "dev-local".to_string(),
        email: Some(email.to_string()),
        name: Some(name.to_string()),
        picture: None,
    }) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "failed to create session"}),
            cors,
        );
    };

    let cookie = build_set_cookie_header(&session_id, state.cookie_secure, 86400);

    json_response_with_cookie(
        200,
        &json!({
            "canonical_id": canonical_id,
            "email": email,
            "name": name,
        }),
        &cookie,
        cors,
    )
}

pub async fn process_receipt_delivered(state: &ServerState, challenge_id: &str) {
    let Some(challenge) =
        read_entity(&state.mqtt_client, "_verification_challenges", challenge_id).await
    else {
        return;
    };
    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" {
        return;
    }
    update_entity(
        &state.mqtt_client,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "delivered"}),
    )
    .await;
}

pub async fn process_receipt_failed(state: &ServerState, challenge_id: &str) {
    let Some(challenge) =
        read_entity(&state.mqtt_client, "_verification_challenges", challenge_id).await
    else {
        return;
    };
    let status = challenge
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status != "pending" && status != "delivered" {
        return;
    }
    update_entity(
        &state.mqtt_client,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "failed"}),
    )
    .await;
}

pub async fn process_receipt_verified(state: &ServerState, challenge_id: &str) {
    let Some(challenge) =
        read_entity(&state.mqtt_client, "_verification_challenges", challenge_id).await
    else {
        return;
    };
    let mode = challenge.get("mode").and_then(|v| v.as_str()).unwrap_or("");
    if mode != "attestation" {
        warn!(
            challenge_id,
            "receipt 'verified' only valid for attestation mode"
        );
        return;
    }
    update_entity(
        &state.mqtt_client,
        "_verification_challenges",
        challenge_id,
        &json!({"status": "verified"}),
    )
    .await;

    if let Some(canonical_id) = challenge.get("canonical_id").and_then(|v| v.as_str()) {
        update_entity(
            &state.mqtt_client,
            "_identities",
            canonical_id,
            &json!({"email_verified": true}),
        )
        .await;
    }
}

pub async fn cleanup_expired_challenges(state: &ServerState) {
    for status_filter in &["status=pending", "status=delivered"] {
        let Some(challenges) = list_entities(
            &state.mqtt_client,
            "_verification_challenges",
            status_filter,
        )
        .await
        else {
            continue;
        };
        let now = now_unix();
        for challenge in challenges {
            let expires_at = challenge
                .get("expires_at")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if expires_at == 0 || expires_at > now {
                continue;
            }
            let Some(id) = challenge.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            update_entity(
                &state.mqtt_client,
                "_verification_challenges",
                id,
                &json!({"status": "expired"}),
            )
            .await;
        }
    }
}

fn generate_verification_code() -> Option<String> {
    super::challenge_utils::generate_verification_code()
}

fn hash_code(code: &str) -> String {
    super::challenge_utils::hash_code(code)
}

async fn expire_pending_challenges(state: &ServerState, target_hash: &str) {
    let filter = format!("target_hash={target_hash}");
    let Some(challenges) =
        list_entities(&state.mqtt_client, "_verification_challenges", &filter).await
    else {
        return;
    };
    for challenge in challenges {
        let status = challenge
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "pending" && status != "delivered" {
            continue;
        }
        let Some(id) = challenge.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        update_entity(
            &state.mqtt_client,
            "_verification_challenges",
            id,
            &json!({"status": "expired"}),
        )
        .await;
    }
}

fn now_unix() -> u64 {
    super::challenge_utils::now_unix()
}
