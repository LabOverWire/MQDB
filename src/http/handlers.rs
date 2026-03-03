// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::cookies::{build_delete_cookie_header, build_set_cookie_header, parse_session_id};
use super::identity_crypto::IdentityCrypto;
use super::jwt_signer::{JwtSigningConfig, sign_jwt, verify_jwt_ignore_expiry};
use super::pkce::PkceCache;
use super::providers::{ProviderIdentity, ProviderRegistry};
use super::rate_limiter::RateLimiter;
use super::session_store::{JtiRevocationStore, SessionStore};
use super::vault_crypto::VaultCrypto;
use crate::VaultKeyStore;
use crate::types::OwnershipConfig;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD};
use http::Response;
use http::header::HeaderMap;
use http_body_util::Full;
use hyper::body::Bytes;
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
    pub frontend_redirect_uri: Option<String>,
    pub session_store: SessionStore,
    pub ticket_expiry_secs: u64,
    pub cookie_secure: bool,
    pub cors_origin: Option<String>,
    pub ticket_rate_limiter: RateLimiter,
    pub vault_unlock_limiter: RateLimiter,
    pub jti_revocation: JtiRevocationStore,
    pub trust_proxy: bool,
    pub identity_crypto: Option<IdentityCrypto>,
    pub ownership_config: Arc<OwnershipConfig>,
    pub vault_key_store: Arc<VaultKeyStore>,
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
    let oauth_state = hex::encode(state_bytes);

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

    let Some(session_id) = state.session_store.create(
        jwt,
        canonical_id,
        provider.to_string(),
        identity.provider_sub.clone(),
        identity.email.clone(),
        identity.name.clone(),
        identity.picture.clone(),
    ) else {
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
            && let Some(existing_cid) = find_canonical_id_by_email(state, email).await
        {
            create_identity_link(state, link_key, &existing_cid, identity).await;
            return Ok(existing_cid);
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
    publish_entity(&state.mqtt_client, "_identity_links", &data).await;
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

fn is_safe_filter_value(value: &str) -> bool {
    !value.is_empty() && !value.contains([',', '<', '>', '=', '!', '~', '?'])
}

async fn find_canonical_id_by_email(state: &ServerState, email: &str) -> Option<String> {
    let filter = if let Some(ref crypto) = state.identity_crypto {
        let hash = crypto.blind_index("_identity_links", email);
        format!("email_hash={hash}")
    } else {
        if !is_safe_filter_value(email) {
            return None;
        }
        format!("email={email}")
    };
    let client = &state.mqtt_client;
    let topic = "$DB/_identity_links/list".to_string();
    let response_topic = format!("$DB/_identity_links/_resp/{}", uuid_v4());

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
        user_properties: vec![("filter".to_string(), filter)],
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
    let items = response.get("data")?.as_array()?;
    items
        .first()?
        .get("canonical_id")
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
    let response_topic = format!("$DB/{entity}/_resp/{}", uuid_v4());

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

async fn publish_entity(client: &MqttClient, entity: &str, data: &serde_json::Value) {
    let topic = format!("$DB/{entity}/create");
    let payload = serde_json::to_vec(data).unwrap_or_default();
    if let Err(e) = client.publish(&topic, payload).await {
        warn!(error = %e, entity = entity, "failed to publish entity");
    }
}

async fn create_entity_with_response(
    client: &MqttClient,
    entity: &str,
    data: &serde_json::Value,
) -> Option<serde_json::Value> {
    let topic = format!("$DB/{entity}/create");
    let response_topic = format!("$DB/{entity}/_resp/{}", uuid_v4());

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
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes)
        .expect("system RNG unavailable — OS CSPRNG failure");
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

async fn list_entities(
    client: &MqttClient,
    entity: &str,
    filter: &str,
) -> Option<Vec<serde_json::Value>> {
    let topic = format!("$DB/{entity}/list");
    let response_topic = format!("$DB/{entity}/_resp/{}", uuid_v4());

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
        user_properties: vec![("filter".to_string(), filter.to_string())],
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
    let payload = serde_json::to_vec(data).unwrap_or_default();
    client.publish(&topic, payload).await.is_ok()
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

#[allow(clippy::too_many_lines)]
pub async fn handle_vault_enable(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (_, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid.to_string(), s),
        Err(resp) => return *resp,
    };

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing passphrase field"}),
            cors,
        );
    };

    let canonical_id = &session.canonical_id;

    let Some(identity) = read_entity(&state.mqtt_client, "_identities", canonical_id).await else {
        return json_response_with_credentials(404, &json!({"error": "identity not found"}), cors);
    };

    if identity
        .get("vault_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return json_response_with_credentials(
            409,
            &json!({"error": "vault already enabled"}),
            cors,
        );
    }

    let salt = VaultCrypto::generate_salt();
    let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);

    let check_token = match crypto.create_check_token() {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "vault check token creation failed");
            return json_response_with_credentials(
                500,
                &json!({"error": "encryption failed"}),
                cors,
            );
        }
    };

    let _fence = state.vault_key_store.acquire_fence(canonical_id).await;
    state.vault_key_store.set(canonical_id, key_bytes);

    let batch = batch_vault_operation(state, canonical_id, &crypto, VaultMode::Encrypt).await;

    let identity_update = json!({
        "vault_enabled": true,
        "vault_salt": BASE64.encode(salt),
        "vault_check": check_token,
    });
    update_entity(
        &state.mqtt_client,
        "_identities",
        canonical_id,
        &identity_update,
    )
    .await;

    state
        .session_store
        .set_vault_unlocked_by_canonical_id(canonical_id, true);

    let mut body = json!({"status": "enabled", "records_encrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    json_response_with_credentials(200, &body, cors)
}

pub async fn handle_vault_unlock(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (session_id, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid.to_string(), s),
        Err(resp) => return *resp,
    };

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing passphrase field"}),
            cors,
        );
    };

    let canonical_id = &session.canonical_id;

    if !state.vault_unlock_limiter.check_and_record(canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many unlock attempts, try again later"}),
            cors,
        );
    }

    let Some(identity) = read_entity(&state.mqtt_client, "_identities", canonical_id).await else {
        return json_response_with_credentials(404, &json!({"error": "identity not found"}), cors);
    };

    if !identity
        .get("vault_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return json_response_with_credentials(400, &json!({"error": "vault not enabled"}), cors);
    }

    let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "vault salt missing from identity"}),
            cors,
        );
    };
    let Ok(salt) = BASE64.decode(salt_b64) else {
        return json_response_with_credentials(500, &json!({"error": "invalid vault salt"}), cors);
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "vault check token missing"}),
            cors,
        );
    };

    let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);
    if !crypto.verify_check_token(check_token) {
        return json_response_with_credentials(
            401,
            &json!({"error": "incorrect passphrase"}),
            cors,
        );
    }

    state.vault_key_store.set(canonical_id, key_bytes);
    state.session_store.set_vault_unlocked(&session_id, true);

    json_response_with_credentials(200, &json!({"status": "unlocked"}), cors)
}

pub fn handle_vault_lock(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (_, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid, s),
        Err(resp) => return *resp,
    };

    state.vault_key_store.remove(&session.canonical_id);
    state
        .session_store
        .set_vault_unlocked_by_canonical_id(&session.canonical_id, false);

    json_response_with_credentials(200, &json!({"status": "locked"}), cors)
}

#[allow(clippy::too_many_lines)]
pub async fn handle_vault_disable(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (_, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid.to_string(), s),
        Err(resp) => return *resp,
    };

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(passphrase) = body_value.get("passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing passphrase field"}),
            cors,
        );
    };

    let canonical_id = &session.canonical_id;

    if !state.vault_unlock_limiter.check_and_record(canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many unlock attempts, try again later"}),
            cors,
        );
    }

    let Some(identity) = read_entity(&state.mqtt_client, "_identities", canonical_id).await else {
        return json_response_with_credentials(404, &json!({"error": "identity not found"}), cors);
    };

    if !identity
        .get("vault_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return json_response_with_credentials(400, &json!({"error": "vault not enabled"}), cors);
    }

    let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(500, &json!({"error": "vault salt missing"}), cors);
    };
    let Ok(salt) = BASE64.decode(salt_b64) else {
        return json_response_with_credentials(500, &json!({"error": "invalid vault salt"}), cors);
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "vault check token missing"}),
            cors,
        );
    };

    let crypto = VaultCrypto::derive(passphrase, &salt);
    if !crypto.verify_check_token(check_token) {
        return json_response_with_credentials(
            401,
            &json!({"error": "incorrect passphrase"}),
            cors,
        );
    }

    let _fence = state.vault_key_store.acquire_fence(canonical_id).await;
    state.vault_key_store.remove(canonical_id);

    let batch = batch_vault_operation(state, canonical_id, &crypto, VaultMode::Decrypt).await;

    let identity_update = json!({
        "vault_enabled": false,
        "vault_salt": null,
        "vault_check": null,
    });
    update_entity(
        &state.mqtt_client,
        "_identities",
        canonical_id,
        &identity_update,
    )
    .await;

    state
        .session_store
        .set_vault_unlocked_by_canonical_id(canonical_id, false);

    let mut body = json!({"status": "disabled", "records_decrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    json_response_with_credentials(200, &body, cors)
}

#[allow(clippy::too_many_lines)]
pub async fn handle_vault_change(
    state: &ServerState,
    headers: &HeaderMap,
    body: &[u8],
) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (_, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid.to_string(), s),
        Err(resp) => return *resp,
    };

    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return json_response_with_credentials(400, &json!({"error": "invalid JSON"}), cors);
        }
    };

    let Some(old_passphrase) = body_value.get("old_passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing old_passphrase field"}),
            cors,
        );
    };
    let Some(new_passphrase) = body_value.get("new_passphrase").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            400,
            &json!({"error": "missing new_passphrase field"}),
            cors,
        );
    };

    let canonical_id = &session.canonical_id;

    if !state.vault_unlock_limiter.check_and_record(canonical_id) {
        return json_response_with_credentials(
            429,
            &json!({"error": "too many unlock attempts, try again later"}),
            cors,
        );
    }

    let Some(identity) = read_entity(&state.mqtt_client, "_identities", canonical_id).await else {
        return json_response_with_credentials(404, &json!({"error": "identity not found"}), cors);
    };

    if !identity
        .get("vault_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return json_response_with_credentials(400, &json!({"error": "vault not enabled"}), cors);
    }

    let Some(old_salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(500, &json!({"error": "vault salt missing"}), cors);
    };
    let Ok(old_salt) = BASE64.decode(old_salt_b64) else {
        return json_response_with_credentials(500, &json!({"error": "invalid vault salt"}), cors);
    };

    let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
        return json_response_with_credentials(
            500,
            &json!({"error": "vault check token missing"}),
            cors,
        );
    };

    let old_crypto = VaultCrypto::derive(old_passphrase, &old_salt);
    if !old_crypto.verify_check_token(check_token) {
        return json_response_with_credentials(
            401,
            &json!({"error": "incorrect old passphrase"}),
            cors,
        );
    }

    let new_salt = VaultCrypto::generate_salt();
    let (new_crypto, new_key_bytes) = VaultCrypto::derive_with_raw_key(new_passphrase, &new_salt);

    let new_check = match new_crypto.create_check_token() {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "new vault check token creation failed");
            return json_response_with_credentials(
                500,
                &json!({"error": "encryption failed"}),
                cors,
            );
        }
    };

    let _fence = state.vault_key_store.acquire_fence(canonical_id).await;
    state.vault_key_store.set(canonical_id, new_key_bytes);

    let batch = batch_vault_re_encrypt(state, canonical_id, &old_crypto, &new_crypto).await;

    let identity_update = json!({
        "vault_salt": BASE64.encode(new_salt),
        "vault_check": new_check,
    });
    update_entity(
        &state.mqtt_client,
        "_identities",
        canonical_id,
        &identity_update,
    )
    .await;

    state
        .session_store
        .set_vault_unlocked_by_canonical_id(canonical_id, true);

    let mut body = json!({"status": "changed", "records_re_encrypted": batch.succeeded});
    if batch.failed > 0 || !batch.entities_skipped.is_empty() {
        body["failed"] = json!(batch.failed);
        body["warning"] = json!("some records could not be processed");
    }
    json_response_with_credentials(200, &body, cors)
}

pub async fn handle_vault_status(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
    let cors = state.cors_origin.as_deref();

    let (_session_id, session) = match require_session(state, headers) {
        Ok((sid, s)) => (sid.to_string(), s),
        Err(resp) => return *resp,
    };

    let canonical_id = &session.canonical_id;
    let vault_enabled = if let Some(identity) =
        read_entity(&state.mqtt_client, "_identities", canonical_id).await
    {
        identity
            .get("vault_enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    } else {
        false
    };

    let unlocked = session.vault_unlocked;

    json_response_with_credentials(
        200,
        &json!({"vault_enabled": vault_enabled, "unlocked": unlocked}),
        cors,
    )
}

enum VaultMode {
    Encrypt,
    Decrypt,
}

struct BatchResult {
    succeeded: usize,
    failed: usize,
    entities_skipped: Vec<String>,
}

async fn batch_vault_operation(
    state: &ServerState,
    canonical_id: &str,
    crypto: &VaultCrypto,
    mode: VaultMode,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &state.ownership_config.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities(&state.mqtt_client, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = record.clone();
            let skip: Vec<&str> = vec!["id", owner_field.as_str()];
            match mode {
                VaultMode::Encrypt => crypto.encrypt_record(entity, id, &mut data, &skip),
                VaultMode::Decrypt => crypto.decrypt_record(entity, id, &mut data, &skip),
            }
            if let Some(obj) = data.as_object_mut() {
                obj.remove("id");
            }
            if update_entity(&state.mqtt_client, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
}

async fn batch_vault_re_encrypt(
    state: &ServerState,
    canonical_id: &str,
    old_crypto: &VaultCrypto,
    new_crypto: &VaultCrypto,
) -> BatchResult {
    let mut result = BatchResult {
        succeeded: 0,
        failed: 0,
        entities_skipped: Vec::new(),
    };
    for (entity, owner_field) in &state.ownership_config.entity_owner_fields {
        let filter = format!("{owner_field}={canonical_id}");
        let Some(records) = list_entities(&state.mqtt_client, entity, &filter).await else {
            result.entities_skipped.push(entity.clone());
            continue;
        };
        for record in records {
            let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut data = record.clone();
            let skip: Vec<&str> = vec!["id", owner_field.as_str()];
            old_crypto.decrypt_record(entity, id, &mut data, &skip);
            new_crypto.encrypt_record(entity, id, &mut data, &skip);
            if let Some(obj) = data.as_object_mut() {
                obj.remove("id");
            }
            if update_entity(&state.mqtt_client, entity, id, &data).await {
                result.succeeded += 1;
            } else {
                result.failed += 1;
            }
        }
    }
    result
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
        publish_entity(&state.mqtt_client, "_identities", &data).await;
    }

    let Some(session_id) = state.session_store.create(
        String::new(),
        canonical_id.clone(),
        "dev".to_string(),
        "dev-local".to_string(),
        Some(email.to_string()),
        Some(name.to_string()),
        None,
    ) else {
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

mod hex {
    pub fn encode(bytes: [u8; 32]) -> String {
        let mut s = String::with_capacity(64);
        for b in bytes {
            s.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
            s.push(char::from(b"0123456789abcdef"[(b & 0x0F) as usize]));
        }
        s
    }
}
