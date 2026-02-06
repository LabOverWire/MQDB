use super::cookies::{build_delete_cookie_header, build_set_cookie_header, parse_session_id};
use super::jwt_signer::{JwtSigningConfig, sign_jwt, verify_jwt_ignore_expiry};
use super::oauth::{self, OAuthConfig};
use super::pkce::PkceCache;
use super::rate_limiter::RateLimiter;
use super::session_store::SessionStore;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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

pub struct ServerState {
    pub oauth_config: OAuthConfig,
    pub jwt_config: JwtSigningConfig,
    pub pkce_cache: Mutex<PkceCache>,
    pub mqtt_client: Arc<MqttClient>,
    pub frontend_redirect_uri: Option<String>,
    pub session_store: SessionStore,
    pub ticket_expiry_secs: u64,
    pub cookie_secure: bool,
    pub cors_origin: Option<String>,
    pub ticket_rate_limiter: RateLimiter,
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
    let origin = cors_origin.unwrap_or("*");
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", origin)
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type");

    if cors_origin.is_some() {
        builder = builder.header("Access-Control-Allow-Credentials", "true");
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
    let origin = cors_origin.unwrap_or("*");
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", origin)
        .header("Set-Cookie", cookie);

    if cors_origin.is_some() {
        builder = builder.header("Access-Control-Allow-Credentials", "true");
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

pub async fn handle_authorize(state: &ServerState) -> HttpResponse {
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
        cache.insert(oauth_state.clone(), code_verifier);
    }

    let url = oauth::build_authorize_url(&state.oauth_config, &oauth_state, &code_challenge);
    redirect_response(&url)
}

pub async fn handle_callback(state: &ServerState, query: &str) -> HttpResponse {
    let params = parse_query(query);

    let Some(code) = params.get("code") else {
        if let Some(err) = params.get("error") {
            let escaped = escape_html(err);
            return html_response(
                400,
                format!("<html><body><h1>OAuth Error</h1><p>{escaped}</p></body></html>"),
            );
        }
        return json_response(400, &json!({"error": "missing code parameter"}));
    };

    let Some(oauth_state) = params.get("state") else {
        return json_response(400, &json!({"error": "missing state parameter"}));
    };

    let code_verifier = {
        let mut cache = state.pkce_cache.lock().await;
        cache.take(oauth_state)
    };

    let Some(code_verifier) = code_verifier else {
        return json_response(400, &json!({"error": "invalid or expired state"}));
    };

    let token_response = match oauth::exchange_code(code, &code_verifier, &state.oauth_config).await
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "OAuth token exchange failed");
            return json_response(
                502,
                &json!({"error": format!("token exchange failed: {e}")}),
            );
        }
    };

    let Some(id_token) = &token_response.id_token else {
        return json_response(502, &json!({"error": "no id_token in response"}));
    };

    let id_payload = match oauth::verify_id_token(id_token, &state.oauth_config.client_id).await {
        Ok(payload) => payload,
        Err(e) => {
            error!(error = %e, "ID token verification failed");
            return json_response(
                401,
                &json!({"error": format!("id_token verification failed: {e}")}),
            );
        }
    };

    if let Some(refresh_token) = &token_response.refresh_token {
        let token_data = json!({
            "id": id_payload.sub,
            "refresh_token": refresh_token,
            "email": id_payload.email,
            "name": id_payload.name,
            "picture": id_payload.picture,
            "updated_at": chrono_now_iso()
        });
        let topic = "$DB/_oauth_tokens/create".to_string();
        let payload = serde_json::to_vec(&token_data).unwrap_or_default();
        if let Err(e) = state.mqtt_client.publish(&topic, payload).await {
            warn!(error = %e, "failed to store OAuth tokens");
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let claims = json!({
        "sub": id_payload.email.as_deref().unwrap_or(&id_payload.sub),
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.jwt_config.expiry_secs,
        "iat": now,
        "google_sub": id_payload.sub,
        "email": id_payload.email,
        "name": id_payload.name,
        "picture": id_payload.picture
    });

    let jwt = sign_jwt(&claims, &state.jwt_config);

    let Some(session_id) = state.session_store.create(
        jwt,
        id_payload.sub.clone(),
        id_payload.email.clone(),
        id_payload.name.clone(),
        id_payload.picture.clone(),
    ) else {
        return json_response(500, &json!({"error": "failed to create session"}));
    };

    let cookie = build_set_cookie_header(&session_id, state.cookie_secure, 86400);

    let redirect_uri = state.frontend_redirect_uri.as_deref().unwrap_or("/");
    let user_json = serde_json::to_string(&serde_json::json!({
        "email": id_payload.email,
        "name": id_payload.name,
        "picture": id_payload.picture,
        "google_sub": id_payload.sub,
    }))
    .unwrap_or_else(|_| "{}".into());

    let user_b64 = URL_SAFE_NO_PAD.encode(user_json.as_bytes());
    let location = format!("{redirect_uri}#user={user_b64}");

    redirect_with_cookie(&location, &cookie)
}

pub async fn handle_refresh(state: &ServerState, body: &[u8]) -> HttpResponse {
    let body_value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_response(400, &json!({"error": "invalid JSON body"})),
    };

    let Some(expired_token) = body_value.get("token").and_then(|v| v.as_str()) else {
        return json_response(400, &json!({"error": "missing token field"}));
    };

    let Some(payload) = verify_jwt_ignore_expiry(expired_token, &state.jwt_config) else {
        return json_response(401, &json!({"error": "invalid or tampered token"}));
    };

    let Some(google_sub) = payload.get("google_sub").and_then(|v| v.as_str()) else {
        return json_response(400, &json!({"error": "token missing google_sub claim"}));
    };

    let Some(stored_data) = read_oauth_token(&state.mqtt_client, google_sub).await else {
        return json_response(404, &json!({"error": "user not found"}));
    };

    let Some(stored_refresh_token) = stored_data
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(String::from)
    else {
        return json_response(404, &json!({"error": "no refresh token stored"}));
    };

    let token_response =
        match oauth::refresh_token(&stored_refresh_token, &state.oauth_config).await {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Google token refresh failed");
                return json_response(502, &json!({"error": format!("refresh failed: {e}")}));
            }
        };

    if let Some(new_refresh) = &token_response.refresh_token {
        let update_data = json!({
            "refresh_token": new_refresh,
            "updated_at": chrono_now_iso()
        });
        let topic = format!("$DB/_oauth_tokens/{google_sub}/update");
        let update_payload = serde_json::to_vec(&update_data).unwrap_or_default();
        if let Err(e) = state.mqtt_client.publish(&topic, update_payload).await {
            warn!(error = %e, "failed to update refresh token");
        }
    }

    let email = stored_data
        .get("email")
        .and_then(|v| v.as_str())
        .unwrap_or(google_sub);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let new_claims = json!({
        "sub": email,
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.jwt_config.expiry_secs,
        "iat": now,
        "google_sub": google_sub,
        "email": stored_data.get("email"),
        "name": stored_data.get("name"),
        "picture": stored_data.get("picture")
    });

    let new_jwt = sign_jwt(&new_claims, &state.jwt_config);

    json_response(
        200,
        &json!({
            "token": new_jwt,
            "expires_in": state.jwt_config.expiry_secs
        }),
    )
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
    let origin = cors_origin.unwrap_or("*");
    let mut builder = Response::builder()
        .status(204)
        .header("Access-Control-Allow-Origin", origin)
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .header("Access-Control-Max-Age", "3600");

    if cors_origin.is_some() {
        builder = builder.header("Access-Control-Allow-Credentials", "true");
    }

    builder
        .body(Full::new(Bytes::new()))
        .expect("static response")
}

pub fn handle_ticket(state: &ServerState, headers: &HeaderMap) -> HttpResponse {
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

    if !state.ticket_rate_limiter.check_and_record(session_id) {
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
        "sub": session.email.as_deref().unwrap_or(&session.google_sub),
        "iss": state.jwt_config.issuer,
        "aud": state.jwt_config.audience,
        "exp": now + state.ticket_expiry_secs,
        "iat": now,
        "google_sub": session.google_sub,
        "email": session.email,
        "name": session.name,
        "picture": session.picture,
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
                    "email": session.email,
                    "name": session.name,
                    "picture": session.picture,
                    "google_sub": session.google_sub
                }
            }),
            cors,
        ),
        None => json_response_with_credentials(200, &json!({"authenticated": false}), cors),
    }
}

async fn read_oauth_token(client: &MqttClient, google_sub: &str) -> Option<serde_json::Value> {
    let topic = format!("$DB/_oauth_tokens/{google_sub}/read");
    let response_topic = format!("$DB/_oauth_tokens/_resp/{}", uuid::Uuid::new_v4());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));

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
        return None;
    }

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
        .await
        .ok()?
        .ok()?;

    let response: serde_json::Value = serde_json::from_slice(&result).ok()?;
    response.get("data").cloned()
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
