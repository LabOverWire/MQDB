// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const JWKS_CACHE_DURATION_SECS: u64 = 3600;

#[derive(Clone)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenResponse {
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    #[serde(flatten)]
    _rest: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IdTokenPayload {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub aud: Option<String>,
    #[serde(default)]
    pub exp: Option<u64>,
    #[serde(default)]
    pub iat: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize, Clone)]
struct Jwk {
    kid: String,
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

struct CachedJwks {
    keys: Vec<Jwk>,
    fetched_at: Instant,
}

static GOOGLE_JWKS_CACHE: OnceLock<RwLock<Option<CachedJwks>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn get_http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(reqwest::Client::new)
}

#[derive(Debug)]
pub enum IdTokenError {
    MissingKid,
    KeyNotFound,
    InvalidSignature,
    InvalidIssuer,
    InvalidAudience,
    Expired,
    JwksFetchFailed(String),
    DecodeFailed(String),
}

impl std::fmt::Display for IdTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingKid => write!(f, "token header missing kid"),
            Self::KeyNotFound => write!(f, "signing key not found in JWKS"),
            Self::InvalidSignature => write!(f, "invalid token signature"),
            Self::InvalidIssuer => write!(f, "invalid token issuer"),
            Self::InvalidAudience => write!(f, "invalid token audience"),
            Self::Expired => write!(f, "token expired"),
            Self::JwksFetchFailed(e) => write!(f, "failed to fetch JWKS: {e}"),
            Self::DecodeFailed(e) => write!(f, "failed to decode token: {e}"),
        }
    }
}

impl std::error::Error for IdTokenError {}

fn get_jwks_cache() -> &'static RwLock<Option<CachedJwks>> {
    GOOGLE_JWKS_CACHE.get_or_init(|| RwLock::new(None))
}

fn is_cache_valid(cached: &CachedJwks) -> bool {
    cached.fetched_at.elapsed() < Duration::from_secs(JWKS_CACHE_DURATION_SECS)
}

async fn fetch_google_jwks() -> Result<Vec<Jwk>, IdTokenError> {
    let cache = get_jwks_cache();

    {
        let guard = cache.read().await;
        if let Some(ref cached) = *guard
            && is_cache_valid(cached)
        {
            return Ok(cached.keys.clone());
        }
    }

    let resp = get_http_client()
        .get(GOOGLE_JWKS_URL)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| IdTokenError::JwksFetchFailed(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(IdTokenError::JwksFetchFailed(format!(
            "HTTP {}",
            resp.status()
        )));
    }

    let jwks: JwksResponse = resp
        .json()
        .await
        .map_err(|e| IdTokenError::JwksFetchFailed(e.to_string()))?;

    {
        let mut guard = cache.write().await;
        if guard.as_ref().is_none_or(|c| !is_cache_valid(c)) {
            *guard = Some(CachedJwks {
                keys: jwks.keys.clone(),
                fetched_at: Instant::now(),
            });
        }
    }

    Ok(jwks.keys)
}

fn find_key_by_kid<'a>(keys: &'a [Jwk], kid: &str) -> Option<&'a Jwk> {
    keys.iter().find(|k| k.kid == kid)
}

pub async fn verify_id_token(
    id_token: &str,
    expected_client_id: &str,
) -> Result<IdTokenPayload, IdTokenError> {
    let header = decode_header(id_token).map_err(|e| IdTokenError::DecodeFailed(e.to_string()))?;
    let kid = header.kid.ok_or(IdTokenError::MissingKid)?;

    let keys = fetch_google_jwks().await?;
    let jwk = find_key_by_kid(&keys, &kid).ok_or(IdTokenError::KeyNotFound)?;

    if jwk.kty != "RSA" {
        return Err(IdTokenError::DecodeFailed("unsupported key type".into()));
    }

    let (Some(n), Some(e)) = (&jwk.n, &jwk.e) else {
        return Err(IdTokenError::DecodeFailed("missing RSA parameters".into()));
    };

    let decoding_key = DecodingKey::from_rsa_components(n, e)
        .map_err(|e| IdTokenError::DecodeFailed(e.to_string()))?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
    validation.set_audience(&[expected_client_id]);
    validation.validate_exp = true;

    let token_data =
        decode::<IdTokenPayload>(id_token, &decoding_key, &validation).map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::InvalidSignature => IdTokenError::InvalidSignature,
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => IdTokenError::InvalidIssuer,
                jsonwebtoken::errors::ErrorKind::InvalidAudience => IdTokenError::InvalidAudience,
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => IdTokenError::Expired,
                _ => IdTokenError::DecodeFailed(e.to_string()),
            }
        })?;

    Ok(token_data.claims)
}

#[allow(dead_code)]
pub fn decode_id_token(id_token: &str) -> Option<IdTokenPayload> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    warn!("decode_id_token called without verification - use verify_id_token instead");

    let parts: Vec<&str> = id_token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    serde_json::from_slice(&payload_bytes).ok()
}

pub fn build_authorize_url(config: &OAuthConfig, state: &str, code_challenge: &str) -> String {
    format!(
        "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope=openid%20email%20profile&state={}&code_challenge={}&code_challenge_method=S256&access_type=offline&prompt=consent",
        urlencod(&config.client_id),
        urlencod(&config.redirect_uri),
        urlencod(state),
        urlencod(code_challenge),
    )
}

pub async fn exchange_code(
    code: &str,
    code_verifier: &str,
    config: &OAuthConfig,
) -> Result<TokenResponse, String> {
    let resp = get_http_client()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("code", code),
            ("client_id", &config.client_id),
            ("client_secret", &config.client_secret),
            ("redirect_uri", &config.redirect_uri),
            ("grant_type", "authorization_code"),
            ("code_verifier", code_verifier),
        ])
        .send()
        .await
        .map_err(|e| format!("token exchange request failed: {e}"))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("token exchange failed: {body}"));
    }

    resp.json::<TokenResponse>()
        .await
        .map_err(|e| format!("failed to parse token response: {e}"))
}

pub async fn refresh_token(
    refresh_token: &str,
    config: &OAuthConfig,
) -> Result<TokenResponse, String> {
    let resp = get_http_client()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("refresh_token", refresh_token),
            ("client_id", &config.client_id),
            ("client_secret", &config.client_secret),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await
        .map_err(|e| format!("refresh request failed: {e}"))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("refresh failed: {body}"));
    }

    resp.json::<TokenResponse>()
        .await
        .map_err(|e| format!("failed to parse refresh response: {e}"))
}

fn urlencod(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => {
                result.push('%');
                result.push(char::from(b"0123456789ABCDEF"[(b >> 4) as usize]));
                result.push(char::from(b"0123456789ABCDEF"[(b & 0x0F) as usize]));
            }
        }
    }
    result
}
