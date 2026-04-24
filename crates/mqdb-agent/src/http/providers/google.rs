// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{ProviderConfig, ProviderError, ProviderIdentity, ProviderTokenResponse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const JWKS_CACHE_DURATION_SECS: u64 = 3600;

#[derive(Clone)]
pub struct GoogleProvider {
    config: ProviderConfig,
}

impl GoogleProvider {
    #[must_use]
    pub fn new(config: ProviderConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn authorize_url(&self, state: &str, code_challenge: &str) -> String {
        format!(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope=openid%20email%20profile&state={}&code_challenge={}&code_challenge_method=S256&access_type=offline&prompt=consent",
            urlencod(&self.config.client_id),
            urlencod(&self.config.redirect_uri),
            urlencod(state),
            urlencod(code_challenge),
        )
    }

    /// # Errors
    /// Returns `ProviderError::TokenExchangeFailed` if the code exchange HTTP request fails.
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<ProviderTokenResponse, ProviderError> {
        let resp = get_http_client()
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("code", code),
                ("client_id", &*self.config.client_id),
                ("client_secret", &*self.config.client_secret),
                ("redirect_uri", &*self.config.redirect_uri),
                ("grant_type", "authorization_code"),
                ("code_verifier", code_verifier),
            ])
            .send()
            .await
            .map_err(|e| ProviderError::TokenExchangeFailed(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ProviderError::TokenExchangeFailed(body));
        }

        let token_resp: GoogleTokenResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::TokenExchangeFailed(e.to_string()))?;

        Ok(ProviderTokenResponse {
            refresh_token: token_resp.refresh_token,
            id_token: token_resp.id_token,
        })
    }

    /// # Errors
    /// Returns `ProviderError::RefreshFailed` if the refresh HTTP request fails.
    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<ProviderTokenResponse, ProviderError> {
        let resp = get_http_client()
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("refresh_token", refresh_token),
                ("client_id", &*self.config.client_id),
                ("client_secret", &*self.config.client_secret),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .map_err(|e| ProviderError::RefreshFailed(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ProviderError::RefreshFailed(body));
        }

        let token_resp: GoogleTokenResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::RefreshFailed(e.to_string()))?;

        Ok(ProviderTokenResponse {
            refresh_token: token_resp.refresh_token,
            id_token: token_resp.id_token,
        })
    }

    /// # Errors
    /// Returns `ProviderError::TokenVerificationFailed` if the ID token is invalid.
    pub async fn verify_id_token(&self, id_token: &str) -> Result<ProviderIdentity, ProviderError> {
        let header = decode_header(id_token)
            .map_err(|e| ProviderError::TokenVerificationFailed(e.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| ProviderError::TokenVerificationFailed("missing kid".into()))?;

        let keys = fetch_google_jwks().await?;
        let jwk = keys
            .iter()
            .find(|k| k.kid == kid)
            .ok_or_else(|| ProviderError::TokenVerificationFailed("key not found".into()))?;

        if jwk.kty != "RSA" {
            return Err(ProviderError::TokenVerificationFailed(
                "unsupported key type".into(),
            ));
        }

        let (Some(n), Some(e)) = (&jwk.n, &jwk.e) else {
            return Err(ProviderError::TokenVerificationFailed(
                "missing RSA parameters".into(),
            ));
        };

        let decoding_key = DecodingKey::from_rsa_components(n, e)
            .map_err(|e| ProviderError::TokenVerificationFailed(e.to_string()))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        validation.set_audience(&[&self.config.client_id]);
        validation.validate_exp = true;

        let token_data = decode::<GoogleIdTokenPayload>(id_token, &decoding_key, &validation)
            .map_err(|e| ProviderError::TokenVerificationFailed(e.to_string()))?;

        let claims = token_data.claims;
        Ok(ProviderIdentity {
            provider: "google",
            provider_sub: claims.sub,
            email: claims.email.clone(),
            name: claims.name,
            picture: claims.picture,
            email_verified: claims.email_verified.unwrap_or(false),
        })
    }
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    refresh_token: Option<String>,
    id_token: Option<String>,
    #[serde(flatten)]
    _rest: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GoogleIdTokenPayload {
    sub: String,
    email: Option<String>,
    email_verified: Option<bool>,
    name: Option<String>,
    picture: Option<String>,
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

fn get_jwks_cache() -> &'static RwLock<Option<CachedJwks>> {
    GOOGLE_JWKS_CACHE.get_or_init(|| RwLock::new(None))
}

fn is_cache_valid(cached: &CachedJwks) -> bool {
    cached.fetched_at.elapsed() < Duration::from_secs(JWKS_CACHE_DURATION_SECS)
}

async fn fetch_google_jwks() -> Result<Vec<Jwk>, ProviderError> {
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
        .map_err(|e| ProviderError::TokenVerificationFailed(format!("JWKS fetch failed: {e}")))?;

    if !resp.status().is_success() {
        return Err(ProviderError::TokenVerificationFailed(format!(
            "JWKS fetch HTTP {}",
            resp.status()
        )));
    }

    let jwks: JwksResponse = resp
        .json()
        .await
        .map_err(|e| ProviderError::TokenVerificationFailed(format!("JWKS parse failed: {e}")))?;

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
