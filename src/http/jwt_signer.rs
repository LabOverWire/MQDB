// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use serde_json::Value;

pub struct JwtSigningConfig {
    pub algorithm: JwtSigningAlgorithm,
    pub key_bytes: Vec<u8>,
    pub issuer: String,
    pub audience: Option<String>,
    pub expiry_secs: u64,
}

#[derive(Clone, Copy)]
pub enum JwtSigningAlgorithm {
    HS256,
}

impl JwtSigningAlgorithm {
    fn alg_str(self) -> &'static str {
        match self {
            Self::HS256 => "HS256",
        }
    }
}

pub fn sign_jwt(claims: &Value, config: &JwtSigningConfig) -> String {
    let header = serde_json::json!({
        "alg": config.algorithm.alg_str(),
        "typ": "JWT"
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let message = format!("{header_b64}.{claims_b64}");

    match config.algorithm {
        JwtSigningAlgorithm::HS256 => {
            let key = hmac::Key::new(hmac::HMAC_SHA256, &config.key_bytes);
            let signature = hmac::sign(&key, message.as_bytes());
            let sig_b64 = URL_SAFE_NO_PAD.encode(signature.as_ref());
            format!("{message}.{sig_b64}")
        }
    }
}

const MAX_REFRESH_AGE_SECS: u64 = 30 * 24 * 3600;

pub fn verify_jwt_ignore_expiry(token: &str, config: &JwtSigningConfig) -> Option<Value> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let message = format!("{}.{}", parts[0], parts[1]);
    let signature_bytes = URL_SAFE_NO_PAD.decode(parts[2]).ok()?;
    match config.algorithm {
        JwtSigningAlgorithm::HS256 => {
            let key = hmac::Key::new(hmac::HMAC_SHA256, &config.key_bytes);
            hmac::verify(&key, message.as_bytes(), &signature_bytes).ok()?;
        }
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let payload: Value = serde_json::from_slice(&payload_bytes).ok()?;

    let iat = payload.get("iat").and_then(Value::as_u64)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    if now.saturating_sub(iat) > MAX_REFRESH_AGE_SECS {
        return None;
    }

    Some(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> JwtSigningConfig {
        JwtSigningConfig {
            algorithm: JwtSigningAlgorithm::HS256,
            key_bytes: b"this-is-a-test-key-at-least-32-bytes!".to_vec(),
            issuer: "test".to_string(),
            audience: None,
            expiry_secs: 3600,
        }
    }

    #[test]
    fn verify_rejects_token_older_than_30_days() {
        let config = test_config();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let old_iat = now - (31 * 24 * 3600);
        let claims = serde_json::json!({
            "sub": "alice",
            "iat": old_iat,
            "exp": old_iat + 3600
        });
        let token = sign_jwt(&claims, &config);
        assert!(verify_jwt_ignore_expiry(&token, &config).is_none());
    }

    #[test]
    fn verify_accepts_recent_token() {
        let config = test_config();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "sub": "alice",
            "iat": now - 3600,
            "exp": now - 1
        });
        let token = sign_jwt(&claims, &config);
        assert!(verify_jwt_ignore_expiry(&token, &config).is_some());
    }

    #[test]
    fn verify_rejects_token_without_iat() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "alice",
            "exp": 9_999_999_999_u64
        });
        let token = sign_jwt(&claims, &config);
        assert!(verify_jwt_ignore_expiry(&token, &config).is_none());
    }
}
