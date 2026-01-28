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

pub fn decode_jwt_payload_unverified(token: &str) -> Option<Value> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    serde_json::from_slice(&payload_bytes).ok()
}
