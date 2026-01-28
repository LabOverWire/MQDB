use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct IdTokenPayload {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
}

pub fn build_authorize_url(
    config: &OAuthConfig,
    state: &str,
    code_challenge: &str,
) -> String {
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
    let client = reqwest::Client::new();
    let resp = client
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
    let client = reqwest::Client::new();
    let resp = client
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

pub fn decode_id_token(id_token: &str) -> Option<IdTokenPayload> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let parts: Vec<&str> = id_token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    serde_json::from_slice(&payload_bytes).ok()
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
