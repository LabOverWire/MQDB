// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod google;

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ProviderIdentity {
    pub provider: &'static str,
    pub provider_sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    pub email_verified: bool,
}

#[derive(Debug, Clone)]
pub struct ProviderTokenResponse {
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
}

#[derive(Debug)]
pub enum ProviderError {
    TokenExchangeFailed(String),
    TokenVerificationFailed(String),
    RefreshFailed(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TokenExchangeFailed(e) => write!(f, "token exchange failed: {e}"),
            Self::TokenVerificationFailed(e) => write!(f, "token verification failed: {e}"),
            Self::RefreshFailed(e) => write!(f, "token refresh failed: {e}"),
        }
    }
}

impl std::error::Error for ProviderError {}

#[derive(Clone)]
pub struct ProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

#[derive(Clone)]
pub enum Provider {
    Google(google::GoogleProvider),
}

impl Provider {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Google(_) => "google",
        }
    }

    /// # Errors
    /// Returns `ProviderError` if the authorization URL cannot be constructed.
    #[must_use]
    pub fn authorize_url(&self, state: &str, code_challenge: &str) -> String {
        match self {
            Self::Google(g) => g.authorize_url(state, code_challenge),
        }
    }

    /// # Errors
    /// Returns `ProviderError` if the code exchange fails.
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<ProviderTokenResponse, ProviderError> {
        match self {
            Self::Google(g) => g.exchange_code(code, code_verifier).await,
        }
    }

    /// # Errors
    /// Returns `ProviderError` if token refresh fails.
    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<ProviderTokenResponse, ProviderError> {
        match self {
            Self::Google(g) => g.refresh_token(refresh_token).await,
        }
    }

    /// # Errors
    /// Returns `ProviderError` if ID token verification fails.
    pub async fn verify_id_token(&self, id_token: &str) -> Result<ProviderIdentity, ProviderError> {
        match self {
            Self::Google(g) => g.verify_id_token(id_token).await,
        }
    }
}

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: HashMap<&'static str, Provider>,
    default_provider: Option<&'static str>,
}

impl ProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            default_provider: None,
        }
    }

    pub fn register(&mut self, provider: Provider) {
        let name = provider.name();
        if self.default_provider.is_none() {
            self.default_provider = Some(name);
        }
        self.providers.insert(name, provider);
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Provider> {
        self.providers.get(name)
    }

    #[must_use]
    pub fn default_provider(&self) -> Option<&Provider> {
        self.default_provider
            .and_then(|name| self.providers.get(name))
    }

    #[must_use]
    pub fn provider_names(&self) -> Vec<&'static str> {
        self.providers.keys().copied().collect()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}
