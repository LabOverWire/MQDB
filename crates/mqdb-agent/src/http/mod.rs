// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod cookies;
pub(crate) mod credentials;
mod handlers;
pub mod identity_crypto;
mod jwt_signer;
mod pkce;
pub mod providers;
pub mod rate_limiter;
mod server;
mod session_store;
pub mod vault_crypto;

pub use identity_crypto::IdentityCrypto;
pub use jwt_signer::{JwtSigningAlgorithm, JwtSigningConfig};
pub use mqdb_core::vault_keys::VaultKeyStore;
pub use providers::google::GoogleProvider;
pub use providers::{Provider, ProviderConfig, ProviderRegistry};
pub use rate_limiter::RateLimiter;
pub use server::{HttpServer, HttpServerConfig};
pub use session_store::SessionStore;
pub use vault_crypto::VaultCrypto;
