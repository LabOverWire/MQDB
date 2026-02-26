// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod cookies;
mod handlers;
pub mod identity_crypto;
mod jwt_signer;
mod pkce;
pub mod providers;
mod rate_limiter;
mod server;
mod session_store;

pub use identity_crypto::IdentityCrypto;
pub use jwt_signer::{JwtSigningAlgorithm, JwtSigningConfig};
pub use providers::google::GoogleProvider;
pub use providers::{Provider, ProviderConfig, ProviderRegistry};
pub use server::{HttpServer, HttpServerConfig};
pub use session_store::SessionStore;
