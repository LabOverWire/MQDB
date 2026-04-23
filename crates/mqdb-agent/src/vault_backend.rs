// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_core::Operation;
use mqdb_core::transport::{Request, Response, VaultConstraintData};
use mqdb_core::types::OwnershipConfig;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("vault not enabled")]
    NotEnabled,
    #[error("invalid passphrase")]
    InvalidPassphrase,
    #[error("vault already enabled")]
    AlreadyEnabled,
    #[error("vault not available")]
    Unavailable,
    #[error("{0}")]
    Internal(String),
}

pub type VaultResult<T> = Result<T, VaultError>;

pub type VaultFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type EncryptRequestOutput = VaultResult<(Request, Option<VaultConstraintData>)>;

pub trait VaultBackend: Send + Sync {
    fn is_eligible(
        &self,
        entity: &str,
        ownership: &OwnershipConfig,
        canonical_id: Option<&str>,
    ) -> bool;

    fn await_read_fence<'a>(&'a self, canonical_id: &'a str) -> VaultFuture<'a, ()>;

    fn encrypt_request<'a>(
        &'a self,
        entity: &'a str,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        request: Request,
    ) -> VaultFuture<'a, EncryptRequestOutput>;

    fn decrypt_response<'a>(
        &'a self,
        entity: &'a str,
        operation: Operation,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        response: &'a mut Response,
    ) -> VaultFuture<'a, ()>;
}

pub struct NoopVaultBackend;

impl VaultBackend for NoopVaultBackend {
    fn is_eligible(
        &self,
        _entity: &str,
        _ownership: &OwnershipConfig,
        _canonical_id: Option<&str>,
    ) -> bool {
        false
    }

    fn await_read_fence<'a>(&'a self, _canonical_id: &'a str) -> VaultFuture<'a, ()> {
        Box::pin(async {})
    }

    fn encrypt_request<'a>(
        &'a self,
        _entity: &'a str,
        _ownership: &'a OwnershipConfig,
        _sender_uid: Option<&'a str>,
        request: Request,
    ) -> VaultFuture<'a, EncryptRequestOutput> {
        Box::pin(async move { Ok((request, None)) })
    }

    fn decrypt_response<'a>(
        &'a self,
        _entity: &'a str,
        _operation: Operation,
        _ownership: &'a OwnershipConfig,
        _sender_uid: Option<&'a str>,
        _response: &'a mut Response,
    ) -> VaultFuture<'a, ()> {
        Box::pin(async {})
    }
}
