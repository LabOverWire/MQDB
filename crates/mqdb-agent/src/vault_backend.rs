// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::database::Database;
use mqdb_core::protocol::DbOp;
use mqdb_core::transport::{Request, Response, VaultConstraintData};
use mqdb_core::types::OwnershipConfig;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("vault not enabled")]
    NotEnabled,
    #[error("vault already enabled")]
    AlreadyEnabled,
    #[error("invalid passphrase")]
    InvalidPassphrase,
    #[error("rate limited")]
    RateLimited,
    #[error("passphrase must be at least {0} characters")]
    PassphraseTooShort(usize),
    #[error("vault not available")]
    Unavailable,
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Internal(String),
}

pub type VaultResult<T> = Result<T, VaultError>;

pub type VaultFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type EncryptRequestOutput = VaultResult<(Request, Option<VaultConstraintData>)>;

pub struct VaultAdminOutcome {
    pub body: serde_json::Value,
    pub session_update: Option<bool>,
}

pub trait DbAccess: Send + Sync {
    fn read_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
    ) -> VaultFuture<'a, Option<serde_json::Value>>;

    fn update_entity<'a>(
        &'a self,
        entity: &'a str,
        id: &'a str,
        data: serde_json::Value,
    ) -> VaultFuture<'a, bool>;

    fn list_entities<'a>(
        &'a self,
        entity: &'a str,
        filter: &'a str,
    ) -> VaultFuture<'a, Option<Vec<serde_json::Value>>>;

    fn create_entity<'a>(
        &'a self,
        entity: &'a str,
        data: serde_json::Value,
    ) -> VaultFuture<'a, bool>;
}

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
        db: &'a Database,
        entity: &'a str,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        request: Request,
    ) -> VaultFuture<'a, EncryptRequestOutput>;

    fn decrypt_response<'a>(
        &'a self,
        entity: &'a str,
        operation: DbOp,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        response: &'a mut Response,
    ) -> VaultFuture<'a, ()>;

    fn admin_enable<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;

    fn admin_unlock<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;

    fn admin_lock<'a>(
        &'a self,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;

    fn admin_disable<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;

    fn admin_change<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        old_passphrase: &'a str,
        new_passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;

    fn admin_status<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>>;
}

pub struct NoopDbAccess;

impl DbAccess for NoopDbAccess {
    fn read_entity<'a>(
        &'a self,
        _entity: &'a str,
        _id: &'a str,
    ) -> VaultFuture<'a, Option<serde_json::Value>> {
        Box::pin(async { None })
    }
    fn update_entity<'a>(
        &'a self,
        _entity: &'a str,
        _id: &'a str,
        _data: serde_json::Value,
    ) -> VaultFuture<'a, bool> {
        Box::pin(async { false })
    }
    fn list_entities<'a>(
        &'a self,
        _entity: &'a str,
        _filter: &'a str,
    ) -> VaultFuture<'a, Option<Vec<serde_json::Value>>> {
        Box::pin(async { None })
    }
    fn create_entity<'a>(
        &'a self,
        _entity: &'a str,
        _data: serde_json::Value,
    ) -> VaultFuture<'a, bool> {
        Box::pin(async { false })
    }
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
        _db: &'a Database,
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
        _operation: DbOp,
        _ownership: &'a OwnershipConfig,
        _sender_uid: Option<&'a str>,
        _response: &'a mut Response,
    ) -> VaultFuture<'a, ()> {
        Box::pin(async {})
    }

    fn admin_enable<'a>(
        &'a self,
        _db: &'a dyn DbAccess,
        _ownership: &'a OwnershipConfig,
        _canonical_id: &'a str,
        _passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }

    fn admin_unlock<'a>(
        &'a self,
        _db: &'a dyn DbAccess,
        _ownership: &'a OwnershipConfig,
        _canonical_id: &'a str,
        _passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }

    fn admin_lock<'a>(
        &'a self,
        _canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }

    fn admin_disable<'a>(
        &'a self,
        _db: &'a dyn DbAccess,
        _ownership: &'a OwnershipConfig,
        _canonical_id: &'a str,
        _passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }

    fn admin_change<'a>(
        &'a self,
        _db: &'a dyn DbAccess,
        _ownership: &'a OwnershipConfig,
        _canonical_id: &'a str,
        _old_passphrase: &'a str,
        _new_passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }

    fn admin_status<'a>(
        &'a self,
        _db: &'a dyn DbAccess,
        _canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async { Err(VaultError::Unavailable) })
    }
}
