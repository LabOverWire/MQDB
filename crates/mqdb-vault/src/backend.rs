// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::crypto::VaultCrypto;
use crate::key_store::VaultKeyStore;
use crate::ops;
use crate::transform::{
    build_vault_skip_fields, ensure_id, is_vault_eligible, vault_decrypt_fields,
    vault_encrypt_fields,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use mqdb_agent::Database;
use mqdb_agent::db_helpers::{read_entity_db, update_entity_db};
use mqdb_agent::http::rate_limiter::RateLimiter;
use mqdb_agent::vault_backend::{
    EncryptRequestOutput, VaultAdminOutcome, VaultBackend, VaultError, VaultFuture, VaultResult,
};
use mqdb_core::protocol::DbOp;
use mqdb_core::transport::{Request, Response, VaultConstraintData};
use mqdb_core::types::{OwnershipConfig, OwnershipDecision};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error};

pub struct VaultBackendImpl {
    key_store: Arc<VaultKeyStore>,
    min_passphrase_length: usize,
    unlock_limiter: RateLimiter,
}

pub struct VaultBackendConfig {
    pub min_passphrase_length: usize,
    pub unlock_rate_limit: u32,
    pub key_store: Option<Arc<VaultKeyStore>>,
}

impl VaultBackendImpl {
    #[must_use]
    pub fn new(config: VaultBackendConfig) -> Self {
        let key_store = config
            .key_store
            .unwrap_or_else(|| Arc::new(VaultKeyStore::new()));
        Self {
            key_store,
            min_passphrase_length: config.min_passphrase_length,
            unlock_limiter: RateLimiter::new(config.unlock_rate_limit),
        }
    }

    #[must_use]
    pub fn key_store(&self) -> Arc<VaultKeyStore> {
        Arc::clone(&self.key_store)
    }

    fn check_rate_limit(&self, canonical_id: &str) -> VaultResult<()> {
        if self.unlock_limiter.check_and_record(canonical_id) {
            Ok(())
        } else {
            Err(VaultError::RateLimited)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn pre_update_encrypt(
    db: &Database,
    crypto: &VaultCrypto,
    entity: &str,
    id: &str,
    ownership: &OwnershipConfig,
    sender_uid: Option<&str>,
    delta: Value,
    skip: &[String],
) -> Result<(Request, Option<(Value, Value)>), Response> {
    if let OwnershipDecision::Check {
        owner_field,
        sender: uid,
    } = ownership.evaluate(entity, sender_uid)
        && let Err(e) = db.check_ownership(entity, id, owner_field, uid)
    {
        return Err(e.into());
    }

    let Ok(mut decrypted_existing) = db
        .read(entity.to_string(), id.to_string(), vec![], None)
        .await
    else {
        return Ok((
            Request::Update {
                entity: entity.to_string(),
                id: id.to_string(),
                fields: delta,
            },
            None,
        ));
    };

    vault_decrypt_fields(crypto, entity, id, &mut decrypted_existing, skip);

    let plaintext_existing = decrypted_existing.clone();

    if let (Some(base), Some(patch)) = (decrypted_existing.as_object_mut(), delta.as_object()) {
        for (k, v) in patch {
            base.insert(k.clone(), v.clone());
        }
    }

    let plaintext_merged = decrypted_existing.clone();

    if let Some(obj) = decrypted_existing.as_object_mut() {
        obj.remove("id");
        for sf in skip {
            if sf != "id" {
                obj.remove(sf.as_str());
            }
        }
    }

    let mut merged = decrypted_existing;
    vault_encrypt_fields(crypto, entity, id, &mut merged, skip);

    Ok((
        Request::Update {
            entity: entity.to_string(),
            id: id.to_string(),
            fields: merged,
        },
        Some((plaintext_merged, plaintext_existing)),
    ))
}

impl VaultBackend for VaultBackendImpl {
    fn is_eligible(
        &self,
        entity: &str,
        ownership: &OwnershipConfig,
        canonical_id: Option<&str>,
    ) -> bool {
        canonical_id.is_some() && is_vault_eligible(entity, ownership)
    }

    fn await_read_fence<'a>(&'a self, canonical_id: &'a str) -> VaultFuture<'a, ()> {
        Box::pin(async move { self.key_store.read_fence(canonical_id).await })
    }

    fn encrypt_request<'a>(
        &'a self,
        db: &'a Database,
        entity: &'a str,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        request: Request,
    ) -> VaultFuture<'a, EncryptRequestOutput> {
        Box::pin(async move {
            if !self.is_eligible(entity, ownership, sender_uid) {
                return Ok((request, None));
            }
            let Some(uid) = sender_uid else {
                return Ok((request, None));
            };
            let Some(key_bytes) = self.key_store.get(uid) else {
                return Ok((request, None));
            };
            let Some(crypto) = VaultCrypto::from_key_bytes(&key_bytes) else {
                return Ok((request, None));
            };

            let skip = build_vault_skip_fields(entity, ownership);
            match request {
                Request::Create {
                    entity: ent,
                    mut data,
                } => {
                    let id = ensure_id(&mut data);
                    let constraint_clone = data.clone();
                    vault_encrypt_fields(&crypto, &ent, &id, &mut data, &skip);
                    debug!(entity = %ent, id = %id, "vault-encrypted create");
                    Ok((
                        Request::Create { entity: ent, data },
                        Some(VaultConstraintData::Create(constraint_clone)),
                    ))
                }
                Request::Update {
                    entity: ent,
                    id,
                    fields: delta,
                } => {
                    match pre_update_encrypt(
                        db, &crypto, &ent, &id, ownership, sender_uid, delta, &skip,
                    )
                    .await
                    {
                        Ok((req, Some((new_data, old_data)))) => Ok((
                            req,
                            Some(VaultConstraintData::Update(new_data, old_data)),
                        )),
                        Ok((req, None)) => Ok((req, None)),
                        Err(err_resp) => {
                            let msg = match &err_resp {
                                Response::Error { message, .. } => message.clone(),
                                Response::Ok { .. } => "vault update rejected".to_string(),
                            };
                            Err(VaultError::BadRequest(msg))
                        }
                    }
                }
                other => Ok((other, None)),
            }
        })
    }

    fn decrypt_response<'a>(
        &'a self,
        entity: &'a str,
        operation: DbOp,
        ownership: &'a OwnershipConfig,
        sender_uid: Option<&'a str>,
        response: &'a mut Response,
    ) -> VaultFuture<'a, ()> {
        Box::pin(async move {
            if !self.is_eligible(entity, ownership, sender_uid) {
                return;
            }
            let Some(uid) = sender_uid else {
                return;
            };
            let Some(key_bytes) = self.key_store.get(uid) else {
                return;
            };
            let Some(crypto) = VaultCrypto::from_key_bytes(&key_bytes) else {
                return;
            };

            let Response::Ok { data } = response else {
                return;
            };
            let skip = build_vault_skip_fields(entity, ownership);

            match operation {
                DbOp::Create | DbOp::Read | DbOp::Update => {
                    if let Some(id) = data.get("id").and_then(|v| v.as_str()).map(String::from) {
                        vault_decrypt_fields(&crypto, entity, &id, data, &skip);
                    }
                }
                DbOp::List => {
                    if let Some(items) = data.as_array_mut() {
                        for item in items {
                            if let Some(id) =
                                item.get("id").and_then(|v| v.as_str()).map(String::from)
                            {
                                vault_decrypt_fields(&crypto, entity, &id, item, &skip);
                            }
                        }
                    }
                }
                DbOp::Delete => {}
            }
        })
    }

    fn admin_enable<'a>(
        &'a self,
        db: &'a Database,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            if self.min_passphrase_length > 0 && passphrase.len() < self.min_passphrase_length {
                return Err(VaultError::PassphraseTooShort(self.min_passphrase_length));
            }
            self.check_rate_limit(canonical_id)?;

            let Some(identity) = read_entity_db(db, "_identities", canonical_id).await else {
                return Err(VaultError::BadRequest("identity not found".into()));
            };
            if identity
                .get("vault_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(VaultError::AlreadyEnabled);
            }

            let salt = VaultCrypto::generate_salt();
            let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);
            let check_token = crypto.create_check_token().map_err(|e| {
                error!(error = %e, "vault check token creation failed");
                VaultError::Internal("encryption failed".into())
            })?;

            let _fence = self.key_store.acquire_fence(canonical_id).await;
            self.key_store.set(canonical_id, key_bytes);

            let salt_b64 = BASE64.encode(salt);
            let migration_start = json!({
                "vault_enabled": true,
                "vault_salt": salt_b64,
                "vault_check": check_token,
                "vault_migration_status": "pending",
                "vault_migration_mode": "encrypt",
            });
            update_entity_db(db, "_identities", canonical_id, &migration_start).await;

            let batch = ops::batch_vault_operation_db(
                db,
                ownership,
                canonical_id,
                &crypto,
                ops::VaultMode::Encrypt,
            )
            .await;

            let migration_done = json!({"vault_migration_status": "complete"});
            update_entity_db(db, "_identities", canonical_id, &migration_done).await;

            let mut body = json!({"status": "enabled", "records_encrypted": batch.succeeded});
            if batch.failed > 0 || !batch.entities_skipped.is_empty() {
                body["failed"] = json!(batch.failed);
                body["warning"] = json!("some records could not be processed");
            }
            Ok(VaultAdminOutcome {
                body,
                session_update: Some(true),
            })
        })
    }

    fn admin_unlock<'a>(
        &'a self,
        db: &'a Database,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            self.check_rate_limit(canonical_id)?;

            let Some(identity) = read_entity_db(db, "_identities", canonical_id).await else {
                return Err(VaultError::BadRequest("identity not found".into()));
            };
            if !identity
                .get("vault_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(VaultError::NotEnabled);
            }

            let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault salt missing".into()));
            };
            let Ok(salt) = BASE64.decode(salt_b64) else {
                return Err(VaultError::Internal("invalid vault salt".into()));
            };
            let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault check token missing".into()));
            };

            let (crypto, key_bytes) = VaultCrypto::derive_with_raw_key(passphrase, &salt);
            if !crypto.verify_check_token(check_token) {
                return Err(VaultError::InvalidPassphrase);
            }

            let _fence = self.key_store.acquire_fence(canonical_id).await;
            self.key_store.set(canonical_id, key_bytes);

            let resume_result = ops::resume_pending_migration_db(
                db,
                ownership,
                &self.key_store,
                canonical_id,
                &crypto,
                &identity,
                passphrase,
            )
            .await;

            let status = if resume_result.as_ref().is_some_and(|r| r.mode == "decrypt") {
                "vault_disabled"
            } else {
                "unlocked"
            };
            let mut body = json!({"status": status});
            if let Some(migration) = resume_result {
                body["migration_resumed"] = json!(migration.mode);
                body["records_processed"] = json!(migration.succeeded);
                if migration.failed > 0 {
                    body["migration_failed"] = json!(migration.failed);
                }
            }
            Ok(VaultAdminOutcome {
                body,
                session_update: Some(true),
            })
        })
    }

    fn admin_lock<'a>(
        &'a self,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            self.key_store.remove(canonical_id);
            Ok(VaultAdminOutcome {
                body: json!({"status": "locked"}),
                session_update: Some(false),
            })
        })
    }

    fn admin_disable<'a>(
        &'a self,
        db: &'a Database,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            self.check_rate_limit(canonical_id)?;

            let Some(identity) = read_entity_db(db, "_identities", canonical_id).await else {
                return Err(VaultError::BadRequest("identity not found".into()));
            };
            if !identity
                .get("vault_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(VaultError::NotEnabled);
            }

            let Some(salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault salt missing".into()));
            };
            let Ok(salt) = BASE64.decode(salt_b64) else {
                return Err(VaultError::Internal("invalid vault salt".into()));
            };
            let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault check token missing".into()));
            };

            let crypto = VaultCrypto::derive(passphrase, &salt);
            if !crypto.verify_check_token(check_token) {
                return Err(VaultError::InvalidPassphrase);
            }

            let _fence = self.key_store.acquire_fence(canonical_id).await;
            self.key_store.remove(canonical_id);

            let migration_start = json!({
                "vault_migration_status": "pending",
                "vault_migration_mode": "decrypt",
            });
            update_entity_db(db, "_identities", canonical_id, &migration_start).await;

            let batch = ops::batch_vault_operation_db(
                db,
                ownership,
                canonical_id,
                &crypto,
                ops::VaultMode::Decrypt,
            )
            .await;

            let identity_update = json!({
                "vault_enabled": false,
                "vault_salt": null,
                "vault_check": null,
                "vault_migration_status": "complete",
                "vault_migration_mode": null,
            });
            update_entity_db(db, "_identities", canonical_id, &identity_update).await;

            let mut body = json!({"status": "disabled", "records_decrypted": batch.succeeded});
            if batch.failed > 0 || !batch.entities_skipped.is_empty() {
                body["failed"] = json!(batch.failed);
                body["warning"] = json!("some records could not be processed");
            }
            Ok(VaultAdminOutcome {
                body,
                session_update: Some(false),
            })
        })
    }

    fn admin_change<'a>(
        &'a self,
        db: &'a Database,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        old_passphrase: &'a str,
        new_passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            if self.min_passphrase_length > 0
                && new_passphrase.len() < self.min_passphrase_length
            {
                return Err(VaultError::PassphraseTooShort(self.min_passphrase_length));
            }
            self.check_rate_limit(canonical_id)?;

            let Some(identity) = read_entity_db(db, "_identities", canonical_id).await else {
                return Err(VaultError::BadRequest("identity not found".into()));
            };
            if !identity
                .get("vault_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(VaultError::NotEnabled);
            }

            let Some(old_salt_b64) = identity.get("vault_salt").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault salt missing".into()));
            };
            let Ok(old_salt) = BASE64.decode(old_salt_b64) else {
                return Err(VaultError::Internal("invalid vault salt".into()));
            };
            let Some(check_token) = identity.get("vault_check").and_then(|v| v.as_str()) else {
                return Err(VaultError::Internal("vault check token missing".into()));
            };

            let old_crypto = VaultCrypto::derive(old_passphrase, &old_salt);
            if !old_crypto.verify_check_token(check_token) {
                return Err(VaultError::InvalidPassphrase);
            }

            let new_salt = VaultCrypto::generate_salt();
            let (new_crypto, new_key_bytes) =
                VaultCrypto::derive_with_raw_key(new_passphrase, &new_salt);
            let new_check = new_crypto.create_check_token().map_err(|e| {
                error!(error = %e, "new vault check token creation failed");
                VaultError::Internal("encryption failed".into())
            })?;

            let _fence = self.key_store.acquire_fence(canonical_id).await;
            self.key_store.set(canonical_id, new_key_bytes);

            let new_salt_b64 = BASE64.encode(new_salt);
            let old_salt_b64_encoded = BASE64.encode(&old_salt);
            let migration_start = json!({
                "vault_salt": new_salt_b64,
                "vault_check": new_check,
                "vault_migration_status": "pending",
                "vault_migration_mode": "re_encrypt",
                "vault_old_check": check_token,
                "vault_old_salt": old_salt_b64_encoded,
            });
            update_entity_db(db, "_identities", canonical_id, &migration_start).await;

            let batch = ops::batch_vault_re_encrypt_db(
                db,
                ownership,
                canonical_id,
                &old_crypto,
                &new_crypto,
            )
            .await;

            let migration_done = json!({
                "vault_migration_status": "complete",
                "vault_migration_mode": null,
                "vault_old_check": null,
                "vault_old_salt": null,
            });
            update_entity_db(db, "_identities", canonical_id, &migration_done).await;

            let mut body = json!({"status": "changed", "records_re_encrypted": batch.succeeded});
            if batch.failed > 0 || !batch.entities_skipped.is_empty() {
                body["failed"] = json!(batch.failed);
                body["warning"] = json!("some records could not be processed");
            }
            Ok(VaultAdminOutcome {
                body,
                session_update: Some(true),
            })
        })
    }

    fn admin_status<'a>(
        &'a self,
        db: &'a Database,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        Box::pin(async move {
            let identity = read_entity_db(db, "_identities", canonical_id).await;
            let vault_enabled = identity
                .as_ref()
                .and_then(|i| i.get("vault_enabled"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let migration_pending = identity
                .as_ref()
                .and_then(|i| i.get("vault_migration_status"))
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == "pending");
            let unlocked = self.key_store.get(canonical_id).is_some();

            let mut body = json!({"vault_enabled": vault_enabled, "unlocked": unlocked});
            if migration_pending {
                body["migration_pending"] = json!(true);
            }
            Ok(VaultAdminOutcome {
                body,
                session_update: None,
            })
        })
    }
}

