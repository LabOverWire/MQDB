// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_agent::Database;
use mqdb_agent::vault_backend::{VaultBackend, VaultError};
use mqdb_core::protocol::DbOp;
use mqdb_core::transport::{Request, Response};
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_vault::{VaultBackendConfig, VaultBackendImpl};
use serde_json::json;

async fn setup(
    passphrase_min: usize,
) -> (
    tempfile::TempDir,
    Database,
    OwnershipConfig,
    VaultBackendImpl,
) {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let db = Database::open_without_background_tasks(tmpdir.path())
        .await
        .expect("open db");
    let mut ownership = OwnershipConfig::default();
    ownership
        .entity_owner_fields
        .insert("notes".to_string(), "userId".to_string());
    let backend = VaultBackendImpl::new(VaultBackendConfig {
        min_passphrase_length: passphrase_min,
        unlock_rate_limit: u32::MAX,
        key_store: None,
    });
    (tmpdir, db, ownership, backend)
}

async fn seed_identity(db: &Database, canonical_id: &str) {
    let scope = ScopeConfig::default();
    db.create(
        "_identities".to_string(),
        json!({"id": canonical_id, "primary_email": format!("{canonical_id}@example.test")}),
        None,
        None,
        None,
        &scope,
    )
    .await
    .expect("seed identity");
}

#[tokio::test]
async fn noop_admin_returns_unavailable() {
    use mqdb_agent::vault_backend::NoopVaultBackend;
    let tmpdir = tempfile::tempdir().unwrap();
    let db = Database::open_without_background_tasks(tmpdir.path())
        .await
        .unwrap();
    let ownership = OwnershipConfig::default();
    let backend = NoopVaultBackend;

    let result = backend.admin_enable(&db, &ownership, "u1", "pw").await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    let result = backend.admin_unlock(&db, &ownership, "u1", "pw").await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    let result = backend.admin_lock("u1").await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    let result = backend.admin_disable(&db, &ownership, "u1", "pw").await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    let result = backend
        .admin_change(&db, &ownership, "u1", "old", "new")
        .await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    let result = backend.admin_status(&db, "u1").await;
    assert!(matches!(result, Err(VaultError::Unavailable)));

    assert!(!backend.is_eligible("notes", &ownership, Some("u1")));
}

#[tokio::test]
async fn noop_hot_path_is_passthrough() {
    use mqdb_agent::vault_backend::NoopVaultBackend;
    let tmpdir = tempfile::tempdir().unwrap();
    let db = Database::open_without_background_tasks(tmpdir.path())
        .await
        .unwrap();
    let ownership = OwnershipConfig::default();
    let backend = NoopVaultBackend;

    let req = Request::Create {
        entity: "notes".to_string(),
        data: json!({"id": "n1", "title": "plain"}),
    };
    let (out_req, constraint) = backend
        .encrypt_request(&db, "notes", &ownership, Some("u1"), req)
        .await
        .expect("noop passes through");
    assert!(constraint.is_none());
    let Request::Create { data, .. } = out_req else {
        panic!("expected Create");
    };
    assert_eq!(data["title"], "plain");
}

#[tokio::test]
async fn admin_enable_rejects_short_passphrase() {
    let (_td, db, ownership, backend) = setup(12).await;
    seed_identity(&db, "u1").await;
    let result = backend.admin_enable(&db, &ownership, "u1", "short").await;
    assert!(matches!(result, Err(VaultError::PassphraseTooShort(12))));
}

#[tokio::test]
async fn admin_enable_missing_identity_returns_not_found() {
    let (_td, db, ownership, backend) = setup(0).await;
    let result = backend.admin_enable(&db, &ownership, "ghost", "pw").await;
    assert!(matches!(result, Err(VaultError::NotFound(_))));
}

#[tokio::test]
async fn admin_unlock_wrong_passphrase_returns_invalid() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "correct")
        .await
        .expect("enable");

    let result = backend.admin_unlock(&db, &ownership, "u1", "wrong").await;
    assert!(matches!(result, Err(VaultError::InvalidPassphrase)));
}

#[tokio::test]
async fn admin_enable_rejects_when_already_enabled() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "pw")
        .await
        .expect("first enable");
    let result = backend.admin_enable(&db, &ownership, "u1", "pw").await;
    assert!(matches!(result, Err(VaultError::AlreadyEnabled)));
}

#[tokio::test]
async fn admin_status_reports_vault_state() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;

    let outcome = backend.admin_status(&db, "u1").await.expect("status");
    assert_eq!(outcome.body["vault_enabled"], false);
    assert_eq!(outcome.body["unlocked"], false);

    backend
        .admin_enable(&db, &ownership, "u1", "pw")
        .await
        .expect("enable");
    let outcome = backend.admin_status(&db, "u1").await.expect("status");
    assert_eq!(outcome.body["vault_enabled"], true);
    assert_eq!(outcome.body["unlocked"], true);

    backend.admin_lock("u1").await.expect("lock");
    let outcome = backend.admin_status(&db, "u1").await.expect("status");
    assert_eq!(outcome.body["vault_enabled"], true);
    assert_eq!(outcome.body["unlocked"], false);
}

#[tokio::test]
async fn hot_path_encrypts_create_and_decrypts_read() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "pw")
        .await
        .expect("enable");

    let req = Request::Create {
        entity: "notes".to_string(),
        data: json!({"id": "note-1", "userId": "u1", "title": "sensitive", "body": "secret"}),
    };
    let (encrypted_req, _vc) = backend
        .encrypt_request(&db, "notes", &ownership, Some("u1"), req)
        .await
        .expect("encrypt_request");

    let Request::Create {
        data: encrypted_data,
        ..
    } = encrypted_req
    else {
        panic!("expected Create");
    };
    assert_eq!(encrypted_data["id"], "note-1");
    assert_eq!(encrypted_data["userId"], "u1");
    assert_ne!(
        encrypted_data["title"].as_str().expect("title"),
        "sensitive"
    );
    assert_ne!(encrypted_data["body"].as_str().expect("body"), "secret");

    let scope = ScopeConfig::default();
    db.create(
        "notes".to_string(),
        encrypted_data.clone(),
        None,
        None,
        None,
        &scope,
    )
    .await
    .expect("store encrypted");

    let stored_on_disk = db
        .read("notes".to_string(), "note-1".to_string(), vec![], None)
        .await
        .expect("read raw");
    assert_ne!(
        stored_on_disk["title"].as_str().expect("title"),
        "sensitive",
        "data at rest must stay encrypted"
    );

    let mut response = Response::Ok {
        data: stored_on_disk,
    };
    backend
        .decrypt_response("notes", DbOp::Read, &ownership, Some("u1"), &mut response)
        .await;
    let Response::Ok { data: decrypted } = response else {
        panic!("expected Ok");
    };
    assert_eq!(decrypted["title"], "sensitive");
    assert_eq!(decrypted["body"], "secret");
}

#[tokio::test]
async fn hot_path_skips_when_locked() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "pw")
        .await
        .expect("enable");
    backend.admin_lock("u1").await.expect("lock");

    let req = Request::Create {
        entity: "notes".to_string(),
        data: json!({"id": "note-1", "userId": "u1", "title": "plain"}),
    };
    let (out_req, _vc) = backend
        .encrypt_request(&db, "notes", &ownership, Some("u1"), req)
        .await
        .expect("encrypt");
    let Request::Create { data, .. } = out_req else {
        panic!();
    };
    assert_eq!(
        data["title"], "plain",
        "locked vault should not encrypt (passthrough)"
    );
}

#[tokio::test]
async fn admin_change_rotates_keys() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "old-pw")
        .await
        .expect("enable");

    let result = backend
        .admin_change(&db, &ownership, "u1", "wrong-old", "new-pw")
        .await;
    assert!(matches!(result, Err(VaultError::InvalidPassphrase)));

    backend
        .admin_change(&db, &ownership, "u1", "old-pw", "new-pw")
        .await
        .expect("rotate");

    let result = backend.admin_unlock(&db, &ownership, "u1", "old-pw").await;
    assert!(matches!(result, Err(VaultError::InvalidPassphrase)));
    backend
        .admin_unlock(&db, &ownership, "u1", "new-pw")
        .await
        .expect("unlock with new");
}

#[tokio::test]
async fn admin_unlock_skips_re_encrypt_when_records_already_rotated() {
    use mqdb_agent::vault_backend::DbAccess;

    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "old-pw")
        .await
        .expect("enable");

    let req = Request::Create {
        entity: "notes".to_string(),
        data: json!({"id": "note-1", "userId": "u1", "title": "sensitive", "body": "secret"}),
    };
    let (encrypted_req, _vc) = backend
        .encrypt_request(&db, "notes", &ownership, Some("u1"), req)
        .await
        .expect("encrypt_request");
    let Request::Create {
        data: encrypted_data,
        ..
    } = encrypted_req
    else {
        panic!("expected Create");
    };
    let scope = ScopeConfig::default();
    db.create(
        "notes".to_string(),
        encrypted_data,
        None,
        None,
        None,
        &scope,
    )
    .await
    .expect("store encrypted");

    let identity_before = DbAccess::read_entity(&db, "_identities", "u1")
        .await
        .expect("read identity")
        .expect("identity exists");
    let old_salt = identity_before["vault_salt"]
        .as_str()
        .expect("old salt")
        .to_string();
    let old_check = identity_before["vault_check"]
        .as_str()
        .expect("old check")
        .to_string();

    backend
        .admin_change(&db, &ownership, "u1", "old-pw", "new-pw")
        .await
        .expect("rotate");

    DbAccess::update_entity(
        &db,
        "_identities",
        "u1",
        json!({
            "vault_migration_status": "pending",
            "vault_migration_mode": "re_encrypt",
            "vault_old_check": old_check,
            "vault_old_salt": old_salt,
        }),
    )
    .await
    .expect("inject half-finalized markers");

    backend
        .admin_unlock(&db, &ownership, "u1", "new-pw")
        .await
        .expect("unlock with new");

    let identity_after = DbAccess::read_entity(&db, "_identities", "u1")
        .await
        .expect("read identity")
        .expect("identity exists");
    assert_ne!(
        identity_after
            .get("vault_migration_status")
            .and_then(|v| v.as_str()),
        Some("pending"),
        "resume must clear the pending marker"
    );

    let stored = db
        .read("notes".to_string(), "note-1".to_string(), vec![], None)
        .await
        .expect("read note");
    let mut response = Response::Ok { data: stored };
    backend
        .decrypt_response("notes", DbOp::Read, &ownership, Some("u1"), &mut response)
        .await;
    let Response::Ok { data: decrypted } = response else {
        panic!("expected Ok");
    };
    assert_eq!(
        decrypted["title"], "sensitive",
        "record must not be double-encrypted by a spurious re_encrypt resume"
    );
    assert_eq!(decrypted["body"], "secret");
}

#[tokio::test]
async fn admin_unlock_resume_strands_unrotated_records_after_mid_batch_crash() {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use mqdb_agent::vault_backend::DbAccess;
    use mqdb_vault::VaultCrypto;

    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "old-pw")
        .await
        .expect("enable");

    let identity = DbAccess::read_entity(&db, "_identities", "u1")
        .await
        .expect("read identity")
        .expect("identity exists");
    let old_salt_b64 = identity["vault_salt"]
        .as_str()
        .expect("old salt")
        .to_string();
    let old_check = identity["vault_check"]
        .as_str()
        .expect("old check")
        .to_string();
    let old_salt = BASE64.decode(&old_salt_b64).expect("decode old salt");
    let old_crypto = VaultCrypto::derive("old-pw", &old_salt);

    let new_salt = VaultCrypto::generate_salt();
    let new_crypto = VaultCrypto::derive("new-pw", &new_salt);
    let new_check = new_crypto.create_check_token().expect("new check");
    let new_salt_b64 = BASE64.encode(new_salt);

    let scope = ScopeConfig::default();

    let mut rotated =
        json!({"id": "note-rotated", "userId": "u1", "title": "rotated", "body": "after"});
    new_crypto.encrypt_record("notes", "note-rotated", &mut rotated, &["id", "userId"]);
    db.create("notes".to_string(), rotated, None, None, None, &scope)
        .await
        .expect("store rotated");

    let mut stranded =
        json!({"id": "note-stranded", "userId": "u1", "title": "stranded", "body": "before"});
    old_crypto.encrypt_record("notes", "note-stranded", &mut stranded, &["id", "userId"]);
    db.create("notes".to_string(), stranded, None, None, None, &scope)
        .await
        .expect("store stranded");

    DbAccess::update_entity(
        &db,
        "_identities",
        "u1",
        json!({
            "vault_salt": new_salt_b64,
            "vault_check": new_check,
            "vault_migration_status": "pending",
            "vault_migration_mode": "re_encrypt",
            "vault_old_check": old_check,
            "vault_old_salt": old_salt_b64,
        }),
    )
    .await
    .expect("inject mid-batch markers");

    backend
        .admin_unlock(&db, &ownership, "u1", "new-pw")
        .await
        .expect("unlock with new");

    let identity_after = DbAccess::read_entity(&db, "_identities", "u1")
        .await
        .expect("read identity")
        .expect("identity exists");
    assert_ne!(
        identity_after
            .get("vault_migration_status")
            .and_then(|v| v.as_str()),
        Some("pending"),
        "resume must clear the pending marker"
    );

    let rotated_stored = db
        .read(
            "notes".to_string(),
            "note-rotated".to_string(),
            vec![],
            None,
        )
        .await
        .expect("read rotated");
    let mut rotated_resp = Response::Ok {
        data: rotated_stored,
    };
    backend
        .decrypt_response(
            "notes",
            DbOp::Read,
            &ownership,
            Some("u1"),
            &mut rotated_resp,
        )
        .await;
    let Response::Ok { data: rotated_dec } = rotated_resp else {
        panic!("expected Ok");
    };
    assert_eq!(
        rotated_dec["title"], "rotated",
        "already-rotated record must stay readable under the new key, not be double-encrypted"
    );

    let stranded_stored = db
        .read(
            "notes".to_string(),
            "note-stranded".to_string(),
            vec![],
            None,
        )
        .await
        .expect("read stranded");
    let mut stranded_resp = Response::Ok {
        data: stranded_stored,
    };
    backend
        .decrypt_response(
            "notes",
            DbOp::Read,
            &ownership,
            Some("u1"),
            &mut stranded_resp,
        )
        .await;
    let Response::Ok { data: stranded_dec } = stranded_resp else {
        panic!("expected Ok");
    };
    assert_ne!(
        stranded_dec["title"], "stranded",
        "records left under the old key by a mid-batch crash are unrecoverable after resume"
    );
}

#[tokio::test]
async fn admin_disable_clears_vault_state() {
    let (_td, db, ownership, backend) = setup(0).await;
    seed_identity(&db, "u1").await;
    backend
        .admin_enable(&db, &ownership, "u1", "pw")
        .await
        .expect("enable");

    let outcome = backend
        .admin_disable(&db, &ownership, "u1", "pw")
        .await
        .expect("disable");
    assert_eq!(outcome.session_update, Some(false));

    let outcome = backend.admin_status(&db, "u1").await.expect("status");
    assert_eq!(outcome.body["vault_enabled"], false);
    assert_eq!(outcome.body["unlocked"], false);
}
