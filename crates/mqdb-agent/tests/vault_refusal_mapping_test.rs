// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;
mod running_agent;

use common::next_test_port;
use mqdb_agent::vault_backend::{
    DbAccess, EncryptRequestOutput, NoopVaultBackend, VaultAdminOutcome, VaultBackend, VaultError,
    VaultFuture, VaultResult,
};
use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::config::DatabaseConfig;
use mqdb_core::protocol::DbOp;
use mqdb_core::transport::{Request, Response};
use mqdb_core::types::OwnershipConfig;
use mqtt5::QoS;
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use running_agent::RunningAgent;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const REFUSAL: &str = "the ownership field 'userId' cannot be changed";

struct RefusingVault {
    inner: NoopVaultBackend,
}

impl VaultBackend for RefusingVault {
    fn is_eligible(
        &self,
        entity: &str,
        ownership: &OwnershipConfig,
        canonical_id: Option<&str>,
    ) -> bool {
        !self.inner.is_eligible(entity, ownership, canonical_id)
    }

    fn await_read_fence<'a>(&'a self, canonical_id: &'a str) -> VaultFuture<'a, ()> {
        self.inner.await_read_fence(canonical_id)
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
            self.inner
                .encrypt_request(db, entity, ownership, sender_uid, request)
                .await?;
            Err(VaultError::Forbidden(REFUSAL.to_string()))
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
        self.inner
            .decrypt_response(entity, operation, ownership, sender_uid, response)
    }

    fn admin_enable<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner
            .admin_enable(db, ownership, canonical_id, passphrase)
    }

    fn admin_unlock<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner
            .admin_unlock(db, ownership, canonical_id, passphrase)
    }

    fn admin_lock<'a>(
        &'a self,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner.admin_lock(canonical_id)
    }

    fn admin_disable<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner
            .admin_disable(db, ownership, canonical_id, passphrase)
    }

    fn admin_change<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        ownership: &'a OwnershipConfig,
        canonical_id: &'a str,
        old_passphrase: &'a str,
        new_passphrase: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner
            .admin_change(db, ownership, canonical_id, old_passphrase, new_passphrase)
    }

    fn admin_status<'a>(
        &'a self,
        db: &'a dyn DbAccess,
        canonical_id: &'a str,
    ) -> VaultFuture<'a, VaultResult<VaultAdminOutcome>> {
        self.inner.admin_status(db, canonical_id)
    }
}

#[tokio::test]
async fn a_vault_refusal_reaches_the_mqtt_client_as_forbidden_with_its_message() {
    let tmp = TempDir::new().unwrap();
    let config = DatabaseConfig::new(tmp.path().join("agent")).without_background_tasks();
    let db = Database::open_with_config(config).await.unwrap();
    let agent = MqdbAgent::new(db)
        .with_anonymous(true)
        .with_vault_backend(Arc::new(RefusingVault {
            inner: NoopVaultBackend,
        }));
    let running = RunningAgent::start(agent, next_test_port(), tmp).await;

    let client = MqttClient::new("vault-refusal-client");
    client
        .connect(&format!("mqtt://127.0.0.1:{}", running.port))
        .await
        .unwrap();
    let (tx, rx) = flume::bounded::<Vec<u8>>(1);
    client
        .subscribe("resp/vault-refusal", move |msg| {
            let _ = tx.try_send(msg.payload.clone());
        })
        .await
        .unwrap();
    let options = PublishOptions {
        qos: QoS::AtLeastOnce,
        properties: PublishProperties {
            response_topic: Some("resp/vault-refusal".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    client
        .publish_with_options(
            "$DB/notes/n1/update",
            serde_json::to_vec(&json!({"userId": "bob"})).unwrap(),
            options,
        )
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("no reply to the update")
        .unwrap();
    let answer: Value = serde_json::from_slice(&reply).unwrap();

    assert_eq!(answer["status"], "error", "{answer}");
    assert_eq!(answer["code"], 403, "{answer}");
    assert_eq!(answer["message"], REFUSAL, "{answer}");

    client.disconnect().await.unwrap();
    running.stop().await;
}
