// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod binary_ops;
pub(crate) mod helpers;
mod json_ops;
#[cfg(test)]
mod tests;

use super::NodeId;
use super::db_topic::{DbTopicOperation, ParsedDbTopic};
use super::node_controller::{
    NodeController, PendingFkDeleteWork, PendingFkWork, PendingUniqueWork,
};
use super::transport::ClusterTransport;
use mqdb_core::types::{OwnershipConfig, ScopeConfig};
use mqdb_vault::VaultKeyStore;
use std::sync::Arc;

pub struct MqttRequestContext<'a> {
    pub response_topic: Option<&'a str>,
    pub correlation_data: Option<&'a [u8]>,
    pub sender: Option<&'a str>,
    pub client_id: Option<&'a str>,
}

pub struct DbPublishResponse {
    pub topic: String,
    pub payload: Vec<u8>,
    pub correlation_data: Option<Vec<u8>>,
}

pub enum DbPublishResult {
    Response(DbPublishResponse),
    NoResponse,
    PendingUniqueCheck(Box<PendingUniqueWork>),
    PendingFkCheck(Box<PendingFkWork>),
    PendingFkDelete(Box<PendingFkDeleteWork>),
}

impl DbPublishResult {
    #[cfg(test)]
    fn is_some(&self) -> bool {
        matches!(self, Self::Response(_))
    }

    #[cfg(test)]
    fn is_none(&self) -> bool {
        matches!(self, Self::NoResponse)
    }

    #[cfg(test)]
    fn unwrap(self) -> DbPublishResponse {
        match self {
            Self::Response(r) => r,
            Self::NoResponse => panic!("called unwrap on NoResponse"),
            Self::PendingUniqueCheck(_) => panic!("called unwrap on PendingUniqueCheck"),
            Self::PendingFkCheck(_) => panic!("called unwrap on PendingFkCheck"),
            Self::PendingFkDelete(_) => panic!("called unwrap on PendingFkDelete"),
        }
    }
}

pub enum FkCheckCompletion {
    Done(Option<DbPublishResponse>),
    NeedUniqueCheck(Box<PendingUniqueWork>),
}

pub struct DbRequestHandler {
    node_id: NodeId,
    ownership: Arc<OwnershipConfig>,
    scope_config: Arc<ScopeConfig>,
    vault_key_store: Arc<VaultKeyStore>,
}

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            ownership: Arc::new(OwnershipConfig::default()),
            scope_config: Arc::new(ScopeConfig::default()),
            vault_key_store: Arc::new(VaultKeyStore::new()),
        }
    }

    #[must_use]
    pub fn with_ownership(mut self, ownership: Arc<OwnershipConfig>) -> Self {
        self.ownership = ownership;
        self
    }

    #[must_use]
    pub fn with_scope_config(mut self, scope_config: Arc<ScopeConfig>) -> Self {
        self.scope_config = scope_config;
        self
    }

    #[must_use]
    pub fn with_vault_key_store(mut self, store: Arc<VaultKeyStore>) -> Self {
        self.vault_key_store = store;
        self
    }

    pub async fn handle_publish<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        topic: &str,
        payload: &[u8],
        ctx: &MqttRequestContext<'_>,
    ) -> DbPublishResult {
        let Some(parsed) = ParsedDbTopic::parse(topic) else {
            return DbPublishResult::NoResponse;
        };

        let response_payload = match parsed.operation {
            DbTopicOperation::QueryRequest { .. } | DbTopicOperation::QueryResponse { .. } => {
                return DbPublishResult::NoResponse;
            }
            ref op if op.is_binary() => {
                match self
                    .handle_binary_operation(controller, &parsed, payload)
                    .await
                {
                    Some(payload) => payload,
                    None => return DbPublishResult::NoResponse,
                }
            }
            ref op => {
                if ctx.response_topic.is_none() {
                    return DbPublishResult::NoResponse;
                }
                match self
                    .handle_json_operation(controller, op, payload, ctx)
                    .await
                {
                    json_ops::JsonOpResult::Response(payload) => payload,
                    json_ops::JsonOpResult::NoResponse => return DbPublishResult::NoResponse,
                    json_ops::JsonOpResult::PendingUniqueCheck(pending) => {
                        return DbPublishResult::PendingUniqueCheck(pending);
                    }
                    json_ops::JsonOpResult::PendingFkCheck(pending) => {
                        return DbPublishResult::PendingFkCheck(pending);
                    }
                    json_ops::JsonOpResult::PendingFkDelete(pending) => {
                        return DbPublishResult::PendingFkDelete(pending);
                    }
                }
            }
        };

        let Some(resp_topic) = ctx.response_topic else {
            return DbPublishResult::NoResponse;
        };

        DbPublishResult::Response(DbPublishResponse {
            topic: resp_topic.to_string(),
            payload: response_payload,
            correlation_data: ctx.correlation_data.map(<[u8]>::to_vec),
        })
    }
}

impl DbRequestHandler {
    fn resolve_vault_crypto(
        &self,
        entity: &str,
        sender: Option<&str>,
    ) -> Option<mqdb_vault::VaultCrypto> {
        let uid = sender
            .filter(|_| mqdb_vault::transform::is_vault_eligible(entity, &self.ownership))?;
        let key_bytes = self.vault_key_store.get(uid)?;
        mqdb_vault::VaultCrypto::from_key_bytes(&key_bytes)
    }

    fn vault_encrypt_if_needed(
        &self,
        vault_crypto: Option<&mqdb_vault::VaultCrypto>,
        entity: &str,
        id: &str,
        mut data: serde_json::Value,
    ) -> serde_json::Value {
        if let Some(crypto) = vault_crypto {
            let skip =
                mqdb_vault::transform::build_vault_skip_fields(entity, &self.ownership);
            mqdb_vault::transform::vault_encrypt_fields(crypto, entity, id, &mut data, &skip);
        }
        data
    }

    fn vault_decrypt_response_payload(
        &self,
        payload: Vec<u8>,
        entity: &str,
        op: &str,
        sender: Option<&str>,
    ) -> Vec<u8> {
        let Some(crypto) = self.resolve_vault_crypto(entity, sender) else {
            return payload;
        };
        let skip = mqdb_vault::transform::build_vault_skip_fields(entity, &self.ownership);
        let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&payload) else {
            return payload;
        };

        match op {
            "create" | "read" | "update" => {
                if let Some(data) = parsed.get_mut("data") {
                    let id = data.get("id").and_then(|v| v.as_str()).map(String::from);
                    if let Some(id) = id {
                        mqdb_vault::transform::vault_decrypt_fields(
                            &crypto, entity, &id, data, &skip,
                        );
                    }
                }
            }
            _ => {}
        }
        serde_json::to_vec(&parsed).unwrap_or(payload)
    }
}

impl std::fmt::Debug for DbRequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbRequestHandler")
            .field("node_id", &self.node_id)
            .field("ownership", &self.ownership)
            .field("scope_config", &self.scope_config)
            .finish_non_exhaustive()
    }
}
