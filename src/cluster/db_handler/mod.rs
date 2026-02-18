// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod binary_ops;
mod helpers;
mod json_ops;
#[cfg(test)]
mod tests;

use super::NodeId;
use super::db_topic::{DbTopicOperation, ParsedDbTopic};
use super::node_controller::{
    NodeController, PendingFkDeleteWork, PendingFkWork, PendingUniqueWork,
};
use super::transport::ClusterTransport;
use crate::types::{OwnershipConfig, ScopeConfig};
use std::sync::Arc;

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

pub struct DbRequestHandler {
    node_id: NodeId,
    ownership: Arc<OwnershipConfig>,
    scope_config: Arc<ScopeConfig>,
}

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            ownership: Arc::new(OwnershipConfig::default()),
            scope_config: Arc::new(ScopeConfig::default()),
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

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_publish<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        topic: &str,
        payload: &[u8],
        response_topic: Option<&str>,
        correlation_data: Option<&[u8]>,
        sender: Option<&str>,
        client_id: Option<&str>,
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
                let Some(resp_topic) = response_topic else {
                    return DbPublishResult::NoResponse;
                };
                match self
                    .handle_json_operation(
                        controller,
                        op,
                        payload,
                        resp_topic,
                        correlation_data,
                        sender,
                        client_id,
                    )
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

        let Some(resp_topic) = response_topic else {
            return DbPublishResult::NoResponse;
        };

        DbPublishResult::Response(DbPublishResponse {
            topic: resp_topic.to_string(),
            payload: response_payload,
            correlation_data: correlation_data.map(<[u8]>::to_vec),
        })
    }
}

impl std::fmt::Debug for DbRequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbRequestHandler")
            .field("node_id", &self.node_id)
            .field("ownership", &self.ownership)
            .field("scope_config", &self.scope_config)
            .finish()
    }
}
