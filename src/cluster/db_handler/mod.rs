mod binary_ops;
mod helpers;
mod json_ops;
#[cfg(test)]
mod tests;

use super::NodeId;
use super::db_topic::{DbTopicOperation, ParsedDbTopic};
use super::node_controller::NodeController;
use super::transport::ClusterTransport;

pub struct DbPublishResponse {
    pub topic: String,
    pub payload: Vec<u8>,
    pub correlation_data: Option<Vec<u8>>,
}

pub struct DbRequestHandler {
    node_id: NodeId,
}

#[allow(clippy::unused_self)]
impl DbRequestHandler {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub async fn handle_publish<T: ClusterTransport>(
        &self,
        controller: &mut NodeController<T>,
        topic: &str,
        payload: &[u8],
        response_topic: Option<&str>,
        correlation_data: Option<&[u8]>,
    ) -> Option<DbPublishResponse> {
        let parsed = ParsedDbTopic::parse(topic)?;

        let response_payload = match parsed.operation {
            DbTopicOperation::QueryRequest { .. } | DbTopicOperation::QueryResponse { .. } => {
                return None;
            }
            ref op if op.is_binary() => {
                self.handle_binary_operation(controller, &parsed, payload)
                    .await?
            }
            ref op => {
                self.handle_json_operation(
                    controller,
                    op,
                    payload,
                    response_topic?,
                    correlation_data,
                )
                .await?
            }
        };

        let resp_topic = response_topic?;
        Some(DbPublishResponse {
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
            .finish()
    }
}
