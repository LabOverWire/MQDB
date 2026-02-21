// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::{
    BatchReadRequest, BatchReadResponse, JsonDbOp, JsonDbRequest, QueryRequest, QueryResponse,
    QueryStatus,
};
use super::query_coordinator::QueryCoordinator;
use super::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId, PendingScatterRequest,
};
use crate::Database;
use crate::types::MAX_LIST_RESULTS;

impl<T: ClusterTransport> NodeController<T> {
    pub fn query_coordinator(&self) -> &QueryCoordinator {
        &self.query_coordinator
    }

    pub fn query_coordinator_mut(&mut self) -> &mut QueryCoordinator {
        &mut self.query_coordinator
    }

    pub fn handle_query_request(
        &self,
        partition: PartitionId,
        request: &QueryRequest,
    ) -> QueryResponse {
        if self.partition_map.primary(partition) != Some(self.node_id) {
            return QueryResponse::error(request.query_id, partition, QueryStatus::NotPrimary);
        }

        let results = self.stores.query_entity(
            &request.entity,
            request.filter.as_deref(),
            request.limit,
            request.cursor.as_deref(),
        );

        match results {
            Ok((data, has_more, cursor)) => {
                QueryResponse::ok(request.query_id, partition, data, has_more, cursor)
            }
            Err(_) => QueryResponse::error(request.query_id, partition, QueryStatus::Error),
        }
    }

    pub fn handle_batch_read_request(&self, request: &BatchReadRequest) -> BatchReadResponse {
        let results = request
            .ids
            .iter()
            .map(|id| {
                let data = self.stores.get_entity(&request.entity, id);
                (id.clone(), data)
            })
            .collect();

        BatchReadResponse::new(request.request_id, request.partition, results)
    }

    pub fn check_query_complete(&mut self, query_id: u64) -> bool {
        !self.query_coordinator.has_pending(query_id)
    }

    pub fn check_query_timeouts(&mut self, now: u64) -> Vec<super::query_coordinator::QueryResult> {
        self.query_coordinator.check_timeouts(now)
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn start_scatter_list_query(
        &mut self,
        entity: &str,
        payload: &[u8],
        client_response_topic: String,
        filters: Vec<crate::Filter>,
        sorts: Vec<crate::SortOrder>,
        projection: Option<Vec<String>>,
    ) -> bool {
        let alive_nodes = self.heartbeat.alive_nodes();
        let remote_nodes: Vec<NodeId> = alive_nodes
            .into_iter()
            .filter(|&n| n != self.node_id)
            .collect();

        if remote_nodes.is_empty() {
            return false;
        }

        #[allow(clippy::cast_possible_truncation)]
        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);

        let local_results = self.handle_json_list_local(entity, payload);
        let local_items: Vec<serde_json::Value> =
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&local_results) {
                parsed
                    .get("data")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

        let pending = PendingScatterRequest {
            expected_count: remote_nodes.len(),
            received: local_items,
            client_response_topic,
            created_at_ms: self.current_time,
            filters,
            sorts,
            projection,
        };
        self.pending_scatter_requests.insert(request_id, pending);

        let scatter_response_topic = format!("_mqdb/scatter/{}/{request_id}", self.node_id.get());

        for &target_node in &remote_nodes {
            let request = JsonDbRequest::new(
                request_id,
                JsonDbOp::List,
                entity.to_string(),
                None,
                payload.to_vec(),
                scatter_response_topic.clone(),
                None,
                None,
            );

            let msg = ClusterMessage::JsonDbRequest {
                partition: PartitionId::ZERO,
                request,
            };

            if let Err(e) = self.transport.send(target_node, msg).await {
                tracing::warn!(?target_node, ?e, "failed to send scatter LIST request");
            }
        }

        tracing::debug!(
            request_id,
            entity,
            remote_count = remote_nodes.len(),
            "started scatter LIST query"
        );

        true
    }

    pub async fn handle_scatter_list_response(
        &mut self,
        request_id: u64,
        mut items: Vec<serde_json::Value>,
    ) {
        let Some(pending) = self.pending_scatter_requests.get_mut(&request_id) else {
            tracing::debug!(request_id, "scatter response for unknown request");
            return;
        };

        items.truncate(MAX_LIST_RESULTS);
        let remaining_capacity = MAX_LIST_RESULTS.saturating_sub(pending.received.len());
        pending
            .received
            .extend(items.into_iter().take(remaining_capacity));
        pending.expected_count = pending.expected_count.saturating_sub(1);

        if pending.expected_count == 0
            && let Some(completed) = self.pending_scatter_requests.remove(&request_id)
        {
            let mut seen_ids = std::collections::HashSet::new();
            let deduped: Vec<serde_json::Value> = completed
                .received
                .into_iter()
                .filter(|item| {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        seen_ids.insert(id.to_string())
                    } else {
                        true
                    }
                })
                .collect();

            let mut filtered: Vec<serde_json::Value> = deduped
                .into_iter()
                .filter(|item| {
                    if let Some(data) = item.get("data") {
                        Self::matches_filters(data, &completed.filters)
                    } else {
                        false
                    }
                })
                .collect();

            Self::sort_scatter_results(&mut filtered, &completed.sorts);
            filtered.truncate(MAX_LIST_RESULTS);

            let projected = if let Some(ref fields) = completed.projection {
                filtered
                    .into_iter()
                    .map(|mut item| {
                        if let Some(data) = item.get("data").cloned()
                            && let Some(obj) = item.as_object_mut()
                        {
                            obj.insert("data".to_string(), Database::project_fields(data, fields));
                        }
                        item
                    })
                    .collect()
            } else {
                filtered
            };

            let result = serde_json::json!({
                "status": "ok",
                "data": projected
            });
            let payload = serde_json::to_vec(&result).unwrap_or_default();

            self.transport
                .queue_local_publish(completed.client_response_topic, payload, 0)
                .await;
        }
    }

    fn sort_scatter_results(results: &mut [serde_json::Value], sorts: &[crate::SortOrder]) {
        if sorts.is_empty() {
            return;
        }

        results.sort_by(|a, b| {
            let a_data = a.get("data");
            let b_data = b.get("data");

            for order in sorts {
                let a_val = a_data.and_then(|d| d.get(&order.field));
                let b_val = b_data.and_then(|d| d.get(&order.field));

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => Self::compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = match order.direction {
                    crate::SortDirection::Asc => cmp,
                    crate::SortDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        use serde_json::Value;
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            _ => std::cmp::Ordering::Equal,
        }
    }
}
