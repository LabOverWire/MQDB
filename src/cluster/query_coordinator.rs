// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;

use super::cursor::{PartitionCursor, ScatterCursor};
use super::types::NUM_PARTITIONS;
use super::{NodeId, PartitionId, QueryRequest, QueryResponse};

const DEFAULT_QUERY_TIMEOUT_MS: u32 = 10_000;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub query_id: u64,
    pub results: Vec<Vec<u8>>,
    pub has_more: bool,
    pub cursor: Option<ScatterCursor>,
    pub partial: bool,
    pub missing_partitions: Vec<PartitionId>,
}

impl QueryResult {
    #[must_use]
    pub fn empty(query_id: u64) -> Self {
        Self {
            query_id,
            results: Vec::new(),
            has_more: false,
            cursor: None,
            partial: false,
            missing_partitions: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct PendingQuery {
    query_id: u64,
    timeout_ms: u32,
    started_at: u64,
    expected_partitions: Vec<PartitionId>,
    responses: HashMap<PartitionId, QueryResponse>,
}

impl PendingQuery {
    fn new(
        query_id: u64,
        timeout_ms: u32,
        expected_partitions: Vec<PartitionId>,
        now: u64,
    ) -> Self {
        Self {
            query_id,
            timeout_ms,
            started_at: now,
            expected_partitions,
            responses: HashMap::new(),
        }
    }

    fn is_complete(&self) -> bool {
        self.expected_partitions
            .iter()
            .all(|p| self.responses.contains_key(p))
    }

    fn is_timed_out(&self, now: u64) -> bool {
        now.saturating_sub(self.started_at) >= u64::from(self.timeout_ms)
    }

    fn missing_partitions(&self) -> Vec<PartitionId> {
        self.expected_partitions
            .iter()
            .filter(|p| !self.responses.contains_key(p))
            .copied()
            .collect()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn build_result(&self) -> QueryResult {
        let mut all_results = Vec::new();
        let mut has_more = false;
        let mut cursor = ScatterCursor::new();

        for partition in &self.expected_partitions {
            if let Some(resp) = self.responses.get(partition) {
                if resp.status.is_ok() && !resp.results.is_empty() {
                    all_results.push(resp.results.clone());
                }
                if resp.has_more {
                    has_more = true;
                }
                if let Some(c) = &resp.cursor
                    && let Some(pc) = PartitionCursor::from_bytes(c)
                {
                    cursor.add(pc);
                }
            }
        }

        let missing = self.missing_partitions();
        let partial = !missing.is_empty();

        QueryResult {
            query_id: self.query_id,
            results: all_results,
            has_more,
            cursor: if cursor.is_empty() {
                None
            } else {
                Some(cursor)
            },
            partial,
            missing_partitions: missing,
        }
    }
}

#[derive(Debug)]
pub struct QueryCoordinator {
    node_id: NodeId,
    pending_queries: HashMap<u64, PendingQuery>,
    next_query_id: u64,
}

impl QueryCoordinator {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            pending_queries: HashMap::new(),
            next_query_id: 1,
        }
    }

    fn generate_query_id(&mut self) -> u64 {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn start_query(
        &mut self,
        entity: &str,
        filter: Option<&str>,
        limit: u32,
        cursor: Option<&ScatterCursor>,
        timeout_ms: Option<u32>,
        partitions: Vec<PartitionId>,
        now: u64,
    ) -> (u64, Vec<QueryRequest>) {
        let query_id = self.generate_query_id();
        let timeout = timeout_ms.unwrap_or(DEFAULT_QUERY_TIMEOUT_MS);

        let pending = PendingQuery::new(query_id, timeout, partitions.clone(), now);
        self.pending_queries.insert(query_id, pending);

        let requests: Vec<QueryRequest> = partitions
            .into_iter()
            .map(|partition| {
                let partition_cursor = cursor
                    .and_then(|c| c.get(partition))
                    .map(PartitionCursor::to_bytes);

                QueryRequest::new(
                    query_id,
                    timeout,
                    entity.to_string(),
                    filter.map(String::from),
                    limit,
                    partition_cursor,
                )
            })
            .collect();

        (query_id, requests)
    }

    pub fn receive_response(&mut self, response: QueryResponse) -> Option<QueryResult> {
        let query_id = response.query_id;
        let partition = response.partition;

        let pending = self.pending_queries.get_mut(&query_id)?;

        if pending.expected_partitions.contains(&partition)
            && !pending.responses.contains_key(&partition)
        {
            pending.responses.insert(partition, response);
        }

        if pending.is_complete() {
            let pending = self.pending_queries.remove(&query_id)?;
            return Some(pending.build_result());
        }

        None
    }

    pub fn check_timeouts(&mut self, now: u64) -> Vec<QueryResult> {
        let timed_out: Vec<u64> = self
            .pending_queries
            .iter()
            .filter(|(_, p)| p.is_timed_out(now))
            .map(|(id, _)| *id)
            .collect();

        timed_out
            .into_iter()
            .filter_map(|id| {
                self.pending_queries.remove(&id).map(|p| {
                    let mut result = p.build_result();
                    result.partial = true;
                    result
                })
            })
            .collect()
    }

    #[must_use]
    pub fn prune_partitions(
        entity: &str,
        filter: Option<&str>,
        id: Option<&str>,
    ) -> Option<PartitionId> {
        if let Some(entity_id) = id {
            let key = format!("{entity}/{entity_id}");
            let hash = crc32fast::hash(key.as_bytes());
            #[allow(clippy::cast_possible_truncation)]
            let partition = (hash % u32::from(NUM_PARTITIONS)) as u16;
            return PartitionId::new(partition);
        }

        if let Some(f) = filter
            && let Some(id_value) = Self::extract_id_from_filter(f)
        {
            let key = format!("{entity}/{id_value}");
            let hash = crc32fast::hash(key.as_bytes());
            #[allow(clippy::cast_possible_truncation)]
            let partition = (hash % u32::from(NUM_PARTITIONS)) as u16;
            return PartitionId::new(partition);
        }

        None
    }

    fn extract_id_from_filter(filter: &str) -> Option<&str> {
        let filter = filter.trim();

        if filter.starts_with("id=") || filter.starts_with("id =") {
            let value = filter.split('=').nth(1)?.trim();
            let value = value.trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Some(value);
            }
        }

        None
    }

    #[must_use]
    pub fn all_partitions() -> Vec<PartitionId> {
        (0..NUM_PARTITIONS).filter_map(PartitionId::new).collect()
    }

    #[must_use]
    pub fn has_pending(&self, query_id: u64) -> bool {
        self.pending_queries.contains_key(&query_id)
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending_queries.len()
    }

    pub fn cancel_query(&mut self, query_id: u64) -> bool {
        self.pending_queries.remove(&query_id).is_some()
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    #[test]
    fn start_query_creates_requests() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0), partition(1), partition(2)];

        let (query_id, requests) =
            coordinator.start_query("users", None, 100, None, Some(5000), partitions, 1000);

        assert_eq!(query_id, 1);
        assert_eq!(requests.len(), 3);
        assert!(coordinator.has_pending(query_id));

        for req in &requests {
            assert_eq!(req.query_id, query_id);
            assert_eq!(req.entity, "users");
            assert_eq!(req.limit, 100);
            assert_eq!(req.timeout_ms, 5000);
        }
    }

    #[test]
    fn receive_responses_completes_query() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0), partition(1)];

        let (query_id, _) =
            coordinator.start_query("users", None, 100, None, None, partitions.clone(), 1000);

        let resp0 = QueryResponse::ok(query_id, partition(0), b"data0".to_vec(), false, None);
        let result = coordinator.receive_response(resp0);
        assert!(result.is_none());

        let resp1 = QueryResponse::ok(query_id, partition(1), b"data1".to_vec(), false, None);
        let result = coordinator.receive_response(resp1);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.query_id, query_id);
        assert_eq!(result.results.len(), 2);
        assert!(!result.partial);
        assert!(result.missing_partitions.is_empty());
    }

    #[test]
    fn timeout_returns_partial_results() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0), partition(1), partition(2)];

        let (query_id, _) =
            coordinator.start_query("users", None, 100, None, Some(1000), partitions, 1000);

        let resp0 = QueryResponse::ok(query_id, partition(0), b"data0".to_vec(), false, None);
        coordinator.receive_response(resp0);

        let results = coordinator.check_timeouts(2500);
        assert_eq!(results.len(), 1);

        let result = &results[0];
        assert!(result.partial);
        assert_eq!(result.missing_partitions.len(), 2);
        assert!(result.missing_partitions.contains(&partition(1)));
        assert!(result.missing_partitions.contains(&partition(2)));
    }

    #[test]
    fn prune_partitions_with_id() {
        let result = QueryCoordinator::prune_partitions("users", None, Some("123"));
        assert!(result.is_some());
    }

    #[test]
    fn prune_partitions_with_filter() {
        let result = QueryCoordinator::prune_partitions("users", Some("id = 'test-user'"), None);
        assert!(result.is_some());

        let result2 = QueryCoordinator::prune_partitions("users", Some("id=abc"), None);
        assert!(result2.is_some());
    }

    #[test]
    fn prune_partitions_no_prune() {
        let result = QueryCoordinator::prune_partitions("users", Some("age > 30"), None);
        assert!(result.is_none());

        let result2 = QueryCoordinator::prune_partitions("users", None, None);
        assert!(result2.is_none());
    }

    #[test]
    fn all_partitions_returns_expected_count() {
        let partitions = QueryCoordinator::all_partitions();
        assert_eq!(partitions.len(), NUM_PARTITIONS as usize);
    }

    #[test]
    fn cancel_query() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0)];

        let (query_id, _) =
            coordinator.start_query("users", None, 100, None, None, partitions, 1000);

        assert!(coordinator.has_pending(query_id));
        assert!(coordinator.cancel_query(query_id));
        assert!(!coordinator.has_pending(query_id));
    }

    #[test]
    fn has_more_propagates() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0), partition(1)];

        let (query_id, _) =
            coordinator.start_query("users", None, 10, None, None, partitions, 1000);

        let resp0 = QueryResponse::ok(query_id, partition(0), b"data".to_vec(), true, None);
        coordinator.receive_response(resp0);

        let resp1 = QueryResponse::ok(query_id, partition(1), b"data".to_vec(), false, None);
        let result = coordinator.receive_response(resp1).unwrap();

        assert!(result.has_more);
    }

    #[test]
    fn cursor_collected_from_responses() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0), partition(1)];

        let (query_id, _) =
            coordinator.start_query("users", None, 10, None, None, partitions, 1000);

        let cursor0 = PartitionCursor::new(partition(0), 100, None).to_bytes();
        let resp0 = QueryResponse::ok(
            query_id,
            partition(0),
            b"data".to_vec(),
            true,
            Some(cursor0),
        );
        coordinator.receive_response(resp0);

        let cursor1 = PartitionCursor::new(partition(1), 200, None).to_bytes();
        let resp1 = QueryResponse::ok(
            query_id,
            partition(1),
            b"data".to_vec(),
            true,
            Some(cursor1),
        );
        let result = coordinator.receive_response(resp1).unwrap();

        assert!(result.cursor.is_some());
        let cursor = result.cursor.unwrap();
        assert_eq!(cursor.len(), 2);
        assert_eq!(cursor.get(partition(0)).unwrap().sequence, 100);
        assert_eq!(cursor.get(partition(1)).unwrap().sequence, 200);
    }

    #[test]
    fn duplicate_responses_ignored() {
        let mut coordinator = QueryCoordinator::new(node(1));
        let partitions = vec![partition(0)];

        let (query_id, _) =
            coordinator.start_query("users", None, 100, None, None, partitions, 1000);

        let resp = QueryResponse::ok(query_id, partition(0), b"data".to_vec(), false, None);
        let result = coordinator.receive_response(resp.clone());
        assert!(result.is_some());

        let result2 = coordinator.receive_response(resp);
        assert!(result2.is_none());
    }
}
