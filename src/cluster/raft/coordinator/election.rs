// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::super::state::RaftCommand;
use super::CoordinatorError;
use super::RaftCoordinator;
use crate::cluster::transport::ClusterTransport;
use crate::cluster::{NodeId, PartitionId};

impl<T: ClusterTransport> RaftCoordinator<T> {
    pub async fn tick(&mut self, now_ms: u64) -> Vec<u64> {
        let outputs = self.node.tick(now_ms);
        self.process_outputs(outputs).await;

        let is_leader = self.is_leader();
        let just_became_leader = is_leader && !self.was_leader;
        self.was_leader = is_leader;

        if just_became_leader {
            tracing::info!("became Raft leader, processing pending nodes");
            let mut indices = self.process_pending_dead_nodes().await;
            indices.extend(self.process_pending_draining_nodes().await);
            if self.partitions_initialized() {
                indices.extend(self.process_pending_new_nodes().await);
            } else {
                indices.extend(self.trigger_rebalance_internal().await);
            }
            return indices;
        }

        if is_leader
            && self.pending_partition_proposals == 0
            && !self.pending_new_nodes.is_empty()
            && self.partitions_initialized()
        {
            tracing::info!(
                pending_nodes = self.pending_new_nodes.len(),
                "partition proposals complete, processing pending new nodes"
            );
            return self.process_pending_new_nodes().await;
        }

        Vec::new()
    }

    pub(super) async fn process_pending_draining_nodes(&mut self) -> Vec<u64> {
        let mut proposed_indices = Vec::new();
        let pending: Vec<NodeId> = self.pending_draining_nodes.drain().collect();

        for draining_node in pending {
            if self.processed_draining_nodes.insert(draining_node) {
                let indices = self.trigger_drain_rebalance(draining_node).await;
                proposed_indices.extend(indices);
            }
        }

        proposed_indices
    }

    pub(super) async fn process_pending_new_nodes(&mut self) -> Vec<u64> {
        let pending: Vec<NodeId> = self.pending_new_nodes.drain().collect();

        if pending.is_empty() {
            return Vec::new();
        }

        for node in &pending {
            self.processed_new_nodes.insert(*node);
        }

        tracing::info!(?pending, "processing pending new nodes for rebalance");
        self.trigger_rebalance_for_new_node().await
    }

    pub(super) async fn process_pending_dead_nodes(&mut self) -> Vec<u64> {
        let mut proposed_indices = Vec::new();
        let pending: Vec<NodeId> = self.pending_dead_nodes.drain().collect();

        for dead_node in pending {
            if self.processed_dead_nodes.insert(dead_node) {
                let indices = self.reassign_partitions_for_dead_node(dead_node).await;
                proposed_indices.extend(indices);
            }
        }

        self.scan_partition_map_for_dead_primaries(&mut proposed_indices)
            .await;

        proposed_indices
    }

    pub(super) async fn scan_partition_map_for_dead_primaries(
        &mut self,
        proposed_indices: &mut Vec<u64>,
    ) {
        let alive_nodes: Vec<NodeId> = self.cluster_members.clone();

        for partition in PartitionId::all() {
            if let Some(primary) = self.partition_map.primary(partition)
                && !alive_nodes.contains(&primary)
                && !self.pending_dead_nodes.contains(&primary)
                && !self.processed_dead_nodes.contains(&primary)
            {
                tracing::warn!(?partition, ?primary, "found stale primary in partition map");
                self.processed_dead_nodes.insert(primary);
                let indices = self.reassign_partitions_for_dead_node(primary).await;
                proposed_indices.extend(indices);
            }
        }
    }

    /// # Errors
    /// Returns `NotLeader` if not the leader, `ProposeFailed` if proposal fails.
    pub async fn propose_partition_update(
        &mut self,
        command: RaftCommand,
    ) -> Result<u64, CoordinatorError> {
        if !self.is_leader() {
            return Err(CoordinatorError::NotLeader(self.leader_id()));
        }

        let (index, outputs) = self.node.propose(command);
        self.process_outputs(outputs).await;
        index.ok_or(CoordinatorError::ProposeFailed)
    }

    pub async fn handle_request_vote(
        &mut self,
        from: NodeId,
        request: RequestVoteRequest,
        now_ms: u64,
    ) -> RequestVoteResponse {
        tracing::debug!(
            from = from.get(),
            term = request.term,
            "received RequestVote"
        );
        let (response, outputs) = self.node.handle_request_vote(from, request, now_ms);
        tracing::debug!(granted = response.is_granted(), "responding to RequestVote");
        self.process_outputs(outputs).await;
        response
    }

    pub async fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        response: RequestVoteResponse,
    ) {
        tracing::debug!(
            from = from.get(),
            granted = response.is_granted(),
            "received RequestVoteResponse"
        );
        let outputs = self.node.handle_request_vote_response(from, response);
        self.process_outputs(outputs).await;
    }

    pub async fn handle_append_entries(
        &mut self,
        from: NodeId,
        request: AppendEntriesRequest,
        now_ms: u64,
    ) -> AppendEntriesResponse {
        tracing::debug!(
            from = from.get(),
            term = request.term,
            entries = request.entries.len(),
            prev_log_index = request.prev_log_index,
            leader_commit = request.leader_commit,
            "received AppendEntries"
        );
        let (response, outputs) = self.node.handle_append_entries(from, request, now_ms);
        self.process_outputs(outputs).await;
        response
    }

    pub async fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        response: AppendEntriesResponse,
    ) {
        tracing::debug!(
            from = from.get(),
            success = response.is_success(),
            match_index = response.match_index,
            "received AppendEntriesResponse"
        );
        let outputs = self.node.handle_append_entries_response(from, response);
        self.process_outputs(outputs).await;
    }
}
