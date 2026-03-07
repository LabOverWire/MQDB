// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod election;
mod replication;
#[cfg(test)]
mod tests;

use super::node::{RaftConfig, RaftNode, RaftOutput};
use super::state::PartitionUpdate;
use crate::cluster::transport::{ClusterMessage, ClusterTransport, TransportError};
use crate::cluster::{NodeId, PartitionId, PartitionMap};
use crate::storage::StorageBackend;
use std::collections::HashSet;
use std::sync::Arc;

pub struct RaftCoordinator<T: ClusterTransport> {
    pub(super) node: RaftNode,
    pub(super) partition_map: PartitionMap,
    pub(super) transport: T,
    pub(super) cluster_members: Vec<NodeId>,
    pub(super) pending_dead_nodes: HashSet<NodeId>,
    pub(super) processed_dead_nodes: HashSet<NodeId>,
    pub(super) pending_draining_nodes: HashSet<NodeId>,
    pub(super) processed_draining_nodes: HashSet<NodeId>,
    pub(super) pending_new_nodes: HashSet<NodeId>,
    pub(super) processed_new_nodes: HashSet<NodeId>,
    pub(super) was_leader: bool,
    pub(super) pending_partition_proposals: usize,
    pub(super) last_balance_check: u64,
}

impl<T: ClusterTransport> RaftCoordinator<T> {
    pub fn new(node_id: NodeId, transport: T, config: RaftConfig) -> Self {
        Self {
            node: RaftNode::create(node_id, config),
            partition_map: PartitionMap::new(),
            transport,
            cluster_members: vec![node_id],
            pending_dead_nodes: HashSet::new(),
            processed_dead_nodes: HashSet::new(),
            pending_draining_nodes: HashSet::new(),
            processed_draining_nodes: HashSet::new(),
            pending_new_nodes: HashSet::new(),
            processed_new_nodes: HashSet::new(),
            was_leader: false,
            pending_partition_proposals: 0,
            last_balance_check: 0,
        }
    }

    /// # Errors
    /// Returns an error if loading persisted state fails.
    pub fn new_with_storage(
        node_id: NodeId,
        transport: T,
        config: RaftConfig,
        backend: Arc<dyn StorageBackend>,
    ) -> crate::error::Result<Self> {
        let node = RaftNode::create_with_storage(node_id, config, backend)?;

        let mut coordinator = Self {
            node,
            partition_map: PartitionMap::new(),
            transport,
            cluster_members: vec![node_id],
            pending_dead_nodes: HashSet::new(),
            processed_dead_nodes: HashSet::new(),
            pending_draining_nodes: HashSet::new(),
            processed_draining_nodes: HashSet::new(),
            pending_new_nodes: HashSet::new(),
            processed_new_nodes: HashSet::new(),
            was_leader: false,
            pending_partition_proposals: 0,
            last_balance_check: 0,
        };

        coordinator.rebuild_partition_map();
        Ok(coordinator)
    }

    fn rebuild_partition_map(&mut self) {
        let outputs = self.node.tick(0);
        for output in outputs {
            if let RaftOutput::ApplyCommand(cmd) = output {
                self.apply_command_local(&cmd);
            }
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    pub fn current_term(&self) -> u64 {
        self.node.current_term()
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.node.leader_id()
    }

    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    pub fn commit_index(&self) -> u64 {
        self.node.commit_index()
    }

    pub fn last_applied(&self) -> u64 {
        self.node.last_applied()
    }

    pub fn last_log_index(&self) -> u64 {
        self.node.last_log_index()
    }

    pub fn log_len(&self) -> usize {
        self.node.log_len()
    }

    pub fn apply_external_update(&mut self, update: &PartitionUpdate) {
        self.partition_map.apply_update(update);
    }

    pub fn add_peer(&mut self, peer: NodeId) {
        self.node.add_peer(peer);
        if !self.cluster_members.contains(&peer) {
            self.cluster_members.push(peer);
        }
    }

    pub fn cluster_members(&self) -> &[NodeId] {
        &self.cluster_members
    }

    pub fn set_pending_partition_proposals(&mut self, count: usize) {
        self.pending_partition_proposals = count;
    }

    /// # Errors
    /// Returns `TransportError` if the message cannot be sent.
    pub async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        self.transport.send(to, message).await
    }

    pub fn partitions_initialized(&self) -> bool {
        PartitionId::all().any(|p| self.partition_map.primary(p).is_some())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorError {
    NotLeader(Option<NodeId>),
    ProposeFailed,
    Transport(TransportError),
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader(leader) => match leader {
                Some(id) => write!(f, "not leader, leader is node {}", id.get()),
                None => write!(f, "not leader, leader unknown"),
            },
            Self::ProposeFailed => write!(f, "propose failed"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

impl From<TransportError> for CoordinatorError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}
