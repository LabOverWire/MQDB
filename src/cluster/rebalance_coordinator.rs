// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use super::rebalancer::PartitionReassignment;
use bebytes::BeBytes;

const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct RebalanceProposal {
    pub id: String,
    pub current_version: u64,
    pub proposed_version: u64,
    pub coordinator: NodeId,
    pub assignments: Vec<PartitionReassignment>,
}

impl RebalanceProposal {
    #[must_use]
    pub fn create(
        id: &str,
        current_version: u64,
        proposed_version: u64,
        coordinator: NodeId,
        assignments: Vec<PartitionReassignment>,
    ) -> Self {
        Self {
            id: id.to_string(),
            current_version,
            proposed_version,
            coordinator,
            assignments,
        }
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct RebalanceAck {
    pub version: u8,
    pub id_len: u8,
    #[FromField(id_len)]
    pub id: Vec<u8>,
    pub node_id: u16,
    pub accepted: u8,
}

impl RebalanceAck {
    pub const VERSION: u8 = 1;

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn accept(id: &str, node_id: NodeId) -> Self {
        let id_bytes = id.as_bytes().to_vec();
        Self {
            version: Self::VERSION,
            id_len: id_bytes.len() as u8,
            id: id_bytes,
            node_id: node_id.get(),
            accepted: 1,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn reject(id: &str, node_id: NodeId) -> Self {
        let id_bytes = id.as_bytes().to_vec();
        Self {
            version: Self::VERSION,
            id_len: id_bytes.len() as u8,
            id: id_bytes,
            node_id: node_id.get(),
            accepted: 0,
        }
    }

    #[must_use]
    pub fn id_str(&self) -> &str {
        std::str::from_utf8(&self.id).unwrap_or("")
    }

    #[must_use]
    pub fn node(&self) -> Option<NodeId> {
        NodeId::validated(self.node_id)
    }

    #[must_use]
    pub fn is_accepted(&self) -> bool {
        self.accepted != 0
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct RebalanceCommit {
    pub version: u8,
    pub id_len: u8,
    #[FromField(id_len)]
    pub id: Vec<u8>,
    pub committed_version: u64,
}

impl RebalanceCommit {
    pub const VERSION: u8 = 1;

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(id: &str, committed_version: u64) -> Self {
        let id_bytes = id.as_bytes().to_vec();
        Self {
            version: Self::VERSION,
            id_len: id_bytes.len() as u8,
            id: id_bytes,
            committed_version,
        }
    }

    #[must_use]
    pub fn id_str(&self) -> &str {
        std::str::from_utf8(&self.id).unwrap_or("")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceState {
    Proposed,
    Committed,
    Aborted,
}

#[derive(Debug)]
struct ActiveProposal {
    proposal: RebalanceProposal,
    acks: Vec<RebalanceAck>,
    expected_nodes: Vec<NodeId>,
    started_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceError {
    ProposalInProgress,
    NoActiveProposal,
    InvalidProposal,
    QuorumNotReached,
    Timeout,
}

impl std::fmt::Display for RebalanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProposalInProgress => write!(f, "rebalance proposal already in progress"),
            Self::NoActiveProposal => write!(f, "no active rebalance proposal"),
            Self::InvalidProposal => write!(f, "invalid rebalance proposal"),
            Self::QuorumNotReached => write!(f, "quorum not reached for rebalance"),
            Self::Timeout => write!(f, "rebalance timeout"),
        }
    }
}

impl std::error::Error for RebalanceError {}

#[derive(Debug)]
pub struct RebalanceCoordinator {
    node_id: NodeId,
    current_proposal: Option<ActiveProposal>,
    timeout_ms: u64,
    next_proposal_id: u64,
}

impl RebalanceCoordinator {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            current_proposal: None,
            timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            next_proposal_id: 0,
        }
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn generate_proposal_id(&mut self) -> String {
        self.next_proposal_id += 1;
        format!("rb-{}-{}", self.node_id.get(), self.next_proposal_id)
    }

    /// # Errors
    /// Returns `ProposalInProgress` if there's already an active proposal.
    pub fn propose_rebalance(
        &mut self,
        assignments: Vec<PartitionReassignment>,
        current_version: u64,
        expected_nodes: Vec<NodeId>,
        now: u64,
    ) -> Result<RebalanceProposal, RebalanceError> {
        if self.current_proposal.is_some() {
            return Err(RebalanceError::ProposalInProgress);
        }

        let id = self.generate_proposal_id();
        let proposed_version = current_version + 1;

        let proposal = RebalanceProposal::create(
            &id,
            current_version,
            proposed_version,
            self.node_id,
            assignments,
        );

        self.current_proposal = Some(ActiveProposal {
            proposal: proposal.clone(),
            acks: Vec::new(),
            expected_nodes,
            started_at: now,
        });

        Ok(proposal)
    }

    /// # Errors
    /// Returns `NoActiveProposal` if there's no proposal in progress.
    pub fn receive_ack(
        &mut self,
        ack: RebalanceAck,
    ) -> Result<Option<RebalanceCommit>, RebalanceError> {
        let active = self
            .current_proposal
            .as_mut()
            .ok_or(RebalanceError::NoActiveProposal)?;

        if ack.id_str() != active.proposal.id {
            return Ok(None);
        }

        if !active.acks.iter().any(|a| a.node_id == ack.node_id) {
            active.acks.push(ack);
        }

        if Self::has_quorum_internal(active) {
            let commit =
                RebalanceCommit::create(&active.proposal.id, active.proposal.proposed_version);
            self.current_proposal = None;
            return Ok(Some(commit));
        }

        Ok(None)
    }

    fn has_quorum_internal(active: &ActiveProposal) -> bool {
        let expected_count = active.expected_nodes.len();
        if expected_count == 0 {
            return true;
        }

        let accepted_count = active
            .acks
            .iter()
            .filter(|a| {
                a.is_accepted() && active.expected_nodes.iter().any(|n| n.get() == a.node_id)
            })
            .count();

        accepted_count > expected_count / 2
    }

    #[must_use]
    pub fn has_quorum(&self) -> bool {
        self.current_proposal
            .as_ref()
            .is_some_and(Self::has_quorum_internal)
    }

    #[must_use]
    pub fn check_timeout(&self, now: u64) -> bool {
        self.current_proposal
            .as_ref()
            .is_some_and(|p| now.saturating_sub(p.started_at) >= self.timeout_ms)
    }

    pub fn abort_if_timeout(&mut self, now: u64) -> bool {
        if self.check_timeout(now) {
            self.current_proposal = None;
            return true;
        }
        false
    }

    pub fn abort(&mut self) {
        self.current_proposal = None;
    }

    #[must_use]
    pub fn has_active_proposal(&self) -> bool {
        self.current_proposal.is_some()
    }

    #[must_use]
    pub fn active_proposal_id(&self) -> Option<&str> {
        self.current_proposal
            .as_ref()
            .map(|p| p.proposal.id.as_str())
    }

    #[must_use]
    pub fn ack_count(&self) -> usize {
        self.current_proposal.as_ref().map_or(0, |p| p.acks.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Epoch, PartitionId};

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    fn sample_reassignment() -> PartitionReassignment {
        PartitionReassignment {
            partition: partition(0),
            old_primary: Some(node(1)),
            new_primary: node(2),
            old_replicas: vec![node(2)],
            new_replicas: vec![node(1)],
            new_epoch: Epoch::new(2),
        }
    }

    #[test]
    fn rebalance_ack_bebytes_roundtrip() {
        let accept = RebalanceAck::accept("rb-1-1", node(2));
        let bytes = accept.to_be_bytes();
        let (parsed, _) = RebalanceAck::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(parsed.id_str(), "rb-1-1");
        assert_eq!(parsed.node(), Some(node(2)));
        assert!(parsed.is_accepted());

        let reject = RebalanceAck::reject("rb-1-1", node(3));
        let bytes = reject.to_be_bytes();
        let (parsed, _) = RebalanceAck::try_from_be_bytes(&bytes).unwrap();

        assert!(!parsed.is_accepted());
    }

    #[test]
    fn rebalance_commit_bebytes_roundtrip() {
        let commit = RebalanceCommit::create("rb-1-1", 5);
        let bytes = commit.to_be_bytes();
        let (parsed, _) = RebalanceCommit::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(parsed.id_str(), "rb-1-1");
        assert_eq!(parsed.committed_version, 5);
    }

    #[test]
    fn coordinator_propose_rebalance() {
        let mut coordinator = RebalanceCoordinator::new(node(1));

        let proposal = coordinator
            .propose_rebalance(vec![sample_reassignment()], 1, vec![node(2), node(3)], 1000)
            .unwrap();

        assert!(proposal.id.starts_with("rb-1-"));
        assert_eq!(proposal.current_version, 1);
        assert_eq!(proposal.proposed_version, 2);
        assert!(coordinator.has_active_proposal());
    }

    #[test]
    fn coordinator_rejects_duplicate_proposal() {
        let mut coordinator = RebalanceCoordinator::new(node(1));

        coordinator
            .propose_rebalance(vec![], 1, vec![node(2)], 1000)
            .unwrap();

        let result = coordinator.propose_rebalance(vec![], 1, vec![node(2)], 1000);
        assert!(matches!(result, Err(RebalanceError::ProposalInProgress)));
    }

    #[test]
    fn coordinator_quorum_with_acks() {
        let mut coordinator = RebalanceCoordinator::new(node(1));

        let proposal = coordinator
            .propose_rebalance(vec![], 1, vec![node(2), node(3), node(4)], 1000)
            .unwrap();

        let proposal_id = proposal.id.clone();

        assert!(!coordinator.has_quorum());

        let ack1 = RebalanceAck::accept(&proposal_id, node(2));
        let result = coordinator.receive_ack(ack1).unwrap();
        assert!(result.is_none());
        assert!(!coordinator.has_quorum());

        let ack2 = RebalanceAck::accept(&proposal_id, node(3));
        let result = coordinator.receive_ack(ack2).unwrap();
        assert!(result.is_some());

        let commit = result.unwrap();
        assert_eq!(commit.id_str(), proposal_id);
        assert_eq!(commit.committed_version, 2);

        assert!(!coordinator.has_active_proposal());
    }

    #[test]
    fn coordinator_timeout_aborts_proposal() {
        let mut coordinator = RebalanceCoordinator::new(node(1)).with_timeout(5000);

        coordinator
            .propose_rebalance(vec![], 1, vec![node(2)], 1000)
            .unwrap();

        assert!(coordinator.has_active_proposal());
        assert!(!coordinator.check_timeout(5000));
        assert!(coordinator.check_timeout(7000));

        assert!(coordinator.abort_if_timeout(7000));
        assert!(!coordinator.has_active_proposal());
    }

    #[test]
    fn coordinator_ignores_wrong_proposal_id() {
        let mut coordinator = RebalanceCoordinator::new(node(1));

        coordinator
            .propose_rebalance(vec![], 1, vec![node(2)], 1000)
            .unwrap();

        let wrong_ack = RebalanceAck::accept("rb-wrong-id", node(2));
        let result = coordinator.receive_ack(wrong_ack).unwrap();
        assert!(result.is_none());
    }
}
