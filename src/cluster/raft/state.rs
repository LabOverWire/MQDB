// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BeBytes)]
pub struct PartitionUpdate {
    pub partition: u8,
    pub primary: u16,
    pub replica1: u16,
    pub replica2: u16,
    pub epoch: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftCommand {
    UpdatePartition(PartitionUpdate),
    AddNode { node_id: u16 },
    RemoveNode { node_id: u16 },
    Noop,
}

impl RaftCommand {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn update_partition(
        partition: PartitionId,
        primary: NodeId,
        replicas: &[NodeId],
        epoch: Epoch,
    ) -> Self {
        Self::UpdatePartition(PartitionUpdate::new(
            partition.get() as u8,
            primary.get(),
            replicas.first().map_or(0, |n| n.get()),
            replicas.get(1).map_or(0, |n| n.get()),
            epoch.get() as u32,
        ))
    }

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::UpdatePartition(update) => {
                buf.push(0);
                buf.extend_from_slice(&update.to_be_bytes());
            }
            Self::AddNode { node_id } => {
                buf.push(1);
                buf.extend_from_slice(&node_id.to_be_bytes());
            }
            Self::RemoveNode { node_id } => {
                buf.push(2);
                buf.extend_from_slice(&node_id.to_be_bytes());
            }
            Self::Noop => {
                buf.push(3);
            }
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        match bytes[0] {
            0 => {
                let (update, _) = PartitionUpdate::try_from_be_bytes(&bytes[1..]).ok()?;
                Some(Self::UpdatePartition(update))
            }
            1 if bytes.len() >= 3 => Some(Self::AddNode {
                node_id: u16::from_be_bytes([bytes[1], bytes[2]]),
            }),
            2 if bytes.len() >= 3 => Some(Self::RemoveNode {
                node_id: u16::from_be_bytes([bytes[1], bytes[2]]),
            }),
            3 => Some(Self::Noop),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub cmd_len: u16,
    #[FromField(cmd_len)]
    pub cmd_bytes: Vec<u8>,
}

impl LogEntry {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(index: u64, term: u64, command: RaftCommand) -> Self {
        let cmd_bytes = command.to_bytes();
        let cmd_len = cmd_bytes.len() as u16;
        Self::new(index, term, cmd_len, cmd_bytes)
    }

    #[must_use]
    pub fn command(&self) -> Option<RaftCommand> {
        RaftCommand::from_bytes(&self.cmd_bytes)
    }
}

#[derive(Debug)]
pub struct RaftState {
    node_id: NodeId,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    log_base_index: u64,
    commit_index: u64,
    last_applied: u64,
    role: RaftRole,
    leader_id: Option<NodeId>,
    next_index: Vec<(NodeId, u64)>,
    match_index: Vec<(NodeId, u64)>,
    votes_received: Vec<NodeId>,
    peers: Vec<NodeId>,
}

impl RaftState {
    #[must_use]
    pub fn create(node_id: NodeId) -> Self {
        Self {
            node_id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            log_base_index: 0,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
            next_index: Vec::new(),
            match_index: Vec::new(),
            votes_received: Vec::new(),
            peers: Vec::new(),
        }
    }

    #[must_use]
    pub fn recover(
        node_id: NodeId,
        current_term: u64,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
    ) -> Self {
        let log_base_index = log.first().map_or(0, |e| e.index.saturating_sub(1));
        Self {
            node_id,
            current_term,
            voted_for,
            log,
            log_base_index,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
            next_index: Vec::new(),
            match_index: Vec::new(),
            votes_received: Vec::new(),
            peers: Vec::new(),
        }
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    #[must_use]
    pub fn role(&self) -> RaftRole {
        self.role
    }

    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn set_leader(&mut self, leader: Option<NodeId>) {
        self.leader_id = leader;
    }

    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    #[must_use]
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    #[must_use]
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map_or(0, |e| e.index)
    }

    #[must_use]
    pub fn log_len(&self) -> usize {
        self.log.len()
    }

    #[must_use]
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    #[must_use]
    pub fn last_log_entry(&self) -> Option<&LogEntry> {
        self.log.last()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn log_position(&self, index: u64) -> Option<usize> {
        if index <= self.log_base_index {
            return None;
        }
        Some((index - self.log_base_index - 1) as usize)
    }

    #[must_use]
    pub fn log_term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        let pos = self.log_position(index)?;
        self.log
            .get(pos)
            .filter(|e| e.index == index)
            .map(|e| e.term)
    }

    pub fn add_peer(&mut self, peer: NodeId) {
        if peer != self.node_id && !self.peers.contains(&peer) {
            self.peers.push(peer);
            if self.role == RaftRole::Leader {
                let next = self.last_log_index() + 1;
                self.next_index.push((peer, next));
                self.match_index.push((peer, 0));
            }
        }
    }

    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    pub fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = leader;
        self.votes_received.clear();
    }

    pub fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.push(self.node_id);
    }

    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.node_id);
        self.votes_received.clear();

        let next = self.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            self.next_index.push((peer, next));
            self.match_index.push((peer, 0));
        }
    }

    #[must_use]
    pub fn record_vote(&mut self, from: NodeId) -> bool {
        if !self.votes_received.contains(&from) {
            self.votes_received.push(from);
        }
        self.has_quorum()
    }

    #[must_use]
    pub fn has_quorum(&self) -> bool {
        self.votes_received.len() >= self.quorum_size()
    }

    #[must_use]
    pub fn can_grant_vote(
        &self,
        candidate_term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> bool {
        if candidate_term < self.current_term {
            return false;
        }

        let vote_available = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
        if !vote_available {
            return false;
        }

        let my_last_term = self.last_log_term();
        let my_last_index = self.last_log_index();

        if last_log_term > my_last_term {
            return true;
        }
        if last_log_term == my_last_term && last_log_index >= my_last_index {
            return true;
        }

        false
    }

    pub fn grant_vote(&mut self, term: u64, candidate: NodeId) {
        self.current_term = term;
        self.voted_for = Some(candidate);
    }

    pub fn append_entry(&mut self, entry: LogEntry) {
        let Some(pos) = self.log_position(entry.index) else {
            return;
        };
        if pos < self.log.len() {
            if self.log[pos].term != entry.term {
                self.log.truncate(pos);
                self.log.push(entry);
            }
        } else if entry.index == self.last_log_index() + 1 {
            self.log.push(entry);
        }
    }

    #[must_use]
    pub fn append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
    ) -> bool {
        if prev_log_index > 0 {
            match self.log_term_at(prev_log_index) {
                Some(term) if term == prev_log_term => {}
                _ => return false,
            }
        }

        for entry in entries {
            self.append_entry(entry);
        }

        true
    }

    pub fn update_commit_index(&mut self, leader_commit: u64) {
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
        }
    }

    #[must_use]
    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let Some(pos) = self.log_position(start_index) else {
            return self.log.clone();
        };
        if pos >= self.log.len() {
            return Vec::new();
        }
        self.log[pos..].to_vec()
    }

    #[must_use]
    pub fn next_index_for(&self, peer: NodeId) -> u64 {
        self.next_index
            .iter()
            .find(|(n, _)| *n == peer)
            .map_or(1, |(_, idx)| *idx)
    }

    pub fn update_next_index(&mut self, peer: NodeId, index: u64) {
        if let Some(entry) = self.next_index.iter_mut().find(|(n, _)| *n == peer) {
            entry.1 = index;
        }
    }

    pub fn update_match_index(&mut self, peer: NodeId, index: u64) {
        if let Some(entry) = self.match_index.iter_mut().find(|(n, _)| *n == peer) {
            entry.1 = index;
        }
    }

    pub fn decrement_next_index(&mut self, peer: NodeId) {
        if let Some(entry) = self.next_index.iter_mut().find(|(n, _)| *n == peer)
            && entry.1 > 1
        {
            entry.1 -= 1;
        }
    }

    pub fn try_advance_commit_index(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }

        let start = std::time::Instant::now();
        let iterations = self.last_log_index().saturating_sub(self.commit_index);

        for n in (self.commit_index + 1)..=self.last_log_index() {
            let Some(term_at_n) = self.log_term_at(n) else {
                continue;
            };

            if term_at_n != self.current_term {
                continue;
            }

            let match_count = self.match_index.iter().filter(|(_, idx)| *idx >= n).count() + 1;

            if match_count >= self.quorum_size() {
                self.commit_index = n;
            }
        }

        let elapsed = start.elapsed();
        if elapsed.as_micros() > 100 {
            tracing::warn!(
                elapsed_us = elapsed.as_micros(),
                log_len = self.log.len(),
                iterations = iterations,
                "slow try_advance_commit_index"
            );
        }
    }

    #[must_use]
    pub fn pending_commands(&mut self) -> Vec<RaftCommand> {
        let mut commands = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let Some(pos) = self.log_position(self.last_applied) else {
                continue;
            };
            if let Some(entry) = self.log.get(pos)
                && entry.index == self.last_applied
                && let Some(cmd) = entry.command()
            {
                commands.push(cmd);
            }
        }
        commands
    }

    pub fn compact_log(&mut self, keep_entries: usize) {
        if self.log.len() <= keep_entries {
            return;
        }
        let safe_index = self.last_applied.min(self.commit_index);
        let Some(safe_pos) = self.log_position(safe_index) else {
            return;
        };
        let remove_count = safe_pos.saturating_sub(keep_entries);
        if remove_count == 0 {
            return;
        }
        self.log.drain(..remove_count);
        if let Some(first) = self.log.first() {
            self.log_base_index = first.index - 1;
        }
    }

    #[must_use]
    pub fn propose(&mut self, command: RaftCommand) -> Option<u64> {
        if self.role != RaftRole::Leader {
            return None;
        }

        let index = self.last_log_index() + 1;
        let entry = LogEntry::create(index, self.current_term, command);
        self.log.push(entry);

        Some(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_is_follower() {
        let node = NodeId::validated(1).unwrap();
        let state = RaftState::create(node);
        assert_eq!(state.role(), RaftRole::Follower);
        assert_eq!(state.current_term(), 0);
        assert!(state.voted_for().is_none());
    }

    #[test]
    fn become_candidate_increments_term() {
        let node = NodeId::validated(1).unwrap();
        let mut state = RaftState::create(node);
        state.become_candidate();
        assert_eq!(state.role(), RaftRole::Candidate);
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.voted_for(), Some(node));
    }

    #[test]
    fn quorum_size_calculation() {
        let node = NodeId::validated(1).unwrap();
        let mut state = RaftState::create(node);
        assert_eq!(state.quorum_size(), 1);

        state.add_peer(NodeId::validated(2).unwrap());
        assert_eq!(state.quorum_size(), 2);

        state.add_peer(NodeId::validated(3).unwrap());
        assert_eq!(state.quorum_size(), 2);

        state.add_peer(NodeId::validated(4).unwrap());
        assert_eq!(state.quorum_size(), 3);

        state.add_peer(NodeId::validated(5).unwrap());
        assert_eq!(state.quorum_size(), 3);
    }

    #[test]
    fn vote_collection() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();
        let node4 = NodeId::validated(4).unwrap();
        let node5 = NodeId::validated(5).unwrap();

        let mut state = RaftState::create(node1);
        state.add_peer(node2);
        state.add_peer(node3);
        state.add_peer(node4);
        state.add_peer(node5);
        state.become_candidate();

        assert!(!state.record_vote(node2));
        assert!(state.record_vote(node3));
    }

    #[test]
    fn log_append_and_query() {
        let node = NodeId::validated(1).unwrap();
        let mut state = RaftState::create(node);
        state.become_candidate();
        state.become_leader();

        let idx = state.propose(RaftCommand::Noop).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(state.last_log_index(), 1);
        assert_eq!(state.last_log_term(), 1);
    }

    #[test]
    fn grant_vote_checks_log_up_to_date() {
        let node = NodeId::validated(1).unwrap();
        let candidate = NodeId::validated(2).unwrap();
        let state = RaftState::create(node);

        assert!(state.can_grant_vote(1, candidate, 0, 0));
        assert!(state.can_grant_vote(1, candidate, 1, 1));
    }

    #[test]
    fn commit_index_advancement() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let mut state = RaftState::create(node1);
        state.add_peer(node2);
        state.add_peer(node3);
        state.become_candidate();
        state.become_leader();

        let _ = state.propose(RaftCommand::Noop);
        assert_eq!(state.commit_index(), 0);

        state.update_match_index(node2, 1);
        state.try_advance_commit_index();
        assert_eq!(state.commit_index(), 1);
    }

    #[test]
    fn log_entry_bebytes_roundtrip() {
        let entry = LogEntry::create(5, 2, RaftCommand::Noop);
        let bytes = entry.to_be_bytes();
        let (parsed, _) = LogEntry::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(entry.index, parsed.index);
        assert_eq!(entry.term, parsed.term);
        assert_eq!(entry.command(), parsed.command());
    }

    #[test]
    fn partition_update_bebytes_roundtrip() {
        let update = PartitionUpdate::new(5, 1, 2, 3, 100);
        let bytes = update.to_be_bytes();
        let (parsed, _) = PartitionUpdate::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(update, parsed);
    }
}
