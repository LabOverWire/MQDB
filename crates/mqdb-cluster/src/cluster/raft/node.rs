// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, RaftSnapshot,
    RequestVoteRequest, RequestVoteResponse,
};
use super::state::{LogEntry, PreparedAppend, RaftCommand, RaftRole, RaftState};
use super::storage::RaftStorage;
use crate::cluster::NodeId;
use mqdb_core::error::Result;
use mqdb_core::storage::StorageBackend;
use std::sync::Arc;

#[derive(Debug)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub startup_grace_period_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 3000,
            election_timeout_max_ms: 5000,
            heartbeat_interval_ms: 500,
            startup_grace_period_ms: 10000,
        }
    }
}

#[derive(Debug)]
pub enum RaftOutput {
    SendRequestVote {
        to: NodeId,
        request: RequestVoteRequest,
    },
    SendAppendEntries {
        to: NodeId,
        request: AppendEntriesRequest,
    },
    SendSnapshot {
        to: NodeId,
        last_index: u64,
        last_term: u64,
    },
    InstallSnapshot(RaftSnapshot),
    ApplyCommand(RaftCommand),
    BecameLeader,
    BecameFollower {
        leader: Option<NodeId>,
    },
}

pub struct RaftNode {
    state: RaftState,
    config: RaftConfig,
    storage: Option<RaftStorage>,
    last_heartbeat_time: u64,
    election_timeout: u64,
    last_election_time: u64,
    random_seed: u64,
    startup_time: Option<u64>,
    restored_snapshot: Option<RaftSnapshot>,
    storage_failure: Option<String>,
}

impl RaftNode {
    #[must_use]
    pub fn create(node_id: NodeId, config: RaftConfig) -> Self {
        let timeout = config.election_timeout_min_ms;
        Self {
            state: RaftState::create(node_id),
            config,
            storage: None,
            last_heartbeat_time: 0,
            election_timeout: timeout,
            last_election_time: 0,
            random_seed: u64::from(node_id.get()),
            startup_time: None,
            restored_snapshot: None,
            storage_failure: None,
        }
    }

    /// # Errors
    /// Returns an error if loading persisted state fails.
    pub fn create_with_storage(
        node_id: NodeId,
        config: RaftConfig,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        let storage = RaftStorage::new(backend);

        let (current_term, voted_for, log) = match storage.load_state()? {
            Some(persisted) => {
                let log = storage.load_log()?;
                (persisted.current_term, persisted.voted_for_node(), log)
            }
            None => (0, None, Vec::new()),
        };
        let restored_snapshot = storage.load_snapshot()?;
        let snapshot_point = restored_snapshot
            .as_ref()
            .map(|snapshot| (snapshot.last_index, snapshot.last_term));
        let first_expected = snapshot_point.map_or(1, |(index, _)| index + 1);
        let mut log: Vec<LogEntry> = log
            .into_iter()
            .filter(|entry| entry.index >= first_expected)
            .collect();
        let contiguous = log
            .iter()
            .zip(first_expected..)
            .take_while(|(entry, expected)| entry.index == *expected)
            .count();
        if let (Some(first_dropped), Some(last)) = (log.get(contiguous), log.last()) {
            tracing::warn!(
                kept = contiguous,
                first_dropped = first_dropped.index,
                last_index = last.index,
                "discarding raft log entries after a gap"
            );
            storage.replace_log_from(first_dropped.index, last.index, &[])?;
            log.truncate(contiguous);
        }

        let state = RaftState::recover(node_id, current_term, voted_for, log, snapshot_point);
        let timeout = config.election_timeout_min_ms;

        Ok(Self {
            state,
            config,
            storage: Some(storage),
            last_heartbeat_time: 0,
            election_timeout: timeout,
            last_election_time: 0,
            random_seed: u64::from(node_id.get()),
            startup_time: None,
            restored_snapshot,
            storage_failure: None,
        })
    }

    pub fn take_restored_snapshot(&mut self) -> Option<RaftSnapshot> {
        self.restored_snapshot.take()
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.state.node_id()
    }

    #[must_use]
    pub fn role(&self) -> RaftRole {
        self.state.role()
    }

    #[must_use]
    pub fn current_term(&self) -> u64 {
        self.state.current_term()
    }

    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.state.leader_id()
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.state.role() == RaftRole::Leader
    }

    pub fn add_peer(&mut self, peer: NodeId) {
        self.state.add_peer(peer);
    }

    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        self.state.peers()
    }

    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.state.commit_index()
    }

    #[must_use]
    pub fn last_applied(&self) -> u64 {
        self.state.last_applied()
    }

    #[must_use]
    pub fn last_log_index(&self) -> u64 {
        self.state.last_log_index()
    }

    #[must_use]
    pub fn log_len(&self) -> usize {
        self.state.log_len()
    }

    fn next_random(&mut self) -> u64 {
        self.random_seed = self
            .random_seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.random_seed
    }

    fn reset_election_timeout(&mut self) {
        let range = self.config.election_timeout_max_ms - self.config.election_timeout_min_ms;
        let offset = self.next_random() % (range + 1);
        self.election_timeout = self.config.election_timeout_min_ms + offset;
    }

    fn can_start_election(&self, now_ms: u64) -> bool {
        let has_peers = !self.state.peers().is_empty();
        if has_peers {
            return true;
        }
        let Some(startup) = self.startup_time else {
            return false;
        };
        now_ms >= startup + self.config.startup_grace_period_ms
    }

    #[must_use]
    pub fn storage_failure(&self) -> Option<&str> {
        self.storage_failure.as_deref()
    }

    fn record_storage_failure(&mut self, what: &str, error: &mqdb_core::error::Error) {
        tracing::error!(%error, "raft storage failure while persisting {what}");
        self.storage_failure
            .get_or_insert_with(|| format!("failed to persist raft {what}: {error}"));
    }

    fn persist_state(&mut self) -> bool {
        let result = match self.storage {
            Some(ref storage) => {
                storage.persist_state(self.state.current_term(), self.state.voted_for())
            }
            None => Ok(()),
        };
        match result {
            Ok(()) => true,
            Err(error) => {
                self.record_storage_failure("term and vote", &error);
                false
            }
        }
    }

    fn persist_last_entry(&mut self) -> bool {
        let result = match (&self.storage, self.state.last_log_entry()) {
            (Some(storage), Some(entry)) => storage.append_log_entry(entry),
            _ => Ok(()),
        };
        match result {
            Ok(()) => true,
            Err(error) => {
                self.record_storage_failure("log entry", &error);
                false
            }
        }
    }

    fn persist_append(&self, prepared: &PreparedAppend) -> Result<()> {
        match (&self.storage, prepared.first_index) {
            (Some(storage), Some(first_index)) => storage.replace_log_from(
                first_index,
                self.state.last_log_index(),
                &prepared.entries,
            ),
            _ => Ok(()),
        }
    }

    fn propose_leader_noop(&mut self) -> bool {
        if self.state.propose(RaftCommand::Noop).is_none() {
            return true;
        }
        if !self.persist_last_entry() {
            return false;
        }
        self.state.try_advance_commit_index();
        true
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<RaftOutput> {
        if self.storage_failure.is_some() {
            return Vec::new();
        }
        if self.startup_time.is_none() {
            self.startup_time = Some(now_ms);
            if self.last_heartbeat_time == 0 {
                self.last_heartbeat_time = now_ms;
            }
            self.reset_election_timeout();
        }

        let mut outputs = Vec::new();

        match self.state.role() {
            RaftRole::Follower | RaftRole::Candidate => {
                if now_ms >= self.last_heartbeat_time + self.election_timeout
                    && self.can_start_election(now_ms)
                {
                    outputs.extend(self.start_election(now_ms));
                }
            }
            RaftRole::Leader => {
                if now_ms >= self.last_heartbeat_time + self.config.heartbeat_interval_ms {
                    outputs.extend(self.send_heartbeats());
                    self.last_heartbeat_time = now_ms;
                }
            }
        }

        outputs.extend(self.apply_committed());
        outputs
    }

    fn start_election(&mut self, now_ms: u64) -> Vec<RaftOutput> {
        self.state.become_candidate();
        if !self.persist_state() {
            return Vec::new();
        }
        self.last_election_time = now_ms;
        self.last_heartbeat_time = now_ms;
        self.reset_election_timeout();

        if self.state.has_quorum() {
            self.state.become_leader();
            if !self.propose_leader_noop() {
                return Vec::new();
            }
            let mut outputs = vec![RaftOutput::BecameLeader];
            outputs.extend(self.send_heartbeats());
            return outputs;
        }

        let request = RequestVoteRequest::create(
            self.state.current_term(),
            self.state.node_id().get(),
            self.state.last_log_index(),
            self.state.last_log_term(),
        );

        self.state
            .peers()
            .to_vec()
            .into_iter()
            .map(|peer| RaftOutput::SendRequestVote { to: peer, request })
            .collect()
    }

    fn send_heartbeats(&mut self) -> Vec<RaftOutput> {
        let peers: Vec<NodeId> = self.state.peers().to_vec();
        let mut outputs = Vec::new();

        for peer in peers {
            let next_idx = self.state.next_index_for(peer);
            if next_idx <= self.state.log_base_index() {
                let last_index = self.state.last_applied();
                if let Some(last_term) = self.state.log_term_at(last_index) {
                    outputs.push(RaftOutput::SendSnapshot {
                        to: peer,
                        last_index,
                        last_term,
                    });
                }
                continue;
            }
            let prev_idx = next_idx.saturating_sub(1);
            let prev_term = self.state.log_term_at(prev_idx).unwrap_or(0);
            let entries = self.state.entries_from(next_idx);

            let request = AppendEntriesRequest::create(
                self.state.current_term(),
                self.state.node_id().get(),
                prev_idx,
                prev_term,
                entries,
                self.state.commit_index(),
            );

            outputs.push(RaftOutput::SendAppendEntries { to: peer, request });
        }

        outputs
    }

    pub fn take_committed(&mut self) -> Vec<RaftOutput> {
        self.apply_committed()
    }

    fn apply_committed(&mut self) -> Vec<RaftOutput> {
        let commands: Vec<_> = self
            .state
            .pending_commands()
            .into_iter()
            .map(RaftOutput::ApplyCommand)
            .collect();
        self.state.compact_log(1000);
        commands
    }

    pub fn handle_request_vote(
        &mut self,
        from: NodeId,
        request: RequestVoteRequest,
        now_ms: u64,
    ) -> (RequestVoteResponse, Vec<RaftOutput>) {
        let mut outputs = Vec::new();

        if self.storage_failure.is_some() || from.get() != request.candidate_id {
            return (
                RequestVoteResponse::rejected(self.state.current_term()),
                outputs,
            );
        }

        if request.term > self.state.current_term() {
            self.state.become_follower(request.term, None);
            if !self.persist_state() {
                return (RequestVoteResponse::rejected(request.term), Vec::new());
            }
            outputs.push(RaftOutput::BecameFollower { leader: None });
        }

        let candidate = NodeId::validated(request.candidate_id);
        let can_grant = candidate.is_some_and(|c| {
            self.state.can_grant_vote(
                request.term,
                c,
                request.last_log_index,
                request.last_log_term,
            )
        });

        let response = if can_grant {
            if let Some(c) = candidate {
                self.state.grant_vote(request.term, c);
                if !self.persist_state() {
                    return (RequestVoteResponse::rejected(request.term), Vec::new());
                }
                self.last_heartbeat_time = now_ms;
                self.reset_election_timeout();
            }
            RequestVoteResponse::granted(self.state.current_term())
        } else {
            RequestVoteResponse::rejected(self.state.current_term())
        };

        (response, outputs)
    }

    pub fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        response: RequestVoteResponse,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        if self.storage_failure.is_some() {
            return outputs;
        }

        if response.term > self.state.current_term() {
            self.state.become_follower(response.term, None);
            if !self.persist_state() {
                return Vec::new();
            }
            outputs.push(RaftOutput::BecameFollower { leader: None });
            return outputs;
        }

        if self.state.role() != RaftRole::Candidate {
            return outputs;
        }

        if response.term != self.state.current_term() {
            return outputs;
        }

        if response.is_granted() && self.state.record_vote(from) {
            self.state.become_leader();
            if !self.propose_leader_noop() {
                return Vec::new();
            }
            outputs.push(RaftOutput::BecameLeader);
            outputs.extend(self.send_heartbeats());
        }

        outputs
    }

    pub fn handle_append_entries(
        &mut self,
        from: NodeId,
        request: AppendEntriesRequest,
        now_ms: u64,
    ) -> (AppendEntriesResponse, Vec<RaftOutput>) {
        let mut outputs = Vec::new();

        if self.storage_failure.is_some() || from.get() != request.leader_id {
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        }

        if request.term < self.state.current_term() {
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        }

        let leader = NodeId::validated(request.leader_id);

        if request.term > self.state.current_term() || self.state.role() != RaftRole::Follower {
            self.state.become_follower(request.term, leader);
            if !self.persist_state() {
                return (AppendEntriesResponse::failure(request.term), Vec::new());
            }
            outputs.push(RaftOutput::BecameFollower { leader });
        } else if self.state.leader_id() != leader {
            self.state.set_leader(leader);
        }

        self.last_heartbeat_time = now_ms;
        self.reset_election_timeout();

        let verified_index = request.prev_log_index + request.entries.len() as u64;
        let Some(prepared) = self.state.prepare_append(
            request.prev_log_index,
            request.prev_log_term,
            request.entries,
        ) else {
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        };

        if let Err(error) = self.persist_append(&prepared) {
            self.record_storage_failure("log entries", &error);
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        }
        self.state.commit_append(prepared);
        self.state
            .update_commit_index(request.leader_commit.min(verified_index));
        outputs.extend(self.apply_committed());

        (
            AppendEntriesResponse::success(
                self.state.current_term(),
                verified_index.max(self.state.log_base_index()),
            ),
            outputs,
        )
    }

    pub fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        request: InstallSnapshotRequest,
        now_ms: u64,
    ) -> (AppendEntriesResponse, Vec<RaftOutput>) {
        let mut outputs = Vec::new();

        if self.storage_failure.is_some()
            || from.get() != request.leader_id
            || request.term < self.state.current_term()
        {
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        }

        let leader = NodeId::validated(request.leader_id);
        if request.term > self.state.current_term() || self.state.role() != RaftRole::Follower {
            self.state.become_follower(request.term, leader);
            if !self.persist_state() {
                return (AppendEntriesResponse::failure(request.term), Vec::new());
            }
            outputs.push(RaftOutput::BecameFollower { leader });
        } else if self.state.leader_id() != leader {
            self.state.set_leader(leader);
        }

        self.last_heartbeat_time = now_ms;
        self.reset_election_timeout();

        let snapshot = request.snapshot;
        if snapshot.last_index <= self.state.commit_index() {
            return (
                AppendEntriesResponse::success(
                    self.state.current_term(),
                    self.state.commit_index(),
                ),
                outputs,
            );
        }

        let keeps_suffix = self
            .state
            .holds_entry(snapshot.last_index, snapshot.last_term);
        let persisted = match self.storage {
            Some(ref storage) => storage.install_snapshot(&snapshot, keeps_suffix),
            None => Ok(()),
        };
        if let Err(error) = persisted {
            self.record_storage_failure("snapshot", &error);
            return (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            );
        }
        self.state
            .install_snapshot(snapshot.last_index, snapshot.last_term);
        let last_index = snapshot.last_index;
        outputs.push(RaftOutput::InstallSnapshot(snapshot));

        (
            AppendEntriesResponse::success(self.state.current_term(), last_index),
            outputs,
        )
    }

    pub fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        response: AppendEntriesResponse,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        if self.storage_failure.is_some() {
            return outputs;
        }

        if response.term > self.state.current_term() {
            self.state.become_follower(response.term, None);
            if !self.persist_state() {
                return Vec::new();
            }
            outputs.push(RaftOutput::BecameFollower { leader: None });
            return outputs;
        }

        if self.state.role() != RaftRole::Leader {
            return outputs;
        }

        if response.is_success() {
            self.state.record_match(from, response.match_index);
            self.state.try_advance_commit_index();
            outputs.extend(self.apply_committed());
        } else if response.match_index > 0 {
            self.state.update_next_index(from, response.match_index + 1);
        } else {
            self.state.update_next_index(from, 1);
        }

        outputs
    }

    pub fn propose(&mut self, command: RaftCommand) -> (Option<u64>, Vec<RaftOutput>) {
        if self.storage_failure.is_some() {
            return (None, vec![]);
        }
        let Some(index) = self.state.propose(command) else {
            return (None, vec![]);
        };
        if !self.persist_last_entry() {
            return (None, vec![]);
        }
        self.state.try_advance_commit_index();
        let outputs = self.apply_committed();
        (Some(index), outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::FlushFailingBackend;
    use super::*;

    fn test_config() -> RaftConfig {
        RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            startup_grace_period_ms: 0,
        }
    }

    fn make_node(id: u16) -> RaftNode {
        let node_id = NodeId::validated(id).unwrap();
        RaftNode::create(node_id, test_config())
    }

    fn heartbeat_from(leader: u16, term: u64, prev_log_index: u64) -> AppendEntriesRequest {
        AppendEntriesRequest::create(term, leader, prev_log_index, 0, Vec::new(), 0)
    }

    #[test]
    fn candidate_stepping_down_in_same_term_keeps_its_vote() {
        let mut node = make_node(2);
        node.add_peer(NodeId::validated(1).unwrap());
        node.add_peer(NodeId::validated(3).unwrap());
        node.add_peer(NodeId::validated(4).unwrap());
        node.add_peer(NodeId::validated(5).unwrap());
        node.tick(0);
        node.tick(1000);
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);

        let _ = node.handle_append_entries(
            NodeId::validated(1).unwrap(),
            heartbeat_from(1, 1, 257),
            1100,
        );
        assert_eq!(node.role(), RaftRole::Follower);
        assert_eq!(node.current_term(), 1);

        let request = RequestVoteRequest::create(1, 5, 0, 0);
        let (response, _) = node.handle_request_vote(NodeId::validated(5).unwrap(), request, 1150);
        assert!(
            !response.is_granted(),
            "a node that voted for itself in term 1 must not grant a second term-1 vote"
        );
    }

    #[test]
    fn first_tick_starts_the_election_timer_instead_of_firing_it() {
        let mut node = make_node(2);
        node.add_peer(NodeId::validated(1).unwrap());
        let now = 1_790_000_000_000;

        let outputs = node.tick(now);
        assert!(outputs.is_empty());
        assert_eq!(node.role(), RaftRole::Follower);

        let outputs = node.tick(now + 301);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
        );
    }

    fn first_campaign_time(node_id: u16, start_ms: u64) -> Option<u64> {
        let mut node = make_node(node_id);
        node.add_peer(NodeId::validated(if node_id == 1 { 2 } else { 1 }).unwrap());
        node.tick(start_ms);
        (start_ms..=start_ms + 300).find(|&now| {
            node.tick(now)
                .iter()
                .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
        })
    }

    #[test]
    fn nodes_started_together_do_not_campaign_together() {
        let start = 1_790_000_000_000;
        let campaigns: Vec<_> = (1..=5).map(|id| first_campaign_time(id, start)).collect();
        assert!(campaigns.iter().all(Option::is_some));
        let mut distinct = campaigns.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            campaigns.len(),
            "first election timeouts must differ per node: {campaigns:?}"
        );
    }

    #[test]
    fn election_timeout_is_redrawn_for_every_election() {
        let mut node = make_node(3);
        node.add_peer(NodeId::validated(1).unwrap());
        node.add_peer(NodeId::validated(2).unwrap());
        node.tick(0);
        let campaigns: Vec<u64> = (1..=2000)
            .filter(|&now| {
                node.tick(now)
                    .iter()
                    .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
            })
            .collect();
        let intervals: Vec<u64> = campaigns.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(intervals.len() >= 4, "campaigns: {campaigns:?}");
        assert!(
            intervals.windows(2).any(|w| w[0] != w[1]),
            "every election used the same timeout: {intervals:?}"
        );
    }

    #[test]
    fn snapshot_older_than_the_commit_index_is_ignored() {
        let peer1 = NodeId::validated(1).unwrap();
        let mut follower = make_node(2);
        follower.add_peer(peer1);
        let entries = (1..=5)
            .map(|i| LogEntry::create(i, 1, RaftCommand::Noop))
            .collect();
        let append = AppendEntriesRequest::create(1, 1, 0, 0, entries, 5);
        let (response, _) = follower.handle_append_entries(peer1, append, 100);
        assert!(response.is_success());
        assert_eq!(follower.commit_index(), 5);

        let request = InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            snapshot: RaftSnapshot::capture(3, 1, &crate::cluster::PartitionMap::new(), &[peer1]),
        };
        let (response, outputs) = follower.handle_install_snapshot(peer1, request, 200);
        assert!(response.is_success());
        assert_eq!(response.match_index, 5);
        assert!(
            !outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::InstallSnapshot(_)))
        );
        assert_eq!(follower.last_log_index(), 5);
    }

    #[test]
    fn persisted_log_without_its_prefix_is_discarded_on_restart() {
        let backend: Arc<dyn StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
        let storage = RaftStorage::new(backend.clone());
        storage.persist_state(1, None).unwrap();
        for index in 272..=275 {
            storage
                .append_log_entry(&LogEntry::create(index, 1, RaftCommand::Noop))
                .unwrap();
        }

        let node =
            RaftNode::create_with_storage(NodeId::validated(5).unwrap(), test_config(), backend)
                .unwrap();
        assert_eq!(node.last_log_index(), 0);
        assert!(storage.load_log().unwrap().is_empty());
    }

    #[test]
    fn persisted_log_is_kept_only_up_to_its_first_gap() {
        let backend: Arc<dyn StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
        let storage = RaftStorage::new(backend.clone());
        storage.persist_state(1, None).unwrap();
        for index in [1, 2, 3, 10, 11, 12] {
            storage
                .append_log_entry(&LogEntry::create(index, 1, RaftCommand::Noop))
                .unwrap();
        }

        let node =
            RaftNode::create_with_storage(NodeId::validated(5).unwrap(), test_config(), backend)
                .unwrap();
        assert_eq!(node.last_log_index(), 3);
        let persisted: Vec<u64> = storage
            .load_log()
            .unwrap()
            .iter()
            .map(|e| e.index)
            .collect();
        assert_eq!(persisted, vec![1, 2, 3]);
    }

    #[test]
    fn persisted_log_must_start_right_after_the_snapshot() {
        let backend: Arc<dyn StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
        let storage = RaftStorage::new(backend.clone());
        storage.persist_state(2, None).unwrap();
        let snapshot = RaftSnapshot::capture(5, 2, &crate::cluster::PartitionMap::new(), &[]);
        storage.install_snapshot(&snapshot, false).unwrap();
        for index in [8, 9] {
            storage
                .append_log_entry(&LogEntry::create(index, 2, RaftCommand::Noop))
                .unwrap();
        }

        let node =
            RaftNode::create_with_storage(NodeId::validated(5).unwrap(), test_config(), backend)
                .unwrap();
        assert_eq!(node.last_log_index(), 5);
        assert!(storage.load_log().unwrap().is_empty());
    }

    #[test]
    fn snapshot_that_cannot_be_persisted_is_not_acknowledged() {
        let backend = FlushFailingBackend::shared();
        let peer1 = NodeId::validated(1).unwrap();
        let mut follower = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        follower.add_peer(peer1);
        backend.fail_flushes(true);

        let request = InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            snapshot: RaftSnapshot::capture(40, 1, &crate::cluster::PartitionMap::new(), &[peer1]),
        };
        let (response, outputs) = follower.handle_install_snapshot(peer1, request, 100);
        assert!(!response.is_success());
        assert!(
            !outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::InstallSnapshot(_)))
        );
        assert_eq!(follower.commit_index(), 0);
        assert!(follower.storage_failure().is_some());
    }

    fn noop_entries(range: std::ops::RangeInclusive<u64>, term: u64) -> Vec<LogEntry> {
        range
            .map(|index| LogEntry::create(index, term, RaftCommand::Noop))
            .collect()
    }

    #[test]
    fn entries_that_cannot_be_persisted_are_not_acknowledged() {
        let backend = FlushFailingBackend::shared();
        let peer1 = NodeId::validated(1).unwrap();
        let mut follower = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        follower.add_peer(peer1);
        backend.fail_flushes(true);

        let append = AppendEntriesRequest::create(1, 1, 0, 0, noop_entries(1..=3, 1), 3);
        let (response, _) = follower.handle_append_entries(peer1, append.clone(), 100);
        assert!(!response.is_success());
        assert_eq!(follower.last_log_index(), 0);
        assert_eq!(follower.commit_index(), 0);
        assert!(follower.storage_failure().is_some());

        backend.fail_flushes(false);
        let (response, _) = follower.handle_append_entries(peer1, append, 200);
        assert!(!response.is_success());
        assert!(RaftStorage::new(backend).load_log().unwrap().is_empty());
    }

    #[test]
    fn snapshot_keeps_acknowledged_entries_after_its_point() {
        let backend: Arc<dyn StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
        let peer1 = NodeId::validated(1).unwrap();
        let mut follower = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        follower.add_peer(peer1);
        let append = AppendEntriesRequest::create(1, 1, 0, 0, noop_entries(1..=10, 1), 3);
        let (response, _) = follower.handle_append_entries(peer1, append, 100);
        assert_eq!(response.match_index, 10);

        let request = InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            snapshot: RaftSnapshot::capture(6, 1, &crate::cluster::PartitionMap::new(), &[peer1]),
        };
        let (response, _) = follower.handle_install_snapshot(peer1, request, 200);
        assert!(response.is_success());
        assert_eq!(follower.last_log_index(), 10);
        let persisted: Vec<u64> = RaftStorage::new(backend)
            .load_log()
            .unwrap()
            .iter()
            .map(|e| e.index)
            .collect();
        assert_eq!(persisted, vec![7, 8, 9, 10]);
    }

    #[test]
    fn success_reports_only_the_range_the_request_verified() {
        let peer1 = NodeId::validated(1).unwrap();
        let mut follower = make_node(2);
        follower.add_peer(peer1);
        let stale = AppendEntriesRequest::create(1, 1, 0, 0, noop_entries(1..=10, 1), 0);
        let _ = follower.handle_append_entries(peer1, stale, 100);

        let append = AppendEntriesRequest::create(2, 1, 5, 1, noop_entries(6..=7, 1), 0);
        let (response, _) = follower.handle_append_entries(peer1, append, 200);
        assert!(response.is_success());
        assert_eq!(response.match_index, 7);
    }

    #[test]
    fn vote_that_cannot_be_persisted_is_not_granted() {
        let backend = FlushFailingBackend::shared();
        let mut node = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        node.add_peer(NodeId::validated(1).unwrap());
        backend.fail_flushes(true);

        let request = RequestVoteRequest::create(1, 1, 0, 0);
        let (response, outputs) =
            node.handle_request_vote(NodeId::validated(1).unwrap(), request, 100);
        assert!(!response.is_granted());
        assert!(outputs.is_empty());
        assert!(node.storage_failure().is_some());
        assert!(node.tick(10_000).is_empty());
    }

    #[test]
    fn same_term_vote_that_cannot_be_persisted_is_not_granted() {
        let backend = FlushFailingBackend::shared();
        let peer1 = NodeId::validated(1).unwrap();
        let peer3 = NodeId::validated(3).unwrap();
        let mut node = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        node.add_peer(peer1);
        node.add_peer(peer3);
        let heartbeat = AppendEntriesRequest::create(1, 1, 0, 0, Vec::new(), 0);
        let (response, _) = node.handle_append_entries(peer1, heartbeat, 100);
        assert!(response.is_success());
        assert_eq!(node.current_term(), 1);
        backend.fail_flushes(true);

        let request = RequestVoteRequest::create(1, 3, 0, 0);
        let (response, _) = node.handle_request_vote(peer3, request, 200);
        assert!(!response.is_granted());
        assert!(node.storage_failure().is_some());
    }

    #[test]
    fn election_whose_term_cannot_be_persisted_sends_no_requests() {
        let backend = FlushFailingBackend::shared();
        let mut node = RaftNode::create_with_storage(
            NodeId::validated(2).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        node.add_peer(NodeId::validated(1).unwrap());
        node.tick(0);
        backend.fail_flushes(true);

        let outputs = node.tick(1000);
        assert!(
            !outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
        );
        assert!(node.storage_failure().is_some());
    }

    #[test]
    fn leader_entry_that_cannot_be_persisted_is_not_committed() {
        let backend = FlushFailingBackend::shared();
        let mut leader = RaftNode::create_with_storage(
            NodeId::validated(1).unwrap(),
            test_config(),
            backend.clone(),
        )
        .unwrap();
        leader.tick(0);
        leader.tick(1000);
        assert!(leader.is_leader());
        let committed = leader.commit_index();
        backend.fail_flushes(true);

        let (index, outputs) = leader.propose(RaftCommand::Noop);
        assert!(index.is_none());
        assert!(outputs.is_empty());
        assert_eq!(leader.commit_index(), committed);
        assert!(leader.storage_failure().is_some());
        assert!(leader.propose(RaftCommand::Noop).0.is_none());
    }

    #[test]
    fn starts_as_follower() {
        let node = make_node(1);
        assert_eq!(node.role(), RaftRole::Follower);
    }

    #[test]
    fn starts_election_on_timeout() {
        let mut node = make_node(1);
        node.add_peer(NodeId::validated(2).unwrap());
        node.add_peer(NodeId::validated(3).unwrap());

        node.tick(0);
        let outputs = node.tick(1000);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
        );
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn wins_election_with_quorum() {
        let mut node = make_node(1);
        let peer2 = NodeId::validated(2).unwrap();
        let peer3 = NodeId::validated(3).unwrap();
        node.add_peer(peer2);
        node.add_peer(peer3);

        node.tick(0);
        node.tick(1000);
        assert_eq!(node.role(), RaftRole::Candidate);

        let response = RequestVoteResponse::granted(1);
        let outputs = node.handle_request_vote_response(peer2, response);

        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::BecameLeader))
        );
        assert_eq!(node.role(), RaftRole::Leader);
    }

    #[test]
    fn steps_down_on_higher_term() {
        let mut node = make_node(1);
        node.add_peer(NodeId::validated(2).unwrap());
        node.tick(0);
        node.tick(1000);
        assert_eq!(node.current_term(), 1);

        let request = AppendEntriesRequest::heartbeat(5, 2, 0, 0, 0);
        let (_, outputs) = node.handle_append_entries(NodeId::validated(2).unwrap(), request, 2000);

        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::BecameFollower { .. }))
        );
        assert_eq!(node.role(), RaftRole::Follower);
        assert_eq!(node.current_term(), 5);
    }

    #[test]
    fn leader_sends_heartbeats() {
        let mut node = make_node(1);
        let peer2 = NodeId::validated(2).unwrap();
        node.add_peer(peer2);

        node.tick(0);
        node.tick(1000);
        let response = RequestVoteResponse::granted(1);
        node.handle_request_vote_response(peer2, response);
        assert_eq!(node.role(), RaftRole::Leader);

        let outputs = node.tick(1100);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendAppendEntries { .. }))
        );
    }

    #[test]
    fn follower_applies_committed_entries() {
        let mut leader = make_node(1);
        let mut follower = make_node(2);
        let peer2 = NodeId::validated(2).unwrap();
        let peer1 = NodeId::validated(1).unwrap();

        leader.add_peer(peer2);
        follower.add_peer(peer1);

        leader.tick(0);
        leader.tick(1000);
        let response = RequestVoteResponse::granted(1);
        leader.handle_request_vote_response(peer2, response);

        leader.propose(RaftCommand::Noop);

        let outputs = leader.tick(1100);
        let append_req = outputs
            .iter()
            .find_map(|o| match o {
                RaftOutput::SendAppendEntries { request, .. } => Some(request.clone()),
                _ => None,
            })
            .unwrap();

        let (resp, _) = follower.handle_append_entries(peer1, append_req.clone(), 1100);
        assert!(resp.is_success());

        leader.handle_append_entries_response(peer2, resp);

        let append_with_commit = leader
            .tick(1200)
            .into_iter()
            .find_map(|o| match o {
                RaftOutput::SendAppendEntries { request, .. } => Some(request),
                _ => None,
            })
            .unwrap();

        let (_, outputs) = follower.handle_append_entries(peer1, append_with_commit, 1200);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::ApplyCommand(_)))
        );
    }

    #[test]
    fn no_election_without_peers_during_grace_period() {
        let config = RaftConfig {
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 50,
            startup_grace_period_ms: 5000,
        };
        let node_id = NodeId::validated(1).unwrap();
        let mut node = RaftNode::create(node_id, config);

        let outputs = node.tick(0);
        assert!(outputs.is_empty());
        assert_eq!(node.role(), RaftRole::Follower);

        let outputs = node.tick(1000);
        assert!(outputs.is_empty());
        assert_eq!(node.role(), RaftRole::Follower);

        let outputs = node.tick(3000);
        assert!(outputs.is_empty());
        assert_eq!(node.role(), RaftRole::Follower);
    }

    #[test]
    fn election_allowed_after_grace_period_without_peers() {
        let config = RaftConfig {
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 50,
            startup_grace_period_ms: 1000,
        };
        let node_id = NodeId::validated(1).unwrap();
        let mut node = RaftNode::create(node_id, config);

        node.tick(0);
        assert_eq!(node.role(), RaftRole::Follower);

        let outputs = node.tick(500);
        assert!(outputs.is_empty());
        assert_eq!(node.role(), RaftRole::Follower);

        let outputs = node.tick(1500);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::BecameLeader))
        );
        assert_eq!(node.role(), RaftRole::Leader);
    }

    #[test]
    fn election_allowed_immediately_with_peers() {
        let config = RaftConfig {
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 50,
            startup_grace_period_ms: 10000,
        };
        let node_id = NodeId::validated(1).unwrap();
        let mut node = RaftNode::create(node_id, config);
        node.add_peer(NodeId::validated(2).unwrap());

        node.tick(0);

        let outputs = node.tick(500);
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendRequestVote { .. }))
        );
        assert_eq!(node.role(), RaftRole::Candidate);
    }
}
