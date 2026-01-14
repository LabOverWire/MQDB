use super::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::state::{LogEntry, RaftCommand, RaftRole, RaftState};
use super::storage::RaftStorage;
use crate::cluster::NodeId;
use crate::error::Result;
use crate::storage::StorageBackend;
use std::sync::Arc;

#[derive(Debug)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 3000,
            election_timeout_max_ms: 5000,
            heartbeat_interval_ms: 500,
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

        let state = RaftState::recover(node_id, current_term, voted_for, log);
        let timeout = config.election_timeout_min_ms;

        Ok(Self {
            state,
            config,
            storage: Some(storage),
            last_heartbeat_time: 0,
            election_timeout: timeout,
            last_election_time: 0,
            random_seed: u64::from(node_id.get()),
        })
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

    fn persist_state(&self) {
        if let Some(ref storage) = self.storage {
            let _ = storage.persist_state(self.state.current_term(), self.state.voted_for());
        }
    }

    fn persist_log_entry(&self, entry: &LogEntry) {
        if let Some(ref storage) = self.storage {
            let _ = storage.append_log_entry(entry);
        }
    }

    fn persist_log_entries(&self, entries: &[LogEntry]) {
        if let Some(ref storage) = self.storage {
            let _ = storage.append_log_entries_batch(entries);
        }
    }

    fn propose_leader_noop(&mut self) {
        if let Some(_index) = self.state.propose(RaftCommand::Noop)
            && let Some(entry) = self.state.last_log_entry()
        {
            self.persist_log_entry(entry);
            self.state.try_advance_commit_index();
        }
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        match self.state.role() {
            RaftRole::Follower | RaftRole::Candidate => {
                if now_ms >= self.last_heartbeat_time + self.election_timeout {
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
        self.persist_state();
        self.last_election_time = now_ms;
        self.last_heartbeat_time = now_ms;
        self.reset_election_timeout();

        if self.state.has_quorum() {
            self.state.become_leader();
            self.propose_leader_noop();
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

    fn apply_committed(&mut self) -> Vec<RaftOutput> {
        self.state
            .pending_commands()
            .into_iter()
            .map(RaftOutput::ApplyCommand)
            .collect()
    }

    pub fn handle_request_vote(
        &mut self,
        from: NodeId,
        request: RequestVoteRequest,
        now_ms: u64,
    ) -> (RequestVoteResponse, Vec<RaftOutput>) {
        let mut outputs = Vec::new();

        if from.get() != request.candidate_id {
            return (
                RequestVoteResponse::rejected(self.state.current_term()),
                outputs,
            );
        }

        if request.term > self.state.current_term() {
            self.state.become_follower(request.term, None);
            self.persist_state();
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
                self.persist_state();
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

        if response.term > self.state.current_term() {
            self.state.become_follower(response.term, None);
            self.persist_state();
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
            self.propose_leader_noop();
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

        if from.get() != request.leader_id {
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
            self.persist_state();
            outputs.push(RaftOutput::BecameFollower { leader });
        } else if self.state.leader_id() != leader {
            self.state.set_leader(leader);
        }

        self.last_heartbeat_time = now_ms;
        self.reset_election_timeout();

        self.persist_log_entries(&request.entries);

        let success = self.state.append_entries(
            request.prev_log_index,
            request.prev_log_term,
            request.entries,
        );

        if success {
            self.state.update_commit_index(request.leader_commit);
            outputs.extend(self.apply_committed());

            (
                AppendEntriesResponse::success(
                    self.state.current_term(),
                    self.state.last_log_index(),
                ),
                outputs,
            )
        } else {
            (
                AppendEntriesResponse::failure(self.state.current_term()),
                outputs,
            )
        }
    }

    pub fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        response: AppendEntriesResponse,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        if response.term > self.state.current_term() {
            self.state.become_follower(response.term, None);
            self.persist_state();
            outputs.push(RaftOutput::BecameFollower { leader: None });
            return outputs;
        }

        if self.state.role() != RaftRole::Leader {
            return outputs;
        }

        if response.is_success() {
            self.state.update_next_index(from, response.match_index + 1);
            self.state.update_match_index(from, response.match_index);
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
        let Some(index) = self.state.propose(command) else {
            return (None, vec![]);
        };
        if let Some(entry) = self.state.last_log_entry() {
            self.persist_log_entry(entry);
        }
        self.state.try_advance_commit_index();
        let outputs = self.apply_committed();
        (Some(index), outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RaftConfig {
        RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }

    fn make_node(id: u16) -> RaftNode {
        let node_id = NodeId::validated(id).unwrap();
        RaftNode::create(node_id, test_config())
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
}
