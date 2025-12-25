use super::node::{RaftConfig, RaftNode, RaftOutput};
use super::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::state::RaftCommand;
use crate::cluster::transport::{ClusterMessage, ClusterTransport, TransportError};
use crate::cluster::{NodeId, PartitionId, PartitionMap, PartitionRole};

pub struct RaftCoordinator<T: ClusterTransport> {
    node: RaftNode,
    partition_map: PartitionMap,
    transport: T,
    cluster_members: Vec<NodeId>,
}

impl<T: ClusterTransport> RaftCoordinator<T> {
    pub fn new(node_id: NodeId, transport: T, config: RaftConfig) -> Self {
        Self {
            node: RaftNode::create(node_id, config),
            partition_map: PartitionMap::new(),
            transport,
            cluster_members: vec![node_id],
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.node.leader_id()
    }

    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    pub fn add_peer(&mut self, peer: NodeId) {
        self.node.add_peer(peer);
        if !self.cluster_members.contains(&peer) {
            self.cluster_members.push(peer);
        }
    }

    pub fn tick(&mut self, now_ms: u64) {
        let outputs = self.node.tick(now_ms);
        self.process_outputs(outputs);
    }

    /// # Errors
    /// Returns `NotLeader` if not the leader, `ProposeFailed` if proposal fails.
    pub fn propose_partition_update(
        &mut self,
        command: RaftCommand,
    ) -> Result<u64, CoordinatorError> {
        if !self.is_leader() {
            return Err(CoordinatorError::NotLeader(self.leader_id()));
        }

        self.node
            .propose(command)
            .ok_or(CoordinatorError::ProposeFailed)
    }

    pub fn handle_request_vote(
        &mut self,
        from: NodeId,
        request: RequestVoteRequest,
        now_ms: u64,
    ) -> RequestVoteResponse {
        tracing::debug!(from = from.get(), term = request.term, "received RequestVote");
        let (response, outputs) = self.node.handle_request_vote(from, request, now_ms);
        tracing::debug!(granted = response.is_granted(), "responding to RequestVote");
        self.process_outputs(outputs);
        response
    }

    pub fn handle_request_vote_response(&mut self, from: NodeId, response: RequestVoteResponse) {
        tracing::debug!(from = from.get(), granted = response.is_granted(), "received RequestVoteResponse");
        let outputs = self.node.handle_request_vote_response(from, response);
        self.process_outputs(outputs);
    }

    pub fn handle_append_entries(
        &mut self,
        from: NodeId,
        request: AppendEntriesRequest,
        now_ms: u64,
    ) -> AppendEntriesResponse {
        tracing::trace!(from = from.get(), term = request.term, entries = request.entries.len(), "received AppendEntries");
        let (response, outputs) = self.node.handle_append_entries(from, request, now_ms);
        self.process_outputs(outputs);
        response
    }

    pub fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        response: AppendEntriesResponse,
    ) {
        tracing::trace!(from = from.get(), success = response.is_success(), "received AppendEntriesResponse");
        let outputs = self.node.handle_append_entries_response(from, response);
        self.process_outputs(outputs);
    }

    fn process_outputs(&mut self, outputs: Vec<RaftOutput>) {
        for output in outputs {
            self.process_output(output);
        }
    }

    fn process_output(&mut self, output: RaftOutput) {
        match output {
            RaftOutput::SendRequestVote { to, request } => {
                tracing::debug!(to = to.get(), term = request.term, "sending RequestVote");
                let _ = self
                    .transport
                    .send(to, ClusterMessage::RequestVote(request));
            }
            RaftOutput::SendAppendEntries { to, request } => {
                tracing::trace!(to = to.get(), term = request.term, "sending AppendEntries");
                let _ = self
                    .transport
                    .send(to, ClusterMessage::AppendEntries(request));
            }
            RaftOutput::ApplyCommand(cmd) => {
                tracing::info!(?cmd, "applying Raft command");
                self.apply_command(cmd);
            }
            RaftOutput::BecameLeader => {
                tracing::info!(node = self.node.node_id().get(), "became Raft leader");
            }
            RaftOutput::BecameFollower { leader } => {
                tracing::info!(node = self.node.node_id().get(), ?leader, "became Raft follower");
            }
        }
    }

    fn apply_command(&mut self, command: RaftCommand) {
        match command {
            RaftCommand::UpdatePartition(update) => {
                self.partition_map.apply_update(&update);
            }
            RaftCommand::AddNode { node_id } => {
                if let Some(node) = NodeId::validated(node_id)
                    && !self.cluster_members.contains(&node)
                {
                    self.cluster_members.push(node);
                }
            }
            RaftCommand::RemoveNode { node_id } => {
                if let Some(node) = NodeId::validated(node_id) {
                    self.cluster_members.retain(|&n| n != node);
                }
            }
            RaftCommand::Noop => {}
        }
    }

    pub fn cluster_members(&self) -> &[NodeId] {
        &self.cluster_members
    }

    /// # Errors
    /// Returns a transport error if sending fails.
    pub fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        self.transport.send(to, message)
    }

    pub fn handle_node_death(&mut self, dead_node: NodeId) -> Vec<u64> {
        if !self.is_leader() {
            return Vec::new();
        }

        let mut proposed_indices = Vec::new();
        let alive_nodes: Vec<NodeId> = self
            .cluster_members
            .iter()
            .filter(|&&n| n != dead_node)
            .copied()
            .collect();

        for partition in PartitionId::all() {
            let role = self.partition_map.role_for(partition, dead_node);

            match role {
                PartitionRole::Primary => {
                    if let Some(new_primary) = self.select_new_primary(partition, &alive_nodes) {
                        let epoch = self.partition_map.epoch(partition).next();
                        let remaining_replicas: Vec<NodeId> = self
                            .partition_map
                            .replicas(partition)
                            .iter()
                            .filter(|&&n| n != dead_node && n != new_primary)
                            .copied()
                            .collect();

                        let cmd = RaftCommand::update_partition(
                            partition,
                            new_primary,
                            &remaining_replicas,
                            epoch,
                        );

                        if let Ok(idx) = self.propose_partition_update(cmd) {
                            proposed_indices.push(idx);
                        }
                    }
                }
                PartitionRole::Replica => {
                    if let Some(current_primary) = self.partition_map.primary(partition) {
                        let epoch = self.partition_map.epoch(partition).next();
                        let remaining_replicas: Vec<NodeId> = self
                            .partition_map
                            .replicas(partition)
                            .iter()
                            .filter(|&&n| n != dead_node)
                            .copied()
                            .collect();

                        let cmd = RaftCommand::update_partition(
                            partition,
                            current_primary,
                            &remaining_replicas,
                            epoch,
                        );

                        if let Ok(idx) = self.propose_partition_update(cmd) {
                            proposed_indices.push(idx);
                        }
                    }
                }
                PartitionRole::None => {}
            }
        }

        proposed_indices
    }

    fn select_new_primary(&self, partition: PartitionId, alive_nodes: &[NodeId]) -> Option<NodeId> {
        let replicas = self.partition_map.replicas(partition);
        replicas
            .iter()
            .find(|r| alive_nodes.contains(r))
            .copied()
            .or_else(|| alive_nodes.first().copied())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::transport::InboundMessage;
    use crate::cluster::{Epoch, PartitionId};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct MockTransport {
        node_id: NodeId,
        outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    }

    impl MockTransport {
        fn new(node_id: NodeId) -> Self {
            Self {
                node_id,
                outbox: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn sent_messages(&self) -> Vec<(NodeId, ClusterMessage)> {
            self.outbox.lock().unwrap().clone()
        }

        fn clear(&self) {
            self.outbox.lock().unwrap().clear();
        }
    }

    impl ClusterTransport for MockTransport {
        fn local_node(&self) -> NodeId {
            self.node_id
        }

        fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
            self.outbox.lock().unwrap().push((to, message));
            Ok(())
        }

        fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
            self.outbox.lock().unwrap().push((self.node_id, message));
            Ok(())
        }

        fn send_to_partition_primary(
            &self,
            _partition: PartitionId,
            _message: ClusterMessage,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        fn recv(&self) -> Option<InboundMessage> {
            None
        }

        fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
            None
        }
    }

    #[test]
    fn coordinator_creates_as_follower() {
        let node_id = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node_id);
        let coord = RaftCoordinator::new(node_id, transport, RaftConfig::default());

        assert!(!coord.is_leader());
        assert!(coord.leader_id().is_none());
    }

    #[test]
    fn coordinator_election_and_propose() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, RaftConfig::default());
        let mut coord2 = RaftCoordinator::new(node2, transport2, RaftConfig::default());

        coord1.add_peer(node2);
        coord2.add_peer(node1);

        coord1.tick(1000);
        let sent = coord1.transport.sent_messages();
        assert_eq!(sent.len(), 1);

        let request = match &sent[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };

        let response = coord2.handle_request_vote(node1, request, 1000);
        assert!(response.is_granted());

        coord1.handle_request_vote_response(node2, response);
        assert!(coord1.is_leader());
    }

    #[test]
    fn coordinator_applies_partition_update() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, RaftConfig::default());
        let mut coord2 = RaftCoordinator::new(node2, transport2, RaftConfig::default());

        coord1.add_peer(node2);
        coord2.add_peer(node1);

        coord1.tick(1000);
        let request = match &coord1.transport.sent_messages()[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };
        let response = coord2.handle_request_vote(node1, request, 1000);
        coord1.handle_request_vote_response(node2, response);
        assert!(coord1.is_leader());

        let partition = PartitionId::new(5).unwrap();
        let cmd = RaftCommand::update_partition(partition, node1, &[node2], Epoch::new(1));

        let idx = coord1.propose_partition_update(cmd).unwrap();
        assert_eq!(idx, 1);

        coord1.transport.clear();
        coord1.tick(1100);

        let append_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        let response = coord2.handle_append_entries(node1, append_req.clone(), 1100);
        assert!(response.is_success());

        coord1.handle_append_entries_response(node2, response);

        coord1.transport.clear();
        coord1.tick(1200);

        let commit_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        coord2.handle_append_entries(node1, commit_req, 1200);

        assert_eq!(coord2.partition_map().primary(partition), Some(node1));
        assert_eq!(coord2.partition_map().replicas(partition), &[node2]);
    }

    #[test]
    fn coordinator_rejects_propose_when_not_leader() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut coord = RaftCoordinator::new(node1, transport, RaftConfig::default());

        let partition = PartitionId::new(0).unwrap();
        let cmd = RaftCommand::update_partition(partition, node1, &[], Epoch::new(1));

        let result = coord.propose_partition_update(cmd);
        assert!(matches!(result, Err(CoordinatorError::NotLeader(None))));
    }

    #[test]
    fn handle_node_death_reassigns_partitions() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, RaftConfig::default());
        let mut coord2 = RaftCoordinator::new(node2, transport2, RaftConfig::default());

        coord1.add_peer(node2);
        coord1.add_peer(node3);
        coord2.add_peer(node1);
        coord2.add_peer(node3);

        coord1.tick(1000);
        let request = match &coord1.transport.sent_messages()[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };
        let response = coord2.handle_request_vote(node1, request, 1000);
        coord1.handle_request_vote_response(node2, response);
        assert!(coord1.is_leader());

        let partition = PartitionId::new(0).unwrap();
        let cmd = RaftCommand::update_partition(partition, node2, &[node3], Epoch::new(1));
        coord1.propose_partition_update(cmd).unwrap();

        coord1.transport.clear();
        coord1.tick(1100);

        let append_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        let response = coord2.handle_append_entries(node1, append_req, 1100);
        coord1.handle_append_entries_response(node2, response);

        coord1.transport.clear();
        coord1.tick(1200);

        assert_eq!(coord1.partition_map().primary(partition), Some(node2));
        assert_eq!(coord1.partition_map().replicas(partition), &[node3]);

        let indices = coord1.handle_node_death(node2);
        assert!(!indices.is_empty());
    }

    #[test]
    fn handle_node_death_does_nothing_when_not_leader() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut coord = RaftCoordinator::new(node1, transport, RaftConfig::default());
        coord.add_peer(node2);

        let indices = coord.handle_node_death(node2);
        assert!(indices.is_empty());
    }
}
