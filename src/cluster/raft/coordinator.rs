use super::node::{RaftConfig, RaftNode, RaftOutput};
use super::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::state::{PartitionUpdate, RaftCommand};
use crate::cluster::rebalancer::{
    PartitionReassignment, RebalanceConfig, compute_balanced_assignments,
    compute_incremental_assignments, compute_removal_assignments,
};
use crate::cluster::transport::{ClusterMessage, ClusterTransport, TransportError};
use crate::cluster::{Epoch, NodeId, PartitionId, PartitionMap, PartitionRole};
use crate::storage::StorageBackend;
use std::collections::HashSet;
use std::sync::Arc;

pub struct RaftCoordinator<T: ClusterTransport> {
    node: RaftNode,
    partition_map: PartitionMap,
    transport: T,
    cluster_members: Vec<NodeId>,
    pending_dead_nodes: HashSet<NodeId>,
    processed_dead_nodes: HashSet<NodeId>,
    pending_draining_nodes: HashSet<NodeId>,
    processed_draining_nodes: HashSet<NodeId>,
    pending_new_nodes: HashSet<NodeId>,
    processed_new_nodes: HashSet<NodeId>,
    was_leader: bool,
    pending_partition_proposals: usize,
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

    async fn process_pending_draining_nodes(&mut self) -> Vec<u64> {
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

    async fn process_pending_new_nodes(&mut self) -> Vec<u64> {
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

    async fn process_pending_dead_nodes(&mut self) -> Vec<u64> {
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

    async fn scan_partition_map_for_dead_primaries(&mut self, proposed_indices: &mut Vec<u64>) {
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

    async fn process_outputs(&mut self, outputs: Vec<RaftOutput>) {
        for output in outputs {
            self.process_output(output).await;
        }
    }

    async fn process_output(&mut self, output: RaftOutput) {
        match output {
            RaftOutput::SendRequestVote { to, request } => {
                tracing::debug!(to = to.get(), term = request.term, "sending RequestVote");
                let _ = self
                    .transport
                    .send(to, ClusterMessage::RequestVote(request))
                    .await;
            }
            RaftOutput::SendAppendEntries { to, request } => {
                tracing::debug!(
                    to = to.get(),
                    term = request.term,
                    entries = request.entries.len(),
                    "sending AppendEntries"
                );
                let _ = self
                    .transport
                    .send(to, ClusterMessage::AppendEntries(request))
                    .await;
            }
            RaftOutput::ApplyCommand(cmd) => {
                tracing::info!(?cmd, "applying Raft command");
                self.apply_command(cmd).await;
            }
            RaftOutput::BecameLeader => {
                tracing::info!(node = self.node.node_id().get(), "became Raft leader");
            }
            RaftOutput::BecameFollower { leader } => {
                tracing::info!(
                    node = self.node.node_id().get(),
                    ?leader,
                    "became Raft follower"
                );
            }
        }
    }

    fn apply_command_local(&mut self, command: &RaftCommand) {
        match command {
            RaftCommand::UpdatePartition(update) => {
                self.partition_map.apply_update(update);
            }
            RaftCommand::AddNode { node_id } => {
                if let Some(node) = NodeId::validated(*node_id)
                    && !self.cluster_members.contains(&node)
                {
                    self.cluster_members.push(node);
                }
            }
            RaftCommand::RemoveNode { node_id } => {
                if let Some(node) = NodeId::validated(*node_id) {
                    self.cluster_members.retain(|&n| n != node);
                }
            }
            RaftCommand::Noop => {}
        }
    }

    async fn apply_command(&mut self, command: RaftCommand) {
        self.apply_command_local(&command);
        if let RaftCommand::UpdatePartition(update) = command {
            let _ = self
                .transport
                .broadcast(ClusterMessage::PartitionUpdate(update))
                .await;

            self.pending_partition_proposals = self.pending_partition_proposals.saturating_sub(1);
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

    pub async fn handle_node_death(&mut self, dead_node: NodeId) -> Vec<u64> {
        self.cluster_members.retain(|&n| n != dead_node);

        if !self.is_leader() {
            tracing::info!(
                ?dead_node,
                "not leader, queuing dead node for later processing"
            );
            self.pending_dead_nodes.insert(dead_node);
            return Vec::new();
        }

        if !self.processed_dead_nodes.insert(dead_node) {
            tracing::debug!(?dead_node, "already processed dead node, skipping");
            return Vec::new();
        }

        self.reassign_partitions_for_dead_node(dead_node).await
    }

    pub async fn handle_node_alive(&mut self, node: NodeId) -> Vec<u64> {
        let was_dead =
            self.pending_dead_nodes.remove(&node) || self.processed_dead_nodes.remove(&node);
        if was_dead {
            tracing::debug!(?node, "node came back from dead state");
            self.processed_new_nodes.remove(&node);
        }

        let is_new_member = !self.cluster_members.contains(&node);
        if is_new_member {
            self.cluster_members.push(node);
            self.node.add_peer(node);
            tracing::info!(?node, "added new node as Raft peer");
        }

        let node_has_partitions = self.node_has_partitions(node);
        let partitions_initialized = self.partitions_initialized();
        let not_yet_processed = !self.processed_new_nodes.contains(&node);

        if !node_has_partitions && !partitions_initialized && not_yet_processed {
            self.pending_new_nodes.insert(node);
            tracing::debug!(
                ?node,
                "partitions not initialized yet, queuing node for later"
            );
            return Vec::new();
        }

        let needs_rebalance = !node_has_partitions && partitions_initialized && not_yet_processed;

        if !self.is_leader() {
            if needs_rebalance {
                tracing::info!(?node, "not leader, queuing node for later rebalance");
                self.pending_new_nodes.insert(node);
            }
            return Vec::new();
        }

        if needs_rebalance {
            self.processed_new_nodes.insert(node);
            if self.pending_partition_proposals > 0 {
                tracing::info!(
                    ?node,
                    pending = self.pending_partition_proposals,
                    "node needs partitions but rebalance in progress, queuing for later"
                );
                self.pending_new_nodes.insert(node);
                return Vec::new();
            }
            tracing::info!(?node, "node has no partitions, triggering rebalance");
            return self.trigger_rebalance_for_new_node().await;
        }

        Vec::new()
    }

    pub async fn handle_drain_notification(&mut self, draining_node: NodeId) -> Vec<u64> {
        if !self.is_leader() {
            tracing::info!(
                ?draining_node,
                "not leader, queuing draining node for later processing"
            );
            self.pending_draining_nodes.insert(draining_node);
            return Vec::new();
        }

        if !self.processed_draining_nodes.insert(draining_node) {
            tracing::debug!(?draining_node, "already processed draining node, skipping");
            return Vec::new();
        }

        self.trigger_drain_rebalance(draining_node).await
    }

    fn node_has_partitions(&self, node: NodeId) -> bool {
        for partition in PartitionId::all() {
            let role = self.partition_map.role_for(partition, node);
            if role != PartitionRole::None {
                return true;
            }
        }
        false
    }

    pub fn partitions_initialized(&self) -> bool {
        PartitionId::all().any(|p| self.partition_map.primary(p).is_some())
    }

    async fn trigger_rebalance_for_new_node(&mut self) -> Vec<u64> {
        self.trigger_rebalance_internal().await
    }

    async fn trigger_rebalance_internal(&mut self) -> Vec<u64> {
        let config = RebalanceConfig::default();

        let reassignments = if self.partitions_initialized() {
            compute_incremental_assignments(&self.partition_map, &self.cluster_members, &config)
        } else {
            tracing::info!("initializing partitions for fresh cluster");
            let new_map = compute_balanced_assignments(&self.cluster_members, &config, Epoch::ZERO);
            PartitionId::all()
                .filter_map(|p| {
                    let assignment = new_map.get(p);
                    assignment.primary.map(|primary| PartitionReassignment {
                        partition: p,
                        old_primary: None,
                        new_primary: primary,
                        old_replicas: Vec::new(),
                        new_replicas: assignment.replicas.clone(),
                        new_epoch: assignment.epoch,
                    })
                })
                .collect()
        };

        if reassignments.is_empty() {
            tracing::debug!("no partition reassignments needed");
            return Vec::new();
        }

        tracing::info!(
            count = reassignments.len(),
            "proposing partition reassignments"
        );

        let mut proposed_indices = Vec::new();
        for reassignment in reassignments {
            let cmd = RaftCommand::update_partition(
                reassignment.partition,
                reassignment.new_primary,
                &reassignment.new_replicas,
                reassignment.new_epoch,
            );

            if let Ok(idx) = self.propose_partition_update(cmd).await {
                proposed_indices.push(idx);
            }
        }

        self.pending_partition_proposals = proposed_indices.len();
        if self.pending_partition_proposals > 0 {
            tracing::debug!(
                count = self.pending_partition_proposals,
                "tracking pending partition proposals"
            );
        }

        proposed_indices
    }

    pub async fn force_rebalance(&mut self) -> Vec<u64> {
        if !self.is_leader() {
            tracing::warn!("cannot force rebalance - not the Raft leader");
            return Vec::new();
        }

        tracing::info!(
            cluster_members = ?self.cluster_members,
            "forcing partition rebalance"
        );

        self.trigger_rebalance_internal().await
    }

    async fn trigger_drain_rebalance(&mut self, draining_node: NodeId) -> Vec<u64> {
        let remaining_nodes: Vec<NodeId> = self
            .cluster_members
            .iter()
            .filter(|&&n| n != draining_node)
            .copied()
            .collect();

        let config = RebalanceConfig::default();
        let reassignments = compute_removal_assignments(
            &self.partition_map,
            &remaining_nodes,
            draining_node,
            &config,
        );

        if reassignments.is_empty() {
            tracing::debug!(
                ?draining_node,
                "no partition reassignments needed for drain"
            );
            return Vec::new();
        }

        tracing::info!(
            ?draining_node,
            count = reassignments.len(),
            "proposing partition reassignments for draining node"
        );

        let mut proposed_indices = Vec::new();
        for reassignment in reassignments {
            let cmd = RaftCommand::update_partition(
                reassignment.partition,
                reassignment.new_primary,
                &reassignment.new_replicas,
                reassignment.new_epoch,
            );

            if let Ok(idx) = self.propose_partition_update(cmd).await {
                proposed_indices.push(idx);
            }
        }

        proposed_indices
    }

    async fn reassign_partitions_for_dead_node(&mut self, dead_node: NodeId) -> Vec<u64> {
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
                        tracing::info!(
                            ?partition,
                            ?dead_node,
                            ?new_primary,
                            "promoting replica to primary"
                        );
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

                        if let Ok(idx) = self.propose_partition_update(cmd).await {
                            proposed_indices.push(idx);
                        }
                    } else {
                        tracing::error!(?partition, ?dead_node, "no replica available to promote");
                    }
                }
                PartitionRole::Replica => {
                    if let Some(current_primary) = self.partition_map.primary(partition) {
                        tracing::info!(
                            ?partition,
                            ?dead_node,
                            "removing dead replica from partition"
                        );
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

                        if let Ok(idx) = self.propose_partition_update(cmd).await {
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

    #[derive(Debug, Clone)]
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

        async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
            self.outbox.lock().unwrap().push((to, message));
            Ok(())
        }

        async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
            self.outbox.lock().unwrap().push((self.node_id, message));
            Ok(())
        }

        async fn send_to_partition_primary(
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

        fn pending_count(&self) -> usize {
            0
        }

        fn requeue(&self, _msg: InboundMessage) {}

        async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

        async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
    }

    fn test_config() -> RaftConfig {
        RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }

    #[test]
    fn coordinator_creates_as_follower() {
        let node_id = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node_id);
        let coord = RaftCoordinator::new(node_id, transport, test_config());

        assert!(!coord.is_leader());
        assert!(coord.leader_id().is_none());
    }

    #[tokio::test]
    async fn coordinator_election_and_propose() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
        let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

        coord1.add_peer(node2);
        coord2.add_peer(node1);

        coord1.tick(1000).await;
        let sent = coord1.transport.sent_messages();
        assert_eq!(sent.len(), 1);

        let request = match &sent[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };

        let response = coord2.handle_request_vote(node1, request, 1000).await;
        assert!(response.is_granted());

        coord1.handle_request_vote_response(node2, response).await;
        assert!(coord1.is_leader());
    }

    #[tokio::test]
    async fn coordinator_applies_partition_update() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
        let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

        coord1.add_peer(node2);
        coord2.add_peer(node1);

        coord1.tick(1000).await;
        let request = match &coord1.transport.sent_messages()[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };
        let response = coord2.handle_request_vote(node1, request, 1000).await;
        coord1.handle_request_vote_response(node2, response).await;
        assert!(coord1.is_leader());

        let partition = PartitionId::new(5).unwrap();
        let cmd = RaftCommand::update_partition(partition, node1, &[node2], Epoch::new(1));

        let idx = coord1.propose_partition_update(cmd).await.unwrap();
        assert_eq!(idx, 2);

        coord1.transport.clear();
        coord1.tick(1100).await;

        let append_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        let response = coord2
            .handle_append_entries(node1, append_req.clone(), 1100)
            .await;
        assert!(response.is_success());

        coord1.handle_append_entries_response(node2, response).await;

        coord1.transport.clear();
        coord1.tick(1200).await;

        let commit_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        coord2.handle_append_entries(node1, commit_req, 1200).await;

        assert_eq!(coord2.partition_map().primary(partition), Some(node1));
        assert_eq!(coord2.partition_map().replicas(partition), &[node2]);
    }

    #[tokio::test]
    async fn coordinator_rejects_propose_when_not_leader() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut coord = RaftCoordinator::new(node1, transport, test_config());

        let partition = PartitionId::new(0).unwrap();
        let cmd = RaftCommand::update_partition(partition, node1, &[], Epoch::new(1));

        let result = coord.propose_partition_update(cmd).await;
        assert!(matches!(result, Err(CoordinatorError::NotLeader(None))));
    }

    #[tokio::test]
    async fn handle_node_death_reassigns_partitions() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let transport1 = MockTransport::new(node1);
        let transport2 = MockTransport::new(node2);

        let mut coord1 = RaftCoordinator::new(node1, transport1, test_config());
        let mut coord2 = RaftCoordinator::new(node2, transport2, test_config());

        coord1.add_peer(node2);
        coord1.add_peer(node3);
        coord2.add_peer(node1);
        coord2.add_peer(node3);

        coord1.tick(1000).await;
        let request = match &coord1.transport.sent_messages()[0].1 {
            ClusterMessage::RequestVote(req) => *req,
            _ => panic!("expected RequestVote"),
        };
        let response = coord2.handle_request_vote(node1, request, 1000).await;
        coord1.handle_request_vote_response(node2, response).await;
        assert!(coord1.is_leader());

        let partition = PartitionId::new(0).unwrap();
        let cmd = RaftCommand::update_partition(partition, node2, &[node3], Epoch::new(1));
        coord1.propose_partition_update(cmd).await.unwrap();

        coord1.transport.clear();
        coord1.tick(1100).await;

        let append_req = coord1
            .transport
            .sent_messages()
            .iter()
            .find_map(|(_, msg)| match msg {
                ClusterMessage::AppendEntries(req) => Some(req.clone()),
                _ => None,
            })
            .unwrap();

        let response = coord2.handle_append_entries(node1, append_req, 1100).await;
        coord1.handle_append_entries_response(node2, response).await;

        coord1.transport.clear();
        coord1.tick(1200).await;

        assert_eq!(coord1.partition_map().primary(partition), Some(node2));
        assert_eq!(coord1.partition_map().replicas(partition), &[node3]);

        let indices = coord1.handle_node_death(node2).await;
        assert!(!indices.is_empty());
    }

    #[tokio::test]
    async fn handle_node_death_does_nothing_when_not_leader() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut coord = RaftCoordinator::new(node1, transport, test_config());
        coord.add_peer(node2);

        let indices = coord.handle_node_death(node2).await;
        assert!(indices.is_empty());
    }
}
