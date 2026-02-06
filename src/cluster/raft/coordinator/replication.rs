use super::super::node::RaftOutput;
use super::super::state::RaftCommand;
use super::RaftCoordinator;
use crate::cluster::rebalancer::{
    RebalanceConfig, compute_balanced_assignments, compute_incremental_assignments,
    compute_removal_assignments,
};
use crate::cluster::transport::{ClusterMessage, ClusterTransport};
use crate::cluster::{Epoch, NodeId, PartitionId, PartitionRole};

impl<T: ClusterTransport> RaftCoordinator<T> {
    pub(super) async fn process_outputs(&mut self, outputs: Vec<RaftOutput>) {
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

    pub(super) fn apply_command_local(&mut self, command: &RaftCommand) {
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

    pub(super) async fn trigger_rebalance_for_new_node(&mut self) -> Vec<u64> {
        self.trigger_rebalance_internal().await
    }

    pub(super) async fn trigger_rebalance_internal(&mut self) -> Vec<u64> {
        let config = RebalanceConfig::default();

        let reassignments = if self.partitions_initialized() {
            compute_incremental_assignments(&self.partition_map, &self.cluster_members, &config)
        } else {
            tracing::info!("initializing partitions for fresh cluster");
            let new_map = compute_balanced_assignments(&self.cluster_members, &config, Epoch::ZERO);
            PartitionId::all()
                .filter_map(|p| {
                    let assignment = new_map.get(p);
                    assignment.primary.map(|primary| {
                        crate::cluster::rebalancer::PartitionReassignment {
                            partition: p,
                            old_primary: None,
                            new_primary: primary,
                            old_replicas: Vec::new(),
                            new_replicas: assignment.replicas.clone(),
                            new_epoch: assignment.epoch,
                        }
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

    pub(super) async fn trigger_drain_rebalance(&mut self, draining_node: NodeId) -> Vec<u64> {
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

    pub(super) async fn reassign_partitions_for_dead_node(
        &mut self,
        dead_node: NodeId,
    ) -> Vec<u64> {
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
