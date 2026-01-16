use crate::cluster::raft::{RaftCommand, RaftCoordinator};
use crate::cluster::transport::ClusterTransport;
use crate::cluster::{ClusterMessage, Epoch, NodeId, PartitionId, PartitionMap, NUM_PARTITIONS};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::interval;
use tracing::info;

use super::node_controller::RaftMessage;
use super::raft::PartitionUpdate;

#[allow(clippy::cast_possible_truncation)]
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default)]
pub struct RaftStatus {
    pub is_leader: bool,
    pub current_term: u64,
    pub leader_id: Option<NodeId>,
    pub log_len: usize,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
}

#[derive(Debug)]
pub enum RaftEvent {
    NodeAlive(NodeId),
    NodeDead(NodeId),
    DrainNotification(NodeId),
    ExternalUpdate(PartitionUpdate),
}

pub enum RaftAdminCommand {
    ForceRebalance(oneshot::Sender<usize>),
}

pub struct RaftTask<T: ClusterTransport> {
    raft: RaftCoordinator<T>,
    rx_messages: flume::Receiver<RaftMessage>,
    rx_events: flume::Receiver<RaftEvent>,
    rx_admin: flume::Receiver<RaftAdminCommand>,
    tx_partition_map: watch::Sender<PartitionMap>,
    tx_status: watch::Sender<RaftStatus>,
    shutdown_rx: broadcast::Receiver<()>,
    all_nodes: Vec<NodeId>,
    partitions_initialized: bool,
}

impl<T: ClusterTransport> RaftTask<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft: RaftCoordinator<T>,
        rx_messages: flume::Receiver<RaftMessage>,
        rx_events: flume::Receiver<RaftEvent>,
        rx_admin: flume::Receiver<RaftAdminCommand>,
        tx_partition_map: watch::Sender<PartitionMap>,
        tx_status: watch::Sender<RaftStatus>,
        shutdown_rx: broadcast::Receiver<()>,
        all_nodes: Vec<NodeId>,
    ) -> Self {
        Self {
            raft,
            rx_messages,
            rx_events,
            rx_admin,
            tx_partition_map,
            tx_status,
            shutdown_rx,
            all_nodes,
            partitions_initialized: false,
        }
    }

    pub async fn run(mut self) {
        let mut tick_interval = interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown_rx.recv() => {
                    info!("Raft task shutting down");
                    break;
                }
                _ = tick_interval.tick() => {
                    self.handle_tick().await;
                }
                Ok(msg) = self.rx_messages.recv_async() => {
                    self.handle_raft_message(msg).await;
                }
                Ok(event) = self.rx_events.recv_async() => {
                    self.handle_event(event).await;
                }
                Ok(cmd) = self.rx_admin.recv_async() => {
                    self.handle_admin_command(cmd).await;
                }
            }
        }
    }

    async fn handle_admin_command(&mut self, cmd: RaftAdminCommand) {
        match cmd {
            RaftAdminCommand::ForceRebalance(response_tx) => {
                let proposals = if self.raft.is_leader() {
                    self.raft.force_rebalance().await.len()
                } else {
                    0
                };
                let _ = response_tx.send(proposals);
            }
        }
    }

    async fn handle_tick(&mut self) {
        let now = current_time_ms();

        if self.raft.is_leader() && !self.partitions_initialized {
            self.initialize_partitions().await;
        }

        let leader_proposals = self.raft.tick(now).await;
        if !leader_proposals.is_empty() {
            info!(
                proposals = leader_proposals.len(),
                "new Raft leader proposed partition reassignments"
            );
        }

        let _ = self.tx_partition_map.send(self.raft.partition_map().clone());
        let _ = self.tx_status.send(RaftStatus {
            is_leader: self.raft.is_leader(),
            current_term: self.raft.current_term(),
            leader_id: self.raft.leader_id(),
            log_len: self.raft.log_len(),
            commit_index: self.raft.commit_index(),
            last_applied: self.raft.last_applied(),
            last_log_index: self.raft.last_log_index(),
        });
    }

    async fn handle_raft_message(&mut self, msg: RaftMessage) {
        let now = current_time_ms();
        match msg {
            RaftMessage::RequestVote { from, request } => {
                let response = self.raft.handle_request_vote(from, request, now).await;
                let _ = self
                    .raft
                    .send(from, ClusterMessage::RequestVoteResponse(response))
                    .await;
            }
            RaftMessage::RequestVoteResponse { from, response } => {
                self.raft.handle_request_vote_response(from, response).await;
            }
            RaftMessage::AppendEntries { from, request } => {
                let response = self.raft.handle_append_entries(from, request, now).await;
                let _ = self
                    .raft
                    .send(from, ClusterMessage::AppendEntriesResponse(response))
                    .await;
            }
            RaftMessage::AppendEntriesResponse { from, response } => {
                self.raft
                    .handle_append_entries_response(from, response)
                    .await;
            }
        }
    }

    async fn handle_event(&mut self, event: RaftEvent) {
        match event {
            RaftEvent::NodeAlive(node) => {
                let rebalance_proposals = self.raft.handle_node_alive(node).await;
                if !rebalance_proposals.is_empty() {
                    info!(
                        ?node,
                        count = rebalance_proposals.len(),
                        "triggered rebalance for new node"
                    );
                }
            }
            RaftEvent::NodeDead(node) => {
                let proposed = self.raft.handle_node_death(node).await;
                if !proposed.is_empty() {
                    info!(
                        ?node,
                        proposals = proposed.len(),
                        "Raft leader proposing partition reassignments for dead node"
                    );
                }
            }
            RaftEvent::DrainNotification(node) => {
                let proposed = self.raft.handle_drain_notification(node).await;
                if !proposed.is_empty() {
                    info!(
                        ?node,
                        proposals = proposed.len(),
                        "Raft leader proposing partition reassignments for draining node"
                    );
                }
            }
            RaftEvent::ExternalUpdate(update) => {
                self.raft.apply_external_update(&update);
            }
        }
    }

    async fn initialize_partitions(&mut self) {
        const BATCH_SIZE: usize = 8;

        let mut cluster_nodes: Vec<NodeId> = self.raft.cluster_members().to_vec();
        cluster_nodes.sort_by_key(|n| n.get());

        let min_nodes = self.all_nodes.len().max(2);
        if cluster_nodes.len() < min_nodes {
            return;
        }

        info!(?cluster_nodes, "Raft leader initializing partition assignments");
        let node_count = cluster_nodes.len();
        let mut proposal_count = 0usize;

        for partition in PartitionId::all() {
            let partition_num = partition.get() as usize;
            let primary_idx = partition_num % node_count;
            let replica_idx = (partition_num + 1) % node_count;

            let primary = cluster_nodes[primary_idx];
            let replicas = if node_count > 1 {
                vec![cluster_nodes[replica_idx]]
            } else {
                vec![]
            };

            let cmd = RaftCommand::update_partition(partition, primary, &replicas, Epoch::new(1));
            let _ = self.raft.propose_partition_update(cmd).await;
            proposal_count += 1;

            if proposal_count.is_multiple_of(BATCH_SIZE) {
                let _ = self.raft.tick(current_time_ms()).await;
                tokio::task::yield_now().await;
            }
        }

        self.raft
            .set_pending_partition_proposals(NUM_PARTITIONS as usize);
        self.partitions_initialized = true;
    }
}
