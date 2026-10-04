// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::raft::{RaftCommand, RaftCoordinator};
use crate::cluster::transport::ClusterTransport;
use crate::cluster::{ClusterMessage, Epoch, NUM_PARTITIONS, NodeId, PartitionId, PartitionMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::interval;
use tracing::{error, info};

use super::node_controller::RaftMessage;
use super::raft::PartitionUpdate;

#[allow(clippy::cast_possible_truncation)]
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
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
    pub(crate) raft: RaftCoordinator<T>,
    pub(crate) rx_messages: flume::Receiver<RaftMessage>,
    pub(crate) rx_events: flume::Receiver<RaftEvent>,
    pub(crate) rx_admin: flume::Receiver<RaftAdminCommand>,
    pub(crate) tx_partition_map: watch::Sender<PartitionMap>,
    pub(crate) tx_status: watch::Sender<RaftStatus>,
    pub(crate) shutdown_rx: broadcast::Receiver<()>,
    pub(crate) shutdown_tx: broadcast::Sender<()>,
    pub(crate) fatal_error: Arc<std::sync::OnceLock<String>>,
    pub(crate) all_nodes: Vec<NodeId>,
    pub(crate) partitions_initialized: bool,
}

impl<T: ClusterTransport> RaftTask<T> {
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
            if let Some(reason) = self.raft.storage_failure() {
                error!(%reason, "stopping node after a raft storage failure");
                let _ = self.fatal_error.set(reason.to_string());
                let _ = self.shutdown_tx.send(());
                break;
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

        let _ = self
            .tx_partition_map
            .send(self.raft.partition_map().clone());
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
                if self.raft.storage_failure().is_none() {
                    let _ = self
                        .raft
                        .send(from, ClusterMessage::RequestVoteResponse(response))
                        .await;
                }
            }
            RaftMessage::RequestVoteResponse { from, response } => {
                self.raft.handle_request_vote_response(from, response).await;
            }
            RaftMessage::AppendEntries { from, request } => {
                let response = self.raft.handle_append_entries(from, request, now).await;
                if self.raft.storage_failure().is_none() {
                    let _ = self
                        .raft
                        .send(from, ClusterMessage::AppendEntriesResponse(response))
                        .await;
                }
            }
            RaftMessage::AppendEntriesResponse { from, response } => {
                self.raft
                    .handle_append_entries_response(from, response)
                    .await;
            }
            RaftMessage::InstallSnapshot { from, request } => {
                let response = self.raft.handle_install_snapshot(from, *request, now).await;
                if self.raft.storage_failure().is_none() {
                    let _ = self
                        .raft
                        .send(from, ClusterMessage::AppendEntriesResponse(response))
                        .await;
                }
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

        info!(
            ?cluster_nodes,
            "Raft leader initializing partition assignments"
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::raft::RaftConfig;
    use crate::cluster::raft::RequestVoteRequest;
    use crate::cluster::raft::test_support::FlushFailingBackend;
    use crate::cluster::transport::{InboundMessage, TransportError};
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct RecordingTransport {
        node_id: NodeId,
        sent: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    }

    impl ClusterTransport for RecordingTransport {
        fn local_node(&self) -> NodeId {
            self.node_id
        }

        async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push((to, message));
            Ok(())
        }

        async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push((self.node_id, message));
            Ok(())
        }

        async fn send_to_partition_primary(
            &self,
            _partition: PartitionId,
            _message: ClusterMessage,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn direct_peers(&self) -> Option<Vec<NodeId>> {
            None
        }

        fn recv(&self) -> Option<InboundMessage> {
            None
        }

        fn pending_count(&self) -> usize {
            0
        }

        fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
            None
        }

        fn requeue(&self, _msg: InboundMessage) {}

        async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

        async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
    }

    #[tokio::test]
    async fn storage_failure_stops_the_task_and_the_node() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let backend = FlushFailingBackend::shared();
        let transport = RecordingTransport {
            node_id: node2,
            sent: Arc::new(Mutex::new(Vec::new())),
        };
        let sent = Arc::clone(&transport.sent);
        let mut raft = RaftCoordinator::new_with_storage(
            node2,
            transport,
            RaftConfig::default(),
            backend.clone(),
        )
        .unwrap();
        raft.add_peer(node1);

        let (tx_messages, rx_messages) = flume::unbounded();
        let (_tx_events, rx_events) = flume::unbounded();
        let (_tx_admin, rx_admin) = flume::unbounded();
        let (tx_partition_map, _rx_map) = watch::channel(PartitionMap::new());
        let (tx_status, _rx_status) = watch::channel(RaftStatus::default());
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let mut node_shutdown = shutdown_tx.subscribe();
        let fatal_error = Arc::new(std::sync::OnceLock::new());

        let task = RaftTask {
            raft,
            rx_messages,
            rx_events,
            rx_admin,
            tx_partition_map,
            tx_status,
            shutdown_rx,
            shutdown_tx,
            fatal_error: Arc::clone(&fatal_error),
            all_nodes: vec![node1, node2],
            partitions_initialized: false,
        };
        let handle = tokio::spawn(task.run());

        backend.fail_flushes(true);
        tx_messages
            .send(RaftMessage::RequestVote {
                from: node1,
                request: RequestVoteRequest::create(1, 1, 0, 0),
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("raft task must stop after a storage failure")
            .unwrap();
        assert!(fatal_error.get().is_some());
        assert!(node_shutdown.try_recv().is_ok());
        assert!(
            !sent
                .lock()
                .unwrap()
                .iter()
                .any(|(_, m)| matches!(m, ClusterMessage::RequestVoteResponse(_)))
        );
    }
}
