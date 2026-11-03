// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::raft::RaftCoordinator;
use crate::cluster::transport::ClusterTransport;
use crate::cluster::{ClusterMessage, NodeId, PartitionMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::interval;
use tracing::{error, info};

use super::node_controller::RaftMessage;

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
            self.raft.replicate_due().await;
            self.publish_partition_map();
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

        let leader_proposals = self.raft.tick(now).await;
        if !leader_proposals.is_empty() {
            info!(
                proposals = leader_proposals.len(),
                "new Raft leader proposed partition reassignments"
            );
        }

        self.publish_partition_map();
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

    fn publish_partition_map(&self) {
        if self.raft.holds_restored_map() {
            return;
        }
        let map = self.raft.partition_map();
        self.tx_partition_map.send_if_modified(|published| {
            if published == map {
                false
            } else {
                published.clone_from(map);
                true
            }
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
                self.raft.handle_node_death(node).await;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::PartitionId;
    use crate::cluster::raft::RaftConfig;
    use crate::cluster::raft::RequestVoteRequest;
    use crate::cluster::test_support::FaultyBackend;
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
        async fn queue_retained_refresh(&self, _topic: String) {}
    }

    #[tokio::test]
    async fn storage_failure_stops_the_task_and_the_node() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let backend = FaultyBackend::shared();
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

    fn map_naming(nodes: &[(u16, u16)]) -> PartitionMap {
        let mut map = PartitionMap::new();
        for (partition, (primary, replica)) in (0u16..).zip(nodes) {
            map.set(
                PartitionId::new(partition).unwrap(),
                crate::cluster::PartitionAssignment::new(
                    NodeId::validated(*primary).unwrap(),
                    NodeId::validated(*replica).into_iter().collect(),
                    crate::cluster::Epoch::new(1),
                ),
            );
        }
        map
    }

    fn restarted_task(
        node: NodeId,
        snapshot_map: &PartitionMap,
    ) -> (RaftTask<RecordingTransport>, watch::Receiver<PartitionMap>) {
        let backend: Arc<dyn mqdb_core::StorageBackend> = Arc::new(mqdb_core::MemoryBackend::new());
        let storage = crate::cluster::raft::RaftStorage::new(Arc::clone(&backend));
        let snapshot = crate::cluster::raft::RaftSnapshot::capture(40, 2, snapshot_map, &[node]);
        storage.persist_state(2, None).unwrap();
        storage.install_snapshot(&snapshot, false).unwrap();
        let transport = RecordingTransport {
            node_id: node,
            sent: Arc::new(Mutex::new(Vec::new())),
        };
        let raft =
            RaftCoordinator::new_with_storage(node, transport, RaftConfig::default(), backend)
                .unwrap();
        let (_tx_messages, rx_messages) = flume::unbounded();
        let (_tx_events, rx_events) = flume::unbounded();
        let (_tx_admin, rx_admin) = flume::unbounded();
        let (tx_partition_map, rx_map) = watch::channel(PartitionMap::new());
        let (tx_status, _rx_status) = watch::channel(RaftStatus::default());
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        (
            RaftTask {
                raft,
                rx_messages,
                rx_events,
                rx_admin,
                tx_partition_map,
                tx_status,
                shutdown_rx,
                shutdown_tx,
                fatal_error: Arc::new(std::sync::OnceLock::new()),
            },
            rx_map,
        )
    }

    #[tokio::test]
    async fn a_restored_map_naming_other_nodes_stays_unpublished_until_the_leader_is_heard() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let stale = map_naming(&[(1, 2), (2, 1)]);
        let (mut task, rx_map) = restarted_task(node1, &stale);

        task.publish_partition_map();
        assert_eq!(
            *rx_map.borrow(),
            PartitionMap::new(),
            "the stale map would make node 1 serve as primary for partitions that moved"
        );

        task.handle_raft_message(RaftMessage::AppendEntries {
            from: node2,
            request: crate::cluster::raft::AppendEntriesRequest::create(
                3,
                node2.get(),
                40,
                2,
                vec![crate::cluster::raft::LogEntry::create(
                    41,
                    3,
                    crate::cluster::raft::RaftCommand::update_partition(
                        PartitionId::new(0).unwrap(),
                        node2,
                        &[],
                        crate::cluster::Epoch::new(2),
                    ),
                )],
                41,
            ),
        })
        .await;
        task.publish_partition_map();

        assert_eq!(
            rx_map.borrow().primary(PartitionId::new(0).unwrap()),
            Some(node2)
        );
    }

    #[tokio::test]
    async fn a_failed_consistency_check_keeps_the_restored_map_unpublished() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let (mut task, rx_map) = restarted_task(node1, &map_naming(&[(1, 2)]));

        task.handle_raft_message(RaftMessage::AppendEntries {
            from: node2,
            request: crate::cluster::raft::AppendEntriesRequest::create(
                3,
                node2.get(),
                90,
                3,
                Vec::new(),
                90,
            ),
        })
        .await;
        task.publish_partition_map();

        assert_eq!(*rx_map.borrow(), PartitionMap::new());
    }

    #[tokio::test]
    async fn a_restored_single_node_map_is_published_at_once() {
        let node1 = NodeId::validated(1).unwrap();
        let only_self = map_naming(&[(1, 0)]);
        let (task, rx_map) = restarted_task(node1, &only_self);

        task.publish_partition_map();

        assert_eq!(*rx_map.borrow(), only_self);
    }

    #[tokio::test]
    async fn a_leader_with_a_stale_commit_must_not_release_the_held_map() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let stale = map_naming(&[(1, 2)]);
        let (mut task, rx_map) = restarted_task(node1, &stale);
        task.handle_raft_message(RaftMessage::AppendEntries {
            from: node2,
            request: crate::cluster::raft::AppendEntriesRequest::create(
                3,
                node2.get(),
                40,
                2,
                vec![crate::cluster::raft::LogEntry::create(
                    41,
                    2,
                    crate::cluster::raft::RaftCommand::update_partition(
                        PartitionId::new(0).unwrap(),
                        node2,
                        &[],
                        crate::cluster::Epoch::new(2),
                    ),
                )],
                0,
            ),
        })
        .await;
        task.publish_partition_map();
        assert_eq!(
            *rx_map.borrow(),
            PartitionMap::new(),
            "entry 41 moved partition 0 off node 1, but the map says node 1 is still its primary"
        );
    }

    #[tokio::test]
    async fn an_installed_snapshot_releases_the_held_map() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let (mut task, rx_map) = restarted_task(node1, &map_naming(&[(1, 2)]));
        let leader_map = map_naming(&[(2, 1)]);

        task.handle_raft_message(RaftMessage::InstallSnapshot {
            from: node2,
            request: Box::new(crate::cluster::raft::InstallSnapshotRequest {
                term: 3,
                leader_id: node2.get(),
                snapshot: crate::cluster::raft::RaftSnapshot::capture(
                    90,
                    3,
                    &leader_map,
                    &[node1, node2],
                ),
            }),
        })
        .await;
        task.publish_partition_map();

        assert_eq!(
            rx_map.borrow().primary(PartitionId::new(0).unwrap()),
            Some(node2)
        );
    }

    #[tokio::test]
    async fn a_snapshot_behind_the_restored_one_keeps_the_map_held() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let (mut task, rx_map) = restarted_task(node1, &map_naming(&[(1, 2)]));

        task.handle_raft_message(RaftMessage::InstallSnapshot {
            from: node2,
            request: Box::new(crate::cluster::raft::InstallSnapshotRequest {
                term: 3,
                leader_id: node2.get(),
                snapshot: crate::cluster::raft::RaftSnapshot::capture(
                    10,
                    1,
                    &map_naming(&[(2, 1)]),
                    &[node1, node2],
                ),
            }),
        })
        .await;
        task.publish_partition_map();

        assert_eq!(*rx_map.borrow(), PartitionMap::new());
    }
}
