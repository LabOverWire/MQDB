use crate::cluster::raft::{RaftConfig, RaftCoordinator};
use crate::cluster::{
    ClusterEventHandler, Epoch, MqttTransport, NodeController, NodeId, PartitionId, RaftMessage,
    TransportConfig,
};
use mqtt5::QoS;
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection};
use mqtt5::broker::config::{StorageBackend, StorageConfig};
use mqtt5::broker::{BrokerConfig, MqttBroker};
use mqtt5::time::Duration;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::time::interval;
use tracing::info;

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub node_id: u16,
    pub address: String,
}

impl PeerConfig {
    #[must_use]
    pub fn new(node_id: u16, address: String) -> Self {
        Self { node_id, address }
    }
}

pub struct ClusterConfig {
    pub node_id: u16,
    pub node_name: String,
    pub bind_address: SocketAddr,
    pub db_path: PathBuf,
    pub peers: Vec<PeerConfig>,
    pub password_file: Option<PathBuf>,
    pub acl_file: Option<PathBuf>,
}

impl ClusterConfig {
    /// # Panics
    /// Panics if the default bind address cannot be parsed (should never happen).
    #[must_use]
    pub fn new(node_id: u16, db_path: PathBuf, peers: Vec<PeerConfig>) -> Self {
        Self {
            node_id,
            node_name: format!("node-{node_id}"),
            bind_address: "0.0.0.0:1883".parse().expect("valid default address"),
            db_path,
            peers,
            password_file: None,
            acl_file: None,
        }
    }

    #[must_use]
    pub fn with_node_name(mut self, name: String) -> Self {
        self.node_name = name;
        self
    }

    #[must_use]
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }

    #[must_use]
    pub fn with_password_file(mut self, path: PathBuf) -> Self {
        self.password_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_acl_file(mut self, path: PathBuf) -> Self {
        self.acl_file = Some(path);
        self
    }
}

pub struct ClusteredAgent {
    node_id: NodeId,
    node_name: String,
    controller: Arc<RwLock<NodeController<MqttTransport>>>,
    raft: Arc<RwLock<RaftCoordinator<MqttTransport>>>,
    shutdown_tx: broadcast::Sender<()>,
    bind_address: SocketAddr,
    peers: Vec<PeerConfig>,
    password_file: Option<PathBuf>,
    acl_file: Option<PathBuf>,
}

impl ClusteredAgent {
    /// # Errors
    /// Returns an error if the node ID is invalid (must be 1-65535).
    pub fn new(config: ClusterConfig) -> Result<Self, String> {
        let node_id =
            NodeId::validated(config.node_id).ok_or("invalid node_id: must be 1-65535")?;

        let transport = MqttTransport::new(node_id);
        let transport_config = TransportConfig::default();
        let controller = NodeController::new(node_id, transport.clone(), transport_config);
        let raft = RaftCoordinator::new(node_id, transport, RaftConfig::default());

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            node_id,
            node_name: config.node_name,
            controller: Arc::new(RwLock::new(controller)),
            raft: Arc::new(RwLock::new(raft)),
            shutdown_tx,
            bind_address: config.bind_address,
            peers: config.peers,
            password_file: config.password_file,
            acl_file: config.acl_file,
        })
    }

    fn create_bridge_configs(&self) -> Vec<BridgeConfig> {
        self.peers
            .iter()
            .map(|peer| {
                let bridge_name = format!("bridge-to-node-{}", peer.node_id);
                let mut config = BridgeConfig::new(&bridge_name, &peer.address)
                    .add_topic("_mqdb/cluster/#", BridgeDirection::Both, QoS::AtLeastOnce)
                    .add_topic("_mqdb/forward/#", BridgeDirection::Both, QoS::AtLeastOnce)
                    .add_topic("_mqdb/repl/#", BridgeDirection::Both, QoS::AtLeastOnce);
                config.client_id = format!("{}-to-node-{}", self.node_name, peer.node_id);
                config.clean_start = false;
                config.try_private = true;
                config
            })
            .collect()
    }

    /// # Errors
    /// Returns an error if broker startup or transport connection fails.
    #[allow(clippy::too_many_lines)]
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event_handler = Arc::new(ClusterEventHandler::new(
            self.node_id,
            self.controller.clone(),
        ));

        {
            let mut ctrl = self.controller.write().await;
            ctrl.set_synced_retained_topics(event_handler.synced_retained_topics());
        }

        let storage_config = StorageConfig {
            backend: StorageBackend::Memory,
            enable_persistence: true,
            ..Default::default()
        };

        let mut broker_config = BrokerConfig {
            bind_addresses: vec![self.bind_address],
            max_clients: 10000,
            max_packet_size: 10 * 1024 * 1024,
            session_expiry_interval: Duration::from_secs(3600),
            maximum_qos: 2,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifier_available: true,
            shared_subscription_available: true,
            topic_alias_maximum: 100,
            bridges: self.create_bridge_configs(),
            ..Default::default()
        }
        .with_storage(storage_config)
        .with_event_handler(event_handler);

        if let Some(ref path) = self.password_file {
            broker_config.auth_config.password_file = Some(path.clone());
            broker_config.auth_config.allow_anonymous = false;
        }
        if let Some(ref path) = self.acl_file {
            broker_config.auth_config.acl_file = Some(path.clone());
        }

        let mut broker = MqttBroker::with_config(broker_config).await?;
        let mut ready_rx = broker.ready_receiver();

        let broker_handle = tokio::spawn(async move {
            let _ = broker.run().await;
        });

        info!("waiting for broker ready signal");
        let _ = ready_rx.changed().await;
        info!("broker ready signal received");

        info!(
            node_id = self.node_id.get(),
            node_name = %self.node_name,
            bind = %self.bind_address,
            peers = ?self.peers,
            "cluster node started"
        );

        let broker_addr = format!("127.0.0.1:{}", self.bind_address.port());
        info!(broker_addr = %broker_addr, "connecting transport to local broker");
        let transport = {
            let ctrl = self.controller.read().await;
            ctrl.transport().clone()
        };
        transport
            .connect(&broker_addr)
            .await
            .map_err(|e| format!("failed to connect transport to local broker: {e}"))?;
        info!("transport connected to local broker");

        {
            let mut ctrl = self.controller.write().await;
            let mut raft = self.raft.write().await;
            for peer in &self.peers {
                if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                    ctrl.register_peer(peer_node_id);
                    raft.add_peer(peer_node_id);
                    info!(peer_id = peer.node_id, address = %peer.address, "registered peer");
                }
            }
        }

        let all_nodes: Vec<NodeId> = {
            let mut nodes: Vec<NodeId> = self
                .peers
                .iter()
                .filter_map(|p| NodeId::validated(p.node_id))
                .collect();
            nodes.push(self.node_id);
            nodes.sort_by_key(|n| n.get());
            nodes
        };

        let mut partitions_initialized = false;

        let mut tick_interval = interval(Duration::from_millis(100));
        let mut cleanup_interval = interval(Duration::from_secs(3600));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let now = current_time_ms();
                    let mut ctrl = self.controller.write().await;
                    ctrl.tick(now);
                    ctrl.process_messages();

                    let raft_msgs: Vec<RaftMessage> = ctrl.drain_raft_messages().collect();
                    let dead_nodes: Vec<NodeId> = ctrl.drain_dead_nodes().collect();
                    let alive_nodes: Vec<NodeId> = ctrl.alive_nodes();
                    drop(ctrl);

                    let mut raft = self.raft.write().await;

                    for alive_node in alive_nodes {
                        raft.handle_node_alive(alive_node);
                    }

                    for msg in raft_msgs {
                        match msg {
                            RaftMessage::RequestVote { from, request } => {
                                let response = raft.handle_request_vote(from, request, now);
                                let _ = raft.send(from, crate::cluster::ClusterMessage::RequestVoteResponse(response));
                            }
                            RaftMessage::RequestVoteResponse { from, response } => {
                                raft.handle_request_vote_response(from, response);
                            }
                            RaftMessage::AppendEntries { from, request } => {
                                let response = raft.handle_append_entries(from, request, now);
                                let _ = raft.send(from, crate::cluster::ClusterMessage::AppendEntriesResponse(response));
                            }
                            RaftMessage::AppendEntriesResponse { from, response } => {
                                raft.handle_append_entries_response(from, response);
                            }
                        }
                    }

                    for dead_node in dead_nodes {
                        let proposed = raft.handle_node_death(dead_node);
                        if !proposed.is_empty() {
                            info!(
                                ?dead_node,
                                proposals = proposed.len(),
                                "Raft leader proposing partition reassignments"
                            );
                        }
                    }

                    if raft.is_leader() && !partitions_initialized {
                        info!("Raft leader initializing partition assignments");
                        let node_count = all_nodes.len();

                        for partition in PartitionId::all() {
                            let partition_num = partition.get() as usize;
                            let primary_idx = partition_num % node_count;
                            let replica_idx = (partition_num + 1) % node_count;

                            let primary = all_nodes[primary_idx];
                            let replicas = if node_count > 1 {
                                vec![all_nodes[replica_idx]]
                            } else {
                                vec![]
                            };

                            let cmd = crate::cluster::raft::RaftCommand::update_partition(
                                partition,
                                primary,
                                &replicas,
                                Epoch::new(1),
                            );
                            let _ = raft.propose_partition_update(cmd);
                        }
                        partitions_initialized = true;
                    }

                    let leader_proposals = raft.tick(now);
                    if !leader_proposals.is_empty() {
                        info!(
                            proposals = leader_proposals.len(),
                            "new Raft leader proposed partition reassignments"
                        );
                    }

                    let raft_partition_map = raft.partition_map().clone();
                    drop(raft);

                    let mut ctrl = self.controller.write().await;
                    let current_map = ctrl.partition_map().clone();
                    if current_map != raft_partition_map {
                        for partition in PartitionId::all() {
                            let new_assignment = raft_partition_map.get(partition);
                            let old_assignment = current_map.get(partition);

                            if new_assignment != old_assignment {
                                let is_primary = new_assignment.primary == Some(self.node_id);
                                let is_replica = new_assignment.replicas.contains(&self.node_id);

                                if is_primary {
                                    ctrl.become_primary(partition, new_assignment.epoch);
                                } else if is_replica {
                                    ctrl.become_replica(partition, new_assignment.epoch, 0);
                                }
                            }
                        }
                        ctrl.update_partition_map(raft_partition_map);
                    }
                }
                _ = cleanup_interval.tick() => {
                    let now = current_time_ms();
                    let ctrl = self.controller.read().await;
                    let expired = ctrl.stores().idempotency.cleanup_expired(now);
                    if expired > 0 {
                        info!(expired, "cleaned up expired idempotency records");
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("cluster node shutting down");
                    break;
                }
            }
        }

        broker_handle.abort();
        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    #[must_use]
    pub fn controller(&self) -> &Arc<RwLock<NodeController<MqttTransport>>> {
        &self.controller
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}
