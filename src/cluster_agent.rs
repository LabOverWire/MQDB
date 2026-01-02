use crate::cluster::raft::{RaftConfig, RaftCoordinator};
use crate::cluster::{
    ClusterEventHandler, ClusterMessage, ClusterTransport, Epoch, MqttTransport, NodeController,
    NodeId, PartitionId, RaftMessage, TransportConfig,
};
use crate::config::DurabilityMode;
use crate::storage::FjallBackend;
use mqtt5::QoS;
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection, BridgeProtocol};
use mqtt5::broker::config::{QuicConfig, StorageBackend, StorageConfig};
use mqtt5::broker::{BrokerConfig, MqttBroker};
use mqtt5::time::Duration;
use mqtt5::transport::StreamStrategy;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::time::interval;
use tracing::{debug, info};

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
    pub use_quic: bool,
    pub quic_insecure: bool,
    pub quic_cert_file: Option<PathBuf>,
    pub quic_key_file: Option<PathBuf>,
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
            use_quic: true,
            quic_insecure: true,
            quic_cert_file: None,
            quic_key_file: None,
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

    #[must_use]
    pub fn with_quic(mut self, enabled: bool) -> Self {
        self.use_quic = enabled;
        self
    }

    #[must_use]
    pub fn with_quic_insecure(mut self, insecure: bool) -> Self {
        self.quic_insecure = insecure;
        self
    }

    #[must_use]
    pub fn with_quic_certs(mut self, cert_file: PathBuf, key_file: PathBuf) -> Self {
        self.quic_cert_file = Some(cert_file);
        self.quic_key_file = Some(key_file);
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
    use_quic: bool,
    quic_insecure: bool,
    quic_cert_file: Option<PathBuf>,
    quic_key_file: Option<PathBuf>,
}

impl ClusteredAgent {
    /// # Errors
    /// Returns an error if the node ID is invalid (must be 1-65535) or storage fails to open.
    pub fn new(config: ClusterConfig) -> Result<Self, String> {
        let node_id =
            NodeId::validated(config.node_id).ok_or("invalid node_id: must be 1-65535")?;

        let transport = MqttTransport::new(node_id);
        let transport_config = TransportConfig::default();
        let controller = NodeController::new(node_id, transport.clone(), transport_config);

        let raft_path = config.db_path.join("raft");
        let raft_backend = Arc::new(
            FjallBackend::open(&raft_path, DurabilityMode::Immediate).map_err(|e| {
                format!(
                    "failed to open raft storage at {}: {e}",
                    raft_path.display()
                )
            })?,
        );

        let raft = RaftCoordinator::new_with_storage(
            node_id,
            transport,
            RaftConfig::default(),
            raft_backend,
        )
        .map_err(|e| format!("failed to initialize raft coordinator: {e}"))?;

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
            use_quic: config.use_quic,
            quic_insecure: config.quic_insecure,
            quic_cert_file: config.quic_cert_file,
            quic_key_file: config.quic_key_file,
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
                    .add_topic("_mqdb/repl/#", BridgeDirection::Both, QoS::AtLeastOnce)
                    .add_topic("$DB/#", BridgeDirection::Both, QoS::ExactlyOnce);
                config.client_id = format!("{}-to-node-{}", self.node_name, peer.node_id);
                config.clean_start = false;
                config.try_private = true;

                if self.use_quic {
                    config.protocol = BridgeProtocol::Quic;
                    config.quic_stream_strategy = Some(StreamStrategy::DataPerTopic);
                    config.quic_flow_headers = Some(true);
                    config.quic_datagrams = Some(true);
                    config.fallback_tcp = true;
                    if self.quic_insecure {
                        config.insecure = Some(true);
                    }
                }

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

        if self.use_quic {
            if let (Some(cert_file), Some(key_file)) = (&self.quic_cert_file, &self.quic_key_file) {
                let quic_config = QuicConfig::new(cert_file.clone(), key_file.clone())
                    .with_bind_address(self.bind_address);
                broker_config = broker_config.with_quic(quic_config);
                info!(quic_bind = %self.bind_address, "QUIC listener configured (same port as TCP)");
            } else {
                info!(
                    "QUIC enabled but no certs provided - bridges will use QUIC, but no QUIC listener"
                );
            }
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
        let mut wildcard_reconciliation_interval = interval(Duration::from_secs(60));
        let mut subscription_reconciliation_interval = interval(Duration::from_secs(300));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let now = current_time_ms();
                    let mut ctrl = self.controller.write().await;
                    ctrl.tick(now);
                    ctrl.process_messages();

                    let raft_msgs: Vec<RaftMessage> = ctrl.drain_raft_messages().collect();
                    let partition_updates: Vec<_> = ctrl.drain_partition_updates().collect();
                    let dead_nodes: Vec<NodeId> = ctrl.drain_dead_nodes().collect();
                    let draining_nodes: Vec<NodeId> = ctrl.drain_draining_nodes().collect();
                    let alive_nodes: Vec<NodeId> = ctrl.alive_nodes();
                    drop(ctrl);

                    let mut raft = self.raft.write().await;

                    for update in &partition_updates {
                        raft.apply_external_update(update);
                    }

                    for alive_node in alive_nodes {
                        let rebalance_proposals = raft.handle_node_alive(alive_node);
                        if !rebalance_proposals.is_empty() {
                            info!(
                                ?alive_node,
                                count = rebalance_proposals.len(),
                                "triggered rebalance for new node"
                            );
                        }
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

                    for draining_node in draining_nodes {
                        let proposed = raft.handle_drain_notification(draining_node);
                        if !proposed.is_empty() {
                            info!(
                                ?draining_node,
                                proposals = proposed.len(),
                                "Raft leader proposing partition reassignments for draining node"
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
                    let expired_idempotency = ctrl.stores().idempotency.cleanup_expired(now);
                    if expired_idempotency > 0 {
                        info!(expired_idempotency, "cleaned up expired idempotency records");
                    }
                    let expired_sessions = ctrl.stores().cleanup_expired_sessions(now);
                    if !expired_sessions.is_empty() {
                        info!(count = expired_sessions.len(), "cleaned up expired sessions");
                    }
                    let stale_offsets = ctrl.stores().cleanup_stale_offsets(now);
                    if stale_offsets > 0 {
                        info!(stale_offsets, "cleaned up stale consumer offsets");
                    }
                }
                _ = wildcard_reconciliation_interval.tick() => {
                    let now = current_time_ms();
                    let ctrl = self.controller.read().await;
                    let pending_store = &ctrl.stores().wildcard_pending;
                    if pending_store.needs_reconciliation(now) {
                        let pending = pending_store.get_pending_for_retry();
                        if !pending.is_empty() {
                            debug!(
                                count = pending.len(),
                                "retrying pending wildcard broadcasts"
                            );
                            for p in &pending {
                                let broadcast = p.to_broadcast();
                                let msg = ClusterMessage::WildcardBroadcast(broadcast);
                                let _ = ctrl.transport().broadcast(msg);
                                pending_store.mark_retried(&p.pattern, &p.client_id);
                            }
                            info!(
                                count = pending.len(),
                                "rebroadcast pending wildcard subscriptions"
                            );
                        }
                        pending_store.mark_reconciliation(now);
                        let max_age_ms = 5 * 60 * 1000;
                        let removed = pending_store.clear_old_entries(now, max_age_ms);
                        if removed > 0 {
                            debug!(removed, "cleared old wildcard pending entries");
                        }
                    }
                }
                _ = subscription_reconciliation_interval.tick() => {
                    let now = current_time_ms();
                    let ctrl = self.controller.read().await;
                    let stores = ctrl.stores();
                    if stores.subscriptions.needs_reconciliation(now) {
                        let result = stores.subscriptions.reconcile(
                            &stores.topics,
                            &stores.wildcards,
                        );
                        if result.subscriptions_added > 0 || result.subscriptions_removed > 0 {
                            info!(
                                clients = result.clients_checked,
                                added = result.subscriptions_added,
                                removed = result.subscriptions_removed,
                                "reconciled subscription cache"
                            );
                        }
                        stores.subscriptions.mark_reconciliation(now);
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
