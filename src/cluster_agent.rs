use crate::cluster::raft::{RaftConfig, RaftCoordinator};
use crate::cluster::{
    ClusterEventHandler, ClusterMessage, ClusterTransport, Epoch, ForwardTarget, ForwardedPublish,
    LwtPublisher, MqttTransport, NUM_PARTITIONS, NodeController, NodeId, PartitionId,
    PublishRouter, RaftMessage, TransportConfig, WildcardBroadcast,
};
use crate::config::DurabilityMode;
use crate::storage::FjallBackend;
use crate::transport::{ErrorCode, Response};
use mqtt5::QoS;
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection, BridgeProtocol};
use mqtt5::broker::config::{QuicConfig, StorageBackend, StorageConfig};
use mqtt5::broker::{BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::time::Duration;
use mqtt5::transport::StreamStrategy;
use mqtt5::types::Message;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::interval;
use tracing::{debug, info, warn};

const BROKER_MAX_CLIENTS: usize = 10_000;
const BROKER_MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;
const SESSION_EXPIRY_SECS: u64 = 3600;
const CLEANUP_INTERVAL_SECS: u64 = 3600;
const TTL_CLEANUP_INTERVAL_SECS: u64 = 60;

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
    pub persist_stores: bool,
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
            persist_stores: true,
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
    pub fn with_persist_stores(mut self, persist: bool) -> Self {
        self.persist_stores = persist;
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

#[derive(Debug)]
struct AdminRequest {
    topic: String,
    response_topic: Option<String>,
    payload: Vec<u8>,
}

pub struct ClusteredAgent {
    node_id: NodeId,
    node_name: String,
    controller: Arc<RwLock<NodeController<MqttTransport>>>,
    raft: Arc<RwLock<RaftCoordinator<MqttTransport>>>,
    shutdown_tx: broadcast::Sender<()>,
    bind_address: SocketAddr,
    db_path: PathBuf,
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
        use crate::storage::StorageBackend;

        let node_id =
            NodeId::validated(config.node_id).ok_or("invalid node_id: must be 1-65535")?;

        let stores_backend: Option<Arc<dyn StorageBackend>> = if config.persist_stores {
            let stores_path = config.db_path.join("stores");
            Some(Arc::new(
                FjallBackend::open(&stores_path, DurabilityMode::Immediate).map_err(|e| {
                    format!(
                        "failed to open stores storage at {}: {e}",
                        stores_path.display()
                    )
                })?,
            ))
        } else {
            None
        };

        let transport = MqttTransport::new(node_id);
        let transport_config = TransportConfig::default();
        let controller = NodeController::new_with_storage(
            node_id,
            transport.clone(),
            transport_config,
            stores_backend,
        );

        if controller.stores().has_persistence() {
            match controller.stores().recover() {
                Ok(stats) => {
                    info!(
                        sessions = stats.sessions,
                        subscriptions = stats.subscriptions,
                        retained = stats.retained,
                        db_data = stats.db_data,
                        topic_index_rebuilt = stats.topic_index_rebuilt,
                        wildcards_rebuilt = stats.wildcards_rebuilt,
                        total = stats.total(),
                        "recovered cluster stores from disk"
                    );
                }
                Err(e) => {
                    warn!(error = ?e, "failed to recover stores, starting fresh");
                }
            }
        }

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
            db_path: config.db_path,
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
                    .add_topic("_mqdb/repl/#", BridgeDirection::Both, QoS::AtLeastOnce);
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
            max_clients: BROKER_MAX_CLIENTS,
            max_packet_size: BROKER_MAX_PACKET_SIZE,
            session_expiry_interval: Duration::from_secs(SESSION_EXPIRY_SECS),
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

        let (service_username, service_password, custom_auth_provider) =
            if let Some(ref path) = self.password_file {
                let svc_user = format!("mqdb-internal-{}", uuid::Uuid::new_v4());
                let svc_pass = uuid::Uuid::new_v4().to_string();
                let auth_provider = PasswordAuthProvider::from_file(path)
                    .await
                    .map_err(|e| format!("failed to load password file: {e}"))?;
                auth_provider
                    .add_user(svc_user.clone(), &svc_pass)
                    .await
                    .map_err(|e| format!("failed to add service user: {e}"))?;
                broker_config.auth_config.allow_anonymous = false;
                (
                    Some(svc_user),
                    Some(svc_pass),
                    Some(Arc::new(auth_provider)),
                )
            } else {
                (None, None, None)
            };

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

        let mut broker = if let Some(provider) = custom_auth_provider {
            MqttBroker::with_config(broker_config)
                .await?
                .with_auth_provider(provider)
        } else {
            MqttBroker::with_config(broker_config).await?
        };
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

        let connect_ip = if self.bind_address.ip().is_unspecified() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            self.bind_address.ip()
        };
        let broker_addr = format!("{}:{}", connect_ip, self.bind_address.port());
        info!(broker_addr = %broker_addr, "connecting transport to local broker");
        let transport = {
            let ctrl = self.controller.read().await;
            ctrl.transport().clone()
        };
        Box::pin(transport.connect_with_credentials(
            &broker_addr,
            service_username.as_deref(),
            service_password.as_deref(),
        ))
        .await
        .map_err(|e| format!("failed to connect transport to local broker: {e}"))?;
        info!("transport connected to local broker");

        let (admin_tx, mut admin_rx) = mpsc::channel::<AdminRequest>(32);
        let admin_client = transport.client().clone();
        admin_client
            .subscribe("$SYS/mqdb/cluster/#", {
                let tx = admin_tx.clone();
                move |msg: Message| {
                    let req = AdminRequest {
                        topic: msg.topic.clone(),
                        response_topic: msg.properties.response_topic.clone(),
                        payload: msg.payload.clone(),
                    };
                    let _ = tx.try_send(req);
                }
            })
            .await
            .map_err(|e| format!("failed to subscribe to admin topics: {e}"))?;

        admin_client
            .subscribe("$DB/_health", {
                let tx = admin_tx.clone();
                move |msg: Message| {
                    let req = AdminRequest {
                        topic: msg.topic.clone(),
                        response_topic: msg.properties.response_topic.clone(),
                        payload: msg.payload.clone(),
                    };
                    let _ = tx.try_send(req);
                }
            })
            .await
            .map_err(|e| format!("failed to subscribe to health topic: {e}"))?;

        admin_client
            .subscribe("$DB/_admin/#", {
                let tx = admin_tx.clone();
                move |msg: Message| {
                    let req = AdminRequest {
                        topic: msg.topic.clone(),
                        response_topic: msg.properties.response_topic.clone(),
                        payload: msg.payload.clone(),
                    };
                    let _ = tx.try_send(req);
                }
            })
            .await
            .map_err(|e| format!("failed to subscribe to db admin topics: {e}"))?;

        info!("subscribed to admin topics");

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
        let mut cleanup_interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        let mut ttl_cleanup_interval = interval(Duration::from_secs(TTL_CLEANUP_INTERVAL_SECS));
        let mut wildcard_reconciliation_interval = interval(Duration::from_secs(60));
        let mut subscription_reconciliation_interval = interval(Duration::from_secs(300));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        self.recover_pending_lwts().await;

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let now = current_time_ms();
                    let mut ctrl = self.controller.write().await;
                    ctrl.tick(now).await;
                    ctrl.process_messages().await;

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
                        let rebalance_proposals = raft.handle_node_alive(alive_node).await;
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
                                let response = raft.handle_request_vote(from, request, now).await;
                                let _ = raft.send(from, crate::cluster::ClusterMessage::RequestVoteResponse(response)).await;
                            }
                            RaftMessage::RequestVoteResponse { from, response } => {
                                raft.handle_request_vote_response(from, response).await;
                            }
                            RaftMessage::AppendEntries { from, request } => {
                                let response = raft.handle_append_entries(from, request, now).await;
                                let _ = raft.send(from, crate::cluster::ClusterMessage::AppendEntriesResponse(response)).await;
                            }
                            RaftMessage::AppendEntriesResponse { from, response } => {
                                raft.handle_append_entries_response(from, response).await;
                            }
                        }
                    }

                    for dead_node in &dead_nodes {
                        let proposed = raft.handle_node_death(*dead_node).await;
                        if !proposed.is_empty() {
                            info!(
                                ?dead_node,
                                proposals = proposed.len(),
                                "Raft leader proposing partition reassignments"
                            );
                        }
                    }

                    for draining_node in draining_nodes {
                        let proposed = raft.handle_drain_notification(draining_node).await;
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
                            let _ = raft.propose_partition_update(cmd).await;
                        }
                        partitions_initialized = true;
                    }

                    let leader_proposals = raft.tick(now).await;
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
                    let mut became_primary = false;
                    if current_map != raft_partition_map {
                        for partition in PartitionId::all() {
                            let new_assignment = raft_partition_map.get(partition);
                            let old_assignment = current_map.get(partition);

                            if new_assignment != old_assignment {
                                let is_primary = new_assignment.primary == Some(self.node_id);
                                let is_replica = new_assignment.replicas.contains(&self.node_id);
                                let was_primary = old_assignment.primary == Some(self.node_id);

                                if is_primary {
                                    ctrl.become_primary(partition, new_assignment.epoch);
                                    if !was_primary {
                                        became_primary = true;
                                    }
                                } else if is_replica {
                                    ctrl.become_replica(partition, new_assignment.epoch, 0);
                                }
                            }
                        }
                        ctrl.update_partition_map(raft_partition_map);
                    }
                    if became_primary {
                        let stores = ctrl.stores();
                        let result = stores.subscriptions.reconcile(
                            &stores.topics,
                            &stores.wildcards,
                        );
                        if result.subscriptions_added > 0 || result.subscriptions_removed > 0 {
                            info!(
                                clients = result.clients_checked,
                                added = result.subscriptions_added,
                                removed = result.subscriptions_removed,
                                "reconciled subscriptions after partition takeover"
                            );
                        }
                    }
                    for dead_node in &dead_nodes {
                        let affected_sessions = ctrl.stores().sessions.sessions_on_node(*dead_node);
                        if !affected_sessions.is_empty() {
                            info!(
                                node = dead_node.get(),
                                sessions = affected_sessions.len(),
                                "marking sessions disconnected due to node death"
                            );
                            let now = current_time_ms();
                            for session in affected_sessions {
                                let client_id = session.client_id_str();
                                let result = ctrl.stores_mut().update_session_replicated(
                                    client_id,
                                    |s| s.set_connected(false, *dead_node, now),
                                );
                                if let Ok((_session, write)) = result {
                                    ctrl.write_or_forward(write).await;
                                }
                            }
                        }
                    }
                }
                _ = ttl_cleanup_interval.tick() => {
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let ctrl = self.controller.read().await;
                    let expired_ttl = ctrl.stores().db_data.cleanup_expired_ttl(now_secs);
                    if !expired_ttl.is_empty() {
                        info!(count = expired_ttl.len(), "cleaned up TTL-expired entities");
                    }
                }
                _ = cleanup_interval.tick() => {
                    let now = current_time_ms();
                    let expired_sessions = {
                        let ctrl = self.controller.read().await;
                        let expired_idempotency = ctrl.stores().idempotency.cleanup_expired(now);
                        if expired_idempotency > 0 {
                            info!(expired_idempotency, "cleaned up expired idempotency records");
                        }
                        let stale_offsets = ctrl.stores().cleanup_stale_offsets(now);
                        if stale_offsets > 0 {
                            info!(stale_offsets, "cleaned up stale consumer offsets");
                        }
                        let expired_unique = ctrl.stores().unique_cleanup_expired(now);
                        if expired_unique > 0 {
                            info!(expired_unique, "cleaned up expired unique reservations");
                        }
                        ctrl.stores().cleanup_expired_sessions(now)
                    };
                    if !expired_sessions.is_empty() {
                        info!(count = expired_sessions.len(), "cleaning up expired sessions and subscriptions");
                        let mut ctrl = self.controller.write().await;
                        for session in &expired_sessions {
                            let client_id = session.client_id_str();
                            clear_expired_session_subscriptions(&mut ctrl, client_id).await;
                        }
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
                                let _ = ctrl.transport().broadcast(msg).await;
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
                Some(req) = admin_rx.recv() => {
                    self.handle_admin_request(&admin_client, req).await;
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

    async fn handle_admin_request(&self, client: &mqtt5::client::MqttClient, req: AdminRequest) {
        let Some(response_topic) = req.response_topic else {
            warn!(topic = %req.topic, "admin request without response topic");
            return;
        };

        let response = if req.topic == "$DB/_health" {
            self.build_health_response().await
        } else if req.topic.ends_with("/status") {
            self.build_status_response().await
        } else if req.topic.ends_with("/rebalance") {
            self.handle_rebalance_request().await
        } else if let Some(entity) = req.topic.strip_prefix("$DB/_admin/schema/") {
            if let Some(entity) = entity.strip_suffix("/set") {
                self.handle_schema_set(entity, &req.payload).await
            } else if let Some(entity) = entity.strip_suffix("/get") {
                self.handle_schema_get(entity).await
            } else {
                Response::error(ErrorCode::BadRequest, "invalid schema operation")
            }
        } else if let Some(rest) = req.topic.strip_prefix("$DB/_admin/constraint/") {
            if let Some(entity) = rest.strip_suffix("/add") {
                self.handle_constraint_add(entity, &req.payload).await
            } else if let Some(entity) = rest.strip_suffix("/list") {
                self.handle_constraint_list(entity).await
            } else if let Some(entity) = rest.strip_suffix("/remove") {
                self.handle_constraint_remove(entity, &req.payload).await
            } else {
                Response::error(ErrorCode::BadRequest, "invalid constraint operation")
            }
        } else if req.topic == "$DB/_admin/backup" {
            self.handle_backup_create(&req.payload)
        } else if req.topic == "$DB/_admin/backup/list" {
            self.handle_backup_list()
        } else if req.topic == "$DB/_admin/restore" {
            Response::error(
                ErrorCode::BadRequest,
                "restore requires agent restart - use CLI with --restore flag",
            )
        } else if req.topic == "$DB/_admin/consumer-groups" {
            Self::handle_consumer_group_list()
        } else if let Some(name) = req.topic.strip_prefix("$DB/_admin/consumer-groups/") {
            Self::handle_consumer_group_show(name)
        } else {
            Response::error(
                ErrorCode::BadRequest,
                format!("unknown admin command: {}", req.topic),
            )
        };

        let payload = serde_json::to_vec(&response).unwrap_or_default();
        if let Err(e) = client.publish(&response_topic, payload).await {
            warn!(error = %e, "failed to send admin response");
        }
    }

    async fn build_status_response(&self) -> Response {
        let ctrl = self.controller.read().await;
        let raft = self.raft.read().await;

        let alive_nodes: Vec<u16> = ctrl.alive_nodes().iter().map(|n| n.get()).collect();

        let mut partitions = Vec::new();
        for partition in PartitionId::all() {
            let assignment = ctrl.partition_map().get(partition);
            let replicas: Vec<_> = assignment
                .replicas
                .iter()
                .copied()
                .map(NodeId::get)
                .collect();
            partitions.push(json!({
                "id": partition.get(),
                "primary": assignment.primary.map(NodeId::get),
                "replicas": replicas,
                "epoch": assignment.epoch.get()
            }));
        }

        Response::ok(json!({
            "node_id": self.node_id.get(),
            "node_name": self.node_name,
            "is_raft_leader": raft.is_leader(),
            "raft_term": raft.current_term(),
            "alive_nodes": alive_nodes,
            "partition_count": NUM_PARTITIONS,
            "partitions": partitions
        }))
    }

    async fn handle_rebalance_request(&self) -> Response {
        let mut raft = self.raft.write().await;

        if !raft.is_leader() {
            return Response::error(ErrorCode::Forbidden, "not the Raft leader");
        }

        let proposals = raft.force_rebalance().await;

        Response::ok(json!({
            "proposed_changes": proposals.len()
        }))
    }

    async fn handle_schema_set(&self, entity: &str, payload: &[u8]) -> Response {
        let schema_data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let schema_bytes = serde_json::to_vec(&schema_data).unwrap_or_default();
        let mut ctrl = self.controller.write().await;

        if ctrl.schema_get(entity).is_some() {
            match ctrl.schema_update(entity, &schema_bytes).await {
                Ok(_) => Response::ok(json!({"message": "schema updated"})),
                Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
            }
        } else {
            match ctrl.schema_register(entity, &schema_bytes).await {
                Ok(_) => Response::ok(json!({"message": "schema set"})),
                Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
            }
        }
    }

    async fn handle_schema_get(&self, entity: &str) -> Response {
        let ctrl = self.controller.read().await;
        match ctrl.schema_get(entity) {
            Some(schema) => {
                let data: serde_json::Value =
                    serde_json::from_slice(&schema.data).unwrap_or(serde_json::Value::Null);
                Response::ok(json!({
                    "entity": entity,
                    "version": schema.schema_version,
                    "schema": data
                }))
            }
            None => Response::error(
                ErrorCode::NotFound,
                format!("no schema for entity: {entity}"),
            ),
        }
    }

    async fn handle_constraint_add(&self, entity: &str, payload: &[u8]) -> Response {
        let constraint_def: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let constraint_type = constraint_def
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unique");
        let name = constraint_def
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let field = constraint_def
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if name.is_empty() || field.is_empty() {
            return Response::error(
                ErrorCode::BadRequest,
                "constraint requires 'name' and 'field' parameters",
            );
        }

        match constraint_type {
            "unique" => {
                let constraint = crate::cluster::db::ClusterConstraint::unique(entity, name, field);
                let mut ctrl = self.controller.write().await;
                match ctrl.constraint_add(&constraint).await {
                    Ok(()) => Response::ok(json!({"message": "constraint added"})),
                    Err(crate::cluster::db::ConstraintStoreError::AlreadyExists) => {
                        Response::error(ErrorCode::Conflict, "constraint already exists")
                    }
                    Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
                }
            }
            _ => Response::error(
                ErrorCode::BadRequest,
                format!("unsupported constraint type: {constraint_type}"),
            ),
        }
    }

    async fn handle_constraint_remove(&self, entity: &str, payload: &[u8]) -> Response {
        let remove_def: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return Response::error(ErrorCode::BadRequest, e.to_string()),
        };

        let Some(name) = remove_def.get("name").and_then(|v| v.as_str()) else {
            return Response::error(
                ErrorCode::BadRequest,
                "constraint removal requires 'name' parameter",
            );
        };

        let mut ctrl = self.controller.write().await;
        match ctrl.constraint_remove(entity, name).await {
            Ok(()) => Response::ok(json!({"message": "constraint removed"})),
            Err(crate::cluster::db::ConstraintStoreError::NotFound) => {
                Response::error(ErrorCode::NotFound, "constraint not found")
            }
            Err(e) => Response::error(ErrorCode::Internal, e.to_string()),
        }
    }

    async fn handle_constraint_list(&self, entity: &str) -> Response {
        let ctrl = self.controller.read().await;
        let constraints = ctrl.constraint_list(entity);
        let data: Vec<serde_json::Value> = constraints
            .iter()
            .map(|c| {
                json!({
                    "name": c.name_str(),
                    "type": "unique",
                    "field": c.field_str()
                })
            })
            .collect();
        Response::ok(json!(data))
    }

    fn handle_backup_create(&self, payload: &[u8]) -> Response {
        let name = serde_json::from_slice::<serde_json::Value>(payload)
            .ok()
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
            .unwrap_or_else(|| "backup".to_string());

        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Response::error(
                ErrorCode::BadRequest,
                "invalid backup name: must be alphanumeric, underscore, or hyphen only",
            );
        }

        let backup_dir = self.db_path.join("backups").join(&name);
        let stores_dir = self.db_path.join("stores");

        if backup_dir.exists() {
            return Response::error(
                ErrorCode::BadRequest,
                format!("backup already exists: {name}"),
            );
        }

        if !stores_dir.exists() {
            return Response::error(ErrorCode::Internal, "no stores directory to backup");
        }

        match Self::copy_dir_recursive(&stores_dir, &backup_dir) {
            Ok(()) => Response::ok(json!({
                "message": "backup created",
                "name": name,
                "path": backup_dir.display().to_string()
            })),
            Err(e) => Response::error(ErrorCode::Internal, format!("backup failed: {e}")),
        }
    }

    fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                Self::copy_dir_recursive(&src_path, &dst_path)?;
            } else {
                std::fs::copy(&src_path, &dst_path)?;
            }
        }
        Ok(())
    }

    fn handle_backup_list(&self) -> Response {
        let backup_dir = self.db_path.join("backups");
        if !backup_dir.exists() {
            return Response::ok(json!([]));
        }

        match std::fs::read_dir(&backup_dir) {
            Ok(entries) => {
                let backups: Vec<String> = entries
                    .filter_map(std::result::Result::ok)
                    .filter(|e| e.path().is_dir())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect();
                Response::ok(json!(backups))
            }
            Err(e) => Response::error(ErrorCode::Internal, format!("failed to list backups: {e}")),
        }
    }

    fn handle_consumer_group_list() -> Response {
        Response::ok(json!([]))
    }

    fn handle_consumer_group_show(name: &str) -> Response {
        Response::error(
            ErrorCode::NotFound,
            format!("consumer group not found: {name}"),
        )
    }

    async fn build_health_response(&self) -> Response {
        let ctrl = self.controller.read().await;
        let raft = self.raft.read().await;

        let is_leader = raft.is_leader();
        let leader_exists = is_leader || raft.leader_id().is_some();
        let alive_nodes: Vec<u16> = ctrl.alive_nodes().iter().map(|n| n.get()).collect();

        let mut partitions_ready = 0u16;
        for partition in PartitionId::all() {
            if ctrl.partition_map().get(partition).primary.is_some() {
                partitions_ready += 1;
            }
        }

        let ready = leader_exists && partitions_ready == NUM_PARTITIONS;
        let health_status = if partitions_ready == NUM_PARTITIONS && leader_exists {
            "healthy"
        } else if partitions_ready > 0 && leader_exists {
            "degraded"
        } else {
            "unhealthy"
        };

        Response::ok(json!({
            "health_status": health_status,
            "ready": ready,
            "mode": "cluster",
            "details": {
                "node_id": self.node_id.get(),
                "is_leader": is_leader,
                "raft_term": raft.current_term(),
                "partitions_ready": partitions_ready,
                "partitions_total": NUM_PARTITIONS,
                "alive_nodes": alive_nodes
            }
        }))
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

    async fn recover_pending_lwts(&self) {
        let ctrl = self.controller.read().await;
        let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
        let pending = lwt_publisher.recover_pending_lwts();

        if pending.is_empty() {
            return;
        }

        info!(
            count = pending.len(),
            "recovering pending LWTs from previous crash"
        );

        let router = PublishRouter::new(&ctrl.stores().topics);
        let transport = ctrl.transport().clone();
        let node_id = self.node_id;

        for lwt in pending {
            let wildcards = ctrl.stores().wildcards.match_topic(&lwt.topic);
            let route = router.route_with_wildcards(&lwt.topic, &wildcards);

            let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
            for target in route.targets {
                let connected_node = ctrl
                    .stores()
                    .client_locations
                    .get(&target.client_id)
                    .or_else(|| {
                        ctrl.stores()
                            .sessions
                            .get(&target.client_id)
                            .filter(|s| s.connected == 1)
                            .and_then(|s| NodeId::validated(s.connected_node))
                    });

                if let Some(target_node) = connected_node
                    && target_node != node_id
                {
                    remote_nodes
                        .entry(target_node)
                        .or_default()
                        .push(ForwardTarget::new(target.client_id, target.qos));
                }
            }

            for (target_node, targets) in remote_nodes {
                let fwd = ForwardedPublish::new(
                    node_id,
                    lwt.topic.clone(),
                    lwt.qos,
                    lwt.retain,
                    lwt.payload.clone(),
                    targets,
                );
                let fwd_msg = ClusterMessage::ForwardedPublish(fwd);
                if let Err(e) = transport.send(target_node, fwd_msg).await {
                    warn!(target = target_node.get(), error = %e, "failed to forward recovered LWT");
                } else {
                    debug!(target = target_node.get(), topic = %lwt.topic, "forwarded recovered LWT to node");
                }
            }

            if let Err(e) = lwt_publisher.complete_lwt(&lwt.client_id, lwt.token) {
                warn!(client_id = %lwt.client_id, error = %e, "failed to mark recovered LWT as published");
            } else {
                info!(client_id = %lwt.client_id, topic = %lwt.topic, "recovered and published pending LWT");
            }
        }
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

async fn clear_expired_session_subscriptions(
    ctrl: &mut NodeController<MqttTransport>,
    client_id: &str,
) {
    let snapshot = ctrl.stores().subscriptions.get_snapshot(client_id);
    if let Some(snapshot) = snapshot {
        for entry in &snapshot.topics {
            let topic = std::str::from_utf8(&entry.topic).unwrap_or("");
            if topic.is_empty() {
                continue;
            }

            let is_wildcard = entry.is_wildcard != 0;
            if is_wildcard {
                let result = ctrl
                    .stores_mut()
                    .unsubscribe_wildcard_replicated(topic, client_id);
                if result.is_ok() {
                    let broadcast = WildcardBroadcast::unsubscribe(topic, client_id);
                    let msg = ClusterMessage::WildcardBroadcast(broadcast);
                    let _ = ctrl.transport().broadcast(msg).await;
                }
            } else {
                let result = ctrl
                    .stores_mut()
                    .unsubscribe_topic_replicated(topic, client_id);
                if let Ok((_entry, writes)) = result
                    && let Some(write) = writes.into_iter().next()
                {
                    ctrl.write_or_forward(write).await;
                }
            }

            let result = ctrl
                .stores_mut()
                .remove_subscription_replicated(client_id, topic);
            if let Ok((_snapshot, write)) = result {
                ctrl.write_or_forward(write).await;
            }
        }
    }

    for write in ctrl.stores_mut().clear_qos2_client_replicated(client_id) {
        ctrl.write_or_forward(write).await;
    }
    for write in ctrl
        .stores_mut()
        .clear_inflight_client_replicated(client_id)
    {
        ctrl.write_or_forward(write).await;
    }
}
