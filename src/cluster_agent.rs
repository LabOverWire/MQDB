#[allow(deprecated)]
use crate::cluster::raft::{RaftConfig, RaftCoordinator};
#[allow(deprecated)]
use crate::cluster::{
    ClusterEventHandler, ClusterMessage, ClusterTransport, DedicatedExecutor, ForwardTarget,
    ForwardedPublish, InboundMessage, LwtPublisher, MessageProcessor, MqttTransport,
    NUM_PARTITIONS, NodeController, NodeId, PartitionId, PartitionMap, ProcessingBatch,
    PublishRouter, QuicDirectTransport, RaftAdminCommand, RaftEvent, RaftStatus, RaftTask,
    TopicSubscriptionBroadcast, TransportConfig, WildcardBroadcast,
};
use crate::config::DurabilityMode;
use crate::storage::FjallBackend;
use crate::topic_protection::TopicProtectionAuthProvider;
use crate::transport::{ErrorCode, Response};
use mqtt5::QoS;
use mqtt5::broker::auth::CompositeAuthProvider;
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection, BridgeManager, BridgeProtocol};
use mqtt5::broker::config::{
    ChangeOnlyDeliveryConfig, ClusterListenerConfig, FederatedJwtConfig, JwtConfig, QuicConfig,
    RateLimitConfig, StorageBackend, StorageConfig, WebSocketConfig,
};
use mqtt5::broker::{BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::time::Duration;
use mqtt5::transport::StreamStrategy;
use mqtt5::types::Message;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, oneshot, watch};
use tokio::time::interval;
use tracing::{debug, info, warn};

const BROKER_MAX_CLIENTS: usize = 10_000;
const BROKER_MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;
const SESSION_EXPIRY_SECS: u64 = 3600;
const CLEANUP_INTERVAL_SECS: u64 = 3600;
const TTL_CLEANUP_INTERVAL_SECS: u64 = 60;
const RETAINED_SYNC_CLEANUP_INTERVAL_SECS: u64 = 30;
const RETAINED_SYNC_TTL_SECS: u64 = 5;

const RAFT_CHANNEL_CAPACITY: usize = 4096;
const MAIN_QUEUE_CAPACITY: usize = 4096;
const BATCH_QUEUE_CAPACITY: usize = 16;

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

struct MessageProcessorChannels {
    tx_raft_messages: flume::Sender<crate::cluster::RaftMessage>,
    tx_raft_events: flume::Sender<RaftEvent>,
    tx_main_queue: flume::Sender<InboundMessage>,
    transport_inbox_rx: flume::Receiver<InboundMessage>,
    rx_tick: flume::Receiver<u64>,
    tx_batch: flume::Sender<ProcessingBatch>,
}

#[allow(clippy::struct_excessive_bools)]
pub struct ClusterConfig {
    pub node_id: u16,
    pub node_name: String,
    pub bind_address: SocketAddr,
    pub cluster_port_offset: u16,
    pub db_path: PathBuf,
    pub persist_stores: bool,
    pub stores_durability: DurabilityMode,
    pub peers: Vec<PeerConfig>,
    pub password_file: Option<PathBuf>,
    pub acl_file: Option<PathBuf>,
    pub auth_setup: crate::auth_config::AuthSetupConfig,
    pub use_quic: bool,
    #[cfg(feature = "dev-insecure")]
    pub quic_insecure: bool,
    pub quic_cert_file: Option<PathBuf>,
    pub quic_key_file: Option<PathBuf>,
    pub quic_ca_file: Option<PathBuf>,
    pub bridge_out_only: bool,
    pub use_direct_quic: bool,
    pub ws_bind_address: Option<SocketAddr>,
    pub http_config: Option<crate::http::HttpServerConfig>,
}

impl ClusterConfig {
    /// # Panics
    /// Panics if the default bind address cannot be parsed (should never happen).
    #[must_use]
    pub fn new(node_id: u16, db_path: PathBuf, peers: Vec<PeerConfig>) -> Self {
        Self {
            node_id,
            node_name: format!("node-{node_id}"),
            bind_address: SocketAddr::from(([0, 0, 0, 0], 1883)),
            cluster_port_offset: 100,
            db_path,
            persist_stores: true,
            stores_durability: DurabilityMode::PeriodicMs(10),
            peers,
            password_file: None,
            acl_file: None,
            auth_setup: crate::auth_config::AuthSetupConfig::default(),
            use_quic: true,
            #[cfg(feature = "dev-insecure")]
            quic_insecure: false,
            quic_cert_file: None,
            quic_key_file: None,
            quic_ca_file: None,
            bridge_out_only: false,
            use_direct_quic: true,
            ws_bind_address: None,
            http_config: None,
        }
    }

    #[must_use]
    pub fn cluster_port(&self) -> u16 {
        self.bind_address.port() + self.cluster_port_offset
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
    pub fn with_auth_setup(mut self, config: crate::auth_config::AuthSetupConfig) -> Self {
        self.auth_setup = config;
        self
    }

    #[must_use]
    pub fn with_scram_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.scram_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_jwt_config(mut self, config: JwtConfig) -> Self {
        self.auth_setup.jwt_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_federated_jwt_config(mut self, config: FederatedJwtConfig) -> Self {
        self.auth_setup.federated_jwt_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_cert_auth_file(mut self, path: PathBuf) -> Self {
        self.auth_setup.cert_auth_file = Some(path);
        self
    }

    #[must_use]
    pub fn with_rate_limit_config(mut self, config: RateLimitConfig) -> Self {
        self.auth_setup.rate_limit = Some(config);
        self
    }

    #[must_use]
    pub fn with_no_rate_limit(mut self) -> Self {
        self.auth_setup.no_rate_limit = true;
        self
    }

    #[must_use]
    pub fn with_admin_users(mut self, users: std::collections::HashSet<String>) -> Self {
        self.auth_setup.admin_users = users;
        self
    }

    #[must_use]
    pub fn with_persist_stores(mut self, persist: bool) -> Self {
        self.persist_stores = persist;
        self
    }

    #[must_use]
    pub fn with_stores_durability(mut self, mode: DurabilityMode) -> Self {
        self.stores_durability = mode;
        self
    }

    #[must_use]
    pub fn with_quic(mut self, enabled: bool) -> Self {
        self.use_quic = enabled;
        self
    }

    #[cfg(feature = "dev-insecure")]
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

    #[must_use]
    pub fn with_quic_ca(mut self, ca_file: PathBuf) -> Self {
        self.quic_ca_file = Some(ca_file);
        self
    }

    #[must_use]
    pub fn with_bridge_out_only(mut self, out_only: bool) -> Self {
        self.bridge_out_only = out_only;
        self
    }

    #[must_use]
    pub fn with_cluster_port_offset(mut self, offset: u16) -> Self {
        self.cluster_port_offset = offset;
        self
    }

    #[must_use]
    pub fn with_direct_quic(mut self, use_direct_quic: bool) -> Self {
        self.use_direct_quic = use_direct_quic;
        self
    }

    #[must_use]
    pub fn with_ws_bind_address(mut self, addr: SocketAddr) -> Self {
        self.ws_bind_address = Some(addr);
        self
    }

    #[must_use]
    pub fn with_http_config(mut self, config: crate::http::HttpServerConfig) -> Self {
        self.http_config = Some(config);
        self
    }
}

#[derive(Debug, Clone)]
pub enum ClusterTransportKind {
    #[deprecated(since = "0.2.0", note = "use Quic variant instead")]
    Mqtt(#[allow(deprecated)] MqttTransport),
    Quic(QuicDirectTransport),
}

impl ClusterTransportKind {
    #[deprecated(since = "0.2.0", note = "MQTT transport is deprecated, use QUIC")]
    #[must_use]
    #[allow(deprecated)]
    pub fn as_mqtt(&self) -> Option<&MqttTransport> {
        match self {
            #[allow(deprecated)]
            Self::Mqtt(t) => Some(t),
            Self::Quic(_) => None,
        }
    }

    #[must_use]
    #[allow(deprecated)]
    pub fn as_quic(&self) -> Option<&QuicDirectTransport> {
        match self {
            Self::Mqtt(_) => None,
            Self::Quic(t) => Some(t),
        }
    }

    pub fn log_queue_stats(&self) {
        if let Self::Quic(t) = self {
            t.log_queue_stats();
        }
    }
}

#[allow(deprecated)]
impl ClusterTransport for ClusterTransportKind {
    fn local_node(&self) -> NodeId {
        match self {
            Self::Mqtt(t) => t.local_node(),
            Self::Quic(t) => t.local_node(),
        }
    }

    async fn send(
        &self,
        to: NodeId,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.send(to, message).await,
            Self::Quic(t) => t.send(to, message).await,
        }
    }

    async fn broadcast(
        &self,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.broadcast(message).await,
            Self::Quic(t) => t.broadcast(message).await,
        }
    }

    async fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.send_to_partition_primary(partition, message).await,
            Self::Quic(t) => t.send_to_partition_primary(partition, message).await,
        }
    }

    fn recv(&self) -> Option<InboundMessage> {
        match self {
            Self::Mqtt(t) => t.recv(),
            Self::Quic(t) => t.recv(),
        }
    }

    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage> {
        match self {
            Self::Mqtt(t) => t.try_recv_timeout(timeout_ms),
            Self::Quic(t) => t.try_recv_timeout(timeout_ms),
        }
    }

    fn pending_count(&self) -> usize {
        match self {
            Self::Mqtt(t) => t.pending_count(),
            Self::Quic(t) => t.pending_count(),
        }
    }

    fn requeue(&self, msg: InboundMessage) {
        match self {
            Self::Mqtt(t) => t.requeue(msg),
            Self::Quic(t) => t.requeue(msg),
        }
    }

    async fn queue_local_publish(&self, topic: String, payload: Vec<u8>, qos: u8) {
        match self {
            Self::Mqtt(t) => t.queue_local_publish(topic, payload, qos).await,
            Self::Quic(t) => t.queue_local_publish(topic, payload, qos).await,
        }
    }

    async fn queue_local_publish_retained(&self, topic: String, payload: Vec<u8>, qos: u8) {
        match self {
            Self::Mqtt(t) => t.queue_local_publish_retained(topic, payload, qos).await,
            Self::Quic(t) => t.queue_local_publish_retained(topic, payload, qos).await,
        }
    }
}

#[derive(Debug)]
struct AdminRequest {
    topic: String,
    response_topic: Option<String>,
    payload: Vec<u8>,
}

#[allow(clippy::struct_excessive_bools)]
pub struct ClusteredAgent {
    node_id: NodeId,
    node_name: String,
    controller: Arc<RwLock<NodeController<ClusterTransportKind>>>,
    raft: Option<RaftCoordinator<ClusterTransportKind>>,
    rx_raft_messages: Option<flume::Receiver<crate::cluster::RaftMessage>>,
    rx_raft_events: Option<flume::Receiver<RaftEvent>>,
    rx_raft_admin: Option<flume::Receiver<RaftAdminCommand>>,
    tx_raft_admin: flume::Sender<RaftAdminCommand>,
    #[allow(dead_code)]
    tx_raft_events: flume::Sender<RaftEvent>,
    tx_partition_map: Option<watch::Sender<PartitionMap>>,
    tx_raft_status: Option<watch::Sender<RaftStatus>>,
    rx_partition_map: watch::Receiver<PartitionMap>,
    rx_raft_status: watch::Receiver<RaftStatus>,
    shutdown_tx: broadcast::Sender<()>,
    bind_address: SocketAddr,
    db_path: PathBuf,
    peers: Vec<PeerConfig>,
    password_file: Option<PathBuf>,
    acl_file: Option<PathBuf>,
    auth_setup: crate::auth_config::AuthSetupConfig,
    use_quic: bool,
    #[cfg(feature = "dev-insecure")]
    quic_insecure: bool,
    quic_cert_file: Option<PathBuf>,
    quic_key_file: Option<PathBuf>,
    bridge_out_only: bool,
    cluster_port_offset: u16,
    bridge_executor: Option<DedicatedExecutor>,
    #[allow(dead_code)]
    processor_executor: Option<DedicatedExecutor>,
    tx_tick: Option<flume::Sender<u64>>,
    rx_main_queue: Option<flume::Receiver<InboundMessage>>,
    rx_batch: Option<flume::Receiver<ProcessingBatch>>,
    use_direct_quic: bool,
    ws_bind_address: Option<SocketAddr>,
    http_config: Option<crate::http::HttpServerConfig>,
}

impl ClusteredAgent {
    fn open_stores_backend(
        config: &ClusterConfig,
    ) -> Result<Option<Arc<dyn crate::storage::StorageBackend>>, String> {
        if config.persist_stores {
            let stores_path = config.db_path.join("stores");
            Ok(Some(Arc::new(
                FjallBackend::open(&stores_path, config.stores_durability).map_err(|e| {
                    format!(
                        "failed to open stores storage at {}: {e}",
                        stores_path.display()
                    )
                })?,
            )))
        } else {
            Ok(None)
        }
    }

    #[allow(deprecated)]
    fn build_transport(
        node_id: NodeId,
        config: &ClusterConfig,
    ) -> (ClusterTransportKind, flume::Receiver<InboundMessage>) {
        if config.use_direct_quic {
            let quic_transport = QuicDirectTransport::new(node_id);
            #[cfg(feature = "dev-insecure")]
            quic_transport.set_insecure(config.quic_insecure);
            if let Some(ca_path) = &config.quic_ca_file {
                quic_transport.set_ca_file(ca_path.clone());
            }
            let inbox_rx = quic_transport.inbox_rx();
            (ClusterTransportKind::Quic(quic_transport), inbox_rx)
        } else {
            let mqtt_transport = MqttTransport::new(node_id);
            let inbox_rx = mqtt_transport.inbox_rx();
            (ClusterTransportKind::Mqtt(mqtt_transport), inbox_rx)
        }
    }

    fn open_raft(
        node_id: NodeId,
        db_path: &std::path::Path,
        transport: ClusterTransportKind,
    ) -> Result<RaftCoordinator<ClusterTransportKind>, String> {
        let raft_path = db_path.join("raft");
        let raft_backend = Arc::new(
            FjallBackend::open(&raft_path, DurabilityMode::Immediate).map_err(|e| {
                format!(
                    "failed to open raft storage at {}: {e}",
                    raft_path.display()
                )
            })?,
        );
        RaftCoordinator::new_with_storage(node_id, transport, RaftConfig::default(), raft_backend)
            .map_err(|e| format!("failed to initialize raft coordinator: {e}"))
    }

    fn spawn_message_processor(
        node_id: NodeId,
        transport_config: TransportConfig,
        peers: &[PeerConfig],
        channels: MessageProcessorChannels,
    ) -> DedicatedExecutor {
        let mut processor = MessageProcessor::new(
            node_id,
            transport_config,
            channels.tx_raft_messages,
            channels.tx_raft_events,
            channels.tx_main_queue,
            channels.transport_inbox_rx,
            channels.rx_tick,
        );
        for peer in peers {
            if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                processor.register_peer(peer_node_id);
            }
        }
        let executor = DedicatedExecutor::new("msg-processor", 2);
        executor.handle().spawn(async move {
            processor.run(channels.tx_batch).await;
        });
        info!("started message processor on dedicated executor");
        executor
    }

    /// # Errors
    /// Returns an error if the node ID is invalid (must be 1-65535) or storage fails to open.
    pub fn new(config: ClusterConfig) -> Result<Self, String> {
        let node_id =
            NodeId::validated(config.node_id).ok_or("invalid node_id: must be 1-65535")?;
        let stores_backend = Self::open_stores_backend(&config)?;

        let (tx_raft_messages, rx_raft_messages) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let (tx_raft_events, rx_raft_events) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let tx_raft_events_clone = tx_raft_events.clone();
        let (tx_raft_admin, rx_raft_admin) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let (tx_partition_map, rx_partition_map) = watch::channel(PartitionMap::default());
        let (tx_raft_status, rx_raft_status) = watch::channel(RaftStatus::default());

        let (transport, transport_inbox_rx) = Self::build_transport(node_id, &config);
        let transport_config = TransportConfig::default();
        let controller = NodeController::new_with_storage(
            node_id,
            transport.clone(),
            transport_config,
            stores_backend,
            tx_raft_messages.clone(),
            tx_raft_events.clone(),
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

        let raft = Self::open_raft(node_id, &config.db_path, transport)?;
        let (shutdown_tx, _) = broadcast::channel(1);

        let (tx_tick, rx_tick) = flume::bounded(1);
        let (tx_main_queue, rx_main_queue) = flume::bounded(MAIN_QUEUE_CAPACITY);
        let (tx_batch, rx_batch) = flume::bounded(BATCH_QUEUE_CAPACITY);

        let processor_channels = MessageProcessorChannels {
            tx_raft_messages,
            tx_raft_events,
            tx_main_queue,
            transport_inbox_rx,
            rx_tick,
            tx_batch,
        };
        let processor_executor = Self::spawn_message_processor(
            node_id,
            transport_config,
            &config.peers,
            processor_channels,
        );

        Ok(Self {
            node_id,
            node_name: config.node_name,
            controller: Arc::new(RwLock::new(controller)),
            raft: Some(raft),
            rx_raft_messages: Some(rx_raft_messages),
            rx_raft_events: Some(rx_raft_events),
            rx_raft_admin: Some(rx_raft_admin),
            tx_raft_admin,
            tx_raft_events: tx_raft_events_clone,
            tx_partition_map: Some(tx_partition_map),
            tx_raft_status: Some(tx_raft_status),
            rx_partition_map,
            rx_raft_status,
            shutdown_tx,
            bind_address: config.bind_address,
            db_path: config.db_path,
            peers: config.peers,
            password_file: config.password_file,
            acl_file: config.acl_file,
            auth_setup: config.auth_setup,
            use_quic: config.use_quic,
            #[cfg(feature = "dev-insecure")]
            quic_insecure: config.quic_insecure,
            quic_cert_file: config.quic_cert_file.clone(),
            quic_key_file: config.quic_key_file.clone(),
            bridge_out_only: config.bridge_out_only,
            cluster_port_offset: config.cluster_port_offset,
            bridge_executor: None,
            processor_executor: Some(processor_executor),
            tx_tick: Some(tx_tick),
            rx_main_queue: Some(rx_main_queue),
            rx_batch: Some(rx_batch),
            use_direct_quic: config.use_direct_quic,
            ws_bind_address: config.ws_bind_address,
            http_config: config.http_config,
        })
    }

    #[must_use]
    fn cluster_port(&self) -> u16 {
        self.bind_address.port() + self.cluster_port_offset
    }

    fn create_bridge_configs(&self) -> Result<Vec<BridgeConfig>, std::net::AddrParseError> {
        let direction = if self.bridge_out_only {
            BridgeDirection::Out
        } else {
            BridgeDirection::Both
        };

        self.peers
            .iter()
            .map(|peer| {
                let peer_addr: SocketAddr = peer.address.parse()?;
                let cluster_addr = format!(
                    "{}:{}",
                    peer_addr.ip(),
                    peer_addr.port() + self.cluster_port_offset
                );

                let bridge_name = format!("bridge-to-node-{}", peer.node_id);
                let local_node_topic = format!("_mqdb/cluster/nodes/{}", self.node_id.get());
                let peer_node_topic = format!("_mqdb/cluster/nodes/{}", peer.node_id);
                let mut config = BridgeConfig::new(&bridge_name, &cluster_addr)
                    .add_topic(&local_node_topic, direction, QoS::AtLeastOnce)
                    .add_topic(&peer_node_topic, direction, QoS::AtLeastOnce)
                    .add_topic("_mqdb/cluster/broadcast", direction, QoS::AtLeastOnce)
                    .add_topic("_mqdb/cluster/heartbeat/+", direction, QoS::AtLeastOnce)
                    .add_topic("_mqdb/forward/#", direction, QoS::AtLeastOnce)
                    .add_topic("_mqdb/repl/#", direction, QoS::AtLeastOnce);
                config.client_id = format!("{}-to-node-{}", self.node_name, peer.node_id);
                config.clean_start = false;
                config.try_private = true;

                if self.use_quic {
                    config.protocol = BridgeProtocol::Quic;
                    config.quic_stream_strategy = Some(StreamStrategy::DataPerTopic);
                    config.quic_flow_headers = Some(true);
                    config.quic_datagrams = Some(true);
                    config.fallback_tcp = true;
                    #[cfg(feature = "dev-insecure")]
                    if self.quic_insecure {
                        config.insecure = Some(true);
                    }
                }

                Ok(config)
            })
            .collect()
    }

    /// # Errors
    /// Returns an error if broker startup, transport connection, or address parsing fails.
    /// Also returns an error if `run()` is called more than once.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let synced_retained_topics = self.initialize_event_handler().await;

        let (bridge_configs, use_external_bridge_manager) = self.prepare_bridges()?;

        let (broker_config, service_username, service_password, needs_composite, admin_users) =
            self.configure_broker_with_event_handler(&bridge_configs, use_external_bridge_manager)
                .await?;

        let broker = self
            .build_broker(
                broker_config,
                needs_composite,
                service_username.as_ref(),
                service_password.as_ref(),
                &admin_users,
            )
            .await?;

        let (broker_handle, _bridge_manager) = self
            .start_broker(broker, &bridge_configs, use_external_bridge_manager)
            .await;

        let admin_client = self
            .connect_transport(service_username.as_ref(), service_password.as_ref())
            .await?;

        let (admin_tx, admin_rx) = flume::bounded::<AdminRequest>(32);
        subscribe_admin_topics(&admin_client, &admin_tx).await?;

        self.setup_and_spawn_raft().await?;

        self.run_event_loop(
            synced_retained_topics,
            broker_handle,
            admin_client,
            admin_rx,
            service_username.as_ref(),
            service_password.as_ref(),
        )
        .await
    }

    async fn initialize_event_handler(
        &self,
    ) -> Arc<tokio::sync::RwLock<HashMap<String, std::time::Instant>>> {
        let event_handler = Arc::new(ClusterEventHandler::new(
            self.node_id,
            self.controller.clone(),
        ));
        let synced_retained_topics = event_handler.synced_retained_topics();

        {
            let mut ctrl = self.controller.write().await;
            ctrl.set_synced_retained_topics(synced_retained_topics.clone());
        }
        synced_retained_topics
    }

    async fn configure_broker_with_event_handler(
        &self,
        bridge_configs: &[BridgeConfig],
        use_external_bridge_manager: bool,
    ) -> Result<
        (
            BrokerConfig,
            Option<String>,
            Option<String>,
            bool,
            std::collections::HashSet<String>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let event_handler = Arc::new(ClusterEventHandler::new(
            self.node_id,
            self.controller.clone(),
        ));
        self.configure_broker(event_handler, bridge_configs, use_external_bridge_manager)
            .await
    }

    async fn build_broker(
        &self,
        broker_config: BrokerConfig,
        needs_composite: bool,
        service_username: Option<&String>,
        service_password: Option<&String>,
        admin_users: &std::collections::HashSet<String>,
    ) -> Result<MqttBroker, Box<dyn std::error::Error + Send + Sync>> {
        let broker_config = self.apply_quic_config(broker_config);
        let broker_config = self.apply_ws_config(broker_config);
        let broker_config = self.apply_cluster_listener(broker_config)?;

        let mut broker = MqttBroker::with_config(broker_config).await?;

        if needs_composite
            && let (Some(svc_user), Some(svc_pass)) = (service_username, service_password)
        {
            let primary = broker.auth_provider();
            let fallback = PasswordAuthProvider::new();
            fallback
                .add_user(svc_user.clone(), svc_pass)
                .map_err(|e| format!("failed to create service account: {e}"))?;
            let composite = CompositeAuthProvider::new(primary, Arc::new(fallback));
            broker = broker.with_auth_provider(Arc::new(composite));
            info!("composite auth provider configured for internal service clients");
        }

        let current_provider = broker.auth_provider();
        let protected_provider =
            TopicProtectionAuthProvider::new(current_provider, admin_users.clone())
                .with_internal_service_username(service_username.cloned());
        broker = broker.with_auth_provider(Arc::new(protected_provider));
        if admin_users.is_empty() {
            info!("topic protection enabled (no admin users configured)");
        } else {
            info!(
                admin_users = ?admin_users,
                "topic protection enabled with admin users"
            );
        }

        Ok(broker)
    }

    async fn setup_and_spawn_raft(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

        let mut raft = self
            .raft
            .take()
            .ok_or("run() called twice: raft already taken")?;
        {
            let mut ctrl = self.controller.write().await;
            for peer in &self.peers {
                if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                    ctrl.register_peer(peer_node_id);
                    raft.add_peer(peer_node_id);
                    info!(peer_id = peer.node_id, address = %peer.address, "registered peer");
                }
            }
        }

        let raft_task = RaftTask::new(
            raft,
            self.rx_raft_messages
                .take()
                .ok_or("run() called twice: rx_raft_messages already taken")?,
            self.rx_raft_events
                .take()
                .ok_or("run() called twice: rx_raft_events already taken")?,
            self.rx_raft_admin
                .take()
                .ok_or("run() called twice: rx_raft_admin already taken")?,
            self.tx_partition_map
                .take()
                .ok_or("run() called twice: tx_partition_map already taken")?,
            self.tx_raft_status
                .take()
                .ok_or("run() called twice: tx_raft_status already taken")?,
            self.shutdown_tx.subscribe(),
            all_nodes,
        );
        tokio::spawn(async move {
            raft_task.run().await;
        });
        info!("spawned Raft task");

        Ok(())
    }

    async fn take_local_publish_receiver(
        &self,
    ) -> Option<flume::Receiver<crate::cluster::LocalPublishRequest>> {
        if self.use_direct_quic {
            let ctrl = self.controller.read().await;
            ctrl.transport()
                .as_quic()
                .map(QuicDirectTransport::local_publish_rx)
        } else {
            None
        }
    }

    fn spawn_http_task(
        &mut self,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let http_config = self.http_config.take()?;
        let http_bind = http_config.bind_address;
        let http_shutdown_rx = self.shutdown_tx.subscribe();

        let http_mqtt_client = mqtt5::MqttClient::new("mqdb-http-oauth");
        let http_connect_ip = if self.bind_address.ip().is_unspecified() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            self.bind_address.ip()
        };
        let http_addr = format!("{}:{}", http_connect_ip, self.bind_address.port());

        let http_svc_user = service_username.cloned();
        let http_svc_pass = service_password.cloned();

        Some(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;

            let connect_result = if let (Some(user), Some(pass)) = (http_svc_user, http_svc_pass) {
                let options = mqtt5::types::ConnectOptions::new("mqdb-http-oauth")
                    .with_credentials(user, pass);
                Box::pin(http_mqtt_client.connect_with_options(&http_addr, options))
                    .await
                    .map(|_| ())
            } else {
                http_mqtt_client.connect(&http_addr).await
            };

            if let Err(e) = connect_result {
                tracing::error!("Failed to connect HTTP OAuth MQTT client: {}", e);
                return;
            }

            info!(addr = %http_bind, "starting HTTP OAuth server");
            let server = crate::http::HttpServer::new(
                http_config,
                std::sync::Arc::new(http_mqtt_client),
                http_shutdown_rx,
            );
            if let Err(e) = server.run().await {
                tracing::error!("HTTP server error: {}", e);
            }
        }))
    }

    async fn run_event_loop(
        &mut self,
        synced_retained_topics: Arc<tokio::sync::RwLock<HashMap<String, std::time::Instant>>>,
        broker_handle: tokio::task::JoinHandle<()>,
        admin_client: mqtt5::MqttClient,
        admin_rx: flume::Receiver<AdminRequest>,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tick_interval = interval(Duration::from_millis(10));
        let mut cleanup_interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        let mut ttl_cleanup_interval = interval(Duration::from_secs(TTL_CLEANUP_INTERVAL_SECS));
        let mut wildcard_reconciliation_interval = interval(Duration::from_secs(60));
        let mut subscription_reconciliation_interval = interval(Duration::from_secs(300));
        let mut retained_sync_cleanup_interval =
            interval(Duration::from_secs(RETAINED_SYNC_CLEANUP_INTERVAL_SECS));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let tx_tick = self
            .tx_tick
            .take()
            .ok_or("run() called twice: tx_tick already taken")?;
        let rx_main_queue = self
            .rx_main_queue
            .take()
            .ok_or("run() called twice: rx_main_queue already taken")?;
        let rx_batch = self
            .rx_batch
            .take()
            .ok_or("run() called twice: rx_batch already taken")?;

        let rx_local_publish = self.take_local_publish_receiver().await;

        let _http_task = self.spawn_http_task(service_username, service_password);

        self.recover_pending_lwts().await;

        loop {
            tokio::select! {
                biased;

                _ = tick_interval.tick() => {
                    self.handle_tick(&tx_tick).await;
                }
                Ok(batch) = rx_batch.recv_async() => {
                    self.handle_processing_batch(batch).await;
                }
                Ok(msg) = rx_main_queue.recv_async() => {
                    self.handle_main_queue_message(msg, &rx_main_queue).await;
                }
                _ = ttl_cleanup_interval.tick() => {
                    self.handle_ttl_cleanup().await;
                }
                _ = cleanup_interval.tick() => {
                    self.handle_session_cleanup().await;
                }
                _ = wildcard_reconciliation_interval.tick() => {
                    self.handle_wildcard_reconciliation().await;
                }
                _ = subscription_reconciliation_interval.tick() => {
                    self.handle_subscription_reconciliation().await;
                }
                _ = retained_sync_cleanup_interval.tick() => {
                    Self::handle_retained_sync_cleanup(&synced_retained_topics).await;
                }
                Ok(req) = admin_rx.recv_async() => {
                    self.handle_admin_request(&admin_client, req).await;
                }
                Some(Ok(req)) = async {
                    match &rx_local_publish {
                        Some(rx) => Some(rx.recv_async().await),
                        None => std::future::pending().await,
                    }
                } => {
                    Self::handle_local_publish(&admin_client, req, rx_local_publish.as_ref()).await;
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

    async fn handle_tick(&self, tx_tick: &flume::Sender<u64>) {
        let now = current_time_ms();
        let _ = tx_tick.try_send(now);

        let raft_partition_map = self.rx_partition_map.borrow().clone();

        let mut ctrl = self.controller.write().await;
        let current_map = ctrl.partition_map().clone();
        let mut became_primary = false;
        if current_map != raft_partition_map {
            let mut changes = 0u32;
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
                    changes += 1;
                    if changes.is_multiple_of(8) {
                        tokio::task::yield_now().await;
                    }
                }
            }
            ctrl.update_partition_map(raft_partition_map);
        }
        if became_primary {
            let stores = ctrl.stores();
            let result = stores
                .subscriptions
                .reconcile(&stores.topics, &stores.wildcards);
            if result.subscriptions_added > 0 || result.subscriptions_removed > 0 {
                info!(
                    clients = result.clients_checked,
                    added = result.subscriptions_added,
                    removed = result.subscriptions_removed,
                    "reconciled subscriptions after partition takeover"
                );
            }
        }
        ctrl.transport().log_queue_stats();
    }

    async fn handle_processing_batch(&self, batch: ProcessingBatch) {
        if let Some(hb) = batch.heartbeat_to_send {
            let ctrl = self.controller.read().await;
            let _ = ctrl.transport().broadcast(hb).await;
        }

        if !batch.heartbeat_updates.is_empty() {
            let mut ctrl = self.controller.write().await;
            ctrl.apply_heartbeat_updates(&batch.heartbeat_updates);
        }

        if !batch.dead_nodes.is_empty() {
            let mut ctrl = self.controller.write().await;
            ctrl.apply_dead_nodes(&batch.dead_nodes);
            let dead_nodes: Vec<NodeId> = ctrl.drain_dead_nodes_for_session_update().collect();

            for dead_node in &dead_nodes {
                let affected_sessions = ctrl.stores().sessions.sessions_on_node(*dead_node);
                if !affected_sessions.is_empty() {
                    info!(
                        node = dead_node.get(),
                        sessions = affected_sessions.len(),
                        "marking sessions disconnected due to node death"
                    );
                    let now = current_time_ms();
                    let mut session_count = 0u32;
                    for session in affected_sessions {
                        let client_id = session.client_id_str();
                        let result = ctrl.stores_mut().update_session_replicated(client_id, |s| {
                            s.set_connected(false, *dead_node, now);
                        });
                        if let Ok((_session, write)) = result {
                            ctrl.write_or_forward(write).await;
                        }
                        session_count += 1;
                        if session_count.is_multiple_of(8) {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_main_queue_message(
        &self,
        msg: InboundMessage,
        rx_main_queue: &flume::Receiver<InboundMessage>,
    ) {
        const BATCH_SIZE: u32 = 8;
        let mut ctrl = self.controller.write().await;
        ctrl.handle_filtered_message(msg).await;
        let mut count = 1u32;
        while let Ok(msg) = rx_main_queue.try_recv() {
            ctrl.handle_filtered_message(msg).await;
            count += 1;
            if count.is_multiple_of(BATCH_SIZE) {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn handle_ttl_cleanup(&self) {
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

    async fn handle_session_cleanup(&self) {
        let now = current_time_ms();
        let expired_sessions = {
            let ctrl = self.controller.read().await;
            let expired_idempotency = ctrl.stores().idempotency.cleanup_expired(now);
            if expired_idempotency > 0 {
                info!(
                    expired_idempotency,
                    "cleaned up expired idempotency records"
                );
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
            info!(
                count = expired_sessions.len(),
                "cleaning up expired sessions and subscriptions"
            );
            let mut ctrl = self.controller.write().await;
            for session in &expired_sessions {
                let client_id = session.client_id_str();
                clear_expired_session_subscriptions(&mut ctrl, client_id).await;
            }
        }
    }

    async fn handle_wildcard_reconciliation(&self) {
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

    async fn handle_subscription_reconciliation(&self) {
        let now = current_time_ms();
        let ctrl = self.controller.read().await;
        let stores = ctrl.stores();
        if stores.subscriptions.needs_reconciliation(now) {
            let result = stores
                .subscriptions
                .reconcile(&stores.topics, &stores.wildcards);
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

    async fn handle_retained_sync_cleanup(
        synced_retained_topics: &Arc<tokio::sync::RwLock<HashMap<String, std::time::Instant>>>,
    ) {
        let ttl = std::time::Duration::from_secs(RETAINED_SYNC_TTL_SECS);
        let mut synced = synced_retained_topics.write().await;
        let before_len = synced.len();
        synced.retain(|_, insert_time| insert_time.elapsed() < ttl);
        let removed = before_len - synced.len();
        if removed > 0 {
            debug!(
                removed,
                remaining = synced.len(),
                "cleaned up stale retained sync entries"
            );
        }
    }

    async fn handle_local_publish(
        admin_client: &mqtt5::MqttClient,
        req: crate::cluster::LocalPublishRequest,
        rx_local_publish: Option<&flume::Receiver<crate::cluster::LocalPublishRequest>>,
    ) {
        const BATCH_SIZE: u32 = 64;
        let options = mqtt5::PublishOptions {
            qos: mqtt5::QoS::from(req.qos),
            retain: req.retain,
            ..Default::default()
        };
        if let Err(e) = admin_client
            .publish_with_options(&req.topic, req.payload, options)
            .await
        {
            warn!(error = %e, topic = %req.topic, "failed to publish local request");
        }

        if let Some(rx) = rx_local_publish {
            let mut count = 1u32;
            while let Ok(req) = rx.try_recv() {
                let options = mqtt5::PublishOptions {
                    qos: mqtt5::QoS::from(req.qos),
                    retain: req.retain,
                    ..Default::default()
                };
                if let Err(e) = admin_client
                    .publish_with_options(&req.topic, req.payload, options)
                    .await
                {
                    warn!(error = %e, topic = %req.topic, "failed to publish local request");
                }
                count += 1;
                if count.is_multiple_of(BATCH_SIZE) {
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    fn prepare_bridges(
        &mut self,
    ) -> Result<(Vec<BridgeConfig>, bool), Box<dyn std::error::Error + Send + Sync>> {
        if self.use_direct_quic {
            return Ok((vec![], false));
        }
        if !self.peers.is_empty() && self.bridge_executor.is_none() {
            let worker_threads = (self.peers.len() + 1).clamp(2, 8);
            self.bridge_executor = Some(DedicatedExecutor::new("bridge-io", worker_threads));
            info!(
                workers = worker_threads,
                bridges = self.peers.len(),
                "created dedicated bridge executor"
            );
        }
        let configs = self.create_bridge_configs()?;
        let use_external = self.bridge_executor.is_some() && !configs.is_empty();
        Ok((configs, use_external))
    }

    #[allow(clippy::type_complexity)]
    async fn configure_broker(
        &self,
        event_handler: Arc<ClusterEventHandler<ClusterTransportKind>>,
        bridge_configs: &[BridgeConfig],
        use_external_bridge_manager: bool,
    ) -> Result<
        (
            BrokerConfig,
            Option<String>,
            Option<String>,
            bool,
            std::collections::HashSet<String>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let storage_config = StorageConfig {
            backend: StorageBackend::Memory,
            enable_persistence: true,
            ..Default::default()
        };

        let broker_config = BrokerConfig {
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
            bridges: if use_external_bridge_manager {
                vec![]
            } else {
                bridge_configs.to_vec()
            },
            change_only_delivery_config: ChangeOnlyDeliveryConfig {
                enabled: true,
                topic_patterns: vec!["$DB/+/events/#".to_string()],
            },
            ..Default::default()
        }
        .with_storage(storage_config)
        .with_event_handler(event_handler);

        let auth_cfg = crate::auth_config::AuthSetupConfig {
            password_file: self.password_file.clone(),
            acl_file: self.acl_file.clone(),
            allow_anonymous: self.password_file.is_none(),
            scram_file: self.auth_setup.scram_file.clone(),
            jwt_config: self.auth_setup.jwt_config.clone(),
            federated_jwt_config: self.auth_setup.federated_jwt_config.clone(),
            cert_auth_file: self.auth_setup.cert_auth_file.clone(),
            rate_limit: self.auth_setup.rate_limit.clone(),
            no_rate_limit: self.auth_setup.no_rate_limit,
            admin_users: self.auth_setup.admin_users.clone(),
        };
        let mut config = broker_config;
        let auth_result =
            crate::auth_config::configure_broker_auth(&auth_cfg, &mut config.auth_config)
                .await
                .map_err(|e| format!("auth setup failed: {e}"))?;

        Ok((
            config,
            auth_result.service_username,
            auth_result.service_password,
            auth_result.needs_composite,
            auth_result.admin_users,
        ))
    }

    fn apply_quic_config(&self, broker_config: BrokerConfig) -> BrokerConfig {
        if self.use_quic {
            if let (Some(cert_file), Some(key_file)) = (&self.quic_cert_file, &self.quic_key_file) {
                let quic_config = QuicConfig::new(cert_file.clone(), key_file.clone())
                    .with_bind_address(self.bind_address);
                info!(quic_bind = %self.bind_address, "QUIC listener configured (same port as TCP)");
                return broker_config.with_quic(quic_config);
            }
            info!(
                "QUIC enabled but no certs provided - bridges will use QUIC, but no QUIC listener"
            );
        }
        broker_config
    }

    fn apply_ws_config(&self, broker_config: BrokerConfig) -> BrokerConfig {
        if let Some(ws_addr) = self.ws_bind_address {
            let ws_config = WebSocketConfig::new().with_bind_addresses(vec![ws_addr]);
            info!(ws_bind = %ws_addr, "WebSocket listener configured");
            return broker_config.with_websocket(ws_config);
        }
        broker_config
    }

    fn apply_cluster_listener(
        &self,
        broker_config: BrokerConfig,
    ) -> Result<BrokerConfig, Box<dyn std::error::Error + Send + Sync>> {
        if self.use_direct_quic {
            return Ok(broker_config);
        }
        let cluster_addr: SocketAddr =
            format!("{}:{}", self.bind_address.ip(), self.cluster_port())
                .parse()
                .map_err(|e| format!("invalid cluster address: {e}"))?;

        let cluster_listener = if let (Some(cert_file), Some(key_file)) =
            (&self.quic_cert_file, &self.quic_key_file)
        {
            ClusterListenerConfig::quic(vec![cluster_addr], cert_file.clone(), key_file.clone())
        } else {
            ClusterListenerConfig::new(vec![cluster_addr])
        };
        info!(cluster_port = %cluster_addr, "cluster listener configured (skip_bridge_forwarding=true)");
        Ok(broker_config.with_cluster_listener(cluster_listener))
    }

    async fn start_broker(
        &self,
        mut broker: MqttBroker,
        bridge_configs: &[BridgeConfig],
        use_external_bridge_manager: bool,
    ) -> (tokio::task::JoinHandle<()>, Option<Arc<BridgeManager>>) {
        let mut ready_rx = broker.ready_receiver();

        let router = if use_external_bridge_manager {
            Some(broker.router())
        } else {
            None
        };

        let broker_handle = tokio::spawn(async move {
            let _ = broker.run().await;
        });

        info!("waiting for broker ready signal");
        let _ = ready_rx.changed().await;
        info!("broker ready signal received");

        let bridge_manager = if let (Some(router), Some(executor)) = (router, &self.bridge_executor)
        {
            let manager = Arc::new(BridgeManager::with_runtime(
                router.clone(),
                executor.handle(),
            ));
            router.set_bridge_manager(manager.clone()).await;
            for config in bridge_configs {
                if let Err(e) = manager.add_bridge(config.clone()) {
                    warn!(bridge = %config.name, error = %e, "failed to add bridge");
                }
            }
            info!(
                bridges = bridge_configs.len(),
                "started bridges on dedicated executor"
            );
            Some(manager)
        } else {
            None
        };

        info!(
            node_id = self.node_id.get(),
            node_name = %self.node_name,
            bind = %self.bind_address,
            peers = ?self.peers,
            "cluster node started"
        );

        (broker_handle, bridge_manager)
    }

    async fn connect_transport(
        &self,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<mqtt5::MqttClient, Box<dyn std::error::Error + Send + Sync>> {
        let connect_ip = if self.bind_address.ip().is_unspecified() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            self.bind_address.ip()
        };
        let broker_addr = format!("{connect_ip}:{}", self.bind_address.port());

        let transport = {
            let ctrl = self.controller.read().await;
            ctrl.transport().clone()
        };

        if self.use_direct_quic {
            self.connect_quic_transport(
                &transport,
                &broker_addr,
                service_username,
                service_password,
            )
            .await
        } else {
            self.connect_mqtt_transport(
                &transport,
                &broker_addr,
                service_username,
                service_password,
            )
            .await
        }
    }

    async fn connect_quic_transport(
        &self,
        transport: &ClusterTransportKind,
        broker_addr: &str,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<mqtt5::MqttClient, Box<dyn std::error::Error + Send + Sync>> {
        let quic_transport = transport
            .as_quic()
            .ok_or("expected QUIC transport but found MQTT transport")?;
        let cluster_addr: SocketAddr =
            format!("{}:{}", self.bind_address.ip(), self.cluster_port())
                .parse()
                .map_err(|e| format!("invalid cluster address: {e}"))?;

        let (cert_file, key_file) = match (&self.quic_cert_file, &self.quic_key_file) {
            (Some(c), Some(k)) => (c.clone(), k.clone()),
            _ => return Err("QUIC cert and key files required for direct QUIC mode".into()),
        };

        quic_transport
            .bind(cluster_addr, &cert_file, &key_file)
            .await
            .map_err(|e| format!("failed to bind QUIC transport: {e}"))?;
        info!(addr = %cluster_addr, "QUIC direct transport bound");

        for peer in &self.peers {
            if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                let peer_addr: SocketAddr = match peer.address.parse() {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!(peer = peer_node_id.get(), address = %peer.address, error = %e, "invalid peer address");
                        continue;
                    }
                };
                let peer_cluster_addr: SocketAddr = match format!(
                    "{}:{}",
                    peer_addr.ip(),
                    peer_addr.port() + self.cluster_port_offset
                )
                .parse()
                {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!(peer = peer_node_id.get(), error = %e, "invalid peer cluster address");
                        continue;
                    }
                };
                if let Err(e) = quic_transport
                    .connect_to_peer(peer_node_id, peer_cluster_addr)
                    .await
                {
                    warn!(peer = peer_node_id.get(), addr = %peer_cluster_addr, error = %e, "failed to connect to peer via QUIC");
                } else {
                    info!(peer = peer_node_id.get(), addr = %peer_cluster_addr, "connected to peer via QUIC");
                }
            }
        }

        info!(broker_addr = %broker_addr, "creating separate MQTT client for admin");
        let admin_client_id = format!("mqdb-admin-{}", self.node_id.get());
        let admin_mqtt_client = mqtt5::MqttClient::new(&admin_client_id);
        if let Some(user) = service_username {
            let options = mqtt5::types::ConnectOptions::new(&admin_client_id)
                .with_credentials(user, service_password.map_or("", String::as_str));
            Box::pin(admin_mqtt_client.connect_with_options(broker_addr, options))
                .await
                .map_err(|e| format!("failed to connect admin client: {e}"))?;
        } else {
            admin_mqtt_client
                .connect(broker_addr)
                .await
                .map_err(|e| format!("failed to connect admin client: {e}"))?;
        }
        info!("admin MQTT client connected");
        Ok(admin_mqtt_client)
    }

    #[allow(deprecated)]
    async fn connect_mqtt_transport(
        &self,
        transport: &ClusterTransportKind,
        broker_addr: &str,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<mqtt5::MqttClient, Box<dyn std::error::Error + Send + Sync>> {
        let mqtt_transport = transport
            .as_mqtt()
            .ok_or("expected MQTT transport but found QUIC")?;
        info!(broker_addr = %broker_addr, "connecting transport to local broker");
        Box::pin(mqtt_transport.connect_with_credentials(
            broker_addr,
            service_username.map(String::as_str),
            service_password.map(String::as_str),
        ))
        .await
        .map_err(|e| format!("failed to connect transport to local broker: {e}"))?;
        info!("transport connected to local broker");
        Ok(mqtt_transport.client().clone())
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
        let raft_status = self.rx_raft_status.borrow().clone();

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

        let stores = ctrl.stores();
        Response::ok(json!({
            "node_id": self.node_id.get(),
            "node_name": self.node_name,
            "is_raft_leader": raft_status.is_leader,
            "raft_term": raft_status.current_term,
            "raft_log_len": raft_status.log_len,
            "raft_commit_index": raft_status.commit_index,
            "raft_last_applied": raft_status.last_applied,
            "raft_last_log_index": raft_status.last_log_index,
            "store_subscriptions": stores.subscriptions.len(),
            "store_topics": stores.topics.len(),
            "store_client_locations": stores.client_locations.len(),
            "store_db_data": stores.db_data.len(),
            "alive_nodes": alive_nodes,
            "partition_count": NUM_PARTITIONS,
            "partitions": partitions
        }))
    }

    async fn handle_rebalance_request(&self) -> Response {
        let raft_status = self.rx_raft_status.borrow().clone();

        if !raft_status.is_leader {
            return Response::error(ErrorCode::Forbidden, "not the Raft leader");
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx_raft_admin
            .send(RaftAdminCommand::ForceRebalance(tx))
            .is_err()
        {
            return Response::error(ErrorCode::Internal, "raft task not available");
        }

        match rx.await {
            Ok(proposals) => Response::ok(json!({
                "proposed_changes": proposals
            })),
            Err(_) => Response::error(ErrorCode::Internal, "failed to get rebalance response"),
        }
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
        let raft_status = self.rx_raft_status.borrow().clone();

        let is_leader = raft_status.is_leader;
        let leader_exists = is_leader || raft_status.leader_id.is_some();
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
                "raft_term": raft_status.current_term,
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
    pub fn controller(&self) -> &Arc<RwLock<NodeController<ClusterTransportKind>>> {
        &self.controller
    }

    #[must_use]
    pub fn bridge_runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        self.bridge_executor.as_ref().map(DedicatedExecutor::handle)
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

async fn subscribe_admin_topics(
    client: &mqtt5::MqttClient,
    tx: &flume::Sender<AdminRequest>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (topic, label) in [
        ("$SYS/mqdb/cluster/#", "admin topics"),
        ("$DB/_health", "health topic"),
        ("$DB/_admin/#", "db admin topics"),
    ] {
        let tx = tx.clone();
        client
            .subscribe(topic, move |msg: Message| {
                let req = AdminRequest {
                    topic: msg.topic.clone(),
                    response_topic: msg.properties.response_topic.clone(),
                    payload: msg.payload.clone(),
                };
                let _ = tx.try_send(req);
            })
            .await
            .map_err(|e| format!("failed to subscribe to {label}: {e}"))?;
    }
    info!("subscribed to admin topics");
    Ok(())
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

async fn clear_expired_session_subscriptions<T: ClusterTransport>(
    ctrl: &mut NodeController<T>,
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
                let _ = ctrl.stores_mut().topics.unsubscribe(topic, client_id);
                let broadcast = TopicSubscriptionBroadcast::unsubscribe(topic, client_id);
                let msg = ClusterMessage::TopicSubscriptionBroadcast(broadcast);
                let _ = ctrl.transport().broadcast(msg).await;
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
