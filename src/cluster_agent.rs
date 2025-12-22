use crate::cluster::{
    MqttTransport, NodeController, NodeId, PartitionId, TransportConfig,
};
use crate::Database;
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection};
use mqtt5::broker::{BrokerConfig, MqttBroker};
use mqtt5::time::Duration;
use mqtt5::QoS;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
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
            bind_address: "0.0.0.0:1883".parse().unwrap(),
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
    db: Arc<Database>,
    controller: NodeController<MqttTransport>,
    shutdown_tx: broadcast::Sender<()>,
    bind_address: SocketAddr,
    peers: Vec<PeerConfig>,
    password_file: Option<PathBuf>,
    acl_file: Option<PathBuf>,
}

impl ClusteredAgent {
    /// # Errors
    /// Returns an error if the node ID is invalid (must be 1-65535).
    pub fn new(config: ClusterConfig, db: Database) -> Result<Self, String> {
        let node_id =
            NodeId::validated(config.node_id).ok_or("invalid node_id: must be 1-65535")?;

        let transport = MqttTransport::new(node_id);
        let transport_config = TransportConfig::default();
        let controller = NodeController::new(node_id, transport, transport_config);

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            node_id,
            node_name: config.node_name,
            db: Arc::new(db),
            controller,
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
                    .add_topic("$SYS/mqdb/cluster/#", BridgeDirection::Both, QoS::AtLeastOnce)
                    .add_topic("$DB/_repl/#", BridgeDirection::Both, QoS::AtLeastOnce);
                config.client_id = format!("{}-to-node-{}", self.node_name, peer.node_id);
                config.clean_start = false;
                config.try_private = true;
                config
            })
            .collect()
    }

    /// # Errors
    /// Returns an error if broker startup, transport connection, or cluster event loop fails.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        };

        if let Some(ref path) = self.password_file {
            broker_config.auth_config.password_file = Some(path.clone());
            broker_config.auth_config.allow_anonymous = false;
        }
        if let Some(ref path) = self.acl_file {
            broker_config.auth_config.acl_file = Some(path.clone());
        }

        let broker = MqttBroker::with_config(broker_config).await?;

        info!(
            node_id = self.node_id.get(),
            node_name = %self.node_name,
            bind = %self.bind_address,
            peers = ?self.peers,
            "cluster node started"
        );

        tokio::time::sleep(Duration::from_millis(500)).await;

        let broker_addr = format!("127.0.0.1:{}", self.bind_address.port());
        self.controller
            .transport()
            .connect(&broker_addr)
            .await
            .map_err(|e| format!("failed to connect transport to local broker: {e}"))?;

        info!("transport connected to local broker");

        for peer in &self.peers {
            if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                self.controller.register_peer(peer_node_id);
                info!(peer_id = peer.node_id, address = %peer.address, "registered peer");
            }
        }

        for partition in PartitionId::all() {
            let partition_num = partition.get();
            let is_primary = partition_num % 2 == (self.node_id.get() % 2);
            if is_primary {
                self.controller
                    .become_primary(partition, crate::cluster::Epoch::new(1));
            }
        }

        let mut tick_interval = interval(Duration::from_millis(100));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let now = current_time_ms();
                    self.controller.tick(now);
                    self.controller.process_messages();
                }
                _ = shutdown_rx.recv() => {
                    info!("cluster node shutting down");
                    break;
                }
            }
        }

        broker.shutdown().await?;
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
    pub fn controller(&self) -> &NodeController<MqttTransport> {
        &self.controller
    }

    pub fn controller_mut(&mut self) -> &mut NodeController<MqttTransport> {
        &mut self.controller
    }
}

#[allow(clippy::cast_possible_truncation)]
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
