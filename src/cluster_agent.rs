use crate::cluster::{
    ClusterEventHandler, Epoch, MqttTransport, NodeController, NodeId, PartitionAssignment,
    PartitionId, PartitionMap, TransportConfig,
};
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection};
use mqtt5::broker::config::{StorageBackend, StorageConfig};
use mqtt5::broker::{BrokerConfig, MqttBroker};
use mqtt5::time::Duration;
use mqtt5::QoS;
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
        let controller = NodeController::new(node_id, transport, transport_config);

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            node_id,
            node_name: config.node_name,
            controller: Arc::new(RwLock::new(controller)),
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
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event_handler = Arc::new(ClusterEventHandler::new(
            self.node_id,
            self.controller.clone(),
        ));

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
            for peer in &self.peers {
                if let Some(peer_node_id) = NodeId::validated(peer.node_id) {
                    ctrl.register_peer(peer_node_id);
                    info!(peer_id = peer.node_id, address = %peer.address, "registered peer");
                }
            }

            let mut all_nodes: Vec<NodeId> = self
                .peers
                .iter()
                .filter_map(|p| NodeId::validated(p.node_id))
                .collect();
            all_nodes.push(self.node_id);
            all_nodes.sort_by_key(|n| n.get());

            let node_count = all_nodes.len();
            let mut partition_map = PartitionMap::new();

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

                let assignment = PartitionAssignment::new(primary, replicas, Epoch::new(1));
                partition_map.set(partition, assignment);

                if primary == self.node_id {
                    ctrl.become_primary(partition, Epoch::new(1));
                } else if node_count > 1 && all_nodes[replica_idx] == self.node_id {
                    ctrl.become_replica(partition, Epoch::new(1), 0);
                }
            }

            ctrl.update_partition_map(partition_map);
        }

        let mut tick_interval = interval(Duration::from_millis(100));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let now = current_time_ms();
                    let mut ctrl = self.controller.write().await;
                    ctrl.tick(now);
                    ctrl.process_messages();
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
