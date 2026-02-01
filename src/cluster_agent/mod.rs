mod admin;
mod broker;
mod config;
mod event_loop;
mod init;
mod transport;

pub use transport::ClusterTransportKind;

use crate::cluster::raft::RaftCoordinator;
use crate::cluster::{
    DedicatedExecutor, InboundMessage, NodeController, NodeId, PartitionMap, ProcessingBatch,
    RaftAdminCommand, RaftEvent, RaftStatus,
};
use crate::config::DurabilityMode;
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, watch};

const CLEANUP_INTERVAL_SECS: u64 = 3600;
const TTL_CLEANUP_INTERVAL_SECS: u64 = 60;
const RETAINED_SYNC_CLEANUP_INTERVAL_SECS: u64 = 30;
const RETAINED_SYNC_TTL_SECS: u64 = 5;

const RAFT_CHANNEL_CAPACITY: usize = 4096;
const MAIN_QUEUE_CAPACITY: usize = 4096;
const BATCH_QUEUE_CAPACITY: usize = 16;

#[derive(Debug)]
pub enum ClusterInitError {
    InvalidNodeId(u16),
    StorageOpen { path: PathBuf, source: crate::Error },
    RaftInit(crate::Error),
}

impl std::fmt::Display for ClusterInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNodeId(id) => write!(f, "invalid node_id {id}: must be 1-65535"),
            Self::StorageOpen { path, source } => {
                write!(f, "failed to open storage at {}: {source}", path.display())
            }
            Self::RaftInit(e) => write!(f, "failed to initialize raft: {e}"),
        }
    }
}

impl std::error::Error for ClusterInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageOpen { source, .. } | Self::RaftInit(source) => Some(source),
            Self::InvalidNodeId(_) => None,
        }
    }
}

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

#[derive(Debug)]
struct AdminRequest {
    topic: String,
    response_topic: Option<String>,
    payload: Vec<u8>,
}

pub struct QuicConfig {
    pub enabled: bool,
    #[cfg(feature = "dev-insecure")]
    pub insecure: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub direct: bool,
}

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
    pub quic: QuicConfig,
    pub bridge_out_only: bool,
    pub ws_bind_address: Option<SocketAddr>,
    pub http_config: Option<crate::http::HttpServerConfig>,
    pub ownership: crate::types::OwnershipConfig,
    pub scope_config: crate::types::ScopeConfig,
}

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
    quic: QuicConfig,
    bridge_out_only: bool,
    cluster_port_offset: u16,
    bridge_executor: Option<DedicatedExecutor>,
    #[allow(dead_code)]
    processor_executor: Option<DedicatedExecutor>,
    tx_tick: Option<flume::Sender<u64>>,
    rx_main_queue: Option<flume::Receiver<InboundMessage>>,
    rx_batch: Option<flume::Receiver<ProcessingBatch>>,
    ws_bind_address: Option<SocketAddr>,
    http_config: Option<crate::http::HttpServerConfig>,
    ownership: Arc<crate::types::OwnershipConfig>,
    auth_providers: Option<Arc<ComprehensiveAuthProvider>>,
}

impl ClusteredAgent {
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
}
