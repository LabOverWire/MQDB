// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::auth::{AuthArgs, OAuthArgs};
use super::base::{ConnectionArgs, DurabilityArg};

#[derive(clap::Args)]
pub(crate) struct AgentStartFields {
    #[arg(
        long,
        env = "MQDB_BIND",
        default_value = "127.0.0.1:1883",
        help = "Address to bind MQTT listener"
    )]
    pub(crate) bind: SocketAddr,
    #[arg(long, env = "MQDB_DB", help = "Path to database directory")]
    pub(crate) db: PathBuf,
    #[arg(
        long,
        env = "MQDB_MEMORY_BACKEND",
        help = "Use in-memory storage backend (no persistence; for benchmarking only)"
    )]
    pub(crate) memory_backend: bool,
    #[command(flatten)]
    pub(crate) auth: Box<AuthArgs>,
    #[arg(
        long,
        env = "MQDB_DURABILITY",
        default_value = "periodic",
        help = "Durability mode: immediate (fsync every write), periodic (fsync periodically), none (no fsync)"
    )]
    pub(crate) durability: DurabilityArg,
    #[arg(
        long,
        env = "MQDB_DURABILITY_MS",
        default_value = "10",
        help = "Fsync interval in ms when using periodic durability"
    )]
    pub(crate) durability_ms: u64,
    #[arg(
        long,
        env = "MQDB_QUIC_CERT_FILE",
        help = "Path to QUIC/TLS certificate file (PEM format)"
    )]
    pub(crate) quic_cert: Option<PathBuf>,
    #[arg(
        long,
        env = "MQDB_QUIC_KEY_FILE",
        help = "Path to QUIC/TLS private key file (PEM format)"
    )]
    pub(crate) quic_key: Option<PathBuf>,
    #[arg(
        long,
        env = "MQDB_WS_BIND",
        help = "WebSocket bind address (e.g. 0.0.0.0:8080)"
    )]
    pub(crate) ws_bind: Option<SocketAddr>,
    #[command(flatten)]
    pub(crate) oauth: Box<OAuthArgs>,
    #[arg(
        long,
        env = "MQDB_OWNERSHIP",
        help = "Ownership config: entity=field pairs (e.g. diagrams=userId)"
    )]
    pub(crate) ownership: Option<String>,
    #[arg(
        long,
        env = "MQDB_EVENT_SCOPE",
        help = "Scope events by entity field (e.g. diagrams=diagramId)"
    )]
    pub(crate) event_scope: Option<String>,
    #[arg(
        long,
        env = "MQDB_PASSPHRASE_FILE",
        help = "Path to file containing encryption passphrase"
    )]
    pub(crate) passphrase_file: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_PASSPHRASE", hide = true)]
    pub(crate) passphrase_data: Option<String>,
    #[arg(long, env = "MQDB_LICENSE_FILE", help = "Path to license key file")]
    pub(crate) license: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_LICENSE", hide = true)]
    pub(crate) license_data: Option<String>,
    #[arg(long = "", env = "MQDB_QUIC_CERT", hide = true)]
    pub(crate) quic_cert_data: Option<String>,
    #[arg(long = "", env = "MQDB_QUIC_KEY", hide = true)]
    pub(crate) quic_key_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_VAULT_MIN_PASSPHRASE_LENGTH",
        default_value = "0",
        help = "Minimum passphrase length for vault enable/change (0 = no minimum)"
    )]
    pub(crate) vault_min_passphrase_length: usize,
    #[arg(
        long,
        env = "MQDB_OTLP_ENDPOINT",
        help = "OTLP collector endpoint (enables OpenTelemetry tracing)"
    )]
    pub(crate) otlp_endpoint: Option<String>,
    #[arg(
        long,
        env = "MQDB_OTEL_SERVICE_NAME",
        default_value = "mqdb",
        help = "Service name for OTel traces"
    )]
    pub(crate) otel_service_name: String,
    #[arg(
        long,
        env = "MQDB_OTEL_SAMPLING_RATIO",
        default_value = "0.1",
        help = "OTel sampling ratio 0.0-1.0"
    )]
    pub(crate) otel_sampling_ratio: f64,
}

#[derive(Subcommand)]
pub(crate) enum AgentAction {
    #[command(about = "Start a standalone MQTT broker agent")]
    Start(Box<AgentStartFields>),
    #[command(about = "Check broker connectivity status")]
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[cfg(feature = "cluster")]
#[derive(clap::Args)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ClusterStartFields {
    #[arg(long, env = "MQDB_NODE_ID", help = "Unique node ID (1-65535)")]
    pub(crate) node_id: u16,
    #[arg(long, env = "MQDB_NODE_NAME", help = "Human-readable node name")]
    pub(crate) node_name: Option<String>,
    #[arg(
        long,
        env = "MQDB_BIND",
        default_value = "0.0.0.0:1883",
        help = "Address to bind MQTT listener"
    )]
    pub(crate) bind: SocketAddr,
    #[arg(long, env = "MQDB_DB", help = "Path to database directory")]
    pub(crate) db: PathBuf,
    #[arg(
        long,
        env = "MQDB_PEERS",
        value_delimiter = ',',
        help = "Peer nodes: id@host:port,..."
    )]
    pub(crate) peers: Vec<String>,
    #[command(flatten)]
    pub(crate) auth: Box<AuthArgs>,
    #[arg(
        long,
        env = "MQDB_QUIC_CERT_FILE",
        help = "Path to QUIC TLS certificate"
    )]
    pub(crate) quic_cert: Option<PathBuf>,
    #[arg(
        long,
        env = "MQDB_QUIC_KEY_FILE",
        help = "Path to QUIC TLS private key"
    )]
    pub(crate) quic_key: Option<PathBuf>,
    #[arg(
        long,
        env = "MQDB_QUIC_CA_FILE",
        help = "Path to CA certificate for QUIC peer verification"
    )]
    pub(crate) quic_ca: Option<PathBuf>,
    #[arg(long, env = "MQDB_NO_QUIC", help = "Disable QUIC transport")]
    pub(crate) no_quic: bool,
    #[arg(
        long,
        env = "MQDB_NO_PERSIST_STORES",
        help = "Disable store persistence (data will not survive restarts)"
    )]
    pub(crate) no_persist_stores: bool,
    #[arg(
        long,
        env = "MQDB_DURABILITY",
        default_value = "periodic",
        help = "Durability mode for stores: immediate (fsync every write), periodic (fsync periodically), none (no fsync). Raft always uses immediate."
    )]
    pub(crate) durability: DurabilityArg,
    #[arg(
        long,
        env = "MQDB_DURABILITY_MS",
        default_value = "10",
        help = "Fsync interval in ms when using periodic durability"
    )]
    pub(crate) durability_ms: u64,
    #[arg(
        long,
        env = "MQDB_BRIDGE_OUT",
        help = "Use outgoing-only bridge direction (for full mesh topology)"
    )]
    pub(crate) bridge_out: bool,
    #[arg(
        long,
        env = "MQDB_CLUSTER_PORT_OFFSET",
        default_value = "100",
        help = "Port offset for cluster listener (bridges connect to main_port + offset)"
    )]
    pub(crate) cluster_port_offset: u16,
    #[cfg(feature = "dev-insecure")]
    #[arg(
        long,
        help = "Skip TLS certificate verification for direct QUIC (dev only)"
    )]
    pub(crate) quic_insecure: bool,
    #[arg(
        long,
        env = "MQDB_WS_BIND",
        help = "WebSocket bind address (e.g. 0.0.0.0:8080)"
    )]
    pub(crate) ws_bind: Option<SocketAddr>,
    #[command(flatten)]
    pub(crate) oauth: Box<OAuthArgs>,
    #[arg(
        long,
        env = "MQDB_OWNERSHIP",
        help = "Ownership config: entity=field pairs (e.g. diagrams=userId)"
    )]
    pub(crate) ownership: Option<String>,
    #[arg(
        long,
        env = "MQDB_EVENT_SCOPE",
        help = "Scope events by entity field (e.g. diagrams=diagramId)"
    )]
    pub(crate) event_scope: Option<String>,
    #[arg(
        long,
        env = "MQDB_PASSPHRASE_FILE",
        help = "Path to file containing encryption passphrase"
    )]
    pub(crate) passphrase_file: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_PASSPHRASE", hide = true)]
    pub(crate) passphrase_data: Option<String>,
    #[arg(long, env = "MQDB_LICENSE_FILE", help = "Path to license key file")]
    pub(crate) license: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_LICENSE", hide = true)]
    pub(crate) license_data: Option<String>,
    #[arg(long = "", env = "MQDB_QUIC_CERT", hide = true)]
    pub(crate) quic_cert_data: Option<String>,
    #[arg(long = "", env = "MQDB_QUIC_KEY", hide = true)]
    pub(crate) quic_key_data: Option<String>,
    #[arg(long = "", env = "MQDB_QUIC_CA", hide = true)]
    pub(crate) quic_ca_data: Option<String>,
}

#[cfg(feature = "cluster")]
#[derive(Subcommand)]
pub(crate) enum ClusterAction {
    #[command(about = "Start a cluster node")]
    Start(Box<ClusterStartFields>),
    #[command(about = "Trigger partition rebalancing across cluster nodes")]
    Rebalance {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    #[command(about = "Show cluster status and partition assignments")]
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}
