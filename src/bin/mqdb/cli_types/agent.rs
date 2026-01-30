use clap::Subcommand;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::auth::{AuthArgs, OAuthArgs};
use super::base::{ConnectionArgs, DurabilityArg};

#[derive(Subcommand)]
pub(crate) enum AgentAction {
    Start {
        #[arg(long, default_value = "127.0.0.1:1883")]
        bind: SocketAddr,
        #[arg(long)]
        db: PathBuf,
        #[command(flatten)]
        auth: Box<AuthArgs>,
        #[arg(
            long,
            default_value = "periodic",
            help = "Durability mode: immediate (fsync every write), periodic (fsync periodically), none (no fsync)"
        )]
        durability: DurabilityArg,
        #[arg(
            long,
            default_value = "10",
            help = "Fsync interval in ms when using periodic durability"
        )]
        durability_ms: u64,
        #[arg(long, help = "Path to QUIC/TLS certificate file (PEM format)")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC/TLS private key file (PEM format)")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "WebSocket bind address (e.g. 0.0.0.0:8080)")]
        ws_bind: Option<SocketAddr>,
        #[command(flatten)]
        oauth: Box<OAuthArgs>,
    },
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
pub(crate) enum ClusterAction {
    #[command(about = "Start a cluster node")]
    Start {
        #[arg(long, help = "Unique node ID (1-65535)")]
        node_id: u16,
        #[arg(long, help = "Human-readable node name")]
        node_name: Option<String>,
        #[arg(
            long,
            default_value = "0.0.0.0:1883",
            help = "Address to bind MQTT listener"
        )]
        bind: SocketAddr,
        #[arg(long, help = "Path to database directory")]
        db: PathBuf,
        #[arg(long, value_delimiter = ',', help = "Peer nodes: id@host:port,...")]
        peers: Vec<String>,
        #[command(flatten)]
        auth: Box<AuthArgs>,
        #[arg(long, help = "Path to QUIC TLS certificate")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC TLS private key")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "Path to CA certificate for QUIC peer verification")]
        quic_ca: Option<PathBuf>,
        #[arg(long, help = "Disable QUIC transport")]
        no_quic: bool,
        #[arg(
            long,
            help = "Disable store persistence (data will not survive restarts)"
        )]
        no_persist_stores: bool,
        #[arg(
            long,
            default_value = "periodic",
            help = "Durability mode for stores: immediate (fsync every write), periodic (fsync periodically), none (no fsync). Raft always uses immediate."
        )]
        durability: DurabilityArg,
        #[arg(
            long,
            default_value = "10",
            help = "Fsync interval in ms when using periodic durability"
        )]
        durability_ms: u64,
        #[arg(
            long,
            help = "Use outgoing-only bridge direction (for full mesh topology)"
        )]
        bridge_out: bool,
        #[arg(
            long,
            default_value = "100",
            help = "Port offset for cluster listener (bridges connect to main_port + offset)"
        )]
        cluster_port_offset: u16,
        #[cfg(feature = "dev-insecure")]
        #[arg(
            long,
            help = "Skip TLS certificate verification for direct QUIC (dev only)"
        )]
        quic_insecure: bool,
        #[arg(long, help = "WebSocket bind address (e.g. 0.0.0.0:8080)")]
        ws_bind: Option<SocketAddr>,
        #[command(flatten)]
        oauth: Box<OAuthArgs>,
    },
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
