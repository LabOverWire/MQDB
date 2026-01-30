use super::{ClusterConfig, QuicConfig};
use crate::config::DurabilityMode;
use mqtt5::broker::config::{FederatedJwtConfig, JwtConfig, RateLimitConfig};
use std::net::SocketAddr;
use std::path::PathBuf;

impl ClusterConfig {
    #[must_use]
    pub fn new(node_id: u16, db_path: PathBuf, peers: Vec<super::PeerConfig>) -> Self {
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
            quic: QuicConfig {
                enabled: true,
                #[cfg(feature = "dev-insecure")]
                insecure: false,
                cert_file: None,
                key_file: None,
                ca_file: None,
                direct: true,
            },
            bridge_out_only: false,
            ws_bind_address: None,
            http_config: None,
            ownership: crate::types::OwnershipConfig::default(),
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
        self.quic.enabled = enabled;
        self
    }

    #[cfg(feature = "dev-insecure")]
    #[must_use]
    pub fn with_quic_insecure(mut self, insecure: bool) -> Self {
        self.quic.insecure = insecure;
        self
    }

    #[must_use]
    pub fn with_quic_certs(mut self, cert_file: PathBuf, key_file: PathBuf) -> Self {
        self.quic.cert_file = Some(cert_file);
        self.quic.key_file = Some(key_file);
        self
    }

    #[must_use]
    pub fn with_quic_ca(mut self, ca_file: PathBuf) -> Self {
        self.quic.ca_file = Some(ca_file);
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
        self.quic.direct = use_direct_quic;
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

    #[must_use]
    pub fn with_ownership(mut self, ownership: crate::types::OwnershipConfig) -> Self {
        self.ownership = ownership;
        self
    }
}
