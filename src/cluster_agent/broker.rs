use super::{AdminRequest, ClusterTransportKind, ClusteredAgent};
use crate::cluster::{ClusterEventHandler, DedicatedExecutor, NodeId, QuicDirectTransport};
use crate::topic_protection::TopicProtectionAuthProvider;
use mqtt5::QoS;
use mqtt5::broker::auth::{CompositeAuthProvider, ComprehensiveAuthProvider};
use mqtt5::broker::bridge::{BridgeConfig, BridgeDirection, BridgeManager, BridgeProtocol};
use mqtt5::broker::config::{
    ChangeOnlyDeliveryConfig, ClusterListenerConfig, QuicConfig as BrokerQuicConfig,
    StorageBackend, StorageConfig, WebSocketConfig,
};
use mqtt5::broker::{AclManager, BrokerConfig, MqttBroker, PasswordAuthProvider};
use mqtt5::time::Duration;
use mqtt5::transport::StreamStrategy;
use mqtt5::types::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

use crate::broker_defaults::{BROKER_MAX_CLIENTS, BROKER_MAX_PACKET_SIZE, SESSION_EXPIRY_SECS};

impl ClusteredAgent {
    #[must_use]
    pub(super) fn cluster_port(&self) -> u16 {
        self.bind_address.port() + self.cluster_port_offset
    }

    pub(super) fn create_bridge_configs(
        &self,
    ) -> Result<Vec<BridgeConfig>, std::net::AddrParseError> {
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

                if self.quic.enabled {
                    config.protocol = BridgeProtocol::Quic;
                    config.quic_stream_strategy = Some(StreamStrategy::DataPerTopic);
                    config.quic_flow_headers = Some(true);
                    config.quic_datagrams = Some(true);
                    config.fallback_tcp = true;
                    #[cfg(feature = "dev-insecure")]
                    if self.quic.insecure {
                        config.insecure = Some(true);
                    }
                }

                Ok(config)
            })
            .collect()
    }

    pub(super) fn prepare_bridges(
        &mut self,
    ) -> Result<(Vec<BridgeConfig>, bool), Box<dyn std::error::Error + Send + Sync>> {
        if self.quic.direct {
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

    pub(super) async fn configure_broker_with_event_handler(
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
        let event_handler = Arc::new(
            ClusterEventHandler::new(self.node_id, self.controller.clone())
                .with_ownership(Arc::clone(&self.ownership)),
        );
        self.configure_broker(event_handler, bridge_configs, use_external_bridge_manager)
            .await
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

    #[allow(clippy::type_complexity)]
    pub(super) async fn build_broker(
        &self,
        broker_config: BrokerConfig,
        needs_composite: bool,
        service_username: Option<&String>,
        service_password: Option<&String>,
        admin_users: &std::collections::HashSet<String>,
    ) -> Result<
        (MqttBroker, Option<Arc<ComprehensiveAuthProvider>>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let broker_config = self.apply_quic_config(broker_config);
        let broker_config = self.apply_ws_config(broker_config);
        let broker_config = self.apply_cluster_listener(broker_config)?;

        let mut broker = MqttBroker::with_config(broker_config).await?;

        let auth_providers = if self.password_file.is_some() || self.acl_file.is_some() {
            let pwd = if let Some(path) = &self.password_file {
                PasswordAuthProvider::from_file(path)
                    .await
                    .map_err(|e| format!("failed to load password file: {e}"))?
            } else {
                PasswordAuthProvider::new()
            };
            let acl = if let Some(path) = &self.acl_file {
                AclManager::from_file(path)
                    .await
                    .map_err(|e| format!("failed to load ACL file: {e}"))?
            } else {
                AclManager::allow_all()
            };
            Some(Arc::new(ComprehensiveAuthProvider::with_providers(
                pwd, acl,
            )))
        } else {
            None
        };

        if needs_composite
            && let (Some(svc_user), Some(svc_pass)) = (service_username, service_password)
        {
            let primary = broker.auth_provider();
            let fallback: Arc<dyn mqtt5::broker::auth::AuthProvider> =
                if let Some(ref comprehensive) = auth_providers {
                    comprehensive
                        .password_provider()
                        .add_user(svc_user.clone(), svc_pass)
                        .map_err(|e| format!("failed to create service account: {e}"))?;
                    comprehensive.clone()
                } else {
                    let fallback = PasswordAuthProvider::new();
                    fallback
                        .add_user(svc_user.clone(), svc_pass)
                        .map_err(|e| format!("failed to create service account: {e}"))?;
                    Arc::new(fallback)
                };
            let composite = CompositeAuthProvider::new(primary, fallback);
            broker = broker.with_auth_provider(Arc::new(composite));
            info!("composite auth provider configured (password file + service account)");
        } else if let Some(ref comprehensive) = auth_providers {
            broker = broker.with_auth_provider(comprehensive.clone());
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

        Ok((broker, auth_providers))
    }

    fn apply_quic_config(&self, broker_config: BrokerConfig) -> BrokerConfig {
        if self.quic.enabled {
            if let (Some(cert_file), Some(key_file)) = (&self.quic.cert_file, &self.quic.key_file) {
                let quic_config = BrokerQuicConfig::new(cert_file.clone(), key_file.clone())
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
        if self.quic.direct {
            return Ok(broker_config);
        }
        let cluster_addr: SocketAddr =
            format!("{}:{}", self.bind_address.ip(), self.cluster_port())
                .parse()
                .map_err(|e| format!("invalid cluster address: {e}"))?;

        let cluster_listener = if let (Some(cert_file), Some(key_file)) =
            (&self.quic.cert_file, &self.quic.key_file)
        {
            ClusterListenerConfig::quic(vec![cluster_addr], cert_file.clone(), key_file.clone())
        } else {
            ClusterListenerConfig::new(vec![cluster_addr])
        };
        info!(cluster_port = %cluster_addr, "cluster listener configured (skip_bridge_forwarding=true)");
        Ok(broker_config.with_cluster_listener(cluster_listener))
    }

    pub(super) async fn start_broker(
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

    pub(super) async fn connect_transport(
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

        #[cfg(feature = "mqtt-bridge")]
        if !self.quic.direct {
            return self
                .connect_mqtt_transport(
                    &transport,
                    &broker_addr,
                    service_username,
                    service_password,
                )
                .await;
        }

        self.connect_quic_transport(&transport, &broker_addr, service_username, service_password)
            .await
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

        let (cert_file, key_file) = match (&self.quic.cert_file, &self.quic.key_file) {
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

    #[cfg(feature = "mqtt-bridge")]
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

    pub(super) async fn initialize_event_handler(
        &self,
    ) -> Arc<tokio::sync::RwLock<std::collections::HashMap<String, std::time::Instant>>> {
        let event_handler = Arc::new(
            ClusterEventHandler::new(self.node_id, self.controller.clone())
                .with_ownership(Arc::clone(&self.ownership)),
        );
        let synced_retained_topics = event_handler.synced_retained_topics();

        {
            let mut ctrl = self.controller.write().await;
            ctrl.set_synced_retained_topics(synced_retained_topics.clone());
        }
        synced_retained_topics
    }

    pub(super) async fn take_local_publish_receiver(
        &self,
    ) -> Option<flume::Receiver<crate::cluster::LocalPublishRequest>> {
        if self.quic.direct {
            let ctrl = self.controller.read().await;
            ctrl.transport()
                .as_quic()
                .map(QuicDirectTransport::local_publish_rx)
        } else {
            None
        }
    }
}

pub(super) async fn subscribe_admin_topics(
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
