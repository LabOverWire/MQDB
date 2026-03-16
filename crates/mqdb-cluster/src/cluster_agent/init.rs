// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    BATCH_QUEUE_CAPACITY, ClusterConfig, ClusterInitError, ClusterTransportKind, ClusteredAgent,
    MAIN_QUEUE_CAPACITY, MessageProcessorChannels, RAFT_CHANNEL_CAPACITY,
};
#[cfg(feature = "mqtt-bridge")]
#[allow(deprecated)]
use crate::cluster::MqttTransport;
use crate::cluster::raft::{RaftConfig, RaftCoordinator};
use crate::cluster::{
    DedicatedExecutor, InboundMessage, MessageProcessor, NodeController, NodeId, PartitionMap,
    QuicDirectTransport, RaftStatus, TransportConfig,
};
use mqdb_core::config::DurabilityMode;
use mqdb_core::storage::{EncryptedBackend, FjallBackend};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::info;

use super::PeerConfig;

impl ClusteredAgent {
    fn open_stores_backend(
        config: &ClusterConfig,
    ) -> Result<Option<Arc<dyn mqdb_core::storage::StorageBackend>>, ClusterInitError> {
        if config.persist_stores {
            let stores_path = config.db_path.join("stores");
            let backend: Arc<dyn mqdb_core::storage::StorageBackend> = Arc::new(
                FjallBackend::open(&stores_path, config.stores_durability).map_err(|e| {
                    ClusterInitError::StorageOpen {
                        path: stores_path.clone(),
                        source: e,
                    }
                })?,
            );
            if let Some(ref passphrase) = config.passphrase {
                let encrypted = EncryptedBackend::open(backend, passphrase).map_err(|e| {
                    ClusterInitError::StorageOpen {
                        path: stores_path,
                        source: e,
                    }
                })?;
                Ok(Some(Arc::new(encrypted)))
            } else {
                Ok(Some(backend))
            }
        } else {
            Ok(None)
        }
    }

    fn build_transport(
        node_id: NodeId,
        config: &ClusterConfig,
    ) -> (ClusterTransportKind, flume::Receiver<InboundMessage>) {
        #[cfg(feature = "mqtt-bridge")]
        if !config.quic.direct {
            #[allow(deprecated)]
            {
                let mqtt_transport = MqttTransport::new(node_id);
                let inbox_rx = mqtt_transport.inbox_rx();
                return (ClusterTransportKind::Mqtt(mqtt_transport), inbox_rx);
            }
        }

        #[cfg(not(feature = "mqtt-bridge"))]
        let _ = config.quic.direct;

        let quic_transport = QuicDirectTransport::new(node_id);
        #[cfg(feature = "dev-insecure")]
        quic_transport.set_insecure(config.quic.insecure);
        if let Some(ca_path) = &config.quic.ca_file {
            quic_transport.set_ca_file(ca_path.clone());
        }
        if let (Some(cert), Some(key)) = (&config.quic.cert_file, &config.quic.key_file) {
            quic_transport.set_cert_key_files(cert.clone(), key.clone());
        }
        let inbox_rx = quic_transport.inbox_rx();
        (ClusterTransportKind::Quic(quic_transport), inbox_rx)
    }

    fn open_raft(
        node_id: NodeId,
        db_path: &std::path::Path,
        transport: ClusterTransportKind,
    ) -> Result<RaftCoordinator<ClusterTransportKind>, ClusterInitError> {
        let raft_path = db_path.join("raft");
        let raft_backend = Arc::new(
            FjallBackend::open(&raft_path, DurabilityMode::Immediate).map_err(|e| {
                ClusterInitError::StorageOpen {
                    path: raft_path.clone(),
                    source: e,
                }
            })?,
        );
        RaftCoordinator::new_with_storage(node_id, transport, RaftConfig::default(), raft_backend)
            .map_err(ClusterInitError::RaftInit)
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
            Box::pin(processor.run(channels.tx_batch)).await;
        });
        info!("started message processor on dedicated executor");
        executor
    }

    /// # Errors
    /// If node initialization fails: invalid node ID, storage backend, or Raft setup.
    pub fn new(config: ClusterConfig) -> Result<Self, ClusterInitError> {
        let node_id = NodeId::validated(config.node_id)
            .ok_or(ClusterInitError::InvalidNodeId(config.node_id))?;
        let stores_backend = Self::open_stores_backend(&config)?;

        let (tx_raft_messages, rx_raft_messages) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let (tx_raft_events, rx_raft_events) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let tx_raft_events_clone = tx_raft_events.clone();
        let (tx_raft_admin, rx_raft_admin) = flume::bounded(RAFT_CHANNEL_CAPACITY);
        let (tx_partition_map, rx_partition_map) = watch::channel(PartitionMap::default());
        let (tx_raft_status, rx_raft_status) = watch::channel(RaftStatus::default());

        let (transport, transport_inbox_rx) = Self::build_transport(node_id, &config);
        let transport_config = TransportConfig::default();
        let ownership_arc = Arc::new(config.ownership.clone());
        let vault_key_store = Arc::new(mqdb_core::vault_keys::VaultKeyStore::new());
        let mut controller = NodeController::new_with_storage(
            node_id,
            transport.clone(),
            transport_config,
            stores_backend,
            tx_raft_messages.clone(),
            tx_raft_events.clone(),
        );
        controller.set_ownership(Arc::clone(&ownership_arc));
        controller.set_vault_key_store(Arc::clone(&vault_key_store));

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
                    tracing::warn!(error = ?e, "failed to recover stores, starting fresh");
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

        let pending_constraints = Arc::clone(controller.pending_constraints());

        Ok(Self {
            node_id,
            node_name: config.node_name,
            pending_constraints,
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
            quic: config.quic,
            bridge_out_only: config.bridge_out_only,
            cluster_port_offset: config.cluster_port_offset,
            bridge_executor: None,
            processor_executor: Some(processor_executor),
            tx_tick: Some(tx_tick),
            rx_main_queue: Some(rx_main_queue),
            rx_batch: Some(rx_batch),
            ws_bind_address: config.ws_bind_address,
            http_config: config.http_config,
            ownership: ownership_arc,
            scope_config: Arc::new(config.scope_config),
            auth_providers: None,
            vault_key_store,
        })
    }

    pub(super) async fn setup_and_spawn_raft(
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

        let raft_task = crate::cluster::RaftTask {
            raft,
            rx_messages: self
                .rx_raft_messages
                .take()
                .ok_or("run() called twice: rx_raft_messages already taken")?,
            rx_events: self
                .rx_raft_events
                .take()
                .ok_or("run() called twice: rx_raft_events already taken")?,
            rx_admin: self
                .rx_raft_admin
                .take()
                .ok_or("run() called twice: rx_raft_admin already taken")?,
            tx_partition_map: self
                .tx_partition_map
                .take()
                .ok_or("run() called twice: tx_partition_map already taken")?,
            tx_status: self
                .tx_raft_status
                .take()
                .ok_or("run() called twice: tx_raft_status already taken")?,
            shutdown_rx: self.shutdown_tx.subscribe(),
            all_nodes,
            partitions_initialized: false,
        };
        tokio::spawn(async move {
            Box::pin(raft_task.run()).await;
        });
        info!("spawned Raft task");

        Ok(())
    }

    pub(super) fn spawn_http_task(
        &mut self,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let mut http_config = self.http_config.take()?;
        http_config.vault_key_store = Some(Arc::clone(&self.vault_key_store));
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
            tokio::time::sleep(mqtt5::time::Duration::from_millis(300)).await;

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
            let server = mqdb_agent::http::HttpServer::new(
                http_config,
                std::sync::Arc::new(http_mqtt_client),
                http_shutdown_rx,
            );
            if let Err(e) = server.run().await {
                tracing::error!("HTTP server error: {}", e);
            }
        }))
    }
}
