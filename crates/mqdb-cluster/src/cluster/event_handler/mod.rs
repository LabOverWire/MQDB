// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod broker_events;
mod routing;

use super::db_handler::DbRequestHandler;
use super::node_controller::NodeController;
use super::share_routing::{
    BrokerView, LocalShare, LocalShareMembers, ShareKey, assign_share_groups, live_remote_node,
    served_locally, share_members,
};
use super::transport::{ClusterMessage, ClusterTransport};
use super::{
    ForwardTarget, LwtPublisher, NodeId, RetainedEchoes, TopicSubscriptionBroadcast,
    WildcardBroadcast,
};
use mqdb_vault::VaultKeyStore;
use mqtt5::QoS;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use tokio::sync::RwLock;
use tracing::{debug, warn};

const MAX_SHARE_ROTATIONS: usize = 4096;

static RETAINED_SET_TOTAL: AtomicU64 = AtomicU64::new(0);
static RETAINED_SET_SKIPPED: AtomicU64 = AtomicU64::new(0);
static RETAINED_SET_PROCESSED: AtomicU64 = AtomicU64::new(0);

pub struct ClusterEventHandler<T: ClusterTransport + 'static> {
    node_id: NodeId,
    controller: Arc<RwLock<NodeController<T>>>,
    retained_echoes: Arc<RetainedEchoes>,
    db_handler: DbRequestHandler,
    vault_key_store: Arc<VaultKeyStore>,
    presence: bool,
    live_connections: mqdb_agent::presence::LiveConnections,
    local_shares: Arc<Mutex<LocalShareMembers>>,
    share_rotation: Arc<Mutex<HashMap<ShareKey, usize>>>,
    broker_view: Arc<OnceLock<BrokerView>>,
}

impl<T: ClusterTransport + 'static> ClusterEventHandler<T> {
    pub fn new(node_id: NodeId, controller: Arc<RwLock<NodeController<T>>>) -> Self {
        Self {
            node_id,
            controller,
            retained_echoes: Arc::new(RetainedEchoes::new()),
            db_handler: DbRequestHandler::new(node_id),
            vault_key_store: Arc::new(VaultKeyStore::new()),
            presence: false,
            live_connections: mqdb_agent::presence::LiveConnections::new(),
            local_shares: Arc::new(Mutex::new(LocalShareMembers::default())),
            share_rotation: Arc::new(Mutex::new(HashMap::new())),
            broker_view: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub fn with_presence(mut self, enabled: bool) -> Self {
        self.presence = enabled;
        self
    }

    #[must_use]
    pub fn with_ownership(
        mut self,
        ownership: std::sync::Arc<mqdb_core::types::OwnershipConfig>,
    ) -> Self {
        self.db_handler = self.db_handler.with_ownership(ownership);
        self
    }

    #[must_use]
    pub fn with_vault_key_store(mut self, store: Arc<VaultKeyStore>) -> Self {
        self.db_handler = self.db_handler.with_vault_key_store(Arc::clone(&store));
        self.vault_key_store = store;
        self
    }

    #[must_use]
    pub fn with_retained_echoes(mut self, echoes: Arc<RetainedEchoes>) -> Self {
        self.retained_echoes = echoes;
        self
    }

    #[must_use]
    pub(crate) fn with_broker_view(mut self, broker_view: Arc<OnceLock<BrokerView>>) -> Self {
        self.broker_view = broker_view;
        self
    }

    fn update_local_shares(&self, update: impl FnOnce(&mut LocalShareMembers)) {
        update(
            &mut self
                .local_shares
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
    }

    async fn held_local_shares(&self, topic: &str) -> Vec<LocalShare> {
        let candidates = self
            .local_shares
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .candidates(topic);
        if candidates.is_empty() {
            return Vec::new();
        }
        let Some(view) = self.broker_view.get() else {
            return candidates
                .into_iter()
                .map(|candidate| LocalShare {
                    client_id: candidate.client_id,
                    key: candidate.key,
                    online: true,
                })
                .collect();
        };
        let checked = view.held_shares(candidates).await;
        if !checked.stale.is_empty() {
            self.update_local_shares(|shares| shares.forget(&checked.stale));
        }
        checked.held
    }

    async fn reconcile_local_shares(&self, client_id: &str) {
        if let Some(view) = self.broker_view.get() {
            let stored = view.stored_shares(client_id).await;
            self.update_local_shares(|shares| shares.reconcile(client_id, &stored));
        }
    }

    fn share_assignment(
        &self,
        ctrl: &NodeController<T>,
        topic: &str,
        held: &[LocalShare],
    ) -> HashMap<NodeId, Vec<String>> {
        let members = share_members(&ctrl.stores().topics, &ctrl.stores().wildcards, topic);
        if members.is_empty() {
            return HashMap::new();
        }
        let locate = |client_id: &str| ctrl.connected_node(client_id);
        let alive: HashSet<NodeId> = ctrl.alive_nodes().into_iter().collect();
        let mut rotations = self
            .share_rotation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if rotations.len() > MAX_SHARE_ROTATIONS {
            rotations.clear();
        }
        assign_share_groups(
            &members,
            &served_locally(held, locate, self.node_id),
            live_remote_node(locate, self.node_id, alive),
            |key| {
                let next = rotations.entry(key.clone()).or_default();
                let current = *next;
                *next = next.wrapping_add(1);
                current
            },
        )
    }
}

pub(super) struct LwtForwardingData<T: ClusterTransport> {
    pub transport: T,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub payload: Vec<u8>,
    pub token: [u8; 16],
    pub remote_nodes: HashMap<NodeId, Vec<ForwardTarget>>,
    pub share_nodes: HashMap<NodeId, Vec<String>>,
    pub is_clean_session: bool,
}

pub(super) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

pub(super) fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

trait SubAckReasonCodeExt {
    fn is_success(&self) -> bool;
}

impl SubAckReasonCodeExt for mqtt5::broker::events::SubAckReasonCode {
    fn is_success(&self) -> bool {
        matches!(
            self,
            mqtt5::broker::events::SubAckReasonCode::GrantedQoS0
                | mqtt5::broker::events::SubAckReasonCode::GrantedQoS1
                | mqtt5::broker::events::SubAckReasonCode::GrantedQoS2
        )
    }
}

async fn clear_client_subscriptions<T: ClusterTransport + 'static>(
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
                let is_response_topic = crate::cluster::is_response_topic(topic);
                if !is_response_topic {
                    let broadcast = TopicSubscriptionBroadcast::unsubscribe(topic, client_id);
                    ClusterEventHandler::<T>::broadcast_topic_subscription(ctrl, broadcast).await;
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

async fn handle_clean_session_cleanup<T: ClusterTransport + 'static>(
    ctrl: &mut NodeController<T>,
    client_id: &str,
) {
    let session = ctrl.stores().sessions.get(client_id);
    if let Some(session) = session {
        if session.is_clean_session() {
            debug!(
                client_id,
                "clean_session disconnect - clearing subscriptions"
            );
            clear_client_subscriptions(ctrl, client_id).await;
            let _ = ctrl.stores_mut().remove_session_replicated(client_id);
        } else {
            debug!(
                client_id,
                clean_session = session.clean_session,
                "session is NOT clean_session, skipping subscription cleanup"
            );
        }
    } else {
        debug!(client_id, "session not found for disconnect");
    }
}

async fn collect_pending_retained(
    pending_queries: Vec<(
        String,
        tokio::sync::oneshot::Receiver<Vec<super::retained_store::RetainedMessage>>,
    )>,
) -> Vec<super::retained_store::RetainedMessage> {
    let mut retained = Vec::new();
    for (topic, rx) in pending_queries {
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
            Ok(Ok(messages)) => {
                debug!(
                    topic,
                    count = messages.len(),
                    "received remote retained messages"
                );
                retained.extend(messages);
            }
            Ok(Err(_)) => {
                warn!(topic, "retained query channel closed");
            }
            Err(_) => {
                warn!(topic, "retained query timed out");
            }
        }
    }
    retained
}

async fn deliver_retained_messages<T: ClusterTransport>(
    transport: &T,
    retained: Vec<super::retained_store::RetainedMessage>,
) {
    for msg in retained {
        let topic = msg.topic_str().to_string();
        debug!(
            topic,
            qos = msg.qos,
            payload_len = msg.payload.len(),
            "delivering retained message to subscriber"
        );
        transport
            .queue_local_publish_retained(topic, msg.payload, msg.qos)
            .await;
    }
}
