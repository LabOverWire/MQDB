mod broker_events;
mod routing;

use super::db_handler::DbRequestHandler;
use super::node_controller::NodeController;
use super::transport::{ClusterMessage, ClusterTransport};
use super::{ForwardTarget, LwtPublisher, NodeId, TopicSubscriptionBroadcast, WildcardBroadcast};
use mqtt5::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

static RETAINED_SET_TOTAL: AtomicU64 = AtomicU64::new(0);
static RETAINED_SET_SKIPPED: AtomicU64 = AtomicU64::new(0);
static RETAINED_SET_PROCESSED: AtomicU64 = AtomicU64::new(0);

const RETAINED_SYNC_TTL: Duration = Duration::from_secs(5);

pub struct ClusterEventHandler<T: ClusterTransport + 'static> {
    node_id: NodeId,
    controller: Arc<RwLock<NodeController<T>>>,
    synced_retained_topics: Arc<RwLock<HashMap<String, Instant>>>,
    db_handler: DbRequestHandler,
}

impl<T: ClusterTransport + 'static> ClusterEventHandler<T> {
    pub fn new(node_id: NodeId, controller: Arc<RwLock<NodeController<T>>>) -> Self {
        Self {
            node_id,
            controller,
            synced_retained_topics: Arc::new(RwLock::new(HashMap::new())),
            db_handler: DbRequestHandler::new(node_id),
        }
    }

    #[must_use]
    pub fn with_ownership(
        mut self,
        ownership: std::sync::Arc<crate::types::OwnershipConfig>,
    ) -> Self {
        self.db_handler = self.db_handler.with_ownership(ownership);
        self
    }

    #[must_use]
    pub fn synced_retained_topics(&self) -> Arc<RwLock<HashMap<String, Instant>>> {
        Arc::clone(&self.synced_retained_topics)
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
                let is_response_topic = topic.starts_with("resp/") || topic.contains("/resp/");
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
    synced_topics: &RwLock<HashMap<String, Instant>>,
    retained: Vec<super::retained_store::RetainedMessage>,
) {
    let mut synced = synced_topics.write().await;
    for msg in retained {
        let topic = msg.topic_str().to_string();
        synced.insert(topic.clone(), Instant::now());
        debug!(
            topic,
            qos = msg.qos,
            payload_len = msg.payload.len(),
            "delivering retained message to subscriber"
        );
        transport
            .queue_local_publish_retained(topic, msg.payload.clone(), msg.qos)
            .await;
    }
}
