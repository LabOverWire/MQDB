use crate::cluster::Epoch;
use crate::cluster::client_location::{ClientLocationEntry, client_location_key};
use crate::cluster::db_handler::DbRequestHandler;
use crate::cluster::entity;
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::session::session_partition;
use crate::cluster::transport::{ClusterMessage, ClusterTransport};
use crate::cluster::{
    ForwardTarget, ForwardedPublish, LwtPublisher, NodeController, NodeId, PublishRouter,
    SubscriptionType, TopicSubscriptionBroadcast, WildcardBroadcast,
};
use bebytes::BeBytes;
use mqtt5::QoS;
use mqtt5::broker::events::{
    BrokerEventHandler, ClientConnectEvent, ClientDisconnectEvent, ClientPublishEvent,
    ClientSubscribeEvent, ClientUnsubscribeEvent, MessageDeliveredEvent, RetainedSetEvent,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, trace, warn};

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
    pub fn synced_retained_topics(&self) -> Arc<RwLock<HashMap<String, Instant>>> {
        Arc::clone(&self.synced_retained_topics)
    }

    async fn broadcast_topic_subscription(
        ctrl: &NodeController<T>,
        broadcast: TopicSubscriptionBroadcast,
    ) {
        let msg = ClusterMessage::TopicSubscriptionBroadcast(broadcast);
        let _ = ctrl.transport().broadcast(msg).await;
    }
}

impl<T: ClusterTransport + 'static> BrokerEventHandler for ClusterEventHandler<T> {
    fn on_client_connect<'a>(
        &'a self,
        event: ClientConnectEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if event.client_id.starts_with("mqdb-") {
                trace!("skipping internal client connect");
                return;
            }

            debug!(
                client_id = %event.client_id,
                clean_start = event.clean_start,
                has_will_topic = event.will_topic.is_some(),
                "client connected"
            );

            let mut ctrl = self.controller.write().await;
            let client_id = event.client_id.as_ref();

            if event.clean_start {
                let existing = ctrl.stores().sessions.get(client_id);
                if existing.is_some() {
                    debug!(client_id, "clean_start=true, clearing old session state");
                    clear_client_subscriptions(&mut ctrl, client_id).await;
                    let _ = ctrl.stores_mut().remove_session_replicated(client_id);
                }
            }

            let result = ctrl.stores_mut().create_session_replicated(client_id);
            if let Err(e) = result {
                warn!(client_id, error = %e, "failed to create session");
                return;
            }
            let (_session, create_write) = result.unwrap();

            let result = ctrl.stores_mut().update_session_replicated(client_id, |s| {
                s.set_clean_session(event.clean_start);
            });
            let clean_session_write = match result {
                Ok((_session, write)) => {
                    ctrl.write_or_forward(write.clone()).await;
                    Some(write)
                }
                Err(e) => {
                    warn!(client_id, error = ?e, "failed to set clean_session flag");
                    None
                }
            };

            let location_entry = ClientLocationEntry::create(client_id, self.node_id);
            let location_write = ReplicationWrite::new(
                session_partition(client_id),
                Operation::Insert,
                Epoch::new(0),
                0,
                entity::CLIENT_LOCATIONS.to_string(),
                client_location_key(client_id),
                location_entry.to_be_bytes(),
            );
            ctrl.write_or_forward(location_write).await;

            if let Some(ref topic) = event.will_topic {
                let will_qos = qos_to_u8(event.will_qos.unwrap_or(QoS::AtMostOnce));
                let will_retain = event.will_retain.unwrap_or(false);
                let will_payload = event
                    .will_payload
                    .as_ref()
                    .map(|b| b.to_vec())
                    .unwrap_or_default();

                let result = ctrl.stores_mut().update_session_replicated(client_id, |s| {
                    s.set_will(will_qos, will_retain, topic.as_ref(), &will_payload);
                });
                match result {
                    Ok((session, will_write)) => {
                        debug!(
                            client_id,
                            has_will = session.has_will,
                            "will stored in session"
                        );
                        ctrl.write_or_forward(will_write).await;
                    }
                    Err(e) => {
                        warn!(client_id, error = ?e, "failed to store will in session");
                        if clean_session_write.is_none() {
                            ctrl.write_or_forward(create_write).await;
                        }
                    }
                }
            } else if clean_session_write.is_none() {
                ctrl.write_or_forward(create_write).await;
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    fn on_client_disconnect<'a>(
        &'a self,
        event: ClientDisconnectEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let node_id = self.node_id;
        Box::pin(async move {
            if event.client_id.starts_with("mqdb-") {
                trace!("skipping internal client disconnect");
                return;
            }

            debug!(
                client_id = %event.client_id,
                reason = ?event.reason,
                unexpected = event.unexpected,
                "client disconnected"
            );

            let mut ctrl = self.controller.write().await;
            let client_id = event.client_id.as_ref();
            let timestamp = current_time_ms();

            let result = ctrl.stores_mut().update_session_replicated(client_id, |s| {
                s.set_connected(false, node_id, timestamp);
            });

            if let Ok((_session, write)) = result {
                ctrl.write_or_forward(write).await;
            }

            let location_entry = ClientLocationEntry::create(client_id, node_id);
            let location_delete = ReplicationWrite::new(
                session_partition(client_id),
                Operation::Delete,
                Epoch::new(0),
                0,
                entity::CLIENT_LOCATIONS.to_string(),
                client_location_key(client_id),
                location_entry.to_be_bytes(),
            );
            ctrl.write_or_forward(location_delete).await;

            if event.unexpected {
                let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
                let prepared = lwt_publisher.prepare_lwt(client_id);

                debug!(
                    client_id,
                    has_lwt = prepared.as_ref().is_ok_and(Option::is_some),
                    "checking LWT conditions"
                );

                if let Ok(Some(lwt)) = prepared {
                    debug!(client_id, topic = %lwt.topic, qos = lwt.qos, "routing LWT to subscribers");

                    let wildcards = ctrl.stores().wildcards.match_topic(&lwt.topic);
                    let router = PublishRouter::new(&ctrl.stores().topics);
                    let route = router.route_with_wildcards(&lwt.topic, &wildcards);

                    let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
                    for target in route.targets {
                        let connected_node = ctrl
                            .stores()
                            .client_locations
                            .get(&target.client_id)
                            .or_else(|| {
                                ctrl.stores()
                                    .sessions
                                    .get(&target.client_id)
                                    .filter(|s| s.connected == 1)
                                    .and_then(|s| NodeId::validated(s.connected_node))
                            });

                        if let Some(target_node) = connected_node
                            && target_node != node_id
                        {
                            remote_nodes
                                .entry(target_node)
                                .or_default()
                                .push(ForwardTarget::new(target.client_id, target.qos));
                        }
                    }

                    let transport = ctrl.transport().clone();
                    let session = ctrl.stores().sessions.get(client_id);
                    let is_clean_session = session.is_some_and(|s| s.is_clean_session());
                    drop(ctrl);

                    for (target_node, targets) in remote_nodes {
                        let fwd = ForwardedPublish::new(
                            node_id,
                            lwt.topic.clone(),
                            lwt.qos,
                            lwt.retain,
                            lwt.payload.clone(),
                            targets,
                        );
                        let fwd_msg = super::transport::ClusterMessage::ForwardedPublish(fwd);
                        if let Err(e) = transport.send(target_node, fwd_msg).await {
                            warn!(target = target_node.get(), error = %e, "failed to forward LWT");
                        } else {
                            debug!(target = target_node.get(), topic = %lwt.topic, "forwarded LWT to node");
                        }
                    }

                    let mut ctrl = self.controller.write().await;
                    let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
                    if let Err(e) = lwt_publisher.complete_lwt(client_id, lwt.token) {
                        warn!(client_id, error = %e, "failed to mark LWT as published");
                    } else {
                        debug!(client_id, "marked LWT as published");
                    }

                    if is_clean_session {
                        debug!(
                            client_id,
                            "clean_session disconnect - clearing subscriptions"
                        );
                        clear_client_subscriptions(&mut ctrl, client_id).await;
                        let _ = ctrl.stores_mut().remove_session_replicated(client_id);
                    }
                    return;
                }
            }

            let session = ctrl.stores().sessions.get(client_id);
            if let Some(session) = session {
                if session.is_clean_session() {
                    debug!(
                        client_id,
                        "clean_session disconnect - clearing subscriptions"
                    );
                    clear_client_subscriptions(&mut ctrl, client_id).await;
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
        })
    }

    #[allow(clippy::too_many_lines)]
    fn on_client_subscribe<'a>(
        &'a self,
        event: ClientSubscribeEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let synced_topics = Arc::clone(&self.synced_retained_topics);
        Box::pin(async move {
            if event.client_id.starts_with("mqdb-") {
                trace!("skipping internal client subscription");
                return;
            }

            debug!(
                client_id = %event.client_id,
                subscriptions = event.subscriptions.len(),
                "client subscribed"
            );

            let mut retained_to_deliver: Vec<super::retained_store::RetainedMessage> = Vec::new();
            let mut pending_queries: Vec<(
                String,
                tokio::sync::oneshot::Receiver<Vec<super::retained_store::RetainedMessage>>,
            )> = Vec::new();

            {
                let mut ctrl = self.controller.write().await;
                let client_id = event.client_id.as_ref();

                for sub in &event.subscriptions {
                    let topic = sub.topic_filter.as_ref();
                    if topic.starts_with("_mqdb/") || topic.starts_with("$SYS/") {
                        trace!(topic, "skipping internal topic subscription");
                        continue;
                    }
                    if !sub.result.is_success() {
                        continue;
                    }

                    let qos = qos_to_u8(sub.qos);
                    let is_wildcard = topic.contains('+') || topic.contains('#');
                    let is_response_topic = topic.starts_with("resp/") || topic.contains("/resp/");

                    if is_wildcard {
                        trace!(
                            topic,
                            "wildcard subscription - broker handles local retained"
                        );
                    } else if is_response_topic {
                        trace!(topic, "response topic - skipping retained query");
                    } else {
                        trace!(topic, "broker handles local retained delivery natively");
                        if let Some(rx) = ctrl.start_async_retained_query(topic).await {
                            debug!(topic, "started remote retained query");
                            pending_queries.push((topic.to_string(), rx));
                        }
                    }

                    let (_snapshot, write) = ctrl
                        .stores_mut()
                        .add_subscription_replicated(client_id, topic, qos);

                    ctrl.write_or_forward(write).await;

                    if is_wildcard {
                        let client_partition = crate::cluster::session_partition(client_id);
                        let result = ctrl.stores_mut().subscribe_wildcard_replicated(
                            topic,
                            client_id,
                            client_partition,
                            qos,
                            SubscriptionType::Mqtt,
                        );
                        if result.is_ok() {
                            let broadcast = WildcardBroadcast::subscribe(
                                topic,
                                client_id,
                                client_partition,
                                qos,
                                SubscriptionType::Mqtt as u8,
                            );
                            let msg = ClusterMessage::WildcardBroadcast(broadcast);
                            let _ = ctrl.transport().broadcast(msg).await;
                            debug!(
                                topic,
                                client_id, "broadcast wildcard subscription to cluster"
                            );
                        }
                    } else {
                        let client_partition = crate::cluster::session_partition(client_id);
                        let topic_partition = crate::cluster::topic_partition(topic);
                        debug!(
                            client_id,
                            topic,
                            ?topic_partition,
                            ?client_partition,
                            "adding topic subscription"
                        );
                        let _ = ctrl.stores_mut().topics.subscribe(
                            topic,
                            client_id,
                            client_partition,
                            qos,
                        );
                        if is_response_topic {
                            trace!(topic, client_id, "skipping broadcast for response topic");
                        } else {
                            let broadcast = TopicSubscriptionBroadcast::subscribe(
                                topic,
                                client_id,
                                client_partition,
                                qos,
                            );
                            Self::broadcast_topic_subscription(&ctrl, broadcast).await;
                            debug!(topic, client_id, "broadcast topic subscription to cluster");
                        }
                    }
                }
            }

            for (topic, rx) in pending_queries {
                match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
                    Ok(Ok(messages)) => {
                        debug!(
                            topic,
                            count = messages.len(),
                            "received remote retained messages"
                        );
                        retained_to_deliver.extend(messages);
                    }
                    Ok(Err(_)) => {
                        warn!(topic, "retained query channel closed");
                    }
                    Err(_) => {
                        warn!(topic, "retained query timed out");
                    }
                }
            }

            if !retained_to_deliver.is_empty() {
                let ctrl = self.controller.read().await;
                let transport = ctrl.transport().clone();
                drop(ctrl);

                let mut synced = synced_topics.write().await;
                for msg in retained_to_deliver {
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
        })
    }

    fn on_client_unsubscribe<'a>(
        &'a self,
        event: ClientUnsubscribeEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if event.client_id.starts_with("mqdb-") {
                trace!("skipping internal client unsubscribe");
                return;
            }

            debug!(
                client_id = %event.client_id,
                topics = event.topic_filters.len(),
                "client unsubscribed"
            );

            let mut ctrl = self.controller.write().await;
            let client_id = event.client_id.as_ref();

            for topic_filter in &event.topic_filters {
                let topic = topic_filter.as_ref();
                if topic.starts_with("_mqdb/") || topic.starts_with("$SYS/") {
                    trace!(topic, "skipping internal topic unsubscribe");
                    continue;
                }

                let result = ctrl
                    .stores_mut()
                    .remove_subscription_replicated(client_id, topic);

                if let Ok((_snapshot, write)) = result {
                    ctrl.write_or_forward(write).await;
                }

                let is_wildcard = topic.contains('+') || topic.contains('#');
                if is_wildcard {
                    let result = ctrl
                        .stores_mut()
                        .unsubscribe_wildcard_replicated(topic, client_id);
                    if result.is_ok() {
                        let broadcast = WildcardBroadcast::unsubscribe(topic, client_id);
                        let msg = ClusterMessage::WildcardBroadcast(broadcast);
                        let _ = ctrl.transport().broadcast(msg).await;
                        debug!(
                            topic,
                            client_id, "broadcast wildcard unsubscription to cluster"
                        );
                    }
                } else {
                    let _ = ctrl.stores_mut().topics.unsubscribe(topic, client_id);
                    let is_response_topic = topic.starts_with("resp/") || topic.contains("/resp/");
                    if is_response_topic {
                        trace!(
                            topic,
                            client_id, "skipping broadcast for response topic unsubscribe"
                        );
                    } else {
                        let broadcast = TopicSubscriptionBroadcast::unsubscribe(topic, client_id);
                        Self::broadcast_topic_subscription(&ctrl, broadcast).await;
                        debug!(
                            topic,
                            client_id, "broadcast topic unsubscription to cluster"
                        );
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    fn on_client_publish<'a>(
        &'a self,
        event: ClientPublishEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let node_id = self.node_id;
        Box::pin(async move {
            debug!(
                client_id = %event.client_id,
                topic = %event.topic,
                qos = ?event.qos,
                retain = event.retain,
                "client published"
            );

            if event.client_id.starts_with("mqdb-forward-") {
                trace!("skipping forwarded publish from internal client");
                return;
            }

            if event.topic.starts_with("_mqdb/") {
                trace!("skipping internal cluster topic");
                return;
            }

            if event.topic.starts_with("$DB/") {
                debug!(topic = %event.topic, "handling $DB/ request");
                let start = std::time::Instant::now();
                let mut ctrl = self.controller.write().await;
                #[allow(clippy::cast_possible_truncation)]
                let t_lock = start.elapsed().as_micros() as u64;
                if let Some(response) = self
                    .db_handler
                    .handle_publish(
                        &mut ctrl,
                        event.topic.as_ref(),
                        &event.payload,
                        event.response_topic.as_deref(),
                        event.correlation_data.as_deref(),
                    )
                    .await
                {
                    #[allow(clippy::cast_possible_truncation)]
                    let t_handle = start.elapsed().as_micros() as u64;
                    ctrl.transport()
                        .queue_local_publish(response.topic, response.payload, qos_to_u8(event.qos))
                        .await;
                    #[allow(clippy::cast_possible_truncation)]
                    let t_queue = start.elapsed().as_micros() as u64;
                    tracing::info!(
                        node = node_id.get(),
                        t_lock,
                        t_handle,
                        t_queue,
                        "db_event_timing"
                    );
                }
                return;
            }

            let (ctrl, try_read_success) = match self.controller.try_read() {
                Ok(guard) => (guard, true),
                Err(_) => (self.controller.read().await, false),
            };
            tracing::debug!(try_read_success, "publish_routing_lock");
            let topic = event.topic.as_ref();

            let wildcards = ctrl.stores().wildcards.match_topic(topic);
            let router = PublishRouter::new(&ctrl.stores().topics);
            let targets = router.route_targets(topic, &wildcards);

            debug!(
                topic,
                target_count = targets.len(),
                wildcard_matches = wildcards.len(),
                "routing publish"
            );

            let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
            for target in targets {
                let connected_node = ctrl
                    .stores()
                    .client_locations
                    .get(&target.client_id)
                    .or_else(|| {
                        ctrl.stores()
                            .sessions
                            .get(&target.client_id)
                            .filter(|s| s.connected == 1)
                            .and_then(|s| NodeId::validated(s.connected_node))
                    });

                let is_local = connected_node == Some(node_id);

                debug!(
                    client_id = %target.client_id,
                    client_partition = ?target.client_partition,
                    ?connected_node,
                    is_local,
                    "routing target"
                );

                if let Some(target_node) = connected_node
                    && target_node != node_id
                {
                    remote_nodes
                        .entry(target_node)
                        .or_default()
                        .push(ForwardTarget::new(target.client_id, target.qos));
                }
            }

            if remote_nodes.is_empty() {
                drop(ctrl);
            } else {
                let transport = ctrl.transport().clone();
                let fwd_topic = topic.to_string();
                let fwd_qos = qos_to_u8(event.qos);
                let fwd_payload = event.payload.to_vec();
                let fwd_retain = event.retain;

                drop(ctrl);

                for (target_node, targets) in remote_nodes {
                    let fwd = ForwardedPublish::new(
                        node_id,
                        fwd_topic.clone(),
                        fwd_qos,
                        fwd_retain,
                        fwd_payload.clone(),
                        targets,
                    );
                    let fwd_msg = super::transport::ClusterMessage::ForwardedPublish(fwd);
                    if let Err(e) = transport.send(target_node, fwd_msg).await {
                        warn!(target = target_node.get(), error = %e, "failed to forward publish");
                    } else {
                        debug!(target = target_node.get(), topic = %fwd_topic, "forwarded publish to node");
                    }
                }
            }

            if event.qos == QoS::ExactlyOnce
                && let Some(packet_id) = event.packet_id
            {
                let mut ctrl = self.controller.write().await;
                let client_id = event.client_id.as_ref();
                let topic = event.topic.as_ref();
                let payload = event.payload.to_vec();
                let timestamp = current_time_ms();

                let result = ctrl.stores_mut().start_qos2_inbound_replicated(
                    client_id, packet_id, topic, &payload, timestamp,
                );

                if let Ok((_state, write)) = result {
                    ctrl.write_or_forward(write).await;
                }
            }
        })
    }

    fn on_retained_set<'a>(
        &'a self,
        event: RetainedSetEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let synced_topics = Arc::clone(&self.synced_retained_topics);
        let node_id = self.node_id;
        Box::pin(async move {
            let total = RETAINED_SET_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
            if total.is_multiple_of(10000) {
                let skipped = RETAINED_SET_SKIPPED.load(AtomicOrdering::Relaxed);
                let processed = RETAINED_SET_PROCESSED.load(AtomicOrdering::Relaxed);
                tracing::warn!(
                    node = node_id.get(),
                    total,
                    skipped,
                    processed,
                    topic = %event.topic,
                    "RETAINED_SET_COUNTER milestone"
                );
            }

            if event.topic.starts_with("$SYS/") || event.topic.starts_with("_mqdb/") {
                trace!("skipping internal retained message");
                return;
            }

            let topic_str = event.topic.as_ref().to_string();
            {
                let synced = synced_topics.read().await;
                if let Some(&insert_time) = synced.get(&topic_str)
                    && insert_time.elapsed() < RETAINED_SYNC_TTL
                {
                    RETAINED_SET_SKIPPED.fetch_add(1, AtomicOrdering::Relaxed);
                    trace!(topic = %topic_str, "skipping synced retained message");
                    return;
                }
            }

            RETAINED_SET_PROCESSED.fetch_add(1, AtomicOrdering::Relaxed);
            debug!(
                topic = %event.topic,
                cleared = event.cleared,
                qos = ?event.qos,
                "retained message set"
            );

            let mut ctrl = self.controller.write().await;
            let topic = event.topic.as_ref();
            let payload = event.payload.to_vec();
            let timestamp = current_time_ms();
            let qos = qos_to_u8(event.qos);

            let (_msg, write) = ctrl
                .stores_mut()
                .set_retained_replicated(topic, qos, &payload, timestamp);

            ctrl.write_or_forward(write).await;
        })
    }

    fn on_message_delivered<'a>(
        &'a self,
        event: MessageDeliveredEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if event.client_id.starts_with("mqdb-") {
                trace!("skipping internal client message delivered");
                return;
            }

            debug!(
                client_id = %event.client_id,
                packet_id = event.packet_id,
                qos = ?event.qos,
                "message delivered"
            );

            let mut ctrl = self.controller.write().await;
            let client_id = event.client_id.as_ref();

            match event.qos {
                QoS::AtLeastOnce => {
                    let result = ctrl
                        .stores_mut()
                        .acknowledge_inflight_replicated(client_id, event.packet_id);

                    if let Ok((_msg, write)) = result {
                        ctrl.write_or_forward(write).await;
                    }
                }
                QoS::ExactlyOnce => {
                    let result = ctrl
                        .stores_mut()
                        .complete_qos2_replicated(client_id, event.packet_id);

                    if let Ok((_state, write)) = result {
                        ctrl.write_or_forward(write).await;
                    }
                }
                QoS::AtMostOnce => {}
            }
        })
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

fn qos_to_u8(qos: QoS) -> u8 {
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
