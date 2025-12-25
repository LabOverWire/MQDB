use crate::cluster::{
    ForwardTarget, ForwardedPublish, MqttTransport, NodeController, NodeId, PublishRouter,
    SubscriptionType,
};
use mqtt5::broker::events::{
    BrokerEventHandler, ClientConnectEvent, ClientDisconnectEvent, ClientPublishEvent,
    ClientSubscribeEvent, ClientUnsubscribeEvent, MessageDeliveredEvent, RetainedSetEvent,
};
use mqtt5::QoS;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace, warn};

pub struct ClusterEventHandler {
    node_id: NodeId,
    controller: Arc<RwLock<NodeController<MqttTransport>>>,
}

impl ClusterEventHandler {
    pub fn new(
        node_id: NodeId,
        controller: Arc<RwLock<NodeController<MqttTransport>>>,
    ) -> Self {
        Self { node_id, controller }
    }
}

impl BrokerEventHandler for ClusterEventHandler {
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
                "client connected"
            );

            let mut ctrl = self.controller.write().await;
            let client_id = event.client_id.as_ref();

            let result = ctrl.stores_mut().create_session_replicated(client_id);
            if let Err(e) = result {
                warn!(client_id, error = %e, "failed to create session");
                return;
            }
            let (_session, write) = result.unwrap();

            if let Some(ref topic) = event.will_topic {
                let will_qos = qos_to_u8(event.will_qos.unwrap_or(QoS::AtMostOnce));
                let will_retain = event.will_retain.unwrap_or(false);
                let will_payload = event
                    .will_payload
                    .as_ref()
                    .map(|b| b.to_vec())
                    .unwrap_or_default();

                let _ = ctrl.stores_mut().sessions.update(client_id, |s| {
                    s.set_will(will_qos, will_retain, topic.as_ref(), &will_payload);
                });
            }

            ctrl.write_or_forward(write);
        })
    }

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
                ctrl.write_or_forward(write);
            }

            if event.unexpected
                && let Some(session) = ctrl.stores().sessions.get(client_id)
                && session.needs_lwt_publish()
            {
                debug!(client_id, "triggering LWT publication");
            }
        })
    }

    fn on_client_subscribe<'a>(
        &'a self,
        event: ClientSubscribeEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
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

                let (_snapshot, write) = ctrl
                    .stores_mut()
                    .add_subscription_replicated(client_id, topic, qos);

                ctrl.write_or_forward(write);

                let is_wildcard = topic.contains('+') || topic.contains('#');
                if is_wildcard {
                    let client_partition = crate::cluster::session_partition(client_id);
                    let result = ctrl.stores_mut().subscribe_wildcard_replicated(
                        topic,
                        client_id,
                        client_partition,
                        qos,
                        SubscriptionType::Mqtt,
                    );
                    if let Ok((_entry, write)) = result {
                        ctrl.write_or_forward(write);
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
                    let (_entry, write) = ctrl.stores_mut().subscribe_topic_replicated(
                        topic,
                        client_id,
                        client_partition,
                        qos,
                    );
                    ctrl.write_or_forward(write);
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
                    ctrl.write_or_forward(write);
                }

                let is_wildcard = topic.contains('+') || topic.contains('#');
                if is_wildcard {
                    let client_partition = crate::cluster::session_partition(client_id);
                    let result = ctrl
                        .stores_mut()
                        .unsubscribe_wildcard_replicated(topic, client_id, client_partition);
                    if let Ok(write) = result {
                        ctrl.write_or_forward(write);
                    }
                } else {
                    let result = ctrl
                        .stores_mut()
                        .unsubscribe_topic_replicated(topic, client_id);
                    if let Ok((_entry, write)) = result {
                        ctrl.write_or_forward(write);
                    }
                }
            }
        })
    }

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

            let ctrl = self.controller.read().await;
            let topic = event.topic.as_ref();

            let wildcards = ctrl.stores().wildcards.match_topic(topic);
            let router = PublishRouter::new(&ctrl.stores().topics);
            let route = router.route_with_wildcards(topic, &wildcards);

            debug!(
                topic,
                target_count = route.targets.len(),
                wildcard_matches = wildcards.len(),
                "routing publish"
            );

            let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
            for target in route.targets {
                let session = ctrl.stores().sessions.get(&target.client_id);
                let connected_node = session
                    .as_ref()
                    .filter(|s| s.connected == 1)
                    .and_then(|s| NodeId::validated(s.connected_node));

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
                    if let Err(e) = transport.send_async(target_node, fwd_msg).await {
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
                    ctrl.write_or_forward(write);
                }
            }
        })
    }

    fn on_retained_set<'a>(
        &'a self,
        event: RetainedSetEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if event.topic.starts_with("$SYS/") || event.topic.starts_with("_mqdb/") {
                trace!("skipping internal retained message");
                return;
            }

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

            ctrl.write_or_forward(write);
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
                        ctrl.write_or_forward(write);
                    }
                }
                QoS::ExactlyOnce => {
                    let result = ctrl
                        .stores_mut()
                        .complete_qos2_replicated(client_id, event.packet_id);

                    if let Ok((_state, write)) = result {
                        ctrl.write_or_forward(write);
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
