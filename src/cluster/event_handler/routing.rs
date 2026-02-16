// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::node_controller::NodeController;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{
    ForwardTarget, ForwardedPublish, LwtPublisher, NodeId, PublishRouter, SubscriptionType,
    TopicSubscriptionBroadcast, WildcardBroadcast,
};
use super::{ClusterEventHandler, LwtForwardingData, qos_to_u8};
use mqtt5::broker::events::ClientPublishEvent;
use std::collections::HashMap;
use tracing::{debug, trace, warn};

impl<T: ClusterTransport + 'static> ClusterEventHandler<T> {
    pub(super) async fn broadcast_topic_subscription(
        ctrl: &NodeController<T>,
        broadcast: TopicSubscriptionBroadcast,
    ) {
        let msg = ClusterMessage::TopicSubscriptionBroadcast(broadcast);
        let _ = ctrl.transport().broadcast(msg).await;
    }

    pub(super) fn resolve_connected_node(
        ctrl: &NodeController<T>,
        client_id: &str,
    ) -> Option<NodeId> {
        ctrl.stores().client_locations.get(client_id).or_else(|| {
            ctrl.stores()
                .sessions
                .get(client_id)
                .filter(|s| s.connected == 1)
                .and_then(|s| NodeId::validated(s.connected_node))
        })
    }

    pub(super) fn collect_remote_targets(
        ctrl: &NodeController<T>,
        targets: impl IntoIterator<Item = ForwardTarget>,
        local_node: NodeId,
    ) -> HashMap<NodeId, Vec<ForwardTarget>> {
        let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
        for target in targets {
            let connected_node = Self::resolve_connected_node(ctrl, &target.client_id);
            if let Some(target_node) = connected_node
                && target_node != local_node
            {
                remote_nodes.entry(target_node).or_default().push(target);
            }
        }
        remote_nodes
    }

    pub(super) async fn forward_publish_to_remotes(
        transport: &T,
        origin: NodeId,
        topic: &str,
        qos: u8,
        retain: bool,
        payload: &[u8],
        remote_nodes: HashMap<NodeId, Vec<ForwardTarget>>,
    ) {
        for (target_node, targets) in remote_nodes {
            let fwd = ForwardedPublish::new(
                origin,
                topic.to_string(),
                qos,
                retain,
                payload.to_vec(),
                targets,
            );
            let fwd_msg = ClusterMessage::ForwardedPublish(fwd);
            if let Err(e) = transport.send(target_node, fwd_msg).await {
                warn!(target = target_node.get(), error = %e, "failed to forward publish");
            } else {
                debug!(
                    target = target_node.get(),
                    topic, "forwarded publish to node"
                );
            }
        }
    }

    pub(super) async fn handle_wildcard_subscribe(
        ctrl: &mut NodeController<T>,
        topic: &str,
        client_id: &str,
        qos: u8,
    ) {
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
    }

    pub(super) async fn handle_topic_subscribe(
        ctrl: &mut NodeController<T>,
        topic: &str,
        client_id: &str,
        qos: u8,
    ) {
        let client_partition = crate::cluster::session_partition(client_id);
        let topic_partition = crate::cluster::topic_partition(topic);
        debug!(
            client_id,
            topic,
            ?topic_partition,
            ?client_partition,
            "adding topic subscription"
        );
        let _ = ctrl
            .stores_mut()
            .topics
            .subscribe(topic, client_id, client_partition, qos);
        let is_response_topic = topic.starts_with("resp/") || topic.contains("/resp/");
        if is_response_topic {
            trace!(topic, client_id, "skipping broadcast for response topic");
        } else {
            let broadcast =
                TopicSubscriptionBroadcast::subscribe(topic, client_id, client_partition, qos);
            Self::broadcast_topic_subscription(ctrl, broadcast).await;
            debug!(topic, client_id, "broadcast topic subscription to cluster");
        }
    }

    pub(super) async fn handle_db_publish(&self, node_id: NodeId, event: &ClientPublishEvent) {
        use super::super::db_handler::DbPublishResult;
        use super::super::node_controller::unique::await_unique_reserves;

        debug!(topic = %event.topic, "handling $DB/ request");
        let start = std::time::Instant::now();
        let mut ctrl = self.controller.write().await;
        #[allow(clippy::cast_possible_truncation)]
        let t_lock = start.elapsed().as_micros() as u64;
        let result = self
            .db_handler
            .handle_publish(
                &mut ctrl,
                event.topic.as_ref(),
                &event.payload,
                event.response_topic.as_deref(),
                event.correlation_data.as_deref(),
                event.user_id.as_deref(),
                Some(event.client_id.as_ref()),
            )
            .await;

        match result {
            DbPublishResult::Response(response) => {
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
            DbPublishResult::NoResponse => {}
            DbPublishResult::PendingUniqueCheck(pending) => {
                let local_reserved = pending.phase1.local_reserved;
                let pending_remote = pending.phase1.pending_remote;
                let continuation = pending.continuation;
                drop(ctrl);
                let remote_results = await_unique_reserves(pending_remote).await;
                let mut ctrl = self.controller.write().await;
                if let Some(response) = self
                    .db_handler
                    .complete_pending_unique_check(
                        &mut ctrl,
                        local_reserved,
                        remote_results,
                        continuation,
                    )
                    .await
                {
                    ctrl.transport()
                        .queue_local_publish(response.topic, response.payload, qos_to_u8(event.qos))
                        .await;
                }
            }
        }
    }

    pub(super) async fn route_and_forward_publish(
        &self,
        node_id: NodeId,
        event: &ClientPublishEvent,
    ) {
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
            let connected_node = Self::resolve_connected_node(&ctrl, &target.client_id);
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
            drop(ctrl);

            Self::forward_publish_to_remotes(
                &transport,
                node_id,
                topic,
                qos_to_u8(event.qos),
                event.retain,
                &event.payload,
                remote_nodes,
            )
            .await;
        }
    }

    pub(super) fn prepare_lwt_forwarding(
        ctrl: &NodeController<T>,
        node_id: NodeId,
        client_id: &str,
    ) -> Option<LwtForwardingData<T>> {
        let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
        let prepared = lwt_publisher.prepare_lwt(client_id);

        debug!(
            client_id,
            has_lwt = prepared.as_ref().is_ok_and(Option::is_some),
            "checking LWT conditions"
        );

        let Ok(Some(lwt)) = prepared else {
            return None;
        };

        debug!(client_id, topic = %lwt.topic, qos = lwt.qos, "routing LWT to subscribers");

        let wildcards = ctrl.stores().wildcards.match_topic(&lwt.topic);
        let router = PublishRouter::new(&ctrl.stores().topics);
        let route = router.route_with_wildcards(&lwt.topic, &wildcards);

        let remote_nodes = Self::collect_remote_targets(
            ctrl,
            route
                .targets
                .into_iter()
                .map(|t| ForwardTarget::new(t.client_id, t.qos)),
            node_id,
        );

        let is_clean_session = ctrl
            .stores()
            .sessions
            .get(client_id)
            .is_some_and(|s| s.is_clean_session());

        Some(LwtForwardingData {
            transport: ctrl.transport().clone(),
            topic: lwt.topic,
            qos: lwt.qos,
            retain: lwt.retain,
            payload: lwt.payload,
            token: lwt.token,
            remote_nodes,
            is_clean_session,
        })
    }
}
