// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::node_controller::{
    NodeController, PendingFkDeleteWork, PendingFkWork, PendingUniqueWork,
};
use super::super::protocol::PresenceBroadcast;
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{
    ForwardTarget, ForwardedPublish, LwtPublisher, NodeId, PublishRouter, SubscriptionType,
    TopicSubscriptionBroadcast, WildcardBroadcast,
};
use super::{ClusterEventHandler, LwtForwardingData, qos_to_u8};
use crate::cluster::{LocalShare, RemoteForward, remote_forwards};
use mqtt5::broker::events::ClientPublishEvent;
use std::collections::HashMap;
use tokio::sync::RwLockWriteGuard;
use tracing::{debug, trace, warn};

impl<T: ClusterTransport + 'static> ClusterEventHandler<T> {
    pub(super) async fn client_is_live_elsewhere(&self, client_id: &str) -> bool {
        let ctrl = self.controller.read().await;
        ctrl.connected_node(client_id)
            .is_some_and(|node| node != self.node_id)
    }

    pub(super) async fn emit_presence(&self, presence: &mqdb_agent::presence::PresenceEvent) {
        if mqdb_agent::presence::is_internal_client(&presence.client_id)
            || !mqdb_agent::presence::is_publishable_client_id(&presence.client_id)
        {
            return;
        }
        let payload = match presence.payload() {
            Ok(payload) => payload,
            Err(e) => {
                warn!(error = %e, "failed to serialize presence event");
                return;
            }
        };
        let topic = presence.topic();

        let transport = {
            let ctrl = self.controller.read().await;
            ctrl.transport().clone()
        };

        transport
            .queue_local_publish_retained(topic.clone(), payload.clone(), 0)
            .await;

        let Some(broadcast) = PresenceBroadcast::try_new(&topic, &payload) else {
            warn!(topic, "presence message too large to broadcast");
            return;
        };
        let _ = transport
            .broadcast(ClusterMessage::PresenceBroadcast(broadcast))
            .await;
    }

    pub(super) async fn broadcast_topic_subscription(
        ctrl: &NodeController<T>,
        broadcast: TopicSubscriptionBroadcast,
    ) {
        let msg = ClusterMessage::TopicSubscriptionBroadcast(broadcast);
        let _ = ctrl.transport().broadcast(msg).await;
    }

    pub(super) async fn forward_publish_to_remotes(
        transport: &T,
        origin: NodeId,
        topic: &str,
        qos: u8,
        retain: bool,
        payload: &[u8],
        forwards: HashMap<NodeId, RemoteForward>,
    ) {
        for (target_node, forward) in forwards {
            let fwd = ForwardedPublish::new(
                origin,
                topic.to_string(),
                qos,
                retain,
                payload.to_vec(),
                forward.targets,
            )
            .with_share_groups(forward.share_groups);
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
        let is_response_topic = crate::cluster::is_response_topic(topic);
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
        use super::super::db_handler::{DbPublishResult, MqttRequestContext};

        debug!(topic = %event.topic, "handling $DB/ request");
        let start = std::time::Instant::now();
        let mut ctrl = self.controller.write().await;
        let t_lock = crate::cluster::node_controller::elapsed_micros(start);
        let ctx = MqttRequestContext {
            response_topic: event.response_topic.as_deref(),
            correlation_data: event.correlation_data.as_deref(),
            sender: event.user_id.as_deref(),
            client_id: Some(event.client_id.as_ref()),
        };
        let result = self
            .db_handler
            .handle_publish(&mut ctrl, event.topic.as_ref(), &event.payload, &ctx)
            .await;
        let qos = qos_to_u8(event.qos);

        match result {
            DbPublishResult::Response(response) => {
                let t_handle = crate::cluster::node_controller::elapsed_micros(start);
                ctrl.transport()
                    .queue_local_response(
                        response.topic,
                        response.payload,
                        qos,
                        response.correlation_data,
                    )
                    .await;
                let t_queue = crate::cluster::node_controller::elapsed_micros(start);
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
                self.finish_unique_check(ctrl, *pending, qos).await;
            }
            DbPublishResult::PendingFkCheck(pending) => {
                self.finish_fk_check(ctrl, *pending, qos).await;
            }
            DbPublishResult::PendingFkDelete(pending) => {
                self.finish_fk_delete(ctrl, *pending, qos).await;
            }
        }
    }

    async fn finish_unique_check(
        &self,
        ctrl: RwLockWriteGuard<'_, NodeController<T>>,
        pending: PendingUniqueWork,
        qos: u8,
    ) {
        let PendingUniqueWork {
            phase1,
            continuation,
        } = pending;
        drop(ctrl);
        let remote_results = combine_quorum(phase1.pending_remote, phase1.pending_quorum).await;
        let mut ctrl = self.controller.write().await;
        if let Some(response) = self
            .db_handler
            .complete_pending_unique_check(
                &mut ctrl,
                phase1.local_reserved,
                remote_results,
                continuation,
            )
            .await
        {
            ctrl.transport()
                .queue_local_response(
                    response.topic,
                    response.payload,
                    qos,
                    response.correlation_data,
                )
                .await;
        }
    }

    async fn finish_fk_check(
        &self,
        ctrl: RwLockWriteGuard<'_, NodeController<T>>,
        pending: PendingFkWork,
        qos: u8,
    ) {
        use super::super::db_handler::FkCheckCompletion;
        use super::super::node_controller::fk::await_fk_checks;

        drop(ctrl);
        let fk_result = await_fk_checks(pending.pending_checks).await;
        let mut ctrl = self.controller.write().await;
        let (fk_ok, fk_error) = match fk_result {
            Ok(()) => (true, None),
            Err(msg) => (false, Some(msg)),
        };
        match self
            .db_handler
            .complete_pending_fk_check(&mut ctrl, fk_ok, fk_error, pending.continuation)
            .await
        {
            FkCheckCompletion::Done(Some(response)) => {
                ctrl.transport()
                    .queue_local_response(
                        response.topic,
                        response.payload,
                        qos,
                        response.correlation_data,
                    )
                    .await;
            }
            FkCheckCompletion::NeedUniqueCheck(unique) => {
                self.finish_unique_check(ctrl, *unique, qos).await;
            }
            FkCheckCompletion::Done(None) => {}
        }
    }

    async fn finish_fk_delete(
        &self,
        ctrl: RwLockWriteGuard<'_, NodeController<T>>,
        pending: PendingFkDeleteWork,
        qos: u8,
    ) {
        let PendingFkDeleteWork {
            local_results,
            pending_lookups,
            continuation,
        } = pending;
        let (del_entity, del_id) = continuation.entity_id();
        let (del_entity, del_id) = (del_entity.to_string(), del_id.to_string());
        let sender_owned = continuation.sender_ref().map(String::from);
        drop(ctrl);
        let (restrict_error, side_effects) =
            super::super::node_controller::fk::collect_recursive_cascade(
                &self.controller,
                &del_entity,
                &del_id,
                local_results,
                pending_lookups,
                sender_owned.as_deref(),
            )
            .await;
        let mut ctrl = self.controller.write().await;
        if let Some(response) = self
            .db_handler
            .complete_pending_fk_delete(&mut ctrl, restrict_error, side_effects, continuation)
            .await
        {
            ctrl.transport()
                .queue_local_response(
                    response.topic,
                    response.payload,
                    qos,
                    response.correlation_data,
                )
                .await;
        }
    }

    pub(super) async fn route_and_forward_publish(
        &self,
        node_id: NodeId,
        event: &ClientPublishEvent,
    ) {
        let held = self.held_local_shares(event.topic.as_ref()).await;
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

        let remote_nodes = ctrl.remote_targets(
            targets
                .into_iter()
                .map(|target| ForwardTarget::new(target.client_id, target.qos)),
        );
        let share_nodes = self.share_assignment(&ctrl, topic, &held);

        if remote_nodes.is_empty() && share_nodes.is_empty() {
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
                remote_forwards(remote_nodes, share_nodes),
            )
            .await;
        }
    }

    pub(super) fn prepare_lwt_forwarding(
        &self,
        ctrl: &NodeController<T>,
        client_id: &str,
        held: &[LocalShare],
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

        let remote_nodes = ctrl.remote_targets(
            route
                .targets
                .into_iter()
                .map(|t| ForwardTarget::new(t.client_id, t.qos)),
        );

        let share_nodes = self.share_assignment(ctrl, &lwt.topic, held);

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
            share_nodes,
            is_clean_session,
        })
    }
}

async fn combine_quorum(
    pending_remote: Vec<super::super::node_controller::PendingUniqueReserve>,
    pending_quorum: Vec<(String, tokio::sync::oneshot::Receiver<bool>)>,
) -> super::super::node_controller::RemoteReserveResult {
    use super::super::node_controller::ReserveFailure;
    use super::super::node_controller::unique::{await_unique_quorum, await_unique_reserves};
    let (remote_results, quorum) = tokio::join!(
        await_unique_reserves(pending_remote),
        await_unique_quorum(pending_quorum),
    );
    match remote_results {
        Ok(confirmed) => match quorum {
            Ok(durable) => {
                tracing::debug!(?durable, "unique reserve is majority-durable");
                Ok(confirmed)
            }
            Err(field) => Err((ReserveFailure::NotDurable(field), confirmed)),
        },
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::db::data_partition;
    use super::super::super::node_controller::{
        FkCheckContinuation, FkDeleteContinuation, PendingFkDeleteWork, PendingFkWork,
        PendingUniqueWork, UniqueCheckContinuation, UniqueCheckPhase1Result,
    };
    use super::*;
    use crate::cluster::db_handler::tests::{
        LocalResponses, MockTransport, setup_controller_all_partitions,
    };
    use mqtt5::broker::events::BrokerEventHandler;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const RESPONSE_TOPIC: &str = "resp/t";

    type Harness = (
        ClusterEventHandler<MockTransport>,
        Arc<RwLock<NodeController<MockTransport>>>,
        LocalResponses,
    );

    fn harness() -> Harness {
        let ctrl = setup_controller_all_partitions();
        let responses = Arc::clone(&ctrl.transport().local_responses);
        let node = ctrl.node_id();
        let ctrl = Arc::new(RwLock::new(ctrl));
        let handler = ClusterEventHandler::new(node, Arc::clone(&ctrl));
        (handler, ctrl, responses)
    }

    async fn request(
        handler: &ClusterEventHandler<MockTransport>,
        topic: &str,
        payload: &serde_json::Value,
        correlation: &[u8],
    ) {
        handler
            .on_client_publish(ClientPublishEvent {
                client_id: "client-1".into(),
                user_id: None,
                topic: topic.into(),
                payload: serde_json::to_vec(payload).unwrap().into(),
                qos: mqtt5::QoS::AtLeastOnce,
                retain: false,
                packet_id: Some(1),
                response_topic: Some(RESPONSE_TOPIC.into()),
                correlation_data: Some(correlation.to_vec().into()),
            })
            .await;
    }

    fn take(responses: &LocalResponses) -> Vec<(String, Option<Vec<u8>>)> {
        std::mem::take(&mut *responses.lock().unwrap())
    }

    fn expect_one(responses: &LocalResponses, correlation: &[u8]) {
        assert_eq!(
            take(responses),
            vec![(RESPONSE_TOPIC.to_string(), Some(correlation.to_vec()))]
        );
    }

    #[tokio::test]
    async fn crud_replies_carry_the_request_correlation_data() {
        let (handler, _, responses) = harness();
        let record = serde_json::json!({"id": "x1", "n": 1});
        let empty = serde_json::json!({});

        request(&handler, "$DB/item/create", &record, b"create-1").await;
        expect_one(&responses, b"create-1");

        request(&handler, "$DB/item/x1", &empty, b"read-2").await;
        expect_one(&responses, b"read-2");

        request(
            &handler,
            "$DB/item/x1/update",
            &serde_json::json!({"n": 2}),
            b"update-3",
        )
        .await;
        expect_one(&responses, b"update-3");

        request(&handler, "$DB/item/list", &empty, b"list-4").await;
        expect_one(&responses, b"list-4");

        request(&handler, "$DB/item/x1/delete", &empty, b"delete-5").await;
        expect_one(&responses, b"delete-5");
    }

    #[tokio::test]
    async fn a_reply_without_request_correlation_data_carries_none() {
        let (handler, _, responses) = harness();
        handler
            .on_client_publish(ClientPublishEvent {
                client_id: "client-1".into(),
                user_id: None,
                topic: "$DB/item/create".into(),
                payload: serde_json::to_vec(&serde_json::json!({"id": "x1"}))
                    .unwrap()
                    .into(),
                qos: mqtt5::QoS::AtLeastOnce,
                retain: false,
                packet_id: Some(1),
                response_topic: Some(RESPONSE_TOPIC.into()),
                correlation_data: None,
            })
            .await;
        assert_eq!(take(&responses), vec![(RESPONSE_TOPIC.to_string(), None)]);
    }

    #[tokio::test]
    async fn the_unique_check_continuation_reply_carries_the_correlation_data() {
        let (handler, ctrl, responses) = harness();
        let data = serde_json::json!({"id": "u1", "email": "a@b.c"});
        let pending = PendingUniqueWork {
            phase1: UniqueCheckPhase1Result {
                local_reserved: Vec::new(),
                pending_remote: Vec::new(),
                pending_quorum: Vec::new(),
            },
            continuation: UniqueCheckContinuation::CreateFromDbHandler {
                entity: "user".to_string(),
                id: "u1".to_string(),
                data: data.clone(),
                constraint_data: data,
                partition: data_partition("user", "u1"),
                request_id: "req-1".to_string(),
                now_ms: 1000,
                sender: None,
                client_id: None,
                response_topic: RESPONSE_TOPIC.to_string(),
                correlation_data: Some(b"unique-1".to_vec()),
            },
        };

        let guard = ctrl.write().await;
        handler.finish_unique_check(guard, pending, 1).await;

        expect_one(&responses, b"unique-1");
    }

    #[tokio::test]
    async fn the_fk_check_continuation_reply_carries_the_correlation_data() {
        let (handler, ctrl, responses) = harness();
        let pending = PendingFkWork {
            pending_checks: Vec::new(),
            continuation: FkCheckContinuation::CreateFromDbHandler {
                entity: "post".to_string(),
                id: "p1".to_string(),
                data: serde_json::json!({"id": "p1", "title": "t"}),
                partition: data_partition("post", "p1"),
                request_id: "req-2".to_string(),
                sender: None,
                client_id: None,
                response_topic: RESPONSE_TOPIC.to_string(),
                correlation_data: Some(b"fk-1".to_vec()),
            },
        };

        let guard = ctrl.write().await;
        handler.finish_fk_check(guard, pending, 1).await;

        expect_one(&responses, b"fk-1");
    }

    #[tokio::test]
    async fn the_fk_delete_continuation_reply_carries_the_correlation_data() {
        let (handler, ctrl, responses) = harness();
        ctrl.write()
            .await
            .db_create("user", "u1", br#"{"name":"a"}"#, 1000)
            .await
            .unwrap();
        let pending = PendingFkDeleteWork {
            local_results: Vec::new(),
            pending_lookups: Vec::new(),
            continuation: FkDeleteContinuation::DeleteFromDbHandler {
                entity: "user".to_string(),
                id: "u1".to_string(),
                sender: None,
                client_id: None,
                response_topic: RESPONSE_TOPIC.to_string(),
                correlation_data: Some(b"fk-del-1".to_vec()),
            },
        };

        let guard = ctrl.write().await;
        handler.finish_fk_delete(guard, pending, 1).await;

        expect_one(&responses, b"fk-del-1");
    }

    #[tokio::test]
    async fn a_forwarded_reply_carries_the_correlation_data_of_the_original_request() {
        let (_, ctrl, responses) = harness();
        let response = super::super::super::protocol::JsonDbResponse::new(
            7,
            br#"{"status":"ok"}"#.to_vec(),
            RESPONSE_TOPIC.to_string(),
            Some(b"fwd-1".to_vec()),
        );

        ctrl.write().await.handle_json_db_response(&response).await;

        expect_one(&responses, b"fwd-1");
    }
}
