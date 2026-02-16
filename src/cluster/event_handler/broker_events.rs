// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::Epoch;
use super::super::client_location::{ClientLocationEntry, client_location_key};
use super::super::entity;
use super::super::protocol::{Operation, ReplicationWrite};
use super::super::session::session_partition;
use super::super::transport::ClusterTransport;
use super::{
    ClusterEventHandler, LwtPublisher, RETAINED_SET_PROCESSED, RETAINED_SET_SKIPPED,
    RETAINED_SET_TOTAL, RETAINED_SYNC_TTL, SubAckReasonCodeExt, clear_client_subscriptions,
    collect_pending_retained, current_time_ms, deliver_retained_messages,
    handle_clean_session_cleanup, qos_to_u8,
};
use bebytes::BeBytes;
use mqtt5::QoS;
use mqtt5::broker::events::{
    BrokerEventHandler, ClientConnectEvent, ClientDisconnectEvent, ClientPublishEvent,
    ClientSubscribeEvent, ClientUnsubscribeEvent, MessageDeliveredEvent, RetainedSetEvent,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use tracing::{debug, trace, warn};

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

            if event.unexpected
                && let Some(lwt_data) = Self::prepare_lwt_forwarding(&ctrl, node_id, client_id)
            {
                let is_clean = lwt_data.is_clean_session;
                let token = lwt_data.token;
                drop(ctrl);

                Self::forward_publish_to_remotes(
                    &lwt_data.transport,
                    node_id,
                    &lwt_data.topic,
                    lwt_data.qos,
                    lwt_data.retain,
                    &lwt_data.payload,
                    lwt_data.remote_nodes,
                )
                .await;

                let mut ctrl = self.controller.write().await;
                let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
                if let Err(e) = lwt_publisher.complete_lwt(client_id, token) {
                    warn!(client_id, error = %e, "failed to mark LWT as published");
                } else {
                    debug!(client_id, "marked LWT as published");
                }

                if is_clean {
                    debug!(
                        client_id,
                        "clean_session disconnect - clearing subscriptions"
                    );
                    clear_client_subscriptions(&mut ctrl, client_id).await;
                    let _ = ctrl.stores_mut().remove_session_replicated(client_id);
                }
                return;
            }

            handle_clean_session_cleanup(&mut ctrl, client_id).await;
        })
    }

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

            let mut pending_queries: Vec<(
                String,
                tokio::sync::oneshot::Receiver<Vec<super::super::retained_store::RetainedMessage>>,
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

                    if !is_wildcard && !is_response_topic {
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
                        Self::handle_wildcard_subscribe(&mut ctrl, topic, client_id, qos).await;
                    } else {
                        Self::handle_topic_subscribe(&mut ctrl, topic, client_id, qos).await;
                    }
                }
            }

            let retained_to_deliver = collect_pending_retained(pending_queries).await;

            if !retained_to_deliver.is_empty() {
                let ctrl = self.controller.read().await;
                let transport = ctrl.transport().clone();
                drop(ctrl);

                deliver_retained_messages(&transport, &synced_topics, retained_to_deliver).await;
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
                        let broadcast = super::WildcardBroadcast::unsubscribe(topic, client_id);
                        let msg =
                            super::super::transport::ClusterMessage::WildcardBroadcast(broadcast);
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
                        let broadcast =
                            super::TopicSubscriptionBroadcast::unsubscribe(topic, client_id);
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
                self.handle_db_publish(node_id, &event).await;
                return;
            }

            self.route_and_forward_publish(node_id, &event).await;

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
