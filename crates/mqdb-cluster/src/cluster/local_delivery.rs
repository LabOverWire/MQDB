// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::LocalPublishRequest;
use mqtt5::QoS;
use mqtt5::broker::router::MessageRouter;
use mqtt5::packet::publish::PublishPacket;
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tracing::warn;

const ROUTED_LANES: usize = 32;
const ROUTED_LANE_CAPACITY: usize = 1024;

struct RoutedDelivery {
    publish: PublishPacket,
    share_groups: Option<HashSet<String>>,
}

pub(crate) struct RoutedDeliveryLanes {
    lanes: [flume::Sender<RoutedDelivery>; ROUTED_LANES],
}

impl RoutedDeliveryLanes {
    pub(crate) fn spawn(router: &Arc<MessageRouter>) -> Self {
        let lanes = [(); ROUTED_LANES].map(|()| {
            let (tx, rx) = flume::bounded(ROUTED_LANE_CAPACITY);
            tokio::spawn(run_lane(Arc::clone(router), rx));
            tx
        });
        Self { lanes }
    }

    pub(crate) async fn deliver(&self, request: LocalPublishRequest) {
        let lane = &self.lanes[lane_of(&request)];
        let publish = request.user_properties.into_iter().fold(
            PublishPacket::new(request.topic, request.payload, QoS::from(request.qos)),
            |publish, (key, value)| publish.with_user_property(key, value),
        );
        let delivery = RoutedDelivery {
            publish,
            share_groups: request
                .share_groups
                .map(|groups| groups.into_iter().collect()),
        };
        if let Err(e) = lane.send_async(delivery).await {
            warn!(topic = %e.0.publish.topic_name, "routed delivery lane closed, dropping message");
        }
    }
}

fn lane_of(request: &LocalPublishRequest) -> usize {
    lane_for(request.order_key.as_deref().unwrap_or(&request.topic))
}

fn lane_for(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    usize::from(hasher.finish().to_le_bytes()[0]) % ROUTED_LANES
}

async fn run_lane(router: Arc<MessageRouter>, rx: flume::Receiver<RoutedDelivery>) {
    while let Ok(delivery) = rx.recv_async().await {
        match &delivery.share_groups {
            Some(groups) => {
                router
                    .route_message_local_only_with_share_groups(&delivery.publish, None, groups)
                    .await;
            }
            None => router.route_message(&delivery.publish, None).await,
        }
    }
}

#[cfg(test)]
pub(crate) async fn subscribe_test_client(
    router: &MessageRouter,
    client_id: &str,
    topic: &str,
    capacity: usize,
) -> mqtt5::broker::router::LaneReceivers {
    use mqtt5::broker::router::{DeliveryLanes, SubscriptionRequest};

    let (lanes, receivers) = DeliveryLanes::channel(capacity);
    let disconnect_tx = tokio::sync::oneshot::channel().0;
    router
        .register_client(
            client_id.to_string(),
            lanes,
            router.queue_handle(client_id),
            disconnect_tx,
        )
        .await;
    router
        .subscribe(SubscriptionRequest::new(client_id, topic, QoS::AtLeastOnce))
        .await
        .unwrap();
    receivers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::LocalPublishKind;
    use std::time::Duration;

    fn routed(topic: &str, payload: &[u8]) -> LocalPublishRequest {
        LocalPublishRequest {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos: 1,
            kind: LocalPublishKind::Routed,
            user_properties: Vec::new(),
            order_key: None,
            share_groups: None,
        }
    }

    async fn received_within(receivers: &mut mqtt5::broker::router::LaneReceivers) -> bool {
        tokio::time::timeout(Duration::from_millis(300), receivers.recv())
            .await
            .is_ok_and(|delivered| delivered.is_some())
    }

    #[tokio::test]
    async fn a_routed_publish_serves_only_its_assigned_share_groups() {
        let router = Arc::new(MessageRouter::new());
        let mut plain = subscribe_test_client(&router, "plain", "sh/x", 8).await;
        let mut assigned = subscribe_test_client(&router, "member-g", "$share/g/sh/x", 8).await;
        let mut unassigned = subscribe_test_client(&router, "member-h", "$share/h/sh/x", 8).await;
        let delivery = RoutedDeliveryLanes::spawn(&router);

        delivery
            .deliver(LocalPublishRequest {
                share_groups: Some(vec!["g".to_string()]),
                ..routed("sh/x", b"m")
            })
            .await;

        assert!(
            received_within(&mut plain).await,
            "plain subscriber missed it"
        );
        assert!(
            received_within(&mut assigned).await,
            "assigned group g missed it"
        );
        assert!(
            !received_within(&mut unassigned).await,
            "unassigned group h received it"
        );
    }

    #[tokio::test]
    async fn a_routed_publish_from_an_older_node_serves_every_share_group() {
        let router = Arc::new(MessageRouter::new());
        let mut g = subscribe_test_client(&router, "member-g", "$share/g/sh/x", 8).await;
        let mut h = subscribe_test_client(&router, "member-h", "$share/h/sh/x", 8).await;
        let delivery = RoutedDeliveryLanes::spawn(&router);

        delivery.deliver(routed("sh/x", b"m")).await;

        assert!(received_within(&mut g).await, "group g missed it");
        assert!(received_within(&mut h).await, "group h missed it");
    }

    #[tokio::test]
    async fn a_stalled_subscriber_does_not_delay_topics_on_other_lanes() {
        let stalled_topic = "plant/stalled";
        let live_topic = (0..)
            .map(|n| format!("plant/live/{n}"))
            .find(|topic| lane_for(topic) != lane_for(stalled_topic))
            .unwrap();
        let router = Arc::new(MessageRouter::new());
        let stalled_receivers = subscribe_test_client(&router, "stalled", stalled_topic, 1).await;
        let mut live_receivers = subscribe_test_client(&router, "live", &live_topic, 8).await;
        let delivery = RoutedDeliveryLanes::spawn(&router);

        delivery
            .deliver(routed(stalled_topic, b"fills the lane"))
            .await;
        delivery.deliver(routed(stalled_topic, b"waits")).await;
        delivery.deliver(routed(&live_topic, b"live")).await;

        let delivered = tokio::time::timeout(Duration::from_millis(500), live_receivers.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&delivered.publish.payload[..], b"live");
        drop(stalled_receivers);
    }

    #[test]
    fn requests_with_one_order_key_share_a_lane_across_topics() {
        let created = "$DB/diagrams/d1/events/created";
        let other_lane_topic = (0..)
            .map(|n| format!("$DB/diagrams/d1/events/updated/{n}"))
            .find(|topic| lane_for(topic) != lane_for(created))
            .unwrap();
        let keyed = |topic: &str| LocalPublishRequest {
            order_key: Some("diagrams/d1".to_string()),
            ..routed(topic, b"{}")
        };

        assert_eq!(lane_of(&keyed(created)), lane_of(&keyed(&other_lane_topic)));
    }

    #[tokio::test]
    async fn user_properties_reach_the_subscriber() {
        let router = Arc::new(MessageRouter::new());
        let mut receivers = subscribe_test_client(&router, "watcher", "$DB/ev/events/r1", 8).await;
        let delivery = RoutedDeliveryLanes::spawn(&router);

        delivery
            .deliver(LocalPublishRequest {
                user_properties: vec![("x-origin-client-id".to_string(), "editor-1".to_string())],
                ..routed("$DB/ev/events/r1", b"{}")
            })
            .await;

        let delivered = tokio::time::timeout(Duration::from_secs(1), receivers.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            delivered
                .publish
                .properties
                .get_user_property_value("x-origin-client-id"),
            Some("editor-1")
        );
    }
}
