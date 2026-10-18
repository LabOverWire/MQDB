// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::LocalPublishRequest;
use mqtt5::QoS;
use mqtt5::broker::router::MessageRouter;
use mqtt5::packet::publish::PublishPacket;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tracing::warn;

const FORWARDED_LANES: usize = 32;
const FORWARDED_LANE_CAPACITY: usize = 1024;

async fn deliver_forwarded_publish(router: &MessageRouter, topic: &str, payload: Vec<u8>, qos: u8) {
    let publish = PublishPacket::new(topic, payload, QoS::from(qos));
    router.route_message(&publish, None).await;
}

pub(crate) struct ForwardedDeliveryLanes {
    lanes: [flume::Sender<LocalPublishRequest>; FORWARDED_LANES],
}

impl ForwardedDeliveryLanes {
    pub(crate) fn spawn(router: &Arc<MessageRouter>) -> Self {
        let lanes = [(); FORWARDED_LANES].map(|()| {
            let (tx, rx) = flume::bounded(FORWARDED_LANE_CAPACITY);
            tokio::spawn(run_lane(Arc::clone(router), rx));
            tx
        });
        Self { lanes }
    }

    pub(crate) async fn deliver(&self, request: LocalPublishRequest) {
        let lane = &self.lanes[lane_for(&request.topic)];
        if let Err(e) = lane.send_async(request).await {
            warn!(topic = %e.0.topic, "forwarded delivery lane closed, dropping message");
        }
    }
}

fn lane_for(topic: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    usize::from(hasher.finish().to_le_bytes()[0]) % FORWARDED_LANES
}

async fn run_lane(router: Arc<MessageRouter>, rx: flume::Receiver<LocalPublishRequest>) {
    while let Ok(request) = rx.recv_async().await {
        deliver_forwarded_publish(&router, &request.topic, request.payload, request.qos).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::LocalPublishKind;
    use mqtt5::broker::router::{DeliveryLanes, LaneReceivers, SubscriptionRequest};
    use std::time::Duration;

    fn forwarded(topic: &str, payload: &[u8]) -> LocalPublishRequest {
        LocalPublishRequest {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos: 1,
            kind: LocalPublishKind::Forwarded,
            user_properties: Vec::new(),
        }
    }

    async fn subscribe(
        router: &MessageRouter,
        client_id: &str,
        topic: &str,
        capacity: usize,
    ) -> LaneReceivers {
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

    #[tokio::test]
    async fn a_stalled_subscriber_does_not_delay_topics_on_other_lanes() {
        let stalled_topic = "plant/stalled";
        let live_topic = (0..)
            .map(|n| format!("plant/live/{n}"))
            .find(|topic| lane_for(topic) != lane_for(stalled_topic))
            .unwrap();
        let router = Arc::new(MessageRouter::new());
        let stalled_receivers = subscribe(&router, "stalled", stalled_topic, 1).await;
        let mut live_receivers = subscribe(&router, "live", &live_topic, 8).await;
        let delivery = ForwardedDeliveryLanes::spawn(&router);

        delivery
            .deliver(forwarded(stalled_topic, b"fills the lane"))
            .await;
        delivery.deliver(forwarded(stalled_topic, b"waits")).await;
        delivery.deliver(forwarded(&live_topic, b"live")).await;

        let delivered = tokio::time::timeout(Duration::from_millis(500), live_receivers.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&delivered.publish.payload[..], b"live");
        drop(stalled_receivers);
    }
}
