// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqtt5::QoS;
use mqtt5::broker::router::MessageRouter;
use mqtt5::packet::publish::PublishPacket;

pub async fn deliver_forwarded_publish(
    router: &MessageRouter,
    topic: &str,
    payload: Vec<u8>,
    qos: u8,
) {
    let publish = PublishPacket::new(topic, payload, QoS::from(qos));
    router.route_message(&publish, None).await;
}
