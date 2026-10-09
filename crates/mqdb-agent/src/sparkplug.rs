// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;

use mqtt5::broker::events::{BrokerEventHandler, ClientPublishEvent, PublishAction};
use tracing::debug;

pub const CERTIFICATES_PREFIX: &str = "$sparkplug/certificates/";

pub const CERTIFICATE_CHANNEL_CAPACITY: usize = 1024;

const NAMESPACE: &str = "spBv1.0";

#[must_use]
pub fn certificate_topic(topic: &str) -> Option<String> {
    let mut levels = topic.split('/');
    let (Some(NAMESPACE), Some(group), Some(kind), Some(edge)) =
        (levels.next(), levels.next(), levels.next(), levels.next())
    else {
        return None;
    };
    let device = levels.next();
    if levels.next().is_some() || group.is_empty() || edge.is_empty() {
        return None;
    }
    let is_birth = match (kind, device) {
        ("NBIRTH", None) => true,
        ("DBIRTH", Some(device)) => !device.is_empty(),
        _ => false,
    };
    is_birth.then(|| format!("{CERTIFICATES_PREFIX}{topic}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certificate {
    pub topic: String,
    pub payload: Vec<u8>,
}

impl Certificate {
    #[must_use]
    pub fn from_publish(topic: &str, payload: &[u8]) -> Option<Self> {
        if payload.is_empty() {
            return None;
        }
        certificate_topic(topic).map(|topic| Self {
            topic,
            payload: payload.to_vec(),
        })
    }
}

pub struct CertificateEventHandler {
    sender: flume::Sender<Certificate>,
}

impl CertificateEventHandler {
    #[must_use]
    pub fn new(sender: flume::Sender<Certificate>) -> Self {
        Self { sender }
    }

    pub fn observe(&self, topic: &str, payload: &[u8]) {
        let Some(certificate) = Certificate::from_publish(topic, payload) else {
            return;
        };
        if self.sender.try_send(certificate).is_err() {
            debug!(
                topic,
                "certificate queue full or closed, dropping certificate"
            );
        }
    }
}

impl BrokerEventHandler for CertificateEventHandler {
    fn on_client_publish<'a>(
        &'a self,
        event: ClientPublishEvent,
    ) -> Pin<Box<dyn Future<Output = PublishAction> + Send + 'a>> {
        Box::pin(async move {
            self.observe(&event.topic, &event.payload);
            PublishAction::Continue
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_birth_maps_to_its_certificate_topic() {
        assert_eq!(
            certificate_topic("spBv1.0/G1/NBIRTH/E1").as_deref(),
            Some("$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1")
        );
    }

    #[test]
    fn device_birth_maps_to_its_certificate_topic() {
        assert_eq!(
            certificate_topic("spBv1.0/G1/DBIRTH/E1/D1").as_deref(),
            Some("$sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/D1")
        );
    }

    #[test]
    fn other_messages_have_no_certificate() {
        for topic in [
            "spBv1.0/G1/NDATA/E1",
            "spBv1.0/G1/NDEATH/E1",
            "spBv1.0/G1/DDATA/E1/D1",
            "spBv1.0/G1/DDEATH/E1/D1",
            "spBv1.0/G1/NCMD/E1",
            "spBv1.0/STATE/host",
            "spBv1.0/G1/NBIRTH/E1/D1",
            "spBv1.0/G1/DBIRTH/E1",
            "spBv1.0/G1/DBIRTH/E1/D1/extra",
            "spBv1.0//NBIRTH/E1",
            "spBv1.0/G1/NBIRTH/",
            "spBv1.0/G1/DBIRTH/E1/",
            "spAv1.0/G1/NBIRTH/E1",
            "sensors/G1/NBIRTH/E1",
        ] {
            assert_eq!(certificate_topic(topic), None, "{topic}");
        }
    }

    #[test]
    fn empty_birth_payload_is_not_stored() {
        assert_eq!(Certificate::from_publish("spBv1.0/G1/NBIRTH/E1", b""), None);
    }

    #[tokio::test]
    async fn handler_queues_births_and_lets_every_publish_through() {
        let (tx, rx) = flume::bounded(4);
        let handler = CertificateEventHandler::new(tx);

        for (topic, payload) in [
            ("spBv1.0/G1/NBIRTH/E1", b"node-birth".as_slice()),
            ("spBv1.0/G1/NDATA/E1", b"data".as_slice()),
            ("spBv1.0/G1/DBIRTH/E1/D1", b"device-birth".as_slice()),
        ] {
            let action = handler
                .on_client_publish(ClientPublishEvent {
                    client_id: "E1".into(),
                    user_id: None,
                    topic: topic.into(),
                    payload: payload.to_vec().into(),
                    qos: mqtt5::QoS::AtMostOnce,
                    retain: false,
                    packet_id: None,
                    response_topic: None,
                    correlation_data: None,
                })
                .await;
            assert!(matches!(action, PublishAction::Continue), "{topic}");
        }

        assert_eq!(
            rx.try_recv().ok(),
            Some(Certificate {
                topic: "$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1".into(),
                payload: b"node-birth".to_vec(),
            })
        );
        assert_eq!(
            rx.try_recv().ok(),
            Some(Certificate {
                topic: "$sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/D1".into(),
                payload: b"device-birth".to_vec(),
            })
        );
        assert!(rx.try_recv().is_err(), "data messages have no certificate");
    }
}
