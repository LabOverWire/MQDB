// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use mqtt5::QoS;
use mqtt5::broker::router::MessageRouter;
use mqtt5::packet::publish::PublishPacket;
use tracing::{debug, warn};

pub const RESERVED_ROOT: &str = "$sparkplug";

pub const CERTIFICATES_PREFIX: &str = "$sparkplug/certificates/";

const NAMESPACE: &str = "spBv1.0";

#[must_use]
pub fn is_reserved_topic(topic: &str) -> bool {
    topic
        .strip_prefix(RESERVED_ROOT)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Birth<'a> {
    Node {
        group: &'a str,
        edge: &'a str,
    },
    Device {
        group: &'a str,
        edge: &'a str,
        device: &'a str,
    },
}

#[must_use]
pub fn parse_birth(topic: &str) -> Option<Birth<'_>> {
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
    match (kind, device) {
        ("NBIRTH", None) => Some(Birth::Node { group, edge }),
        ("DBIRTH", Some(device)) if !device.is_empty() => Some(Birth::Device {
            group,
            edge,
            device,
        }),
        _ => None,
    }
}

#[must_use]
pub fn certificate_topic(topic: &str) -> Option<String> {
    parse_birth(topic).map(|_| format!("{CERTIFICATES_PREFIX}{topic}"))
}

fn device_certificates_filter(group: &str, edge: &str) -> String {
    format!("{CERTIFICATES_PREFIX}{NAMESPACE}/{group}/DBIRTH/{edge}/+")
}

fn retained(topic: String, payload: Vec<u8>) -> PublishPacket {
    PublishPacket::new(topic, payload, QoS::AtLeastOnce).with_retain(true)
}

#[derive(Default)]
pub struct CertificateStore {
    router: OnceLock<Arc<MessageRouter>>,
}

impl CertificateStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach(&self, router: Arc<MessageRouter>) {
        if self.router.set(router).is_err() {
            warn!("certificate store already attached to a broker");
        }
    }

    pub async fn observe(&self, topic: &str, payload: &[u8]) {
        let (Some(birth), Some(certificate)) = (parse_birth(topic), certificate_topic(topic))
        else {
            return;
        };
        if payload.is_empty() {
            return;
        }
        let Some(router) = self.router.get() else {
            warn!(
                topic,
                "certificate store not attached to a broker, birth not stored"
            );
            return;
        };
        if let Birth::Node { group, edge } = birth {
            for stale in router
                .get_retained_messages(&device_certificates_filter(group, edge))
                .await
            {
                debug!(topic = %stale.topic_name, "clearing device certificate after node rebirth");
                router
                    .route_message(&retained(stale.topic_name, Vec::new()), None)
                    .await;
            }
        }
        router
            .route_message(&retained(certificate, payload.to_vec()), None)
            .await;
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
        assert_eq!(
            parse_birth("spBv1.0/G1/NBIRTH/E1"),
            Some(Birth::Node {
                group: "G1",
                edge: "E1"
            })
        );
    }

    #[test]
    fn device_birth_maps_to_its_certificate_topic() {
        assert_eq!(
            certificate_topic("spBv1.0/G1/DBIRTH/E1/D1").as_deref(),
            Some("$sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/D1")
        );
        assert_eq!(
            parse_birth("spBv1.0/G1/DBIRTH/E1/D1"),
            Some(Birth::Device {
                group: "G1",
                edge: "E1",
                device: "D1"
            })
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
            assert_eq!(parse_birth(topic), None, "{topic}");
        }
    }

    #[test]
    fn reserved_namespace_is_the_sparkplug_root_only() {
        assert!(is_reserved_topic("$sparkplug"));
        assert!(is_reserved_topic(
            "$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1"
        ));
        assert!(is_reserved_topic("$sparkplug/anything"));
        assert!(!is_reserved_topic("$sparkplugs/x"));
        assert!(!is_reserved_topic("spBv1.0/G1/NBIRTH/E1"));
        assert!(!is_reserved_topic("$SYS/broker"));
    }

    #[test]
    fn device_filter_covers_only_that_edge() {
        assert_eq!(
            device_certificates_filter("G1", "E1"),
            "$sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/+"
        );
    }
}
