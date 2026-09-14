// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;

use mqtt5::broker::events::{BrokerEventHandler, ClientConnectEvent, ClientDisconnectEvent};
use serde::Serialize;
use tracing::debug;

pub const PRESENCE_TOPIC_PREFIX: &str = "$DB/_presence/";

pub const PRESENCE_CHANNEL_CAPACITY: usize = 1024;

const INTERNAL_CLIENT_PREFIX: &str = "mqdb-";

#[must_use]
pub fn is_internal_client(client_id: &str) -> bool {
    client_id.starts_with(INTERNAL_CLIENT_PREFIX)
}

#[must_use]
pub fn presence_topic(client_id: &str) -> String {
    format!("{PRESENCE_TOPIC_PREFIX}{client_id}")
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PresenceState {
    Connect,
    Disconnect,
}

#[derive(Debug, Clone, Serialize)]
pub struct PresenceEvent {
    pub client_id: String,
    pub user_id: Option<String>,
    pub event: PresenceState,
    pub unexpected: bool,
    pub ts: u64,
}

impl PresenceEvent {
    #[must_use]
    pub fn connect(client_id: &str, user_id: Option<&str>) -> Self {
        Self {
            client_id: client_id.to_string(),
            user_id: user_id.map(str::to_string),
            event: PresenceState::Connect,
            unexpected: false,
            ts: current_time_ms(),
        }
    }

    #[must_use]
    pub fn disconnect(client_id: &str, user_id: Option<&str>, unexpected: bool) -> Self {
        Self {
            client_id: client_id.to_string(),
            user_id: user_id.map(str::to_string),
            event: PresenceState::Disconnect,
            unexpected,
            ts: current_time_ms(),
        }
    }

    #[must_use]
    pub fn topic(&self) -> String {
        presence_topic(&self.client_id)
    }

    #[must_use]
    pub fn payload(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

pub struct PresenceEventHandler {
    sender: flume::Sender<PresenceEvent>,
}

impl PresenceEventHandler {
    #[must_use]
    pub fn new(sender: flume::Sender<PresenceEvent>) -> Self {
        Self { sender }
    }

    fn emit(&self, presence: PresenceEvent) {
        if self.sender.try_send(presence).is_err() {
            debug!("presence queue full or closed, dropping presence event");
        }
    }
}

impl BrokerEventHandler for PresenceEventHandler {
    fn on_client_connect<'a>(
        &'a self,
        event: ClientConnectEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if is_internal_client(&event.client_id) {
                return;
            }
            self.emit(PresenceEvent::connect(
                &event.client_id,
                event.user_id.as_deref(),
            ));
        })
    }

    fn on_client_disconnect<'a>(
        &'a self,
        event: ClientDisconnectEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if is_internal_client(&event.client_id) {
                return;
            }
            self.emit(PresenceEvent::disconnect(
                &event.client_id,
                event.user_id.as_deref(),
                event.unexpected,
            ));
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_payload_shape() {
        let presence = PresenceEvent::connect("seat-holder-7", Some("alice"));
        let value: serde_json::Value =
            serde_json::from_slice(&presence.payload()).expect("payload is valid JSON");
        assert_eq!(value["client_id"], "seat-holder-7");
        assert_eq!(value["user_id"], "alice");
        assert_eq!(value["event"], "connect");
        assert_eq!(value["unexpected"], false);
        assert!(value["ts"].as_u64().is_some_and(|ts| ts > 0));
    }

    #[test]
    fn disconnect_payload_preserves_unexpected_and_anonymous_user() {
        let presence = PresenceEvent::disconnect("seat-holder-7", None, true);
        let value: serde_json::Value =
            serde_json::from_slice(&presence.payload()).expect("payload is valid JSON");
        assert_eq!(value["event"], "disconnect");
        assert_eq!(value["unexpected"], true);
        assert!(value["user_id"].is_null());
    }

    #[test]
    fn topic_is_namespaced_per_client() {
        let presence = PresenceEvent::connect("seat-holder-7", None);
        assert_eq!(presence.topic(), "$DB/_presence/seat-holder-7");
    }

    #[test]
    fn internal_clients_are_recognized() {
        assert!(is_internal_client("mqdb-admin-1"));
        assert!(is_internal_client("mqdb-forward-2"));
        assert!(is_internal_client("mqdb-presence-publisher"));
        assert!(!is_internal_client("seat-holder-7"));
        assert!(!is_internal_client("janitor"));
    }

    #[tokio::test]
    async fn handler_emits_connect_and_skips_internal_clients() {
        let (tx, rx) = flume::bounded(PRESENCE_CHANNEL_CAPACITY);
        let handler = PresenceEventHandler::new(tx);

        handler
            .on_client_disconnect(ClientDisconnectEvent {
                client_id: "mqdb-admin-1".into(),
                user_id: None,
                reason: mqtt5::types::ReasonCode::Success,
                unexpected: false,
            })
            .await;
        assert!(
            rx.is_empty(),
            "internal clients must not produce presence events"
        );

        handler
            .on_client_disconnect(ClientDisconnectEvent {
                client_id: "seat-holder-7".into(),
                user_id: Some("alice".into()),
                reason: mqtt5::types::ReasonCode::Success,
                unexpected: false,
            })
            .await;
        let presence = rx.try_recv().expect("a presence event was queued");
        assert_eq!(presence.client_id, "seat-holder-7");
        assert_eq!(presence.event, PresenceState::Disconnect);
        assert!(
            !presence.unexpected,
            "a clean disconnect must still emit presence"
        );
    }
}
