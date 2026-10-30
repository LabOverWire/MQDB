// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::retained_store::RetainedStore;
use mqtt5::QoS;
use mqtt5::broker::router::MessageRouter;
use mqtt5::packet::publish::PublishPacket;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

struct PendingEcho {
    payload: Vec<u8>,
    recorded: Instant,
}

type PendingEchoes = HashMap<String, Vec<PendingEcho>>;

#[derive(Default)]
pub struct RetainedEchoes {
    pending: Mutex<PendingEchoes>,
}

impl RetainedEchoes {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, topic: &str, payload: &[u8]) {
        self.lock()
            .entry(topic.to_string())
            .or_default()
            .push(PendingEcho {
                payload: payload.to_vec(),
                recorded: Instant::now(),
            });
    }

    pub fn consume(&self, topic: &str, payload: &[u8]) -> bool {
        let mut pending = self.lock();
        let Some(echoes) = pending.get_mut(topic) else {
            return false;
        };
        let Some(index) = echoes.iter().position(|echo| echo.payload == payload) else {
            return false;
        };
        echoes.swap_remove(index);
        if echoes.is_empty() {
            pending.remove(topic);
        }
        true
    }

    pub fn prune(&self, max_age: Duration) -> usize {
        let mut pending = self.lock();
        let mut removed = 0;
        pending.retain(|_, echoes| {
            let before = echoes.len();
            echoes.retain(|echo| echo.recorded.elapsed() < max_age);
            removed += before - echoes.len();
            !echoes.is_empty()
        });
        removed
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.lock().values().map(Vec::len).sum()
    }

    fn lock(&self) -> MutexGuard<'_, PendingEchoes> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub struct LocalRetainedWriter {
    router: Arc<MessageRouter>,
    store: Arc<RetainedStore>,
    echoes: Arc<RetainedEchoes>,
}

impl LocalRetainedWriter {
    #[must_use]
    pub fn new(
        router: Arc<MessageRouter>,
        store: Arc<RetainedStore>,
        echoes: Arc<RetainedEchoes>,
    ) -> Self {
        Self {
            router,
            store,
            echoes,
        }
    }

    pub async fn refresh(&self, topic: &str) {
        match self.store.get(topic) {
            Some(message) => self.write(topic, &message.payload, message.qos).await,
            None => self.write(topic, &[], 0).await,
        }
    }

    pub async fn write(&self, topic: &str, payload: &[u8], qos: u8) {
        if self.broker_holds(topic, payload, qos).await {
            return;
        }
        self.echoes.record(topic, payload);
        let publish = PublishPacket::new(topic, payload.to_vec(), QoS::from(qos)).with_retain(true);
        self.router.route_sys_message(&publish).await;
    }

    async fn broker_holds(&self, topic: &str, payload: &[u8], qos: u8) -> bool {
        let current = self.router.get_retained_messages(topic).await;
        match current.iter().find(|message| message.topic_name == topic) {
            Some(message) => message.payload[..] == *payload && message.qos == QoS::from(qos),
            None => payload.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;

    fn writer() -> (
        LocalRetainedWriter,
        Arc<MessageRouter>,
        Arc<RetainedStore>,
        Arc<RetainedEchoes>,
    ) {
        let router = Arc::new(MessageRouter::new());
        let store = Arc::new(RetainedStore::new(NodeId::validated(1).unwrap()));
        let echoes = Arc::new(RetainedEchoes::new());
        let writer =
            LocalRetainedWriter::new(Arc::clone(&router), Arc::clone(&store), Arc::clone(&echoes));
        (writer, router, store, echoes)
    }

    async fn broker_payload(router: &MessageRouter, topic: &str) -> Option<Vec<u8>> {
        router
            .get_retained_messages(topic)
            .await
            .into_iter()
            .find(|message| message.topic_name == topic)
            .map(|message| message.payload.to_vec())
    }

    #[test]
    fn an_echo_is_consumed_once_and_only_for_its_own_payload() {
        let echoes = RetainedEchoes::new();
        echoes.record("t", b"v1");
        assert!(!echoes.consume("t", b"v2"));
        assert!(!echoes.consume("other", b"v1"));
        assert!(echoes.consume("t", b"v1"));
        assert!(!echoes.consume("t", b"v1"));
        assert_eq!(echoes.pending_count(), 0);
    }

    #[test]
    fn each_recorded_echo_matches_one_change() {
        let echoes = RetainedEchoes::new();
        echoes.record("t", b"");
        echoes.record("t", b"");
        assert!(echoes.consume("t", b""));
        assert!(echoes.consume("t", b""));
        assert!(!echoes.consume("t", b""));
    }

    #[test]
    fn prune_drops_echoes_older_than_the_limit() {
        let echoes = RetainedEchoes::new();
        echoes.record("t", b"v1");
        assert_eq!(echoes.prune(Duration::from_secs(60)), 0);
        assert_eq!(echoes.prune(Duration::ZERO), 1);
        assert_eq!(echoes.pending_count(), 0);
    }

    #[tokio::test]
    async fn refresh_writes_the_stored_value_and_records_its_echo() {
        let (writer, router, store, echoes) = writer();
        store.set("t", 1, b"v1", 0);

        writer.refresh("t").await;

        assert_eq!(broker_payload(&router, "t").await, Some(b"v1".to_vec()));
        assert!(echoes.consume("t", b"v1"));
    }

    #[tokio::test]
    async fn refresh_reads_the_store_when_applied_not_when_queued() {
        let (writer, router, store, _echoes) = writer();
        store.set("t", 1, b"v1", 0);
        store.set("t", 1, b"v2", 0);

        writer.refresh("t").await;

        assert_eq!(broker_payload(&router, "t").await, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn refresh_of_a_deleted_topic_clears_the_broker() {
        let (writer, router, store, echoes) = writer();
        store.set("t", 1, b"v1", 0);
        writer.refresh("t").await;
        assert!(echoes.consume("t", b"v1"));
        store.set("t", 1, b"", 0);

        writer.refresh("t").await;

        assert_eq!(broker_payload(&router, "t").await, None);
        assert!(echoes.consume("t", b""));
    }

    #[tokio::test]
    async fn a_value_the_broker_already_holds_is_not_written_again() {
        let (writer, _router, store, echoes) = writer();
        store.set("t", 1, b"v1", 0);
        writer.refresh("t").await;
        assert!(echoes.consume("t", b"v1"));

        writer.refresh("t").await;
        writer.write("t", b"v1", 1).await;

        assert_eq!(echoes.pending_count(), 0);
    }

    #[tokio::test]
    async fn refresh_of_a_topic_absent_everywhere_writes_nothing() {
        let (writer, router, _store, echoes) = writer();

        writer.refresh("t").await;

        assert_eq!(broker_payload(&router, "t").await, None);
        assert_eq!(echoes.pending_count(), 0);
    }
}
