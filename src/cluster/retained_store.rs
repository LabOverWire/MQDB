use super::{NodeId, PartitionId, topic_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct RetainedMessage {
    pub version: u8,
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub qos: u8,
    pub timestamp_ms: u64,
    pub payload_len: u32,
    #[FromField(payload_len)]
    pub payload: Vec<u8>,
}

impl RetainedMessage {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(topic: &str, qos: u8, payload: &[u8], timestamp_ms: u64) -> Self {
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            qos,
            timestamp_ms,
            payload_len: payload.len() as u32,
            payload: payload.to_vec(),
        }
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    #[must_use]
    pub fn is_empty_payload(&self) -> bool {
        self.payload.is_empty()
    }
}

#[must_use]
pub fn retained_message_key(topic: &str) -> String {
    let partition = topic_partition(topic);
    format!("_mqtt_retained/p{}/topics/{}", partition.get(), topic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedStoreError {
    NotFound,
    SerializationError,
}

impl std::fmt::Display for RetainedStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "retained message not found"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for RetainedStoreError {}

pub struct RetainedStore {
    node_id: NodeId,
    messages: RwLock<HashMap<String, RetainedMessage>>,
}

impl RetainedStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            messages: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn set(&self, topic: &str, qos: u8, payload: &[u8], timestamp_ms: u64) {
        let mut messages = self.messages.write().unwrap();
        if payload.is_empty() {
            messages.remove(topic);
        } else {
            let msg = RetainedMessage::create(topic, qos, payload, timestamp_ms);
            messages.insert(topic.to_string(), msg);
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, topic: &str) -> Option<RetainedMessage> {
        self.messages.read().unwrap().get(topic).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn remove(&self, topic: &str) -> Option<RetainedMessage> {
        self.messages.write().unwrap().remove(topic)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn topics_on_partition(&self, partition: PartitionId) -> Vec<String> {
        self.messages
            .read()
            .unwrap()
            .keys()
            .filter(|t| topic_partition(t) == partition)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn get_matching_exact(&self, topic: &str) -> Option<RetainedMessage> {
        self.get(topic)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn message_count(&self) -> usize {
        self.messages.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn all_topics(&self) -> Vec<String> {
        self.messages.read().unwrap().keys().cloned().collect()
    }

    #[must_use]
    pub fn serialize(msg: &RetainedMessage) -> Vec<u8> {
        msg.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<RetainedMessage> {
        RetainedMessage::try_from_be_bytes(bytes)
            .ok()
            .map(|(m, _)| m)
    }
}

impl std::fmt::Debug for RetainedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedStore")
            .field("node_id", &self.node_id)
            .field("message_count", &self.message_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn retained_message_bebytes_roundtrip() {
        let msg = RetainedMessage::create("sensors/temp", 1, b"25.5", 1000);
        let bytes = RetainedStore::serialize(&msg);
        let parsed = RetainedStore::deserialize(&bytes).unwrap();

        assert_eq!(msg.topic, parsed.topic);
        assert_eq!(msg.qos, parsed.qos);
        assert_eq!(msg.payload, parsed.payload);
        assert_eq!(msg.timestamp_ms, parsed.timestamp_ms);
    }

    #[test]
    fn retained_message_key_format() {
        let key = retained_message_key("sensors/temp");
        assert!(key.starts_with("_mqtt_retained/p"));
        assert!(key.ends_with("/topics/sensors/temp"));
    }

    #[test]
    fn retained_store_set_get() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        let msg = store.get("sensors/temp").unwrap();

        assert_eq!(msg.topic_str(), "sensors/temp");
        assert_eq!(msg.payload, b"25.5");
    }

    #[test]
    fn retained_store_empty_payload_deletes() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        assert!(store.get("sensors/temp").is_some());

        store.set("sensors/temp", 1, b"", 2000);
        assert!(store.get("sensors/temp").is_none());
    }

    #[test]
    fn retained_store_update_replaces() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        store.set("sensors/temp", 2, b"30.0", 2000);

        let msg = store.get("sensors/temp").unwrap();
        assert_eq!(msg.payload, b"30.0");
        assert_eq!(msg.qos, 2);
        assert_eq!(msg.timestamp_ms, 2000);
    }

    #[test]
    fn retained_store_multiple_topics() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        store.set("sensors/humidity", 1, b"65%", 1000);
        store.set("actuators/fan", 0, b"on", 1000);

        assert_eq!(store.message_count(), 3);

        let topics = store.all_topics();
        assert_eq!(topics.len(), 3);
    }
}
