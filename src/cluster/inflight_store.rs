use super::{NodeId, PartitionId, session_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct InflightMessage {
    pub version: u8,
    pub packet_id: u16,
    pub qos: u8,
    pub attempts: u8,
    pub last_attempt: u64,
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub payload_len: u32,
    #[FromField(payload_len)]
    pub payload: Vec<u8>,
}

impl InflightMessage {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        qos: u8,
        timestamp: u64,
    ) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            packet_id,
            qos,
            attempts: 1,
            last_attempt: timestamp,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            payload_len: payload.len() as u32,
            payload: payload.to_vec(),
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    pub fn increment_attempts(&mut self, timestamp: u64) {
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt = timestamp;
    }

    #[must_use]
    pub fn should_retry(&self, now: u64, max_attempts: u8) -> bool {
        if self.attempts >= max_attempts {
            return false;
        }
        let backoff_ms = self.backoff_ms();
        now >= self.last_attempt + backoff_ms
    }

    fn backoff_ms(&self) -> u64 {
        let base: u64 = 1000;
        let max_backoff: u64 = 300_000;
        let backoff = base.saturating_mul(1 << self.attempts.min(10));
        backoff.min(max_backoff)
    }
}

#[must_use]
pub fn inflight_key(client_id: &str, packet_id: u16) -> String {
    let partition = session_partition(client_id);
    format!(
        "_mqtt_inflight/p{}/clients/{}/pending/{}",
        partition.get(),
        client_id,
        packet_id
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightStoreError {
    NotFound,
    AlreadyExists,
    MaxRetriesExceeded,
    SerializationError,
}

impl std::fmt::Display for InflightStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "inflight message not found"),
            Self::AlreadyExists => write!(f, "inflight message already exists"),
            Self::MaxRetriesExceeded => write!(f, "max retries exceeded"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for InflightStoreError {}

type MessageKey = (String, u16);

pub struct InflightStore {
    node_id: NodeId,
    max_attempts: u8,
    messages: RwLock<HashMap<MessageKey, InflightMessage>>,
}

impl InflightStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            max_attempts: 10,
            messages: RwLock::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_max_attempts(node_id: NodeId, max_attempts: u8) -> Self {
        Self {
            node_id,
            max_attempts,
            messages: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a message with the same client/packet ID exists.
    pub fn add(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        qos: u8,
        timestamp: u64,
    ) -> Result<(), InflightStoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut messages = self.messages.write().unwrap();

        if messages.contains_key(&key) {
            return Err(InflightStoreError::AlreadyExists);
        }

        let msg = InflightMessage::create(client_id, packet_id, topic, payload, qos, timestamp);
        messages.insert(key, msg);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, client_id: &str, packet_id: u16) -> Option<InflightMessage> {
        let key = (client_id.to_string(), packet_id);
        self.messages.read().unwrap().get(&key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no message exists.
    pub fn acknowledge(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<InflightMessage, InflightStoreError> {
        let key = (client_id.to_string(), packet_id);
        self.messages
            .write()
            .unwrap()
            .remove(&key)
            .ok_or(InflightStoreError::NotFound)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no message exists, or `MaxRetriesExceeded` if attempts exhausted.
    pub fn mark_retry(
        &self,
        client_id: &str,
        packet_id: u16,
        timestamp: u64,
    ) -> Result<u8, InflightStoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut messages = self.messages.write().unwrap();

        let msg = messages.get_mut(&key).ok_or(InflightStoreError::NotFound)?;

        if msg.attempts >= self.max_attempts {
            return Err(InflightStoreError::MaxRetriesExceeded);
        }

        msg.increment_attempts(timestamp);
        Ok(msg.attempts)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn pending_for_client(&self, client_id: &str) -> Vec<InflightMessage> {
        self.messages
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| cid == client_id)
            .map(|(_, msg)| msg.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn messages_due_for_retry(&self, now: u64) -> Vec<InflightMessage> {
        self.messages
            .read()
            .unwrap()
            .values()
            .filter(|msg| msg.should_retry(now, self.max_attempts))
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn expired_messages(&self) -> Vec<InflightMessage> {
        self.messages
            .read()
            .unwrap()
            .values()
            .filter(|msg| msg.attempts >= self.max_attempts)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clear_client(&self, client_id: &str) -> usize {
        let mut messages = self.messages.write().unwrap();
        let before = messages.len();
        messages.retain(|(cid, _), _| cid != client_id);
        before - messages.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn messages_on_partition(&self, partition: PartitionId) -> Vec<InflightMessage> {
        self.messages
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| session_partition(cid) == partition)
            .map(|(_, msg)| msg.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.messages.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize(msg: &InflightMessage) -> Vec<u8> {
        msg.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<InflightMessage> {
        InflightMessage::try_from_be_bytes(bytes)
            .ok()
            .map(|(m, _)| m)
    }
}

impl std::fmt::Debug for InflightStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightStore")
            .field("node_id", &self.node_id)
            .field("max_attempts", &self.max_attempts)
            .field("count", &self.count())
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
    fn inflight_message_bebytes_roundtrip() {
        let msg = InflightMessage::create("client1", 100, "test/topic", b"payload", 1, 1000);
        let bytes = InflightStore::serialize(&msg);
        let parsed = InflightStore::deserialize(&bytes).unwrap();

        assert_eq!(msg.client_id, parsed.client_id);
        assert_eq!(msg.packet_id, parsed.packet_id);
        assert_eq!(msg.topic, parsed.topic);
        assert_eq!(msg.payload, parsed.payload);
        assert_eq!(msg.qos, parsed.qos);
    }

    #[test]
    fn inflight_key_format() {
        let key = inflight_key("client1", 100);
        assert!(key.starts_with("_mqtt_inflight/p"));
        assert!(key.contains("/clients/client1/pending/100"));
    }

    #[test]
    fn add_and_acknowledge() {
        let store = InflightStore::new(node(1));

        store
            .add("client1", 100, "topic", b"data", 1, 1000)
            .unwrap();

        let msg = store.get("client1", 100).unwrap();
        assert_eq!(msg.packet_id, 100);
        assert_eq!(msg.attempts, 1);

        let acked = store.acknowledge("client1", 100).unwrap();
        assert_eq!(acked.topic_str(), "topic");

        assert!(store.get("client1", 100).is_none());
    }

    #[test]
    fn duplicate_rejected() {
        let store = InflightStore::new(node(1));

        store
            .add("client1", 100, "topic", b"data", 1, 1000)
            .unwrap();

        let result = store.add("client1", 100, "topic", b"other", 1, 2000);
        assert_eq!(result, Err(InflightStoreError::AlreadyExists));
    }

    #[test]
    fn retry_tracking() {
        let store = InflightStore::with_max_attempts(node(1), 3);

        store
            .add("client1", 100, "topic", b"data", 1, 1000)
            .unwrap();

        let attempts = store.mark_retry("client1", 100, 2000).unwrap();
        assert_eq!(attempts, 2);

        let attempts = store.mark_retry("client1", 100, 3000).unwrap();
        assert_eq!(attempts, 3);

        let result = store.mark_retry("client1", 100, 4000);
        assert_eq!(result, Err(InflightStoreError::MaxRetriesExceeded));
    }

    #[test]
    fn backoff_calculation() {
        let mut msg = InflightMessage::create("c", 1, "t", b"", 1, 0);

        assert!(!msg.should_retry(500, 10));
        assert!(msg.should_retry(2000, 10));

        msg.increment_attempts(2000);
        assert!(!msg.should_retry(3000, 10));
        assert!(msg.should_retry(6000, 10));
    }

    #[test]
    fn messages_due_for_retry() {
        let store = InflightStore::new(node(1));

        store.add("c1", 1, "t1", b"a", 1, 0).unwrap();
        store.add("c1", 2, "t2", b"b", 1, 1000).unwrap();
        store.add("c1", 3, "t3", b"c", 1, 2000).unwrap();

        let due = store.messages_due_for_retry(3000);
        assert_eq!(due.len(), 2);

        let due = store.messages_due_for_retry(10000);
        assert_eq!(due.len(), 3);
    }

    #[test]
    fn clear_client() {
        let store = InflightStore::new(node(1));

        store.add("c1", 1, "t", b"a", 1, 0).unwrap();
        store.add("c1", 2, "t", b"b", 1, 0).unwrap();
        store.add("c2", 1, "t", b"c", 1, 0).unwrap();

        let cleared = store.clear_client("c1");
        assert_eq!(cleared, 2);
        assert_eq!(store.count(), 1);
    }
}
