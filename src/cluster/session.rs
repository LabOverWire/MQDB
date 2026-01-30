use super::protocol::Operation;
use super::{NUM_PARTITIONS, NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct SessionData {
    pub version: u8,
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub session_partition: u16,
    pub clean_session: u8,
    pub connected: u8,
    pub connected_node: u16,
    pub last_seen: u64,
    pub session_expiry_interval: u32,
    pub mqtt_sub_version: u64,
    pub has_will: u8,
    pub lwt_published: u8,
    pub lwt_token_present: u8,
    pub lwt_token: [u8; 16],
    pub will_qos: u8,
    pub will_retain: u8,
    pub will_topic_len: u16,
    #[FromField(will_topic_len)]
    pub will_topic: Vec<u8>,
    pub will_payload_len: u32,
    #[FromField(will_payload_len)]
    pub will_payload: Vec<u8>,
}

impl SessionData {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(client_id: &str, node_id: NodeId) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        let partition = session_partition(client_id);
        Self {
            version: 2,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            session_partition: partition.get(),
            clean_session: 0,
            connected: 1,
            connected_node: node_id.get(),
            last_seen: 0,
            session_expiry_interval: 0,
            mqtt_sub_version: 0,
            has_will: 0,
            lwt_published: 0,
            lwt_token_present: 0,
            lwt_token: [0u8; 16],
            will_qos: 0,
            will_retain: 0,
            will_topic_len: 0,
            will_topic: Vec::new(),
            will_payload_len: 0,
            will_payload: Vec::new(),
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        super::store_utils::bytes_to_str(&self.client_id)
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        PartitionId::new(self.session_partition).unwrap_or(PartitionId::ZERO)
    }

    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected != 0
    }

    pub fn set_connected(&mut self, connected: bool, node: NodeId, timestamp: u64) {
        self.connected = u8::from(connected);
        self.connected_node = node.get();
        self.last_seen = timestamp;
    }

    pub fn set_clean_session(&mut self, clean: bool) {
        self.clean_session = u8::from(clean);
    }

    #[must_use]
    pub fn is_clean_session(&self) -> bool {
        self.clean_session != 0
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn set_will(&mut self, qos: u8, retain: bool, topic: &str, payload: &[u8]) {
        self.has_will = 1;
        self.lwt_published = 0;
        self.will_qos = qos;
        self.will_retain = u8::from(retain);
        let topic_bytes = topic.as_bytes().to_vec();
        self.will_topic_len = topic_bytes.len() as u16;
        self.will_topic = topic_bytes;
        self.will_payload_len = payload.len() as u32;
        self.will_payload = payload.to_vec();
    }

    pub fn clear_will(&mut self) {
        self.has_will = 0;
        self.lwt_published = 0;
        self.lwt_token_present = 0;
        self.lwt_token = [0u8; 16];
        self.will_qos = 0;
        self.will_retain = 0;
        self.will_topic_len = 0;
        self.will_topic.clear();
        self.will_payload_len = 0;
        self.will_payload.clear();
    }

    pub fn mark_lwt_published(&mut self) {
        self.lwt_published = 1;
    }

    #[must_use]
    pub fn has_pending_lwt(&self) -> bool {
        self.has_will != 0 && self.lwt_published == 0
    }

    pub fn set_lwt_token(&mut self, token: [u8; 16]) {
        self.lwt_token_present = 1;
        self.lwt_token = token;
    }

    #[must_use]
    pub fn get_lwt_token(&self) -> Option<[u8; 16]> {
        if self.lwt_token_present != 0 {
            Some(self.lwt_token)
        } else {
            None
        }
    }

    #[must_use]
    pub fn has_lwt_token(&self) -> bool {
        self.lwt_token_present != 0
    }

    #[must_use]
    pub fn needs_lwt_publish(&self) -> bool {
        self.has_will != 0 && self.lwt_published == 0 && self.lwt_token_present == 0
    }

    #[must_use]
    pub fn lwt_in_progress(&self) -> bool {
        self.has_will != 0 && self.lwt_published == 0 && self.lwt_token_present != 0
    }

    #[must_use]
    pub fn is_expired(&self, now: u64) -> bool {
        if self.is_connected() {
            return false;
        }
        if self.session_expiry_interval == 0 {
            return false;
        }
        let expiry_ms = u64::from(self.session_expiry_interval) * 1000;
        now.saturating_sub(self.last_seen) > expiry_ms
    }

    pub fn set_session_expiry_interval(&mut self, interval_secs: u32) {
        self.session_expiry_interval = interval_secs;
    }
}

/// # Panics
/// Panics if the partition number is out of range.
#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub fn session_partition(client_id: &str) -> PartitionId {
    let hash = crc32fast::hash(client_id.as_bytes());
    let partition_num = (hash % u32::from(NUM_PARTITIONS)) as u16;
    PartitionId::new(partition_num).unwrap()
}

#[must_use]
pub fn session_key(client_id: &str) -> String {
    let partition = session_partition(client_id);
    format!("_sessions/p{}/{}", partition.get(), client_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    NotFound,
    AlreadyExists,
    InvalidPartition,
    SerializationError,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "session not found"),
            Self::AlreadyExists => write!(f, "session already exists"),
            Self::InvalidPartition => write!(f, "invalid partition"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for SessionError {}

pub struct SessionStore {
    node_id: NodeId,
    sessions: RwLock<HashMap<String, SessionData>>,
}

impl SessionStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a session with the same client ID exists.
    pub fn create_session(&self, client_id: &str) -> Result<SessionData, SessionError> {
        let mut sessions = self.sessions.write().unwrap();
        if sessions.contains_key(client_id) {
            return Err(SessionError::AlreadyExists);
        }

        let session = SessionData::create(client_id, self.node_id);
        sessions.insert(client_id.to_string(), session.clone());
        Ok(session)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_or_create(&self, client_id: &str) -> SessionData {
        let mut sessions = self.sessions.write().unwrap();
        sessions
            .entry(client_id.to_string())
            .or_insert_with(|| SessionData::create(client_id, self.node_id))
            .clone()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<SessionData> {
        self.sessions.read().unwrap().get(client_id).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the session does not exist.
    pub fn update<F>(&self, client_id: &str, f: F) -> Result<SessionData, SessionError>
    where
        F: FnOnce(&mut SessionData),
    {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(client_id).ok_or(SessionError::NotFound)?;
        f(session);
        Ok(session.clone())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn remove(&self, client_id: &str) -> Option<SessionData> {
        self.sessions.write().unwrap().remove(client_id)
    }

    /// # Errors
    /// Returns `NotFound` if the session does not exist.
    pub fn mark_connected(&self, client_id: &str, timestamp: u64) -> Result<(), SessionError> {
        self.update(client_id, |s| {
            s.set_connected(true, self.node_id, timestamp);
        })?;
        Ok(())
    }

    /// # Errors
    /// Returns `NotFound` if the session does not exist.
    pub fn mark_disconnected(&self, client_id: &str, timestamp: u64) -> Result<(), SessionError> {
        self.update(client_id, |s| {
            s.set_connected(false, self.node_id, timestamp);
        })?;
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn sessions_on_partition(&self, partition: PartitionId) -> Vec<SessionData> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.partition() == partition)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn connected_sessions(&self) -> Vec<SessionData> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.is_connected())
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn sessions_on_node(&self, node_id: NodeId) -> Vec<SessionData> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.connected_node == node_id.get())
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn sessions_with_pending_lwt(&self) -> Vec<SessionData> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.has_pending_lwt())
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a session with the same client ID exists.
    pub fn create_session_with_data(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, Vec<u8>), SessionError> {
        let session = self.create_session(client_id)?;
        let data = Self::serialize(&session);
        Ok((session, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the session does not exist.
    pub fn update_with_data<F>(
        &self,
        client_id: &str,
        f: F,
    ) -> Result<(SessionData, Vec<u8>), SessionError>
    where
        F: FnOnce(&mut SessionData),
    {
        let session = self.update(client_id, f)?;
        let data = Self::serialize(&session);
        Ok((session, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the session does not exist.
    pub fn remove_with_data(
        &self,
        client_id: &str,
    ) -> Result<(SessionData, Vec<u8>), SessionError> {
        let session = self.remove(client_id).ok_or(SessionError::NotFound)?;
        let data = Self::serialize(&session);
        Ok((session, data))
    }

    #[must_use]
    pub fn serialize(session: &SessionData) -> Vec<u8> {
        super::store_utils::serialize(session)
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<SessionData> {
        super::store_utils::deserialize(bytes)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if deserialization fails.
    pub fn apply_replicated(&self, operation: Operation, data: &[u8]) -> Result<(), SessionError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let session = Self::deserialize(data).ok_or(SessionError::SerializationError)?;
                let client_id = session.client_id_str().to_string();
                let mut sessions = self.sessions.write().unwrap();
                sessions.insert(client_id, session);
                Ok(())
            }
            Operation::Delete => {
                let session = Self::deserialize(data).ok_or(SessionError::SerializationError)?;
                let client_id = session.client_id_str().to_string();
                let mut sessions = self.sessions.write().unwrap();
                sessions.remove(&client_id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if deserialization fails.
    pub fn apply_replicated_by_id(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), SessionError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let session = Self::deserialize(data).ok_or(SessionError::SerializationError)?;
                let mut sessions = self.sessions.write().unwrap();
                sessions.insert(id.to_string(), session);
                Ok(())
            }
            Operation::Delete => {
                let mut sessions = self.sessions.write().unwrap();
                sessions.remove(id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let sessions = self.sessions.read().unwrap();
        let partition_sessions: Vec<_> = sessions
            .iter()
            .filter(|(_, s)| s.partition() == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_sessions.len() as u32).to_be_bytes());

        for (client_id, session) in partition_sessions {
            let id_bytes = client_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = session.to_be_bytes();
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if UTF-8 parsing fails.
    pub fn import_sessions(&self, data: &[u8]) -> Result<usize, SessionError> {
        if data.len() < 4 {
            return Ok(0);
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut offset = 4;
        let mut imported = 0;

        for _ in 0..count {
            if offset + 2 > data.len() {
                break;
            }
            let id_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + id_len > data.len() {
                break;
            }
            let id = std::str::from_utf8(&data[offset..offset + id_len])
                .map_err(|_| SessionError::SerializationError)?;
            offset += id_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let session_data = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(session) = Self::deserialize(session_data) {
                self.sessions
                    .write()
                    .unwrap()
                    .insert(id.to_string(), session);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut sessions = self.sessions.write().unwrap();
        let before = sessions.len();
        sessions.retain(|_, s| s.partition() != partition);
        before - sessions.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn expired_sessions(&self, now: u64) -> Vec<SessionData> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.is_expired(now))
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired_sessions(&self, now: u64) -> Vec<SessionData> {
        let mut sessions = self.sessions.write().unwrap();
        let mut expired = Vec::new();

        sessions.retain(|_, session| {
            if session.is_expired(now) {
                expired.push(session.clone());
                false
            } else {
                true
            }
        });

        expired
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn query(
        &self,
        _filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<SessionData>, bool, Option<Vec<u8>>) {
        let sessions = self.sessions.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = sessions
            .iter()
            .filter(|(key, _)| start_key.is_none_or(|sk| key.as_str() > sk))
            .take(limit as usize + 1)
            .collect();

        results.sort_by_key(|(k, _)| k.as_str());
        let has_more = results.len() > limit as usize;
        if has_more {
            results.pop();
        }

        let next_cursor = if has_more {
            results.last().map(|(k, _)| k.as_bytes().to_vec())
        } else {
            None
        };

        let data = results.into_iter().map(|(_, v)| v.clone()).collect();
        (data, has_more, next_cursor)
    }
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("node_id", &self.node_id)
            .field("session_count", &self.session_count())
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
    fn session_partition_deterministic() {
        let p1 = session_partition("client1");
        let p2 = session_partition("client1");
        assert_eq!(p1, p2);
    }

    #[test]
    fn session_key_format() {
        let key = session_key("test_client");
        assert!(key.starts_with("_sessions/p"));
        assert!(key.ends_with("/test_client"));
    }

    #[test]
    fn session_data_bebytes_roundtrip() {
        let mut session = SessionData::create("client1", node(1));
        session.set_will(1, true, "test/topic", b"hello");

        let bytes = session.to_be_bytes();
        let (parsed, _) = SessionData::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(session.client_id, parsed.client_id);
        assert_eq!(session.will_qos, parsed.will_qos);
        assert_eq!(session.will_topic, parsed.will_topic);
        assert_eq!(session.will_payload, parsed.will_payload);
    }

    #[test]
    fn session_store_create_get() {
        let store = SessionStore::new(node(1));

        let session = store.create_session("client1").unwrap();
        assert_eq!(session.client_id_str(), "client1");
        assert!(session.is_connected());

        let fetched = store.get("client1").unwrap();
        assert_eq!(fetched.client_id, session.client_id);

        let result = store.create_session("client1");
        assert!(matches!(result, Err(SessionError::AlreadyExists)));
    }

    #[test]
    fn session_store_update() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();

        let updated = store
            .update("client1", |s| {
                s.set_will(2, false, "will/topic", b"goodbye");
            })
            .unwrap();

        assert_eq!(updated.will_qos, 2);
        assert_eq!(updated.will_retain, 0);
        assert_eq!(updated.will_topic, b"will/topic");
    }

    #[test]
    fn session_store_disconnect_reconnect() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();

        store.mark_disconnected("client1", 1000).unwrap();
        let session = store.get("client1").unwrap();
        assert!(!session.is_connected());
        assert_eq!(session.last_seen, 1000);

        store.mark_connected("client1", 2000).unwrap();
        let session = store.get("client1").unwrap();
        assert!(session.is_connected());
        assert_eq!(session.last_seen, 2000);
    }

    #[test]
    fn session_store_lwt_tracking() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();

        store
            .update("client1", |s| {
                s.set_will(1, false, "lwt/topic", b"client died");
            })
            .unwrap();

        let pending = store.sessions_with_pending_lwt();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].client_id_str(), "client1");

        store
            .update("client1", SessionData::mark_lwt_published)
            .unwrap();

        let pending = store.sessions_with_pending_lwt();
        assert!(pending.is_empty());
    }

    #[test]
    fn session_store_remove() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();

        let removed = store.remove("client1");
        assert!(removed.is_some());
        assert!(store.get("client1").is_none());
    }

    #[test]
    fn session_partition_query() {
        let store = SessionStore::new(node(1));

        for i in 0..10 {
            let client_id = format!("client{i}");
            store.create_session(&client_id).unwrap();
        }

        let partition = session_partition("client0");
        let on_partition = store.sessions_on_partition(partition);
        assert!(!on_partition.is_empty());
    }

    #[test]
    fn session_is_expired_connected_never_expires() {
        let mut session = SessionData::create("client1", node(1));
        session.session_expiry_interval = 60;
        session.last_seen = 0;
        assert!(!session.is_expired(1_000_000));
    }

    #[test]
    fn session_is_expired_zero_interval_never_expires() {
        let mut session = SessionData::create("client1", node(1));
        session.connected = 0;
        session.session_expiry_interval = 0;
        session.last_seen = 0;
        assert!(!session.is_expired(1_000_000));
    }

    #[test]
    fn session_is_expired_within_interval_not_expired() {
        let mut session = SessionData::create("client1", node(1));
        session.connected = 0;
        session.session_expiry_interval = 60;
        session.last_seen = 1000;
        assert!(!session.is_expired(30_000));
    }

    #[test]
    fn session_is_expired_beyond_interval_expires() {
        let mut session = SessionData::create("client1", node(1));
        session.connected = 0;
        session.session_expiry_interval = 60;
        session.last_seen = 1000;
        assert!(session.is_expired(100_000));
    }

    #[test]
    fn cleanup_expired_sessions_removes_expired() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();
        store.create_session("client2").unwrap();
        store.create_session("client3").unwrap();

        store
            .update("client1", |s| {
                s.set_connected(false, node(1), 1000);
                s.set_session_expiry_interval(60);
            })
            .unwrap();

        store
            .update("client2", |s| {
                s.set_connected(false, node(1), 50_000);
                s.set_session_expiry_interval(60);
            })
            .unwrap();

        let expired = store.cleanup_expired_sessions(100_000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].client_id_str(), "client1");

        assert!(store.get("client1").is_none());
        assert!(store.get("client2").is_some());
        assert!(store.get("client3").is_some());
    }

    #[test]
    fn expired_sessions_returns_list_without_removing() {
        let store = SessionStore::new(node(1));
        store.create_session("client1").unwrap();

        store
            .update("client1", |s| {
                s.set_connected(false, node(1), 1000);
                s.set_session_expiry_interval(60);
            })
            .unwrap();

        let expired = store.expired_sessions(100_000);
        assert_eq!(expired.len(), 1);

        assert!(store.get("client1").is_some());
    }
}
