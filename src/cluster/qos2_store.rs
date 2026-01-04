use super::protocol::Operation;
use super::{NodeId, PartitionId, session_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos2Direction {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos2Phase {
    PubrecSent,
    PubrelReceived,
    PubcompSent,
    PublishSent,
    PubrelSent,
}

impl Qos2Phase {
    fn from_byte(direction: Qos2Direction, byte: u8) -> Option<Self> {
        match (direction, byte) {
            (Qos2Direction::Inbound, 1) => Some(Self::PubrecSent),
            (Qos2Direction::Inbound, 2) => Some(Self::PubrelReceived),
            (Qos2Direction::Inbound, 3) => Some(Self::PubcompSent),
            (Qos2Direction::Outbound, 1) => Some(Self::PublishSent),
            (Qos2Direction::Outbound, 2) => Some(Self::PubrelSent),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct Qos2State {
    pub version: u8,
    pub direction: u8,
    pub state: u8,
    pub packet_id: u16,
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub payload_len: u32,
    #[FromField(payload_len)]
    pub payload: Vec<u8>,
    pub created_at: u64,
}

impl Qos2State {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create_inbound(
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        created_at: u64,
    ) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            direction: 0,
            state: 1,
            packet_id,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            payload_len: payload.len() as u32,
            payload: payload.to_vec(),
            created_at,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create_outbound(
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        created_at: u64,
    ) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            direction: 1,
            state: 1,
            packet_id,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            payload_len: payload.len() as u32,
            payload: payload.to_vec(),
            created_at,
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

    #[must_use]
    pub fn direction_enum(&self) -> Qos2Direction {
        if self.direction == 0 {
            Qos2Direction::Inbound
        } else {
            Qos2Direction::Outbound
        }
    }

    #[must_use]
    pub fn phase(&self) -> Option<Qos2Phase> {
        Qos2Phase::from_byte(self.direction_enum(), self.state)
    }

    pub fn advance_phase(&mut self) {
        self.state += 1;
    }
}

#[must_use]
pub fn qos2_state_key(client_id: &str, packet_id: u16) -> String {
    let partition = session_partition(client_id);
    format!(
        "_mqtt_qos2/p{}/clients/{}/inflight/{}",
        partition.get(),
        client_id,
        packet_id
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos2StoreError {
    NotFound,
    AlreadyExists,
    InvalidState,
    SerializationError,
}

impl std::fmt::Display for Qos2StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "QoS 2 state not found"),
            Self::AlreadyExists => write!(f, "QoS 2 state already exists"),
            Self::InvalidState => write!(f, "invalid QoS 2 state"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for Qos2StoreError {}

type StateKey = (String, u16);

pub struct Qos2Store {
    node_id: NodeId,
    states: RwLock<HashMap<StateKey, Qos2State>>,
}

impl Qos2Store {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            states: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a state with the same client/packet ID exists.
    pub fn start_inbound(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        if states.contains_key(&key) {
            return Err(Qos2StoreError::AlreadyExists);
        }

        let state = Qos2State::create_inbound(client_id, packet_id, topic, payload, timestamp);
        states.insert(key, state);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a state with the same client/packet ID exists.
    pub fn start_outbound(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        if states.contains_key(&key) {
            return Err(Qos2StoreError::AlreadyExists);
        }

        let state = Qos2State::create_outbound(client_id, packet_id, topic, payload, timestamp);
        states.insert(key, state);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, client_id: &str, packet_id: u16) -> Option<Qos2State> {
        let key = (client_id.to_string(), packet_id);
        self.states.read().unwrap().get(&key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no state exists, or `InvalidState` if the phase is invalid.
    pub fn advance(&self, client_id: &str, packet_id: u16) -> Result<Qos2Phase, Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        let state = states.get_mut(&key).ok_or(Qos2StoreError::NotFound)?;
        state.advance_phase();

        state.phase().ok_or(Qos2StoreError::InvalidState)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no state exists.
    pub fn complete(&self, client_id: &str, packet_id: u16) -> Result<Qos2State, Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        self.states
            .write()
            .unwrap()
            .remove(&key)
            .ok_or(Qos2StoreError::NotFound)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn pending_for_client(&self, client_id: &str) -> Vec<Qos2State> {
        self.states
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| cid == client_id)
            .map(|(_, state)| state.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn pending_inbound(&self, client_id: &str) -> Vec<Qos2State> {
        self.states
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), state)| {
                cid == client_id && state.direction_enum() == Qos2Direction::Inbound
            })
            .map(|(_, state)| state.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn pending_outbound(&self, client_id: &str) -> Vec<Qos2State> {
        self.states
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), state)| {
                cid == client_id && state.direction_enum() == Qos2Direction::Outbound
            })
            .map(|(_, state)| state.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clear_client(&self, client_id: &str) -> usize {
        let mut states = self.states.write().unwrap();
        let before = states.len();
        states.retain(|(cid, _), _| cid != client_id);
        before - states.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clear_client_with_data(&self, client_id: &str) -> Vec<(u16, Vec<u8>)> {
        let mut states = self.states.write().unwrap();
        let removed: Vec<_> = states
            .iter()
            .filter(|((cid, _), _)| cid == client_id)
            .map(|((_, packet_id), state)| (*packet_id, Self::serialize(state)))
            .collect();
        states.retain(|(cid, _), _| cid != client_id);
        removed
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn states_on_partition(&self, partition: PartitionId) -> Vec<Qos2State> {
        self.states
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| session_partition(cid) == partition)
            .map(|(_, state)| state.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.states.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a state with the same client/packet ID exists.
    pub fn start_inbound_with_data(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(Qos2State, Vec<u8>), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        if states.contains_key(&key) {
            return Err(Qos2StoreError::AlreadyExists);
        }

        let state = Qos2State::create_inbound(client_id, packet_id, topic, payload, timestamp);
        let data = Self::serialize(&state);
        states.insert(key, state.clone());
        Ok((state, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a state with the same client/packet ID exists.
    pub fn start_outbound_with_data(
        &self,
        client_id: &str,
        packet_id: u16,
        topic: &str,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<(Qos2State, Vec<u8>), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        if states.contains_key(&key) {
            return Err(Qos2StoreError::AlreadyExists);
        }

        let state = Qos2State::create_outbound(client_id, packet_id, topic, payload, timestamp);
        let data = Self::serialize(&state);
        states.insert(key, state.clone());
        Ok((state, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no state exists, or `InvalidState` if the phase is invalid.
    pub fn advance_with_data(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<(Qos2Phase, Vec<u8>), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let mut states = self.states.write().unwrap();

        let state = states.get_mut(&key).ok_or(Qos2StoreError::NotFound)?;
        state.advance_phase();

        let phase = state.phase().ok_or(Qos2StoreError::InvalidState)?;
        let data = Self::serialize(state);
        Ok((phase, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if no state exists.
    pub fn complete_with_data(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<(Qos2State, Vec<u8>), Qos2StoreError> {
        let key = (client_id.to_string(), packet_id);
        let state = self
            .states
            .write()
            .unwrap()
            .remove(&key)
            .ok_or(Qos2StoreError::NotFound)?;
        let data = Self::serialize(&state);
        Ok((state, data))
    }

    #[must_use]
    pub fn serialize(state: &Qos2State) -> Vec<u8> {
        state.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<Qos2State> {
        Qos2State::try_from_be_bytes(bytes).ok().map(|(s, _)| s)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns an error if ID parsing or deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), Qos2StoreError> {
        let parts: Vec<&str> = id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(Qos2StoreError::InvalidState);
        }
        let client_id = parts[0];
        let packet_id: u16 = parts[1].parse().map_err(|_| Qos2StoreError::InvalidState)?;

        match operation {
            Operation::Insert | Operation::Update => {
                let state = Self::deserialize(data).ok_or(Qos2StoreError::SerializationError)?;
                let key = (client_id.to_string(), packet_id);
                let mut states = self.states.write().unwrap();
                states.insert(key, state);
                Ok(())
            }
            Operation::Delete => {
                let key = (client_id.to_string(), packet_id);
                let mut states = self.states.write().unwrap();
                states.remove(&key);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let states = self.states.read().unwrap();
        let partition_states: Vec<_> = states
            .iter()
            .filter(|((cid, _), _)| session_partition(cid) == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_states.len() as u32).to_be_bytes());

        for ((client_id, packet_id), state) in partition_states {
            let id = format!("{client_id}:{packet_id}");
            let id_bytes = id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = state.to_be_bytes();
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
    pub fn import_states(&self, data: &[u8]) -> Result<usize, Qos2StoreError> {
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
                .map_err(|_| Qos2StoreError::SerializationError)?;
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
            let state_data = &data[offset..offset + data_len];
            offset += data_len;

            let parts: Vec<&str> = id.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let client_id = parts[0];
            let packet_id: u16 = match parts[1].parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            if let Some(state) = Self::deserialize(state_data) {
                let key = (client_id.to_string(), packet_id);
                self.states.write().unwrap().insert(key, state);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut states = self.states.write().unwrap();
        let before = states.len();
        states.retain(|(cid, _), _| session_partition(cid) != partition);
        before - states.len()
    }
}

impl std::fmt::Debug for Qos2Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Qos2Store")
            .field("node_id", &self.node_id)
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
    fn qos2_state_bebytes_roundtrip() {
        let state = Qos2State::create_inbound("client1", 100, "test/topic", b"hello", 1000);
        let bytes = Qos2Store::serialize(&state);
        let parsed = Qos2Store::deserialize(&bytes).unwrap();

        assert_eq!(state.client_id, parsed.client_id);
        assert_eq!(state.packet_id, parsed.packet_id);
        assert_eq!(state.topic, parsed.topic);
        assert_eq!(state.payload, parsed.payload);
    }

    #[test]
    fn qos2_state_key_format() {
        let key = qos2_state_key("client1", 100);
        assert!(key.starts_with("_mqtt_qos2/p"));
        assert!(key.contains("/clients/client1/inflight/100"));
    }

    #[test]
    fn inbound_flow() {
        let store = Qos2Store::new(node(1));

        store
            .start_inbound("client1", 100, "test/topic", b"payload", 1000)
            .unwrap();

        let state = store.get("client1", 100).unwrap();
        assert_eq!(state.direction_enum(), Qos2Direction::Inbound);
        assert_eq!(state.phase(), Some(Qos2Phase::PubrecSent));

        let phase = store.advance("client1", 100).unwrap();
        assert_eq!(phase, Qos2Phase::PubrelReceived);

        let completed = store.complete("client1", 100).unwrap();
        assert_eq!(completed.topic_str(), "test/topic");

        assert!(store.get("client1", 100).is_none());
    }

    #[test]
    fn outbound_flow() {
        let store = Qos2Store::new(node(1));

        store
            .start_outbound("client1", 200, "out/topic", b"data", 2000)
            .unwrap();

        let state = store.get("client1", 200).unwrap();
        assert_eq!(state.direction_enum(), Qos2Direction::Outbound);
        assert_eq!(state.phase(), Some(Qos2Phase::PublishSent));

        let phase = store.advance("client1", 200).unwrap();
        assert_eq!(phase, Qos2Phase::PubrelSent);

        store.complete("client1", 200).unwrap();
    }

    #[test]
    fn duplicate_rejected() {
        let store = Qos2Store::new(node(1));

        store
            .start_inbound("client1", 100, "topic", b"data", 1000)
            .unwrap();

        let result = store.start_inbound("client1", 100, "topic", b"other", 2000);
        assert_eq!(result, Err(Qos2StoreError::AlreadyExists));
    }

    #[test]
    fn pending_for_client() {
        let store = Qos2Store::new(node(1));

        store
            .start_inbound("client1", 100, "topic1", b"a", 1000)
            .unwrap();
        store
            .start_outbound("client1", 200, "topic2", b"b", 1000)
            .unwrap();
        store
            .start_inbound("client2", 100, "topic3", b"c", 1000)
            .unwrap();

        let pending = store.pending_for_client("client1");
        assert_eq!(pending.len(), 2);

        let inbound = store.pending_inbound("client1");
        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].packet_id, 100);

        let outbound = store.pending_outbound("client1");
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].packet_id, 200);
    }

    #[test]
    fn clear_client() {
        let store = Qos2Store::new(node(1));

        store
            .start_inbound("client1", 100, "t1", b"a", 1000)
            .unwrap();
        store
            .start_inbound("client1", 101, "t2", b"b", 1000)
            .unwrap();
        store
            .start_inbound("client2", 100, "t3", b"c", 1000)
            .unwrap();

        assert_eq!(store.count(), 3);

        let cleared = store.clear_client("client1");
        assert_eq!(cleared, 2);
        assert_eq!(store.count(), 1);
    }
}
