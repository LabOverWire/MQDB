// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use super::protocol::Operation;
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct ClientLocationEntry {
    pub version: u8,
    pub timestamp_ms: u64,
    pub client_id_len: u16,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub connected_node: u16,
}

impl ClientLocationEntry {
    #[must_use]
    pub fn create(client_id: &str, connected_node: NodeId) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let client_bytes = client_id.as_bytes().to_vec();
        Self {
            version: 2,
            timestamp_ms,
            client_id_len: u16::try_from(client_bytes.len()).unwrap_or(u16::MAX),
            client_id: client_bytes,
            connected_node: connected_node.get(),
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn node(&self) -> Option<NodeId> {
        NodeId::validated(self.connected_node)
    }
}

#[must_use]
pub fn client_location_key(client_id: &str) -> String {
    format!("_client_loc/{client_id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientLocationError {
    NotFound,
    SerializationError,
}

impl std::fmt::Display for ClientLocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "client location not found"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for ClientLocationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    pub node: NodeId,
    pub since_ms: u64,
}

pub struct ClientLocationStore {
    entries: RwLock<HashMap<String, Located>>,
}

impl ClientLocationStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_if_newer(&self, client_id: &str, located: Located) -> bool {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        let newer = entries
            .get(client_id)
            .is_none_or(|current| current.since_ms <= located.since_ms);
        if newer {
            entries.insert(client_id.to_string(), located);
        }
        newer
    }

    pub fn remove_if_current(&self, client_id: &str, gone: Located) -> bool {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        let current = entries
            .get(client_id)
            .is_some_and(|located| located.node == gone.node && located.since_ms <= gone.since_ms);
        if current {
            entries.remove(client_id);
        }
        current
    }

    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<NodeId> {
        self.located(client_id).map(|located| located.node)
    }

    #[must_use]
    pub fn located(&self, client_id: &str) -> Option<Located> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .copied()
    }

    #[must_use]
    pub fn clients_on(&self, node: NodeId) -> HashMap<String, u64> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, located)| located.node == node)
            .map(|(client_id, located)| (client_id.clone(), located.since_ms))
            .collect()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.len()
    }

    /// # Errors
    /// Returns `SerializationError` if the entry bytes do not decode.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        data: &[u8],
    ) -> Result<(), ClientLocationError> {
        if data.is_empty() {
            return Ok(());
        }
        let (entry, _) = ClientLocationEntry::try_from_be_bytes(data)
            .map_err(|_| ClientLocationError::SerializationError)?;
        if let Some(node) = entry.node() {
            let located = Located {
                node,
                since_ms: entry.timestamp_ms,
            };
            match operation {
                Operation::Insert | Operation::Update => {
                    self.set_if_newer(entry.client_id_str(), located);
                }
                Operation::Delete => {
                    self.remove_if_current(entry.client_id_str(), located);
                }
            }
        }
        Ok(())
    }
}

impl Default for ClientLocationStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_location_roundtrip() {
        let entry = ClientLocationEntry::create("client123", NodeId::validated(5).unwrap());
        let bytes = entry.to_be_bytes();
        let (decoded, _) = ClientLocationEntry::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.client_id_str(), "client123");
        assert_eq!(decoded.connected_node, 5);
    }

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn entry_bytes(client_id: &str, node_id: u16, timestamp_ms: u64) -> Vec<u8> {
        let mut entry = ClientLocationEntry::create(client_id, node(node_id));
        entry.timestamp_ms = timestamp_ms;
        entry.to_be_bytes()
    }

    #[test]
    fn a_location_is_set_and_removed_by_its_own_node() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 3, 10))
            .unwrap();
        assert_eq!(store.get("c"), Some(node(3)));

        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 3, 20))
            .unwrap();
        assert_eq!(store.get("c"), None);
    }

    #[test]
    fn a_disconnect_from_the_old_node_keeps_the_newer_location() {
        let store = ClientLocationStore::new();
        store.set_if_newer(
            "c",
            Located {
                node: node(3),
                since_ms: 10,
            },
        );
        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 1, 20))
            .unwrap();
        assert_eq!(store.get("c"), Some(node(3)));
    }

    #[test]
    fn an_older_location_does_not_replace_a_newer_one() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 3, 20))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 10))
            .unwrap();
        assert_eq!(store.get("c"), Some(node(3)));
    }

    #[test]
    fn a_disconnect_older_than_a_reconnect_to_the_same_node_keeps_it() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 30))
            .unwrap();
        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 1, 20))
            .unwrap();
        assert_eq!(store.get("c"), Some(node(1)));
    }
}
