// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use super::protocol::Operation;
use bebytes::BeBytes;
use std::collections::{HashMap, HashSet};
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

pub struct ClientLocationStore {
    claims: RwLock<HashMap<String, Vec<NodeId>>>,
    dead: RwLock<HashSet<NodeId>>,
}

impl ClientLocationStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            claims: RwLock::new(HashMap::new()),
            dead: RwLock::new(HashSet::new()),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn claim(&self, client_id: &str, node: NodeId) {
        if self
            .dead
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&node)
        {
            return;
        }
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let nodes = claims.entry(client_id.to_string()).or_default();
        if !nodes.contains(&node) {
            nodes.push(node);
        }
    }

    pub fn release(&self, client_id: &str, node: NodeId) {
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(nodes) = claims.get_mut(client_id) {
            nodes.retain(|claimant| *claimant != node);
            if nodes.is_empty() {
                claims.remove(client_id);
            }
        }
    }

    pub fn release_node(&self, node: NodeId) -> usize {
        self.dead
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node);
        self.retain_claims_of(node, &HashSet::new())
    }

    pub fn revive_node(&self, node: NodeId) {
        self.dead
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&node);
    }

    pub fn retain_claims_of(&self, node: NodeId, kept: &HashSet<String>) -> usize {
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let mut released = 0;
        claims.retain(|client_id, nodes| {
            if !kept.contains(client_id) && nodes.contains(&node) {
                nodes.retain(|claimant| *claimant != node);
                released += 1;
            }
            !nodes.is_empty()
        });
        released
    }

    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<NodeId> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .and_then(|nodes| nodes.last().copied())
    }

    #[must_use]
    pub fn claimants(&self, client_id: &str) -> Vec<NodeId> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn clients_on(&self, node: NodeId) -> HashSet<String> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, nodes)| nodes.contains(&node))
            .map(|(client_id, _)| client_id.clone())
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
            match operation {
                Operation::Insert | Operation::Update => self.claim(entry.client_id_str(), node),
                Operation::Delete => self.release(entry.client_id_str(), node),
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

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn entry_bytes(client_id: &str, node_id: u16, timestamp_ms: u64) -> Vec<u8> {
        let mut entry = ClientLocationEntry::create(client_id, node(node_id));
        entry.timestamp_ms = timestamp_ms;
        entry.to_be_bytes()
    }

    #[test]
    fn client_location_roundtrip() {
        let entry = ClientLocationEntry::create("client123", node(5));
        let bytes = entry.to_be_bytes();
        let (decoded, _) = ClientLocationEntry::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.client_id_str(), "client123");
        assert_eq!(decoded.connected_node, 5);
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
    fn a_disconnect_from_the_old_node_keeps_the_new_location() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 10))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 3, 20))
            .unwrap();
        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 1, 30))
            .unwrap();
        assert_eq!(store.claimants("c"), [node(3)]);
    }

    #[test]
    fn a_move_from_a_node_whose_clock_runs_ahead_keeps_the_new_location() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 70_000))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 2, 10_000))
            .unwrap();
        store.release_node(node(1));
        assert_eq!(store.get("c"), Some(node(2)));
    }

    #[test]
    fn a_backward_clock_step_on_one_node_does_not_freeze_the_location() {
        let store = ClientLocationStore::new();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 10_000))
            .unwrap();
        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 1, 9_000))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 2, 9_500))
            .unwrap();
        assert_eq!(store.get("c"), Some(node(2)));
    }

    #[test]
    fn equal_stamps_from_two_nodes_converge_once_the_old_node_lets_go() {
        let one = ClientLocationStore::new();
        let two = ClientLocationStore::new();
        one.apply_replicated(Operation::Insert, &entry_bytes("c", 1, 1000))
            .unwrap();
        one.apply_replicated(Operation::Insert, &entry_bytes("c", 2, 1000))
            .unwrap();
        two.apply_replicated(Operation::Insert, &entry_bytes("c", 2, 1000))
            .unwrap();
        two.apply_replicated(Operation::Insert, &entry_bytes("c", 1, 1000))
            .unwrap();
        for store in [&one, &two] {
            store
                .apply_replicated(Operation::Delete, &entry_bytes("c", 1, 1000))
                .unwrap();
        }
        assert_eq!(one.claimants("c"), two.claimants("c"));
        assert_eq!(one.get("c"), Some(node(2)));
    }

    #[test]
    fn a_location_from_a_node_declared_dead_is_ignored_until_it_comes_back() {
        let store = ClientLocationStore::new();
        store.release_node(node(1));
        store
            .apply_replicated(Operation::Insert, &entry_bytes("late", 1, 10))
            .unwrap();
        assert_eq!(store.claimants("late"), []);

        store.revive_node(node(1));
        store
            .apply_replicated(Operation::Insert, &entry_bytes("late", 1, 20))
            .unwrap();
        assert_eq!(store.claimants("late"), [node(1)]);
    }

    #[test]
    fn a_completed_resync_drops_claims_its_sender_no_longer_holds() {
        let store = ClientLocationStore::new();
        store.claim("gone", node(1));
        store.claim("kept", node(1));
        store.claim("shared", node(1));
        store.claim("shared", node(2));

        let released = store.retain_claims_of(node(1), &HashSet::from(["kept".to_string()]));

        assert_eq!(released, 2);
        assert_eq!(store.claimants("gone"), []);
        assert_eq!(store.claimants("kept"), [node(1)]);
        assert_eq!(store.claimants("shared"), [node(2)]);
    }
}
