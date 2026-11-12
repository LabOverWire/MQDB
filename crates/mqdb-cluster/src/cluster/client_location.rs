// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use super::protocol::Operation;
use bebytes::BeBytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocationOrder(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocationStamp {
    pub order: LocationOrder,
    pub instance: u64,
}

const STAMP_LEN: usize = 16;
const INSTANCES_PER_BOOT_MS: u64 = 1_000_000;

#[must_use]
pub(crate) fn boot_seeded_counter() -> u64 {
    let boot_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    boot_ms.saturating_mul(INSTANCES_PER_BOOT_MS).max(1)
}

impl ClientLocationEntry {
    #[must_use]
    pub fn encode_ordered(&self, stamp: LocationStamp) -> Vec<u8> {
        let mut bytes = self.to_be_bytes();
        bytes.extend_from_slice(&stamp.order.0.to_be_bytes());
        bytes.extend_from_slice(&stamp.instance.to_be_bytes());
        bytes
    }

    #[must_use]
    pub fn decode_ordered(data: &[u8]) -> Option<(Self, Option<LocationStamp>)> {
        let (entry, consumed) = Self::try_from_be_bytes(data).ok()?;
        let tail = &data[consumed..];
        match tail.len() {
            0 => Some((entry, None)),
            STAMP_LEN => {
                let (order, instance) = tail.split_at(8);
                Some((
                    entry,
                    Some(LocationStamp {
                        order: LocationOrder(u64::from_be_bytes(order.try_into().ok()?)),
                        instance: u64::from_be_bytes(instance.try_into().ok()?),
                    }),
                ))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Claim {
    order: u64,
    node: NodeId,
    instance: u64,
}

impl Claim {
    fn key(self) -> (u64, u16) {
        (self.order, self.node.get())
    }

    fn stamp(self) -> LocationStamp {
        LocationStamp {
            order: LocationOrder(self.order),
            instance: self.instance,
        }
    }
}

fn place(claims: &mut Vec<Claim>, claim: Claim) {
    claims.retain(|held| held.node != claim.node);
    let at = claims.partition_point(|held| held.key() < claim.key());
    claims.insert(at, claim);
}

fn newest_order(claims: &[Claim]) -> u64 {
    claims.iter().map(|claim| claim.order).max().unwrap_or(0)
}

pub struct ClientLocationStore {
    owner: NodeId,
    claims: RwLock<HashMap<String, Vec<Claim>>>,
    dead: RwLock<HashSet<NodeId>>,
    parked: RwLock<HashMap<NodeId, Vec<(String, Claim)>>>,
    next_instance: AtomicU64,
}

impl ClientLocationStore {
    #[must_use]
    pub fn new(owner: NodeId) -> Self {
        Self {
            owner,
            claims: RwLock::new(HashMap::new()),
            dead: RwLock::new(HashSet::new()),
            parked: RwLock::new(HashMap::new()),
            next_instance: AtomicU64::new(boot_seeded_counter()),
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

    fn is_dead(&self, node: NodeId) -> bool {
        self.dead
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&node)
    }

    pub fn connect_here(&self, client_id: &str) -> LocationStamp {
        let instance = self.next_instance.fetch_add(1, AtomicOrdering::Relaxed);
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let held = claims.entry(client_id.to_string()).or_default();
        let claim = Claim {
            order: newest_order(held) + 1,
            node: self.owner,
            instance,
        };
        held.clear();
        held.push(claim);
        claim.stamp()
    }

    pub fn disconnect_here(&self, client_id: &str) {
        self.release(client_id, self.owner);
    }

    pub fn claim(&self, client_id: &str, node: NodeId, stamp: Option<LocationStamp>) {
        if node == self.owner || self.is_dead(node) {
            return;
        }
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let held = claims.entry(client_id.to_string()).or_default();
        let claim = stamp.map_or_else(
            || Claim {
                order: newest_order(held) + 1,
                node,
                instance: 0,
            },
            |stamp| Claim {
                order: stamp.order.0,
                node,
                instance: stamp.instance,
            },
        );
        held.retain(|older| older.node == self.owner || older.key() > claim.key());
        place(held, claim);
    }

    pub fn claim_listed(&self, client_id: &str, node: NodeId, stamp: LocationStamp) {
        if node == self.owner || self.is_dead(node) {
            return;
        }
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        place(
            claims.entry(client_id.to_string()).or_default(),
            Claim {
                order: stamp.order.0,
                node,
                instance: stamp.instance,
            },
        );
    }

    pub fn release(&self, client_id: &str, node: NodeId) {
        self.release_up_to(client_id, node, None);
    }

    fn release_up_to(&self, client_id: &str, node: NodeId, instance: Option<u64>) {
        let released = |claim: &Claim| instance.is_none_or(|instance| claim.instance <= instance);
        if let Some(parked) = self
            .parked
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&node)
        {
            parked.retain(|(parked_client, claim)| parked_client != client_id || !released(claim));
        }
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(held) = claims.get_mut(client_id) {
            held.retain(|claim| claim.node != node || !released(claim));
            if held.is_empty() {
                claims.remove(client_id);
            }
        }
    }

    pub fn release_node(&self, node: NodeId) -> usize {
        self.dead
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node);
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let mut parked = Vec::new();
        claims.retain(|client_id, held| {
            if let Some(claim) = held.iter().find(|claim| claim.node == node) {
                parked.push((client_id.clone(), *claim));
                held.retain(|claim| claim.node != node);
            }
            !held.is_empty()
        });
        let released = parked.len();
        self.parked
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node, parked);
        released
    }

    pub fn revive_node(&self, node: NodeId) -> usize {
        self.dead
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&node);
        let parked = self
            .parked
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&node)
            .unwrap_or_default();
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let mut restored = 0;
        for (client_id, claim) in parked {
            let held = claims.entry(client_id).or_default();
            if !held.iter().any(|newer| newer.key() > claim.key()) {
                place(held, claim);
                restored += 1;
            }
        }
        claims.retain(|_, held| !held.is_empty());
        restored
    }

    pub fn retain_claims_of(&self, node: NodeId, kept: &HashSet<String>) -> usize {
        let mut claims = self.claims.write().unwrap_or_else(PoisonError::into_inner);
        let mut released = 0;
        claims.retain(|client_id, held| {
            if !kept.contains(client_id) && held.iter().any(|claim| claim.node == node) {
                held.retain(|claim| claim.node != node);
                released += 1;
            }
            !held.is_empty()
        });
        released
    }

    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<NodeId> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .and_then(|held| held.last())
            .map(|claim| claim.node)
    }

    #[must_use]
    pub fn claimants(&self, client_id: &str) -> Vec<NodeId> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .map(|held| held.iter().map(|claim| claim.node).collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn stamp_of(&self, client_id: &str, node: NodeId) -> Option<LocationStamp> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .and_then(|held| held.iter().find(|claim| claim.node == node))
            .map(|claim| claim.stamp())
    }

    #[must_use]
    pub fn is_authority_here(&self, client_id: &str) -> bool {
        self.get(client_id) == Some(self.owner)
    }

    #[must_use]
    pub fn outranks_here(&self, client_id: &str, node: NodeId, order: LocationOrder) -> bool {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(client_id)
            .and_then(|held| held.last())
            .is_some_and(|newest| newest.node == self.owner && newest.key() > (order.0, node.get()))
    }

    #[must_use]
    pub fn authoritative_here(&self) -> HashSet<String> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, held)| held.last().is_some_and(|claim| claim.node == self.owner))
            .map(|(client_id, _)| client_id.clone())
            .collect()
    }

    #[must_use]
    pub fn clients_on(&self, node: NodeId) -> HashSet<String> {
        self.claims
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, held)| held.iter().any(|claim| claim.node == node))
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
        let (entry, stamp) = ClientLocationEntry::decode_ordered(data)
            .ok_or(ClientLocationError::SerializationError)?;
        if let Some(node) = entry.node().filter(|node| *node != self.owner) {
            match operation {
                Operation::Insert | Operation::Update => {
                    self.claim(entry.client_id_str(), node, stamp);
                }
                Operation::Delete => self.release_up_to(
                    entry.client_id_str(),
                    node,
                    stamp.map(|stamp| stamp.instance),
                ),
            }
        }
        Ok(())
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
        let store = ClientLocationStore::new(node(9));
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
        let store = ClientLocationStore::new(node(9));
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
        let store = ClientLocationStore::new(node(9));
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
        let store = ClientLocationStore::new(node(9));
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
    fn a_connect_replaces_claims_whose_disconnect_was_lost() {
        let store = ClientLocationStore::new(node(9));
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 1000))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 2, 1000))
            .unwrap();
        assert_eq!(store.claimants("c"), [node(2)]);

        store
            .apply_replicated(Operation::Delete, &entry_bytes("c", 2, 1000))
            .unwrap();
        assert_eq!(store.claimants("c"), []);
    }

    fn stamp(order: u64) -> LocationStamp {
        LocationStamp {
            order: LocationOrder(order),
            instance: order,
        }
    }

    fn ordered_bytes(client_id: &str, node_id: u16, order: u64) -> Vec<u8> {
        ClientLocationEntry::create(client_id, node(node_id)).encode_ordered(stamp(order))
    }

    #[test]
    fn an_ordered_entry_survives_the_wire_and_a_legacy_one_still_decodes() {
        let ordered = ordered_bytes("c", 3, 7);
        let (entry, order) = ClientLocationEntry::decode_ordered(&ordered).unwrap();
        assert_eq!((entry.client_id_str(), entry.connected_node), ("c", 3));
        assert_eq!(order, Some(stamp(7)));
        let (legacy_view, consumed) = ClientLocationEntry::try_from_be_bytes(&ordered).unwrap();
        assert_eq!(legacy_view.connected_node, 3);
        assert_eq!(consumed + 16, ordered.len());

        let legacy = entry_bytes("c", 3, 10);
        assert_eq!(
            ClientLocationEntry::decode_ordered(&legacy).map(|(_, order)| order),
            Some(None)
        );
        assert!(ClientLocationEntry::decode_ordered(&ordered[..ordered.len() - 1]).is_none());
    }

    #[test]
    fn two_connects_that_cross_leave_both_nodes_agreeing() {
        let first = ClientLocationStore::new(node(1));
        let second = ClientLocationStore::new(node(3));
        let first_order = first.connect_here("c");
        let second_order = second.connect_here("c");
        first
            .apply_replicated(
                Operation::Insert,
                &ordered_bytes("c", 3, second_order.order.0),
            )
            .unwrap();
        second
            .apply_replicated(
                Operation::Insert,
                &ordered_bytes("c", 1, first_order.order.0),
            )
            .unwrap();

        assert_eq!(first.get("c"), second.get("c"));
        assert_eq!(
            [first.is_authority_here("c"), second.is_authority_here("c")]
                .iter()
                .filter(|here| **here)
                .count(),
            1
        );
    }

    #[test]
    fn a_connect_made_after_hearing_of_another_outranks_it_whatever_the_node_ids() {
        let low = ClientLocationStore::new(node(1));
        let high = ClientLocationStore::new(node(3));
        let older = high.connect_here("c");
        low.apply_replicated(Operation::Insert, &ordered_bytes("c", 3, older.order.0))
            .unwrap();
        let newer = low.connect_here("c");
        high.apply_replicated(Operation::Insert, &ordered_bytes("c", 1, newer.order.0))
            .unwrap();

        assert_eq!(low.get("c"), Some(node(1)));
        assert_eq!(high.get("c"), Some(node(1)));
        assert!(low.is_authority_here("c"));
        assert!(!high.is_authority_here("c"));
    }

    #[test]
    fn a_disconnect_of_an_older_connection_keeps_the_nodes_newer_claim() {
        let store = ClientLocationStore::new(node(9));
        store
            .apply_replicated(Operation::Insert, &ordered_bytes("c", 1, 4))
            .unwrap();
        store
            .apply_replicated(Operation::Delete, &ordered_bytes("c", 1, 3))
            .unwrap();
        assert_eq!(store.claimants("c"), [node(1)]);
        store
            .apply_replicated(Operation::Delete, &ordered_bytes("c", 1, 4))
            .unwrap();
        assert_eq!(store.claimants("c"), []);
    }

    #[test]
    fn a_connect_elsewhere_does_not_drop_a_client_connected_here() {
        let store = ClientLocationStore::new(node(2));
        store.connect_here("c");
        store
            .apply_replicated(Operation::Insert, &entry_bytes("c", 1, 10))
            .unwrap();

        assert_eq!(store.claimants("c"), [node(2), node(1)]);
        store.release_node(node(1));
        assert_eq!(store.claimants("c"), [node(2)]);
    }

    #[test]
    fn a_listed_claim_ranks_by_its_order_and_drops_no_other_claim() {
        let store = ClientLocationStore::new(node(9));
        store.claim("c", node(3), Some(stamp(5)));
        store.claim_listed("c", node(1), stamp(2));
        assert_eq!(store.claimants("c"), [node(1), node(3)]);
        assert_eq!(store.get("c"), Some(node(3)));

        store.claim_listed("c", node(1), stamp(7));
        assert_eq!(store.claimants("c"), [node(3), node(1)]);
        assert_eq!(store.get("c"), Some(node(1)));
    }

    #[test]
    fn a_location_from_a_node_declared_dead_is_ignored_until_it_comes_back() {
        let store = ClientLocationStore::new(node(9));
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
        let store = ClientLocationStore::new(node(9));
        store.claim("gone", node(1), None);
        store.claim("kept", node(1), None);
        store.claim("shared", node(2), None);
        store.claim_listed("shared", node(1), stamp(0));

        let released = store.retain_claims_of(node(1), &HashSet::from(["kept".to_string()]));

        assert_eq!(released, 2);
        assert_eq!(store.claimants("gone"), []);
        assert_eq!(store.claimants("kept"), [node(1)]);
        assert_eq!(store.claimants("shared"), [node(2)]);
    }

    #[test]
    fn truncated_or_padded_location_writes_are_refused() {
        let store = ClientLocationStore::new(node(2));
        let bytes = entry_bytes("mover", 1, 5);
        let mut padded = bytes.clone();
        padded.push(0);

        assert!(
            store
                .apply_replicated(Operation::Insert, &bytes[..bytes.len() - 1])
                .is_err()
        );
        assert!(
            store
                .apply_replicated(Operation::Insert, &bytes[..bytes.len() - 3])
                .is_err()
        );
        assert!(store.apply_replicated(Operation::Insert, &padded).is_err());
        assert_eq!(store.claimants("mover"), []);
        assert!(store.apply_replicated(Operation::Insert, &bytes).is_ok());
        assert_eq!(store.claimants("mover"), [node(1)]);
    }

    #[test]
    fn the_newest_connect_in_the_order_is_the_authority() {
        let store = ClientLocationStore::new(node(1));
        assert_eq!(store.connect_here("mover").order, LocationOrder(1));
        assert!(store.is_authority_here("mover"));
        store.claim("mover", node(3), Some(stamp(2)));
        assert_eq!(store.claimants("mover"), [node(1), node(3)]);
        assert!(!store.is_authority_here("mover"));
        assert!(!store.authoritative_here().contains("mover"));
        assert_eq!(store.connect_here("mover").order, LocationOrder(3));
        store.claim_listed("mover", node(3), stamp(2));
        assert!(store.is_authority_here("mover"));
        assert_eq!(
            store.authoritative_here(),
            HashSet::from(["mover".to_string()])
        );
    }

    #[test]
    fn a_node_heard_from_again_gets_back_the_claims_it_held_when_declared_dead() {
        let store = ClientLocationStore::new(node(2));
        store
            .apply_replicated(Operation::Insert, &ordered_bytes("kept", 1, 1))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &ordered_bytes("moved", 1, 1))
            .unwrap();
        store
            .apply_replicated(Operation::Insert, &ordered_bytes("left", 1, 1))
            .unwrap();
        assert_eq!(store.release_node(node(1)), 3);
        assert_eq!(store.claimants("kept"), []);

        store
            .apply_replicated(Operation::Insert, &ordered_bytes("moved", 3, 2))
            .unwrap();
        store
            .apply_replicated(Operation::Delete, &ordered_bytes("left", 1, 1))
            .unwrap();
        assert_eq!(store.revive_node(node(1)), 1);

        assert_eq!(store.claimants("kept"), [node(1)]);
        assert_eq!(store.claimants("moved"), [node(3)]);
        assert_eq!(store.claimants("left"), []);
    }

    #[test]
    fn a_restarted_node_numbers_its_connections_above_those_of_its_previous_run() {
        let before_restart = ClientLocationStore::new(node(1));
        let old_connection = before_restart.connect_here("c");
        std::thread::sleep(std::time::Duration::from_millis(2));
        let after_restart = ClientLocationStore::new(node(1));
        let new_connection = after_restart.connect_here("c");
        assert_eq!(new_connection.order, old_connection.order);
        assert!(new_connection.instance > old_connection.instance);

        let observer = ClientLocationStore::new(node(2));
        let entry = ClientLocationEntry::create("c", node(1));
        observer
            .apply_replicated(Operation::Insert, &entry.encode_ordered(new_connection))
            .unwrap();
        observer
            .apply_replicated(Operation::Delete, &entry.encode_ordered(old_connection))
            .unwrap();
        assert_eq!(observer.claimants("c"), [node(1)]);
    }
}
