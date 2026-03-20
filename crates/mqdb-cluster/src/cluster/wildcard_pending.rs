// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::{Operation, WildcardBroadcast};
use super::{NodeId, PartitionId, SubscriptionType};
use std::collections::HashMap;
use std::sync::RwLock;

pub const WILDCARD_RECONCILIATION_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct PendingWildcard {
    pub pattern: String,
    pub client_id: String,
    pub client_partition: PartitionId,
    pub qos: u8,
    pub subscription_type: SubscriptionType,
    pub operation: Operation,
    pub failed_partitions: Vec<PartitionId>,
    pub created_at: u64,
    pub retry_count: u32,
}

impl PendingWildcard {
    #[must_use]
    pub fn new_subscribe(
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
        now: u64,
    ) -> Self {
        Self {
            pattern: pattern.to_string(),
            client_id: client_id.to_string(),
            client_partition,
            qos,
            subscription_type,
            operation: Operation::Insert,
            failed_partitions: Vec::new(),
            created_at: now,
            retry_count: 0,
        }
    }

    /// # Panics
    /// Panics if partition 0 is invalid (should never happen).
    #[must_use]
    pub fn new_unsubscribe(pattern: &str, client_id: &str, now: u64) -> Self {
        Self {
            pattern: pattern.to_string(),
            client_id: client_id.to_string(),
            client_partition: PartitionId::ZERO,
            qos: 0,
            subscription_type: SubscriptionType::Mqtt,
            operation: Operation::Delete,
            failed_partitions: Vec::new(),
            created_at: now,
            retry_count: 0,
        }
    }

    #[must_use]
    pub fn key(&self) -> String {
        format!("{}:{}", self.pattern, self.client_id)
    }

    #[must_use]
    pub fn to_broadcast(&self) -> WildcardBroadcast {
        match self.operation {
            Operation::Insert => WildcardBroadcast::subscribe(
                &self.pattern,
                &self.client_id,
                self.client_partition,
                self.qos,
                self.subscription_type as u8,
            ),
            Operation::Delete | Operation::Update => {
                WildcardBroadcast::unsubscribe(&self.pattern, &self.client_id)
            }
        }
    }

    pub fn add_failed_partition(&mut self, partition: PartitionId) {
        if !self.failed_partitions.contains(&partition) {
            self.failed_partitions.push(partition);
        }
    }

    pub fn mark_retry(&mut self) {
        self.retry_count += 1;
        self.failed_partitions.clear();
    }
}

pub struct WildcardPendingStore {
    node_id: NodeId,
    pending: RwLock<HashMap<String, PendingWildcard>>,
    last_reconciliation: RwLock<u64>,
}

impl WildcardPendingStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            pending: RwLock::new(HashMap::new()),
            last_reconciliation: RwLock::new(0),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn add_pending(&self, pending: PendingWildcard) {
        let key = pending.key();
        self.pending.write().unwrap().insert(key, pending);
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn remove_pending(&self, pattern: &str, client_id: &str) {
        let key = format!("{pattern}:{client_id}");
        self.pending.write().unwrap().remove(&key);
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn mark_partition_failed(&self, pattern: &str, client_id: &str, partition: PartitionId) {
        let key = format!("{pattern}:{client_id}");
        if let Some(pending) = self.pending.write().unwrap().get_mut(&key) {
            pending.add_failed_partition(partition);
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn needs_reconciliation(&self, now: u64) -> bool {
        let last = *self.last_reconciliation.read().unwrap();
        now.saturating_sub(last) >= WILDCARD_RECONCILIATION_INTERVAL_MS
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn mark_reconciliation(&self, now: u64) {
        *self.last_reconciliation.write().unwrap() = now;
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_pending_for_retry(&self) -> Vec<PendingWildcard> {
        self.pending.read().unwrap().values().cloned().collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn mark_retried(&self, pattern: &str, client_id: &str) {
        let key = format!("{pattern}:{client_id}");
        if let Some(pending) = self.pending.write().unwrap().get_mut(&key) {
            pending.mark_retry();
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.pending.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_old_entries(&self, now: u64, max_age_ms: u64) -> usize {
        let mut pending = self.pending.write().unwrap();
        let before = pending.len();
        pending.retain(|_, p| now.saturating_sub(p.created_at) <= max_age_ms);
        before - pending.len()
    }
}

impl std::fmt::Debug for WildcardPendingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WildcardPendingStore")
            .field("node_id", &self.node_id)
            .field("pending_count", &self.count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::validated(1).unwrap()
    }

    fn partition() -> PartitionId {
        PartitionId::new(5).unwrap()
    }

    #[test]
    fn add_and_remove_pending() {
        let store = WildcardPendingStore::new(node());

        let pending = PendingWildcard::new_subscribe(
            "sensors/+/temp",
            "client1",
            partition(),
            1,
            SubscriptionType::Mqtt,
            1000,
        );

        store.add_pending(pending);
        assert_eq!(store.count(), 1);

        store.remove_pending("sensors/+/temp", "client1");
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn needs_reconciliation_after_interval() {
        let store = WildcardPendingStore::new(node());

        assert!(store.needs_reconciliation(60_000));

        store.mark_reconciliation(1000);
        assert!(!store.needs_reconciliation(30_000));
        assert!(!store.needs_reconciliation(60_000));
        assert!(store.needs_reconciliation(61_001));
    }

    #[test]
    fn clear_old_entries_removes_expired() {
        let store = WildcardPendingStore::new(node());

        store.add_pending(PendingWildcard::new_subscribe(
            "old/+",
            "client1",
            partition(),
            1,
            SubscriptionType::Mqtt,
            1000,
        ));
        store.add_pending(PendingWildcard::new_subscribe(
            "new/+",
            "client2",
            partition(),
            1,
            SubscriptionType::Mqtt,
            50_000,
        ));

        let removed = store.clear_old_entries(100_000, 60_000);
        assert_eq!(removed, 1);
        assert_eq!(store.count(), 1);
    }
}
