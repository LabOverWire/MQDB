// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_core::error::Result;
use mqdb_core::events::ChangeEvent;
use mqdb_core::keys;
use mqdb_core::storage::Storage;
use mqdb_core::subscription::Subscription;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SubscriptionRegistry {
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    storage: Arc<Storage>,
}

impl SubscriptionRegistry {
    #[allow(clippy::must_use_candidate)]
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            storage,
        }
    }

    /// # Errors
    /// Returns an error if reading or deserializing subscriptions fails.
    pub async fn load(&self) -> Result<()> {
        let prefix = b"sub/";
        let items = self.storage.prefix_scan(prefix)?;

        let mut subs = self.subscriptions.write().await;

        for (_key, value) in items {
            let sub: Subscription = serde_json::from_slice(&value)?;
            subs.insert(sub.id.clone(), sub);
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if storing the subscription fails.
    pub async fn register(&self, subscription: Subscription) -> Result<()> {
        let key = keys::encode_subscription_key(&subscription.id);
        let value = serde_json::to_vec(&subscription)?;

        self.storage.insert(&key, &value)?;

        let mut subs = self.subscriptions.write().await;
        subs.insert(subscription.id.clone(), subscription);

        Ok(())
    }

    /// # Errors
    /// Returns an error if removing the subscription fails.
    pub async fn unregister(&self, sub_id: &str) -> Result<()> {
        let key = keys::encode_subscription_key(sub_id);
        self.storage.remove(&key)?;

        let mut subs = self.subscriptions.write().await;
        subs.remove(sub_id);

        Ok(())
    }

    pub async fn find_matching(&self, event: &ChangeEvent) -> Vec<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.values()
            .filter(|sub| sub.matches(event))
            .cloned()
            .collect()
    }

    pub async fn count(&self) -> usize {
        let subs = self.subscriptions.read().await;
        subs.len()
    }

    pub async fn get(&self, sub_id: &str) -> Option<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.get(sub_id).cloned()
    }
}
