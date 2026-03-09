// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod background;
mod backup;
mod crud;
mod query;
mod schema_ops;
mod subscriptions;

use crate::config::DatabaseConfig;
use crate::constraint::ConstraintManager;
use crate::consumer_group::ConsumerGroup;
use crate::dispatcher::EventDispatcher;
use crate::index::IndexManager;
use crate::outbox::Outbox;
use crate::relationship::RelationshipRegistry;
use crate::schema::SchemaRegistry;
use crate::storage::Storage;
use crate::subscription::SubscriptionRegistry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct SubscriptionResult {
    pub id: String,
    pub assigned_partitions: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct Database {
    storage: Arc<Storage>,
    registry: Arc<SubscriptionRegistry>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    relationship_registry: Arc<RwLock<RelationshipRegistry>>,
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
    consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    entity_names: Arc<RwLock<HashSet<String>>>,
    id_gen_lock: Arc<Mutex<()>>,
    config: Arc<DatabaseConfig>,
    shutdown_tx: watch::Sender<bool>,
    background_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl Database {
    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open<P: AsRef<std::path::Path>>(path: P) -> crate::error::Result<Self> {
        let config = DatabaseConfig::new(path.as_ref().to_path_buf());
        Self::open_with_config(config).await
    }

    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open_without_background_tasks<P: AsRef<std::path::Path>>(
        path: P,
    ) -> crate::error::Result<Self> {
        let config = DatabaseConfig::new(path.as_ref().to_path_buf()).without_background_tasks();
        Self::open_with_config(config).await
    }

    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open_with_config(config: DatabaseConfig) -> crate::error::Result<Self> {
        let storage = if let Some(ref passphrase) = config.passphrase {
            Arc::new(Storage::open_encrypted(
                &config.path,
                passphrase,
                config.durability,
            )?)
        } else {
            Arc::new(Storage::open(&config.path, config.durability)?)
        };
        Self::init_with_storage(storage, config).await
    }

    /// # Errors
    /// Returns an error if initialization fails.
    pub async fn open_with_backend(
        backend: Arc<dyn crate::storage::StorageBackend>,
        config: DatabaseConfig,
    ) -> crate::error::Result<Self> {
        let storage = Arc::new(Storage::with_backend(backend));
        Self::init_with_storage(storage, config).await
    }

    async fn init_with_storage(
        storage: Arc<Storage>,
        config: DatabaseConfig,
    ) -> crate::error::Result<Self> {
        let registry = Arc::new(SubscriptionRegistry::new(Arc::clone(&storage)));
        let consumer_groups = Arc::new(RwLock::new(HashMap::new()));
        let dispatcher = Arc::new(EventDispatcher::new(
            Arc::clone(&registry),
            config.event_channel_capacity,
            Arc::clone(&consumer_groups),
            config.shared_subscription.num_partitions,
        ));
        let outbox = Arc::new(Outbox::new(Arc::clone(&storage)));
        let index_manager = Arc::new(RwLock::new(IndexManager::new()));
        let relationship_registry = Arc::new(RwLock::new(RelationshipRegistry::new()));

        let mut schema_registry = SchemaRegistry::new();
        schema_registry.load_schemas(&storage)?;
        let schema_registry = Arc::new(RwLock::new(schema_registry));

        let mut constraint_manager = ConstraintManager::new();
        constraint_manager.load_constraints(&storage)?;
        let constraint_manager = Arc::new(RwLock::new(constraint_manager));

        let mut entity_names: HashSet<String> = HashSet::new();
        for name in schema_registry.read().await.entity_names() {
            entity_names.insert(name);
        }
        for name in constraint_manager.read().await.entity_names() {
            entity_names.insert(name);
        }
        if let Ok(items) = storage.prefix_scan(b"data/") {
            for (key, _) in items {
                if let Ok((entity, _)) = crate::keys::decode_data_key(&key)
                    && !entity.starts_with('_')
                {
                    entity_names.insert(entity);
                }
            }
        }
        let entity_names = Arc::new(RwLock::new(entity_names));

        registry.load().await?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handles = Self::spawn_background_tasks(
            &config,
            &outbox,
            &dispatcher,
            &storage,
            &index_manager,
            &consumer_groups,
            &shutdown_rx,
        );

        Ok(Self {
            storage,
            registry,
            dispatcher,
            outbox,
            index_manager,
            relationship_registry,
            schema_registry,
            constraint_manager,
            consumer_groups,
            entity_names,
            id_gen_lock: Arc::new(Mutex::new(())),
            config: Arc::new(config),
            shutdown_tx,
            background_handles: Arc::new(std::sync::Mutex::new(handles)),
        })
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        tracing::info!("database shutdown signal sent");
    }

    pub async fn close(self) {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<_> = self
            .background_handles
            .lock()
            .map(|mut guard| guard.drain(..).collect())
            .unwrap_or_default();
        for handle in handles {
            let _ = handle.await;
        }
    }

    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.config.path
    }

    #[must_use]
    pub fn num_partitions(&self) -> u8 {
        self.config.shared_subscription.num_partitions
    }

    #[must_use]
    pub fn event_receiver(&self) -> tokio::sync::broadcast::Receiver<crate::events::ChangeEvent> {
        self.dispatcher.subscribe()
    }

    pub async fn entity_names(&self) -> Vec<String> {
        let names = self.entity_names.read().await;
        let mut result: Vec<String> = names.iter().cloned().collect();
        result.sort();
        result
    }

    pub async fn register_entity_name(&self, name: &str) {
        if !name.starts_with('_') {
            let mut names = self.entity_names.write().await;
            names.insert(name.to_string());
        }
    }

    #[must_use]
    pub fn entity_record_count(&self, entity_name: &str) -> usize {
        let prefix = format!("data/{entity_name}/");
        self.storage
            .prefix_scan(prefix.as_bytes())
            .map_or(0, |items| items.len())
    }

    pub async fn all_schemas(&self) -> Vec<crate::schema::Schema> {
        let registry = self.schema_registry.read().await;
        let names = registry.entity_names();
        names
            .iter()
            .filter_map(|name| registry.get_schema(name).cloned())
            .collect()
    }

    pub async fn all_constraints(
        &self,
    ) -> std::collections::HashMap<String, Vec<crate::constraint::Constraint>> {
        let manager = self.constraint_manager.read().await;
        manager.all_constraints().clone()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Ok(mut handles) = self.background_handles.lock() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Filter;

    #[test]
    fn test_glob_match() {
        assert!(Filter::glob_match("Alice", "*li*"));
        assert!(Filter::glob_match("Charlie", "*li*"));
        assert!(!Filter::glob_match("Bob", "*li*"));
        assert!(!Filter::glob_match("David", "*li*"));

        assert!(Filter::glob_match("test@example.com", "*@example.com"));
        assert!(Filter::glob_match("a@example.com", "*@example.com"));

        assert!(Filter::glob_match("Charlie", "*lie"));
        assert!(!Filter::glob_match("Alice", "*lie"));
    }
}
