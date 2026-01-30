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
use std::collections::HashMap;
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
        let storage = Arc::new(Storage::open(&config.path, config.durability)?);
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
