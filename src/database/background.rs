// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Database;
use crate::consumer_group::ConsumerGroup;
use crate::dispatcher::EventDispatcher;
use crate::entity::Entity;
use crate::events::ChangeEvent;
use crate::index::IndexManager;
use crate::outbox::{Outbox, OutboxProcessor};
use crate::storage::Storage;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;

impl Database {
    pub(super) fn spawn_background_tasks(
        config: &crate::config::DatabaseConfig,
        outbox: &Arc<Outbox>,
        dispatcher: &Arc<EventDispatcher>,
        storage: &Arc<Storage>,
        index_manager: &Arc<RwLock<IndexManager>>,
        consumer_groups: &Arc<RwLock<HashMap<String, ConsumerGroup>>>,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        if !config.spawn_background_tasks {
            return handles;
        }

        if config.outbox.enabled {
            handles.push(Self::spawn_outbox_processor(
                outbox,
                dispatcher,
                &config.outbox,
                shutdown_rx,
            ));
        }

        if let Some(interval_secs) = config.ttl_cleanup_interval_secs {
            handles.push(Self::spawn_ttl_cleanup(
                storage,
                dispatcher,
                outbox,
                index_manager,
                interval_secs,
            ));
        }

        if config.shared_subscription.consumer_timeout_ms > 0 {
            handles.push(Self::spawn_consumer_timeout_cleanup(
                consumer_groups,
                config.shared_subscription.consumer_timeout_ms,
                shutdown_rx,
            ));
        }

        handles
    }

    fn spawn_outbox_processor(
        outbox: &Arc<Outbox>,
        dispatcher: &Arc<EventDispatcher>,
        outbox_config: &crate::config::OutboxConfig,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let pending_count = outbox.pending_count().unwrap_or(0);
        if pending_count > 0 {
            tracing::info!(
                pending = pending_count,
                "found pending outbox entries, starting processor"
            );
        }

        let mut processor = OutboxProcessor::new(
            Arc::clone(outbox),
            Arc::clone(dispatcher),
            outbox_config.clone(),
            shutdown_rx.clone(),
        );
        tokio::spawn(async move {
            processor.run().await;
        })
    }

    fn spawn_ttl_cleanup(
        storage: &Arc<Storage>,
        dispatcher: &Arc<EventDispatcher>,
        outbox: &Arc<Outbox>,
        index_manager: &Arc<RwLock<IndexManager>>,
        interval_secs: u64,
    ) -> JoinHandle<()> {
        let storage_clone = Arc::clone(storage);
        let dispatcher_clone = Arc::clone(dispatcher);
        let outbox_clone = Arc::clone(outbox);
        let index_manager_clone = Arc::clone(index_manager);

        tokio::spawn(async move {
            ttl_cleanup_task(
                storage_clone,
                dispatcher_clone,
                outbox_clone,
                index_manager_clone,
                interval_secs,
            )
            .await;
        })
    }

    fn spawn_consumer_timeout_cleanup(
        consumer_groups: &Arc<RwLock<HashMap<String, ConsumerGroup>>>,
        timeout_ms: u64,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let consumer_groups_clone = Arc::clone(consumer_groups);
        let mut shutdown_rx_clone = shutdown_rx.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(timeout_ms / 2));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut groups = consumer_groups_clone.write().await;
                        for (name, group) in groups.iter_mut() {
                            let stale = group.remove_stale_members(timeout_ms);
                            if !stale.is_empty() {
                                tracing::info!(
                                    group = %name,
                                    removed = ?stale,
                                    "removed stale consumers"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx_clone.changed() => {
                        tracing::debug!("heartbeat cleanup task shutting down");
                        break;
                    }
                }
            }
        })
    }
}

async fn ttl_cleanup_task(
    storage: Arc<Storage>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(e) => {
                tracing::warn!("TTL cleanup: failed to get system time: {e}");
                continue;
            }
        };

        let prefix = b"data/";
        let Ok(items) = storage.prefix_scan(prefix) else {
            continue;
        };

        let mut expired_entities = Vec::new();

        for (key, value) in items {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };

            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() != 3 {
                continue;
            }

            let entity_name = parts[1];
            let id = parts[2];

            let Ok(entity) = Entity::deserialize(entity_name.to_string(), id.to_string(), &value)
            else {
                continue;
            };

            if let Some(expires_at) = entity.data.get("_expires_at").and_then(Value::as_u64)
                && expires_at <= now
            {
                expired_entities.push((key, entity));
            }
        }

        if expired_entities.is_empty() {
            continue;
        }

        let operation_id = uuid::Uuid::new_v4().to_string();
        let mut batch = storage.batch();
        let mut events = Vec::new();

        for (key, entity) in &expired_entities {
            batch.remove(key.clone());

            let index_mgr = index_manager.read().await;
            index_mgr.remove_indexes(&mut batch, entity);

            events.push(ChangeEvent::delete(entity.name.clone(), entity.id.clone()));
        }

        outbox.enqueue_events(&mut batch, &operation_id, &events);

        if let Err(e) = batch.commit() {
            tracing::error!(err = %e, "TTL cleanup batch commit failed");
            continue;
        }

        for event in events {
            let _ = dispatcher.dispatch(event).await;
        }

        if let Err(e) = outbox.mark_delivered(&operation_id) {
            tracing::warn!(op_id = %operation_id, err = %e, "TTL cleanup mark_delivered failed");
        }

        tracing::debug!(
            count = expired_entities.len(),
            op_id = %operation_id,
            "TTL cleanup processed expired entities"
        );
    }
}
