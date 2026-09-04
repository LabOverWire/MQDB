// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use crate::consumer_group::ConsumerGroup;
use crate::dispatcher::EventDispatcher;
use crate::outbox_processor::OutboxProcessor;
use mqdb_core::constraint::ConstraintManager;
use mqdb_core::entity::Entity;
use mqdb_core::events::ChangeEvent;
use mqdb_core::index::IndexManager;
use mqdb_core::outbox::Outbox;
use mqdb_core::storage::Storage;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;

pub(super) struct BackgroundDeps<'a> {
    pub config: &'a mqdb_core::config::DatabaseConfig,
    pub outbox: &'a Arc<Outbox>,
    pub dispatcher: &'a Arc<EventDispatcher>,
    pub storage: &'a Arc<Storage>,
    pub index_manager: &'a Arc<RwLock<IndexManager>>,
    pub constraint_manager: &'a Arc<RwLock<ConstraintManager>>,
    pub consumer_groups: &'a Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    pub shutdown_rx: &'a watch::Receiver<bool>,
}

impl Database {
    pub(super) fn spawn_background_tasks(deps: &BackgroundDeps<'_>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        if !deps.config.spawn_background_tasks {
            return handles;
        }

        if deps.config.outbox.enabled {
            handles.push(Self::spawn_outbox_processor(
                deps.outbox,
                deps.dispatcher,
                &deps.config.outbox,
                deps.shutdown_rx,
            ));
        }

        if let Some(interval_secs) = deps.config.ttl_cleanup_interval_secs {
            handles.push(Self::spawn_ttl_cleanup(
                deps.storage,
                deps.dispatcher,
                deps.outbox,
                deps.index_manager,
                deps.constraint_manager,
                interval_secs,
            ));
        }

        if deps.config.shared_subscription.consumer_timeout_ms > 0 {
            handles.push(Self::spawn_consumer_timeout_cleanup(
                deps.consumer_groups,
                deps.config.shared_subscription.consumer_timeout_ms,
                deps.shutdown_rx,
            ));
        }

        handles
    }

    fn spawn_outbox_processor(
        outbox: &Arc<Outbox>,
        dispatcher: &Arc<EventDispatcher>,
        outbox_config: &mqdb_core::config::OutboxConfig,
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
        constraint_manager: &Arc<RwLock<ConstraintManager>>,
        interval_secs: u64,
    ) -> JoinHandle<()> {
        let ctx = TtlSweepCtx {
            storage: Arc::clone(storage),
            dispatcher: Arc::clone(dispatcher),
            outbox: Arc::clone(outbox),
            index_manager: Arc::clone(index_manager),
            constraint_manager: Arc::clone(constraint_manager),
        };

        tokio::spawn(async move {
            ttl_cleanup_task(ctx, interval_secs).await;
        })
    }

    #[cfg(test)]
    pub(crate) async fn ttl_cleanup_pass_for_test(&self, now: u64) -> usize {
        self.ttl_sweep_ctx().pass(now).await
    }

    #[cfg(test)]
    pub(crate) fn raw_row_for_test(
        &self,
        entity: &str,
        id: &str,
    ) -> Option<(Vec<u8>, Vec<u8>, Entity)> {
        let key = mqdb_core::keys::encode_data_key(entity, id);
        let value = self.storage.get(&key).ok()??;
        let entity = Entity::deserialize(entity.to_string(), id.to_string(), &value).ok()?;
        Some((key, value, entity))
    }

    #[cfg(test)]
    pub(crate) async fn reap_one_for_test(
        &self,
        key: &[u8],
        value: &[u8],
        entity: &Entity,
    ) -> bool {
        self.ttl_sweep_ctx().reap(key, value, entity).await
    }

    #[cfg(test)]
    fn ttl_sweep_ctx(&self) -> TtlSweepCtx {
        TtlSweepCtx {
            storage: Arc::clone(&self.storage),
            dispatcher: Arc::clone(&self.dispatcher),
            outbox: Arc::clone(&self.outbox),
            index_manager: Arc::clone(&self.index_manager),
            constraint_manager: Arc::clone(&self.constraint_manager),
        }
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

struct TtlSweepCtx {
    storage: Arc<Storage>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
}

async fn ttl_cleanup_task(ctx: TtlSweepCtx, interval_secs: u64) {
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

        ctx.pass(now).await;
    }
}

impl TtlSweepCtx {
    async fn pass(&self, now: u64) -> usize {
        let Ok(items) = self.storage.prefix_scan(b"data/") else {
            return 0;
        };

        let mut expired = Vec::new();
        for (key, value) in items {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() != 3 {
                continue;
            }
            let Ok(entity) =
                Entity::deserialize(parts[1].to_string(), parts[2].to_string(), &value)
            else {
                continue;
            };
            if let Some(expires_at) = entity.data.get("_expires_at").and_then(Value::as_u64)
                && expires_at <= now
            {
                expired.push((key, value, entity));
            }
        }

        let expired_count = expired.len();
        let mut reaped = 0usize;
        for (key, value, entity) in expired {
            if self.reap(&key, &value, &entity).await {
                reaped += 1;
            }
        }

        if expired_count > 0 {
            tracing::debug!(reaped, expired = expired_count, "TTL cleanup processed");
        }
        reaped
    }

    async fn reap(&self, key: &[u8], value: &[u8], entity: &Entity) -> bool {
        let operation_id = uuid::Uuid::new_v4().to_string();
        let mut batch = self.storage.batch();

        batch.expect_value(key.to_vec(), value.to_vec());
        batch.remove(key.to_vec());

        {
            let index_mgr = self.index_manager.read().await;
            index_mgr.remove_indexes(&mut batch, entity);
        }

        {
            let constraint_mgr = self.constraint_manager.read().await;
            if let Err(e) = constraint_mgr.release_unique_guards(entity, &mut batch) {
                tracing::warn!(
                    entity = %entity.name,
                    id = %entity.id,
                    err = %e,
                    "TTL cleanup: release_unique_guards failed, skipping"
                );
                return false;
            }
        }

        let event =
            ChangeEvent::delete(entity.name.clone(), entity.id.clone(), entity.data.clone());
        self.outbox
            .enqueue_events(&mut batch, &operation_id, std::slice::from_ref(&event));

        match batch.commit() {
            Ok(()) => {
                let _ = self.dispatcher.dispatch(event).await;
                if let Err(e) = self.outbox.mark_delivered(&operation_id) {
                    tracing::warn!(op_id = %operation_id, err = %e, "TTL cleanup mark_delivered failed");
                }
                true
            }
            Err(e) => {
                tracing::debug!(
                    entity = %entity.name,
                    id = %entity.id,
                    err = %e,
                    "TTL cleanup: skipped expired row (renewed or deleted concurrently)"
                );
                false
            }
        }
    }
}
