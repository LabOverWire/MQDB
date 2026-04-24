// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::dispatcher::EventDispatcher;
use mqdb_core::config::OutboxConfig;
use mqdb_core::outbox::Outbox;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub struct OutboxProcessor {
    outbox: Arc<Outbox>,
    dispatcher: Arc<EventDispatcher>,
    config: OutboxConfig,
    shutdown_rx: watch::Receiver<bool>,
}

impl OutboxProcessor {
    #[allow(clippy::must_use_candidate)]
    pub fn new(
        outbox: Arc<Outbox>,
        dispatcher: Arc<EventDispatcher>,
        config: OutboxConfig,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            outbox,
            dispatcher,
            config,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) {
        tracing::info!("outbox processor started");
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(self.config.retry_interval_ms)) => {
                    self.process_pending().await;
                }
                _ = self.shutdown_rx.changed() => {
                    tracing::info!("outbox processor shutting down");
                    break;
                }
            }
        }
    }

    async fn process_pending(&self) {
        let entries = match self.outbox.pending_events() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(err = %e, "failed to fetch pending outbox entries");
                return;
            }
        };

        for entry in entries.into_iter().take(self.config.batch_size) {
            if entry.retry_count >= self.config.max_retries {
                tracing::error!(
                    op_id = %entry.operation_id,
                    retries = entry.retry_count,
                    "max retries exceeded, moving to dead letter"
                );
                if let Err(e) = self.outbox.move_to_dead_letter(&entry.operation_id) {
                    tracing::error!(err = %e, "failed to move to dead letter");
                }
                continue;
            }

            let mut dispatched = entry.dispatched_count;
            let mut failed = false;
            for event in entry.events.iter().skip(entry.dispatched_count) {
                let event_with_op_id = event.clone().with_operation_id(entry.operation_id.clone());
                if let Err(e) = self.dispatcher.dispatch(event_with_op_id).await {
                    tracing::warn!(
                        op_id = %entry.operation_id,
                        err = %e,
                        dispatched,
                        total = entry.events.len(),
                        "dispatch failed"
                    );
                    failed = true;
                    break;
                }
                dispatched += 1;
            }

            if failed {
                if let Err(e) = self
                    .outbox
                    .update_dispatched_count(&entry.operation_id, dispatched)
                {
                    tracing::error!(
                        op_id = %entry.operation_id,
                        err = %e,
                        "failed to update dispatched count"
                    );
                }
            } else if let Err(e) = self.outbox.mark_delivered(&entry.operation_id) {
                tracing::error!(
                    op_id = %entry.operation_id,
                    err = %e,
                    "failed to mark as delivered"
                );
            }
        }
    }
}
