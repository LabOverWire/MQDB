// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{RecoveryField, RecoveryStats, StoreApplyError, StoreManager};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::session::session_partition;
use crate::cluster::{Epoch, SubscriptionType, entity};

impl StoreManager {
    /// # Errors
    /// Returns `StoreApplyError::PersistenceError` if no storage backend is configured.
    pub fn recover(&self) -> Result<RecoveryStats, StoreApplyError> {
        let storage = self
            .storage
            .as_ref()
            .ok_or(StoreApplyError::PersistenceError)?;

        let mut stats = RecoveryStats::default();

        let entities_to_recover = [
            (entity::SESSIONS, RecoveryField::Sessions),
            (entity::QOS2, RecoveryField::Qos2),
            (entity::SUBSCRIPTIONS, RecoveryField::Subscriptions),
            (entity::RETAINED, RecoveryField::Retained),
            (entity::INFLIGHT, RecoveryField::Inflight),
            (entity::OFFSETS, RecoveryField::Offsets),
            (entity::IDEMPOTENCY, RecoveryField::Idempotency),
            (entity::DB_DATA, RecoveryField::DbData),
            (entity::DB_SCHEMA, RecoveryField::DbSchema),
            (entity::DB_INDEX, RecoveryField::DbIndex),
            (entity::DB_UNIQUE, RecoveryField::DbUnique),
            (entity::DB_FK, RecoveryField::DbFk),
        ];

        for (entity_name, field) in entities_to_recover {
            match storage.scan_entity(entity_name) {
                Ok(entries) => {
                    for (partition, id, data) in entries {
                        let write = ReplicationWrite::new(
                            partition,
                            Operation::Insert,
                            Epoch::ZERO,
                            0,
                            entity_name.to_string(),
                            id,
                            data,
                        );
                        if self.apply_to_memory(&write).is_ok() {
                            stats.increment(field);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(entity = entity_name, error = ?e, "failed to scan entity during recovery");
                }
            }
        }

        stats.topic_index_rebuilt = self.rebuild_topic_index();
        stats.wildcards_rebuilt = self.rebuild_wildcards();

        Ok(stats)
    }

    fn rebuild_topic_index(&self) -> usize {
        let mut count = 0;
        for snapshot in self.subscriptions.all_snapshots() {
            let client_id = snapshot.client_id_str();
            let partition = session_partition(client_id);
            for entry in snapshot.exact_subscriptions() {
                if self
                    .topics
                    .subscribe(entry.topic_str(), client_id, partition, entry.qos)
                    .is_ok()
                {
                    count += 1;
                }
            }
        }
        count
    }

    fn rebuild_wildcards(&self) -> usize {
        let mut count = 0;
        for snapshot in self.subscriptions.all_snapshots() {
            let client_id = snapshot.client_id_str();
            for entry in snapshot.wildcard_subscriptions() {
                if self
                    .wildcards
                    .subscribe(
                        entry.topic_str(),
                        client_id,
                        entry.qos,
                        SubscriptionType::Mqtt,
                    )
                    .is_ok()
                {
                    count += 1;
                }
            }
        }
        count
    }
}
