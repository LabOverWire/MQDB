// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{StoreApplyError, StoreManager};
use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::{Epoch, NUM_PARTITIONS, PartitionId, entity};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeldPrimaries {
    epochs: [Option<Epoch>; NUM_PARTITIONS as usize],
    legacy_store: bool,
}

impl HeldPrimaries {
    pub(crate) fn trusts(&self, partition: PartitionId, epoch: Epoch) -> bool {
        self.legacy_store || self.epochs[usize::from(partition.get())] == Some(epoch)
    }
}

impl StoreManager {
    pub(crate) fn record_held_primary(
        &self,
        partition: PartitionId,
        epoch: Epoch,
    ) -> Result<(), StoreApplyError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            epoch,
            0,
            entity::HELD_PRIMARY.to_string(),
            String::new(),
            epoch.get().to_be_bytes().to_vec(),
        );
        storage
            .write(&write)
            .map_err(|_| StoreApplyError::PersistenceError)
    }

    pub(crate) fn held_primaries(&self) -> HeldPrimaries {
        let mut held = HeldPrimaries {
            epochs: [None; NUM_PARTITIONS as usize],
            legacy_store: false,
        };
        let Some(storage) = self.storage.as_ref() else {
            return held;
        };
        let entries = match storage.scan_entity(entity::HELD_PRIMARY) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "failed to read held primary markers");
                return held;
            }
        };
        held.legacy_store = entries.is_empty() && self.db_data.count() > 0;
        for (partition, _, data) in entries {
            if let Ok(bytes) = <[u8; 8]>::try_from(data.as_slice()) {
                held.epochs[usize::from(partition.get())] =
                    Some(Epoch::new(u64::from_be_bytes(bytes)));
            }
        }
        held
    }
}
