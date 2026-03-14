// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub use crate::partition::{PartitionAssignment, PartitionMap, PartitionRole};

use super::{Epoch, NodeId, PartitionId};

impl PartitionMap {
    pub fn apply_update(&mut self, update: &super::raft::PartitionUpdate) -> bool {
        let Some(partition) = PartitionId::new(u16::from(update.partition)) else {
            return false;
        };
        let Some(primary) = NodeId::validated(update.primary) else {
            return false;
        };

        let mut replicas = Vec::new();
        if let Some(r1) = NodeId::validated(update.replica1) {
            replicas.push(r1);
        }
        if let Some(r2) = NodeId::validated(update.replica2) {
            replicas.push(r2);
        }

        let epoch = Epoch::new(u64::from(update.epoch));
        self.set(
            partition,
            PartitionAssignment::new(primary, replicas, epoch),
        );
        true
    }
}
