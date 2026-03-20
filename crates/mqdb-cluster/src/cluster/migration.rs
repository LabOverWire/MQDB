// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    Preparing,
    Overlapping,
    HandingOff,
    Complete,
}

impl MigrationPhase {
    #[must_use]
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Preparing => 0,
            Self::Overlapping => 1,
            Self::HandingOff => 2,
            Self::Complete => 3,
        }
    }

    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Preparing),
            1 => Some(Self::Overlapping),
            2 => Some(Self::HandingOff),
            3 => Some(Self::Complete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct MigrationCheckpoint {
    pub version: u8,
    pub partition: u16,
    pub migrated_seq: u64,
    pub snapshot_complete: u8,
    pub timestamp: u64,
}

impl MigrationCheckpoint {
    #[must_use]
    pub fn create(
        partition: PartitionId,
        migrated_seq: u64,
        snapshot_complete: bool,
        timestamp: u64,
    ) -> Self {
        Self {
            version: 1,
            partition: partition.get(),
            migrated_seq,
            snapshot_complete: u8::from(snapshot_complete),
            timestamp,
        }
    }

    #[must_use]
    pub fn partition_id(&self) -> Option<PartitionId> {
        PartitionId::new(self.partition)
    }

    #[must_use]
    pub fn is_snapshot_complete(&self) -> bool {
        self.snapshot_complete != 0
    }
}

#[derive(Debug, Clone)]
pub struct MigrationState {
    pub partition: PartitionId,
    pub phase: MigrationPhase,
    pub old_primary: NodeId,
    pub new_primary: NodeId,
    pub epoch: Epoch,
    pub started_at: u64,
    pub snapshot_complete: bool,
    pub catchup_sequence: u64,
}

impl MigrationState {
    #[must_use]
    pub fn new(
        partition: PartitionId,
        old_primary: NodeId,
        new_primary: NodeId,
        epoch: Epoch,
        started_at: u64,
    ) -> Self {
        Self {
            partition,
            phase: MigrationPhase::Preparing,
            old_primary,
            new_primary,
            epoch,
            started_at,
            snapshot_complete: false,
            catchup_sequence: 0,
        }
    }

    pub fn set_snapshot_complete(&mut self, sequence: u64) {
        self.snapshot_complete = true;
        self.catchup_sequence = sequence;
    }

    #[must_use]
    pub fn to_checkpoint(&self, timestamp: u64) -> MigrationCheckpoint {
        MigrationCheckpoint::create(
            self.partition,
            self.catchup_sequence,
            self.snapshot_complete,
            timestamp,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationError {
    NotMigrating,
    InvalidPhaseTransition,
    PartitionNotFound,
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMigrating => write!(f, "partition is not migrating"),
            Self::InvalidPhaseTransition => write!(f, "invalid phase transition"),
            Self::PartitionNotFound => write!(f, "partition not found"),
        }
    }
}

impl std::error::Error for MigrationError {}

pub struct MigrationManager {
    node_id: NodeId,
    migrations: HashMap<PartitionId, MigrationState>,
}

impl MigrationManager {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            migrations: HashMap::new(),
        }
    }

    pub fn start_migration(
        &mut self,
        partition: PartitionId,
        old_primary: NodeId,
        new_primary: NodeId,
        epoch: Epoch,
        started_at: u64,
    ) {
        let state = MigrationState::new(partition, old_primary, new_primary, epoch, started_at);
        self.migrations.insert(partition, state);
    }

    /// # Errors
    /// Returns `NotMigrating` if partition is not migrating.
    /// Returns `InvalidPhaseTransition` if the transition is not allowed.
    pub fn advance_phase(
        &mut self,
        partition: PartitionId,
        new_phase: MigrationPhase,
    ) -> Result<MigrationPhase, MigrationError> {
        let state = self
            .migrations
            .get_mut(&partition)
            .ok_or(MigrationError::NotMigrating)?;

        let valid_transition = matches!(
            (state.phase, new_phase),
            (MigrationPhase::Preparing, MigrationPhase::Overlapping)
                | (MigrationPhase::Overlapping, MigrationPhase::HandingOff)
                | (MigrationPhase::HandingOff, MigrationPhase::Complete)
        );

        if !valid_transition {
            return Err(MigrationError::InvalidPhaseTransition);
        }

        state.phase = new_phase;
        Ok(new_phase)
    }

    #[must_use]
    pub fn is_migrating(&self, partition: PartitionId) -> bool {
        self.migrations.contains_key(&partition)
    }

    #[must_use]
    pub fn get_phase(&self, partition: PartitionId) -> Option<MigrationPhase> {
        self.migrations.get(&partition).map(|s| s.phase)
    }

    #[must_use]
    pub fn get_state(&self, partition: PartitionId) -> Option<&MigrationState> {
        self.migrations.get(&partition)
    }

    /// # Errors
    /// Returns `NotMigrating` if partition is not migrating.
    pub fn checkpoint(
        &mut self,
        partition: PartitionId,
        sequence: u64,
    ) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&partition)
            .ok_or(MigrationError::NotMigrating)?;
        state.catchup_sequence = sequence;
        Ok(())
    }

    /// # Errors
    /// Returns `NotMigrating` if partition is not migrating.
    pub fn mark_snapshot_complete(
        &mut self,
        partition: PartitionId,
        sequence: u64,
    ) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&partition)
            .ok_or(MigrationError::NotMigrating)?;
        state.set_snapshot_complete(sequence);
        Ok(())
    }

    pub fn complete_migration(&mut self, partition: PartitionId) -> Option<MigrationState> {
        self.migrations.remove(&partition)
    }

    #[must_use]
    pub fn active_migrations(&self) -> Vec<PartitionId> {
        self.migrations.keys().copied().collect()
    }

    #[must_use]
    pub fn migration_count(&self) -> usize {
        self.migrations.len()
    }

    #[must_use]
    pub fn is_receiving(&self, partition: PartitionId) -> bool {
        self.migrations
            .get(&partition)
            .is_some_and(|s| s.new_primary == self.node_id)
    }

    #[must_use]
    pub fn is_sending(&self, partition: PartitionId) -> bool {
        self.migrations
            .get(&partition)
            .is_some_and(|s| s.old_primary == self.node_id)
    }
}

impl std::fmt::Debug for MigrationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationManager")
            .field("node_id", &self.node_id)
            .field("active_migrations", &self.migration_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    #[test]
    fn migration_phase_roundtrip() {
        for phase in [
            MigrationPhase::Preparing,
            MigrationPhase::Overlapping,
            MigrationPhase::HandingOff,
            MigrationPhase::Complete,
        ] {
            assert_eq!(MigrationPhase::from_u8(phase.as_u8()), Some(phase));
        }
        assert_eq!(MigrationPhase::from_u8(99), None);
    }

    #[test]
    fn migration_checkpoint_bebytes_roundtrip() {
        let checkpoint = MigrationCheckpoint::create(partition(10), 12345, true, 1000);
        let bytes = checkpoint.to_be_bytes();
        let (parsed, _) = MigrationCheckpoint::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(checkpoint.partition, parsed.partition);
        assert_eq!(checkpoint.migrated_seq, parsed.migrated_seq);
        assert_eq!(checkpoint.snapshot_complete, parsed.snapshot_complete);
        assert_eq!(checkpoint.timestamp, parsed.timestamp);
    }

    #[test]
    fn migration_state_lifecycle() {
        let state = MigrationState::new(partition(5), node(1), node(2), Epoch::new(10), 1000);

        assert_eq!(state.partition, partition(5));
        assert_eq!(state.phase, MigrationPhase::Preparing);
        assert!(!state.snapshot_complete);
        assert_eq!(state.catchup_sequence, 0);
    }

    #[test]
    fn migration_manager_start_and_advance() {
        let mut manager = MigrationManager::new(node(1));

        manager.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        assert!(manager.is_migrating(partition(5)));
        assert!(!manager.is_migrating(partition(6)));

        assert_eq!(
            manager.get_phase(partition(5)),
            Some(MigrationPhase::Preparing)
        );

        manager
            .advance_phase(partition(5), MigrationPhase::Overlapping)
            .unwrap();
        assert_eq!(
            manager.get_phase(partition(5)),
            Some(MigrationPhase::Overlapping)
        );

        manager
            .advance_phase(partition(5), MigrationPhase::HandingOff)
            .unwrap();
        assert_eq!(
            manager.get_phase(partition(5)),
            Some(MigrationPhase::HandingOff)
        );

        manager
            .advance_phase(partition(5), MigrationPhase::Complete)
            .unwrap();
        assert_eq!(
            manager.get_phase(partition(5)),
            Some(MigrationPhase::Complete)
        );
    }

    #[test]
    fn migration_manager_invalid_phase_transition() {
        let mut manager = MigrationManager::new(node(1));
        manager.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        let result = manager.advance_phase(partition(5), MigrationPhase::HandingOff);
        assert_eq!(result, Err(MigrationError::InvalidPhaseTransition));

        let result = manager.advance_phase(partition(5), MigrationPhase::Complete);
        assert_eq!(result, Err(MigrationError::InvalidPhaseTransition));
    }

    #[test]
    fn migration_manager_checkpoint() {
        let mut manager = MigrationManager::new(node(1));
        manager.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        manager.checkpoint(partition(5), 100).unwrap();
        assert_eq!(
            manager.get_state(partition(5)).unwrap().catchup_sequence,
            100
        );

        manager.mark_snapshot_complete(partition(5), 200).unwrap();
        let state = manager.get_state(partition(5)).unwrap();
        assert!(state.snapshot_complete);
        assert_eq!(state.catchup_sequence, 200);
    }

    #[test]
    fn migration_manager_complete() {
        let mut manager = MigrationManager::new(node(1));
        manager.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        assert_eq!(manager.migration_count(), 1);

        let completed = manager.complete_migration(partition(5));
        assert!(completed.is_some());
        assert_eq!(manager.migration_count(), 0);
        assert!(!manager.is_migrating(partition(5)));
    }

    #[test]
    fn migration_manager_sending_receiving() {
        let mut manager = MigrationManager::new(node(1));
        manager.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        assert!(manager.is_sending(partition(5)));
        assert!(!manager.is_receiving(partition(5)));

        let mut manager2 = MigrationManager::new(node(2));
        manager2.start_migration(partition(5), node(1), node(2), Epoch::new(10), 1000);

        assert!(!manager2.is_sending(partition(5)));
        assert!(manager2.is_receiving(partition(5)));
    }
}
