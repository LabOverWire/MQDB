// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::PartitionId;
use super::protocol::ReplicationWrite;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

const DEFAULT_MAX_ENTRIES: usize = 10_000;

struct WriteLogEntry {
    sequence: u64,
    write: ReplicationWrite,
}

struct BoundedLog {
    entries: VecDeque<WriteLogEntry>,
    base_sequence: u64,
}

impl BoundedLog {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            base_sequence: 0,
        }
    }

    fn append(&mut self, sequence: u64, write: ReplicationWrite, max_entries: usize) {
        while self
            .entries
            .back()
            .is_some_and(|last| last.sequence >= sequence && last.write.epoch < write.epoch)
        {
            self.entries.pop_back();
        }
        if self.entries.is_empty() {
            self.base_sequence = 0;
        }
        if sequence <= self.base_sequence
            || self
                .entries
                .back()
                .is_some_and(|last| last.sequence >= sequence)
        {
            return;
        }

        self.entries.push_back(WriteLogEntry { sequence, write });

        while self.entries.len() > max_entries {
            if let Some(front) = self.entries.pop_front() {
                self.base_sequence = front.sequence;
            }
        }
    }

    fn get_range(&self, from_seq: u64, to_seq: u64) -> Vec<ReplicationWrite> {
        let start = std::time::Instant::now();

        let result: Vec<ReplicationWrite> = self
            .entries
            .iter()
            .filter(|e| e.sequence >= from_seq && e.sequence <= to_seq)
            .map(|e| e.write.clone())
            .collect();

        let elapsed = start.elapsed();
        if elapsed.as_micros() > 500 {
            tracing::warn!(
                elapsed_us = elapsed.as_micros(),
                entries = self.entries.len(),
                from_seq,
                to_seq,
                "slow write_log get_range"
            );
        }

        result
    }

    fn get_from(&self, from_seq: u64) -> Vec<ReplicationWrite> {
        self.entries
            .iter()
            .filter(|e| e.sequence >= from_seq)
            .map(|e| e.write.clone())
            .collect()
    }

    fn has_sequence(&self, seq: u64) -> bool {
        if seq <= self.base_sequence {
            return false;
        }
        let start = std::time::Instant::now();
        let result = self.entries.iter().any(|e| e.sequence == seq);

        let elapsed = start.elapsed();
        if elapsed.as_micros() > 500 {
            tracing::warn!(
                elapsed_us = elapsed.as_micros(),
                entries = self.entries.len(),
                seq,
                "slow write_log has_sequence"
            );
        }
        result
    }

    fn can_catchup(&self, from_seq: u64) -> bool {
        if let Some(oldest) = self.oldest_sequence() {
            from_seq >= oldest
        } else {
            true
        }
    }

    fn oldest_sequence(&self) -> Option<u64> {
        self.entries.front().map(|e| e.sequence)
    }

    fn newest_sequence(&self) -> Option<u64> {
        self.entries.back().map(|e| e.sequence)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

pub struct PartitionWriteLog {
    logs: RwLock<HashMap<u16, BoundedLog>>,
    max_entries: usize,
}

impl PartitionWriteLog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            logs: RwLock::new(HashMap::new()),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    #[must_use]
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            logs: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear(&self, partition: PartitionId) {
        self.logs.write().unwrap().remove(&partition.get());
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn append(&self, partition: PartitionId, sequence: u64, write: ReplicationWrite) {
        let mut logs = self.logs.write().unwrap();
        let log = logs.entry(partition.get()).or_insert_with(BoundedLog::new);
        log.append(sequence, write, self.max_entries);
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_range(
        &self,
        partition: PartitionId,
        from_seq: u64,
        to_seq: u64,
    ) -> Vec<ReplicationWrite> {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .map(|log| log.get_range(from_seq, to_seq))
            .unwrap_or_default()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_from(&self, partition: PartitionId, from_seq: u64) -> Vec<ReplicationWrite> {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .map(|log| log.get_from(from_seq))
            .unwrap_or_default()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn can_catchup(&self, partition: PartitionId, from_seq: u64) -> bool {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .is_none_or(|log| log.can_catchup(from_seq))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn has_sequence(&self, partition: PartitionId, seq: u64) -> bool {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .is_some_and(|log| log.has_sequence(seq))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn oldest_sequence(&self, partition: PartitionId) -> Option<u64> {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .and_then(BoundedLog::oldest_sequence)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn newest_sequence(&self, partition: PartitionId) -> Option<u64> {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get())
            .and_then(BoundedLog::newest_sequence)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn entry_count(&self, partition: PartitionId) -> usize {
        let logs = self.logs.read().unwrap();
        logs.get(&partition.get()).map_or(0, BoundedLog::len)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn total_entries(&self) -> usize {
        let logs = self.logs.read().unwrap();
        logs.values().map(BoundedLog::len).sum()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) {
        let mut logs = self.logs.write().unwrap();
        logs.remove(&partition.get());
    }
}

impl Default for PartitionWriteLog {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PartitionWriteLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionWriteLog")
            .field("max_entries", &self.max_entries)
            .field("total_entries", &self.total_entries())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Epoch, protocol::Operation};

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    fn write(seq: u64) -> ReplicationWrite {
        ReplicationWrite::new(
            partition(0),
            Operation::Insert,
            Epoch::new(1),
            seq,
            "test".to_string(),
            format!("id{seq}"),
            vec![],
        )
    }

    fn write_in(epoch: u64, seq: u64) -> ReplicationWrite {
        ReplicationWrite {
            epoch: Epoch::new(epoch),
            ..write(seq)
        }
    }

    fn sequences_and_epochs(log: &PartitionWriteLog) -> Vec<(u64, u64)> {
        log.get_from(partition(0), 0)
            .iter()
            .map(|w| (w.sequence, w.epoch.get()))
            .collect()
    }

    #[test]
    fn a_write_of_a_newer_epoch_drops_older_entries_at_or_above_its_sequence() {
        let log = PartitionWriteLog::new();
        for seq in 1..=4 {
            log.append(partition(0), seq, write_in(1, seq));
        }

        log.append(partition(0), 3, write_in(2, 3));

        assert_eq!(sequences_and_epochs(&log), vec![(1, 1), (2, 1), (3, 2)]);
    }

    #[test]
    fn a_repeated_write_of_the_same_epoch_keeps_the_entries_after_it() {
        let log = PartitionWriteLog::new();
        for seq in 1..=4 {
            log.append(partition(0), seq, write_in(1, seq));
        }

        log.append(partition(0), 2, write_in(1, 2));

        assert_eq!(
            sequences_and_epochs(&log),
            vec![(1, 1), (2, 1), (3, 1), (4, 1)]
        );
    }

    #[test]
    fn a_write_of_an_older_epoch_is_not_appended() {
        let log = PartitionWriteLog::new();
        log.append(partition(0), 1, write_in(2, 1));
        log.append(partition(0), 2, write_in(2, 2));

        log.append(partition(0), 2, write_in(1, 2));

        assert_eq!(sequences_and_epochs(&log), vec![(1, 2), (2, 2)]);
    }

    #[test]
    fn a_newer_epoch_below_the_evicted_base_replaces_the_whole_log() {
        let log = PartitionWriteLog::with_max_entries(2);
        for seq in 1..=5 {
            log.append(partition(0), seq, write_in(1, seq));
        }

        log.append(partition(0), 1, write_in(2, 1));
        log.append(partition(0), 2, write_in(2, 2));

        assert_eq!(sequences_and_epochs(&log), vec![(1, 2), (2, 2)]);
    }

    #[test]
    fn clearing_forgets_only_that_partition() {
        let log = PartitionWriteLog::new();
        log.append(partition(0), 1, write(1));
        log.append(partition(1), 1, write(1));

        log.clear(partition(0));

        assert!(log.get_from(partition(0), 0).is_empty());
        assert_eq!(log.get_from(partition(1), 0).len(), 1);
    }

    #[test]
    fn append_and_retrieve() {
        let log = PartitionWriteLog::new();

        log.append(partition(0), 1, write(1));
        log.append(partition(0), 2, write(2));
        log.append(partition(0), 3, write(3));

        let entries = log.get_from(partition(0), 1);
        assert_eq!(entries.len(), 3);

        let entries = log.get_from(partition(0), 2);
        assert_eq!(entries.len(), 2);

        let entries = log.get_range(partition(0), 1, 2);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn respects_max_entries() {
        let log = PartitionWriteLog::with_max_entries(3);

        for i in 1..=5 {
            log.append(partition(0), i, write(i));
        }

        assert_eq!(log.entry_count(partition(0)), 3);
        assert_eq!(log.oldest_sequence(partition(0)), Some(3));
        assert_eq!(log.newest_sequence(partition(0)), Some(5));
    }

    #[test]
    fn can_catchup_check() {
        let log = PartitionWriteLog::with_max_entries(3);

        for i in 3..=5 {
            log.append(partition(0), i, write(i));
        }

        assert!(log.can_catchup(partition(0), 3));
        assert!(log.can_catchup(partition(0), 4));
        assert!(!log.can_catchup(partition(0), 1));
    }

    #[test]
    fn separate_partitions() {
        let log = PartitionWriteLog::new();

        log.append(partition(0), 1, write(1));
        log.append(partition(1), 1, write(1));
        log.append(partition(1), 2, write(2));

        assert_eq!(log.entry_count(partition(0)), 1);
        assert_eq!(log.entry_count(partition(1)), 2);
        assert_eq!(log.total_entries(), 3);
    }

    #[test]
    fn clear_partition() {
        let log = PartitionWriteLog::new();

        log.append(partition(0), 1, write(1));
        log.append(partition(0), 2, write(2));

        log.clear_partition(partition(0));

        assert_eq!(log.entry_count(partition(0)), 0);
    }
}
