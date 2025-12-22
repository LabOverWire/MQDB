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
        if sequence <= self.base_sequence && !self.entries.is_empty() {
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
        self.entries
            .iter()
            .filter(|e| e.sequence >= from_seq && e.sequence <= to_seq)
            .map(|e| e.write.clone())
            .collect()
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
        self.entries.iter().any(|e| e.sequence == seq)
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
