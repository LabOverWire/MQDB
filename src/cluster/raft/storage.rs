// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::NodeId;
use super::state::LogEntry;
use crate::error::Result;
use crate::storage::StorageBackend;
use bebytes::BeBytes;
use std::sync::Arc;

const RAFT_STATE_KEY: &[u8] = b"_raft/state";
const RAFT_LOG_PREFIX: &[u8] = b"_raft/log/";

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct RaftPersistentState {
    pub current_term: u64,
    pub voted_for: u16,
}

impl RaftPersistentState {
    #[must_use]
    pub fn voted_for_node(&self) -> Option<NodeId> {
        if self.voted_for == 0 {
            None
        } else {
            NodeId::validated(self.voted_for)
        }
    }

    #[must_use]
    pub fn create(term: u64, voted_for: Option<NodeId>) -> Self {
        Self::new(term, voted_for.map_or(0, NodeId::get))
    }
}

pub struct RaftStorage {
    backend: Arc<dyn StorageBackend>,
}

impl RaftStorage {
    #[must_use]
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    /// # Errors
    /// Returns an error if persistence fails.
    pub fn persist_state(&self, term: u64, voted_for: Option<NodeId>) -> Result<()> {
        let state = RaftPersistentState::create(term, voted_for);
        let bytes = state.to_be_bytes();
        self.backend.insert(RAFT_STATE_KEY, &bytes)?;
        self.backend.flush()
    }

    /// # Errors
    /// Returns an error if loading fails or data is corrupted.
    pub fn load_state(&self) -> Result<Option<RaftPersistentState>> {
        match self.backend.get(RAFT_STATE_KEY)? {
            Some(bytes) => {
                let (state, _) = RaftPersistentState::try_from_be_bytes(&bytes)
                    .map_err(|e| crate::error::Error::StorageGeneric(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// # Errors
    /// Returns an error if persistence fails.
    pub fn append_log_entry(&self, entry: &LogEntry) -> Result<()> {
        let key = log_entry_key(entry.index);
        let bytes = entry.to_be_bytes();
        self.backend.insert(&key, &bytes)?;
        self.backend.flush()
    }

    /// # Errors
    /// Returns an error if persistence fails.
    pub fn append_log_entries_batch(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = self.backend.batch();
        for entry in entries {
            let key = log_entry_key(entry.index);
            let bytes = entry.to_be_bytes();
            batch.insert(key, bytes);
        }
        batch.commit()?;
        self.backend.flush()
    }

    /// # Errors
    /// Returns an error if loading fails or data is corrupted.
    pub fn load_log(&self) -> Result<Vec<LogEntry>> {
        let entries_raw = self.backend.prefix_scan(RAFT_LOG_PREFIX)?;
        let mut entries = Vec::with_capacity(entries_raw.len());

        for (_, value) in entries_raw {
            let (entry, _) = LogEntry::try_from_be_bytes(&value)
                .map_err(|e| crate::error::Error::StorageGeneric(e.to_string()))?;
            entries.push(entry);
        }

        entries.sort_by_key(|e| e.index);
        Ok(entries)
    }

    /// # Errors
    /// Returns an error if deletion fails.
    pub fn truncate_log_from(&self, index: u64) -> Result<()> {
        let entries = self.backend.prefix_scan(RAFT_LOG_PREFIX)?;
        let mut batch = self.backend.batch();

        for (key, value) in entries {
            let (entry, _) = LogEntry::try_from_be_bytes(&value)
                .map_err(|e| crate::error::Error::StorageGeneric(e.to_string()))?;
            if entry.index >= index {
                batch.remove(key);
            }
        }

        batch.commit()?;
        self.backend.flush()
    }
}

fn log_entry_key(index: u64) -> Vec<u8> {
    let mut key = RAFT_LOG_PREFIX.to_vec();
    key.extend_from_slice(&index.to_be_bytes());
    key
}

impl std::fmt::Debug for RaftStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftStorage").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::RaftCommand;
    use super::*;
    use crate::storage::MemoryBackend;

    fn storage() -> RaftStorage {
        RaftStorage::new(Arc::new(MemoryBackend::new()))
    }

    #[test]
    fn persist_and_load_state() {
        let s = storage();
        let node = NodeId::validated(5).unwrap();

        assert!(s.load_state().unwrap().is_none());

        s.persist_state(10, Some(node)).unwrap();
        let loaded = s.load_state().unwrap().unwrap();

        assert_eq!(loaded.current_term, 10);
        assert_eq!(loaded.voted_for_node(), Some(node));
    }

    #[test]
    fn persist_state_with_no_vote() {
        let s = storage();

        s.persist_state(5, None).unwrap();
        let loaded = s.load_state().unwrap().unwrap();

        assert_eq!(loaded.current_term, 5);
        assert!(loaded.voted_for_node().is_none());
    }

    #[test]
    fn append_and_load_log() {
        let s = storage();

        let e1 = LogEntry::create(1, 1, RaftCommand::Noop);
        let e2 = LogEntry::create(2, 1, RaftCommand::Noop);
        let e3 = LogEntry::create(3, 2, RaftCommand::Noop);

        s.append_log_entry(&e1).unwrap();
        s.append_log_entry(&e3).unwrap();
        s.append_log_entry(&e2).unwrap();

        let log = s.load_log().unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].index, 1);
        assert_eq!(log[1].index, 2);
        assert_eq!(log[2].index, 3);
    }

    #[test]
    fn truncate_log() {
        let s = storage();

        for i in 1..=5 {
            let entry = LogEntry::create(i, 1, RaftCommand::Noop);
            s.append_log_entry(&entry).unwrap();
        }

        s.truncate_log_from(3).unwrap();

        let log = s.load_log().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].index, 1);
        assert_eq!(log[1].index, 2);
    }

    #[test]
    fn persistent_state_bebytes_roundtrip() {
        let state = RaftPersistentState::create(42, NodeId::validated(7));
        let bytes = state.to_be_bytes();
        let (parsed, _) = RaftPersistentState::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(state, parsed);
    }
}
