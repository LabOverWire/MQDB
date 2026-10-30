// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::backend::{BatchOperations, StorageBackend};
use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, Slice};
use std::path::Path;
use std::sync::{Arc, Mutex};

fn fits_key_limit(key: &[u8]) -> bool {
    u16::try_from(key.len()).is_ok()
}

fn oversized_key_error(key: &[u8]) -> crate::error::Error {
    crate::error::Error::InvalidKey(format!(
        "key of {} bytes exceeds the {} byte storage limit",
        key.len(),
        u16::MAX
    ))
}

pub struct FjallBackend {
    db: Database,
    keyspace: Keyspace,
    durability: DurabilityMode,
    commit_lock: Arc<Mutex<()>>,
}

impl FjallBackend {
    /// # Errors
    /// Returns an error if the storage fails to open.
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(fjall::Error::Io)?;
        let db = Database::builder(path.as_ref()).open()?;
        let keyspace = db.keyspace("main", KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            keyspace,
            durability,
            commit_lock: Arc::new(Mutex::new(())),
        })
    }

    fn sync_if_needed(&self) -> Result<()> {
        match self.durability {
            DurabilityMode::Immediate => {
                self.db.persist(PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }
        Ok(())
    }
}

impl StorageBackend for FjallBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !fits_key_limit(key) {
            return Ok(None);
        }
        Ok(self.keyspace.get(key)?.map(|v| v.to_vec()))
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if !fits_key_limit(key) {
            return Err(oversized_key_error(key));
        }
        self.keyspace.insert(key, value)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        if !fits_key_limit(key) {
            return Ok(());
        }
        self.keyspace.remove(key)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            let (k, v) = guard.into_inner()?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        let snapshot = self.db.snapshot();
        let mut count = 0;
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            let _ = guard.key()?;
            count += 1;
        }
        Ok(count)
    }

    fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            results.push(guard.key()?.to_vec());
        }
        Ok(results)
    }

    fn prefix_scan_batch(
        &self,
        prefix: &[u8],
        batch_size: usize,
        after_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.db.snapshot();
        let mut results = Vec::with_capacity(batch_size);
        let start: Vec<u8> = if let Some(after) = after_key {
            let mut s = after.to_vec();
            s.push(0);
            s
        } else {
            prefix.to_vec()
        };
        for guard in snapshot.range(&self.keyspace, start..) {
            let (k, v) = guard.into_inner()?;
            if !k.starts_with(prefix) {
                break;
            }
            results.push((k.to_vec(), v.to_vec()));
            if results.len() >= batch_size {
                break;
            }
        }
        Ok(results)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in snapshot.range(&self.keyspace, start..end) {
            let (k, v) = guard.into_inner()?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(FjallBatch {
            db: self.db.clone(),
            keyspace: self.keyspace.clone(),
            durability: self.durability,
            operations: Vec::new(),
            preconditions: Vec::new(),
            commit_lock: Arc::clone(&self.commit_lock),
        })
    }

    fn flush(&self) -> Result<()> {
        self.sync_if_needed()
    }

    fn sync(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

enum Precondition {
    Equals {
        key: Vec<u8>,
        expected_value: Vec<u8>,
    },
    Absent {
        key: Vec<u8>,
    },
}

pub struct FjallBatch {
    db: Database,
    keyspace: Keyspace,
    durability: DurabilityMode,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
    commit_lock: Arc<Mutex<()>>,
}

impl BatchOperations for FjallBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.preconditions.push(Precondition::Equals {
            key,
            expected_value,
        });
    }

    fn expect_absent(&mut self, key: Vec<u8>) {
        self.preconditions.push(Precondition::Absent { key });
    }

    fn commit(self: Box<Self>) -> Result<()> {
        if let Some(BatchOp::Insert(key, _)) = self
            .operations
            .iter()
            .find(|op| matches!(op, BatchOp::Insert(key, _) if !fits_key_limit(key)))
        {
            return Err(oversized_key_error(key));
        }
        {
            let _guard = self
                .commit_lock
                .lock()
                .map_err(|e| crate::error::Error::Internal(e.to_string()))?;

            let snapshot = self.db.snapshot();
            for precondition in &self.preconditions {
                match precondition {
                    Precondition::Equals {
                        key,
                        expected_value,
                    } => {
                        let actual: Option<Slice> = if fits_key_limit(key) {
                            snapshot.get(&self.keyspace, key)?
                        } else {
                            None
                        };
                        match actual {
                            Some(val) if val.as_ref() == expected_value.as_slice() => {}
                            _ => {
                                return Err(crate::error::Error::Conflict(
                                    "optimistic lock failed: value was modified".into(),
                                ));
                            }
                        }
                    }
                    Precondition::Absent { key } => {
                        if fits_key_limit(key) && snapshot.get(&self.keyspace, key)?.is_some() {
                            return Err(crate::error::Error::AbsentPreconditionViolated(
                                key.clone(),
                            ));
                        }
                    }
                }
            }

            let mut batch = self.db.batch();
            for op in self.operations {
                match op {
                    BatchOp::Insert(k, v) => batch.insert(&self.keyspace, k, v),
                    BatchOp::Remove(k) if fits_key_limit(&k) => batch.remove(&self.keyspace, k),
                    BatchOp::Remove(_) => {}
                }
            }
            batch.commit()?;
        }

        match self.durability {
            DurabilityMode::Immediate => {
                self.db.persist(PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_backend(dir: &tempfile::TempDir) -> FjallBackend {
        FjallBackend::open(dir.path(), DurabilityMode::None).unwrap()
    }

    #[test]
    fn oversized_key_is_refused_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let key = vec![b'k'; 70_000];

        assert!(matches!(
            backend.insert(&key, b"v"),
            Err(crate::error::Error::InvalidKey(_))
        ));
        assert_eq!(backend.get(&key).unwrap(), None);
        backend.remove(&key).unwrap();

        let mut batch = backend.batch();
        batch.insert(b"small".to_vec(), b"v".to_vec());
        batch.insert(key.clone(), b"v".to_vec());
        assert!(matches!(
            batch.commit(),
            Err(crate::error::Error::InvalidKey(_))
        ));
        assert_eq!(backend.get(b"small").unwrap(), None);

        let mut batch = backend.batch();
        batch.expect_absent(key.clone());
        batch.remove(key.clone());
        batch.insert(b"small".to_vec(), b"v".to_vec());
        batch.commit().unwrap();
        assert_eq!(backend.get(b"small").unwrap(), Some(b"v".to_vec()));

        let at_limit = vec![b'k'; usize::from(u16::MAX)];
        backend.insert(&at_limit, b"v").unwrap();
        assert_eq!(backend.get(&at_limit).unwrap(), Some(b"v".to_vec()));

        let mut batch = backend.batch();
        batch.expect_value(key, b"v".to_vec());
        assert!(matches!(
            batch.commit(),
            Err(crate::error::Error::Conflict(_))
        ));
    }
}
