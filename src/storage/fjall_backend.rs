// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::backend::{BatchOperations, StorageBackend};
use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, Slice};
use std::path::Path;

pub struct FjallBackend {
    db: Database,
    keyspace: Keyspace,
    durability: DurabilityMode,
}

impl FjallBackend {
    /// # Errors
    /// Returns an error if the storage fails to open.
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        let db = Database::builder(path.as_ref()).open()?;
        let keyspace = db.keyspace("main", KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            keyspace,
            durability,
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
        Ok(self.keyspace.get(key)?.map(|v| v.to_vec()))
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.keyspace.insert(key, value)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
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
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            let (k, v) = guard.into_inner()?;
            if let Some(after) = after_key
                && k.as_ref() <= after
            {
                continue;
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
        })
    }

    fn flush(&self) -> Result<()> {
        self.sync_if_needed()
    }
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

struct Precondition {
    key: Vec<u8>,
    expected_value: Vec<u8>,
}

pub struct FjallBatch {
    db: Database,
    keyspace: Keyspace,
    durability: DurabilityMode,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
}

impl BatchOperations for FjallBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.preconditions.push(Precondition {
            key,
            expected_value,
        });
    }

    fn commit(self: Box<Self>) -> Result<()> {
        let snapshot = self.db.snapshot();

        for precondition in &self.preconditions {
            let actual: Option<Slice> = snapshot.get(&self.keyspace, &precondition.key)?;
            match actual {
                Some(val) if val.as_ref() == precondition.expected_value.as_slice() => {}
                _ => {
                    return Err(crate::error::Error::Conflict(
                        "optimistic lock failed: value was modified".into(),
                    ));
                }
            }
        }

        let mut batch = self.db.batch();

        for op in self.operations {
            match op {
                BatchOp::Insert(k, v) => batch.insert(&self.keyspace, k, v),
                BatchOp::Remove(k) => batch.remove(&self.keyspace, k),
            }
        }

        batch.commit()?;

        match self.durability {
            DurabilityMode::Immediate => {
                self.db.persist(PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }

        Ok(())
    }
}
