// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::db::{DbDataStore, DbEntity};
use crate::cluster::protocol::Operation;
use mqdb_core::error::{Error, Result};
use mqdb_core::storage::{BatchOperations, MemoryBackend, StorageBackend};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct FaultyBackend {
    inner: MemoryBackend,
    fail_flush: AtomicBool,
    fail_writes: AtomicBool,
    failing_key: Mutex<Option<String>>,
    writes_left: Mutex<Option<usize>>,
    inserts: AtomicUsize,
}

impl FaultyBackend {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn fail_flushes(&self, fail: bool) {
        self.fail_flush.store(fail, Ordering::SeqCst);
    }

    pub(crate) fn fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }

    pub(crate) fn fail_writes_to_keys_containing(&self, needle: &str) {
        if let Ok(mut failing) = self.failing_key.lock() {
            *failing = Some(needle.to_string());
        }
    }

    pub(crate) fn fail_writes_after(&self, writes: usize) {
        if let Ok(mut left) = self.writes_left.lock() {
            *left = Some(writes);
        }
    }

    fn budget_exhausted(&self) -> bool {
        self.writes_left
            .lock()
            .ok()
            .is_some_and(|mut left| match left.as_mut() {
                Some(0) => true,
                Some(remaining) => {
                    *remaining -= 1;
                    false
                }
                None => false,
            })
    }

    pub(crate) fn stop_failing_keys(&self) {
        if let Ok(mut failing) = self.failing_key.lock() {
            *failing = None;
        }
    }

    fn failing_key(&self) -> Option<String> {
        self.failing_key
            .lock()
            .ok()
            .and_then(|failing| failing.clone())
    }

    pub(crate) fn inserts(&self) -> usize {
        self.inserts.load(Ordering::SeqCst)
    }

    pub(crate) fn keys_containing(&self, needle: &str) -> Result<usize> {
        Ok(self
            .inner
            .prefix_scan(b"")?
            .iter()
            .filter(|(key, _)| String::from_utf8_lossy(key).contains(needle))
            .count())
    }

    fn write_failure(&self, key: &[u8]) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst)
            || key_matches(self.failing_key().as_deref(), key)
            || self.budget_exhausted()
        {
            return Err(Error::StorageGeneric("write failed".into()));
        }
        Ok(())
    }
}

impl StorageBackend for FaultyBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_failure(key)?;
        self.inserts.fetch_add(1, Ordering::SeqCst);
        self.inner.insert(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.write_failure(key)?;
        self.inner.remove(key)
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.prefix_scan(prefix)
    }

    fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        self.inner.prefix_count(prefix)
    }

    fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.inner.prefix_scan_keys(prefix)
    }

    fn prefix_scan_batch(
        &self,
        prefix: &[u8],
        batch_size: usize,
        after_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.prefix_scan_batch(prefix, batch_size, after_key)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.range_scan(start, end)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(FaultyBatch {
            inner: self.inner.batch(),
            fail: self.fail_writes.load(Ordering::SeqCst) || self.budget_exhausted(),
            failing_key: self.failing_key(),
        })
    }

    fn flush(&self) -> Result<()> {
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(Error::StorageGeneric("flush failed".into()));
        }
        self.inner.flush()
    }
}

fn key_matches(needle: Option<&str>, key: &[u8]) -> bool {
    needle.is_some_and(|needle| String::from_utf8_lossy(key).contains(needle))
}

struct FaultyBatch {
    inner: Box<dyn BatchOperations>,
    fail: bool,
    failing_key: Option<String>,
}

impl BatchOperations for FaultyBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.fail |= key_matches(self.failing_key.as_deref(), &key);
        self.inner.insert(key, value);
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.fail |= key_matches(self.failing_key.as_deref(), &key);
        self.inner.remove(key);
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.inner.expect_value(key, expected_value);
    }

    fn expect_absent(&mut self, key: Vec<u8>) {
        self.inner.expect_absent(key);
    }

    fn commit(self: Box<Self>) -> Result<()> {
        if self.fail {
            return Err(Error::StorageGeneric("commit failed".into()));
        }
        self.inner.commit()
    }
}

pub(crate) fn put_record(
    store: &DbDataStore,
    entity: &str,
    id: &str,
    data: &[u8],
    timestamp_ms: u64,
) {
    let record = DbEntity::create(entity, id, data, timestamp_ms);
    store
        .apply_replicated(
            Operation::Insert,
            &record.key(),
            &DbDataStore::serialize(&record),
        )
        .unwrap();
}

pub(crate) fn remove_record(store: &DbDataStore, entity: &str, id: &str) {
    store
        .apply_replicated(Operation::Delete, &format!("{entity}/{id}"), &[])
        .unwrap();
}
