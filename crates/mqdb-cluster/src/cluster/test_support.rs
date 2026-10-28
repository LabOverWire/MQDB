// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_core::error::{Error, Result};
use mqdb_core::storage::{BatchOperations, MemoryBackend, StorageBackend};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
pub(crate) struct FaultyBackend {
    inner: MemoryBackend,
    fail_flush: AtomicBool,
    fail_writes: AtomicBool,
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

    fn write_failure(&self) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
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
        self.write_failure()?;
        self.inserts.fetch_add(1, Ordering::SeqCst);
        self.inner.insert(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.write_failure()?;
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
            fail: self.fail_writes.load(Ordering::SeqCst),
        })
    }

    fn flush(&self) -> Result<()> {
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(Error::StorageGeneric("flush failed".into()));
        }
        self.inner.flush()
    }
}

struct FaultyBatch {
    inner: Box<dyn BatchOperations>,
    fail: bool,
}

impl BatchOperations for FaultyBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, value);
    }

    fn remove(&mut self, key: Vec<u8>) {
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
