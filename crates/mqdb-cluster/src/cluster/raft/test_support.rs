// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_core::error::Result;
use mqdb_core::storage::{BatchOperations, MemoryBackend, StorageBackend};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct FlushFailingBackend {
    inner: MemoryBackend,
    fail_flush: AtomicBool,
}

impl FlushFailingBackend {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::new(),
            fail_flush: AtomicBool::new(false),
        })
    }

    pub(crate) fn fail_flushes(&self, fail: bool) {
        self.fail_flush.store(fail, Ordering::SeqCst);
    }
}

impl StorageBackend for FlushFailingBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.insert(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
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
        self.inner.batch()
    }

    fn flush(&self) -> Result<()> {
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(mqdb_core::error::Error::StorageGeneric(
                "flush failed".into(),
            ));
        }
        self.inner.flush()
    }
}
