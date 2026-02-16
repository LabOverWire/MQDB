// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod backend;
#[cfg(feature = "agent")]
mod encrypted_backend;
#[cfg(feature = "agent")]
mod fjall_backend;
mod memory_backend;

pub use backend::{AsyncBatchOperations, AsyncStorageBackend, BatchOperations, StorageBackend};
#[cfg(feature = "agent")]
pub use encrypted_backend::EncryptedBackend;
#[cfg(feature = "agent")]
pub use fjall_backend::FjallBackend;
pub use memory_backend::MemoryBackend;

use crate::error::Result;
use std::sync::Arc;

pub struct Storage {
    backend: Arc<dyn StorageBackend>,
}

impl Storage {
    /// # Errors
    /// Returns an error if the storage backend fails to open.
    #[cfg(feature = "agent")]
    pub fn open<P: AsRef<std::path::Path>>(
        path: P,
        durability: crate::config::DurabilityMode,
    ) -> Result<Self> {
        let backend = FjallBackend::open(path, durability)?;
        Ok(Self {
            backend: Arc::new(backend),
        })
    }

    /// # Errors
    /// Returns an error if the storage backend fails to open or passphrase is invalid.
    #[cfg(feature = "agent")]
    pub fn open_encrypted<P: AsRef<std::path::Path>>(
        path: P,
        passphrase: &str,
        durability: crate::config::DurabilityMode,
    ) -> Result<Self> {
        let inner = Arc::new(FjallBackend::open(path, durability)?);
        let encrypted = EncryptedBackend::open(inner, passphrase)?;
        Ok(Self {
            backend: Arc::new(encrypted),
        })
    }

    #[allow(clippy::must_use_candidate)]
    pub fn memory() -> Self {
        Self {
            backend: Arc::new(MemoryBackend::new()),
        }
    }

    pub fn with_backend(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.backend.get(key)
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.backend.insert(key, value)
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn remove(&self, key: &[u8]) -> Result<()> {
        self.backend.remove(key)
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.prefix_scan(prefix)
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.range_scan(start, end)
    }

    #[allow(clippy::must_use_candidate)]
    pub fn batch(&self) -> BatchWriter {
        BatchWriter {
            inner: self.backend.batch(),
        }
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub fn flush(&self) -> Result<()> {
        self.backend.flush()
    }
}

pub struct BatchWriter {
    inner: Box<dyn BatchOperations>,
}

impl BatchWriter {
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, value);
    }

    pub fn remove(&mut self, key: Vec<u8>) {
        self.inner.remove(key);
    }

    pub fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.inner.expect_value(key, expected_value);
    }

    /// Commits all queued operations atomically.
    ///
    /// # Errors
    /// Returns an error if the commit fails or expected values don't match.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}

pub struct AsyncStorage<B: AsyncStorageBackend> {
    backend: B,
}

impl<B: AsyncStorageBackend> AsyncStorage<B> {
    #[allow(clippy::must_use_candidate)]
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.backend.get(key).await
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.backend.insert(key, value).await
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub async fn remove(&self, key: &[u8]) -> Result<()> {
        self.backend.remove(key).await
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.prefix_scan(prefix).await
    }

    /// # Errors
    /// Returns an error if the storage operation fails.
    pub async fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.range_scan(start, end).await
    }

    #[allow(clippy::must_use_candidate)]
    pub fn batch(&self) -> AsyncBatchWriter<B::Batch> {
        AsyncBatchWriter {
            inner: self.backend.batch(),
        }
    }

    /// # Errors
    /// Returns an error if the flush operation fails.
    pub async fn flush(&self) -> Result<()> {
        self.backend.flush().await
    }
}

pub struct AsyncBatchWriter<B: AsyncBatchOperations> {
    inner: B,
}

impl<B: AsyncBatchOperations> AsyncBatchWriter<B> {
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, value);
    }

    pub fn remove(&mut self, key: Vec<u8>) {
        self.inner.remove(key);
    }

    pub fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.inner.expect_value(key, expected_value);
    }

    /// # Errors
    /// Returns an error if the commit fails.
    pub async fn commit(self) -> Result<()> {
        self.inner.commit().await
    }
}
