mod backend;
#[cfg(feature = "native")]
mod fjall_backend;
mod memory_backend;

pub use backend::{BatchOperations, StorageBackend};
#[cfg(feature = "native")]
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
    #[cfg(feature = "native")]
    pub fn open<P: AsRef<std::path::Path>>(
        path: P,
        durability: crate::config::DurabilityMode,
    ) -> Result<Self> {
        let backend = FjallBackend::open(path, durability)?;
        Ok(Self {
            backend: Arc::new(backend),
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

    /// # Errors
    /// Returns an error if the commit fails.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}
