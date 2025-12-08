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

    pub fn memory() -> Self {
        Self {
            backend: Arc::new(MemoryBackend::new()),
        }
    }

    pub fn with_backend(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.backend.get(key)
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.backend.insert(key, value)
    }

    pub fn remove(&self, key: &[u8]) -> Result<()> {
        self.backend.remove(key)
    }

    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.prefix_scan(prefix)
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.range_scan(start, end)
    }

    pub fn batch(&self) -> BatchWriter {
        BatchWriter {
            inner: self.backend.batch(),
        }
    }

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

    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}
