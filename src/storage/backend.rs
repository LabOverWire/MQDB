use crate::error::Result;
use std::future::Future;

/// Storage backend trait for key-value operations.
pub trait StorageBackend: Send + Sync {
    /// Gets a value by key.
    ///
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Inserts a key-value pair.
    ///
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Removes a key.
    ///
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn remove(&self, key: &[u8]) -> Result<()>;

    /// Scans all keys with the given prefix.
    ///
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Scans keys in the given range.
    ///
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Creates a new batch for atomic operations.
    fn batch(&self) -> Box<dyn BatchOperations>;

    /// Flushes pending writes to storage.
    ///
    /// # Errors
    /// Returns an error if the flush operation fails.
    fn flush(&self) -> Result<()>;
}

/// Batch operations for atomic writes.
pub trait BatchOperations: Send {
    /// Queues an insert operation.
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);

    /// Queues a remove operation.
    fn remove(&mut self, key: Vec<u8>);

    /// Sets an expected value for optimistic concurrency.
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);

    /// Commits all queued operations atomically.
    ///
    /// # Errors
    /// Returns an error if the commit fails or expected values don't match.
    fn commit(self: Box<Self>) -> Result<()>;
}

pub trait AsyncStorageBackend {
    type Batch: AsyncBatchOperations;

    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Vec<u8>>>>;
    fn insert(&self, key: &[u8], value: &[u8]) -> impl Future<Output = Result<()>>;
    fn remove(&self, key: &[u8]) -> impl Future<Output = Result<()>>;
    fn prefix_scan(&self, prefix: &[u8]) -> impl Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>>;
    fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>>;
    fn batch(&self) -> Self::Batch;
    fn flush(&self) -> impl Future<Output = Result<()>>;
}

pub trait AsyncBatchOperations {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    fn commit(self) -> impl Future<Output = Result<()>>;
}
