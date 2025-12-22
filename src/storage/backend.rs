use crate::error::Result;

pub trait StorageBackend: Send + Sync {
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn remove(&self, key: &[u8]) -> Result<()>;
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    /// # Errors
    /// Returns an error if the storage operation fails.
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn batch(&self) -> Box<dyn BatchOperations>;
    /// # Errors
    /// Returns an error if the flush operation fails.
    fn flush(&self) -> Result<()>;
}

pub trait BatchOperations: Send {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    /// # Errors
    /// Returns an error if the commit fails.
    fn commit(self: Box<Self>) -> Result<()>;
}
