use crate::error::Result;

pub trait StorageBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;
    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn batch(&self) -> Box<dyn BatchOperations>;
    fn flush(&self) -> Result<()>;
}

pub trait BatchOperations: Send {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn remove(&mut self, key: Vec<u8>);
    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>);
    fn commit(self: Box<Self>) -> Result<()>;
}
