// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::backend::{BatchOperations, StorageBackend};
use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

pub struct MemoryBackend {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryBackend {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self
            .data
            .read()
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(data.get(key).cloned())
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::Internal(e.to_string()))?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::Internal(e.to_string()))?;
        data.remove(key);
        Ok(())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = self
            .data
            .read()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let results: Vec<_> = data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = self
            .data
            .read()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let results: Vec<_> = data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(MemoryBatch {
            data: Arc::clone(&self.data),
            operations: Vec::new(),
            preconditions: Vec::new(),
        })
    }

    fn flush(&self) -> Result<()> {
        Ok(())
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

pub struct MemoryBatch {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
}

impl BatchOperations for MemoryBatch {
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
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::Internal(e.to_string()))?;

        for precondition in &self.preconditions {
            let actual = data.get(&precondition.key);
            match actual {
                Some(val) if val.as_slice() == precondition.expected_value.as_slice() => {}
                _ => {
                    return Err(Error::Conflict(
                        "optimistic lock failed: value was modified".into(),
                    ));
                }
            }
        }

        for op in self.operations {
            match op {
                BatchOp::Insert(k, v) => {
                    data.insert(k, v);
                }
                BatchOp::Remove(k) => {
                    data.remove(&k);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let backend = MemoryBackend::new();

        StorageBackend::insert(&backend, b"key1", b"value1").unwrap();
        assert_eq!(
            StorageBackend::get(&backend, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );

        StorageBackend::remove(&backend, b"key1").unwrap();
        assert_eq!(StorageBackend::get(&backend, b"key1").unwrap(), None);
    }

    #[test]
    fn test_prefix_scan() {
        let backend = MemoryBackend::new();

        StorageBackend::insert(&backend, b"users/1", b"alice").unwrap();
        StorageBackend::insert(&backend, b"users/2", b"bob").unwrap();
        StorageBackend::insert(&backend, b"posts/1", b"hello").unwrap();

        let results = StorageBackend::prefix_scan(&backend, b"users/").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_batch_commit() {
        let backend = MemoryBackend::new();

        let mut batch = StorageBackend::batch(&backend);
        batch.insert(b"key1".to_vec(), b"value1".to_vec());
        batch.insert(b"key2".to_vec(), b"value2".to_vec());
        batch.commit().unwrap();

        assert_eq!(
            StorageBackend::get(&backend, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            StorageBackend::get(&backend, b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_optimistic_lock_success() {
        let backend = MemoryBackend::new();
        StorageBackend::insert(&backend, b"key1", b"value1").unwrap();

        let mut batch = StorageBackend::batch(&backend);
        batch.expect_value(b"key1".to_vec(), b"value1".to_vec());
        batch.insert(b"key1".to_vec(), b"value2".to_vec());
        batch.commit().unwrap();

        assert_eq!(
            StorageBackend::get(&backend, b"key1").unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_optimistic_lock_failure() {
        let backend = MemoryBackend::new();
        StorageBackend::insert(&backend, b"key1", b"value1").unwrap();

        let mut batch = StorageBackend::batch(&backend);
        batch.expect_value(b"key1".to_vec(), b"wrong_value".to_vec());
        batch.insert(b"key1".to_vec(), b"value2".to_vec());

        let result = batch.commit();
        assert!(result.is_err());
        assert_eq!(
            StorageBackend::get(&backend, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
    }
}
