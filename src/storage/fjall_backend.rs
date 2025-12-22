use super::backend::{BatchOperations, StorageBackend};
use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Config, TransactionalKeyspace, TransactionalPartitionHandle};
use std::path::Path;
use std::sync::Arc;

pub struct FjallBackend {
    keyspace: Arc<TransactionalKeyspace>,
    partition: Arc<TransactionalPartitionHandle>,
    durability: DurabilityMode,
}

impl FjallBackend {
    /// # Errors
    /// Returns an error if the storage fails to open.
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        let keyspace = Config::new(path).open_transactional()?;
        let partition =
            keyspace.open_partition("main", fjall::PartitionCreateOptions::default())?;

        Ok(Self {
            keyspace: Arc::new(keyspace),
            partition: Arc::new(partition),
            durability,
        })
    }

    fn sync_if_needed(&self) -> Result<()> {
        match self.durability {
            DurabilityMode::Immediate => {
                self.keyspace.persist(fjall::PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }
        Ok(())
    }
}

impl StorageBackend for FjallBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.partition.get(key)?.map(|v| v.to_vec()))
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.partition.insert(key, value)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.partition.remove(key)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.keyspace.read_tx();
        let mut results = Vec::new();
        for item in tx.prefix(&self.partition, prefix) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.keyspace.read_tx();
        let mut results = Vec::new();
        for item in tx.range(&self.partition, start..end) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(FjallBatch {
            keyspace: Arc::clone(&self.keyspace),
            partition: Arc::clone(&self.partition),
            durability: self.durability.clone(),
            operations: Vec::new(),
            preconditions: Vec::new(),
        })
    }

    fn flush(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
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

pub struct FjallBatch {
    keyspace: Arc<TransactionalKeyspace>,
    partition: Arc<TransactionalPartitionHandle>,
    durability: DurabilityMode,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
}

impl BatchOperations for FjallBatch {
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
        let mut tx = self.keyspace.write_tx();

        for precondition in &self.preconditions {
            let actual = tx.get(&self.partition, &precondition.key)?;
            match actual {
                Some(val) if val.as_ref() == precondition.expected_value.as_slice() => {}
                _ => {
                    return Err(crate::error::Error::Conflict(
                        "optimistic lock failed: value was modified".into(),
                    ));
                }
            }
        }

        for op in self.operations {
            match op {
                BatchOp::Insert(k, v) => tx.insert(&self.partition, &k, &v),
                BatchOp::Remove(k) => tx.remove(&self.partition, &k),
            }
        }

        tx.commit()?;

        match self.durability {
            DurabilityMode::Immediate => {
                self.keyspace.persist(fjall::PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }

        Ok(())
    }
}
