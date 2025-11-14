use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Config, TransactionalKeyspace, TransactionalPartitionHandle, WriteTransaction, ReadTransaction};
use std::path::Path;
use std::sync::Arc;

pub struct Storage {
    pub keyspace: Arc<TransactionalKeyspace>,
    pub main: Arc<TransactionalPartitionHandle>,
    pub durability: DurabilityMode,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        let keyspace = Config::new(path).open_transactional()?;
        let main = keyspace.open_partition("main", Default::default())?;

        Ok(Self {
            keyspace: Arc::new(keyspace),
            main: Arc::new(main),
            durability,
        })
    }

    pub fn write_tx(&self) -> WriteTransaction {
        self.keyspace.write_tx()
    }

    pub fn read_tx(&self) -> ReadTransaction {
        self.keyspace.read_tx()
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

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.main.get(key)?.map(|v| v.to_vec()))
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.main.insert(key, value)?;
        self.sync_if_needed()?;
        Ok(())
    }

    pub fn remove(&self, key: &[u8]) -> Result<()> {
        self.main.remove(key)?;
        self.sync_if_needed()?;
        Ok(())
    }

    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.read_tx();
        let mut results = Vec::new();
        for item in tx.prefix(&self.main, prefix) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.read_tx();
        let mut results = Vec::new();
        for item in tx.range(&self.main, start..end) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    pub fn batch(&self) -> BatchWriter {
        BatchWriter {
            keyspace: Arc::clone(&self.keyspace),
            partition: Arc::clone(&self.main),
            durability: self.durability.clone(),
            operations: Vec::new(),
            preconditions: Vec::new(),
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}

pub struct BatchWriter {
    keyspace: Arc<TransactionalKeyspace>,
    partition: Arc<TransactionalPartitionHandle>,
    durability: DurabilityMode,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

struct Precondition {
    key: Vec<u8>,
    expected_value: Vec<u8>,
}

impl BatchWriter {
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    pub fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    pub fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.preconditions.push(Precondition { key, expected_value });
    }

    pub fn commit(self) -> Result<()> {
        let mut tx = self.keyspace.write_tx();

        for precondition in &self.preconditions {
            let actual = tx.get(&self.partition, &precondition.key)?;
            match actual {
                Some(val) if val.as_ref() == precondition.expected_value.as_slice() => {},
                _ => {
                    return Err(crate::error::Error::Conflict(
                        "optimistic lock failed: value was modified".into()
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
