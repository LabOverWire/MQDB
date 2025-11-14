use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Config, Keyspace, PartitionHandle};
use std::path::Path;
use std::sync::Arc;

pub struct Storage {
    keyspace: Arc<Keyspace>,
    main: Arc<PartitionHandle>,
    durability: DurabilityMode,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        let keyspace = Config::new(path).open()?;
        let main = keyspace.open_partition("main", Default::default())?;

        Ok(Self {
            keyspace: Arc::new(keyspace),
            main: Arc::new(main),
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
        let mut results = Vec::new();
        for item in self.main.prefix(prefix) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        for item in self.main.range(start..end) {
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
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}

pub struct BatchWriter {
    keyspace: Arc<Keyspace>,
    partition: Arc<PartitionHandle>,
    durability: DurabilityMode,
    operations: Vec<BatchOp>,
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

impl BatchWriter {
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    pub fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    pub fn commit(self) -> Result<()> {
        let mut batch = self.keyspace.batch();

        for op in self.operations {
            match op {
                BatchOp::Insert(k, v) => batch.insert(&self.partition, &k, &v),
                BatchOp::Remove(k) => batch.remove(&self.partition, &k),
            }
        }

        batch.commit()?;

        match self.durability {
            DurabilityMode::Immediate => {
                self.keyspace.persist(fjall::PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) | DurabilityMode::None => {}
        }

        Ok(())
    }
}
