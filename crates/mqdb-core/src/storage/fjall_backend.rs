// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::backend::{BatchOperations, StorageBackend};
use crate::config::DurabilityMode;
use crate::error::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, Slice};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

fn fits_key_limit(key: &[u8]) -> bool {
    u16::try_from(key.len()).is_ok()
}

fn within_key_limit(key: &[u8]) -> &[u8] {
    &key[..key.len().min(usize::from(u16::MAX))]
}

fn lower_bound(start: &[u8]) -> Bound<&[u8]> {
    if fits_key_limit(start) {
        Bound::Included(start)
    } else {
        Bound::Excluded(within_key_limit(start))
    }
}

fn upper_bound(end: &[u8]) -> Bound<&[u8]> {
    if fits_key_limit(end) {
        Bound::Excluded(end)
    } else {
        Bound::Included(within_key_limit(end))
    }
}

fn oversized_key_error(key: &[u8]) -> crate::error::Error {
    crate::error::Error::KeyTooLarge(key.len())
}

#[derive(Clone)]
struct WriteDurability {
    mode: DurabilityMode,
    unsynced_writes: Arc<AtomicBool>,
}

impl WriteDurability {
    fn after_write(&self, db: &Database) -> Result<()> {
        match self.mode {
            DurabilityMode::Immediate => {
                db.persist(PersistMode::SyncAll)?;
            }
            DurabilityMode::PeriodicMs(_) => self.unsynced_writes.store(true, Ordering::Release),
            DurabilityMode::None => {}
        }
        Ok(())
    }
}

struct PeriodicSync {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl PeriodicSync {
    fn start<F>(interval: Duration, unsynced_writes: Arc<AtomicBool>, sync: F) -> Result<Self>
    where
        F: Fn() -> Result<()> + Send + 'static,
    {
        let (stop, stopped) = mpsc::channel::<()>();
        let worker = thread::Builder::new()
            .name("mqdb-periodic-sync".to_string())
            .spawn(move || {
                let mut failing = false;
                while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
                    if !unsynced_writes.swap(false, Ordering::AcqRel) {
                        continue;
                    }
                    match sync() {
                        Ok(()) if failing => {
                            failing = false;
                            info!("periodic storage sync recovered");
                        }
                        Ok(()) => {}
                        Err(error) => {
                            unsynced_writes.store(true, Ordering::Release);
                            if !failing {
                                failing = true;
                                warn!(%error, "periodic storage sync failed, retrying every interval");
                            }
                        }
                    }
                }
            })
            .map_err(fjall::Error::Io)?;
        Ok(Self {
            stop: Some(stop),
            worker: Some(worker),
        })
    }
}

impl Drop for PeriodicSync {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            warn!("periodic storage sync worker panicked");
        }
    }
}

pub struct FjallBackend {
    periodic_sync: Option<PeriodicSync>,
    db: Database,
    keyspace: Keyspace,
    durability: WriteDurability,
    commit_lock: Arc<Mutex<()>>,
}

impl FjallBackend {
    /// # Errors
    /// Returns an error if the storage fails to open.
    pub fn open<P: AsRef<Path>>(path: P, durability: DurabilityMode) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(fjall::Error::Io)?;
        let db = Database::builder(path.as_ref()).open()?;
        let keyspace = db.keyspace("main", KeyspaceCreateOptions::default)?;
        let unsynced_writes = Arc::new(AtomicBool::new(false));
        let periodic_sync = match durability {
            DurabilityMode::PeriodicMs(interval_ms) => {
                let synced_db = db.clone();
                Some(PeriodicSync::start(
                    Duration::from_millis(interval_ms.max(1)),
                    Arc::clone(&unsynced_writes),
                    move || Ok(synced_db.persist(PersistMode::SyncAll)?),
                )?)
            }
            DurabilityMode::Immediate | DurabilityMode::None => None,
        };

        Ok(Self {
            periodic_sync,
            db,
            keyspace,
            durability: WriteDurability {
                mode: durability,
                unsynced_writes,
            },
            commit_lock: Arc::new(Mutex::new(())),
        })
    }

    fn sync_if_needed(&self) -> Result<()> {
        self.durability.after_write(&self.db)
    }

    #[cfg(test)]
    fn has_unsynced_writes(&self) -> bool {
        self.durability.unsynced_writes.load(Ordering::Acquire)
    }
}

impl Drop for FjallBackend {
    fn drop(&mut self) {
        drop(self.periodic_sync.take());
    }
}

impl StorageBackend for FjallBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !fits_key_limit(key) {
            return Ok(None);
        }
        Ok(self.keyspace.get(key)?.map(|v| v.to_vec()))
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if !fits_key_limit(key) {
            return Err(oversized_key_error(key));
        }
        self.keyspace.insert(key, value)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        if !fits_key_limit(key) {
            return Ok(());
        }
        self.keyspace.remove(key)?;
        self.sync_if_needed()?;
        Ok(())
    }

    fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !fits_key_limit(prefix) {
            return Ok(Vec::new());
        }
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            let (k, v) = guard.into_inner()?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        if !fits_key_limit(prefix) {
            return Ok(0);
        }
        let snapshot = self.db.snapshot();
        let mut count = 0;
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            let _ = guard.key()?;
            count += 1;
        }
        Ok(count)
    }

    fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        if !fits_key_limit(prefix) {
            return Ok(Vec::new());
        }
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in snapshot.prefix(&self.keyspace, prefix) {
            results.push(guard.key()?.to_vec());
        }
        Ok(results)
    }

    fn prefix_scan_batch(
        &self,
        prefix: &[u8],
        batch_size: usize,
        after_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !fits_key_limit(prefix) {
            return Ok(Vec::new());
        }
        let snapshot = self.db.snapshot();
        let mut results = Vec::with_capacity(batch_size);
        let start = after_key.map_or(Bound::Included(prefix), |after| {
            Bound::Excluded(within_key_limit(after))
        });
        for guard in snapshot.range::<&[u8], _>(&self.keyspace, (start, Bound::Unbounded)) {
            let (k, v) = guard.into_inner()?;
            if !k.starts_with(prefix) {
                break;
            }
            results.push((k.to_vec(), v.to_vec()));
            if results.len() >= batch_size {
                break;
            }
        }
        Ok(results)
    }

    fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.db.snapshot();
        let mut results = Vec::new();
        for guard in
            snapshot.range::<&[u8], _>(&self.keyspace, (lower_bound(start), upper_bound(end)))
        {
            let (k, v) = guard.into_inner()?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    fn batch(&self) -> Box<dyn BatchOperations> {
        Box::new(FjallBatch {
            db: self.db.clone(),
            keyspace: self.keyspace.clone(),
            durability: self.durability.clone(),
            operations: Vec::new(),
            preconditions: Vec::new(),
            commit_lock: Arc::clone(&self.commit_lock),
        })
    }

    fn flush(&self) -> Result<()> {
        self.sync_if_needed()
    }

    fn sync(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

enum Precondition {
    Equals {
        key: Vec<u8>,
        expected_value: Vec<u8>,
    },
    Absent {
        key: Vec<u8>,
    },
}

pub struct FjallBatch {
    db: Database,
    keyspace: Keyspace,
    durability: WriteDurability,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
    commit_lock: Arc<Mutex<()>>,
}

impl BatchOperations for FjallBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.preconditions.push(Precondition::Equals {
            key,
            expected_value,
        });
    }

    fn expect_absent(&mut self, key: Vec<u8>) {
        self.preconditions.push(Precondition::Absent { key });
    }

    fn commit(self: Box<Self>) -> Result<()> {
        if let Some(BatchOp::Insert(key, _)) = self
            .operations
            .iter()
            .find(|op| matches!(op, BatchOp::Insert(key, _) if !fits_key_limit(key)))
        {
            return Err(oversized_key_error(key));
        }
        {
            let _guard = self
                .commit_lock
                .lock()
                .map_err(|e| crate::error::Error::Internal(e.to_string()))?;

            let snapshot = self.db.snapshot();
            for precondition in &self.preconditions {
                match precondition {
                    Precondition::Equals {
                        key,
                        expected_value,
                    } => {
                        let actual: Option<Slice> = if fits_key_limit(key) {
                            snapshot.get(&self.keyspace, key)?
                        } else {
                            None
                        };
                        match actual {
                            Some(val) if val.as_ref() == expected_value.as_slice() => {}
                            _ => {
                                return Err(crate::error::Error::Conflict(
                                    "optimistic lock failed: value was modified".into(),
                                ));
                            }
                        }
                    }
                    Precondition::Absent { key } => {
                        if fits_key_limit(key) && snapshot.get(&self.keyspace, key)?.is_some() {
                            return Err(crate::error::Error::AbsentPreconditionViolated(
                                key.clone(),
                            ));
                        }
                    }
                }
            }

            let mut batch = self.db.batch();
            for op in self.operations {
                match op {
                    BatchOp::Insert(k, v) => batch.insert(&self.keyspace, k, v),
                    BatchOp::Remove(k) if fits_key_limit(&k) => batch.remove(&self.keyspace, k),
                    BatchOp::Remove(_) => {}
                }
            }
            batch.commit()?;
        }

        self.durability.after_write(&self.db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_backend(dir: &tempfile::TempDir) -> FjallBackend {
        FjallBackend::open(dir.path(), DurabilityMode::None).unwrap()
    }

    fn wait_until(condition: impl Fn() -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if condition() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        condition()
    }

    #[test]
    fn periodic_mode_marks_every_write_kind_unsynced() {
        let dir = tempfile::tempdir().unwrap();
        let backend =
            FjallBackend::open(dir.path(), DurabilityMode::PeriodicMs(3_600_000)).unwrap();
        let unsynced = &backend.durability.unsynced_writes;

        backend.insert(b"k", b"v").unwrap();
        assert!(unsynced.swap(false, Ordering::AcqRel));

        let mut batch = backend.batch();
        batch.insert(b"b".to_vec(), b"v".to_vec());
        batch.commit().unwrap();
        assert!(unsynced.swap(false, Ordering::AcqRel));

        backend.remove(b"k").unwrap();
        assert!(unsynced.swap(false, Ordering::AcqRel));

        assert!(backend.get(b"b").unwrap().is_some());
        assert!(!unsynced.load(Ordering::Acquire));
    }

    #[test]
    fn periodic_mode_syncs_writes_and_batches_within_the_interval() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FjallBackend::open(dir.path(), DurabilityMode::PeriodicMs(10)).unwrap();

        backend.insert(b"k", b"v").unwrap();
        assert!(wait_until(|| !backend.has_unsynced_writes()));

        let mut batch = backend.batch();
        batch.insert(b"b".to_vec(), b"v".to_vec());
        batch.commit().unwrap();
        assert!(wait_until(|| !backend.has_unsynced_writes()));

        backend.remove(b"k").unwrap();
        assert!(wait_until(|| !backend.has_unsynced_writes()));
    }

    #[test]
    fn immediate_and_none_modes_leave_nothing_for_a_timer() {
        for mode in [DurabilityMode::Immediate, DurabilityMode::None] {
            let dir = tempfile::tempdir().unwrap();
            let backend = FjallBackend::open(dir.path(), mode).unwrap();
            backend.insert(b"k", b"v").unwrap();
            assert!(!backend.has_unsynced_writes());
            assert!(backend.periodic_sync.is_none());
        }
    }

    #[test]
    fn periodic_sync_runs_only_after_writes_and_retries_failures() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fail_first = Arc::new(AtomicBool::new(true));
        let unsynced = Arc::new(AtomicBool::new(false));
        let counted = Arc::clone(&calls);
        let failing = Arc::clone(&fail_first);
        let worker =
            PeriodicSync::start(Duration::from_millis(5), Arc::clone(&unsynced), move || {
                counted.fetch_add(1, Ordering::SeqCst);
                if failing.swap(false, Ordering::SeqCst) {
                    return Err(crate::error::Error::Internal("disk".into()));
                }
                Ok(())
            })
            .unwrap();

        thread::sleep(Duration::from_millis(50));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        unsynced.store(true, Ordering::Release);
        assert!(wait_until(|| calls.load(Ordering::SeqCst) == 2));
        assert!(!unsynced.load(Ordering::Acquire));

        thread::sleep(Duration::from_millis(50));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(worker);
    }

    #[test]
    fn dropping_the_backend_stops_the_sync_worker_without_waiting_an_interval() {
        let dir = tempfile::tempdir().unwrap();
        let backend =
            FjallBackend::open(dir.path(), DurabilityMode::PeriodicMs(3_600_000)).unwrap();
        backend.insert(b"k", b"v").unwrap();
        let started = std::time::Instant::now();
        drop(backend);
        assert!(started.elapsed() < Duration::from_secs(1));

        let reopened = FjallBackend::open(dir.path(), DurabilityMode::None).unwrap();
        assert_eq!(reopened.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn oversized_key_is_refused_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let key = vec![b'k'; 70_000];

        assert!(matches!(
            backend.insert(&key, b"v"),
            Err(crate::error::Error::KeyTooLarge(_))
        ));
        assert_eq!(backend.get(&key).unwrap(), None);
        backend.remove(&key).unwrap();

        let mut batch = backend.batch();
        batch.insert(b"small".to_vec(), b"v".to_vec());
        batch.insert(key.clone(), b"v".to_vec());
        assert!(matches!(
            batch.commit(),
            Err(crate::error::Error::KeyTooLarge(_))
        ));
        assert_eq!(backend.get(b"small").unwrap(), None);

        let mut batch = backend.batch();
        batch.expect_absent(key.clone());
        batch.remove(key.clone());
        batch.insert(b"small".to_vec(), b"v".to_vec());
        batch.commit().unwrap();
        assert_eq!(backend.get(b"small").unwrap(), Some(b"v".to_vec()));

        let at_limit = vec![b'k'; usize::from(u16::MAX)];
        backend.insert(&at_limit, b"v").unwrap();
        assert_eq!(backend.get(&at_limit).unwrap(), Some(b"v".to_vec()));

        let mut batch = backend.batch();
        batch.expect_value(key, b"v".to_vec());
        assert!(matches!(
            batch.commit(),
            Err(crate::error::Error::Conflict(_))
        ));
    }

    #[test]
    fn scans_with_bounds_past_the_key_limit_do_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let at_limit = vec![b'k'; usize::from(u16::MAX)];
        backend.insert(&at_limit, b"v").unwrap();
        backend.insert(b"k", b"short").unwrap();
        let long = vec![b'k'; 70_000];

        assert!(backend.prefix_scan(&long).unwrap().is_empty());
        assert_eq!(backend.prefix_count(&long).unwrap(), 0);
        assert!(backend.prefix_scan_keys(&long).unwrap().is_empty());
        assert!(
            backend
                .prefix_scan_batch(b"k", 10, Some(&at_limit))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            backend.prefix_scan_batch(b"k", 10, Some(b"k")).unwrap(),
            vec![(at_limit.clone(), b"v".to_vec())]
        );
        assert_eq!(
            backend.range_scan(b"k", &long).unwrap(),
            vec![
                (b"k".to_vec(), b"short".to_vec()),
                (at_limit.clone(), b"v".to_vec())
            ]
        );
        assert!(
            backend
                .range_scan(&long, &[b'z'; 70_000])
                .unwrap()
                .is_empty()
        );
    }
}
