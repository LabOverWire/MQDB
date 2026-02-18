// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster::protocol::{
    FkCheckResponse, FkReverseLookupResponse, UniqueReserveResponse, UniqueReserveStatus,
};
use tokio::sync::oneshot;

pub struct PendingConstraintState {
    unique_requests: Mutex<HashMap<u64, oneshot::Sender<UniqueReserveStatus>>>,
    fk_checks: Mutex<HashMap<u64, oneshot::Sender<bool>>>,
    fk_lookups: Mutex<HashMap<u64, oneshot::Sender<Vec<String>>>>,
    next_unique_id: AtomicU64,
    next_fk_id: AtomicU64,
}

impl PendingConstraintState {
    pub fn new() -> Self {
        Self {
            unique_requests: Mutex::new(HashMap::new()),
            fk_checks: Mutex::new(HashMap::new()),
            fk_lookups: Mutex::new(HashMap::new()),
            next_unique_id: AtomicU64::new(1),
            next_fk_id: AtomicU64::new(1),
        }
    }

    pub fn allocate_unique_id(&self) -> u64 {
        self.next_unique_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn allocate_fk_id(&self) -> u64 {
        self.next_fk_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn insert_unique(&self, id: u64, tx: oneshot::Sender<UniqueReserveStatus>) {
        self.unique_requests.lock().unwrap().insert(id, tx);
    }

    pub fn resolve_unique(&self, resp: &UniqueReserveResponse) {
        if let Some(tx) = self
            .unique_requests
            .lock()
            .unwrap()
            .remove(&resp.request_id)
        {
            let _ = tx.send(resp.status());
        }
    }

    pub fn insert_fk_check(&self, id: u64, tx: oneshot::Sender<bool>) {
        self.fk_checks.lock().unwrap().insert(id, tx);
    }

    pub fn resolve_fk_check(&self, resp: &FkCheckResponse) {
        if let Some(tx) = self.fk_checks.lock().unwrap().remove(&resp.request_id) {
            let _ = tx.send(resp.exists());
        }
    }

    pub fn insert_fk_lookup(&self, id: u64, tx: oneshot::Sender<Vec<String>>) {
        self.fk_lookups.lock().unwrap().insert(id, tx);
    }

    pub fn resolve_fk_lookup(&self, resp: &FkReverseLookupResponse) {
        if let Some(tx) = self.fk_lookups.lock().unwrap().remove(&resp.request_id) {
            let _ = tx.send(resp.referencing_ids());
        }
    }
}
