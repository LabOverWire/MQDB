// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::StoreManager;
use crate::cluster::protocol::ReplicationWrite;
use crate::cluster::{PartitionId, entity};
use std::collections::{HashMap, HashSet};
use std::sync::{PoisonError, RwLock};

pub(crate) const TRANSFER_WINDOW_CAPACITY: usize = 4096;

pub(super) const GLOBAL_SECTIONS: [&str; 2] = [entity::DB_SCHEMA, entity::DB_CONSTRAINT];

pub(super) type WrittenKey = (String, String);

#[derive(Debug, Default)]
struct TransferWindow {
    written: HashSet<WrittenKey>,
    overflowed: bool,
}

impl TransferWindow {
    fn note(&mut self, write: &ReplicationWrite) {
        if self.overflowed {
            return;
        }
        if self.written.len() >= TRANSFER_WINDOW_CAPACITY {
            self.overflowed = true;
            self.written.clear();
            return;
        }
        self.written
            .insert((write.entity.clone(), write.id.clone()));
    }
}

#[derive(Debug, Default)]
pub(super) struct TransferWindows(RwLock<HashMap<PartitionId, TransferWindow>>);

impl StoreManager {
    pub(crate) fn open_transfer_window(&self, partition: PartitionId) {
        self.transfer_windows
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(partition)
            .or_default();
    }

    pub(crate) fn close_transfer_window(&self, partition: PartitionId) {
        self.transfer_windows
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&partition);
    }

    pub(super) fn note_transfer_write(&self, write: &ReplicationWrite) {
        let global = GLOBAL_SECTIONS.contains(&write.entity.as_str());
        if !tracked(&write.entity)
            || self
                .transfer_windows
                .0
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        {
            return;
        }
        let mut windows = self
            .transfer_windows
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if global {
            windows.values_mut().for_each(|window| window.note(write));
        } else if let Some(window) = windows.get_mut(&write.partition) {
            window.note(write);
        }
    }

    pub(super) fn transfer_window_protection(&self, partition: PartitionId) -> Protection {
        match self
            .transfer_windows
            .0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&partition)
        {
            Some(window) => Protection {
                written: window.written.clone(),
                overflowed: window.overflowed,
            },
            None => Protection::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Protection {
    written: HashSet<WrittenKey>,
    overflowed: bool,
}

impl Protection {
    pub(super) fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub(super) fn keeps(&self, write: &ReplicationWrite) -> bool {
        !self.written.is_empty()
            && self
                .written
                .contains(&(write.entity.clone(), write.id.clone()))
    }
}

pub(super) const TRACKED_SECTIONS: [&str; 3] =
    [entity::DB_UNIQUE, entity::DB_SCHEMA, entity::DB_CONSTRAINT];

fn tracked(entity_name: &str) -> bool {
    TRACKED_SECTIONS.contains(&entity_name)
}
