// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod constraints;
mod crud;
mod cursor;
mod execute;
mod indexeddb;
mod query;
mod schema;
mod subscriptions;
mod types;

#[cfg(test)]
mod tests;

pub use cursor::WasmCursor;
pub use indexeddb::{IndexedDbBackend, IndexedDbBatch};

use indexeddb::IndexedDbBackend as IdbBackend;
use mqdb::storage::{AsyncStorageBackend, Storage};
use mqdb::{
    AdminOperation, ChangeEvent, FieldDefinition, FieldType, Filter, OnDeleteAction, Operation,
    Pagination, Request, Schema, SortDirection, SortOrder, build_request, match_pattern,
    parse_admin_topic, parse_db_topic,
};
use serde::{Deserialize, Serialize};
use std::cell::{Ref, RefCell, RefMut};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use types::{
    CursorOptions, DatabaseInner, FieldDefJs, FilterJs, ForeignKeyEntry, ListOptions, PaginationJs,
    Relationship, SchemaDefinition, SortOrderJs, StorageKind, SubscriptionEntry, SubscriptionMode,
    deserialize_js, serialize_event, serialize_js,
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct WasmDatabase {
    storage: Rc<StorageKind>,
    inner: Rc<RefCell<DatabaseInner>>,
}

#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> WasmDatabase {
        let storage = Arc::new(Storage::memory());
        WasmDatabase {
            storage: Rc::new(StorageKind::Memory(storage)),
            inner: Rc::new(RefCell::new(DatabaseInner {
                schemas: HashMap::new(),
                subscriptions: HashMap::new(),
                unique_constraints: HashMap::new(),
                not_null_constraints: HashMap::new(),
                foreign_keys: Vec::new(),
                indexes: HashMap::new(),
                id_counters: HashMap::new(),
                round_robin_counters: HashMap::new(),
                relationships: HashMap::new(),
            })),
        }
    }

    /// Opens a persistent database backed by `IndexedDB`.
    ///
    /// # Errors
    /// Returns an error if `IndexedDB` is unavailable or the database cannot be opened.
    pub async fn open_persistent(db_name: &str) -> Result<WasmDatabase, JsValue> {
        let backend = IdbBackend::open(db_name)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(WasmDatabase {
            storage: Rc::new(StorageKind::IndexedDb(backend)),
            inner: Rc::new(RefCell::new(DatabaseInner {
                schemas: HashMap::new(),
                subscriptions: HashMap::new(),
                unique_constraints: HashMap::new(),
                not_null_constraints: HashMap::new(),
                foreign_keys: Vec::new(),
                indexes: HashMap::new(),
                id_counters: HashMap::new(),
                round_robin_counters: HashMap::new(),
                relationships: HashMap::new(),
            })),
        })
    }
}

impl WasmDatabase {
    pub(crate) fn borrow_inner(&self) -> Result<Ref<'_, DatabaseInner>, JsValue> {
        self.inner
            .try_borrow()
            .map_err(|_| JsValue::from_str("database is busy (concurrent access)"))
    }

    pub(crate) fn borrow_inner_mut(&self) -> Result<RefMut<'_, DatabaseInner>, JsValue> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| JsValue::from_str("database is busy (concurrent mutable access)"))
    }
}

impl Default for WasmDatabase {
    fn default() -> Self {
        Self::new()
    }
}
