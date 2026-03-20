// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod constraints;
mod crud;
mod crypto;
mod cursor;
mod encoding;
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
use mqdb_core::storage::{AsyncStorageBackend, Storage};
use mqdb_core::{
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
    CountOptions, CursorOptions, DatabaseInner, FieldDefJs, FilterJs, ForeignKeyEntry, ListOptions,
    PaginationJs, Relationship, SchemaDefinition, SortOrderJs, StorageKind, SubscriptionEntry,
    SubscriptionMode, deserialize_js, serialize_event, serialize_js,
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
        Self::with_storage(StorageKind::memory(storage))
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn open_persistent(db_name: &str) -> Result<WasmDatabase, JsValue> {
        let backend = IdbBackend::open(db_name)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Self::with_storage(StorageKind::indexed_db(backend)))
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn open_encrypted(db_name: &str, passphrase: &str) -> Result<WasmDatabase, JsValue> {
        use crypto::{CHECK_KEY, CHECK_PLAINTEXT, CryptoHandle, SALT_KEY, generate_salt};
        use mqdb_core::storage::AsyncStorageBackend;

        let backend = IdbBackend::open(db_name)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let existing_salt = backend
            .get(SALT_KEY)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let salt = if let Some(s) = existing_salt {
            s
        } else {
            let new_salt = generate_salt().map_err(|e| JsValue::from_str(&e.to_string()))?;
            backend
                .insert(SALT_KEY, &new_salt)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            new_salt.to_vec()
        };

        let handle = CryptoHandle::derive_from_passphrase(passphrase, &salt)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let existing_check = backend
            .get(CHECK_KEY)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        if let Some(encrypted_check) = existing_check {
            handle
                .decrypt(CHECK_KEY, &encrypted_check)
                .await
                .map_err(|_| JsValue::from_str("invalid passphrase"))?;
        } else {
            let encrypted = handle
                .encrypt(CHECK_KEY, CHECK_PLAINTEXT)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            backend
                .insert(CHECK_KEY, &encrypted)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }

        Ok(Self::with_storage(StorageKind::encrypted_indexed_db(
            backend, handle,
        )))
    }
}

impl WasmDatabase {
    fn with_storage(storage: StorageKind) -> Self {
        Self {
            storage: Rc::new(storage),
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
