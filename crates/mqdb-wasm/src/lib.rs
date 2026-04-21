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
        let db = Self::with_storage(StorageKind::indexed_db(backend));
        db.load_metadata_async().await?;
        Ok(db)
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

        let db = Self::with_storage(StorageKind::encrypted_indexed_db(backend, handle));
        db.load_metadata_async().await?;
        Ok(db)
    }
}

impl WasmDatabase {
    async fn load_metadata_async(&self) -> Result<(), JsValue> {
        self.load_schemas().await?;
        self.load_constraints().await?;
        self.load_indexes().await?;
        self.load_relationships().await?;
        self.recover_id_counters().await?;
        Ok(())
    }

    async fn load_schemas(&self) -> Result<(), JsValue> {
        let prefix = b"meta/schema/";
        let items = self.storage.prefix_scan(prefix).await?;
        let mut inner = self.borrow_inner_mut()?;
        for (key, value) in items {
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| JsValue::from_str(&format!("invalid key: {e}")))?;
            let entity = key_str
                .strip_prefix("meta/schema/")
                .ok_or_else(|| JsValue::from_str("invalid schema key"))?;
            let schema: Schema = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("invalid schema data: {e}")))?;
            inner.schemas.insert(entity.to_string(), schema);
        }
        Ok(())
    }

    async fn load_constraints(&self) -> Result<(), JsValue> {
        let prefix = b"meta/constraint/";
        let items = self.storage.prefix_scan(prefix).await?;
        let mut inner = self.borrow_inner_mut()?;

        for (key, value) in items {
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| JsValue::from_str(&format!("invalid key: {e}")))?;
            let rest = key_str
                .strip_prefix("meta/constraint/")
                .ok_or_else(|| JsValue::from_str("invalid constraint key"))?;

            let parts: Vec<&str> = rest.splitn(3, '/').collect();
            if parts.len() < 3 {
                continue;
            }

            let constraint_type = parts[0];
            let entity = parts[1];

            match constraint_type {
                "unique" => {
                    let data: serde_json::Value = serde_json::from_slice(&value)
                        .map_err(|e| JsValue::from_str(&format!("invalid constraint: {e}")))?;
                    if let Some(fields) = data.get("fields").and_then(|v| v.as_array()) {
                        let field_names: Vec<String> = fields
                            .iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect();
                        inner
                            .unique_constraints
                            .entry(entity.to_string())
                            .or_default()
                            .push(field_names);
                    }
                }
                "not_null" => {
                    let data: serde_json::Value = serde_json::from_slice(&value)
                        .map_err(|e| JsValue::from_str(&format!("invalid constraint: {e}")))?;
                    if let Some(field) = data.get("field").and_then(|v| v.as_str()) {
                        inner
                            .not_null_constraints
                            .entry(entity.to_string())
                            .or_default()
                            .push(field.to_string());
                    }
                }
                "foreign_key" => {
                    let data: serde_json::Value = serde_json::from_slice(&value)
                        .map_err(|e| JsValue::from_str(&format!("invalid constraint: {e}")))?;
                    let source_entity = data
                        .get("source_entity")
                        .and_then(|v| v.as_str())
                        .unwrap_or(entity);
                    let source_field = data
                        .get("source_field")
                        .and_then(|v| v.as_str())
                        .unwrap_or(parts[2]);
                    let target_entity = data
                        .get("target_entity")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let target_field = data
                        .get("target_field")
                        .and_then(|v| v.as_str())
                        .unwrap_or("id");
                    let on_delete = match data.get("on_delete").and_then(|v| v.as_str()) {
                        Some("cascade") => OnDeleteAction::Cascade,
                        Some("set_null") => OnDeleteAction::SetNull,
                        _ => OnDeleteAction::Restrict,
                    };
                    inner.foreign_keys.push(ForeignKeyEntry {
                        source_entity: source_entity.to_string(),
                        source_field: source_field.to_string(),
                        target_entity: target_entity.to_string(),
                        target_field: target_field.to_string(),
                        on_delete,
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn load_indexes(&self) -> Result<(), JsValue> {
        let prefix = b"meta/index/";
        let items = self.storage.prefix_scan(prefix).await?;
        let mut inner = self.borrow_inner_mut()?;
        for (key, value) in items {
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| JsValue::from_str(&format!("invalid key: {e}")))?;
            let entity = key_str
                .strip_prefix("meta/index/")
                .ok_or_else(|| JsValue::from_str("invalid index key"))?;
            let index_defs: Vec<Vec<String>> = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("invalid index data: {e}")))?;
            inner.indexes.insert(entity.to_string(), index_defs);
        }
        Ok(())
    }

    async fn load_relationships(&self) -> Result<(), JsValue> {
        let prefix = b"meta/relationship/";
        let items = self.storage.prefix_scan(prefix).await?;
        let mut inner = self.borrow_inner_mut()?;
        for (key, value) in items {
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| JsValue::from_str(&format!("invalid key: {e}")))?;
            let rest = key_str
                .strip_prefix("meta/relationship/")
                .ok_or_else(|| JsValue::from_str("invalid relationship key"))?;

            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() < 2 {
                continue;
            }

            let source_entity = parts[0];
            let field = parts[1];
            let data: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("invalid relationship: {e}")))?;

            let target_entity = data
                .get("target_entity")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let default_suffix = format!("{field}_id");
            let field_suffix = data
                .get("field_suffix")
                .and_then(|v| v.as_str())
                .unwrap_or(&default_suffix);

            inner
                .relationships
                .entry(source_entity.to_string())
                .or_default()
                .push(Relationship {
                    field: field.to_string(),
                    target_entity: target_entity.to_string(),
                    field_suffix: field_suffix.to_string(),
                });
        }
        Ok(())
    }

    async fn recover_id_counters(&self) -> Result<(), JsValue> {
        let prefix = b"data/";
        let items = self.storage.prefix_scan(prefix).await?;
        let mut max_ids: HashMap<String, u64> = HashMap::new();

        for (key, _) in items {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(rest) = key_str.strip_prefix("data/") else {
                continue;
            };
            let Some(slash_pos) = rest.find('/') else {
                continue;
            };
            let entity = &rest[..slash_pos];
            let id_str = &rest[slash_pos + 1..];
            if let Ok(id_num) = id_str.parse::<u64>() {
                let current_max = max_ids.entry(entity.to_string()).or_insert(0);
                if id_num > *current_max {
                    *current_max = id_num;
                }
            }
        }

        let mut inner = self.borrow_inner_mut()?;
        for (entity, max_id) in max_ids {
            inner.id_counters.insert(entity, max_id);
        }
        Ok(())
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
