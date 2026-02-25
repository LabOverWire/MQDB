// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    Arc, AsyncStorageBackend, ChangeEvent, Deserialize, Filter, HashMap, IdbBackend, JsValue,
    OnDeleteAction, Operation, Pagination, Schema, Serialize, SortDirection, SortOrder, Storage,
};
use crate::crypto::CryptoHandle;
use std::rc::Rc;

pub(crate) type KvPairs = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) struct StorageKind {
    backend: BackendKind,
    crypto: Option<Rc<CryptoHandle>>,
}

enum BackendKind {
    Memory(Arc<Storage>),
    IndexedDb(IdbBackend),
}

fn err_to_js(e: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&e.to_string())
}

impl StorageKind {
    pub(crate) fn memory(storage: Arc<Storage>) -> Self {
        Self {
            backend: BackendKind::Memory(storage),
            crypto: None,
        }
    }

    pub(crate) fn indexed_db(backend: IdbBackend) -> Self {
        Self {
            backend: BackendKind::IndexedDb(backend),
            crypto: None,
        }
    }

    pub(crate) fn encrypted_indexed_db(backend: IdbBackend, crypto: Rc<CryptoHandle>) -> Self {
        Self {
            backend: BackendKind::IndexedDb(backend),
            crypto: Some(crypto),
        }
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, JsValue> {
        let raw = match &self.backend {
            BackendKind::Memory(s) => s.get(key).map_err(err_to_js)?,
            BackendKind::IndexedDb(s) => s.get(key).await.map_err(err_to_js)?,
        };
        match (&self.crypto, raw) {
            (Some(c), Some(encrypted)) => {
                let plaintext = c.decrypt(key, &encrypted).await.map_err(err_to_js)?;
                Ok(Some(plaintext))
            }
            (_, other) => Ok(other),
        }
    }

    pub(crate) async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
        let stored = if let Some(c) = &self.crypto {
            c.encrypt(key, value).await.map_err(err_to_js)?
        } else {
            value.to_vec()
        };
        match &self.backend {
            BackendKind::Memory(s) => s.insert(key, &stored).map_err(err_to_js),
            BackendKind::IndexedDb(s) => s.insert(key, &stored).await.map_err(err_to_js),
        }
    }

    pub(crate) async fn remove(&self, key: &[u8]) -> Result<(), JsValue> {
        match &self.backend {
            BackendKind::Memory(s) => s.remove(key).map_err(err_to_js),
            BackendKind::IndexedDb(s) => s.remove(key).await.map_err(err_to_js),
        }
    }

    pub(crate) async fn prefix_scan(&self, prefix: &[u8]) -> Result<KvPairs, JsValue> {
        let raw = match &self.backend {
            BackendKind::Memory(s) => s.prefix_scan(prefix).map_err(err_to_js)?,
            BackendKind::IndexedDb(s) => s.prefix_scan(prefix).await.map_err(err_to_js)?,
        };
        if let Some(c) = &self.crypto {
            let mut decrypted = Vec::with_capacity(raw.len());
            for (k, v) in raw {
                let plaintext = c.decrypt(&k, &v).await.map_err(err_to_js)?;
                decrypted.push((k, plaintext));
            }
            Ok(decrypted)
        } else {
            Ok(raw)
        }
    }

    pub(crate) fn get_sync(&self, key: &[u8]) -> Result<Option<Vec<u8>>, JsValue> {
        if self.crypto.is_some() {
            return Err(JsValue::from_str(
                "sync operations not available with encryption",
            ));
        }
        match &self.backend {
            BackendKind::Memory(s) => s.get(key).map_err(err_to_js),
            BackendKind::IndexedDb(_) => {
                Err(JsValue::from_str("sync operations require memory backend"))
            }
        }
    }

    pub(crate) fn insert_sync(&self, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
        if self.crypto.is_some() {
            return Err(JsValue::from_str(
                "sync operations not available with encryption",
            ));
        }
        match &self.backend {
            BackendKind::Memory(s) => s.insert(key, value).map_err(err_to_js),
            BackendKind::IndexedDb(_) => {
                Err(JsValue::from_str("sync operations require memory backend"))
            }
        }
    }

    pub(crate) fn remove_sync(&self, key: &[u8]) -> Result<(), JsValue> {
        if self.crypto.is_some() {
            return Err(JsValue::from_str(
                "sync operations not available with encryption",
            ));
        }
        match &self.backend {
            BackendKind::Memory(s) => s.remove(key).map_err(err_to_js),
            BackendKind::IndexedDb(_) => {
                Err(JsValue::from_str("sync operations require memory backend"))
            }
        }
    }

    pub(crate) fn prefix_scan_sync(&self, prefix: &[u8]) -> Result<KvPairs, JsValue> {
        if self.crypto.is_some() {
            return Err(JsValue::from_str(
                "sync operations not available with encryption",
            ));
        }
        match &self.backend {
            BackendKind::Memory(s) => s.prefix_scan(prefix).map_err(err_to_js),
            BackendKind::IndexedDb(_) => {
                Err(JsValue::from_str("sync operations require memory backend"))
            }
        }
    }

    pub(crate) fn is_memory(&self) -> bool {
        matches!(self.backend, BackendKind::Memory(_))
    }
}

pub(crate) struct DatabaseInner {
    pub(crate) schemas: HashMap<String, Schema>,
    pub(crate) subscriptions: HashMap<String, SubscriptionEntry>,
    pub(crate) unique_constraints: HashMap<String, Vec<Vec<String>>>,
    pub(crate) not_null_constraints: HashMap<String, Vec<String>>,
    pub(crate) foreign_keys: Vec<ForeignKeyEntry>,
    pub(crate) indexes: HashMap<String, Vec<Vec<String>>>,
    pub(crate) id_counters: HashMap<String, u64>,
    pub(crate) round_robin_counters: HashMap<String, usize>,
    pub(crate) relationships: HashMap<String, Vec<Relationship>>,
}

#[derive(Clone)]
pub(crate) struct Relationship {
    pub(crate) field: String,
    pub(crate) target_entity: String,
    pub(crate) field_suffix: String,
}

pub(crate) struct SubscriptionEntry {
    pub(crate) pattern: String,
    pub(crate) entity: Option<String>,
    pub(crate) callback: js_sys::Function,
    pub(crate) share_group: Option<String>,
    pub(crate) mode: SubscriptionMode,
    pub(crate) last_heartbeat: f64,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SubscriptionMode {
    #[default]
    Broadcast,
    LoadBalanced,
    Ordered,
}

#[derive(Clone)]
pub(crate) struct ForeignKeyEntry {
    pub(crate) source_entity: String,
    pub(crate) source_field: String,
    pub(crate) target_entity: String,
    pub(crate) target_field: String,
    pub(crate) on_delete: OnDeleteAction,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct CursorOptions {
    #[serde(default)]
    pub(crate) filters: Vec<FilterJs>,
    #[serde(default)]
    pub(crate) sort: Vec<SortOrderJs>,
    #[serde(default)]
    pub(crate) projection: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct CountOptions {
    #[serde(default)]
    pub(crate) filters: Vec<FilterJs>,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct ListOptions {
    #[serde(default)]
    pub(crate) filters: Vec<FilterJs>,
    #[serde(default)]
    pub(crate) sort: Vec<SortOrderJs>,
    #[serde(default)]
    pub(crate) pagination: Option<PaginationJs>,
    #[serde(default)]
    pub(crate) projection: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) includes: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct FilterJs {
    pub(crate) field: String,
    pub(crate) op: String,
    pub(crate) value: serde_json::Value,
}

impl From<Filter> for FilterJs {
    fn from(f: Filter) -> Self {
        Self {
            field: f.field,
            op: format!("{:?}", f.op).to_lowercase(),
            value: f.value,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SortOrderJs {
    pub(crate) field: String,
    #[serde(default = "default_sort_direction")]
    pub(crate) direction: String,
}

fn default_sort_direction() -> String {
    "asc".to_string()
}

impl From<SortOrder> for SortOrderJs {
    fn from(s: SortOrder) -> Self {
        Self {
            field: s.field,
            direction: match s.direction {
                SortDirection::Asc => "asc".to_string(),
                SortDirection::Desc => "desc".to_string(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct PaginationJs {
    pub(crate) limit: usize,
    pub(crate) offset: usize,
}

impl From<Pagination> for PaginationJs {
    fn from(p: Pagination) -> Self {
        Self {
            limit: p.limit,
            offset: p.offset,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SchemaDefinition {
    pub(crate) fields: Vec<FieldDefJs>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct FieldDefJs {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) field_type: String,
    pub(crate) required: Option<bool>,
    pub(crate) default: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub(crate) struct EventJs {
    pub(crate) operation: String,
    pub(crate) entity: String,
    pub(crate) id: String,
    pub(crate) data: Option<serde_json::Value>,
}

pub(crate) fn deserialize_js(value: &JsValue) -> Result<serde_json::Value, JsValue> {
    serde_wasm_bindgen::from_value(value.clone())
        .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))
}

pub(crate) fn serialize_js(value: &serde_json::Value) -> Result<JsValue, JsValue> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
    js_sys::JSON::parse(&json_str)
        .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
}

pub(crate) fn serialize_event(event: &ChangeEvent) -> Result<JsValue, JsValue> {
    let event_js = EventJs {
        operation: match event.operation {
            Operation::Create => "create".to_string(),
            Operation::Update => "update".to_string(),
            Operation::Delete => "delete".to_string(),
        },
        entity: event.entity.clone(),
        id: event.id.clone(),
        data: event.data.clone(),
    };
    let json_str = serde_json::to_string(&event_js)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
    js_sys::JSON::parse(&json_str)
        .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
}
