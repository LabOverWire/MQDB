mod indexeddb;

pub use indexeddb::{IndexedDbBackend, IndexedDbBatch};

use indexeddb::IndexedDbBackend as IdbBackend;
use mqdb::storage::{AsyncStorageBackend, Storage};
use mqdb::{
    AdminOperation, ChangeEvent, FieldDefinition, FieldType, Filter, OnDeleteAction, Operation,
    Pagination, Request, Schema, SortDirection, SortOrder, build_request, match_pattern,
    parse_admin_topic, parse_db_topic,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
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

enum StorageKind {
    Memory(Arc<Storage>),
    IndexedDb(IdbBackend),
}

impl StorageKind {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, JsValue> {
        match self {
            StorageKind::Memory(s) => s.get(key).map_err(|e| JsValue::from_str(&e.to_string())),
            StorageKind::IndexedDb(s) => s
                .get(key)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string())),
        }
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
        match self {
            StorageKind::Memory(s) => s
                .insert(key, value)
                .map_err(|e| JsValue::from_str(&e.to_string())),
            StorageKind::IndexedDb(s) => s
                .insert(key, value)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string())),
        }
    }

    async fn remove(&self, key: &[u8]) -> Result<(), JsValue> {
        match self {
            StorageKind::Memory(s) => s.remove(key).map_err(|e| JsValue::from_str(&e.to_string())),
            StorageKind::IndexedDb(s) => s
                .remove(key)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string())),
        }
    }

    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, JsValue> {
        match self {
            StorageKind::Memory(s) => s
                .prefix_scan(prefix)
                .map_err(|e| JsValue::from_str(&e.to_string())),
            StorageKind::IndexedDb(s) => s
                .prefix_scan(prefix)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string())),
        }
    }
}

struct DatabaseInner {
    schemas: HashMap<String, Schema>,
    subscriptions: HashMap<String, SubscriptionEntry>,
    unique_constraints: HashMap<String, Vec<Vec<String>>>,
    not_null_constraints: HashMap<String, Vec<String>>,
    foreign_keys: Vec<ForeignKeyEntry>,
    indexes: HashMap<String, Vec<Vec<String>>>,
    id_counters: HashMap<String, u64>,
    round_robin_counters: HashMap<String, usize>,
    relationships: HashMap<String, Vec<Relationship>>,
}

#[derive(Clone)]
struct Relationship {
    field: String,
    target_entity: String,
    field_suffix: String,
}

struct SubscriptionEntry {
    pattern: String,
    entity: Option<String>,
    callback: js_sys::Function,
    share_group: Option<String>,
    mode: SubscriptionMode,
    last_heartbeat: f64,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum SubscriptionMode {
    #[default]
    Broadcast,
    LoadBalanced,
    Ordered,
}

#[derive(Clone)]
struct ForeignKeyEntry {
    source_entity: String,
    source_field: String,
    target_entity: String,
    target_field: String,
    on_delete: OnDeleteAction,
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

    /// Creates a new record in the specified entity.
    ///
    /// # Errors
    /// Returns an error if validation fails or the storage operation fails.
    pub async fn create(&self, entity: String, data: JsValue) -> Result<JsValue, JsValue> {
        let mut value: serde_json::Value = deserialize_js(&data)?;

        let id = {
            let mut inner = self.inner.borrow_mut();
            let counter = inner.id_counters.entry(entity.clone()).or_insert(0);
            *counter += 1;
            counter.to_string()
        };

        if let serde_json::Value::Object(ref mut obj) = value {
            obj.insert("id".to_string(), serde_json::Value::String(id.clone()));
        }

        {
            let inner = self.inner.borrow();
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .apply_defaults(&mut value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_async(&entity, &value, None).await?;
        self.validate_foreign_keys_async(&entity, &value).await?;

        let key = format!("data/{entity}/{id}");
        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        self.storage.insert(key.as_bytes(), &serialized).await?;
        self.update_indexes_async(&entity, &id, &value, None)
            .await?;

        let event = ChangeEvent::create(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// Reads a record by entity and ID.
    ///
    /// # Errors
    /// Returns an error if the record is not found or deserialization fails.
    pub async fn read(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        serialize_js(&value)
    }

    /// Reads a record with related entities eagerly loaded.
    ///
    /// # Errors
    /// Returns an error if the record is not found or includes cannot be loaded.
    pub async fn read_with_includes(
        &self,
        entity: String,
        id: String,
        includes: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let include_list: Vec<String> = if includes.is_null() || includes.is_undefined() {
            Vec::new()
        } else {
            serde_wasm_bindgen::from_value(includes)
                .map_err(|e| JsValue::from_str(&format!("invalid includes: {e}")))?
        };

        let data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let mut value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        if !include_list.is_empty() {
            self.load_includes_async(&entity, &mut value, &include_list, 0)
                .await?;
        }

        serialize_js(&value)
    }

    async fn load_includes_async(
        &self,
        entity: &str,
        value: &mut serde_json::Value,
        includes: &[String],
        depth: usize,
    ) -> Result<(), JsValue> {
        const MAX_DEPTH: usize = 3;
        if depth >= MAX_DEPTH {
            return Ok(());
        }

        let relationships: Vec<Relationship> = {
            let inner = self.inner.borrow();
            match inner.relationships.get(entity) {
                Some(rels) => rels.clone(),
                None => return Ok(()),
            }
        };

        for include in includes {
            let parts: Vec<&str> = include.splitn(2, '.').collect();
            let field_name = parts[0];
            let nested_includes: Vec<String> = if parts.len() > 1 {
                vec![parts[1].to_string()]
            } else {
                Vec::new()
            };

            if let Some(rel) = relationships.iter().find(|r| r.field == field_name) {
                let fk_value = value.get(&rel.field_suffix).cloned();
                if let Some(serde_json::Value::String(target_id)) = fk_value {
                    let target_key = format!("data/{}/{}", rel.target_entity, target_id);
                    if let Ok(Some(target_data)) = self.storage.get(target_key.as_bytes()).await
                        && let Ok(mut related) =
                            serde_json::from_slice::<serde_json::Value>(&target_data)
                    {
                        if !nested_includes.is_empty() {
                            Box::pin(self.load_includes_async(
                                &rel.target_entity,
                                &mut related,
                                &nested_includes,
                                depth + 1,
                            ))
                            .await?;
                        }
                        if let serde_json::Value::Object(obj) = value {
                            obj.insert(field_name.to_string(), related);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Updates an existing record with new field values.
    ///
    /// # Errors
    /// Returns an error if the record is not found or validation fails.
    pub async fn update(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");
        let updates: serde_json::Value = deserialize_js(&fields)?;

        let existing_data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let mut value = existing.clone();

        if let (serde_json::Value::Object(existing_obj), serde_json::Value::Object(new_fields)) =
            (&mut value, updates)
        {
            for (k, v) in new_fields {
                existing_obj.insert(k, v);
            }
        }

        {
            let inner = self.inner.borrow();
            if let Some(schema) = inner.schemas.get(&entity) {
                schema
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
        }

        self.validate_not_null_async(&entity, &value)?;
        self.validate_unique_async(&entity, &value, Some(&id))
            .await?;
        self.validate_foreign_keys_async(&entity, &value).await?;

        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        self.storage.insert(key.as_bytes(), &serialized).await?;
        self.update_indexes_async(&entity, &id, &value, Some(&existing))
            .await?;

        let event = ChangeEvent::update(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    /// Deletes a record by entity and ID.
    ///
    /// # Errors
    /// Returns an error if the record is not found or foreign key constraints prevent deletion.
    pub async fn delete(&self, entity: String, id: String) -> Result<(), JsValue> {
        let key = format!("data/{entity}/{id}");

        let existing_data = self
            .storage
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let cascade_deletes = self
            .check_foreign_key_constraints_async(&entity, &id)
            .await?;

        self.storage.remove(key.as_bytes()).await?;
        self.remove_indexes_async(&entity, &id, &existing).await?;

        for (cascade_entity, cascade_id) in cascade_deletes {
            let _ = Box::pin(self.delete(cascade_entity, cascade_id)).await;
        }

        let event = ChangeEvent::delete(entity, id);
        self.dispatch_event(&event);

        Ok(())
    }

    /// Lists records from an entity with optional filtering, sorting, and pagination.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn list(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

        let mut results = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                results.push(parsed);
            }
        }

        if !opts.sort.is_empty() {
            Self::sort_results(&mut results, &opts.sort);
        }

        if let Some(pagination) = &opts.pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            results = results.into_iter().skip(offset).take(limit).collect();
        }

        if let Some(ref includes) = opts.includes {
            for result in &mut results {
                self.load_includes_async(&entity, result, includes, 0)
                    .await?;
            }
        }

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        let json_str = serde_json::to_string(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        js_sys::JSON::parse(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
    }

    #[must_use]
    pub fn subscribe(
        &self,
        pattern: String,
        entity: Option<String>,
        callback: js_sys::Function,
    ) -> String {
        let sub_id = uuid::Uuid::new_v4().to_string();
        let now = js_sys::Date::now();

        let mut inner = self.inner.borrow_mut();
        inner.subscriptions.insert(
            sub_id.clone(),
            SubscriptionEntry {
                pattern,
                entity,
                callback,
                share_group: None,
                mode: SubscriptionMode::default(),
                last_heartbeat: now,
            },
        );

        sub_id
    }

    #[must_use]
    pub fn subscribe_shared(
        &self,
        pattern: String,
        entity: Option<String>,
        group: String,
        mode: &str,
        callback: js_sys::Function,
    ) -> String {
        let sub_id = uuid::Uuid::new_v4().to_string();
        let now = js_sys::Date::now();

        let mode = match mode {
            "load-balanced" | "load_balanced" => SubscriptionMode::LoadBalanced,
            "ordered" => SubscriptionMode::Ordered,
            _ => SubscriptionMode::Broadcast,
        };

        let mut inner = self.inner.borrow_mut();
        inner.subscriptions.insert(
            sub_id.clone(),
            SubscriptionEntry {
                pattern,
                entity,
                callback,
                share_group: Some(group),
                mode,
                last_heartbeat: now,
            },
        );

        sub_id
    }

    #[must_use]
    pub fn heartbeat(&self, sub_id: &str) -> bool {
        let mut inner = self.inner.borrow_mut();
        if let Some(entry) = inner.subscriptions.get_mut(sub_id) {
            entry.last_heartbeat = js_sys::Date::now();
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn get_subscription_info(&self, sub_id: &str) -> JsValue {
        let inner = self.inner.borrow();
        match inner.subscriptions.get(sub_id) {
            Some(entry) => {
                let info = serde_json::json!({
                    "id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "share_group": entry.share_group,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                });
                let json_str = serde_json::to_string(&info).unwrap_or_else(|_| "null".to_string());
                js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
            }
            None => JsValue::NULL,
        }
    }

    #[must_use]
    pub fn list_consumer_groups(&self) -> JsValue {
        let inner = self.inner.borrow();
        let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        for (sub_id, entry) in &inner.subscriptions {
            if let Some(ref group_name) = entry.share_group {
                let member = serde_json::json!({
                    "subscription_id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                });
                groups.entry(group_name.clone()).or_default().push(member);
            }
        }

        let result: Vec<serde_json::Value> = groups
            .into_iter()
            .map(|(name, members)| {
                serde_json::json!({
                    "name": name,
                    "member_count": members.len(),
                    "members": members
                })
            })
            .collect();

        let json_str = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
        js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
    }

    #[must_use]
    pub fn get_consumer_group(&self, group_name: &str) -> JsValue {
        let inner = self.inner.borrow();
        let mut members: Vec<serde_json::Value> = Vec::new();

        for (sub_id, entry) in &inner.subscriptions {
            if entry.share_group.as_deref() == Some(group_name) {
                members.push(serde_json::json!({
                    "subscription_id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                }));
            }
        }

        if members.is_empty() {
            JsValue::NULL
        } else {
            let result = serde_json::json!({
                "name": group_name,
                "member_count": members.len(),
                "members": members
            });
            let json_str = serde_json::to_string(&result).unwrap_or_else(|_| "null".to_string());
            js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
        }
    }

    #[must_use]
    pub fn unsubscribe(&self, sub_id: &str) -> bool {
        let mut inner = self.inner.borrow_mut();
        inner.subscriptions.remove(sub_id).is_some()
    }

    /// Adds a schema definition for an entity.
    ///
    /// # Errors
    /// Returns an error if the schema definition is invalid.
    pub fn add_schema(&self, entity: String, schema_js: JsValue) -> Result<(), JsValue> {
        let schema_def: SchemaDefinition = serde_wasm_bindgen::from_value(schema_js)
            .map_err(|e| JsValue::from_str(&format!("invalid schema: {e}")))?;

        let mut schema = Schema::new(entity.clone());

        for field in schema_def.fields {
            let field_type = match field.field_type.as_str() {
                "string" => FieldType::String,
                "number" => FieldType::Number,
                "boolean" => FieldType::Boolean,
                "array" => FieldType::Array,
                "object" => FieldType::Object,
                other => return Err(JsValue::from_str(&format!("unknown field type: {other}"))),
            };

            let mut field_def = FieldDefinition::new(field.name, field_type);
            if field.required.unwrap_or(false) {
                field_def = field_def.required();
            }
            if let Some(default) = field.default {
                field_def = field_def.with_default(default);
            }

            schema = schema.add_field(field_def);
        }

        let mut inner = self.inner.borrow_mut();
        inner.schemas.insert(entity, schema);

        Ok(())
    }

    /// Gets the schema definition for an entity.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn get_schema(&self, entity: &str) -> Result<JsValue, JsValue> {
        let inner = self.inner.borrow();
        match inner.schemas.get(entity) {
            Some(schema) => {
                let fields: Vec<FieldDefJs> = schema
                    .fields
                    .values()
                    .map(|f| FieldDefJs {
                        name: f.name.clone(),
                        field_type: format!("{:?}", f.field_type).to_lowercase(),
                        required: Some(f.required),
                        default: f.default.clone(),
                    })
                    .collect();

                let def = SchemaDefinition { fields };
                serde_wasm_bindgen::to_value(&def)
                    .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))
            }
            None => Ok(JsValue::NULL),
        }
    }

    /// Adds a unique constraint on the specified fields.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_unique_constraint(
        &self,
        entity: String,
        fields: Vec<String>,
    ) -> Result<(), JsValue> {
        let mut inner = self.inner.borrow_mut();

        inner
            .indexes
            .entry(entity.clone())
            .or_default()
            .push(fields.clone());

        inner
            .unique_constraints
            .entry(entity)
            .or_default()
            .push(fields);

        Ok(())
    }

    /// Adds a NOT NULL constraint on the specified field.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_not_null(&self, entity: String, field: String) -> Result<(), JsValue> {
        let mut inner = self.inner.borrow_mut();
        inner
            .not_null_constraints
            .entry(entity)
            .or_default()
            .push(field);
        Ok(())
    }

    /// Adds a foreign key constraint.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: &str,
    ) -> Result<(), JsValue> {
        let on_delete = match on_delete {
            "cascade" => OnDeleteAction::Cascade,
            "set_null" => OnDeleteAction::SetNull,
            _ => OnDeleteAction::Restrict,
        };

        let mut inner = self.inner.borrow_mut();
        inner.foreign_keys.push(ForeignKeyEntry {
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
        });
        Ok(())
    }

    /// Adds an index on the specified fields.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_index(&self, entity: String, fields: Vec<String>) -> Result<(), JsValue> {
        let mut inner = self.inner.borrow_mut();
        inner.indexes.entry(entity).or_default().push(fields);
        Ok(())
    }

    /// Adds a relationship definition for eager loading.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_relationship(
        &self,
        source_entity: String,
        field: String,
        target_entity: String,
    ) -> Result<(), JsValue> {
        let field_suffix = format!("{field}_id");
        let mut inner = self.inner.borrow_mut();
        inner
            .relationships
            .entry(source_entity)
            .or_default()
            .push(Relationship {
                field,
                target_entity,
                field_suffix,
            });
        Ok(())
    }

    #[must_use]
    pub fn list_relationships(&self, entity: &str) -> JsValue {
        let inner = self.inner.borrow();
        let relationships: Vec<serde_json::Value> = inner
            .relationships
            .get(entity)
            .map(|rels| {
                rels.iter()
                    .map(|r| {
                        serde_json::json!({
                            "field": r.field,
                            "target_entity": r.target_entity,
                            "field_suffix": r.field_suffix
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let json_str = serde_json::to_string(&relationships).unwrap_or_else(|_| "[]".to_string());
        js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
    }

    #[must_use]
    pub fn list_constraints(&self, entity: &str) -> JsValue {
        let inner = self.inner.borrow();
        let mut constraints = Vec::new();

        if let Some(uniques) = inner.unique_constraints.get(entity) {
            for fields in uniques {
                constraints.push(serde_json::json!({
                    "type": "unique",
                    "fields": fields
                }));
            }
        }

        if let Some(not_nulls) = inner.not_null_constraints.get(entity) {
            for field in not_nulls {
                constraints.push(serde_json::json!({
                    "type": "not_null",
                    "field": field
                }));
            }
        }

        for fk in &inner.foreign_keys {
            if fk.source_entity.as_str() == entity {
                constraints.push(serde_json::json!({
                    "type": "foreign_key",
                    "field": fk.source_field,
                    "target_entity": fk.target_entity,
                    "target_field": fk.target_field,
                    "on_delete": format!("{:?}", fk.on_delete).to_lowercase()
                }));
            }
        }

        serde_wasm_bindgen::to_value(&constraints).unwrap_or(JsValue::NULL)
    }

    /// Executes a database operation based on an MQTT-style topic.
    ///
    /// # Errors
    /// Returns an error if the topic is invalid or the operation fails.
    pub async fn execute(&self, topic: String, payload: JsValue) -> Result<JsValue, JsValue> {
        let payload_bytes: Vec<u8> = if payload.is_null() || payload.is_undefined() {
            vec![]
        } else {
            let value: serde_json::Value = deserialize_js(&payload)?;
            serde_json::to_vec(&value)
                .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?
        };

        if let Some(admin_op) = parse_admin_topic(&topic) {
            return self.handle_admin_operation(admin_op, &payload_bytes);
        }

        let Some(db_op) = parse_db_topic(&topic) else {
            return Err(JsValue::from_str(&format!("invalid topic: {topic}")));
        };

        let request = build_request(db_op, &payload_bytes)
            .map_err(|e| JsValue::from_str(&format!("invalid request: {e}")))?;

        match request {
            Request::Create { entity, data } => self.create(entity, serialize_js(&data)?).await,
            Request::Read { entity, id, .. } => self.read(entity, id).await,
            Request::Update { entity, id, fields } => {
                self.update(entity, id, serialize_js(&fields)?).await
            }
            Request::Delete { entity, id } => {
                self.delete(entity, id).await?;
                Ok(JsValue::NULL)
            }
            Request::List {
                entity,
                filters,
                sort,
                pagination,
                projection,
                ..
            } => {
                let opts = ListOptions {
                    filters: filters.into_iter().map(FilterJs::from).collect(),
                    sort: sort.into_iter().map(SortOrderJs::from).collect(),
                    pagination: pagination.map(PaginationJs::from),
                    projection,
                    includes: None,
                };
                let opts_js = serde_wasm_bindgen::to_value(&opts)
                    .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
                self.list(entity, opts_js).await
            }
            Request::Subscribe { .. } | Request::Unsubscribe { .. } => Err(JsValue::from_str(
                "use subscribe/unsubscribe methods directly",
            )),
        }
    }

    fn handle_admin_operation(
        &self,
        op: AdminOperation,
        payload: &[u8],
    ) -> Result<JsValue, JsValue> {
        let data: serde_json::Value = if payload.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(payload)
                .map_err(|e| JsValue::from_str(&format!("invalid JSON: {e}")))?
        };

        match op {
            AdminOperation::Health => Ok(serialize_js(&serde_json::json!({
                "status": "healthy",
                "ready": true,
                "mode": "wasm"
            }))?),
            AdminOperation::SchemaSet { entity } => {
                let schema_js = serialize_js(&data)?;
                self.add_schema(entity, schema_js)?;
                Ok(serialize_js(&serde_json::json!({"message": "schema set"}))?)
            }
            AdminOperation::SchemaGet { entity } => self.get_schema(&entity),
            AdminOperation::ConstraintList { entity } => Ok(self.list_constraints(&entity)),
            AdminOperation::ConstraintAdd { entity } => {
                let constraint_type = data.get("type").and_then(|v| v.as_str());
                match constraint_type {
                    Some("unique") => {
                        let fields: Vec<String> = data
                            .get("fields")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        self.add_unique_constraint(entity, fields)?;
                    }
                    Some("not_null") => {
                        let field = data
                            .get("field")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| JsValue::from_str("field required"))?;
                        self.add_not_null(entity, field.to_string())?;
                    }
                    Some("foreign_key") => {
                        let source_field = data
                            .get("field")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| JsValue::from_str("field required"))?;
                        let target_entity = data
                            .get("target_entity")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| JsValue::from_str("target_entity required"))?;
                        let target_field = data
                            .get("target_field")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| JsValue::from_str("target_field required"))?;
                        let on_delete = data
                            .get("on_delete")
                            .and_then(|v| v.as_str())
                            .unwrap_or("restrict");
                        self.add_foreign_key(
                            entity,
                            source_field.to_string(),
                            target_entity.to_string(),
                            target_field.to_string(),
                            on_delete,
                        )?;
                    }
                    _ => return Err(JsValue::from_str("unknown constraint type")),
                }
                Ok(serialize_js(
                    &serde_json::json!({"message": "constraint added"}),
                )?)
            }
            _ => Err(JsValue::from_str(&format!(
                "admin operation not supported in WASM: {op:?}"
            ))),
        }
    }

    fn validate_not_null_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let inner = self.inner.borrow();
        if let Some(fields) = inner.not_null_constraints.get(entity) {
            for field in fields {
                let field_value = value.get(field);
                if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                    return Err(JsValue::from_str(&format!(
                        "not null constraint violation: {entity}.{field}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn validate_unique_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
        current_id: Option<&str>,
    ) -> Result<(), JsValue> {
        let constraints: Option<Vec<Vec<String>>> = {
            let inner = self.inner.borrow();
            inner.unique_constraints.get(entity).cloned()
        };

        if let Some(constraints) = constraints {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

            for constraint_fields in constraints {
                let new_values: Vec<Option<&serde_json::Value>> =
                    constraint_fields.iter().map(|f| value.get(f)).collect();

                for (_key, existing_data) in &items {
                    let existing: serde_json::Value = serde_json::from_slice(existing_data)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;

                    if let Some(existing_id) = existing.get("id").and_then(|v| v.as_str())
                        && current_id == Some(existing_id)
                    {
                        continue;
                    }

                    let existing_values: Vec<Option<&serde_json::Value>> =
                        constraint_fields.iter().map(|f| existing.get(f)).collect();

                    if new_values == existing_values
                        && new_values
                            .iter()
                            .all(|v| v.is_some() && *v != Some(&serde_json::Value::Null))
                    {
                        return Err(JsValue::from_str(&format!(
                            "unique constraint violation: {entity}.{}",
                            constraint_fields.join(", ")
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn validate_foreign_keys_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.inner.borrow();
            inner.foreign_keys.clone()
        };

        for fk in &fks {
            if fk.source_entity != entity {
                continue;
            }

            let field_value = value.get(&fk.source_field);
            if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                continue;
            }

            let target_id = field_value
                .and_then(|v| v.as_str())
                .ok_or_else(|| JsValue::from_str("foreign key must be a string"))?;

            let target_key = format!("data/{}/{}", fk.target_entity, target_id);
            if self.storage.get(target_key.as_bytes()).await?.is_none() {
                return Err(JsValue::from_str(&format!(
                    "foreign key violation: {entity}.{} references non-existent {}/{}",
                    fk.source_field, fk.target_entity, target_id
                )));
            }
        }
        Ok(())
    }

    async fn check_foreign_key_constraints_async(
        &self,
        entity: &str,
        id: &str,
    ) -> Result<Vec<(String, String)>, JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.inner.borrow();
            inner.foreign_keys.clone()
        };

        let mut cascade_deletes = Vec::new();

        for fk in &fks {
            if fk.target_entity != entity {
                continue;
            }

            let prefix = format!("data/{}/", fk.source_entity);
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

            for (_key, data) in items {
                let value: serde_json::Value =
                    serde_json::from_slice(&data).map_err(|e| JsValue::from_str(&e.to_string()))?;

                if value.get(&fk.source_field).and_then(|v| v.as_str()) == Some(id) {
                    match fk.on_delete {
                        OnDeleteAction::Restrict => {
                            return Err(JsValue::from_str(&format!(
                                "foreign key restrict: cannot delete {entity}/{id} - referenced by {}",
                                fk.source_entity
                            )));
                        }
                        OnDeleteAction::Cascade => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                cascade_deletes
                                    .push((fk.source_entity.clone(), source_id.to_string()));
                            }
                        }
                        OnDeleteAction::SetNull => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                let mut updated = value.clone();
                                if let Some(obj) = updated.as_object_mut() {
                                    obj.insert(fk.source_field.clone(), serde_json::Value::Null);
                                }
                                let key = format!("data/{}/{}", fk.source_entity, source_id);
                                let serialized = serde_json::to_vec(&updated)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                                self.storage.insert(key.as_bytes(), &serialized).await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    async fn update_indexes_async(
        &self,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
        old_value: Option<&serde_json::Value>,
    ) -> Result<(), JsValue> {
        if let Some(old) = old_value {
            self.remove_indexes_async(entity, id, old).await?;
        }

        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.inner.borrow();
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    self.storage.insert(index_key.as_bytes(), &[]).await?;
                }
            }
        }

        Ok(())
    }

    async fn remove_indexes_async(
        &self,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.inner.borrow();
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    let _ = self.storage.remove(index_key.as_bytes()).await;
                }
            }
        }

        Ok(())
    }

    fn matches_filter(value: &serde_json::Value, filter: &FilterJs) -> bool {
        let field_value = value.get(&filter.field);

        match filter.op.as_str() {
            "ne" | "<>" => field_value != Some(&filter.value),
            "gt" | ">" => {
                Self::compare_values(field_value, &filter.value)
                    == Some(std::cmp::Ordering::Greater)
            }
            "lt" | "<" => {
                Self::compare_values(field_value, &filter.value) == Some(std::cmp::Ordering::Less)
            }
            "gte" | ">=" => Self::compare_values(field_value, &filter.value)
                .is_some_and(|ord| ord != std::cmp::Ordering::Less),
            "lte" | "<=" => Self::compare_values(field_value, &filter.value)
                .is_some_and(|ord| ord != std::cmp::Ordering::Greater),
            "glob" | "~" => {
                if let (Some(serde_json::Value::String(s)), serde_json::Value::String(pattern)) =
                    (field_value, &filter.value)
                {
                    Self::glob_match(s, pattern)
                } else {
                    false
                }
            }
            "null" | "?" => field_value.is_none() || field_value == Some(&serde_json::Value::Null),
            "not_null" | "!?" => {
                field_value.is_some() && field_value != Some(&serde_json::Value::Null)
            }
            _ => field_value == Some(&filter.value),
        }
    }

    fn compare_values(
        a: Option<&serde_json::Value>,
        b: &serde_json::Value,
    ) -> Option<std::cmp::Ordering> {
        let a = a?;
        match (a, b) {
            (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
                let a_f = a_num.as_f64()?;
                let b_f = b_num.as_f64()?;
                a_f.partial_cmp(&b_f)
            }
            (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => {
                Some(a_str.cmp(b_str))
            }
            _ => None,
        }
    }

    fn glob_match(text: &str, pattern: &str) -> bool {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 1 {
            return text == pattern;
        }

        let mut pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            if let Some(found) = text[pos..].find(part) {
                if i == 0 && found != 0 {
                    return false;
                }
                pos += found + part.len();
            } else {
                return false;
            }
        }

        parts
            .last()
            .is_none_or(|last| last.is_empty() || text.ends_with(last))
    }

    fn sort_results(results: &mut [serde_json::Value], sort: &[SortOrderJs]) {
        results.sort_by(|a, b| {
            for order in sort {
                let a_val = a.get(&order.field);
                let b_val = b.get(&order.field);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => Self::compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = if order.direction == "desc" {
                    cmp.reverse()
                } else {
                    cmp
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        match (a, b) {
            (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => {
                a_str.cmp(b_str)
            }
            (serde_json::Value::Bool(a_bool), serde_json::Value::Bool(b_bool)) => {
                a_bool.cmp(b_bool)
            }
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn project_fields(value: serde_json::Value, fields: &[String]) -> serde_json::Value {
        if let serde_json::Value::Object(obj) = value {
            let mut projected = serde_json::Map::new();

            if let Some(id) = obj.get("id") {
                projected.insert("id".to_string(), id.clone());
            }

            for field in fields {
                if field != "id"
                    && let Some(v) = obj.get(field)
                {
                    projected.insert(field.clone(), v.clone());
                }
            }

            serde_json::Value::Object(projected)
        } else {
            value
        }
    }

    fn dispatch_event(&self, event: &ChangeEvent) {
        let Ok(event_js) = serialize_event(event) else {
            return;
        };

        let mut broadcast_callbacks: Vec<js_sys::Function> = Vec::new();
        let mut share_groups: HashMap<String, Vec<js_sys::Function>> = HashMap::new();

        {
            let inner = self.inner.borrow();
            for sub in inner.subscriptions.values() {
                if !Self::matches_subscription(sub, event) {
                    continue;
                }

                match (&sub.share_group, sub.mode) {
                    (None, _) | (Some(_), SubscriptionMode::Broadcast) => {
                        broadcast_callbacks.push(sub.callback.clone());
                    }
                    (Some(group), _) => {
                        share_groups
                            .entry(group.clone())
                            .or_default()
                            .push(sub.callback.clone());
                    }
                }
            }
        }

        for callback in broadcast_callbacks {
            let _ = callback.call1(&JsValue::NULL, &event_js);
        }

        if !share_groups.is_empty() {
            let mut inner = self.inner.borrow_mut();
            for (group_name, callbacks) in share_groups {
                if callbacks.is_empty() {
                    continue;
                }

                let counter = inner.round_robin_counters.entry(group_name).or_insert(0);
                let idx = *counter % callbacks.len();
                *counter = counter.wrapping_add(1);

                let selected_callback = &callbacks[idx];
                let _ = selected_callback.call1(&JsValue::NULL, &event_js);
            }
        }
    }

    fn matches_subscription(sub: &SubscriptionEntry, event: &ChangeEvent) -> bool {
        if let Some(ref entity) = sub.entity
            && entity != &event.entity
        {
            return false;
        }

        if sub.pattern == "*" || sub.pattern == "#" {
            return true;
        }

        match_pattern(&sub.pattern, &event.entity, &event.id)
    }

    /// Creates a cursor for streaming iteration over records.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn cursor(&self, entity: String, options: JsValue) -> Result<WasmCursor, JsValue> {
        let opts: CursorOptions = if options.is_null() || options.is_undefined() {
            CursorOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

        let mut all_items: Vec<serde_json::Value> = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                all_items.push(parsed);
            }
        }

        if !opts.sort.is_empty() {
            Self::sort_results(&mut all_items, &opts.sort);
        }

        Ok(WasmCursor {
            buffer: VecDeque::from(all_items),
            current_index: 0,
            exhausted: false,
        })
    }
}

impl Default for WasmDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
pub struct WasmCursor {
    buffer: VecDeque<serde_json::Value>,
    current_index: usize,
    exhausted: bool,
}

#[wasm_bindgen]
impl WasmCursor {
    /// Returns the next item from the cursor.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn next_item(&mut self) -> Result<JsValue, JsValue> {
        if self.exhausted || self.buffer.is_empty() {
            self.exhausted = true;
            return Ok(JsValue::UNDEFINED);
        }

        if let Some(item) = self.buffer.pop_front() {
            self.current_index += 1;
            serialize_js(&item)
        } else {
            self.exhausted = true;
            Ok(JsValue::UNDEFINED)
        }
    }

    /// Returns up to N items from the cursor as an array.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn next_batch(&mut self, size: usize) -> Result<JsValue, JsValue> {
        if self.exhausted || self.buffer.is_empty() {
            self.exhausted = true;
            return serialize_js(&serde_json::Value::Array(Vec::new()));
        }

        let mut batch = Vec::with_capacity(size);
        for _ in 0..size {
            if let Some(item) = self.buffer.pop_front() {
                self.current_index += 1;
                batch.push(item);
            } else {
                self.exhausted = true;
                break;
            }
        }

        serialize_js(&serde_json::Value::Array(batch))
    }

    pub fn reset(&mut self) {
        self.current_index = 0;
        self.exhausted = false;
    }

    #[must_use]
    pub fn has_more(&self) -> bool {
        !self.exhausted && !self.buffer.is_empty()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub fn position(&self) -> usize {
        self.current_index
    }
}

#[derive(Serialize, Deserialize, Default)]
struct CursorOptions {
    #[serde(default)]
    filters: Vec<FilterJs>,
    #[serde(default)]
    sort: Vec<SortOrderJs>,
}

#[derive(Serialize, Deserialize, Default)]
struct ListOptions {
    #[serde(default)]
    filters: Vec<FilterJs>,
    #[serde(default)]
    sort: Vec<SortOrderJs>,
    #[serde(default)]
    pagination: Option<PaginationJs>,
    #[serde(default)]
    projection: Option<Vec<String>>,
    #[serde(default)]
    includes: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct FilterJs {
    field: String,
    op: String,
    value: serde_json::Value,
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
struct SortOrderJs {
    field: String,
    #[serde(default = "default_sort_direction")]
    direction: String,
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
struct PaginationJs {
    limit: usize,
    offset: usize,
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
struct SchemaDefinition {
    fields: Vec<FieldDefJs>,
}

#[derive(Serialize, Deserialize)]
struct FieldDefJs {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    required: Option<bool>,
    default: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct EventJs {
    operation: String,
    entity: String,
    id: String,
    data: Option<serde_json::Value>,
}

fn deserialize_js(value: &JsValue) -> Result<serde_json::Value, JsValue> {
    serde_wasm_bindgen::from_value(value.clone())
        .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))
}

fn serialize_js(value: &serde_json::Value) -> Result<JsValue, JsValue> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
    js_sys::JSON::parse(&json_str)
        .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
}

fn serialize_event(event: &ChangeEvent) -> Result<JsValue, JsValue> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn test_create_and_read() {
        let db = WasmDatabase::new();
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
        let result = db.create("users".to_string(), data).await.unwrap();
        let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let id = result_value["id"].as_str().unwrap().to_string();

        let read_result = db.read("users".to_string(), id).await.unwrap();
        let read_value: serde_json::Value = serde_wasm_bindgen::from_value(read_result).unwrap();
        assert_eq!(read_value["name"], "Alice");
    }

    #[wasm_bindgen_test]
    async fn test_update() {
        let db = WasmDatabase::new();
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
        let result = db.create("users".to_string(), data).await.unwrap();
        let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let id = result_value["id"].as_str().unwrap().to_string();

        let update = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Bob"})).unwrap();
        let updated = db
            .update("users".to_string(), id.clone(), update)
            .await
            .unwrap();
        let updated_value: serde_json::Value = serde_wasm_bindgen::from_value(updated).unwrap();
        assert_eq!(updated_value["name"], "Bob");
    }

    #[wasm_bindgen_test]
    async fn test_delete() {
        let db = WasmDatabase::new();
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
        let result = db.create("users".to_string(), data).await.unwrap();
        let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let id = result_value["id"].as_str().unwrap().to_string();

        db.delete("users".to_string(), id.clone()).await.unwrap();

        let read_result = db.read("users".to_string(), id).await;
        assert!(read_result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_list_with_filter() {
        let db = WasmDatabase::new();

        for name in &["Alice", "Bob", "Charlie"] {
            let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
            db.create("users".to_string(), data).await.unwrap();
        }

        let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
            "filters": [{"field": "name", "op": "eq", "value": "Bob"}]
        }))
        .unwrap();

        let result = db.list("users".to_string(), opts).await.unwrap();
        let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["name"], "Bob");
    }

    #[wasm_bindgen_test]
    async fn test_unique_constraint() {
        let db = WasmDatabase::new();
        db.add_unique_constraint("users".to_string(), vec!["email".to_string()])
            .unwrap();

        let data1 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"}))
            .unwrap();
        db.create("users".to_string(), data1).await.unwrap();

        let data2 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"}))
            .unwrap();
        let result = db.create("users".to_string(), data2).await;
        assert!(result.is_err());
    }
}
