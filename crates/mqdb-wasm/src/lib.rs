use mqdb::storage::Storage;
use mqdb::{
    build_request, match_pattern, parse_admin_topic, parse_db_topic, AdminOperation, ChangeEvent,
    FieldDefinition, FieldType, Filter, OnDeleteAction, Operation, Pagination, Request, Schema,
    SortDirection, SortOrder,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct WasmDatabase {
    inner: Rc<RefCell<DatabaseInner>>,
}

struct DatabaseInner {
    storage: Arc<Storage>,
    schemas: HashMap<String, Schema>,
    subscriptions: HashMap<String, SubscriptionEntry>,
    unique_constraints: HashMap<String, Vec<Vec<String>>>,
    not_null_constraints: HashMap<String, Vec<String>>,
    foreign_keys: Vec<ForeignKeyEntry>,
    indexes: HashMap<String, Vec<Vec<String>>>,
    id_counters: HashMap<String, u64>,
}

struct SubscriptionEntry {
    pattern: String,
    entity: Option<String>,
    callback: js_sys::Function,
    share_group: Option<String>,
    mode: SubscriptionMode,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubscriptionMode {
    Broadcast,
    LoadBalanced,
    Ordered,
}

impl Default for SubscriptionMode {
    fn default() -> Self {
        Self::Broadcast
    }
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
    pub fn new() -> WasmDatabase {
        let storage = Arc::new(Storage::memory());
        WasmDatabase {
            inner: Rc::new(RefCell::new(DatabaseInner {
                storage,
                schemas: HashMap::new(),
                subscriptions: HashMap::new(),
                unique_constraints: HashMap::new(),
                not_null_constraints: HashMap::new(),
                foreign_keys: Vec::new(),
                indexes: HashMap::new(),
                id_counters: HashMap::new(),
            })),
        }
    }

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
                    .validate(&value)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }

            self.validate_not_null(&inner, &entity, &value)?;
            self.validate_unique(&inner, &entity, &value, None)?;
            self.validate_foreign_keys(&inner, &entity, &value)?;
        }

        let key = format!("data/{entity}/{id}");
        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        {
            let inner = self.inner.borrow();
            inner
                .storage
                .insert(key.as_bytes(), &serialized)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            self.update_indexes(&inner, &entity, &id, &value, None)?;
        }

        let event = ChangeEvent::create(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    pub async fn read(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");

        let inner = self.inner.borrow();
        let data = inner
            .storage
            .get(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        serialize_js(&value)
    }

    pub async fn update(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = format!("data/{entity}/{id}");
        let updates: serde_json::Value = deserialize_js(&fields)?;

        let inner = self.inner.borrow();
        let existing_data = inner
            .storage
            .get(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?
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

        if let Some(schema) = inner.schemas.get(&entity) {
            schema
                .validate(&value)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }

        self.validate_not_null(&inner, &entity, &value)?;
        self.validate_unique(&inner, &entity, &value, Some(&id))?;
        self.validate_foreign_keys(&inner, &entity, &value)?;

        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        inner
            .storage
            .insert(key.as_bytes(), &serialized)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.update_indexes(&inner, &entity, &id, &value, Some(&existing))?;

        drop(inner);

        let event = ChangeEvent::update(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    pub async fn delete(&self, entity: String, id: String) -> Result<(), JsValue> {
        let key = format!("data/{entity}/{id}");

        let inner = self.inner.borrow();
        let existing_data = inner
            .storage
            .get(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        let existing: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        let cascade_deletes = self.check_foreign_key_constraints(&inner, &entity, &id)?;

        inner
            .storage
            .remove(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.remove_indexes(&inner, &entity, &id, &existing)?;

        drop(inner);

        for (cascade_entity, cascade_id) in cascade_deletes {
            let _ = Box::pin(self.delete(cascade_entity, cascade_id)).await;
        }

        let event = ChangeEvent::delete(entity, id);
        self.dispatch_event(&event);

        Ok(())
    }

    pub async fn list(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let prefix = format!("data/{entity}/");

        let inner = self.inner.borrow();
        let items = inner
            .storage
            .prefix_scan(prefix.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let mut results = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts.filters.iter().all(|f| self.matches_filter(&parsed, f)) {
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

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        serde_wasm_bindgen::to_value(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))
    }

    pub fn subscribe(
        &self,
        pattern: String,
        entity: Option<String>,
        callback: js_sys::Function,
    ) -> String {
        let sub_id = uuid::Uuid::new_v4().to_string();

        let mut inner = self.inner.borrow_mut();
        inner.subscriptions.insert(
            sub_id.clone(),
            SubscriptionEntry {
                pattern,
                entity,
                callback,
                share_group: None,
                mode: SubscriptionMode::default(),
            },
        );

        sub_id
    }

    pub fn subscribe_shared(
        &self,
        pattern: String,
        entity: Option<String>,
        group: String,
        mode: String,
        callback: js_sys::Function,
    ) -> String {
        let sub_id = uuid::Uuid::new_v4().to_string();

        let mode = match mode.as_str() {
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
            },
        );

        sub_id
    }

    pub fn unsubscribe(&self, sub_id: String) -> bool {
        let mut inner = self.inner.borrow_mut();
        inner.subscriptions.remove(&sub_id).is_some()
    }

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

    pub fn get_schema(&self, entity: String) -> Result<JsValue, JsValue> {
        let inner = self.inner.borrow();
        match inner.schemas.get(&entity) {
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

    pub fn add_unique_constraint(&self, entity: String, fields: Vec<String>) -> Result<(), JsValue> {
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

    pub fn add_not_null(&self, entity: String, field: String) -> Result<(), JsValue> {
        let mut inner = self.inner.borrow_mut();
        inner
            .not_null_constraints
            .entry(entity)
            .or_default()
            .push(field);
        Ok(())
    }

    pub fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: String,
    ) -> Result<(), JsValue> {
        let on_delete = match on_delete.as_str() {
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

    pub fn add_index(&self, entity: String, fields: Vec<String>) -> Result<(), JsValue> {
        let mut inner = self.inner.borrow_mut();
        inner.indexes.entry(entity).or_default().push(fields);
        Ok(())
    }

    pub fn list_constraints(&self, entity: String) -> JsValue {
        let inner = self.inner.borrow();
        let mut constraints = Vec::new();

        if let Some(uniques) = inner.unique_constraints.get(&entity) {
            for fields in uniques {
                constraints.push(serde_json::json!({
                    "type": "unique",
                    "fields": fields
                }));
            }
        }

        if let Some(not_nulls) = inner.not_null_constraints.get(&entity) {
            for field in not_nulls {
                constraints.push(serde_json::json!({
                    "type": "not_null",
                    "field": field
                }));
            }
        }

        for fk in &inner.foreign_keys {
            if fk.source_entity == entity {
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

    pub async fn execute(&self, topic: String, payload: JsValue) -> Result<JsValue, JsValue> {
        let payload_bytes: Vec<u8> = if payload.is_null() || payload.is_undefined() {
            vec![]
        } else {
            let value: serde_json::Value = deserialize_js(&payload)?;
            serde_json::to_vec(&value)
                .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?
        };

        if let Some(admin_op) = parse_admin_topic(&topic) {
            return self.handle_admin_operation(admin_op, &payload_bytes).await;
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
                };
                let opts_js = serde_wasm_bindgen::to_value(&opts)
                    .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
                self.list(entity, opts_js).await
            }
            Request::Subscribe { .. } | Request::Unsubscribe { .. } => {
                Err(JsValue::from_str("use subscribe/unsubscribe methods directly"))
            }
        }
    }

    async fn handle_admin_operation(
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
            AdminOperation::SchemaGet { entity } => self.get_schema(entity),
            AdminOperation::ConstraintList { entity } => Ok(self.list_constraints(entity)),
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
                            on_delete.to_string(),
                        )?;
                    }
                    _ => return Err(JsValue::from_str("unknown constraint type")),
                }
                Ok(serialize_js(&serde_json::json!({"message": "constraint added"}))?)
            }
            _ => Err(JsValue::from_str(&format!(
                "admin operation not supported in WASM: {op:?}"
            ))),
        }
    }

    fn validate_not_null(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
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

    fn validate_unique(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        value: &serde_json::Value,
        current_id: Option<&str>,
    ) -> Result<(), JsValue> {
        if let Some(constraints) = inner.unique_constraints.get(entity) {
            let prefix = format!("data/{entity}/");
            let items = inner
                .storage
                .prefix_scan(prefix.as_bytes())
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            for constraint_fields in constraints {
                let new_values: Vec<Option<&serde_json::Value>> =
                    constraint_fields.iter().map(|f| value.get(f)).collect();

                for (_key, existing_data) in &items {
                    let existing: serde_json::Value = serde_json::from_slice(existing_data)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;

                    if let Some(existing_id) = existing.get("id").and_then(|v| v.as_str()) {
                        if current_id == Some(existing_id) {
                            continue;
                        }
                    }

                    let existing_values: Vec<Option<&serde_json::Value>> =
                        constraint_fields.iter().map(|f| existing.get(f)).collect();

                    if new_values == existing_values
                        && new_values.iter().all(|v| v.is_some() && *v != Some(&serde_json::Value::Null))
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

    fn validate_foreign_keys(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        for fk in &inner.foreign_keys {
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
            if inner.storage.get(target_key.as_bytes()).ok().flatten().is_none() {
                return Err(JsValue::from_str(&format!(
                    "foreign key violation: {entity}.{} references non-existent {}/{}",
                    fk.source_field, fk.target_entity, target_id
                )));
            }
        }
        Ok(())
    }

    fn check_foreign_key_constraints(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        id: &str,
    ) -> Result<Vec<(String, String)>, JsValue> {
        let mut cascade_deletes = Vec::new();

        for fk in &inner.foreign_keys {
            if fk.target_entity != entity {
                continue;
            }

            let prefix = format!("data/{}/", fk.source_entity);
            let items = inner
                .storage
                .prefix_scan(prefix.as_bytes())
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            for (_key, data) in items {
                let value: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;

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
                                cascade_deletes.push((fk.source_entity.clone(), source_id.to_string()));
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
                                inner
                                    .storage
                                    .insert(key.as_bytes(), &serialized)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    fn update_indexes(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
        old_value: Option<&serde_json::Value>,
    ) -> Result<(), JsValue> {
        if let Some(old) = old_value {
            self.remove_indexes(inner, entity, id, old)?;
        }

        if let Some(index_defs) = inner.indexes.get(entity) {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| {
                            match v {
                                serde_json::Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            }
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
                    inner
                        .storage
                        .insert(index_key.as_bytes(), &[])
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    fn remove_indexes(
        &self,
        inner: &DatabaseInner,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        if let Some(index_defs) = inner.indexes.get(entity) {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| {
                            match v {
                                serde_json::Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            }
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
                    let _ = inner.storage.remove(index_key.as_bytes());
                }
            }
        }

        Ok(())
    }

    fn matches_filter(&self, value: &serde_json::Value, filter: &FilterJs) -> bool {
        let field_value = value.get(&filter.field);

        match filter.op.as_str() {
            "eq" => field_value.map_or(false, |v| v == &filter.value),
            "ne" | "<>" => field_value.map_or(true, |v| v != &filter.value),
            "gt" | ">" => {
                self.compare_values(field_value, &filter.value)
                    .map_or(false, |ord| ord == std::cmp::Ordering::Greater)
            }
            "lt" | "<" => {
                self.compare_values(field_value, &filter.value)
                    .map_or(false, |ord| ord == std::cmp::Ordering::Less)
            }
            "gte" | ">=" => {
                self.compare_values(field_value, &filter.value)
                    .map_or(false, |ord| ord != std::cmp::Ordering::Less)
            }
            "lte" | "<=" => {
                self.compare_values(field_value, &filter.value)
                    .map_or(false, |ord| ord != std::cmp::Ordering::Greater)
            }
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
            _ => field_value.map_or(false, |v| v == &filter.value),
        }
    }

    fn compare_values(
        &self,
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

        parts.last().map_or(true, |last| last.is_empty() || text.ends_with(last))
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
            (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => a_str.cmp(b_str),
            (serde_json::Value::Bool(a_bool), serde_json::Value::Bool(b_bool)) => a_bool.cmp(b_bool),
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
                if field != "id" {
                    if let Some(v) = obj.get(field) {
                        projected.insert(field.clone(), v.clone());
                    }
                }
            }

            serde_json::Value::Object(projected)
        } else {
            value
        }
    }

    fn dispatch_event(&self, event: &ChangeEvent) {
        let inner = self.inner.borrow();

        for sub in inner.subscriptions.values() {
            if Self::matches_subscription(sub, event) {
                let event_js = match serialize_event(event) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let _ = sub.callback.call1(&JsValue::NULL, &event_js);
            }
        }
    }

    fn matches_subscription(sub: &SubscriptionEntry, event: &ChangeEvent) -> bool {
        if let Some(ref entity) = sub.entity {
            if entity != &event.entity {
                return false;
            }
        }

        match_pattern(&sub.pattern, &event.entity, &event.id)
    }
}

impl Default for WasmDatabase {
    fn default() -> Self {
        Self::new()
    }
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
    serde_wasm_bindgen::to_value(value)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))
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
    serde_wasm_bindgen::to_value(&event_js)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))
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

        let data1 =
            serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"})).unwrap();
        db.create("users".to_string(), data1).await.unwrap();

        let data2 =
            serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"})).unwrap();
        let result = db.create("users".to_string(), data2).await;
        assert!(result.is_err());
    }
}
