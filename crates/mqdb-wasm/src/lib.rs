use mqdb::storage::Storage;
use mqdb::{match_pattern, ChangeEvent, FieldDefinition, FieldType, Operation, Schema};
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
    id_counters: HashMap<String, u64>,
}

struct SubscriptionEntry {
    pattern: String,
    entity: Option<String>,
    callback: js_sys::Function,
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
        }

        let event = ChangeEvent::create(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    pub async fn get(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
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

        let mut value: serde_json::Value = serde_json::from_slice(&existing_data)
            .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

        if let (serde_json::Value::Object(existing), serde_json::Value::Object(new_fields)) =
            (&mut value, updates)
        {
            for (k, v) in new_fields {
                existing.insert(k, v);
            }
        }

        if let Some(schema) = inner.schemas.get(&entity) {
            schema
                .validate(&value)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }

        let serialized = serde_json::to_vec(&value)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;

        inner
            .storage
            .insert(key.as_bytes(), &serialized)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        drop(inner);

        let event = ChangeEvent::update(entity, id, value.clone());
        self.dispatch_event(&event);

        serialize_js(&value)
    }

    pub async fn delete(&self, entity: String, id: String) -> Result<(), JsValue> {
        let key = format!("data/{entity}/{id}");

        let inner = self.inner.borrow();
        inner
            .storage
            .get(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str(&format!("not found: {entity}/{id}")))?;

        inner
            .storage
            .remove(key.as_bytes())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        drop(inner);

        let event = ChangeEvent::delete(entity, id);
        self.dispatch_event(&event);

        Ok(())
    }

    pub async fn list(&self, entity: String) -> Result<JsValue, JsValue> {
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
            results.push(parsed);
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
                field_def = field_def.default(default);
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
