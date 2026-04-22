// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    AdminOperation, FilterJs, JsValue, ListOptions, PaginationJs, Request, SortOrderJs,
    WasmDatabase, build_request, deserialize_js, parse_admin_topic, parse_db_topic, serialize_js,
    wasm_bindgen,
};

#[wasm_bindgen(js_class = "Database")]
impl WasmDatabase {
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
                    includes: None,
                };
                let opts_json = serde_json::to_string(&opts)
                    .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
                let opts_js = js_sys::JSON::parse(&opts_json)
                    .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))?;
                self.list(entity, opts_js).await
            }
            Request::Subscribe { .. } | Request::Unsubscribe { .. } => Err(JsValue::from_str(
                "use subscribe/unsubscribe methods directly",
            )),
        }
    }
}

impl WasmDatabase {
    pub(crate) async fn handle_admin_operation(
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
                self.add_schema_async(entity, schema_js).await?;
                Ok(serialize_js(&serde_json::json!({"message": "schema set"}))?)
            }
            AdminOperation::SchemaGet { entity } => self.get_schema(&entity),
            AdminOperation::ConstraintList { entity } => Ok(self.list_constraints(&entity)),
            AdminOperation::ConstraintAdd { entity } => {
                self.handle_constraint_add(entity, &data).await
            }
            AdminOperation::IndexAdd { entity } => {
                let fields: Vec<String> = data
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if fields.is_empty() {
                    return Err(JsValue::from_str("fields required for index"));
                }
                self.add_index_async(entity, fields).await?;
                Ok(serialize_js(
                    &serde_json::json!({"message": "index added"}),
                )?)
            }
            AdminOperation::Catalog => {
                let items = self.storage.prefix_scan(b"data/").await?;
                let mut entities = std::collections::BTreeSet::new();
                for (key, _) in &items {
                    if let Ok(key_str) = std::str::from_utf8(key)
                        && let Some(rest) = key_str.strip_prefix("data/")
                        && let Some(slash_pos) = rest.find('/')
                    {
                        entities.insert(rest[..slash_pos].to_string());
                    }
                }
                let entity_list: Vec<&str> = entities.iter().map(String::as_str).collect();
                Ok(serialize_js(&serde_json::json!({"entities": entity_list}))?)
            }
            _ => Err(JsValue::from_str(&format!(
                "admin operation not supported in WASM: {op:?}"
            ))),
        }
    }

    async fn handle_constraint_add(
        &self,
        entity: String,
        data: &serde_json::Value,
    ) -> Result<JsValue, JsValue> {
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
                self.add_unique_constraint_async(entity, fields).await?;
            }
            Some("not_null") => {
                let field = data
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| JsValue::from_str("field required"))?;
                self.add_not_null_async(entity, field.to_string()).await?;
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
                self.add_foreign_key_async(
                    entity,
                    source_field.to_string(),
                    target_entity.to_string(),
                    target_field.to_string(),
                    on_delete,
                )
                .await?;
            }
            _ => return Err(JsValue::from_str("unknown constraint type")),
        }
        serialize_js(&serde_json::json!({"message": "constraint added"}))
    }
}
