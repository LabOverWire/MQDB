// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    FieldDefJs, FieldDefinition, FieldType, JsValue, Schema, SchemaDefinition, WasmDatabase,
    wasm_bindgen,
};
use mqdb_core::keys::encode_schema_key;

#[wasm_bindgen(js_class = "Database")]
impl WasmDatabase {
    /// Adds a schema definition for an entity (sync, memory backend only).
    ///
    /// # Errors
    /// Returns an error if the schema definition is invalid.
    #[wasm_bindgen(js_name = "addSchema")]
    pub fn add_schema(&self, entity: String, schema_js: JsValue) -> Result<(), JsValue> {
        let schema = Self::parse_schema(&entity, schema_js)?;
        let mut inner = self.borrow_inner_mut()?;
        inner.schemas.insert(entity, schema);
        Ok(())
    }

    /// Adds a schema definition for an entity and persists to storage.
    ///
    /// # Errors
    /// Returns an error if the schema definition is invalid or storage fails.
    #[wasm_bindgen(js_name = "addSchemaAsync")]
    pub async fn add_schema_async(
        &self,
        entity: String,
        schema_js: JsValue,
    ) -> Result<(), JsValue> {
        let schema = Self::parse_schema(&entity, schema_js)?;

        if !self.storage.is_memory() {
            let key = encode_schema_key(&entity);
            let bytes = serde_json::to_vec(&schema)
                .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
            self.storage.insert(&key, &bytes).await?;
        }

        let mut inner = self.borrow_inner_mut()?;
        inner.schemas.insert(entity, schema);
        Ok(())
    }

    /// Gets the schema definition for an entity.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    #[wasm_bindgen(js_name = "getSchema")]
    pub fn get_schema(&self, entity: &str) -> Result<JsValue, JsValue> {
        let inner = self.borrow_inner()?;
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
}

impl WasmDatabase {
    fn parse_schema(entity: &str, schema_js: JsValue) -> Result<Schema, JsValue> {
        let schema_def: SchemaDefinition = serde_wasm_bindgen::from_value(schema_js)
            .map_err(|e| JsValue::from_str(&format!("invalid schema: {e}")))?;

        let mut schema = Schema::new(entity);

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

        Ok(schema)
    }
}
