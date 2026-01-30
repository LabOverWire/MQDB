use super::{
    FieldDefJs, FieldDefinition, FieldType, JsValue, Schema, SchemaDefinition, WasmDatabase,
    wasm_bindgen,
};

#[wasm_bindgen]
impl WasmDatabase {
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
}
