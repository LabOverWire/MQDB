use crate::error::{Error, Result};
use crate::keys;
use crate::storage::{BatchWriter, Storage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Array,
    Object,
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub default: Option<Value>,
}

impl FieldDefinition {
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            required: false,
            default: None,
        }
    }

    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    #[must_use]
    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    fn validate_value(&self, value: &Value) -> bool {
        matches!(
            (&self.field_type, value),
            (FieldType::String, Value::String(_))
                | (FieldType::Number, Value::Number(_))
                | (FieldType::Boolean, Value::Bool(_))
                | (FieldType::Array, Value::Array(_))
                | (FieldType::Object, Value::Object(_))
                | (FieldType::Null, Value::Null)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub entity: String,
    pub fields: HashMap<String, FieldDefinition>,
}

impl Schema {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            fields: HashMap::new(),
        }
    }

    #[must_use]
    pub fn add_field(mut self, field: FieldDefinition) -> Self {
        self.fields.insert(field.name.clone(), field);
        self
    }

    /// # Errors
    /// Returns an error if the data does not conform to the schema.
    pub fn validate(&self, data: &Value) -> Result<()> {
        let obj = data.as_object().ok_or_else(|| Error::SchemaViolation {
            entity: self.entity.clone(),
            field: "<root>".to_string(),
            reason: "entity data must be an object".to_string(),
        })?;

        for (field_name, field_def) in &self.fields {
            match obj.get(field_name) {
                Some(value) => {
                    if !field_def.validate_value(value) {
                        return Err(Error::SchemaViolation {
                            entity: self.entity.clone(),
                            field: field_name.clone(),
                            reason: format!(
                                "expected type {:?}, got {}",
                                field_def.field_type,
                                match value {
                                    Value::String(_) => "string",
                                    Value::Number(_) => "number",
                                    Value::Bool(_) => "boolean",
                                    Value::Array(_) => "array",
                                    Value::Object(_) => "object",
                                    Value::Null => "null",
                                }
                            ),
                        });
                    }
                }
                None => {
                    if field_def.required && field_def.default.is_none() {
                        return Err(Error::SchemaViolation {
                            entity: self.entity.clone(),
                            field: field_name.clone(),
                            reason: "required field is missing".to_string(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error if the data is not an object.
    pub fn apply_defaults(&self, data: &mut Value) -> Result<()> {
        let obj = data.as_object_mut().ok_or_else(|| Error::SchemaViolation {
            entity: self.entity.clone(),
            field: "<root>".to_string(),
            reason: "entity data must be an object".to_string(),
        })?;

        for (field_name, field_def) in &self.fields {
            if !obj.contains_key(field_name)
                && let Some(default) = &field_def.default {
                    obj.insert(field_name.clone(), default.clone());
                }
        }

        Ok(())
    }
}

pub struct SchemaRegistry {
    schemas: HashMap<String, Schema>,
}

impl SchemaRegistry {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn add_schema(&mut self, schema: Schema) {
        self.schemas.insert(schema.entity.clone(), schema);
    }

    #[must_use]
    pub fn get_schema(&self, entity: &str) -> Option<&Schema> {
        self.schemas.get(entity)
    }

    /// # Errors
    /// Returns an error if the entity has a schema and validation fails.
    pub fn validate_entity(&self, entity_name: &str, data: &Value) -> Result<()> {
        if let Some(schema) = self.schemas.get(entity_name) {
            schema.validate(data)?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if the entity has a schema and applying defaults fails.
    pub fn apply_defaults(&self, entity_name: &str, data: &mut Value) -> Result<()> {
        if let Some(schema) = self.schemas.get(entity_name) {
            schema.apply_defaults(data)?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if serialization fails.
    pub fn persist_schema(&self, batch: &mut BatchWriter, schema: &Schema) -> Result<()> {
        let key = keys::encode_schema_key(&schema.entity);
        let value = serde_json::to_vec(schema)?;
        batch.insert(key, value);
        Ok(())
    }

    /// # Errors
    /// Returns an error if reading or deserializing schemas fails.
    pub fn load_schemas(&mut self, storage: &Storage) -> Result<()> {
        let prefix = b"meta/schema/";
        let items = storage.prefix_scan(prefix)?;

        for (_key, value) in items {
            let schema: Schema = serde_json::from_slice(&value)?;
            self.schemas.insert(schema.entity.clone(), schema);
        }

        Ok(())
    }

    pub fn remove_schema(&mut self, batch: &mut BatchWriter, entity: &str) {
        self.schemas.remove(entity);
        let key = keys::encode_schema_key(entity);
        batch.remove(key);
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_field_definition() {
        let field = FieldDefinition::new("name", FieldType::String)
            .required()
            .with_default(json!("anonymous"));

        assert_eq!(field.name, "name");
        assert_eq!(field.field_type, FieldType::String);
        assert!(field.required);
        assert_eq!(field.default, Some(json!("anonymous")));
    }

    #[test]
    fn test_schema_validation_valid() {
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String).required())
            .add_field(FieldDefinition::new("age", FieldType::Number));

        let data = json!({
            "name": "Alice",
            "age": 30
        });

        assert!(schema.validate(&data).is_ok());
    }

    #[test]
    fn test_schema_validation_missing_required() {
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String).required());

        let data = json!({
            "age": 30
        });

        let result = schema.validate(&data);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::SchemaViolation { .. })));
    }

    #[test]
    fn test_schema_validation_wrong_type() {
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("age", FieldType::Number));

        let data = json!({
            "age": "thirty"
        });

        let result = schema.validate(&data);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::SchemaViolation { .. })));
    }

    #[test]
    fn test_schema_apply_defaults() {
        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String))
            .add_field(
                FieldDefinition::new("status", FieldType::String).with_default(json!("active")),
            );

        let mut data = json!({
            "name": "Alice"
        });

        schema.apply_defaults(&mut data).unwrap();
        assert_eq!(data["status"], "active");
    }

    #[test]
    fn test_schema_registry() {
        let mut registry = SchemaRegistry::new();

        let schema = Schema::new("users")
            .add_field(FieldDefinition::new("name", FieldType::String).required());

        registry.add_schema(schema);

        let valid = json!({"name": "Alice"});
        assert!(registry.validate_entity("users", &valid).is_ok());

        let invalid = json!({"age": 30});
        assert!(registry.validate_entity("users", &invalid).is_err());
    }
}
