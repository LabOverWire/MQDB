use crate::error::{Error, Result};
use crate::keys;
use serde_json::Value;

#[derive(Clone)]
pub struct Entity {
    pub name: String,
    pub id: String,
    pub data: Value,
}

impl Entity {
    pub fn new(name: String, id: String, data: Value) -> Self {
        Self { name, id, data }
    }

    pub fn from_json(name: String, mut data: Value) -> Result<Self> {
        let id = data
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                data.get("id")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string())
            })
            .ok_or_else(|| Error::Validation("missing 'id' field".into()))?;

        if let Value::Object(ref mut obj) = data {
            obj.remove("id");
        }

        Ok(Self { name, id, data })
    }

    pub fn to_json(&self) -> Value {
        let mut data = self.data.clone();
        if let Value::Object(ref mut obj) = data {
            obj.insert("id".to_string(), Value::String(self.id.clone()));
        }
        data
    }

    pub fn key(&self) -> Vec<u8> {
        keys::encode_data_key(&self.name, &self.id)
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let json_data = serde_json::to_vec(&self.data)?;
        Ok(crate::checksum::encode_with_checksum(&json_data))
    }

    pub fn deserialize(name: String, id: String, data: &[u8]) -> Result<Self> {
        let json_data = crate::checksum::decode_and_verify(data).map_err(|err| {
            tracing::warn!("checksum verification failed for {}/{}: {}", name, id, err);
            Error::Corruption {
                entity: name.clone(),
                id: id.clone(),
            }
        })?;
        let value: Value = serde_json::from_slice(json_data)?;
        Ok(Self { name, id, data: value })
    }

    pub fn get_field(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    pub fn extract_index_values(&self, fields: &[String]) -> Vec<(String, Vec<u8>)> {
        let mut values = Vec::new();

        for field in fields {
            if let Some(value) = self.get_field(field) {
                if let Ok(encoded) = keys::encode_value_for_index(value) {
                    values.push((field.clone(), encoded));
                }
            }
        }

        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_entity_from_json() {
        let data = json!({
            "id": "123",
            "name": "John",
            "email": "john@example.com"
        });

        let entity = Entity::from_json("users".into(), data).unwrap();
        assert_eq!(entity.id, "123");
        assert_eq!(entity.name, "users");
        assert_eq!(entity.get_field("name").unwrap(), "John");
    }

    #[test]
    fn test_entity_to_json() {
        let entity = Entity::new(
            "users".into(),
            "123".into(),
            json!({"name": "John"}),
        );

        let json = entity.to_json();
        assert_eq!(json["id"], "123");
        assert_eq!(json["name"], "John");
    }
}
