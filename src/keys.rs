use crate::error::{Error, Result};

pub const SEPARATOR: u8 = b'/';
pub const DATA_PREFIX: &[u8] = b"data";
pub const INDEX_PREFIX: &[u8] = b"idx";
pub const SUB_PREFIX: &[u8] = b"sub";
pub const DEDUP_PREFIX: &[u8] = b"dedup";
pub const META_PREFIX: &[u8] = b"meta";

#[must_use]
pub fn encode_data_key(entity: &str, id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        DATA_PREFIX.len() + 1 + entity.len() + 1 + id.len(),
    );
    key.extend_from_slice(DATA_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(entity.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(id.as_bytes());
    key
}

/// # Errors
/// Returns an error if the key is not a valid data key.
pub fn decode_data_key(key: &[u8]) -> Result<(String, String)> {
    let parts: Vec<&[u8]> = key.split(|&b| b == SEPARATOR).collect();

    if parts.len() != 3 || parts[0] != DATA_PREFIX {
        return Err(Error::InvalidKey(format!("invalid data key: {key:?}")));
    }

    let entity = String::from_utf8(parts[1].to_vec())
        .map_err(|_| Error::InvalidKey("entity not valid UTF-8".into()))?;
    let id = String::from_utf8(parts[2].to_vec())
        .map_err(|_| Error::InvalidKey("id not valid UTF-8".into()))?;

    Ok((entity, id))
}

#[must_use]
pub fn encode_index_key(entity: &str, field: &str, value: &[u8], id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        INDEX_PREFIX.len() + 1 + entity.len() + 1 + field.len() + 1 + value.len() + 1 + id.len(),
    );
    key.extend_from_slice(INDEX_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(entity.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(field.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(value);
    key.push(SEPARATOR);
    key.extend_from_slice(id.as_bytes());
    key
}

#[must_use]
pub fn encode_index_prefix(entity: &str, field: &str, value: Option<&[u8]>) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(INDEX_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(entity.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(field.as_bytes());

    if let Some(v) = value {
        key.push(SEPARATOR);
        key.extend_from_slice(v);
    }

    key
}

#[must_use]
pub fn encode_subscription_key(sub_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(SUB_PREFIX.len() + 1 + sub_id.len());
    key.extend_from_slice(SUB_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(sub_id.as_bytes());
    key
}

#[must_use]
pub fn encode_dedup_key(correlation_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(DEDUP_PREFIX.len() + 1 + correlation_id.len());
    key.extend_from_slice(DEDUP_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(correlation_id.as_bytes());
    key
}

#[must_use]
pub fn encode_meta_key(key_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(META_PREFIX.len() + 1 + key_name.len());
    key.extend_from_slice(META_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(key_name.as_bytes());
    key
}

/// # Errors
/// Returns an error if the value cannot be indexed.
pub fn encode_value_for_index(value: &serde_json::Value) -> Result<Vec<u8>> {
    match value {
        serde_json::Value::Null => Ok(b"null".to_vec()),
        serde_json::Value::Bool(b) => {
            if *b {
                Ok(b"true".to_vec())
            } else {
                Ok(b"false".to_vec())
            }
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(format!("{i:020}").into_bytes())
            } else if let Some(f) = n.as_f64() {
                Ok(format!("{f:020.6}").into_bytes())
            } else {
                Ok(n.to_string().into_bytes())
            }
        }
        serde_json::Value::String(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(Error::Validation("cannot index arrays or objects directly".into())),
    }
}

#[must_use]
pub fn encode_schema_key(entity: &str) -> Vec<u8> {
    format!("meta/schema/{entity}").into_bytes()
}

#[must_use]
pub fn encode_constraint_key(constraint_type: &str, entity: &str, name: &str) -> Vec<u8> {
    format!("meta/constraint/{constraint_type}/{entity}/{name}").into_bytes()
}

#[must_use]
pub fn encode_fk_reverse_index_key(
    target_entity: &str,
    target_id: &str,
    source_entity: &str,
    source_id: &str,
) -> Vec<u8> {
    format!("fkref/{target_entity}/{target_id}/{source_entity}/{source_id}").into_bytes()
}

#[must_use]
pub fn encode_fk_reverse_prefix(target_entity: &str, target_id: &str) -> Vec<u8> {
    format!("fkref/{target_entity}/{target_id}/").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_data_key() {
        let key = encode_data_key("users", "123");
        assert_eq!(key, b"data/users/123");

        let (entity, id) = decode_data_key(&key).unwrap();
        assert_eq!(entity, "users");
        assert_eq!(id, "123");
    }

    #[test]
    fn test_encode_index_key() {
        let key = encode_index_key("users", "email", b"test@example.com", "123");
        assert_eq!(key, b"idx/users/email/test@example.com/123");
    }

    #[test]
    fn test_encode_value_for_index() {
        let val = serde_json::json!(42);
        let encoded = encode_value_for_index(&val).unwrap();
        assert_eq!(encoded, b"00000000000000000042");

        let val = serde_json::json!("hello");
        let encoded = encode_value_for_index(&val).unwrap();
        assert_eq!(encoded, b"hello");
    }
}
