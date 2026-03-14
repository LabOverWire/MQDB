// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::PartitionId;
use super::DbRequestHandler;
use serde_json::{Value, json};

impl DbRequestHandler {
    pub(super) fn json_error(code: u16, message: &str) -> Vec<u8> {
        let result = json!({
            "status": "error",
            "code": code,
            "message": message
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    pub(super) fn current_time_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX)
    }

    pub(super) fn generate_id_for_partition(
        &self,
        entity: &str,
        partition: PartitionId,
        data: &[u8],
    ) -> String {
        super::super::db::generate_id_for_partition(self.node_id.get(), entity, partition, data)
    }
}

pub(crate) fn json_ok_with_id(id: &str, data: &Value) -> Vec<u8> {
    let mut merged = data.clone();
    if let Value::Object(ref mut obj) = merged {
        obj.insert("id".to_string(), Value::String(id.to_string()));
    }
    let result = json!({"status": "ok", "data": merged});
    serde_json::to_vec(&result).unwrap_or_default()
}

pub(crate) fn flatten_list_items(items: Vec<Value>) -> Vec<Value> {
    items
        .into_iter()
        .map(|item| {
            if let Value::Object(mut obj) = item {
                if let (Some(id), Some(Value::Object(mut data_obj))) =
                    (obj.remove("id"), obj.remove("data"))
                {
                    data_obj.insert("id".to_string(), id);
                    return Value::Object(data_obj);
                }
                return Value::Object(obj);
            }
            item
        })
        .collect()
}

pub(crate) fn validate_projection_against_schema(
    schema_data: &[u8],
    entity: &str,
    fields: &[String],
) -> Option<String> {
    let schema: crate::schema::Schema = serde_json::from_slice(schema_data).ok()?;
    for field in fields {
        if field != "id" && !schema.fields.contains_key(field) {
            return Some(format!(
                "schema violation for '{entity}': projection field '{field}' does not exist in schema",
            ));
        }
    }
    None
}

pub(crate) fn parse_projection(payload: &[u8]) -> Option<Vec<String>> {
    if payload.is_empty() {
        return None;
    }
    let parsed: Value = serde_json::from_slice(payload).ok()?;
    let arr = parsed.get("projection")?.as_array()?;
    let fields: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}
