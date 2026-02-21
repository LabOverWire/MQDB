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

    pub(super) fn json_success(entity: &str, id: &str, data: &Value) -> Vec<u8> {
        let result = json!({ "status": "ok", "id": id, "entity": entity, "data": data });
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
