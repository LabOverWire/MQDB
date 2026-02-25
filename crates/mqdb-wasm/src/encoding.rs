// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use wasm_bindgen::JsValue;

fn encode_i64_sortable(val: i64) -> [u8; 8] {
    let mut out = val.to_be_bytes();
    out[0] ^= 0x80;
    out
}

fn encode_f64_sortable(val: f64) -> [u8; 8] {
    let mut out = val.to_bits().to_be_bytes();
    if val.is_sign_negative() {
        for b in &mut out {
            *b ^= 0xFF;
        }
    } else {
        out[0] ^= 0x80;
    }
    out
}

pub(crate) fn encode_value_for_index(value: &serde_json::Value) -> Result<Vec<u8>, JsValue> {
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
                Ok(encode_i64_sortable(i).to_vec())
            } else if let Some(f) = n.as_f64() {
                Ok(encode_f64_sortable(f).to_vec())
            } else {
                Ok(n.to_string().into_bytes())
            }
        }
        serde_json::Value::String(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(JsValue::from_str("cannot index arrays or objects directly")),
    }
}
