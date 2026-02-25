// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_create_and_read() {
    let db = WasmDatabase::new();
    let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
    let result = db.create("users".to_string(), data).await.unwrap();
    let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    let id = result_value["id"].as_str().unwrap().to_string();

    let read_result = db.read("users".to_string(), id).await.unwrap();
    let read_value: serde_json::Value = serde_wasm_bindgen::from_value(read_result).unwrap();
    assert_eq!(read_value["name"], "Alice");
}

#[wasm_bindgen_test]
async fn test_update() {
    let db = WasmDatabase::new();
    let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
    let result = db.create("users".to_string(), data).await.unwrap();
    let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    let id = result_value["id"].as_str().unwrap().to_string();

    let update = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Bob"})).unwrap();
    let updated = db
        .update("users".to_string(), id.clone(), update)
        .await
        .unwrap();
    let updated_value: serde_json::Value = serde_wasm_bindgen::from_value(updated).unwrap();
    assert_eq!(updated_value["name"], "Bob");
}

#[wasm_bindgen_test]
async fn test_delete() {
    let db = WasmDatabase::new();
    let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
    let result = db.create("users".to_string(), data).await.unwrap();
    let result_value: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    let id = result_value["id"].as_str().unwrap().to_string();

    db.delete("users".to_string(), id.clone()).await.unwrap();

    let read_result = db.read("users".to_string(), id).await;
    assert!(read_result.is_err());
}

#[wasm_bindgen_test]
async fn test_list_with_filter() {
    let db = WasmDatabase::new();

    for name in &["Alice", "Bob", "Charlie"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
        db.create("users".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Bob"}]
    }))
    .unwrap();

    let result = db.list("users".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["name"], "Bob");
}

#[wasm_bindgen_test]
async fn test_unique_constraint() {
    let db = WasmDatabase::new();
    db.add_unique_constraint("users".to_string(), vec!["email".to_string()])
        .unwrap();

    let data1 =
        serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"})).unwrap();
    db.create("users".to_string(), data1).await.unwrap();

    let data2 =
        serde_wasm_bindgen::to_value(&serde_json::json!({"email": "test@example.com"})).unwrap();
    let result = db.create("users".to_string(), data2).await;
    assert!(result.is_err());
}

#[wasm_bindgen_test]
async fn test_index_accelerated_list() {
    let db = WasmDatabase::new();
    db.add_index("users".to_string(), vec!["name".to_string()])
        .unwrap();

    for name in &["Alice", "Bob", "Charlie", "Bob"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
        db.create("users".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Bob"}]
    }))
    .unwrap();

    let result = db.list("users".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.iter().all(|v| v["name"] == "Bob"));
}

#[wasm_bindgen_test]
async fn test_index_scan_fallback_no_index() {
    let db = WasmDatabase::new();

    for name in &["Alice", "Bob"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
        db.create("users".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Alice"}]
    }))
    .unwrap();

    let result = db.list("users".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["name"], "Alice");
}

#[wasm_bindgen_test]
async fn test_count_with_indexed_filter() {
    let db = WasmDatabase::new();
    db.add_index("items".to_string(), vec!["status".to_string()])
        .unwrap();

    for status in &["active", "active", "inactive", "active"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"status": status})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "status", "op": "eq", "value": "active"}]
    }))
    .unwrap();

    let count = db.count("items".to_string(), opts).await.unwrap();
    assert_eq!(count, 3);
}

#[wasm_bindgen_test]
async fn test_count_without_index() {
    let db = WasmDatabase::new();

    for status in &["active", "inactive", "active"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"status": status})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "status", "op": "eq", "value": "active"}]
    }))
    .unwrap();

    let count = db.count("items".to_string(), opts).await.unwrap();
    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
async fn test_count_no_filters() {
    let db = WasmDatabase::new();

    for name in &["Alice", "Bob", "Charlie"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
        db.create("users".to_string(), data).await.unwrap();
    }

    let count = db.count("users".to_string(), JsValue::NULL).await.unwrap();
    assert_eq!(count, 3);
}

#[wasm_bindgen_test]
async fn test_cursor_with_projection() {
    let db = WasmDatabase::new();

    for (name, age) in &[("Alice", 30), ("Bob", 25)] {
        let data =
            serde_wasm_bindgen::to_value(&serde_json::json!({"name": name, "age": age})).unwrap();
        db.create("users".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "projection": ["name"]
    }))
    .unwrap();

    let mut cursor = db.cursor("users".to_string(), opts).await.unwrap();
    assert_eq!(cursor.count(), 2);

    let item = cursor.next_item().unwrap();
    let value: serde_json::Value = serde_wasm_bindgen::from_value(item).unwrap();
    assert!(value.get("id").is_some());
    assert!(value.get("name").is_some());
    assert!(value.get("age").is_none());
}

#[wasm_bindgen_test]
async fn test_range_filter_indexed_numeric() {
    let db = WasmDatabase::new();
    db.add_index("items".to_string(), vec!["score".to_string()])
        .unwrap();

    for score in &[10, 20, 30, 40, 50] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"score": score})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "gt", "value": 20}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "gte", "value": 20}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 4);

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "lt", "value": 30}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "lte", "value": 30}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [
            {"field": "score", "op": "gte", "value": 20},
            {"field": "score", "op": "lte", "value": 40}
        ]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);
}

#[wasm_bindgen_test]
async fn test_range_filter_negative_numbers() {
    let db = WasmDatabase::new();
    db.add_index("temps".to_string(), vec!["temp".to_string()])
        .unwrap();

    for temp in &[-20, -10, 0, 10, 20] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"temp": temp})).unwrap();
        db.create("temps".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "temp", "op": "gte", "value": -10}]
    }))
    .unwrap();
    let result = db.list("temps".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 4);

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "temp", "op": "lt", "value": 0}]
    }))
    .unwrap();
    let result = db.list("temps".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);
}

#[wasm_bindgen_test]
async fn test_range_filter_string_field() {
    let db = WasmDatabase::new();
    db.add_index("items".to_string(), vec!["name".to_string()])
        .unwrap();

    for name in &["alpha", "bravo", "charlie", "delta", "echo"] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"name": name})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "name", "op": "gte", "value": "charlie"}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);
}

#[wasm_bindgen_test]
async fn test_range_with_equality_post_filter() {
    let db = WasmDatabase::new();
    db.add_index("items".to_string(), vec!["score".to_string()])
        .unwrap();

    for (score, label) in &[(10, "a"), (20, "b"), (30, "a"), (40, "b"), (50, "a")] {
        let data =
            serde_wasm_bindgen::to_value(&serde_json::json!({"score": score, "label": label}))
                .unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [
            {"field": "score", "op": "gt", "value": 10},
            {"field": "label", "op": "eq", "value": "a"}
        ]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.iter().all(|v| v["label"] == "a"));
}

#[wasm_bindgen_test]
async fn test_range_filter_no_index_fallback() {
    let db = WasmDatabase::new();

    for score in &[10, 20, 30] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"score": score})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "gt", "value": 10}]
    }))
    .unwrap();
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);
}

#[wasm_bindgen_test]
async fn test_range_count_indexed() {
    let db = WasmDatabase::new();
    db.add_index("items".to_string(), vec!["score".to_string()])
        .unwrap();

    for score in &[10, 20, 30, 40, 50] {
        let data = serde_wasm_bindgen::to_value(&serde_json::json!({"score": score})).unwrap();
        db.create("items".to_string(), data).await.unwrap();
    }

    let opts = serde_wasm_bindgen::to_value(&serde_json::json!({
        "filters": [{"field": "score", "op": "gte", "value": 30}]
    }))
    .unwrap();
    let count = db.count("items".to_string(), opts).await.unwrap();
    assert_eq!(count, 3);
}
