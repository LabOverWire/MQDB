// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn json_to_js(value: &serde_json::Value) -> JsValue {
    js_sys::JSON::parse(&value.to_string()).unwrap()
}

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Bob"}]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Bob"}]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "name", "op": "eq", "value": "Alice"}]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "status", "op": "eq", "value": "active"}]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "status", "op": "eq", "value": "active"}]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "projection": ["name"]
    }));

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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "gt", "value": 20}]
    }));
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "gte", "value": 20}]
    }));
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 4);

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "lt", "value": 30}]
    }));
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 2);

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "lte", "value": 30}]
    }));
    let result = db.list("items".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 3);

    let opts = json_to_js(&serde_json::json!({
        "filters": [
            {"field": "score", "op": "gte", "value": 20},
            {"field": "score", "op": "lte", "value": 40}
        ]
    }));
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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "temp", "op": "gte", "value": -10}]
    }));
    let result = db.list("temps".to_string(), opts).await.unwrap();
    let values: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(values.len(), 4);

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "temp", "op": "lt", "value": 0}]
    }));
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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "name", "op": "gte", "value": "charlie"}]
    }));
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

    let opts = json_to_js(&serde_json::json!({
        "filters": [
            {"field": "score", "op": "gt", "value": 10},
            {"field": "label", "op": "eq", "value": "a"}
        ]
    }));
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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "gt", "value": 10}]
    }));
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

    let opts = json_to_js(&serde_json::json!({
        "filters": [{"field": "score", "op": "gte", "value": 30}]
    }));
    let count = db.count("items".to_string(), opts).await.unwrap();
    assert_eq!(count, 3);
}

#[wasm_bindgen_test]
async fn test_set_null_cascade_bumps_version() {
    let db = WasmDatabase::new();
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        "set_null",
    )
    .unwrap();

    let user =
        serde_wasm_bindgen::to_value(&serde_json::json!({"id": "u1", "name": "Alice"})).unwrap();
    db.create("users".into(), user).await.unwrap();

    let post = serde_wasm_bindgen::to_value(
        &serde_json::json!({"id": "p1", "author_id": "u1", "title": "Hello"}),
    )
    .unwrap();
    db.create("posts".into(), post).await.unwrap();

    db.delete("users".into(), "u1".into()).await.unwrap();

    let result = db.read("posts".into(), "p1".into()).await.unwrap();
    let val: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(val["author_id"], serde_json::Value::Null);
    assert_eq!(val["_version"], 2);
}

#[wasm_bindgen_test]
fn test_set_null_cascade_updates_index() {
    let db = WasmDatabase::new();
    db.add_index("posts".into(), vec!["author_id".into()])
        .unwrap();
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        "set_null",
    )
    .unwrap();

    let user =
        serde_wasm_bindgen::to_value(&serde_json::json!({"id": "u1", "name": "Alice"})).unwrap();
    db.create_sync("users".into(), user).unwrap();

    let post = serde_wasm_bindgen::to_value(
        &serde_json::json!({"id": "p1", "author_id": "u1", "title": "Hello"}),
    )
    .unwrap();
    db.create_sync("posts".into(), post).unwrap();

    let before_count = db
        .storage
        .prefix_scan_sync(b"index/posts/author_id/u1/")
        .unwrap()
        .len();
    assert_eq!(before_count, 1);

    db.delete_sync("users".into(), "u1".into()).unwrap();

    let after_count = db
        .storage
        .prefix_scan_sync(b"index/posts/author_id/u1/")
        .unwrap()
        .len();
    assert_eq!(after_count, 0);
}

#[wasm_bindgen_test]
async fn test_set_null_cascade_dispatches_update_event() {
    let db = WasmDatabase::new();
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        "set_null",
    )
    .unwrap();

    let events: Rc<RefCell<Vec<serde_json::Value>>> = Rc::new(RefCell::new(Vec::new()));
    let events_clone = Rc::clone(&events);
    let closure = Closure::wrap(Box::new(move |val: JsValue| {
        if let Ok(parsed) = serde_wasm_bindgen::from_value::<serde_json::Value>(val) {
            events_clone.borrow_mut().push(parsed);
        }
    }) as Box<dyn FnMut(JsValue)>);
    let js_fn: js_sys::Function = closure.as_ref().unchecked_ref::<js_sys::Function>().clone();
    closure.forget();
    db.subscribe("*".into(), Some("posts".into()), js_fn)
        .unwrap();

    let user =
        serde_wasm_bindgen::to_value(&serde_json::json!({"id": "u1", "name": "Alice"})).unwrap();
    db.create("users".into(), user).await.unwrap();

    let post = serde_wasm_bindgen::to_value(
        &serde_json::json!({"id": "p1", "author_id": "u1", "title": "Hello"}),
    )
    .unwrap();
    db.create("posts".into(), post).await.unwrap();

    let pre_delete_count = events.borrow().len();
    db.delete("users".into(), "u1".into()).await.unwrap();

    let captured = events.borrow();
    let new_events: Vec<_> = captured.iter().skip(pre_delete_count).collect();
    assert!(
        new_events
            .iter()
            .any(|e| e["operation"] == "update" && e["entity"] == "posts" && e["id"] == "p1"),
        "expected update event for posts/p1, got: {new_events:?}"
    );
}

#[wasm_bindgen_test]
async fn test_unique_constraint_via_index_still_catches_violations() {
    let db = WasmDatabase::new();
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .unwrap();

    let u1 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "a@b.com"})).unwrap();
    db.create("users".into(), u1).await.unwrap();

    let u2 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "a@b.com"})).unwrap();
    let result = db.create("users".into(), u2).await;
    assert!(result.is_err());

    let u3 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "c@d.com"})).unwrap();
    db.create("users".into(), u3).await.unwrap();
}

#[wasm_bindgen_test]
async fn test_unique_constraint_allows_null_duplicates() {
    let db = WasmDatabase::new();
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .unwrap();

    let u1 = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Alice"})).unwrap();
    db.create("users".into(), u1).await.unwrap();

    let u2 = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Bob"})).unwrap();
    db.create("users".into(), u2).await.unwrap();
}

#[wasm_bindgen_test]
async fn test_unique_constraint_update_same_record() {
    let db = WasmDatabase::new();
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .unwrap();

    let u1 = serde_wasm_bindgen::to_value(&serde_json::json!({"email": "a@b.com"})).unwrap();
    let result = db.create("users".into(), u1).await.unwrap();
    let val: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    let id = val["id"].as_str().unwrap().to_string();

    let update = serde_wasm_bindgen::to_value(&serde_json::json!({"name": "Updated"})).unwrap();
    db.update("users".into(), id, update).await.unwrap();
}

#[wasm_bindgen_test]
fn test_set_null_cascade_sync_bumps_version() {
    let db = WasmDatabase::new();
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        "set_null",
    )
    .unwrap();

    let user =
        serde_wasm_bindgen::to_value(&serde_json::json!({"id": "u1", "name": "Alice"})).unwrap();
    db.create_sync("users".into(), user).unwrap();

    let post = serde_wasm_bindgen::to_value(
        &serde_json::json!({"id": "p1", "author_id": "u1", "title": "Hello"}),
    )
    .unwrap();
    db.create_sync("posts".into(), post).unwrap();

    db.delete_sync("users".into(), "u1".into()).unwrap();

    let result = db.read_sync("posts".into(), "p1".into()).unwrap();
    let val: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
    assert_eq!(val["author_id"], serde_json::Value::Null);
    assert_eq!(val["_version"], 2);
}

#[wasm_bindgen_test]
fn test_list_constraints_populates_fields() {
    let db = WasmDatabase::new();
    db.add_unique_constraint("users".into(), vec!["email".into()])
        .unwrap();
    db.add_not_null("users".into(), "name".into()).unwrap();
    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        "cascade",
    )
    .unwrap();

    let user_constraints: serde_json::Value =
        serde_wasm_bindgen::from_value(db.list_constraints("users")).unwrap();
    let arr = user_constraints.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let unique = arr.iter().find(|c| c["type"] == "unique").unwrap();
    assert_eq!(unique["fields"][0], "email");
    let not_null = arr.iter().find(|c| c["type"] == "not_null").unwrap();
    assert_eq!(not_null["field"], "name");

    let post_constraints: serde_json::Value =
        serde_wasm_bindgen::from_value(db.list_constraints("posts")).unwrap();
    let arr = post_constraints.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["type"], "foreign_key");
    assert_eq!(arr[0]["field"], "author_id");
    assert_eq!(arr[0]["target_entity"], "users");
    assert_eq!(arr[0]["target_field"], "id");
    assert_eq!(arr[0]["on_delete"], "cascade");
}

#[wasm_bindgen_test]
fn test_get_schema_preserves_default_values() {
    let db = WasmDatabase::new();
    let schema_js = json_to_js(&serde_json::json!({
        "fields": [
            {"name": "name", "type": "string", "required": true},
            {"name": "active", "type": "boolean", "default": true},
            {"name": "status", "type": "string", "default": "pending"}
        ]
    }));
    db.add_schema("users".into(), schema_js).unwrap();

    let schema: serde_json::Value =
        serde_wasm_bindgen::from_value(db.get_schema("users").unwrap()).unwrap();
    let fields = schema["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 3);

    let active = fields.iter().find(|f| f["name"] == "active").unwrap();
    assert_eq!(active["default"], serde_json::Value::Bool(true));

    let status = fields.iter().find(|f| f["name"] == "status").unwrap();
    assert_eq!(status["default"], "pending");
}
