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
