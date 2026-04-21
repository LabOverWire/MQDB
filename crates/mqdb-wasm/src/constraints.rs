// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    ChangeEvent, ForeignKeyEntry, JsValue, OnDeleteAction, Relationship, WasmDatabase, wasm_bindgen,
};
use crate::encoding::encode_value_for_index;
use crate::types::WasmBatch;
use mqdb_core::keys::{encode_constraint_key, encode_index_definition_key};

#[wasm_bindgen]
impl WasmDatabase {
    /// Adds a unique constraint on the specified fields.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_unique_constraint(
        &self,
        entity: String,
        fields: Vec<String>,
    ) -> Result<(), JsValue> {
        let mut inner = self.borrow_inner_mut()?;

        inner
            .indexes
            .entry(entity.clone())
            .or_default()
            .push(fields.clone());

        inner
            .unique_constraints
            .entry(entity)
            .or_default()
            .push(fields);

        Ok(())
    }

    /// Adds a NOT NULL constraint on the specified field.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_not_null(&self, entity: String, field: String) -> Result<(), JsValue> {
        let mut inner = self.borrow_inner_mut()?;
        inner
            .not_null_constraints
            .entry(entity)
            .or_default()
            .push(field);
        Ok(())
    }

    /// Adds a foreign key constraint.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: &str,
    ) -> Result<(), JsValue> {
        let on_delete = match on_delete {
            "cascade" => OnDeleteAction::Cascade,
            "set_null" => OnDeleteAction::SetNull,
            _ => OnDeleteAction::Restrict,
        };

        let mut inner = self.borrow_inner_mut()?;
        inner.foreign_keys.push(ForeignKeyEntry {
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
        });
        Ok(())
    }

    /// Adds a unique constraint and persists to storage.
    ///
    /// # Errors
    /// Returns an error if storage fails.
    pub async fn add_unique_constraint_async(
        &self,
        entity: String,
        fields: Vec<String>,
    ) -> Result<(), JsValue> {
        {
            let mut inner = self.borrow_inner_mut()?;
            inner
                .indexes
                .entry(entity.clone())
                .or_default()
                .push(fields.clone());
            inner
                .unique_constraints
                .entry(entity.clone())
                .or_default()
                .push(fields.clone());
        }

        if !self.storage.is_memory() {
            let name = fields.join("_");
            let key = encode_constraint_key("unique", &entity, &name);
            let bytes = serde_json::to_vec(&serde_json::json!({"fields": fields}))
                .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
            self.storage.insert(&key, &bytes).await?;
            self.persist_index_definitions(&entity).await?;
        }

        Ok(())
    }

    /// Adds a NOT NULL constraint and persists to storage.
    ///
    /// # Errors
    /// Returns an error if storage fails.
    pub async fn add_not_null_async(&self, entity: String, field: String) -> Result<(), JsValue> {
        {
            let mut inner = self.borrow_inner_mut()?;
            inner
                .not_null_constraints
                .entry(entity.clone())
                .or_default()
                .push(field.clone());
        }

        if !self.storage.is_memory() {
            let key = encode_constraint_key("not_null", &entity, &field);
            let bytes = serde_json::to_vec(&serde_json::json!({"field": field}))
                .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
            self.storage.insert(&key, &bytes).await?;
        }

        Ok(())
    }

    /// Adds a foreign key constraint and persists to storage.
    ///
    /// # Errors
    /// Returns an error if storage fails.
    pub async fn add_foreign_key_async(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: &str,
    ) -> Result<(), JsValue> {
        let on_delete_action = match on_delete {
            "cascade" => OnDeleteAction::Cascade,
            "set_null" => OnDeleteAction::SetNull,
            _ => OnDeleteAction::Restrict,
        };

        {
            let mut inner = self.borrow_inner_mut()?;
            inner.foreign_keys.push(ForeignKeyEntry {
                source_entity: source_entity.clone(),
                source_field: source_field.clone(),
                target_entity: target_entity.clone(),
                target_field: target_field.clone(),
                on_delete: on_delete_action,
            });
        }

        if !self.storage.is_memory() {
            let key = encode_constraint_key("foreign_key", &source_entity, &source_field);
            let bytes = serde_json::to_vec(&serde_json::json!({
                "source_entity": source_entity,
                "source_field": source_field,
                "target_entity": target_entity,
                "target_field": target_field,
                "on_delete": on_delete
            }))
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
            self.storage.insert(&key, &bytes).await?;
        }

        Ok(())
    }

    /// Adds an index on the specified fields and backfills from existing records (async).
    ///
    /// # Errors
    /// Returns an error if the storage operation fails during backfill.
    pub async fn add_index_async(
        &self,
        entity: String,
        fields: Vec<String>,
    ) -> Result<(), JsValue> {
        {
            let mut inner = self.borrow_inner_mut()?;
            inner
                .indexes
                .entry(entity.clone())
                .or_default()
                .push(fields.clone());
        }

        if !self.storage.is_memory() {
            self.persist_index_definitions(&entity).await?;
        }

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
        for (key, value) in items {
            let id = Self::extract_id_from_data_key(&key);
            let Some(id) = id else { continue };
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if let Some(index_key) = Self::build_index_key(&entity, &id, &fields, &parsed)? {
                self.storage.insert(&index_key, &[]).await?;
            }
        }
        Ok(())
    }

    /// Adds a relationship definition and persists to storage.
    ///
    /// # Errors
    /// Returns an error if storage fails.
    pub async fn add_relationship_async(
        &self,
        source_entity: String,
        field: String,
        target_entity: String,
    ) -> Result<(), JsValue> {
        let field_suffix = format!("{field}_id");

        {
            let mut inner = self.borrow_inner_mut()?;
            inner
                .relationships
                .entry(source_entity.clone())
                .or_default()
                .push(Relationship {
                    field: field.clone(),
                    target_entity: target_entity.clone(),
                    field_suffix: field_suffix.clone(),
                });
        }

        if !self.storage.is_memory() {
            let key = format!("meta/relationship/{source_entity}/{field}");
            let bytes = serde_json::to_vec(&serde_json::json!({
                "target_entity": target_entity,
                "field_suffix": field_suffix
            }))
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
            self.storage.insert(key.as_bytes(), &bytes).await?;
        }

        Ok(())
    }

    /// Adds an index on the specified fields and backfills from existing records (sync, memory only).
    ///
    /// # Errors
    /// Returns an error if the backend is not memory-based or backfill fails.
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_index(&self, entity: String, fields: Vec<String>) -> Result<(), JsValue> {
        {
            let mut inner = self.borrow_inner_mut()?;
            inner
                .indexes
                .entry(entity.clone())
                .or_default()
                .push(fields.clone());
        }

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
        for (key, value) in items {
            let id = Self::extract_id_from_data_key(&key);
            let Some(id) = id else { continue };
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if let Some(index_key) = Self::build_index_key(&entity, &id, &fields, &parsed)? {
                self.storage.insert_sync(&index_key, &[])?;
            }
        }
        Ok(())
    }

    /// Adds a relationship definition for eager loading.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_relationship(
        &self,
        source_entity: String,
        field: String,
        target_entity: String,
    ) -> Result<(), JsValue> {
        let field_suffix = format!("{field}_id");
        let mut inner = self.borrow_inner_mut()?;
        inner
            .relationships
            .entry(source_entity)
            .or_default()
            .push(Relationship {
                field,
                target_entity,
                field_suffix,
            });
        Ok(())
    }

    #[must_use]
    pub fn list_relationships(&self, entity: &str) -> JsValue {
        let inner = match self.borrow_inner() {
            Ok(inner) => inner,
            Err(e) => return e,
        };
        let relationships: Vec<serde_json::Value> = inner
            .relationships
            .get(entity)
            .map(|rels| {
                rels.iter()
                    .map(|r| {
                        serde_json::json!({
                            "field": r.field,
                            "target_entity": r.target_entity,
                            "field_suffix": r.field_suffix
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let json_str = serde_json::to_string(&relationships).unwrap_or_else(|_| "[]".to_string());
        js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
    }

    #[must_use]
    pub fn list_constraints(&self, entity: &str) -> JsValue {
        let inner = match self.borrow_inner() {
            Ok(inner) => inner,
            Err(e) => return e,
        };
        let mut constraints = Vec::new();

        if let Some(uniques) = inner.unique_constraints.get(entity) {
            for fields in uniques {
                constraints.push(serde_json::json!({
                    "type": "unique",
                    "fields": fields
                }));
            }
        }

        if let Some(not_nulls) = inner.not_null_constraints.get(entity) {
            for field in not_nulls {
                constraints.push(serde_json::json!({
                    "type": "not_null",
                    "field": field
                }));
            }
        }

        for fk in &inner.foreign_keys {
            if fk.source_entity.as_str() == entity {
                constraints.push(serde_json::json!({
                    "type": "foreign_key",
                    "field": fk.source_field,
                    "target_entity": fk.target_entity,
                    "target_field": fk.target_field,
                    "on_delete": format!("{:?}", fk.on_delete).to_lowercase()
                }));
            }
        }

        serde_wasm_bindgen::to_value(&constraints).unwrap_or(JsValue::NULL)
    }
}

impl WasmDatabase {
    pub(crate) fn validate_not_null_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let inner = self.borrow_inner()?;
        if let Some(fields) = inner.not_null_constraints.get(entity) {
            for field in fields {
                let field_value = value.get(field);
                if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                    return Err(JsValue::from_str(&format!(
                        "not null constraint violation: {entity}.{field}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_unique_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
        current_id: Option<&str>,
    ) -> Result<(), JsValue> {
        let constraints: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.unique_constraints.get(entity).cloned()
        };

        if let Some(constraints) = constraints {
            for constraint_fields in constraints {
                if constraint_fields.iter().any(|f| {
                    value.get(f).is_none() || value.get(f) == Some(&serde_json::Value::Null)
                }) {
                    continue;
                }

                let index_prefix =
                    Self::build_unique_index_prefix(entity, &constraint_fields, value)?;
                let matches = self.storage.prefix_scan(&index_prefix).await?;

                for (key, _) in &matches {
                    let hit_id = Self::extract_id_from_index_key(key);
                    if hit_id.as_deref() == current_id {
                        continue;
                    }
                    return Err(JsValue::from_str(&format!(
                        "unique constraint violation: {entity}.{}",
                        constraint_fields.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_foreign_keys_async(
        &self,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.borrow_inner()?;
            inner.foreign_keys.clone()
        };

        for fk in &fks {
            if fk.source_entity != entity {
                continue;
            }

            let field_value = value.get(&fk.source_field);
            if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                continue;
            }

            let target_id = field_value
                .and_then(|v| v.as_str())
                .ok_or_else(|| JsValue::from_str("foreign key must be a string"))?;

            let target_key = format!("data/{}/{}", fk.target_entity, target_id);
            if self.storage.get(target_key.as_bytes()).await?.is_none() {
                return Err(JsValue::from_str(&format!(
                    "foreign key violation: {entity}.{} references non-existent {}/{}",
                    fk.source_field, fk.target_entity, target_id
                )));
            }
        }
        Ok(())
    }

    pub(crate) async fn check_foreign_key_constraints_async(
        &self,
        entity: &str,
        id: &str,
    ) -> Result<Vec<(String, String)>, JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.borrow_inner()?;
            inner.foreign_keys.clone()
        };

        let mut cascade_deletes = Vec::new();

        for fk in &fks {
            if fk.target_entity != entity {
                continue;
            }

            let prefix = format!("data/{}/", fk.source_entity);
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

            for (_key, data) in items {
                let value: serde_json::Value =
                    serde_json::from_slice(&data).map_err(|e| JsValue::from_str(&e.to_string()))?;

                if value.get(&fk.source_field).and_then(|v| v.as_str()) == Some(id) {
                    match fk.on_delete {
                        OnDeleteAction::Restrict => {
                            return Err(JsValue::from_str(&format!(
                                "foreign key restrict: cannot delete {entity}/{id} - referenced by {}",
                                fk.source_entity
                            )));
                        }
                        OnDeleteAction::Cascade => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                cascade_deletes
                                    .push((fk.source_entity.clone(), source_id.to_string()));
                            }
                        }
                        OnDeleteAction::SetNull => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                let data_key = format!("data/{}/{}", fk.source_entity, source_id);
                                let existing_raw = self
                                    .storage
                                    .get_raw(data_key.as_bytes())
                                    .await?
                                    .ok_or_else(|| {
                                        JsValue::from_str("set_null: record disappeared")
                                    })?;

                                let mut updated = value.clone();
                                let old_version = updated
                                    .get("_version")
                                    .and_then(serde_json::Value::as_u64)
                                    .unwrap_or(0);
                                if let Some(obj) = updated.as_object_mut() {
                                    obj.insert(fk.source_field.clone(), serde_json::Value::Null);
                                    obj.insert(
                                        "_version".to_string(),
                                        serde_json::Value::Number((old_version + 1).into()),
                                    );
                                }

                                let serialized = serde_json::to_vec(&updated)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                                let stored = self
                                    .storage
                                    .encrypt_if_needed(data_key.as_bytes(), &serialized)
                                    .await?;

                                let mut batch = self.storage.batch();
                                batch.expect_value(data_key.as_bytes().to_vec(), existing_raw);
                                batch.insert(data_key.as_bytes().to_vec(), stored);
                                self.update_indexes_batch(
                                    &mut batch,
                                    &fk.source_entity,
                                    source_id,
                                    &updated,
                                    Some(&value),
                                )?;
                                batch.commit().await?;

                                let event = ChangeEvent::update(
                                    fk.source_entity.clone(),
                                    source_id.to_string(),
                                    updated,
                                );
                                self.dispatch_event(&event);
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    pub(crate) fn validate_unique_sync(
        &self,
        entity: &str,
        value: &serde_json::Value,
        current_id: Option<&str>,
    ) -> Result<(), JsValue> {
        let constraints: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.unique_constraints.get(entity).cloned()
        };

        if let Some(constraints) = constraints {
            for constraint_fields in constraints {
                if constraint_fields.iter().any(|f| {
                    value.get(f).is_none() || value.get(f) == Some(&serde_json::Value::Null)
                }) {
                    continue;
                }

                let index_prefix =
                    Self::build_unique_index_prefix(entity, &constraint_fields, value)?;
                let matches = self.storage.prefix_scan_sync(&index_prefix)?;

                for (key, _) in &matches {
                    let hit_id = Self::extract_id_from_index_key(key);
                    if hit_id.as_deref() == current_id {
                        continue;
                    }
                    return Err(JsValue::from_str(&format!(
                        "unique constraint violation: {entity}.{}",
                        constraint_fields.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_foreign_keys_sync(
        &self,
        entity: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.borrow_inner()?;
            inner.foreign_keys.clone()
        };

        for fk in &fks {
            if fk.source_entity != entity {
                continue;
            }

            let field_value = value.get(&fk.source_field);
            if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                continue;
            }

            let target_id = field_value
                .and_then(|v| v.as_str())
                .ok_or_else(|| JsValue::from_str("foreign key must be a string"))?;

            let target_key = format!("data/{}/{}", fk.target_entity, target_id);
            if self.storage.get_sync(target_key.as_bytes())?.is_none() {
                return Err(JsValue::from_str(&format!(
                    "foreign key violation: {entity}.{} references non-existent {}/{}",
                    fk.source_field, fk.target_entity, target_id
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn check_foreign_key_constraints_sync(
        &self,
        entity: &str,
        id: &str,
    ) -> Result<Vec<(String, String)>, JsValue> {
        let fks: Vec<ForeignKeyEntry> = {
            let inner = self.borrow_inner()?;
            inner.foreign_keys.clone()
        };

        let mut cascade_deletes = Vec::new();

        for fk in &fks {
            if fk.target_entity != entity {
                continue;
            }

            let prefix = format!("data/{}/", fk.source_entity);
            let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;

            for (_key, data) in items {
                let value: serde_json::Value =
                    serde_json::from_slice(&data).map_err(|e| JsValue::from_str(&e.to_string()))?;

                if value.get(&fk.source_field).and_then(|v| v.as_str()) == Some(id) {
                    match fk.on_delete {
                        OnDeleteAction::Restrict => {
                            return Err(JsValue::from_str(&format!(
                                "foreign key restrict: cannot delete {entity}/{id} - referenced by {}",
                                fk.source_entity
                            )));
                        }
                        OnDeleteAction::Cascade => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                cascade_deletes
                                    .push((fk.source_entity.clone(), source_id.to_string()));
                            }
                        }
                        OnDeleteAction::SetNull => {
                            if let Some(source_id) = value.get("id").and_then(|v| v.as_str()) {
                                let data_key = format!("data/{}/{}", fk.source_entity, source_id);
                                let existing_raw =
                                    self.storage.get_raw_sync(data_key.as_bytes())?.ok_or_else(
                                        || JsValue::from_str("set_null: record disappeared"),
                                    )?;

                                let mut updated = value.clone();
                                let old_version = updated
                                    .get("_version")
                                    .and_then(serde_json::Value::as_u64)
                                    .unwrap_or(0);
                                if let Some(obj) = updated.as_object_mut() {
                                    obj.insert(fk.source_field.clone(), serde_json::Value::Null);
                                    obj.insert(
                                        "_version".to_string(),
                                        serde_json::Value::Number((old_version + 1).into()),
                                    );
                                }

                                let serialized = serde_json::to_vec(&updated)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;

                                let mut batch = self.storage.batch();
                                batch.expect_value(data_key.as_bytes().to_vec(), existing_raw);
                                batch.insert(data_key.as_bytes().to_vec(), serialized);
                                self.update_indexes_batch(
                                    &mut batch,
                                    &fk.source_entity,
                                    source_id,
                                    &updated,
                                    Some(&value),
                                )?;
                                batch.commit_sync()?;

                                let event = ChangeEvent::update(
                                    fk.source_entity.clone(),
                                    source_id.to_string(),
                                    updated,
                                );
                                self.dispatch_event(&event);
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    pub(crate) fn update_indexes_batch(
        &self,
        batch: &mut WasmBatch,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
        old_value: Option<&serde_json::Value>,
    ) -> Result<(), JsValue> {
        if let Some(old) = old_value {
            self.remove_indexes_batch(batch, entity, id, old)?;
        }

        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                if let Some(key) = Self::build_index_key(entity, id, &fields, value)? {
                    batch.insert(key, vec![]);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn remove_indexes_batch(
        &self,
        batch: &mut WasmBatch,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
    ) -> Result<(), JsValue> {
        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                if let Some(key) = Self::build_index_key(entity, id, &fields, value)? {
                    batch.remove(key);
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn persist_index_definitions(&self, entity: &str) -> Result<(), JsValue> {
        let all_indexes: Vec<Vec<String>> = {
            let inner = self.borrow_inner()?;
            inner.indexes.get(entity).cloned().unwrap_or_default()
        };
        let key = encode_index_definition_key(entity);
        let bytes = serde_json::to_vec(&all_indexes)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        self.storage.insert(&key, &bytes).await
    }

    fn build_unique_index_prefix(
        entity: &str,
        fields: &[String],
        value: &serde_json::Value,
    ) -> Result<Vec<u8>, JsValue> {
        let mut encoded_values = Vec::new();
        for field in fields {
            let v = value
                .get(field)
                .ok_or_else(|| JsValue::from_str("missing field for unique check"))?;
            let encoded = encode_value_for_index(v)?;
            if !encoded_values.is_empty() {
                encoded_values.push(0x00);
            }
            encoded_values.extend_from_slice(&encoded);
        }

        let prefix_str = format!("index/{entity}/{}/", fields.join("_"));
        let mut prefix = Vec::with_capacity(prefix_str.len() + encoded_values.len() + 1);
        prefix.extend_from_slice(prefix_str.as_bytes());
        prefix.extend_from_slice(&encoded_values);
        prefix.push(b'/');
        Ok(prefix)
    }

    fn extract_id_from_index_key(key: &[u8]) -> Option<String> {
        let key_str = std::str::from_utf8(key).ok()?;
        let last_slash = key_str.rfind('/')?;
        Some(key_str[last_slash + 1..].to_string())
    }

    fn extract_id_from_data_key(key: &[u8]) -> Option<String> {
        let key_str = std::str::from_utf8(key).ok()?;
        let rest = key_str.strip_prefix("data/")?;
        let slash_pos = rest.find('/')?;
        Some(rest[slash_pos + 1..].to_string())
    }

    fn build_index_key(
        entity: &str,
        id: &str,
        fields: &[String],
        value: &serde_json::Value,
    ) -> Result<Option<Vec<u8>>, JsValue> {
        let mut encoded_values = Vec::new();
        for field in fields {
            let Some(v) = value.get(field) else {
                return Ok(None);
            };
            let encoded = encode_value_for_index(v)?;
            if !encoded_values.is_empty() {
                encoded_values.push(0x00);
            }
            encoded_values.extend_from_slice(&encoded);
        }

        let prefix = format!("index/{entity}/{}/", fields.join("_"));
        let mut key = Vec::with_capacity(prefix.len() + encoded_values.len() + 1 + id.len());
        key.extend_from_slice(prefix.as_bytes());
        key.extend_from_slice(&encoded_values);
        key.push(b'/');
        key.extend_from_slice(id.as_bytes());
        Ok(Some(key))
    }
}
