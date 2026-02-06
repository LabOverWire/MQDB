use super::{ForeignKeyEntry, JsValue, OnDeleteAction, Relationship, WasmDatabase, wasm_bindgen};

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

    /// Adds an index on the specified fields.
    ///
    /// # Errors
    /// This function does not currently return errors but the signature allows for future validation.
    pub fn add_index(&self, entity: String, fields: Vec<String>) -> Result<(), JsValue> {
        let mut inner = self.borrow_inner_mut()?;
        inner.indexes.entry(entity).or_default().push(fields);
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
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

            for constraint_fields in constraints {
                let new_values: Vec<Option<&serde_json::Value>> =
                    constraint_fields.iter().map(|f| value.get(f)).collect();

                for (_key, existing_data) in &items {
                    let existing: serde_json::Value = serde_json::from_slice(existing_data)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;

                    if let Some(existing_id) = existing.get("id").and_then(|v| v.as_str())
                        && current_id == Some(existing_id)
                    {
                        continue;
                    }

                    let existing_values: Vec<Option<&serde_json::Value>> =
                        constraint_fields.iter().map(|f| existing.get(f)).collect();

                    if new_values == existing_values
                        && new_values
                            .iter()
                            .all(|v| v.is_some() && *v != Some(&serde_json::Value::Null))
                    {
                        return Err(JsValue::from_str(&format!(
                            "unique constraint violation: {entity}.{}",
                            constraint_fields.join(", ")
                        )));
                    }
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
                                let mut updated = value.clone();
                                if let Some(obj) = updated.as_object_mut() {
                                    obj.insert(fk.source_field.clone(), serde_json::Value::Null);
                                }
                                let key = format!("data/{}/{}", fk.source_entity, source_id);
                                let serialized = serde_json::to_vec(&updated)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                                self.storage.insert(key.as_bytes(), &serialized).await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    pub(crate) async fn update_indexes_async(
        &self,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
        old_value: Option<&serde_json::Value>,
    ) -> Result<(), JsValue> {
        if let Some(old) = old_value {
            self.remove_indexes_async(entity, id, old).await?;
        }

        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    self.storage.insert(index_key.as_bytes(), &[]).await?;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn remove_indexes_async(
        &self,
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
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    let _ = self.storage.remove(index_key.as_bytes()).await;
                }
            }
        }

        Ok(())
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
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;

            for constraint_fields in constraints {
                let new_values: Vec<Option<&serde_json::Value>> =
                    constraint_fields.iter().map(|f| value.get(f)).collect();

                for (_key, existing_data) in &items {
                    let existing: serde_json::Value = serde_json::from_slice(existing_data)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;

                    if let Some(existing_id) = existing.get("id").and_then(|v| v.as_str())
                        && current_id == Some(existing_id)
                    {
                        continue;
                    }

                    let existing_values: Vec<Option<&serde_json::Value>> =
                        constraint_fields.iter().map(|f| existing.get(f)).collect();

                    if new_values == existing_values
                        && new_values
                            .iter()
                            .all(|v| v.is_some() && *v != Some(&serde_json::Value::Null))
                    {
                        return Err(JsValue::from_str(&format!(
                            "unique constraint violation: {entity}.{}",
                            constraint_fields.join(", ")
                        )));
                    }
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
                                let mut updated = value.clone();
                                if let Some(obj) = updated.as_object_mut() {
                                    obj.insert(fk.source_field.clone(), serde_json::Value::Null);
                                }
                                let key = format!("data/{}/{}", fk.source_entity, source_id);
                                let serialized = serde_json::to_vec(&updated)
                                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                                self.storage.insert_sync(key.as_bytes(), &serialized)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(cascade_deletes)
    }

    pub(crate) fn update_indexes_sync(
        &self,
        entity: &str,
        id: &str,
        value: &serde_json::Value,
        old_value: Option<&serde_json::Value>,
    ) -> Result<(), JsValue> {
        if let Some(old) = old_value {
            self.remove_indexes_sync(entity, id, old)?;
        }

        let index_defs: Option<Vec<Vec<String>>> = {
            let inner = self.borrow_inner()?;
            inner.indexes.get(entity).cloned()
        };

        if let Some(index_defs) = index_defs {
            for fields in index_defs {
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    self.storage.insert_sync(index_key.as_bytes(), &[])?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn remove_indexes_sync(
        &self,
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
                let index_values: Vec<String> = fields
                    .iter()
                    .filter_map(|f| {
                        value.get(f).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                    })
                    .collect();

                if index_values.len() == fields.len() {
                    let index_key = format!(
                        "index/{entity}/{}/{}/{}",
                        fields.join("_"),
                        index_values.join("_"),
                        id
                    );
                    let _ = self.storage.remove_sync(index_key.as_bytes());
                }
            }
        }

        Ok(())
    }
}
