// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    CountOptions, FilterJs, JsValue, ListOptions, SortOrderJs, WasmDatabase, wasm_bindgen,
};

#[wasm_bindgen]
impl WasmDatabase {
    /// Lists records from an entity with optional filtering, sorting, and pagination.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn list(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let mut results = if let Some((records, remaining)) =
            self.index_scan_async(&entity, &opts.filters).await?
        {
            let mut filtered = Vec::new();
            for record in records {
                if remaining.iter().all(|f| Self::matches_filter(&record, f)) {
                    filtered.push(record);
                }
            }
            filtered
        } else {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

            let mut filtered = Vec::new();
            for (_key, value) in items {
                let parsed: serde_json::Value = serde_json::from_slice(&value)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

                if opts
                    .filters
                    .iter()
                    .all(|f| Self::matches_filter(&parsed, f))
                {
                    filtered.push(parsed);
                }
            }
            filtered
        };

        if !opts.sort.is_empty() {
            Self::sort_results(&mut results, &opts.sort);
        }

        if let Some(pagination) = &opts.pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            results = results.into_iter().skip(offset).take(limit).collect();
        }

        if let Some(ref includes) = opts.includes {
            for result in &mut results {
                self.load_includes_async(&entity, result, includes, 0)
                    .await?;
            }
        }

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        let json_str = serde_json::to_string(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        js_sys::JSON::parse(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
    }

    /// # Errors
    /// Returns an error if options are invalid or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    pub fn list_sync(&self, entity: String, options: JsValue) -> Result<JsValue, JsValue> {
        let opts: ListOptions = if options.is_null() || options.is_undefined() {
            ListOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let mut results =
            if let Some((records, remaining)) = self.index_scan_sync(&entity, &opts.filters)? {
                let mut filtered = Vec::new();
                for record in records {
                    if remaining.iter().all(|f| Self::matches_filter(&record, f)) {
                        filtered.push(record);
                    }
                }
                filtered
            } else {
                let prefix = format!("data/{entity}/");
                let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;

                let mut filtered = Vec::new();
                for (_key, value) in items {
                    let parsed: serde_json::Value = serde_json::from_slice(&value)
                        .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

                    if opts
                        .filters
                        .iter()
                        .all(|f| Self::matches_filter(&parsed, f))
                    {
                        filtered.push(parsed);
                    }
                }
                filtered
            };

        if !opts.sort.is_empty() {
            Self::sort_results(&mut results, &opts.sort);
        }

        if let Some(pagination) = &opts.pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            results = results.into_iter().skip(offset).take(limit).collect();
        }

        if let Some(ref projection) = opts.projection {
            results = results
                .into_iter()
                .map(|v| Self::project_fields(v, projection))
                .collect();
        }

        let json_str = serde_json::to_string(&results)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {e}")))?;
        js_sys::JSON::parse(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e:?}")))
    }

    /// Counts records matching optional filters, using indexes when available.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn count(&self, entity: String, options: JsValue) -> Result<usize, JsValue> {
        let opts: CountOptions = if options.is_null() || options.is_undefined() {
            CountOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        if opts.filters.is_empty() {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
            return Ok(items.len());
        }

        if let Some((records, remaining)) = self.index_scan_async(&entity, &opts.filters).await? {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            let count = records
                .iter()
                .filter(|r| remaining.iter().all(|f| Self::matches_filter(r, f)))
                .count();
            return Ok(count);
        }

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
        let mut count = 0usize;
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// # Errors
    /// Returns an error if options are invalid or the backend is not memory-based.
    #[allow(clippy::needless_pass_by_value)]
    pub fn count_sync(&self, entity: String, options: JsValue) -> Result<usize, JsValue> {
        let opts: CountOptions = if options.is_null() || options.is_undefined() {
            CountOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        if opts.filters.is_empty() {
            let prefix = format!("data/{entity}/");
            let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
            return Ok(items.len());
        }

        if let Some((records, remaining)) = self.index_scan_sync(&entity, &opts.filters)? {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            let count = records
                .iter()
                .filter(|r| remaining.iter().all(|f| Self::matches_filter(r, f)))
                .count();
            return Ok(count);
        }

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
        let mut count = 0usize;
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                count += 1;
            }
        }
        Ok(count)
    }
}

impl WasmDatabase {
    fn is_equality_op(op: &str) -> bool {
        matches!(op, "" | "eq")
    }

    fn filter_index_value(filter: &FilterJs) -> String {
        match &filter.value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    fn find_single_field_index<'a>(
        &self,
        entity: &str,
        filters: &'a [FilterJs],
    ) -> Result<Option<(usize, &'a FilterJs)>, JsValue> {
        let inner = self.borrow_inner()?;
        let Some(index_defs) = inner.indexes.get(entity) else {
            return Ok(None);
        };

        for (i, filter) in filters.iter().enumerate() {
            if !Self::is_equality_op(&filter.op) {
                continue;
            }
            let target = vec![filter.field.clone()];
            if index_defs.contains(&target) {
                return Ok(Some((i, filter)));
            }
        }
        Ok(None)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn index_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Option<(Vec<serde_json::Value>, Vec<FilterJs>)>, JsValue> {
        let matched = self.find_single_field_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let value_str = Self::filter_index_value(matched_filter);
        let index_prefix = format!("index/{entity}/{}/{value_str}/", matched_filter.field);
        let index_entries = self.storage.prefix_scan(index_prefix.as_bytes()).await?;

        let mut records = Vec::with_capacity(index_entries.len());
        for (key, _) in &index_entries {
            let key_str = std::str::from_utf8(key)
                .map_err(|e| JsValue::from_str(&format!("invalid index key: {e}")))?;
            let id = key_str
                .rsplit('/')
                .next()
                .ok_or_else(|| JsValue::from_str("malformed index key"))?;
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get(data_key.as_bytes()).await? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != matched_idx)
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn index_scan_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Option<(Vec<serde_json::Value>, Vec<FilterJs>)>, JsValue> {
        let matched = self.find_single_field_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let value_str = Self::filter_index_value(matched_filter);
        let index_prefix = format!("index/{entity}/{}/{value_str}/", matched_filter.field);
        let index_entries = self.storage.prefix_scan_sync(index_prefix.as_bytes())?;

        let mut records = Vec::with_capacity(index_entries.len());
        for (key, _) in &index_entries {
            let key_str = std::str::from_utf8(key)
                .map_err(|e| JsValue::from_str(&format!("invalid index key: {e}")))?;
            let id = key_str
                .rsplit('/')
                .next()
                .ok_or_else(|| JsValue::from_str("malformed index key"))?;
            let data_key = format!("data/{entity}/{id}");
            if let Some(data) = self.storage.get_sync(data_key.as_bytes())? {
                let parsed: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
                records.push(parsed);
            }
        }

        let remaining: Vec<FilterJs> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != matched_idx)
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    pub(crate) fn matches_filter(value: &serde_json::Value, filter: &FilterJs) -> bool {
        let field_value = value.get(&filter.field);

        match filter.op.as_str() {
            "ne" | "<>" => field_value != Some(&filter.value),
            "gt" | ">" => {
                Self::compare_values(field_value, &filter.value)
                    == Some(std::cmp::Ordering::Greater)
            }
            "lt" | "<" => {
                Self::compare_values(field_value, &filter.value) == Some(std::cmp::Ordering::Less)
            }
            "gte" | ">=" => Self::compare_values(field_value, &filter.value)
                .is_some_and(|ord| ord != std::cmp::Ordering::Less),
            "lte" | "<=" => Self::compare_values(field_value, &filter.value)
                .is_some_and(|ord| ord != std::cmp::Ordering::Greater),
            "glob" | "~" => {
                if let (Some(serde_json::Value::String(s)), serde_json::Value::String(pattern)) =
                    (field_value, &filter.value)
                {
                    Self::glob_match(s, pattern)
                } else {
                    false
                }
            }
            "null" | "?" => field_value.is_none() || field_value == Some(&serde_json::Value::Null),
            "not_null" | "!?" => {
                field_value.is_some() && field_value != Some(&serde_json::Value::Null)
            }
            _ => field_value == Some(&filter.value),
        }
    }

    fn compare_values(
        a: Option<&serde_json::Value>,
        b: &serde_json::Value,
    ) -> Option<std::cmp::Ordering> {
        let a = a?;
        match (a, b) {
            (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
                let a_f = a_num.as_f64()?;
                let b_f = b_num.as_f64()?;
                a_f.partial_cmp(&b_f)
            }
            (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => {
                Some(a_str.cmp(b_str))
            }
            _ => None,
        }
    }

    fn glob_match(text: &str, pattern: &str) -> bool {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 1 {
            return text == pattern;
        }

        let mut pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            if let Some(found) = text[pos..].find(part) {
                if i == 0 && found != 0 {
                    return false;
                }
                pos += found + part.len();
            } else {
                return false;
            }
        }

        parts
            .last()
            .is_none_or(|last| last.is_empty() || text.ends_with(last))
    }

    pub(crate) fn sort_results(results: &mut [serde_json::Value], sort: &[SortOrderJs]) {
        results.sort_by(|a, b| {
            for order in sort {
                let a_val = a.get(&order.field);
                let b_val = b.get(&order.field);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => Self::compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = if order.direction == "desc" {
                    cmp.reverse()
                } else {
                    cmp
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        match (a, b) {
            (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => {
                a_str.cmp(b_str)
            }
            (serde_json::Value::Bool(a_bool), serde_json::Value::Bool(b_bool)) => {
                a_bool.cmp(b_bool)
            }
            _ => std::cmp::Ordering::Equal,
        }
    }

    pub(crate) fn project_fields(value: serde_json::Value, fields: &[String]) -> serde_json::Value {
        if let serde_json::Value::Object(obj) = value {
            let mut projected = serde_json::Map::new();

            if let Some(id) = obj.get("id") {
                projected.insert("id".to_string(), id.clone());
            }

            for field in fields {
                if field != "id"
                    && let Some(v) = obj.get(field)
                {
                    projected.insert(field.clone(), v.clone());
                }
            }

            serde_json::Value::Object(projected)
        } else {
            value
        }
    }
}
