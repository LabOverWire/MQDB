// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    CountOptions, FilterJs, JsValue, ListOptions, SortOrderJs, WasmDatabase, wasm_bindgen,
};
use crate::encoding::encode_value_for_index;

type ScanResult = Option<(Vec<serde_json::Value>, Vec<FilterJs>)>;
type RangeBound = Option<(Vec<u8>, bool)>;
type RangeBounds = (RangeBound, RangeBound, Vec<usize>);

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
            self.try_index_scans_async(&entity, &opts.filters).await?
        {
            Self::apply_remaining_filters(records, &remaining)
        } else {
            self.full_scan_async(&entity, &opts.filters).await?
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

        let mut results = if let Some((records, remaining)) =
            self.try_index_scans_sync(&entity, &opts.filters)?
        {
            Self::apply_remaining_filters(records, &remaining)
        } else {
            self.full_scan_sync(&entity, &opts.filters)?
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

        if let Some((records, remaining)) =
            self.try_index_scans_async(&entity, &opts.filters).await?
        {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            return Ok(Self::apply_remaining_filters(records, &remaining).len());
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

        if let Some((records, remaining)) = self.try_index_scans_sync(&entity, &opts.filters)? {
            if remaining.is_empty() {
                return Ok(records.len());
            }
            return Ok(Self::apply_remaining_filters(records, &remaining).len());
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

    fn is_range_op(op: &str) -> bool {
        matches!(op, "gt" | ">" | "gte" | ">=" | "lt" | "<" | "lte" | "<=")
    }

    pub(crate) fn apply_remaining_filters(
        records: Vec<serde_json::Value>,
        remaining: &[FilterJs],
    ) -> Vec<serde_json::Value> {
        records
            .into_iter()
            .filter(|r| remaining.iter().all(|f| Self::matches_filter(r, f)))
            .collect()
    }

    pub(crate) async fn full_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Vec<serde_json::Value>, JsValue> {
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;
        let mut filtered = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if filters.iter().all(|f| Self::matches_filter(&parsed, f)) {
                filtered.push(parsed);
            }
        }
        Ok(filtered)
    }

    fn full_scan_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Vec<serde_json::Value>, JsValue> {
        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan_sync(prefix.as_bytes())?;
        let mut filtered = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;
            if filters.iter().all(|f| Self::matches_filter(&parsed, f)) {
                filtered.push(parsed);
            }
        }
        Ok(filtered)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn try_index_scans_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        if let Some(result) = self.equality_scan_async(entity, filters).await? {
            return Ok(Some(result));
        }
        if let Some(result) = self.range_scan_async(entity, filters).await? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn try_index_scans_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        if let Some(result) = self.equality_scan_sync(entity, filters)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.range_scan_sync(entity, filters)? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    fn find_equality_index<'a>(
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

    fn find_range_field(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<Option<String>, JsValue> {
        let inner = self.borrow_inner()?;
        let Some(index_defs) = inner.indexes.get(entity) else {
            return Ok(None);
        };

        for filter in filters {
            if !Self::is_range_op(&filter.op) {
                continue;
            }
            let target = vec![filter.field.clone()];
            if index_defs.contains(&target) {
                return Ok(Some(filter.field.clone()));
            }
        }
        Ok(None)
    }

    fn collect_range_bounds(filters: &[FilterJs], field: &str) -> Result<RangeBounds, JsValue> {
        let mut lower: Option<(Vec<u8>, bool)> = None;
        let mut upper: Option<(Vec<u8>, bool)> = None;
        let mut consumed_indices = Vec::new();

        for (i, filter) in filters.iter().enumerate() {
            if filter.field != field {
                continue;
            }
            match filter.op.as_str() {
                "gt" | ">" => {
                    lower = Some((encode_value_for_index(&filter.value)?, false));
                    consumed_indices.push(i);
                }
                "gte" | ">=" => {
                    lower = Some((encode_value_for_index(&filter.value)?, true));
                    consumed_indices.push(i);
                }
                "lt" | "<" => {
                    upper = Some((encode_value_for_index(&filter.value)?, false));
                    consumed_indices.push(i);
                }
                "lte" | "<=" => {
                    upper = Some((encode_value_for_index(&filter.value)?, true));
                    consumed_indices.push(i);
                }
                _ => {}
            }
        }

        Ok((lower, upper, consumed_indices))
    }

    fn build_range_keys(
        entity: &str,
        field: &str,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
    ) -> (Vec<u8>, Vec<u8>) {
        let field_prefix = format!("index/{entity}/{field}");
        let field_prefix_bytes = field_prefix.as_bytes();

        let start = if let Some((value, inclusive)) = lower {
            let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1 + value.len() + 2);
            key.extend_from_slice(field_prefix_bytes);
            key.push(b'/');
            key.extend_from_slice(value);
            key.push(b'/');
            if !inclusive {
                key.push(0xFF);
            }
            key
        } else {
            let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1);
            key.extend_from_slice(field_prefix_bytes);
            key.push(b'/');
            key
        };

        let end = if let Some((value, inclusive)) = upper {
            let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1 + value.len() + 2);
            key.extend_from_slice(field_prefix_bytes);
            key.push(b'/');
            key.extend_from_slice(value);
            key.push(b'/');
            if inclusive {
                key.push(0xFF);
            }
            key
        } else {
            let mut key = Vec::with_capacity(field_prefix_bytes.len() + 1);
            key.extend_from_slice(field_prefix_bytes);
            key.push(0xFF);
            key
        };

        (start, end)
    }

    fn extract_ids_from_index_keys(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
        let mut ids = Vec::new();
        for (key, _) in entries {
            if let Some(id_start) = key.iter().rposition(|&b| b == b'/')
                && let Ok(id) = String::from_utf8(key[id_start + 1..].to_vec())
            {
                ids.push(id);
            }
        }
        ids
    }

    async fn equality_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let matched = self.find_equality_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let encoded_value = encode_value_for_index(&matched_filter.value)?;
        let mut index_prefix = format!("index/{entity}/{}/", matched_filter.field).into_bytes();
        index_prefix.extend_from_slice(&encoded_value);
        index_prefix.push(b'/');

        let index_entries = self.storage.prefix_scan(&index_prefix).await?;
        let ids = Self::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
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

    fn equality_scan_sync(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let matched = self.find_equality_index(entity, filters)?;
        let Some((matched_idx, matched_filter)) = matched else {
            return Ok(None);
        };

        let encoded_value = encode_value_for_index(&matched_filter.value)?;
        let mut index_prefix = format!("index/{entity}/{}/", matched_filter.field).into_bytes();
        index_prefix.extend_from_slice(&encoded_value);
        index_prefix.push(b'/');

        let index_entries = self.storage.prefix_scan_sync(&index_prefix)?;
        let ids = Self::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
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

    async fn range_scan_async(
        &self,
        entity: &str,
        filters: &[FilterJs],
    ) -> Result<ScanResult, JsValue> {
        let Some(field) = self.find_range_field(entity, filters)? else {
            return Ok(None);
        };

        let (lower, upper, consumed) = Self::collect_range_bounds(filters, &field)?;
        let (start, end) = Self::build_range_keys(
            entity,
            &field,
            lower.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            upper.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
        );

        let index_entries = self.storage.range_scan(&start, &end).await?;
        let ids = Self::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
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
            .filter(|(i, _)| !consumed.contains(i))
            .map(|(_, f)| f.clone())
            .collect();

        Ok(Some((records, remaining)))
    }

    fn range_scan_sync(&self, entity: &str, filters: &[FilterJs]) -> Result<ScanResult, JsValue> {
        let Some(field) = self.find_range_field(entity, filters)? else {
            return Ok(None);
        };

        let (lower, upper, consumed) = Self::collect_range_bounds(filters, &field)?;
        let (start, end) = Self::build_range_keys(
            entity,
            &field,
            lower.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            upper.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
        );

        let index_entries = self.storage.range_scan_sync(&start, &end)?;
        let ids = Self::extract_ids_from_index_keys(&index_entries);

        let mut records = Vec::with_capacity(ids.len());
        for id in &ids {
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
            .filter(|(i, _)| !consumed.contains(i))
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
