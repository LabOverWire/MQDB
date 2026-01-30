use super::{FilterJs, JsValue, ListOptions, SortOrderJs, WasmDatabase, wasm_bindgen};

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

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

        let mut results = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                results.push(parsed);
            }
        }

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
}

impl WasmDatabase {
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
