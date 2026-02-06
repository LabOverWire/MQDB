use super::{CursorOptions, JsValue, VecDeque, WasmDatabase, serialize_js, wasm_bindgen};

#[wasm_bindgen]
impl WasmDatabase {
    /// Creates a cursor for streaming iteration over records.
    ///
    /// # Errors
    /// Returns an error if options are invalid or the storage operation fails.
    pub async fn cursor(&self, entity: String, options: JsValue) -> Result<WasmCursor, JsValue> {
        let opts: CursorOptions = if options.is_null() || options.is_undefined() {
            CursorOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|e| JsValue::from_str(&format!("invalid options: {e}")))?
        };

        let prefix = format!("data/{entity}/");
        let items = self.storage.prefix_scan(prefix.as_bytes()).await?;

        let mut all_items: Vec<serde_json::Value> = Vec::new();
        for (_key, value) in items {
            let parsed: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| JsValue::from_str(&format!("deserialization error: {e}")))?;

            if opts
                .filters
                .iter()
                .all(|f| Self::matches_filter(&parsed, f))
            {
                all_items.push(parsed);
            }
        }

        if !opts.sort.is_empty() {
            Self::sort_results(&mut all_items, &opts.sort);
        }

        Ok(WasmCursor {
            buffer: VecDeque::from(all_items),
            current_index: 0,
            exhausted: false,
        })
    }
}

#[wasm_bindgen]
pub struct WasmCursor {
    buffer: VecDeque<serde_json::Value>,
    current_index: usize,
    exhausted: bool,
}

#[wasm_bindgen]
impl WasmCursor {
    /// Returns the next item from the cursor.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn next_item(&mut self) -> Result<JsValue, JsValue> {
        if self.exhausted || self.buffer.is_empty() {
            self.exhausted = true;
            return Ok(JsValue::UNDEFINED);
        }

        if let Some(item) = self.buffer.pop_front() {
            self.current_index += 1;
            serialize_js(&item)
        } else {
            self.exhausted = true;
            Ok(JsValue::UNDEFINED)
        }
    }

    /// Returns up to N items from the cursor as an array.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn next_batch(&mut self, size: usize) -> Result<JsValue, JsValue> {
        if self.exhausted || self.buffer.is_empty() {
            self.exhausted = true;
            return serialize_js(&serde_json::Value::Array(Vec::new()));
        }

        let mut batch = Vec::with_capacity(size);
        for _ in 0..size {
            if let Some(item) = self.buffer.pop_front() {
                self.current_index += 1;
                batch.push(item);
            } else {
                self.exhausted = true;
                break;
            }
        }

        serialize_js(&serde_json::Value::Array(batch))
    }

    pub fn reset(&mut self) {
        self.current_index = 0;
        self.exhausted = false;
    }

    #[must_use]
    pub fn has_more(&self) -> bool {
        !self.exhausted && !self.buffer.is_empty()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub fn position(&self) -> usize {
        self.current_index
    }
}
