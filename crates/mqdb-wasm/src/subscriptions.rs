use super::{
    ChangeEvent, HashMap, JsValue, SubscriptionEntry, SubscriptionMode, WasmDatabase,
    match_pattern, serialize_event, wasm_bindgen,
};

#[wasm_bindgen]
impl WasmDatabase {
    pub fn subscribe(
        &self,
        pattern: String,
        entity: Option<String>,
        callback: js_sys::Function,
    ) -> Result<String, JsValue> {
        let sub_id = uuid::Uuid::new_v4().to_string();
        let now = js_sys::Date::now();

        let mut inner = self.borrow_inner_mut()?;
        inner.subscriptions.insert(
            sub_id.clone(),
            SubscriptionEntry {
                pattern,
                entity,
                callback,
                share_group: None,
                mode: SubscriptionMode::default(),
                last_heartbeat: now,
            },
        );

        Ok(sub_id)
    }

    pub fn subscribe_shared(
        &self,
        pattern: String,
        entity: Option<String>,
        group: String,
        mode: &str,
        callback: js_sys::Function,
    ) -> Result<String, JsValue> {
        let sub_id = uuid::Uuid::new_v4().to_string();
        let now = js_sys::Date::now();

        let mode = match mode {
            "load-balanced" | "load_balanced" => SubscriptionMode::LoadBalanced,
            "ordered" => SubscriptionMode::Ordered,
            _ => SubscriptionMode::Broadcast,
        };

        let mut inner = self.borrow_inner_mut()?;
        inner.subscriptions.insert(
            sub_id.clone(),
            SubscriptionEntry {
                pattern,
                entity,
                callback,
                share_group: Some(group),
                mode,
                last_heartbeat: now,
            },
        );

        Ok(sub_id)
    }

    pub fn heartbeat(&self, sub_id: &str) -> Result<bool, JsValue> {
        let mut inner = self.borrow_inner_mut()?;
        if let Some(entry) = inner.subscriptions.get_mut(sub_id) {
            entry.last_heartbeat = js_sys::Date::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_subscription_info(&self, sub_id: &str) -> Result<JsValue, JsValue> {
        let inner = self.borrow_inner()?;
        match inner.subscriptions.get(sub_id) {
            Some(entry) => {
                let info = serde_json::json!({
                    "id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "share_group": entry.share_group,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                });
                let json_str = serde_json::to_string(&info).unwrap_or_else(|_| "null".to_string());
                Ok(js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL))
            }
            None => Ok(JsValue::NULL),
        }
    }

    pub fn list_consumer_groups(&self) -> Result<JsValue, JsValue> {
        let inner = self.borrow_inner()?;
        let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        for (sub_id, entry) in &inner.subscriptions {
            if let Some(ref group_name) = entry.share_group {
                let member = serde_json::json!({
                    "subscription_id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                });
                groups.entry(group_name.clone()).or_default().push(member);
            }
        }

        let result: Vec<serde_json::Value> = groups
            .into_iter()
            .map(|(name, members)| {
                serde_json::json!({
                    "name": name,
                    "member_count": members.len(),
                    "members": members
                })
            })
            .collect();

        let json_str = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
        Ok(js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL))
    }

    pub fn get_consumer_group(&self, group_name: &str) -> Result<JsValue, JsValue> {
        let inner = self.borrow_inner()?;
        let mut members: Vec<serde_json::Value> = Vec::new();

        for (sub_id, entry) in &inner.subscriptions {
            if entry.share_group.as_deref() == Some(group_name) {
                members.push(serde_json::json!({
                    "subscription_id": sub_id,
                    "pattern": entry.pattern,
                    "entity": entry.entity,
                    "mode": match entry.mode {
                        SubscriptionMode::Broadcast => "broadcast",
                        SubscriptionMode::LoadBalanced => "load-balanced",
                        SubscriptionMode::Ordered => "ordered",
                    },
                    "last_heartbeat": entry.last_heartbeat
                }));
            }
        }

        if members.is_empty() {
            Ok(JsValue::NULL)
        } else {
            let result = serde_json::json!({
                "name": group_name,
                "member_count": members.len(),
                "members": members
            });
            let json_str = serde_json::to_string(&result).unwrap_or_else(|_| "null".to_string());
            Ok(js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL))
        }
    }

    pub fn unsubscribe(&self, sub_id: &str) -> Result<bool, JsValue> {
        let mut inner = self.borrow_inner_mut()?;
        Ok(inner.subscriptions.remove(sub_id).is_some())
    }
}

impl WasmDatabase {
    pub(crate) fn dispatch_event(&self, event: &ChangeEvent) {
        let Ok(event_js) = serialize_event(event) else {
            return;
        };

        let mut broadcast_callbacks: Vec<js_sys::Function> = Vec::new();
        let mut share_groups: HashMap<String, Vec<js_sys::Function>> = HashMap::new();

        {
            let Ok(inner) = self.inner.try_borrow() else {
                return;
            };
            for sub in inner.subscriptions.values() {
                if !Self::matches_subscription(sub, event) {
                    continue;
                }

                match (&sub.share_group, sub.mode) {
                    (None, _) | (Some(_), SubscriptionMode::Broadcast) => {
                        broadcast_callbacks.push(sub.callback.clone());
                    }
                    (Some(group), _) => {
                        share_groups
                            .entry(group.clone())
                            .or_default()
                            .push(sub.callback.clone());
                    }
                }
            }
        }

        for callback in broadcast_callbacks {
            let _ = callback.call1(&JsValue::NULL, &event_js);
        }

        if !share_groups.is_empty() {
            let Ok(mut inner) = self.inner.try_borrow_mut() else {
                return;
            };
            for (group_name, callbacks) in share_groups {
                if callbacks.is_empty() {
                    continue;
                }

                let counter = inner.round_robin_counters.entry(group_name).or_insert(0);
                let idx = *counter % callbacks.len();
                *counter = counter.wrapping_add(1);

                let selected_callback = &callbacks[idx];
                let _ = selected_callback.call1(&JsValue::NULL, &event_js);
            }
        }
    }

    fn matches_subscription(sub: &SubscriptionEntry, event: &ChangeEvent) -> bool {
        if let Some(ref entity) = sub.entity
            && entity != &event.entity
        {
            return false;
        }

        if sub.pattern == "*" || sub.pattern == "#" {
            return true;
        }

        match_pattern(&sub.pattern, &event.entity, &event.id)
    }
}
