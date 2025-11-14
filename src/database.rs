use crate::config::DatabaseConfig;
use crate::dispatcher::EventDispatcher;
use crate::entity::Entity;
use crate::error::{Error, Result};
use crate::events::ChangeEvent;
use crate::index::IndexManager;
use crate::keys;
use crate::relationship::{Relationship, RelationshipRegistry};
use crate::storage::Storage;
use crate::subscription::{Subscription, SubscriptionRegistry};
use serde_json::Value;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct Database {
    storage: Arc<Storage>,
    registry: Arc<SubscriptionRegistry>,
    dispatcher: Arc<EventDispatcher>,
    index_manager: Arc<RwLock<IndexManager>>,
    relationship_registry: Arc<RwLock<RelationshipRegistry>>,
    id_gen_lock: Arc<Mutex<()>>,
    config: Arc<DatabaseConfig>,
}

impl Database {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = DatabaseConfig::new(path.as_ref().to_path_buf());
        Self::open_with_config(config).await
    }

    pub async fn open_with_config(config: DatabaseConfig) -> Result<Self> {
        let storage = Arc::new(Storage::open(&config.path, config.durability.clone())?);
        let registry = Arc::new(SubscriptionRegistry::new(Arc::clone(&storage)));
        let dispatcher = Arc::new(EventDispatcher::new(
            Arc::clone(&registry),
            config.event_channel_capacity,
        ));
        let index_manager = Arc::new(RwLock::new(IndexManager::new()));
        let relationship_registry = Arc::new(RwLock::new(RelationshipRegistry::new()));

        registry.load().await?;

        if let Some(interval_secs) = config.ttl_cleanup_interval_secs {
            let storage_clone = Arc::clone(&storage);
            let dispatcher_clone = Arc::clone(&dispatcher);
            let index_manager_clone = Arc::clone(&index_manager);

            tokio::spawn(async move {
                ttl_cleanup_task(
                    storage_clone,
                    dispatcher_clone,
                    index_manager_clone,
                    interval_secs,
                )
                .await;
            });
        }

        Ok(Self {
            storage,
            registry,
            dispatcher,
            index_manager,
            relationship_registry,
            id_gen_lock: Arc::new(Mutex::new(())),
            config: Arc::new(config),
        })
    }

    pub async fn create(&self, entity_name: String, mut data: Value) -> Result<Value> {
        let id = self.generate_id(&entity_name).await?;

        if let Value::Object(ref mut obj) = data {
            obj.insert("id".to_string(), Value::String(id.clone()));

            if let Some(ttl_secs) = obj.get("ttl_secs").and_then(|v| v.as_u64()) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let expires_at = now + ttl_secs;
                obj.insert("_expires_at".to_string(), Value::Number(expires_at.into()));
                obj.remove("ttl_secs");
            }
        }

        let entity = Entity::new(entity_name.clone(), id, data.clone());

        let mut batch = self.storage.batch();
        batch.insert(entity.key(), entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &entity, None);

        batch.commit()?;

        let event = ChangeEvent::create(entity_name, entity.id.clone(), data.clone());
        self.dispatcher.dispatch(event).await?;

        Ok(entity.to_json())
    }

    pub async fn read(
        &self,
        entity_name: String,
        id: String,
        includes: Vec<String>,
    ) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let data = self
            .storage
            .get(&key)?
            .ok_or_else(|| Error::NotFound {
                entity: entity_name.clone(),
                id: id.clone(),
            })?;

        let entity = Entity::deserialize(entity_name.clone(), id, &data)?;
        let mut result = entity.to_json();

        if !includes.is_empty() {
            self.load_includes(&mut result, &entity_name, &includes, 0)
                .await?;
        }

        Ok(result)
    }

    fn load_includes<'a>(
        &'a self,
        entity: &'a mut Value,
        entity_name: &'a str,
        includes: &'a [String],
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a + Send>> {
        Box::pin(async move {
            const MAX_DEPTH: usize = 3;

            if depth >= MAX_DEPTH {
                return Ok(());
            }

            let registry = self.relationship_registry.read().await;

            for include_field in includes {
                if let Some(rel) = registry.get(entity_name, include_field) {
                    if let Some(id_value) = entity.get(&rel.field_suffix) {
                        if let Some(id_str) = id_value.as_str() {
                            match self
                                .read(rel.target_entity.clone(), id_str.to_string(), vec![])
                                .await
                            {
                                Ok(related_entity) => {
                                    if let Value::Object(obj) = entity {
                                        obj.insert(rel.field.clone(), related_entity);
                                    }
                                }
                                Err(Error::NotFound { .. }) => {
                                    tracing::warn!(
                                        "related entity not found: {}/{}",
                                        rel.target_entity,
                                        id_str
                                    );
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }

    pub async fn update(&self, entity_name: String, id: String, fields: Value) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self
            .storage
            .get(&key)?
            .ok_or_else(|| Error::NotFound {
                entity: entity_name.clone(),
                id: id.clone(),
            })?;

        let mut existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;

        if let (Value::Object(existing), Value::Object(updates)) = (&mut existing_entity.data, fields) {
            for (key, value) in updates {
                existing.insert(key, value);
            }
        }

        let updated_entity = Entity::new(entity_name.clone(), id, existing_entity.data.clone());

        let mut batch = self.storage.batch();
        batch.insert(updated_entity.key(), updated_entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &updated_entity, Some(&existing_entity));

        batch.commit()?;

        let event = ChangeEvent::update(
            entity_name,
            updated_entity.id.clone(),
            updated_entity.data.clone(),
        );
        self.dispatcher.dispatch(event).await?;

        Ok(updated_entity.to_json())
    }

    pub async fn delete(&self, entity_name: String, id: String) -> Result<()> {
        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self
            .storage
            .get(&key)?
            .ok_or_else(|| Error::NotFound {
                entity: entity_name.clone(),
                id: id.clone(),
            })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;

        let mut batch = self.storage.batch();
        batch.remove(key);

        let index_manager = self.index_manager.read().await;
        index_manager.remove_indexes(&mut batch, &existing_entity);

        batch.commit()?;

        let event = ChangeEvent::delete(entity_name, id);
        self.dispatcher.dispatch(event).await?;

        Ok(())
    }

    pub async fn list(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        includes: Vec<String>,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        if filters.is_empty() {
            let prefix = format!("data/{entity_name}/");
            let items = self.storage.prefix_scan(prefix.as_bytes())?;

            for (_key, value) in items {
                let value: Value = serde_json::from_slice(&value)?;
                results.push(value);
            }
        } else {
            let index_manager = self.index_manager.read().await;
            let use_index = filters.first().map(|f| f.op == FilterOp::Eq).unwrap_or(false);

            if use_index {
                let filter = filters.first().unwrap();
                let value_bytes = keys::encode_value_for_index(&filter.value)?;
                let ids = index_manager.lookup_by_field(
                    &self.storage,
                    &entity_name,
                    &filter.field,
                    &value_bytes,
                )?;

                for id in ids {
                    match self.read(entity_name.clone(), id.clone(), vec![]).await {
                        Ok(entity_data) => {
                            if self.matches_filters(&entity_data, &filters) {
                                results.push(entity_data);
                            }
                        }
                        Err(Error::NotFound { .. }) => {
                            tracing::warn!(
                                "index pointed to non-existent entity: {}/{}",
                                entity_name,
                                id
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                let prefix = format!("data/{entity_name}/");
                let items = self.storage.prefix_scan(prefix.as_bytes())?;

                for (_key, value) in items {
                    let entity_data: Value = serde_json::from_slice(&value)?;
                    if self.matches_filters(&entity_data, &filters) {
                        results.push(entity_data);
                    }
                }
            }
        }

        if !sort.is_empty() {
            self.sort_results(&mut results, &sort);
        }

        let offset = pagination.as_ref().map(|p| p.offset).unwrap_or(0);
        let limit = pagination.as_ref().map(|p| p.limit).unwrap_or(usize::MAX);

        let requested_end = offset.saturating_add(limit).min(results.len());

        let final_end = if let Some(max) = self.config.max_list_results {
            if requested_end > max {
                tracing::warn!(
                    "list operation would exceed max_list_results limit of {}, truncating",
                    max
                );
                max.min(results.len())
            } else {
                requested_end
            }
        } else {
            requested_end
        };

        if offset >= results.len() {
            return Ok(vec![]);
        }

        let mut paginated_results = results[offset..final_end].to_vec();

        if !includes.is_empty() {
            for entity in &mut paginated_results {
                self.load_includes(entity, &entity_name, &includes, 0)
                    .await?;
            }
        }

        Ok(paginated_results)
    }

    fn sort_results(&self, results: &mut [Value], sort: &[SortOrder]) {
        results.sort_by(|a, b| {
            for order in sort {
                let a_val = a.get(&order.field);
                let b_val = b.get(&order.field);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => self.compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = match order.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64.partial_cmp(&b_f64).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn matches_filters(&self, entity: &Value, filters: &[Filter]) -> bool {
        for filter in filters {
            if let Some(field_value) = entity.get(&filter.field) {
                if !filter.matches(field_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub async fn subscribe(&self, pattern: String, entity: Option<String>) -> Result<String> {
        if let Some(max_subs) = self.config.max_subscriptions {
            let current_count = self.registry.count().await;
            if current_count >= max_subs {
                return Err(Error::Internal(format!(
                    "maximum subscription limit reached: {current_count}/{max_subs}"
                )));
            }
        }

        let sub_id = uuid::Uuid::new_v4().to_string();
        let subscription = Subscription::new(sub_id.clone(), pattern, entity);

        self.registry.register(subscription).await?;

        Ok(sub_id)
    }

    pub async fn unsubscribe(&self, sub_id: &str) -> Result<()> {
        self.registry.unregister(sub_id).await?;
        self.dispatcher.remove_listener(sub_id).await;
        Ok(())
    }

    pub fn event_receiver(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.dispatcher.subscribe()
    }

    async fn generate_id(&self, entity_name: &str) -> Result<String> {
        let _lock = self.id_gen_lock.lock().await;

        let counter_key = keys::encode_meta_key(&format!("seq:{entity_name}"));

        let current = self
            .storage
            .get(&counter_key)?
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let next = current + 1;
        self.storage.insert(&counter_key, next.to_string().as_bytes())?;

        Ok(next.to_string())
    }

    pub async fn add_index(&self, entity: String, fields: Vec<String>) {
        let mut manager = self.index_manager.write().await;
        manager.add_index(crate::index::IndexDefinition::new(entity, fields));
    }

    pub async fn add_relationship(
        &self,
        source_entity: String,
        field: String,
        target_entity: String,
    ) {
        let mut registry = self.relationship_registry.write().await;
        let relationship = Relationship::new(source_entity, field, target_entity);
        registry.add(relationship);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SortOrder {
    pub field: String,
    pub direction: SortDirection,
}

impl SortOrder {
    pub fn new(field: String, direction: SortDirection) -> Self {
        Self { field, direction }
    }

    pub fn asc(field: String) -> Self {
        Self::new(field, SortDirection::Asc)
    }

    pub fn desc(field: String) -> Self {
        Self::new(field, SortDirection::Desc)
    }
}

#[derive(Debug, Clone)]
pub struct Pagination {
    pub limit: usize,
    pub offset: usize,
}

impl Pagination {
    pub fn new(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Like,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
    pub value: Value,
}

impl Filter {
    pub fn new(field: String, op: FilterOp, value: Value) -> Self {
        Self { field, op, value }
    }

    pub fn matches(&self, field_value: &Value) -> bool {
        match self.op {
            FilterOp::Eq => field_value == &self.value,
            FilterOp::Neq => field_value != &self.value,
            FilterOp::Lt => self.compare_values(field_value, &self.value) == Some(std::cmp::Ordering::Less),
            FilterOp::Lte => matches!(
                self.compare_values(field_value, &self.value),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            FilterOp::Gt => self.compare_values(field_value, &self.value) == Some(std::cmp::Ordering::Greater),
            FilterOp::Gte => matches!(
                self.compare_values(field_value, &self.value),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            FilterOp::In => {
                if let Value::Array(values) = &self.value {
                    values.contains(field_value)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Value::String(field_str), Value::String(pattern)) = (field_value, &self.value) {
                    self.glob_match(field_str, pattern)
                } else {
                    false
                }
            }
            FilterOp::IsNull => field_value.is_null(),
            FilterOp::IsNotNull => !field_value.is_null(),
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (Value::Number(a), Value::Number(b)) => {
                let a_f64 = a.as_f64()?;
                let b_f64 = b.as_f64()?;
                a_f64.partial_cmp(&b_f64)
            }
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    fn glob_match(&self, text: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        let parts: Vec<&str> = pattern.split('*').collect();

        if parts.len() == 1 {
            return text == pattern;
        }

        let non_empty_parts: Vec<&str> = parts.iter().filter(|p| !p.is_empty()).copied().collect();

        if non_empty_parts.is_empty() {
            return true;
        }

        let starts_with_wildcard = pattern.starts_with('*');
        let ends_with_wildcard = pattern.ends_with('*');

        let mut text_pos = 0;

        for (idx, part) in non_empty_parts.iter().enumerate() {
            let is_first = idx == 0;
            let is_last = idx == non_empty_parts.len() - 1;

            if is_first && !starts_with_wildcard {
                if !text.starts_with(part) {
                    return false;
                }
                text_pos = part.len();
            } else if is_last && !ends_with_wildcard {
                return text[text_pos..].ends_with(part);
            } else if let Some(pos) = text[text_pos..].find(part) {
                text_pos += pos + part.len();
            } else {
                return false;
            }
        }

        true
    }
}

async fn ttl_cleanup_task(
    storage: Arc<Storage>,
    dispatcher: Arc<EventDispatcher>,
    index_manager: Arc<RwLock<IndexManager>>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let prefix = b"data/";
        let items = match storage.prefix_scan(prefix) {
            Ok(items) => items,
            Err(_) => continue,
        };

        for (key, value) in items {
            let key_str = match std::str::from_utf8(&key) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() != 3 {
                continue;
            }

            let entity_name = parts[1];
            let id = parts[2];

            let entity = match Entity::deserialize(entity_name.to_string(), id.to_string(), &value)
            {
                Ok(e) => e,
                Err(_) => continue,
            };

            if let Some(expires_at) = entity
                .data
                .get("_expires_at")
                .and_then(|v| v.as_u64())
            {
                if expires_at <= now {
                    let mut batch = storage.batch();
                    batch.remove(key.clone());

                    let index_mgr = index_manager.read().await;
                    index_mgr.remove_indexes(&mut batch, &entity);

                    if batch.commit().is_ok() {
                        let event = ChangeEvent::delete(
                            entity_name.to_string(),
                            id.to_string(),
                        );
                        let _ = dispatcher.dispatch(event).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_glob_match() {
        let filter = Filter::new("test".into(), FilterOp::Like, json!("*li*"));

        assert!(filter.glob_match("Alice", "*li*"));
        assert!(filter.glob_match("Charlie", "*li*"));
        assert!(!filter.glob_match("Bob", "*li*"));
        assert!(!filter.glob_match("David", "*li*"));

        let filter2 = Filter::new("test".into(), FilterOp::Like, json!("*@example.com"));
        assert!(filter2.glob_match("test@example.com", "*@example.com"));
        assert!(filter2.glob_match("a@example.com", "*@example.com"));

        let filter3 = Filter::new("test".into(), FilterOp::Like, json!("*lie"));
        assert!(filter3.glob_match("Charlie", "*lie"));
        assert!(!filter3.glob_match("Alice", "*lie"));
    }
}
