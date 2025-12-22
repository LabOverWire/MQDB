use crate::config::DatabaseConfig;
use crate::constraint::ConstraintManager;
use crate::consumer_group::{
    ConsumerGroup, ConsumerGroupDetails, ConsumerGroupInfo, ConsumerMemberInfo,
};
use crate::dispatcher::EventDispatcher;
use crate::entity::Entity;
use crate::error::{Error, Result};
use crate::events::ChangeEvent;
use crate::index::IndexManager;
use crate::keys;
use crate::outbox::{Outbox, OutboxProcessor};
use crate::relationship::{Relationship, RelationshipRegistry};
use crate::schema::{Schema, SchemaRegistry};
use crate::storage::{Storage, StorageBackend};
use crate::subscription::{Subscription, SubscriptionMode, SubscriptionRegistry};
use crate::types::{Filter, FilterOp, Pagination, SortDirection, SortOrder};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, watch};

#[derive(Debug, Clone)]
pub struct SubscriptionResult {
    pub id: String,
    pub assigned_partitions: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct Database {
    storage: Arc<Storage>,
    registry: Arc<SubscriptionRegistry>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    relationship_registry: Arc<RwLock<RelationshipRegistry>>,
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
    consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    id_gen_lock: Arc<Mutex<()>>,
    config: Arc<DatabaseConfig>,
    shutdown_tx: watch::Sender<bool>,
}

impl Database {
    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = DatabaseConfig::new(path.as_ref().to_path_buf());
        Self::open_with_config(config).await
    }

    /// # Errors
    /// Returns an error if the database cannot be opened or initialized.
    pub async fn open_with_config(config: DatabaseConfig) -> Result<Self> {
        let storage = Arc::new(Storage::open(&config.path, config.durability.clone())?);
        Self::init_with_storage(storage, config).await
    }

    /// # Errors
    /// Returns an error if initialization fails.
    pub async fn open_with_backend(
        backend: Arc<dyn StorageBackend>,
        config: DatabaseConfig,
    ) -> Result<Self> {
        let storage = Arc::new(Storage::with_backend(backend));
        Self::init_with_storage(storage, config).await
    }

    async fn init_with_storage(storage: Arc<Storage>, config: DatabaseConfig) -> Result<Self> {
        let registry = Arc::new(SubscriptionRegistry::new(Arc::clone(&storage)));
        let consumer_groups = Arc::new(RwLock::new(HashMap::new()));
        let dispatcher = Arc::new(EventDispatcher::new(
            Arc::clone(&registry),
            config.event_channel_capacity,
            Arc::clone(&consumer_groups),
            config.shared_subscription.num_partitions,
        ));
        let outbox = Arc::new(Outbox::new(Arc::clone(&storage)));
        let index_manager = Arc::new(RwLock::new(IndexManager::new()));
        let relationship_registry = Arc::new(RwLock::new(RelationshipRegistry::new()));

        let mut schema_registry = SchemaRegistry::new();
        schema_registry.load_schemas(&storage)?;
        let schema_registry = Arc::new(RwLock::new(schema_registry));

        let mut constraint_manager = ConstraintManager::new();
        constraint_manager.load_constraints(&storage)?;
        let constraint_manager = Arc::new(RwLock::new(constraint_manager));

        registry.load().await?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        if config.outbox.enabled {
            let pending_count = outbox.pending_count().unwrap_or(0);
            if pending_count > 0 {
                tracing::info!(
                    pending = pending_count,
                    "found pending outbox entries, starting processor"
                );
            }

            let mut processor = OutboxProcessor::new(
                Arc::clone(&outbox),
                Arc::clone(&dispatcher),
                config.outbox.clone(),
                shutdown_rx.clone(),
            );
            tokio::spawn(async move {
                processor.run().await;
            });
        }

        if let Some(interval_secs) = config.ttl_cleanup_interval_secs {
            let storage_clone = Arc::clone(&storage);
            let dispatcher_clone = Arc::clone(&dispatcher);
            let outbox_clone = Arc::clone(&outbox);
            let index_manager_clone = Arc::clone(&index_manager);

            tokio::spawn(async move {
                ttl_cleanup_task(
                    storage_clone,
                    dispatcher_clone,
                    outbox_clone,
                    index_manager_clone,
                    interval_secs,
                )
                .await;
            });
        }

        if config.shared_subscription.consumer_timeout_ms > 0 {
            let consumer_groups_clone = Arc::clone(&consumer_groups);
            let timeout_ms = config.shared_subscription.consumer_timeout_ms;
            let mut shutdown_rx_clone = shutdown_rx.clone();

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(timeout_ms / 2));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let mut groups = consumer_groups_clone.write().await;
                            for (name, group) in groups.iter_mut() {
                                let stale = group.remove_stale_members(timeout_ms);
                                if !stale.is_empty() {
                                    tracing::info!(
                                        group = %name,
                                        removed = ?stale,
                                        "removed stale consumers"
                                    );
                                }
                            }
                        }
                        _ = shutdown_rx_clone.changed() => {
                            tracing::debug!("heartbeat cleanup task shutting down");
                            break;
                        }
                    }
                }
            });
        }

        Ok(Self {
            storage,
            registry,
            dispatcher,
            outbox,
            index_manager,
            relationship_registry,
            schema_registry,
            constraint_manager,
            consumer_groups,
            id_gen_lock: Arc::new(Mutex::new(())),
            config: Arc::new(config),
            shutdown_tx,
        })
    }

    /// # Errors
    /// Returns an error if validation or persistence fails.
    pub async fn create(&self, entity_name: String, mut data: Value) -> Result<Value> {
        let schema_registry = self.schema_registry.read().await;
        schema_registry.apply_defaults(&entity_name, &mut data)?;
        drop(schema_registry);

        let id = self.generate_id(&entity_name).await?;

        if let Value::Object(ref mut obj) = data {
            obj.insert("id".to_string(), Value::String(id.clone()));

            if let Some(ttl_secs) = obj.get("ttl_secs").and_then(Value::as_u64) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| Error::SystemTime(format!("failed to get system time: {e}")))?
                    .as_secs();
                let expires_at = now + ttl_secs;
                obj.insert("_expires_at".to_string(), Value::Number(expires_at.into()));
                obj.remove("ttl_secs");
            }
        }

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &data)?;
        drop(schema_registry);

        let entity = Entity::new(entity_name.clone(), id, data.clone());

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_create(&entity, &mut batch, &self.storage)?;
        drop(constraint_manager);

        batch.insert(entity.key(), entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &entity, None);

        let event = ChangeEvent::create(entity_name, entity.id.clone(), data.clone());
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox.enqueue_event(&mut batch, &operation_id, &event);

        batch.commit()?;

        self.dispatcher.dispatch(event).await?;
        self.outbox.mark_delivered(&operation_id)?;

        Ok(entity.to_json())
    }

    /// # Errors
    /// Returns an error if the entity is not found or reading fails.
    pub async fn read(
        &self,
        entity_name: String,
        id: String,
        includes: Vec<String>,
        projection: Option<Vec<String>>,
    ) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let entity = Entity::deserialize(entity_name.clone(), id, &data)?;
        let mut result = entity.to_json();

        if !includes.is_empty() {
            self.load_includes(&mut result, &entity_name, &includes, 0)
                .await?;
        }

        let result = if let Some(ref fields) = projection {
            Self::project_fields(result, fields)
        } else {
            result
        };

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
                if let Some(rel) = registry.get(entity_name, include_field)
                    && let Some(id_value) = entity.get(&rel.field_suffix)
                    && let Some(id_str) = id_value.as_str()
                {
                    match self
                        .read(rel.target_entity.clone(), id_str.to_string(), vec![], None)
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

            Ok(())
        })
    }

    fn project_fields(entity: Value, fields: &[String]) -> Value {
        if let Value::Object(obj) = entity {
            let mut projected = serde_json::Map::new();

            if let Some(id) = obj.get("id") {
                projected.insert("id".to_string(), id.clone());
            }

            for field in fields {
                if field != "id"
                    && let Some(value) = obj.get(field)
                {
                    projected.insert(field.clone(), value.clone());
                }
            }
            Value::Object(projected)
        } else {
            entity
        }
    }

    /// # Errors
    /// Returns an error if the entity is not found or update fails.
    pub async fn update(&self, entity_name: String, id: String, fields: Value) -> Result<Value> {
        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;
        let mut updated_data = existing_entity.data.clone();

        if let (Value::Object(existing), Value::Object(updates)) = (&mut updated_data, fields) {
            for (key, value) in updates {
                existing.insert(key, value);
            }
        }

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_entity(&entity_name, &updated_data)?;
        drop(schema_registry);

        let updated_entity = Entity::new(entity_name.clone(), id.clone(), updated_data);

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.validate_update(
            &updated_entity,
            &existing_entity,
            &mut batch,
            &self.storage,
        )?;
        drop(constraint_manager);

        batch.expect_value(key, existing_data);
        batch.insert(updated_entity.key(), updated_entity.serialize()?);

        let index_manager = self.index_manager.read().await;
        index_manager.update_indexes(&mut batch, &updated_entity, Some(&existing_entity));

        let event = ChangeEvent::update(
            entity_name,
            updated_entity.id.clone(),
            updated_entity.data.clone(),
        );
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox.enqueue_event(&mut batch, &operation_id, &event);

        batch.commit()?;

        self.dispatcher.dispatch(event).await?;
        self.outbox.mark_delivered(&operation_id)?;

        Ok(updated_entity.to_json())
    }

    /// # Errors
    /// Returns an error if the entity is not found or deletion fails.
    pub async fn delete(&self, entity_name: String, id: String) -> Result<()> {
        use crate::constraint::DeleteOperation;

        let key = keys::encode_data_key(&entity_name, &id);

        let existing_data = self.storage.get(&key)?.ok_or_else(|| Error::NotFound {
            entity: entity_name.clone(),
            id: id.clone(),
        })?;

        let existing_entity = Entity::deserialize(entity_name.clone(), id.clone(), &existing_data)?;

        let constraint_manager = self.constraint_manager.read().await;
        let delete_ops = constraint_manager.validate_delete(&existing_entity, &self.storage)?;
        drop(constraint_manager);

        let mut batch = self.storage.batch();

        batch.remove(key.clone());

        let index_manager = self.index_manager.read().await;
        index_manager.remove_indexes(&mut batch, &existing_entity);
        drop(index_manager);

        let mut deleted_entities = Vec::new();

        for operation in &delete_ops {
            match operation {
                DeleteOperation::Cascade(cascade_op) => {
                    let cascade_key = keys::encode_data_key(&cascade_op.entity, &cascade_op.id);
                    if let Some(cascade_data) = self.storage.get(&cascade_key)? {
                        let cascade_entity = Entity::deserialize(
                            cascade_op.entity.clone(),
                            cascade_op.id.clone(),
                            &cascade_data,
                        )?;

                        batch.remove(cascade_key);

                        let index_manager = self.index_manager.read().await;
                        index_manager.remove_indexes(&mut batch, &cascade_entity);
                        drop(index_manager);

                        deleted_entities.push((cascade_op.entity.clone(), cascade_op.id.clone()));
                    }
                }
                DeleteOperation::SetNull(set_null_op) => {
                    let entity_key = keys::encode_data_key(&set_null_op.entity, &set_null_op.id);
                    if let Some(entity_data) = self.storage.get(&entity_key)? {
                        let mut entity = Entity::deserialize(
                            set_null_op.entity.clone(),
                            set_null_op.id.clone(),
                            &entity_data,
                        )?;

                        let old_entity = entity.clone();

                        if let Some(obj) = entity.data.as_object_mut() {
                            obj.insert(set_null_op.field.clone(), serde_json::Value::Null);
                        }

                        batch.insert(entity_key, entity.serialize()?);

                        let index_manager = self.index_manager.read().await;
                        index_manager.update_indexes(&mut batch, &entity, Some(&old_entity));
                        drop(index_manager);
                    }
                }
            }
        }

        let mut events = vec![ChangeEvent::delete(entity_name, id)];
        for (entity, id) in &deleted_entities {
            events.push(ChangeEvent::delete(entity.clone(), id.clone()));
        }

        let operation_id = uuid::Uuid::new_v4().to_string();
        self.outbox
            .enqueue_events(&mut batch, &operation_id, &events);

        batch.commit()?;

        for event in events {
            self.dispatcher.dispatch(event).await?;
        }
        self.outbox.mark_delivered(&operation_id)?;

        Ok(())
    }

    /// # Errors
    /// Returns an error if listing entities fails.
    pub async fn list(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        includes: Vec<String>,
        projection: Option<Vec<String>>,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        if filters.is_empty() {
            let prefix = format!("data/{entity_name}/");
            let items = self.storage.prefix_scan(prefix.as_bytes())?;

            for (key, value) in items {
                let (_, id) = keys::decode_data_key(&key)?;
                let entity = Entity::deserialize(entity_name.clone(), id, &value)?;
                results.push(entity.to_json());
            }
        } else {
            let index_manager = self.index_manager.read().await;
            let use_index = filters.first().is_some_and(|f| f.op == FilterOp::Eq);

            if use_index {
                if let Some(filter) = filters.first() {
                    let value_bytes = keys::encode_value_for_index(&filter.value)?;
                    let ids = index_manager.lookup_by_field(
                        &self.storage,
                        &entity_name,
                        &filter.field,
                        &value_bytes,
                    )?;

                    for id in ids {
                        match self
                            .read(entity_name.clone(), id.clone(), vec![], None)
                            .await
                        {
                            Ok(entity_data) => {
                                if Self::matches_filters(&entity_data, &filters) {
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
                }
            } else {
                let prefix = format!("data/{entity_name}/");
                let items = self.storage.prefix_scan(prefix.as_bytes())?;

                for (key, value) in items {
                    let (_, id) = keys::decode_data_key(&key)?;
                    let entity = Entity::deserialize(entity_name.clone(), id, &value)?;
                    let entity_data = entity.to_json();
                    if Self::matches_filters(&entity_data, &filters) {
                        results.push(entity_data);
                    }
                }
            }
        }

        if !sort.is_empty() {
            Self::sort_results(&mut results, &sort);
        }

        let offset = pagination.as_ref().map_or(0, |p| p.offset);
        let limit = pagination.as_ref().map_or(usize::MAX, |p| p.limit);

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

        let paginated_results = if let Some(ref fields) = projection {
            paginated_results
                .into_iter()
                .map(|e| Self::project_fields(e, fields))
                .collect()
        } else {
            paginated_results
        };

        Ok(paginated_results)
    }

    /// # Errors
    /// Returns an error if creating the cursor fails.
    pub fn cursor(
        &self,
        entity_name: String,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
    ) -> Result<crate::cursor::Cursor> {
        crate::cursor::Cursor::new(
            entity_name,
            &self.storage,
            filters,
            sort,
            self.config.max_cursor_buffer,
            self.config.max_sort_buffer,
        )
    }

    fn sort_results(results: &mut [Value], sort: &[SortOrder]) {
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

    fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn matches_filters(entity: &Value, filters: &[Filter]) -> bool {
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

    /// # Errors
    /// Returns an error if registration fails or limit is reached.
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

    /// # Errors
    /// Returns an error if registration fails or limit is reached.
    pub async fn subscribe_shared(
        &self,
        pattern: String,
        entity: Option<String>,
        group: String,
        mode: SubscriptionMode,
    ) -> Result<SubscriptionResult> {
        if let Some(max_subs) = self.config.max_subscriptions {
            let current_count = self.registry.count().await;
            if current_count >= max_subs {
                return Err(Error::Internal(format!(
                    "maximum subscription limit reached: {current_count}/{max_subs}"
                )));
            }
        }

        {
            let groups = self.consumer_groups.read().await;
            if let Some(existing_group) = groups.get(&group) {
                for member in existing_group.members() {
                    if let Some(existing_sub) = self.registry.get(&member.consumer_id).await {
                        if existing_sub.mode != mode {
                            return Err(Error::Validation(format!(
                                "share group '{}' already uses {:?} mode, cannot mix with {:?}",
                                group, existing_sub.mode, mode
                            )));
                        }
                        break;
                    }
                }
            }
        }

        let sub_id = uuid::Uuid::new_v4().to_string();
        let consumer_id = sub_id.clone();

        let assigned_partitions = {
            let mut groups = self.consumer_groups.write().await;
            let num_partitions = self.config.shared_subscription.num_partitions;

            let consumer_group = groups
                .entry(group.clone())
                .or_insert_with(|| ConsumerGroup::new(group.clone(), num_partitions));

            let partitions = consumer_group.add_member(consumer_id);

            if mode == SubscriptionMode::Ordered {
                Some(partitions)
            } else {
                None
            }
        };

        let subscription =
            Subscription::new(sub_id.clone(), pattern, entity).with_share_group(group, mode);

        self.registry.register(subscription).await?;

        Ok(SubscriptionResult {
            id: sub_id,
            assigned_partitions,
        })
    }

    /// # Errors
    /// Returns an error if unregistering fails.
    pub async fn unsubscribe(&self, sub_id: &str) -> Result<()> {
        if let Some(sub) = self.registry.get(sub_id).await
            && let Some(group) = &sub.share_group
        {
            let mut groups = self.consumer_groups.write().await;
            if let Some(cg) = groups.get_mut(group) {
                cg.remove_member(sub_id);
                if cg.member_count() == 0 {
                    groups.remove(group);
                }
            }
        }

        self.registry.unregister(sub_id).await?;
        self.dispatcher.remove_listener(sub_id).await;
        Ok(())
    }

    /// # Errors
    /// Returns an error if the subscription is not found.
    pub async fn heartbeat(&self, sub_id: &str) -> Result<()> {
        let sub = self
            .registry
            .get(sub_id)
            .await
            .ok_or_else(|| Error::NotFound {
                entity: "subscription".into(),
                id: sub_id.into(),
            })?;

        if let Some(group) = &sub.share_group {
            let mut groups = self.consumer_groups.write().await;
            if let Some(cg) = groups.get_mut(group) {
                cg.update_heartbeat(sub_id);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn event_receiver(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.dispatcher.subscribe()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.config.path
    }

    #[must_use]
    pub fn num_partitions(&self) -> u8 {
        self.config.shared_subscription.num_partitions
    }

    pub async fn list_consumer_groups(&self) -> Vec<ConsumerGroupInfo> {
        let groups = self.consumer_groups.read().await;
        groups
            .iter()
            .map(|(name, cg)| ConsumerGroupInfo {
                name: name.clone(),
                member_count: cg.member_count(),
                total_partitions: self.config.shared_subscription.num_partitions,
            })
            .collect()
    }

    pub async fn get_consumer_group(&self, name: &str) -> Option<ConsumerGroupDetails> {
        let groups = self.consumer_groups.read().await;
        groups.get(name).map(|cg| ConsumerGroupDetails {
            name: name.to_string(),
            members: cg.members().map(ConsumerMemberInfo::from).collect(),
            total_partitions: self.config.shared_subscription.num_partitions,
        })
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        tracing::info!("database shutdown signal sent");
    }

    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup<P: AsRef<Path>>(&self, backup_path: P) -> Result<()> {
        self.storage.flush()?;

        let src = &self.config.path;
        let dst = backup_path.as_ref();

        if dst.exists() {
            std::fs::remove_dir_all(dst).map_err(|e| {
                Error::BackupFailed(format!("failed to remove existing backup: {e}"))
            })?;
        }

        copy_dir_recursive(src, dst)
            .map_err(|e| Error::BackupFailed(format!("failed to create backup: {e}")))?;

        Ok(())
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
        self.storage
            .insert(&counter_key, next.to_string().as_bytes())?;

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

    pub async fn get_schema(&self, entity: &str) -> Option<Schema> {
        let registry = self.schema_registry.read().await;
        registry.get_schema(entity).cloned()
    }

    pub async fn list_constraints(&self, entity: &str) -> Vec<crate::constraint::Constraint> {
        let manager = self.constraint_manager.read().await;
        manager.get_constraints(entity).to_vec()
    }

    /// # Errors
    /// Returns an error if persisting the schema fails.
    pub async fn add_schema(&self, schema: Schema) -> Result<()> {
        let mut batch = self.storage.batch();

        let schema_registry = self.schema_registry.read().await;
        schema_registry.persist_schema(&mut batch, &schema)?;
        drop(schema_registry);

        batch.commit()?;

        let mut schema_registry = self.schema_registry.write().await;
        schema_registry.add_schema(schema);

        Ok(())
    }

    /// # Errors
    /// Returns an error if persisting the constraint fails.
    pub async fn add_unique_constraint(&self, entity: String, fields: Vec<String>) -> Result<()> {
        use crate::constraint::{Constraint, UniqueConstraint};

        self.add_index(entity.clone(), fields.clone()).await;

        let constraint = Constraint::Unique(UniqueConstraint::new(entity, fields));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }

    /// # Errors
    /// Returns an error if persisting the constraint fails.
    pub async fn add_not_null(&self, entity: String, field: String) -> Result<()> {
        use crate::constraint::{Constraint, NotNullConstraint};

        let constraint = Constraint::NotNull(NotNullConstraint::new(entity, field));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }

    /// # Errors
    /// Returns an error if persisting the constraint fails.
    pub async fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: crate::constraint::OnDeleteAction,
    ) -> Result<()> {
        use crate::constraint::{Constraint, ForeignKeyConstraint};

        let constraint = Constraint::ForeignKey(ForeignKeyConstraint::new(
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
        ));

        let mut batch = self.storage.batch();

        let constraint_manager = self.constraint_manager.read().await;
        constraint_manager.persist_constraint(&mut batch, &constraint)?;
        drop(constraint_manager);

        batch.commit()?;

        let mut constraint_manager = self.constraint_manager.write().await;
        constraint_manager.add_constraint(constraint);

        Ok(())
    }

    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup_physical<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        use std::fs;
        use std::io;

        fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
            fs::create_dir_all(dst)?;

            for entry in fs::read_dir(src)? {
                let entry = entry?;
                let path = entry.path();
                let dest_path = dst.join(entry.file_name());

                if path.is_dir() {
                    copy_dir_recursive(&path, &dest_path)?;
                } else {
                    fs::copy(&path, &dest_path)?;
                }
            }
            Ok(())
        }

        let dest = destination.as_ref();

        self.storage.flush()?;

        if dest.exists() {
            return Err(crate::error::Error::BackupFailed(format!(
                "destination already exists: {}",
                dest.display()
            )));
        }

        let src = &self.config.path;

        copy_dir_recursive(src, dest).map_err(|e| {
            crate::error::Error::BackupFailed(format!("failed to copy database: {e}"))
        })?;

        tracing::info!(
            "physical backup completed: {} -> {}",
            src.display(),
            dest.display()
        );
        Ok(())
    }

    /// # Errors
    /// Returns an error if the backup fails.
    pub fn backup_logical<P: AsRef<Path>>(&self, destination: P) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;

        let dest = destination.as_ref();

        if dest.exists() {
            return Err(crate::error::Error::BackupFailed(format!(
                "destination already exists: {}",
                dest.display()
            )));
        }

        let file = File::create(dest).map_err(|e| {
            crate::error::Error::BackupFailed(format!("failed to create backup file: {e}"))
        })?;

        let mut writer = BufWriter::new(file);

        let prefix = b"data/";
        let items = self.storage.prefix_scan(prefix)?;

        let mut count = 0;
        for (key, value) in items {
            let (entity_name, id) = crate::keys::decode_data_key(&key)?;
            let entity = crate::entity::Entity::deserialize(entity_name.clone(), id, &value)?;

            let backup_record = serde_json::json!({
                "_entity": entity_name,
                "_data": entity.to_json()
            });

            serde_json::to_writer(&mut writer, &backup_record).map_err(|e| {
                crate::error::Error::BackupFailed(format!("failed to write entity: {e}"))
            })?;

            std::io::Write::write_all(&mut writer, b"\n").map_err(|e| {
                crate::error::Error::BackupFailed(format!("failed to write newline: {e}"))
            })?;

            count += 1;
        }

        std::io::Write::flush(&mut writer).map_err(|e| {
            crate::error::Error::BackupFailed(format!("failed to flush backup: {e}"))
        })?;

        tracing::info!(
            "logical backup completed: {count} entities written to {}",
            dest.display()
        );
        Ok(())
    }

    /// # Errors
    /// Returns an error if the restore fails.
    pub async fn restore_logical<P: AsRef<Path>>(&self, source: P) -> Result<usize> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let src = source.as_ref();

        if !src.exists() {
            return Err(crate::error::Error::BackupFailed(format!(
                "backup file not found: {}",
                src.display()
            )));
        }

        let file = File::open(src).map_err(|e| {
            crate::error::Error::BackupFailed(format!("failed to open backup file: {e}"))
        })?;

        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            let line = line.map_err(|e| {
                crate::error::Error::BackupFailed(format!("failed to read line: {e}"))
            })?;

            if line.trim().is_empty() {
                continue;
            }

            let backup_record: Value = serde_json::from_str(&line).map_err(|e| {
                crate::error::Error::BackupFailed(format!("failed to parse entity: {e}"))
            })?;

            let entity_name = backup_record
                .get("_entity")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    crate::error::Error::BackupFailed("entity missing _entity field".to_string())
                })?
                .to_string();

            let entity_data = backup_record
                .get("_data")
                .ok_or_else(|| {
                    crate::error::Error::BackupFailed("entity missing _data field".to_string())
                })?
                .clone();

            self.create(entity_name, entity_data).await?;
            count += 1;
        }

        tracing::info!(
            "logical restore completed: {count} entities restored from {}",
            src.display()
        );
        Ok(count)
    }
}

async fn ttl_cleanup_task(
    storage: Arc<Storage>,
    dispatcher: Arc<EventDispatcher>,
    outbox: Arc<Outbox>,
    index_manager: Arc<RwLock<IndexManager>>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(e) => {
                tracing::warn!("TTL cleanup: failed to get system time: {e}");
                continue;
            }
        };

        let prefix = b"data/";
        let Ok(items) = storage.prefix_scan(prefix) else {
            continue;
        };

        let mut expired_entities = Vec::new();

        for (key, value) in items {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };

            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() != 3 {
                continue;
            }

            let entity_name = parts[1];
            let id = parts[2];

            let Ok(entity) =
                Entity::deserialize(entity_name.to_string(), id.to_string(), &value)
            else {
                continue;
            };

            if let Some(expires_at) = entity.data.get("_expires_at").and_then(Value::as_u64)
                && expires_at <= now
            {
                expired_entities.push((key, entity));
            }
        }

        if expired_entities.is_empty() {
            continue;
        }

        let operation_id = uuid::Uuid::new_v4().to_string();
        let mut batch = storage.batch();
        let mut events = Vec::new();

        for (key, entity) in &expired_entities {
            batch.remove(key.clone());

            let index_mgr = index_manager.read().await;
            index_mgr.remove_indexes(&mut batch, entity);

            events.push(ChangeEvent::delete(entity.name.clone(), entity.id.clone()));
        }

        outbox.enqueue_events(&mut batch, &operation_id, &events);

        if let Err(e) = batch.commit() {
            tracing::error!(err = %e, "TTL cleanup batch commit failed");
            continue;
        }

        for event in events {
            let _ = dispatcher.dispatch(event).await;
        }

        if let Err(e) = outbox.mark_delivered(&operation_id) {
            tracing::warn!(op_id = %operation_id, err = %e, "TTL cleanup mark_delivered failed");
        }

        tracing::debug!(
            count = expired_entities.len(),
            op_id = %operation_id,
            "TTL cleanup processed expired entities"
        );
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(Filter::glob_match("Alice", "*li*"));
        assert!(Filter::glob_match("Charlie", "*li*"));
        assert!(!Filter::glob_match("Bob", "*li*"));
        assert!(!Filter::glob_match("David", "*li*"));

        assert!(Filter::glob_match("test@example.com", "*@example.com"));
        assert!(Filter::glob_match("a@example.com", "*@example.com"));

        assert!(Filter::glob_match("Charlie", "*lie"));
        assert!(!Filter::glob_match("Alice", "*lie"));
    }
}
