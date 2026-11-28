// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Database;
use mqdb_core::entity::Entity;
use mqdb_core::error::Result;
use mqdb_core::keys;
use mqdb_core::relationship::Relationship;
use mqdb_core::schema::Schema;

impl Database {
    /// # Errors
    /// Returns an error if persisting the schema fails.
    #[tracing::instrument(skip(self, schema), fields(entity = %schema.entity))]
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

    pub async fn get_schema(&self, entity: &str) -> Option<Schema> {
        let registry = self.schema_registry.read().await;
        registry.get_schema(entity).cloned()
    }

    /// # Errors
    /// Returns an error if any index field doesn't exist in the entity's schema.
    #[tracing::instrument(skip(self), fields(entity = %entity))]
    pub async fn add_index(&self, entity: String, fields: Vec<String>) -> Result<()> {
        let schema_registry = self.schema_registry.read().await;
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        schema_registry.validate_fields_exist(&entity, &field_refs, "index")?;
        drop(schema_registry);

        let admin = self.index_admin_lock.lock().await;

        let (merged, previous) = {
            let mut manager = self.index_manager.write().await;
            let previous = manager.definition_snapshot(&entity);
            let merged = manager.merged_definition(&entity, fields);
            manager.add_index(merged.clone());
            (merged, previous)
        };

        if let Err(e) = self.backfill_then_persist_index(&entity, &merged).await {
            let mut manager = self.index_manager.write().await;
            manager.restore_definition(&entity, previous);
            return Err(e);
        }

        drop(admin);

        Ok(())
    }

    async fn backfill_then_persist_index(
        &self,
        entity: &str,
        merged: &mqdb_core::index::IndexDefinition,
    ) -> Result<()> {
        let prefix = format!("data/{entity}/");
        let mut after_key: Option<Vec<u8>> = None;
        loop {
            let batch_items =
                self.storage
                    .prefix_scan_batch(prefix.as_bytes(), 1000, after_key.as_deref())?;
            if batch_items.is_empty() {
                break;
            }
            let mut write_batch = self.storage.batch();
            let manager = self.index_manager.read().await;
            for (key, value) in &batch_items {
                if let Ok((_, id)) = keys::decode_data_key(key)
                    && let Ok(entity_obj) = Entity::deserialize(entity.to_string(), id, value)
                {
                    manager.update_indexes(&mut write_batch, &entity_obj, None);
                }
            }
            drop(manager);
            write_batch.commit()?;
            after_key = batch_items.last().map(|(k, _)| k.clone());
        }

        let mut batch = self.storage.batch();
        mqdb_core::index::IndexManager::persist_index(&mut batch, merged)?;
        batch.commit()?;

        Ok(())
    }

    /// Register an index only if the entity is not already indexed on all of `fields`.
    /// Idempotent: skips the reindex scan when the index already exists.
    ///
    /// # Errors
    /// Returns an error if field validation or persisting the index fails.
    pub async fn ensure_index(&self, entity: String, fields: Vec<String>) -> Result<()> {
        {
            let manager = self.index_manager.read().await;
            if let Some(existing) = manager.get_indexed_fields(&entity)
                && fields.iter().all(|f| existing.contains(f))
            {
                return Ok(());
            }
        }
        self.add_index(entity, fields).await
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

    pub async fn list_relationships(&self, entity: &str) -> Vec<Relationship> {
        let registry = self.relationship_registry.read().await;
        registry.get_all(entity).cloned().unwrap_or_default()
    }

    pub async fn list_constraints(&self, entity: &str) -> Vec<mqdb_core::constraint::Constraint> {
        let manager = self.constraint_manager.read().await;
        manager.get_constraints(entity).to_vec()
    }

    async fn refuse_existing_constraint(
        &self,
        constraint: &mqdb_core::constraint::Constraint,
    ) -> Result<()> {
        let manager = self.constraint_manager.read().await;
        refuse_conflicting(&manager, constraint)
    }

    async fn commit_new_constraint(
        &self,
        mut batch: mqdb_core::BatchWriter,
        constraint: mqdb_core::constraint::Constraint,
    ) -> Result<()> {
        let mut manager = self.constraint_manager.write().await;
        refuse_conflicting(&manager, &constraint)?;
        manager.persist_constraint(&mut batch, &constraint)?;
        batch.commit()?;
        manager.add_constraint(constraint);
        Ok(())
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(entity = %entity))]
    pub async fn add_unique_constraint(&self, entity: String, fields: Vec<String>) -> Result<()> {
        use mqdb_core::constraint::{Constraint, UniqueConstraint};
        use mqdb_core::error::Error;
        use std::collections::HashSet;

        if fields.is_empty() || fields.iter().any(String::is_empty) {
            return Err(Error::Validation(
                "a unique constraint needs at least one field and no empty field names".into(),
            ));
        }
        let constraint = Constraint::Unique(UniqueConstraint::new(entity.clone(), fields.clone()));
        self.refuse_existing_constraint(&constraint).await?;

        self.add_index(entity.clone(), fields.clone()).await?;

        let mut batch = self.storage.batch();

        let prefix = keys::encode_data_key(&entity, "");
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for (key, value) in self.storage.prefix_scan(&prefix)? {
            let (_, id) = keys::decode_data_key(&key)?;
            let record = Entity::deserialize(entity.clone(), id, &value)?;
            for field in &fields {
                if let Some(v) = record.get_field(field) {
                    let value_bytes = keys::encode_value_for_index(v)?;
                    let guard = keys::encode_unique_guard_key(&entity, field, &value_bytes);
                    if !seen.insert(guard.clone()) {
                        return Err(Error::UniqueViolation {
                            entity: entity.clone(),
                            field: field.clone(),
                            value: String::from_utf8_lossy(&value_bytes).to_string(),
                        });
                    }
                    batch.insert(guard, record.id.as_bytes().to_vec());
                }
            }
        }

        self.commit_new_constraint(batch, constraint).await
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(entity = %entity, field = %field))]
    pub async fn add_not_null(&self, entity: String, field: String) -> Result<()> {
        use mqdb_core::constraint::{Constraint, NotNullConstraint};

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_fields_exist(&entity, &[field.as_str()], "not-null constraint")?;
        drop(schema_registry);

        let constraint = Constraint::NotNull(NotNullConstraint::new(entity, field));

        self.commit_new_constraint(self.storage.batch(), constraint)
            .await
    }

    /// # Errors
    /// Returns an error if field validation or persisting the constraint fails.
    #[tracing::instrument(skip(self), fields(source = %source_entity, target = %target_entity))]
    pub async fn add_foreign_key(
        &self,
        source_entity: String,
        source_field: String,
        target_entity: String,
        target_field: String,
        on_delete: mqdb_core::constraint::OnDeleteAction,
    ) -> Result<()> {
        use mqdb_core::constraint::{Constraint, ForeignKeyConstraint, OnDeleteAction};
        use mqdb_core::error::Error;
        use mqdb_core::schema::FieldType;

        let schema_registry = self.schema_registry.read().await;
        schema_registry.validate_fields_exist(
            &source_entity,
            &[source_field.as_str()],
            "foreign key",
        )?;
        schema_registry.validate_fields_exist(
            &target_entity,
            &[target_field.as_str()],
            "foreign key",
        )?;

        if on_delete == OnDeleteAction::SetNull
            && let Some(schema) = schema_registry.get_schema(&source_entity)
            && let Some(field_def) = schema.fields.get(&source_field)
            && field_def.field_type != FieldType::Null
        {
            let field_type = field_def.field_type.clone();
            drop(schema_registry);
            return Err(Error::SchemaViolation {
                entity: source_entity.clone(),
                field: source_field.clone(),
                reason: format!(
                    "on_delete=set_null requires a nullable (null-typed) field; '{source_field}' is typed {field_type:?}"
                ),
            });
        }
        drop(schema_registry);

        let constraint = Constraint::ForeignKey(ForeignKeyConstraint::new(
            source_entity,
            source_field,
            target_entity,
            target_field,
            on_delete,
        ));

        self.commit_new_constraint(self.storage.batch(), constraint)
            .await
    }
}

fn refuse_conflicting(
    manager: &mqdb_core::constraint::ConstraintManager,
    constraint: &mqdb_core::constraint::Constraint,
) -> Result<()> {
    use mqdb_core::constraint::{Constraint, OnDeleteAction};
    use mqdb_core::error::Error;

    let existing = manager.get_constraints(constraint.entity());
    if existing.iter().any(|c| c.name() == constraint.name()) {
        return Err(Error::AlreadyExists {
            entity: constraint.entity().to_string(),
            id: constraint.name().to_string(),
        });
    }
    match constraint {
        Constraint::NotNull(not_null)
            if existing.iter().any(|c| {
                matches!(c, Constraint::ForeignKey(fk)
                    if fk.source_field == not_null.field && fk.on_delete == OnDeleteAction::SetNull)
            }) =>
        {
            Err(Error::ConstraintViolation(format!(
                "cannot add not-null constraint on {}.{}: a set-null foreign key exists on the field",
                not_null.entity, not_null.field
            )))
        }
        Constraint::ForeignKey(fk)
            if fk.on_delete == OnDeleteAction::SetNull
                && existing.iter().any(
                    |c| matches!(c, Constraint::NotNull(nn) if nn.field == fk.source_field),
                ) =>
        {
            Err(Error::ConstraintViolation(format!(
                "cannot set on_delete=set_null on {}.{}: a not-null constraint exists on the field",
                fk.source_entity, fk.source_field
            )))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use mqdb_core::config::DatabaseConfig;
    use mqdb_core::constraint::{Constraint, OnDeleteAction};
    use mqdb_core::error::{Error, Result};
    use mqdb_core::keys;
    use mqdb_core::storage::{BatchOperations, MemoryBackend, StorageBackend};
    use mqdb_core::types::ScopeConfig;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailingBackend {
        inner: MemoryBackend,
        fail_commit: Arc<AtomicBool>,
    }

    impl StorageBackend for FailingBackend {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get(key)
        }
        fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.inner.insert(key, value)
        }
        fn remove(&self, key: &[u8]) -> Result<()> {
            self.inner.remove(key)
        }
        fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.prefix_scan(prefix)
        }
        fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
            self.inner.prefix_count(prefix)
        }
        fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            self.inner.prefix_scan_keys(prefix)
        }
        fn prefix_scan_batch(
            &self,
            prefix: &[u8],
            batch_size: usize,
            after_key: Option<&[u8]>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.prefix_scan_batch(prefix, batch_size, after_key)
        }
        fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.range_scan(start, end)
        }
        fn batch(&self) -> Box<dyn BatchOperations> {
            Box::new(FailingBatch {
                inner: self.inner.batch(),
                fail_commit: Arc::clone(&self.fail_commit),
            })
        }
        fn flush(&self) -> Result<()> {
            self.inner.flush()
        }
    }

    struct FailingBatch {
        inner: Box<dyn BatchOperations>,
        fail_commit: Arc<AtomicBool>,
    }

    impl BatchOperations for FailingBatch {
        fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.inner.insert(key, value);
        }
        fn remove(&mut self, key: Vec<u8>) {
            self.inner.remove(key);
        }
        fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
            self.inner.expect_value(key, expected_value);
        }
        fn expect_absent(&mut self, key: Vec<u8>) {
            self.inner.expect_absent(key);
        }
        fn commit(self: Box<Self>) -> Result<()> {
            if self.fail_commit.load(Ordering::SeqCst) {
                return Err(Error::Internal("injected commit failure".to_string()));
            }
            self.inner.commit()
        }
    }

    #[tokio::test]
    async fn add_index_commit_failure_leaves_registry_consistent() {
        let fail = Arc::new(AtomicBool::new(false));
        let backend = Arc::new(FailingBackend {
            inner: MemoryBackend::new(),
            fail_commit: Arc::clone(&fail),
        });
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        let db = Database::open_with_backend(backend, config).await.unwrap();

        db.add_index("users".to_string(), vec!["email".to_string()])
            .await
            .unwrap();

        fail.store(true, Ordering::SeqCst);
        let result = db
            .add_index("users".to_string(), vec!["username".to_string()])
            .await;
        assert!(result.is_err(), "add_index must fail when its commit fails");
        fail.store(false, Ordering::SeqCst);

        db.create(
            "users".to_string(),
            json!({ "id": "u1", "email": "a@x.com", "username": "alice" }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();

        let email_value = keys::encode_value_for_index(&json!("a@x.com")).unwrap();
        let email_key = keys::encode_index_key("users", "email", &email_value, "u1");
        assert!(
            db.storage.get(&email_key).unwrap().is_some(),
            "email must remain indexed after the failed add_index"
        );
        let username_value = keys::encode_value_for_index(&json!("alice")).unwrap();
        let username_key = keys::encode_index_key("users", "username", &username_value, "u1");
        assert!(
            db.storage.get(&username_key).unwrap().is_none(),
            "username must not be indexed after a failed add_index commit"
        );

        db.add_index("users".to_string(), vec!["username".to_string()])
            .await
            .unwrap();
        db.create(
            "users".to_string(),
            json!({ "id": "u2", "email": "b@x.com", "username": "bob" }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
        let bob_value = keys::encode_value_for_index(&json!("bob")).unwrap();
        let bob_key = keys::encode_index_key("users", "username", &bob_value, "u2");
        assert!(
            db.storage.get(&bob_key).unwrap().is_some(),
            "username must be indexed after a successful re-add"
        );
    }

    #[tokio::test]
    async fn add_index_crash_during_reindex_leaves_field_unadvertised() {
        use mqdb_core::index::IndexManager;

        let fail = Arc::new(AtomicBool::new(false));
        let backend = Arc::new(FailingBackend {
            inner: MemoryBackend::new(),
            fail_commit: Arc::clone(&fail),
        });
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        let db = Database::open_with_backend(backend, config).await.unwrap();

        db.add_index("users".to_string(), vec!["email".to_string()])
            .await
            .unwrap();
        for (id, email, username) in [("u1", "a@x.com", "alice"), ("u2", "b@x.com", "bob")] {
            db.create(
                "users".to_string(),
                json!({ "id": id, "email": email, "username": username }),
                None,
                None,
                None,
                &ScopeConfig::default(),
            )
            .await
            .unwrap();
        }

        fail.store(true, Ordering::SeqCst);
        let result = db
            .add_index("users".to_string(), vec!["username".to_string()])
            .await;
        assert!(
            result.is_err(),
            "add_index must fail when a reindex or persist commit fails"
        );
        fail.store(false, Ordering::SeqCst);

        let mut reloaded = IndexManager::new();
        reloaded.load_indexes(&db.storage).unwrap();
        let fields = reloaded
            .get_indexed_fields("users")
            .cloned()
            .unwrap_or_default();
        assert!(
            fields.contains(&"email".to_string()),
            "email must stay advertised after the crash"
        );
        assert!(
            !fields.contains(&"username".to_string()),
            "username must be unadvertised on reload, forcing a full-scan fallback instead of silently-incomplete index results"
        );

        db.create(
            "users".to_string(),
            json!({ "id": "u3", "email": "c@x.com", "username": "carol" }),
            None,
            None,
            None,
            &ScopeConfig::default(),
        )
        .await
        .unwrap();
        let carol_value = keys::encode_value_for_index(&json!("carol")).unwrap();
        let carol_key = keys::encode_index_key("users", "username", &carol_value, "u3");
        assert!(
            db.storage.get(&carol_key).unwrap().is_none(),
            "the in-memory registry must roll back so a failed add_index does not index new writes"
        );

        db.add_index("users".to_string(), vec!["username".to_string()])
            .await
            .unwrap();
        let alice_value = keys::encode_value_for_index(&json!("alice")).unwrap();
        let alice_key = keys::encode_index_key("users", "username", &alice_value, "u1");
        assert!(
            db.storage.get(&alice_key).unwrap().is_some(),
            "a later successful add_index backfills pre-existing rows"
        );
    }

    #[tokio::test]
    async fn concurrent_add_index_same_entity_keeps_memory_and_disk_consistent() {
        use mqdb_core::index::IndexManager;

        let backend = Arc::new(MemoryBackend::new());
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        let db = Database::open_with_backend(backend, config).await.unwrap();

        for (id, email, username) in [("u1", "a@x.com", "alice"), ("u2", "b@x.com", "bob")] {
            db.create(
                "users".to_string(),
                json!({ "id": id, "email": email, "username": username }),
                None,
                None,
                None,
                &ScopeConfig::default(),
            )
            .await
            .unwrap();
        }

        let (r1, r2) = tokio::join!(
            db.add_index("users".to_string(), vec!["email".to_string()]),
            db.add_index("users".to_string(), vec!["username".to_string()]),
        );
        r1.unwrap();
        r2.unwrap();

        let mem_fields = db
            .index_manager
            .read()
            .await
            .get_indexed_fields("users")
            .cloned()
            .unwrap_or_default();
        let mut reloaded = IndexManager::new();
        reloaded.load_indexes(&db.storage).unwrap();
        let disk_fields = reloaded
            .get_indexed_fields("users")
            .cloned()
            .unwrap_or_default();

        for field in ["email", "username"] {
            assert!(
                mem_fields.contains(&field.to_string()),
                "in-memory definition must advertise {field} after concurrent adds, got {mem_fields:?}"
            );
            assert!(
                disk_fields.contains(&field.to_string()),
                "on-disk definition must persist {field} after concurrent adds, got {disk_fields:?}"
            );
        }
    }

    async fn open_memory_db(backend: Arc<MemoryBackend>) -> Database {
        let config = DatabaseConfig::new(PathBuf::from("mem")).without_background_tasks();
        Database::open_with_backend(backend, config).await.unwrap()
    }

    fn unique_field_sets(constraints: &[Constraint]) -> Vec<Vec<String>> {
        let mut sets: Vec<Vec<String>> = constraints
            .iter()
            .filter_map(|c| match c {
                Constraint::Unique(u) => Some(u.fields.clone()),
                _ => None,
            })
            .collect();
        sets.sort();
        sets
    }

    fn foreign_key_targets(constraints: &[Constraint]) -> Vec<(String, String)> {
        let mut targets: Vec<(String, String)> = constraints
            .iter()
            .filter_map(|c| match c {
                Constraint::ForeignKey(fk) => {
                    Some((fk.source_field.clone(), fk.target_entity.clone()))
                }
                _ => None,
            })
            .collect();
        targets.sort();
        targets
    }

    #[tokio::test]
    async fn distinct_unique_constraints_with_underscored_fields_both_persist() {
        let backend = Arc::new(MemoryBackend::new());
        let db = open_memory_db(Arc::clone(&backend)).await;

        db.add_unique_constraint("items".to_string(), vec!["a_b".to_string()])
            .await
            .unwrap();
        db.add_unique_constraint("items".to_string(), vec!["a".to_string(), "b".to_string()])
            .await
            .unwrap();

        let expected = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["a_b".to_string()],
        ];
        assert_eq!(
            unique_field_sets(&db.list_constraints("items").await),
            expected
        );
        drop(db);

        let reopened = open_memory_db(backend).await;
        assert_eq!(
            unique_field_sets(&reopened.list_constraints("items").await),
            expected,
            "both unique constraints must survive a reload"
        );
    }

    #[tokio::test]
    async fn distinct_foreign_keys_with_underscored_names_both_persist() {
        let backend = Arc::new(MemoryBackend::new());
        let db = open_memory_db(Arc::clone(&backend)).await;

        db.add_foreign_key(
            "items".to_string(),
            "x_y".to_string(),
            "z".to_string(),
            "id".to_string(),
            OnDeleteAction::Restrict,
        )
        .await
        .unwrap();
        db.add_foreign_key(
            "items".to_string(),
            "x".to_string(),
            "y_z".to_string(),
            "id".to_string(),
            OnDeleteAction::Restrict,
        )
        .await
        .unwrap();

        let expected = vec![
            ("x".to_string(), "y_z".to_string()),
            ("x_y".to_string(), "z".to_string()),
        ];
        assert_eq!(
            foreign_key_targets(&db.list_constraints("items").await),
            expected
        );
        drop(db);

        let reopened = open_memory_db(backend).await;
        assert_eq!(
            foreign_key_targets(&reopened.list_constraints("items").await),
            expected,
            "both foreign keys must survive a reload"
        );
    }

    #[tokio::test]
    async fn constraint_stored_under_legacy_name_is_renamed_and_refuses_a_duplicate() {
        let backend = Arc::new(MemoryBackend::new());
        let legacy = json!({"ForeignKey": {
            "source_entity": "posts",
            "source_field": "author_id",
            "target_entity": "users",
            "target_field": "id",
            "on_delete": "Cascade",
            "name": "posts_author_id_users_fk"
        }});
        let legacy_key = keys::encode_constraint_key("fk", "posts", "posts_author_id_users_fk");
        backend
            .insert(&legacy_key, &serde_json::to_vec(&legacy).unwrap())
            .unwrap();

        let db = open_memory_db(Arc::clone(&backend)).await;
        assert_eq!(
            foreign_key_targets(&db.list_constraints("posts").await),
            vec![("author_id".to_string(), "users".to_string())]
        );
        assert_eq!(
            (
                backend.get(&legacy_key).unwrap().is_some(),
                backend
                    .get(&keys::encode_constraint_key(
                        "fk",
                        "posts",
                        "posts_author%5Fid_users_fk"
                    ))
                    .unwrap()
                    .is_some()
            ),
            (false, true),
            "legacy key, renamed key"
        );

        let refused = db
            .add_foreign_key(
                "posts".to_string(),
                "author_id".to_string(),
                "users".to_string(),
                "id".to_string(),
                OnDeleteAction::Cascade,
            )
            .await;
        assert!(
            matches!(refused, Err(Error::AlreadyExists { .. })),
            "re-adding a constraint stored under its legacy name must be refused, got {refused:?}"
        );
        assert_eq!(db.list_constraints("posts").await.len(), 1);
    }

    #[tokio::test]
    async fn a_unique_constraint_without_fields_or_with_an_empty_field_is_refused() {
        let db = open_memory_db(Arc::new(MemoryBackend::new())).await;
        for fields in [vec![], vec![String::new()]] {
            let refused = db.add_unique_constraint("items".to_string(), fields).await;
            assert!(
                matches!(refused, Err(Error::Validation(_))),
                "got {refused:?}"
            );
        }
        assert!(db.list_constraints("items").await.is_empty());
    }

    #[tokio::test]
    async fn a_constraint_stored_under_both_names_loads_once() {
        let backend = Arc::new(MemoryBackend::new());
        let value = |name: &str| {
            serde_json::to_vec(&json!({"Unique": {
                "entity": "items",
                "fields": ["a_b"],
                "name": name
            }}))
            .unwrap()
        };
        let legacy_key = keys::encode_constraint_key("unique", "items", "items_a_b_unique");
        backend
            .insert(&legacy_key, &value("items_a_b_unique"))
            .unwrap();
        backend
            .insert(
                &keys::encode_constraint_key("unique", "items", "items_a%5Fb_unique"),
                &value("items_a%5Fb_unique"),
            )
            .unwrap();

        let db = open_memory_db(Arc::clone(&backend)).await;
        assert_eq!(
            (
                db.list_constraints("items").await.len(),
                backend.get(&legacy_key).unwrap().is_some()
            ),
            (1, false),
            "loaded constraints, legacy key still stored"
        );
    }

    #[tokio::test]
    async fn a_legacy_name_does_not_block_the_constraint_that_now_owns_it() {
        let backend = Arc::new(MemoryBackend::new());
        let legacy = json!({"Unique": {
            "entity": "items",
            "fields": ["a_b"],
            "name": "items_a_b_unique"
        }});
        backend
            .insert(
                &keys::encode_constraint_key("unique", "items", "items_a_b_unique"),
                &serde_json::to_vec(&legacy).unwrap(),
            )
            .unwrap();

        let db = open_memory_db(Arc::clone(&backend)).await;
        db.add_unique_constraint("items".to_string(), vec!["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        drop(db);

        let reopened = open_memory_db(backend).await;
        assert_eq!(
            unique_field_sets(&reopened.list_constraints("items").await),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["a_b".to_string()],
            ]
        );
    }
}
