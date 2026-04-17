use std::collections::HashMap;
use std::sync::Arc;

use deadpool_postgres::Pool;
use redis::aio::MultiplexedConnection;
use serde_json::Value;
use tokio::sync::RwLock;

use super::{BoxError, pg, redis_store};

#[derive(Default, Clone)]
pub struct ConstraintRegistry {
    pub unique_fields: HashMap<String, Vec<Vec<String>>>,
    pub foreign_keys: HashMap<String, Vec<ForeignKey>>,
}

#[derive(Clone)]
pub struct ForeignKey {
    pub child_entity: String,
    pub child_field: String,
    pub target_entity: String,
    pub target_field: String,
    pub on_delete: String,
}

#[derive(Clone)]
pub enum Store {
    Pg {
        pool: Pool,
        constraints: Arc<RwLock<ConstraintRegistry>>,
    },
    Redis {
        conn: MultiplexedConnection,
        constraints: Arc<RwLock<ConstraintRegistry>>,
    },
}

impl Store {
    pub fn pg(pool: Pool) -> Self {
        Self::Pg {
            pool,
            constraints: Arc::new(RwLock::new(ConstraintRegistry::default())),
        }
    }

    pub fn redis(conn: MultiplexedConnection) -> Self {
        Self::Redis {
            conn,
            constraints: Arc::new(RwLock::new(ConstraintRegistry::default())),
        }
    }

    fn constraints(&self) -> &Arc<RwLock<ConstraintRegistry>> {
        match self {
            Self::Pg { constraints, .. } | Self::Redis { constraints, .. } => constraints,
        }
    }

    pub async fn insert_record(&self, id: &str, entity: &str, data: &Value) -> Result<(), BoxError> {
        let registry = self.constraints().read().await;
        if let Some(uniques) = registry.unique_fields.get(entity) {
            for field_set in uniques {
                check_unique(self, entity, field_set, data, None).await?;
            }
        }
        if let Some(fks) = registry.foreign_keys.get(entity) {
            for fk in fks {
                check_fk_target(self, &fk.target_entity, &fk.target_field, fk.child_field.as_str(), data).await?;
            }
        }
        drop(registry);
        match self {
            Self::Pg { pool, .. } => pg::insert_record(pool, id, entity, data).await,
            Self::Redis { conn, .. } => redis_store::insert_record(conn, id, entity, data).await,
        }
    }

    pub async fn get_record(&self, entity: &str, id: &str) -> Result<Option<Value>, BoxError> {
        match self {
            Self::Pg { pool, .. } => pg::get_record(pool, entity, id).await,
            Self::Redis { conn, .. } => redis_store::get_record(conn, entity, id).await,
        }
    }

    pub async fn update_record(&self, entity: &str, id: &str, data: &Value) -> Result<bool, BoxError> {
        let registry = self.constraints().read().await;
        if let Some(uniques) = registry.unique_fields.get(entity) {
            for field_set in uniques {
                check_unique(self, entity, field_set, data, Some(id)).await?;
            }
        }
        drop(registry);
        match self {
            Self::Pg { pool, .. } => pg::update_record(pool, entity, id, data).await,
            Self::Redis { conn, .. } => redis_store::update_record(conn, entity, id, data).await,
        }
    }

    pub async fn delete_with_cascade(
        &self,
        entity: &str,
        id: &str,
    ) -> Result<Vec<(String, String)>, BoxError> {
        let mut visited: Vec<(String, String)> = Vec::new();
        let mut stack: Vec<(String, String)> = vec![(entity.to_string(), id.to_string())];

        while let Some((cur_entity, cur_id)) = stack.pop() {
            let registry = self.constraints().read().await;
            let child_fks: Vec<ForeignKey> = registry
                .foreign_keys
                .iter()
                .flat_map(|(_, v)| v.iter().cloned())
                .filter(|fk| fk.target_entity == cur_entity && fk.on_delete == "cascade")
                .collect();
            drop(registry);

            for fk in child_fks {
                let children = self
                    .find_children(&fk.child_entity, &fk.child_field, &cur_id)
                    .await?;
                for (child_entity, child_id) in children {
                    stack.push((child_entity, child_id));
                }
            }

            let deleted = match self {
                Self::Pg { pool, .. } => pg::delete_record(pool, &cur_entity, &cur_id).await?,
                Self::Redis { conn, .. } => {
                    redis_store::delete_record(conn, &cur_entity, &cur_id).await?
                }
            };
            if deleted {
                visited.push((cur_entity, cur_id));
            }
        }

        Ok(visited)
    }

    async fn find_children(
        &self,
        child_entity: &str,
        child_field: &str,
        parent_id: &str,
    ) -> Result<Vec<(String, String)>, BoxError> {
        match self {
            Self::Pg { pool, .. } => pg::find_children(pool, child_entity, child_field, parent_id).await,
            Self::Redis { conn, .. } => {
                redis_store::find_children(conn, child_entity, child_field, parent_id).await
            }
        }
    }

    pub async fn list_records(&self, entity: &str, limit: i64) -> Result<Vec<Value>, BoxError> {
        match self {
            Self::Pg { pool, .. } => pg::list_records(pool, entity, limit).await,
            Self::Redis { conn, .. } => redis_store::list_records(conn, entity, limit).await,
        }
    }

    pub async fn add_unique_constraint(&self, entity: &str, fields: &[String]) -> Result<(), BoxError> {
        let mut registry = self.constraints().write().await;
        registry
            .unique_fields
            .entry(entity.to_string())
            .or_default()
            .push(fields.to_vec());
        Ok(())
    }

    pub async fn add_fk_constraint(
        &self,
        entity: &str,
        field: &str,
        target_entity: &str,
        target_field: &str,
        on_delete: &str,
    ) -> Result<(), BoxError> {
        let mut registry = self.constraints().write().await;
        registry
            .foreign_keys
            .entry(entity.to_string())
            .or_default()
            .push(ForeignKey {
                child_entity: entity.to_string(),
                child_field: field.to_string(),
                target_entity: target_entity.to_string(),
                target_field: target_field.to_string(),
                on_delete: on_delete.to_string(),
            });
        Ok(())
    }
}

async fn check_unique(
    store: &Store,
    entity: &str,
    fields: &[String],
    data: &Value,
    excluding_id: Option<&str>,
) -> Result<(), BoxError> {
    if fields.is_empty() {
        return Ok(());
    }
    let values: Vec<Value> = fields
        .iter()
        .map(|f| data.get(f).cloned().unwrap_or(Value::Null))
        .collect();
    match store {
        Store::Pg { pool, .. } => {
            pg::check_unique(pool, entity, fields, &values, excluding_id).await
        }
        Store::Redis { conn, .. } => {
            redis_store::check_unique(conn, entity, fields, &values, excluding_id).await
        }
    }
}

async fn check_fk_target(
    store: &Store,
    target_entity: &str,
    target_field: &str,
    child_field: &str,
    data: &Value,
) -> Result<(), BoxError> {
    let Some(value) = data.get(child_field) else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    match store {
        Store::Pg { pool, .. } => pg::check_fk(pool, target_entity, target_field, value).await,
        Store::Redis { conn, .. } => {
            redis_store::check_fk(conn, target_entity, target_field, value).await
        }
    }
}
