use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub async fn create_pool(pg_config: &str, pool_size: usize) -> Result<Pool, BoxError> {
    let mut cfg = Config::new();
    cfg.url = Some(pg_config.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(pool_size));

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let client = pool.get().await?;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                entity  TEXT NOT NULL,
                data    JSONB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_records_entity ON records(entity);",
        )
        .await?;

    tracing::info!("PostgreSQL pool created (size={pool_size}), schema initialized");
    Ok(pool)
}

pub async fn insert_record(
    pool: &Pool,
    id: &str,
    entity: &str,
    data: &serde_json::Value,
) -> Result<(), BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("INSERT INTO records (id, entity, data) VALUES ($1, $2, $3)")
        .await?;
    client.execute(&stmt, &[&id, &entity, &data]).await?;
    Ok(())
}

pub async fn get_record(
    pool: &Pool,
    entity: &str,
    id: &str,
) -> Result<Option<serde_json::Value>, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("SELECT data FROM records WHERE id = $1 AND entity = $2")
        .await?;
    let row = client.query_opt(&stmt, &[&id, &entity]).await?;
    Ok(row.map(|r| r.get::<_, serde_json::Value>(0)))
}

pub async fn update_record(
    pool: &Pool,
    entity: &str,
    id: &str,
    data: &serde_json::Value,
) -> Result<bool, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("UPDATE records SET data = $1 WHERE id = $2 AND entity = $3")
        .await?;
    let rows = client.execute(&stmt, &[&data, &id, &entity]).await?;
    Ok(rows > 0)
}

pub async fn delete_record(
    pool: &Pool,
    entity: &str,
    id: &str,
) -> Result<bool, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("DELETE FROM records WHERE id = $1 AND entity = $2")
        .await?;
    let rows = client.execute(&stmt, &[&id, &entity]).await?;
    Ok(rows > 0)
}

pub async fn list_records(
    pool: &Pool,
    entity: &str,
    limit: i64,
) -> Result<Vec<serde_json::Value>, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("SELECT data FROM records WHERE entity = $1 LIMIT $2")
        .await?;
    let rows = client.query(&stmt, &[&entity, &limit]).await?;
    Ok(rows.iter().map(|r| r.get::<_, serde_json::Value>(0)).collect())
}

pub async fn check_unique(
    pool: &Pool,
    entity: &str,
    fields: &[String],
    values: &[serde_json::Value],
    excluding_id: Option<&str>,
) -> Result<(), BoxError> {
    if fields.is_empty() {
        return Ok(());
    }
    let client = pool.get().await?;
    let mut sql = String::from("SELECT id FROM records WHERE entity = $1");
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(entity.to_string())];
    for (i, (field, value)) in fields.iter().zip(values.iter()).enumerate() {
        let idx = i + 2;
        sql.push_str(&format!(" AND data->>'{field}' = ${idx}"));
        let as_str = value.as_str().map(ToString::to_string).unwrap_or_else(|| value.to_string());
        params.push(Box::new(as_str));
    }
    sql.push_str(" LIMIT 1");
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let rows = client.query(sql.as_str(), &param_refs[..]).await?;
    if let Some(row) = rows.first() {
        let existing_id: String = row.get(0);
        if Some(existing_id.as_str()) != excluding_id {
            return Err("unique constraint violation".into());
        }
    }
    Ok(())
}

pub async fn check_fk(
    pool: &Pool,
    target_entity: &str,
    target_field: &str,
    value: &serde_json::Value,
) -> Result<(), BoxError> {
    let client = pool.get().await?;
    let sql = format!(
        "SELECT 1 FROM records WHERE entity = $1 AND data->>'{target_field}' = $2 LIMIT 1"
    );
    let as_str = value.as_str().map(ToString::to_string).unwrap_or_else(|| value.to_string());
    let rows = client
        .query(sql.as_str(), &[&target_entity, &as_str])
        .await?;
    if rows.is_empty() {
        return Err("foreign key violation".into());
    }
    Ok(())
}

pub async fn find_children(
    pool: &Pool,
    child_entity: &str,
    child_field: &str,
    parent_id: &str,
) -> Result<Vec<(String, String)>, BoxError> {
    let client = pool.get().await?;
    let sql = format!(
        "SELECT id FROM records WHERE entity = $1 AND data->>'{child_field}' = $2"
    );
    let rows = client.query(sql.as_str(), &[&child_entity, &parent_id]).await?;
    Ok(rows
        .iter()
        .map(|r| (child_entity.to_string(), r.get::<_, String>(0)))
        .collect())
}
