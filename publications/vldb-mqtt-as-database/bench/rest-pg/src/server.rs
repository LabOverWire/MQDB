use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use std::fmt::Write as _;

use deadpool_postgres::{Config, Pool, Runtime};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{RwLock, broadcast};
use tokio_postgres::{AsyncMessage, NoTls};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Default, Clone)]
struct ConstraintRegistry {
    unique_fields: HashMap<String, Vec<Vec<String>>>,
    foreign_keys: HashMap<String, Vec<ForeignKey>>,
}

#[derive(Clone)]
struct ForeignKey {
    child_entity: String,
    child_field: String,
    target_entity: String,
    target_field: String,
    on_delete: String,
}

struct AppState {
    pool: Pool,
    constraints: Arc<RwLock<ConstraintRegistry>>,
    notify_tx: broadcast::Sender<i64>,
}

pub async fn run(bind: &str, pg_url: &str, pool_size: usize) -> Result<(), BoxError> {
    let pool = create_pool(pg_url, pool_size).await?;

    let (notify_tx, _) = broadcast::channel::<i64>(1024);
    spawn_listener(pg_url.to_string(), notify_tx.clone()).await?;

    let state = Arc::new(AppState {
        pool,
        constraints: Arc::new(RwLock::new(ConstraintRegistry::default())),
        notify_tx,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/db/{entity}", post(create_record))
        .route("/db/{entity}", get(list_records))
        .route("/db/{entity}/since/{cursor}", get(since_records))
        .route("/db/{entity}/constraint", post(add_constraint))
        .route("/db/{entity}/{id}", get(get_record))
        .route("/db/{entity}/{id}", put(update_record))
        .route("/db/{entity}/{id}", delete(delete_record))
        .route("/db/{entity}/clear", post(clear_entity))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("REST server listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn create_pool(pg_url: &str, pool_size: usize) -> Result<Pool, BoxError> {
    let mut cfg = Config::new();
    cfg.url = Some(pg_url.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(pool_size));

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let client = pool.get().await?;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                entity  TEXT NOT NULL,
                data    JSONB NOT NULL,
                seq     BIGSERIAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_records_entity ON records(entity);
            CREATE INDEX IF NOT EXISTS idx_records_seq ON records(seq);

            CREATE OR REPLACE FUNCTION notify_records_change() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('records_changes', NEW.seq::text);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS records_notify ON records;
            CREATE TRIGGER records_notify
                AFTER INSERT OR UPDATE ON records
                FOR EACH ROW EXECUTE FUNCTION notify_records_change();",
        )
        .await?;

    tracing::info!("PostgreSQL pool created (size={pool_size}), schema + LISTEN/NOTIFY initialized");
    Ok(pool)
}

async fn spawn_listener(pg_url: String, notify_tx: broadcast::Sender<i64>) -> Result<(), BoxError> {
    let (client, mut connection) = tokio_postgres::connect(&pg_url, NoTls).await?;

    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel();
    let stream = futures_util::stream::poll_fn(move |cx| connection.poll_message(cx));
    tokio::spawn(async move {
        futures_util::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            if let Ok(msg) = item {
                let _ = msg_tx.send(msg);
            }
        }
    });

    client.batch_execute("LISTEN records_changes").await?;

    tokio::spawn(async move {
        let _client = client;
        while let Some(msg) = msg_rx.recv().await {
            if let AsyncMessage::Notification(n) = msg
                && let Ok(seq) = n.payload().parse::<i64>()
            {
                let _ = notify_tx.send(seq);
            }
        }
    });

    Ok(())
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "ok",
            "data": {"status": "healthy", "ready": true, "mode": "rest-pg"}
        })),
    )
}

async fn create_record(
    State(state): State<Arc<AppState>>,
    Path(entity): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"status": "error", "code": 400, "message": e.to_string()})),
            );
        }
    };

    let mut record = match data {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    let id = if let Some(existing) = record.get("id").and_then(Value::as_str) {
        existing.to_string()
    } else {
        let generated = uuid::Uuid::now_v7().to_string();
        record.insert("id".to_string(), json!(generated.clone()));
        generated
    };
    let full_record = Value::Object(record);

    if let Err(e) = check_constraints_insert(&state, &entity, &full_record).await {
        return (
            StatusCode::CONFLICT,
            axum::Json(json!({"status": "error", "code": 409, "message": e.to_string()})),
        );
    }

    match insert_pg(&state.pool, &id, &entity, &full_record).await {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(json!({"status": "ok", "data": full_record})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

async fn get_record(
    State(state): State<Arc<AppState>>,
    Path((entity, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match get_pg(&state.pool, &entity, &id).await {
        Ok(Some(data)) => (StatusCode::OK, axum::Json(json!({"status": "ok", "data": data}))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"status": "error", "code": 404, "message": "not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

async fn update_record(
    State(state): State<Arc<AppState>>,
    Path((entity, id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"status": "error", "code": 400, "message": e.to_string()})),
            );
        }
    };

    let mut record = match data {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    record.insert("id".to_string(), json!(id));
    let full_record = Value::Object(record);

    if let Err(e) = check_constraints_update(&state, &entity, &id, &full_record).await {
        return (
            StatusCode::CONFLICT,
            axum::Json(json!({"status": "error", "code": 409, "message": e.to_string()})),
        );
    }

    match update_pg(&state.pool, &entity, &id, &full_record).await {
        Ok(true) => (
            StatusCode::OK,
            axum::Json(json!({"status": "ok", "data": full_record})),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"status": "error", "code": 404, "message": "not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

async fn delete_record(
    State(state): State<Arc<AppState>>,
    Path((entity, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match delete_with_cascade(&state, &entity, &id).await {
        Ok(deleted) if !deleted.is_empty() => (
            StatusCode::OK,
            axum::Json(json!({"status": "ok", "data": {"id": id, "deleted": deleted}})),
        ),
        Ok(_) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"status": "error", "code": 404, "message": "not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

#[allow(clippy::cast_possible_wrap)]
async fn list_records(
    State(state): State<Arc<AppState>>,
    Path(entity): Path<String>,
) -> impl IntoResponse {
    match list_pg(&state.pool, &entity, 100).await {
        Ok(records) => (
            StatusCode::OK,
            axum::Json(json!({"status": "ok", "data": records})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

async fn clear_entity(
    State(state): State<Arc<AppState>>,
    Path(entity): Path<String>,
) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
            );
        }
    };
    let stmt = match client
        .prepare_cached("DELETE FROM records WHERE entity = $1")
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
            );
        }
    };
    let rows = client.execute(&stmt, &[&entity]).await.unwrap_or(0);
    (
        StatusCode::OK,
        axum::Json(json!({"status": "ok", "data": {"deleted": rows}})),
    )
}

#[derive(Deserialize)]
struct SinceQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default = "default_wait_ms")]
    wait_ms: u64,
}

fn default_limit() -> i64 {
    100
}

fn default_wait_ms() -> u64 {
    5000
}

async fn since_records(
    State(state): State<Arc<AppState>>,
    Path((entity, cursor)): Path<(String, i64)>,
    Query(q): Query<SinceQuery>,
) -> impl IntoResponse {
    match list_since_pg(&state.pool, &entity, cursor, q.limit).await {
        Ok(rows) if !rows.is_empty() => {
            let next = rows.iter().map(|(seq, _)| *seq).max().unwrap_or(cursor);
            let data: Vec<Value> = rows.into_iter().map(|(_, v)| v).collect();
            (
                StatusCode::OK,
                axum::Json(json!({"status": "ok", "data": data, "cursor": next})),
            )
        }
        Ok(_) => {
            let mut rx = state.notify_tx.subscribe();
            let deadline = tokio::time::sleep(Duration::from_millis(q.wait_ms));
            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    () = &mut deadline => {
                        return (
                            StatusCode::OK,
                            axum::Json(json!({"status": "ok", "data": [], "cursor": cursor})),
                        );
                    }
                    result = rx.recv() => {
                        if result.is_err() {
                            return (
                                StatusCode::OK,
                                axum::Json(json!({"status": "ok", "data": [], "cursor": cursor})),
                            );
                        }
                        match list_since_pg(&state.pool, &entity, cursor, q.limit).await {
                            Ok(rows) if !rows.is_empty() => {
                                let next = rows.iter().map(|(seq, _)| *seq).max().unwrap_or(cursor);
                                let data: Vec<Value> = rows.into_iter().map(|(_, v)| v).collect();
                                return (
                                    StatusCode::OK,
                                    axum::Json(json!({"status": "ok", "data": data, "cursor": next})),
                                );
                            }
                            Ok(_) => {}
                            Err(e) => {
                                return (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
                                );
                            }
                        }
                    }
                }
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"status": "error", "code": 500, "message": e.to_string()})),
        ),
    }
}

async fn add_constraint(
    State(state): State<Arc<AppState>>,
    Path(entity): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"status": "error", "code": 400, "message": e.to_string()})),
            );
        }
    };

    let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
    let mut registry = state.constraints.write().await;
    match kind {
        "unique" => {
            let fields: Vec<String> = payload
                .get("fields")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();
            if fields.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"status": "error", "code": 400, "message": "unique requires fields"})),
                );
            }
            registry
                .unique_fields
                .entry(entity.clone())
                .or_default()
                .push(fields);
            (
                StatusCode::OK,
                axum::Json(json!({"status": "ok", "data": {"entity": entity, "type": "unique"}})),
            )
        }
        "foreign_key" => {
            let field = payload.get("field").and_then(Value::as_str).unwrap_or("").to_string();
            let target_entity = payload.get("target_entity").and_then(Value::as_str).unwrap_or("").to_string();
            let target_field = payload.get("target_field").and_then(Value::as_str).unwrap_or("id").to_string();
            let on_delete = payload.get("on_delete").and_then(Value::as_str).unwrap_or("restrict").to_string();
            if field.is_empty() || target_entity.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"status": "error", "code": 400, "message": "foreign_key requires field and target_entity"})),
                );
            }
            registry
                .foreign_keys
                .entry(entity.clone())
                .or_default()
                .push(ForeignKey {
                    child_entity: entity.clone(),
                    child_field: field,
                    target_entity,
                    target_field,
                    on_delete,
                });
            (
                StatusCode::OK,
                axum::Json(json!({"status": "ok", "data": {"entity": entity, "type": "foreign_key"}})),
            )
        }
        _ => (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"status": "error", "code": 400, "message": "unknown constraint type"})),
        ),
    }
}

async fn check_constraints_insert(
    state: &AppState,
    entity: &str,
    data: &Value,
) -> Result<(), BoxError> {
    let registry = state.constraints.read().await;
    if let Some(uniques) = registry.unique_fields.get(entity) {
        for fields in uniques {
            check_unique_pg(&state.pool, entity, fields, data, None).await?;
        }
    }
    if let Some(fks) = registry.foreign_keys.get(entity) {
        for fk in fks {
            check_fk_pg(&state.pool, &fk.target_entity, &fk.target_field, &fk.child_field, data).await?;
        }
    }
    Ok(())
}

async fn check_constraints_update(
    state: &AppState,
    entity: &str,
    id: &str,
    data: &Value,
) -> Result<(), BoxError> {
    let registry = state.constraints.read().await;
    if let Some(uniques) = registry.unique_fields.get(entity) {
        for fields in uniques {
            check_unique_pg(&state.pool, entity, fields, data, Some(id)).await?;
        }
    }
    Ok(())
}

async fn delete_with_cascade(
    state: &AppState,
    entity: &str,
    id: &str,
) -> Result<Vec<(String, String)>, BoxError> {
    let mut visited: Vec<(String, String)> = Vec::new();
    let mut stack: Vec<(String, String)> = vec![(entity.to_string(), id.to_string())];

    while let Some((cur_entity, cur_id)) = stack.pop() {
        let child_fks: Vec<ForeignKey> = {
            let registry = state.constraints.read().await;
            registry
                .foreign_keys
                .iter()
                .flat_map(|(_, v)| v.iter().cloned())
                .filter(|fk| fk.target_entity == cur_entity && fk.on_delete == "cascade")
                .collect()
        };

        for fk in child_fks {
            let children = find_children_pg(&state.pool, &fk.child_entity, &fk.child_field, &cur_id).await?;
            for (child_entity, child_id) in children {
                stack.push((child_entity, child_id));
            }
        }

        if delete_pg(&state.pool, &cur_entity, &cur_id).await? {
            visited.push((cur_entity, cur_id));
        }
    }

    Ok(visited)
}

async fn insert_pg(pool: &Pool, id: &str, entity: &str, data: &Value) -> Result<(), BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("INSERT INTO records (id, entity, data) VALUES ($1, $2, $3)")
        .await?;
    client.execute(&stmt, &[&id, &entity, &data]).await?;
    Ok(())
}

async fn get_pg(pool: &Pool, entity: &str, id: &str) -> Result<Option<Value>, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("SELECT data FROM records WHERE id = $1 AND entity = $2")
        .await?;
    let row = client.query_opt(&stmt, &[&id, &entity]).await?;
    Ok(row.map(|r| r.get::<_, Value>(0)))
}

async fn update_pg(pool: &Pool, entity: &str, id: &str, data: &Value) -> Result<bool, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("UPDATE records SET data = $1 WHERE id = $2 AND entity = $3")
        .await?;
    let rows = client.execute(&stmt, &[&data, &id, &entity]).await?;
    Ok(rows > 0)
}

async fn delete_pg(pool: &Pool, entity: &str, id: &str) -> Result<bool, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("DELETE FROM records WHERE id = $1 AND entity = $2")
        .await?;
    let rows = client.execute(&stmt, &[&id, &entity]).await?;
    Ok(rows > 0)
}

#[allow(clippy::cast_possible_wrap)]
async fn list_pg(pool: &Pool, entity: &str, limit: i64) -> Result<Vec<Value>, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached("SELECT data FROM records WHERE entity = $1 LIMIT $2")
        .await?;
    let rows = client.query(&stmt, &[&entity, &limit]).await?;
    Ok(rows.iter().map(|r| r.get::<_, Value>(0)).collect())
}

async fn list_since_pg(
    pool: &Pool,
    entity: &str,
    cursor: i64,
    limit: i64,
) -> Result<Vec<(i64, Value)>, BoxError> {
    let client = pool.get().await?;
    let stmt = client
        .prepare_cached(
            "SELECT seq, data FROM records WHERE entity = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3",
        )
        .await?;
    let rows = client.query(&stmt, &[&entity, &cursor, &limit]).await?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, Value>(1)))
        .collect())
}

async fn check_unique_pg(
    pool: &Pool,
    entity: &str,
    fields: &[String],
    data: &Value,
    excluding_id: Option<&str>,
) -> Result<(), BoxError> {
    if fields.is_empty() {
        return Ok(());
    }
    let values: Vec<String> = fields
        .iter()
        .map(|f| {
            let v = data.get(f).cloned().unwrap_or(Value::Null);
            v.as_str().map_or_else(|| v.to_string(), ToString::to_string)
        })
        .collect();

    let client = pool.get().await?;
    let mut sql = String::from("SELECT id FROM records WHERE entity = $1");
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(entity.to_string())];
    for (i, (field, value)) in fields.iter().zip(values.iter()).enumerate() {
        let idx = i + 2;
        let _ = write!(sql, " AND data->>'{field}' = ${idx}");
        params.push(Box::new(value.clone()));
    }
    sql.push_str(" LIMIT 1");
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(sql.as_str(), &param_refs[..]).await?;
    if let Some(row) = rows.first() {
        let existing_id: String = row.get(0);
        if Some(existing_id.as_str()) != excluding_id {
            return Err("unique constraint violation".into());
        }
    }
    Ok(())
}

async fn check_fk_pg(
    pool: &Pool,
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
    let as_str = value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string);

    let client = pool.get().await?;
    let sql = format!(
        "SELECT 1 FROM records WHERE entity = $1 AND data->>'{target_field}' = $2 LIMIT 1"
    );
    let rows = client.query(sql.as_str(), &[&target_entity, &as_str]).await?;
    if rows.is_empty() {
        return Err("foreign key violation".into());
    }
    Ok(())
}

async fn find_children_pg(
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
