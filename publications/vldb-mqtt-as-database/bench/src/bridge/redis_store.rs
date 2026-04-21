use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use serde_json::Value;

use super::BoxError;

pub async fn connect(url: &str) -> Result<MultiplexedConnection, BoxError> {
    let client = redis::Client::open(url)?;
    let conn = client.get_multiplexed_async_connection().await?;
    tracing::info!("Redis connected to {url}");
    Ok(conn)
}

fn record_key(entity: &str, id: &str) -> String {
    format!("record:{entity}:{id}")
}

fn entity_set_key(entity: &str) -> String {
    format!("entity:{entity}")
}

pub async fn insert_record(
    conn: &MultiplexedConnection,
    id: &str,
    entity: &str,
    data: &Value,
) -> Result<(), BoxError> {
    let key = record_key(entity, id);
    let set_key = entity_set_key(entity);
    let json = serde_json::to_string(data)?;

    let mut pipe = redis::pipe();
    pipe.atomic()
        .set(&key, &json)
        .sadd(&set_key, id);
    pipe.exec_async(&mut conn.clone()).await?;
    Ok(())
}

pub async fn get_record(
    conn: &MultiplexedConnection,
    entity: &str,
    id: &str,
) -> Result<Option<Value>, BoxError> {
    let key = record_key(entity, id);
    let result: Option<String> = conn.clone().get(&key).await?;
    match result {
        Some(json) => Ok(Some(serde_json::from_str(&json)?)),
        None => Ok(None),
    }
}

pub async fn update_record(
    conn: &MultiplexedConnection,
    entity: &str,
    id: &str,
    data: &Value,
) -> Result<bool, BoxError> {
    let key = record_key(entity, id);
    let exists: bool = conn.clone().exists(&key).await?;
    if !exists {
        return Ok(false);
    }
    let json = serde_json::to_string(data)?;
    conn.clone().set::<_, _, ()>(&key, &json).await?;
    Ok(true)
}

pub async fn delete_record(
    conn: &MultiplexedConnection,
    entity: &str,
    id: &str,
) -> Result<bool, BoxError> {
    let key = record_key(entity, id);
    let set_key = entity_set_key(entity);
    let deleted: i64 = conn.clone().del(&key).await?;
    if deleted > 0 {
        conn.clone().srem::<_, _, ()>(&set_key, id).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[allow(clippy::cast_possible_truncation)]
pub async fn list_records(
    conn: &MultiplexedConnection,
    entity: &str,
    limit: i64,
) -> Result<Vec<Value>, BoxError> {
    let set_key = entity_set_key(entity);
    let ids: Vec<String> = conn.clone().smembers(&set_key).await?;

    let take_n = (limit as usize).min(ids.len());
    if take_n == 0 {
        return Ok(Vec::new());
    }

    let keys: Vec<String> = ids[..take_n]
        .iter()
        .map(|id| record_key(entity, id))
        .collect();

    let values: Vec<Option<String>> = conn.clone().mget(&keys).await?;
    let records: Vec<Value> = values
        .into_iter()
        .flatten()
        .filter_map(|json| serde_json::from_str(&json).ok())
        .collect();

    Ok(records)
}

fn value_to_string(value: &Value) -> String {
    value
        .as_str()
        .map(ToString::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn unique_key(entity: &str, fields: &[String], values: &[Value]) -> String {
    let parts: Vec<String> = fields
        .iter()
        .zip(values.iter())
        .map(|(f, v)| format!("{f}={}", value_to_string(v)))
        .collect();
    format!("unique:{entity}:{}", parts.join(":"))
}

pub async fn check_unique(
    conn: &MultiplexedConnection,
    entity: &str,
    fields: &[String],
    values: &[Value],
    excluding_id: Option<&str>,
) -> Result<(), BoxError> {
    if fields.is_empty() {
        return Ok(());
    }
    let key = unique_key(entity, fields, values);
    let existing: Option<String> = conn.clone().get(&key).await?;
    match existing {
        Some(id) if Some(id.as_str()) == excluding_id => Ok(()),
        Some(_) => Err("unique constraint violation".into()),
        None => {
            let owner = excluding_id.unwrap_or("new").to_string();
            let ok: bool = redis::cmd("SET")
                .arg(&key)
                .arg(&owner)
                .arg("NX")
                .query_async(&mut conn.clone())
                .await?;
            if ok {
                Ok(())
            } else {
                Err("unique constraint violation".into())
            }
        }
    }
}

fn fk_index_key(entity: &str, field: &str, value: &Value) -> String {
    format!("fkidx:{entity}:{field}:{}", value_to_string(value))
}

pub async fn check_fk(
    conn: &MultiplexedConnection,
    target_entity: &str,
    target_field: &str,
    value: &Value,
) -> Result<(), BoxError> {
    let idx_key = fk_index_key(target_entity, target_field, value);
    let exists: bool = conn.clone().exists(&idx_key).await?;
    if exists {
        return Ok(());
    }
    let set_key = entity_set_key(target_entity);
    let ids: Vec<String> = conn.clone().smembers(&set_key).await?;
    for id in &ids {
        let key = record_key(target_entity, id);
        let stored: Option<String> = conn.clone().get(&key).await?;
        if let Some(json) = stored
            && let Ok(parsed) = serde_json::from_str::<Value>(&json)
            && parsed
                .get(target_field)
                .map(|v| value_to_string(v) == value_to_string(value))
                .unwrap_or(false)
        {
            return Ok(());
        }
    }
    Err("foreign key violation".into())
}

pub async fn find_children(
    conn: &MultiplexedConnection,
    child_entity: &str,
    child_field: &str,
    parent_id: &str,
) -> Result<Vec<(String, String)>, BoxError> {
    let set_key = entity_set_key(child_entity);
    let ids: Vec<String> = conn.clone().smembers(&set_key).await?;
    let mut matches = Vec::new();
    let target = parent_id.to_string();
    for id in ids {
        let key = record_key(child_entity, &id);
        let stored: Option<String> = conn.clone().get(&key).await?;
        if let Some(json) = stored
            && let Ok(parsed) = serde_json::from_str::<Value>(&json)
            && parsed
                .get(child_field)
                .map(|v| value_to_string(v) == target)
                .unwrap_or(false)
        {
            matches.push((child_entity.to_string(), id));
        }
    }
    Ok(matches)
}