// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cli_types::{ConnectionArgs, OutputFormat};
use mqtt5::client::MqttClient;
use mqtt5::types::{ConnectOptions, PublishOptions, PublishProperties};
use serde_json::{Value, json};
use std::time::Duration;

pub(crate) async fn connect_client(
    conn: &ConnectionArgs,
) -> Result<MqttClient, Box<dyn std::error::Error>> {
    let client_id = format!("mqdb-cli-{}", uuid::Uuid::new_v4());
    let client = MqttClient::new(&client_id);

    if let (Some(user), Some(pass)) = (&conn.user, &conn.pass) {
        let options = ConnectOptions::new(&client_id).with_credentials(user.clone(), pass.clone());
        Box::pin(client.connect_with_options(&conn.broker, options)).await?;
    } else {
        client.connect(&conn.broker).await?;
    }

    Ok(client)
}

pub(crate) async fn execute_request(
    conn: &ConnectionArgs,
    topic: &str,
    payload: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let client = Box::pin(connect_client(conn)).await?;

    let response_topic = format!("mqdb-cli/responses/{}", uuid::Uuid::new_v4());
    let (tx, rx) = flume::bounded::<Value>(1);

    client
        .subscribe(&response_topic, move |msg| {
            if let Ok(value) = serde_json::from_slice::<Value>(&msg.payload) {
                let _ = tx.try_send(value);
            }
        })
        .await?;

    let opts = PublishOptions {
        properties: PublishProperties {
            response_topic: Some(response_topic.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    client
        .publish_with_options(topic, serde_json::to_vec(&payload)?, opts)
        .await?;

    let timeout = Duration::from_secs(conn.timeout);
    let response = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .map_err(|_| format!("request timed out after {}s", conn.timeout))?
        .map_err(|_| "no response received (channel closed)")?;

    client.disconnect().await?;

    Ok(response)
}

pub(crate) fn parse_filters(filter_str: &str) -> Vec<Value> {
    filter_str
        .split(',')
        .filter_map(|part| {
            let part = part.trim();

            let ops = ["<>", "!=", ">=", "<=", "!?", ">", "<", "=", "~", "?"];

            for op in ops {
                if let Some(pos) = part.find(op) {
                    let field = part[..pos].trim();
                    let value_str = part[pos + op.len()..].trim();

                    let filter_op = match op {
                        "!=" | "<>" => "neq",
                        ">" => "gt",
                        "<" => "lt",
                        ">=" => "gte",
                        "<=" => "lte",
                        "~" => "like",
                        "?" => "is_null",
                        "!?" => "is_not_null",
                        _ => "eq",
                    };

                    let value: Value = if op == "?" || op == "!?" {
                        Value::Bool(true)
                    } else if let Ok(n) = value_str.parse::<i64>() {
                        Value::Number(n.into())
                    } else if let Ok(f) = value_str.parse::<f64>() {
                        serde_json::Number::from_f64(f)
                            .map(Value::Number)
                            .unwrap_or(Value::String(value_str.to_string()))
                    } else if value_str == "true" {
                        Value::Bool(true)
                    } else if value_str == "false" {
                        Value::Bool(false)
                    } else {
                        Value::String(value_str.to_string())
                    };

                    return Some(json!({
                        "field": field,
                        "op": filter_op,
                        "value": value
                    }));
                }
            }
            None
        })
        .collect()
}

pub(crate) fn matches_filters(data: &Value, filters: &[Value]) -> bool {
    filters.iter().all(|filter| {
        let field = filter.get("field").and_then(Value::as_str).unwrap_or("");
        let op = filter.get("op").and_then(Value::as_str).unwrap_or("eq");
        let expected = filter.get("value").cloned().unwrap_or(Value::Null);

        let actual = data.get(field).cloned().unwrap_or(Value::Null);

        match op {
            "eq" => actual == expected,
            "neq" => actual != expected,
            "gt" => {
                compare_values(&actual, &expected).is_some_and(|o| o == std::cmp::Ordering::Greater)
            }
            "lt" => {
                compare_values(&actual, &expected).is_some_and(|o| o == std::cmp::Ordering::Less)
            }
            "gte" => {
                compare_values(&actual, &expected).is_some_and(|o| o != std::cmp::Ordering::Less)
            }
            "lte" => {
                compare_values(&actual, &expected).is_some_and(|o| o != std::cmp::Ordering::Greater)
            }
            "like" => {
                if let (Some(a), Some(p)) = (actual.as_str(), expected.as_str()) {
                    glob_match(p, a)
                } else {
                    false
                }
            }
            "is_null" => actual.is_null(),
            "is_not_null" => !actual.is_null(),
            _ => false,
        }
    })
}

pub(crate) fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
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
    if parts.last().unwrap_or(&"").is_empty() {
        true
    } else {
        pos == text.len()
    }
}

pub(crate) fn check_response_status(response: &Value) -> Result<(), String> {
    match response.get("status").and_then(Value::as_str) {
        Some("ok") | None => Ok(()),
        Some(_) => {
            let msg = response
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("request failed");
            Err(msg.to_string())
        }
    }
}

pub(crate) fn output_response(response: &Value, format: &OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(response).unwrap());
        }
        OutputFormat::Table => {
            if let Some(data) = response.get("data") {
                if let Some(arr) = data.as_array() {
                    output_table(arr);
                } else if data.is_object() {
                    output_table(std::slice::from_ref(data));
                } else {
                    println!("{}", serde_json::to_string_pretty(response).unwrap());
                }
            } else {
                println!("{}", serde_json::to_string_pretty(response).unwrap());
            }
        }
        OutputFormat::Csv => {
            if let Some(data) = response.get("data") {
                if let Some(arr) = data.as_array() {
                    output_csv(arr);
                } else if data.is_object() {
                    output_csv(std::slice::from_ref(data));
                } else {
                    println!("{}", serde_json::to_string_pretty(response).unwrap());
                }
            } else {
                println!("{}", serde_json::to_string_pretty(response).unwrap());
            }
        }
    }
}

pub(crate) fn output_table(data: &[Value]) {
    use comfy_table::{ContentArrangement, Table};

    if data.is_empty() {
        println!("(no results)");
        return;
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let first = &data[0];
    if let Some(obj) = first.as_object() {
        let headers: Vec<&str> = obj.keys().map(String::as_str).collect();
        table.set_header(&headers);

        for item in data {
            if let Some(obj) = item.as_object() {
                let row: Vec<String> = headers
                    .iter()
                    .map(|h| {
                        obj.get(*h)
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                Value::Null => "null".to_string(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                table.add_row(row);
            }
        }
    }

    println!("{table}");
}

pub(crate) fn output_csv(data: &[Value]) {
    if data.is_empty() {
        return;
    }

    let first = &data[0];
    if let Some(obj) = first.as_object() {
        let headers: Vec<&str> = obj.keys().map(String::as_str).collect();
        println!("{}", headers.join(","));

        for item in data {
            if let Some(obj) = item.as_object() {
                let row: Vec<String> = headers
                    .iter()
                    .map(|h| {
                        obj.get(*h)
                            .map(|v| match v {
                                Value::String(s) => {
                                    if s.contains(',') || s.contains('"') {
                                        format!("\"{}\"", s.replace('"', "\"\""))
                                    } else {
                                        s.clone()
                                    }
                                }
                                Value::Null => String::new(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                println!("{}", row.join(","));
            }
        }
    }
}
