use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde_json::Value;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub async fn publish_request(
    client: &MqttClient,
    topic: &str,
    payload: Value,
) -> Result<Value> {
    let response_topic = format!("client/responses/{}", uuid::Uuid::new_v4());
    let (tx, mut rx) = mpsc::channel::<Value>(1);

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

    let response = timeout(Duration::from_secs(30), rx.recv())
        .await
        .map_err(|_| "Request timed out")?
        .ok_or("No response received")?;

    let status = response.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status != "ok" {
        let error = response
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        return Err(format!("Request failed: {error}").into());
    }

    Ok(response)
}

pub fn now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
