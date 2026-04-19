mod handler;
pub mod pg;
pub mod redis_store;
pub mod store;

use mqtt5::client::MqttClient;
use store::Store;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub async fn run(broker: &str, store: Store) -> Result<(), BoxError> {
    let client = MqttClient::new("baseline-bridge");
    client.connect(broker).await?;
    tracing::info!("Connected to MQTT broker at {broker}");

    subscribe_health(&client).await?;
    subscribe_constraint(&client, &store).await?;
    subscribe_create(&client, &store).await?;
    subscribe_list(&client, &store).await?;
    subscribe_update(&client, &store).await?;
    subscribe_delete(&client, &store).await?;
    subscribe_get(&client, &store).await?;

    tracing::info!("Bridge ready");

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down");
    let _ = client.disconnect().await;
    Ok(())
}

async fn subscribe_health(client: &MqttClient) -> Result<(), BoxError> {
    let client_pub = client.clone();
    client
        .subscribe("$DB/_health", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            tokio::spawn(async move {
                handler::handle_health(&c, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_constraint(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/_admin/constraint/+/add", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let parts: Vec<&str> = msg.topic.split('/').collect();
            if parts.len() != 5 {
                return;
            }
            let entity = parts[3].to_string();
            let payload = msg.payload.clone();
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_constraint(&c, &s, &entity, &payload, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_create(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/+/create", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let Some(entity) = parse_topic_segment(&msg.topic, 3, 2, "create") else {
                return;
            };
            let payload = msg.payload.clone();
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_create(&c, &s, &entity, &payload, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_list(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/+/list", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let Some(entity) = parse_topic_segment(&msg.topic, 3, 2, "list") else {
                return;
            };
            let payload = msg.payload.clone();
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_list(&c, &s, &entity, &payload, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_update(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/+/+/update", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let Some((entity, id)) = parse_entity_id(&msg.topic, 4, 3, "update") else {
                return;
            };
            let payload = msg.payload.clone();
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_update(&c, &s, &entity, &id, &payload, &response_topic, &cd)
                    .await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_delete(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/+/+/delete", move |msg| {
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let Some((entity, id)) = parse_entity_id(&msg.topic, 4, 3, "delete") else {
                return;
            };
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_delete(&c, &s, &entity, &id, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

async fn subscribe_get(client: &MqttClient, store: &Store) -> Result<(), BoxError> {
    let client_pub = client.clone();
    let store = store.clone();
    client
        .subscribe("$DB/+/+", move |msg| {
            if msg.topic.ends_with("/create") || msg.topic.ends_with("/list") {
                return;
            }
            let Some(response_topic) = msg.properties.response_topic.clone() else {
                return;
            };
            let parts: Vec<&str> = msg.topic.split('/').collect();
            if parts.len() != 3 || parts[0] != "$DB" {
                return;
            }
            let entity = parts[1].to_string();
            let id = parts[2].to_string();
            let cd = msg.properties.correlation_data.clone();
            let c = client_pub.clone();
            let s = store.clone();
            tokio::spawn(async move {
                handler::handle_get(&c, &s, &entity, &id, &response_topic, &cd).await;
            });
        })
        .await?;
    Ok(())
}

fn parse_topic_segment(topic: &str, expected_len: usize, suffix_idx: usize, suffix: &str) -> Option<String> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() == expected_len && parts[0] == "$DB" && parts[suffix_idx] == suffix {
        Some(parts[1].to_string())
    } else {
        None
    }
}

fn parse_entity_id(topic: &str, expected_len: usize, suffix_idx: usize, suffix: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() == expected_len && parts[0] == "$DB" && parts[suffix_idx] == suffix {
        Some((parts[1].to_string(), parts[2].to_string()))
    } else {
        None
    }
}
