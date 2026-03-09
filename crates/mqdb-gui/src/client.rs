use crate::state::{CatalogData, Command, FilterSpec, LiveEvent, SortSpec, UiEvent};
use mqtt5::client::MqttClient;
use mqtt5::{PublishOptions, QoS};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct MqttBackend {
    cmd_rx: flume::Receiver<Command>,
    ui_tx: flume::Sender<UiEvent>,
    repaint: Arc<dyn Fn() + Send + Sync>,
    client: Option<MqttClient>,
    event_client: Option<MqttClient>,
    broker_addr: String,
}

impl MqttBackend {
    pub fn new(
        cmd_rx: flume::Receiver<Command>,
        ui_tx: flume::Sender<UiEvent>,
        repaint: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            cmd_rx,
            ui_tx,
            repaint: Arc::from(repaint),
            client: None,
            event_client: None,
            broker_addr: String::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let Ok(cmd) = self.cmd_rx.recv_async().await else {
                break;
            };
            match cmd {
                Command::Connect {
                    host,
                    port,
                    username,
                    password,
                } => Box::pin(self.handle_connect(&host, port, username, password)).await,
                Command::Disconnect => self.handle_disconnect().await,
                Command::FetchCatalog => self.handle_fetch_catalog().await,
                Command::ListRecords {
                    entity,
                    filters,
                    sort,
                    limit,
                    offset,
                } => {
                    self.handle_list_records(&entity, &filters, &sort, limit, offset)
                        .await;
                }
                Command::ReadRecord { entity, id } => {
                    self.handle_read_record(&entity, &id).await;
                }
                Command::CreateRecord { entity, data } => {
                    self.handle_create_record(&entity, data).await;
                }
                Command::UpdateRecord { entity, id, fields } => {
                    self.handle_update_record(&entity, &id, fields).await;
                }
                Command::DeleteRecord { entity, id } => {
                    self.handle_delete_record(&entity, &id).await;
                }
                Command::SetSchema { entity, schema } => {
                    self.handle_set_schema(&entity, schema).await;
                }
                Command::AddConstraint { entity, constraint } => {
                    self.handle_add_constraint(&entity, constraint).await;
                }
                Command::SubscribeEvents { entity } => {
                    Box::pin(self.handle_subscribe_events(&entity)).await;
                }
                Command::UnsubscribeEvents => {
                    self.handle_unsubscribe_events().await;
                }
            }
        }
    }

    fn send_event(&self, event: UiEvent) {
        let _ = self.ui_tx.send(event);
        (self.repaint)();
    }

    async fn handle_connect(
        &mut self,
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    ) {
        let addr = format!("{host}:{port}");
        let client_id = format!("mqdb-gui-{}", uuid::Uuid::new_v4());
        info!(addr = %addr, "connecting to broker");

        let client = MqttClient::new(&client_id);
        let options = if let (Some(user), Some(pass)) = (username, password) {
            mqtt5::ConnectOptions::new(&client_id).with_credentials(user, pass)
        } else {
            mqtt5::ConnectOptions::new(&client_id)
        };

        match Box::pin(client.connect_with_options(&addr, options)).await {
            Ok(_) => {
                info!("connected to broker");
                self.broker_addr = addr;
                self.client = Some(client);
                self.send_event(UiEvent::Connected);
            }
            Err(e) => {
                error!(error = %e, "connection failed");
                self.send_event(UiEvent::ConnectionError(e.to_string()));
            }
        }
    }

    async fn handle_disconnect(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.disconnect().await;
        }
        if let Some(ec) = self.event_client.take() {
            let _ = ec.disconnect().await;
        }
        self.broker_addr.clear();
        self.send_event(UiEvent::Disconnected);
    }

    async fn request_response(&self, topic: &str, payload: Value) -> Option<Value> {
        let client = self.client.as_ref()?;
        let response_topic = format!("_gui/resp/{}", uuid::Uuid::new_v4());

        let (resp_tx, resp_rx) = flume::bounded::<Vec<u8>>(1);

        if let Err(e) = client
            .subscribe(&response_topic, move |msg| {
                let _ = resp_tx.send(msg.payload.clone());
            })
            .await
        {
            warn!(error = %e, "failed to subscribe to response topic");
            return None;
        }

        let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
        let opts = PublishOptions {
            qos: QoS::AtLeastOnce,
            properties: mqtt5::PublishProperties {
                response_topic: Some(response_topic.clone()),
                ..Default::default()
            },
            ..Default::default()
        };

        if let Err(e) = client
            .publish_with_options(topic, payload_bytes, opts)
            .await
        {
            warn!(error = %e, topic = %topic, "failed to publish request");
            let _ = client.unsubscribe(&response_topic).await;
            return None;
        }

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), resp_rx.recv_async()).await;

        let _ = client.unsubscribe(&response_topic).await;

        match result {
            Ok(Ok(data)) => serde_json::from_slice(&data).ok(),
            Ok(Err(e)) => {
                warn!(error = %e, "failed to receive response");
                None
            }
            Err(_) => {
                warn!(topic = %topic, "request timed out");
                None
            }
        }
    }

    async fn handle_fetch_catalog(&self) {
        debug!("fetching catalog");
        let response = self.request_response("$DB/_admin/catalog", json!({})).await;
        match response {
            Some(resp) => {
                if let Some(data) = resp.get("data") {
                    match serde_json::from_value::<CatalogData>(data.clone()) {
                        Ok(catalog) => {
                            self.send_event(UiEvent::CatalogReceived(catalog));
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to parse catalog");
                            self.send_event(UiEvent::OperationError(format!(
                                "failed to parse catalog: {e}"
                            )));
                        }
                    }
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("catalog error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError(
                    "catalog request failed".to_string(),
                ));
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_list_records(
        &self,
        entity: &str,
        filters: &[FilterSpec],
        sort: &[SortSpec],
        limit: usize,
        offset: usize,
    ) {
        let mut payload = json!({
            "limit": limit,
            "offset": offset,
        });

        if !filters.is_empty() {
            let filter_json: Vec<Value> = filters
                .iter()
                .filter(|f| !f.field.is_empty())
                .map(|f| {
                    let op = match f.op.as_str() {
                        "!=" | "<>" => "neq",
                        ">" => "gt",
                        ">=" => "gte",
                        "<" => "lt",
                        "<=" => "lte",
                        "~" => "like",
                        _ => "eq",
                    };
                    json!({
                        "field": f.field,
                        "op": op,
                        "value": parse_filter_value(&f.value),
                    })
                })
                .collect();
            if !filter_json.is_empty() {
                payload["filters"] = Value::Array(filter_json);
            }
        }

        if !sort.is_empty() && !sort[0].field.is_empty() {
            let sort_json: Vec<Value> = sort
                .iter()
                .filter(|s| !s.field.is_empty())
                .map(|s| {
                    json!({
                        "field": s.field,
                        "direction": s.direction,
                    })
                })
                .collect();
            if !sort_json.is_empty() {
                payload["sort"] = Value::Array(sort_json);
            }
        }

        let topic = format!("$DB/{entity}/list");
        match self.request_response(&topic, payload).await {
            Some(resp) => {
                if let Some(data) = resp.get("data") {
                    let records = if let Some(arr) = data.as_array() {
                        arr.clone()
                    } else {
                        vec![data.clone()]
                    };
                    self.send_event(UiEvent::RecordsReceived {
                        entity: entity.to_string(),
                        records,
                    });
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("list error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("list request failed".to_string()));
            }
        }
    }

    async fn handle_read_record(&self, entity: &str, id: &str) {
        let topic = format!("$DB/{entity}/{id}");
        match self.request_response(&topic, json!({})).await {
            Some(resp) => {
                if let Some(data) = resp.get("data") {
                    self.send_event(UiEvent::RecordReceived {
                        entity: entity.to_string(),
                        record: data.clone(),
                    });
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("read error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("read request failed".to_string()));
            }
        }
    }

    async fn handle_create_record(&self, entity: &str, data: Value) {
        let topic = format!("$DB/{entity}/create");
        match self.request_response(&topic, data).await {
            Some(resp) => {
                if resp.get("data").is_some() {
                    self.send_event(UiEvent::OperationSuccess("record created".to_string()));
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("create error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("create request failed".to_string()));
            }
        }
    }

    async fn handle_update_record(&self, entity: &str, id: &str, fields: Value) {
        let topic = format!("$DB/{entity}/{id}/update");
        match self.request_response(&topic, fields).await {
            Some(resp) => {
                if resp.get("data").is_some() {
                    self.send_event(UiEvent::OperationSuccess("record updated".to_string()));
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("update error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("update request failed".to_string()));
            }
        }
    }

    async fn handle_delete_record(&self, entity: &str, id: &str) {
        let topic = format!("$DB/{entity}/{id}/delete");
        match self.request_response(&topic, json!({})).await {
            Some(resp) => {
                if resp.get("error").is_none() {
                    self.send_event(UiEvent::OperationSuccess("record deleted".to_string()));
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("delete error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("delete request failed".to_string()));
            }
        }
    }

    async fn handle_set_schema(&self, entity: &str, schema: Value) {
        let topic = format!("$DB/_admin/schema/{entity}/set");
        match self.request_response(&topic, schema).await {
            Some(resp) => {
                if resp.get("error").is_none() {
                    self.send_event(UiEvent::OperationSuccess("schema set".to_string()));
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("schema error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError("schema request failed".to_string()));
            }
        }
    }

    async fn handle_add_constraint(&self, entity: &str, constraint: Value) {
        let topic = format!("$DB/_admin/constraint/{entity}/add");
        match self.request_response(&topic, constraint).await {
            Some(resp) => {
                if resp.get("error").is_none() {
                    self.send_event(UiEvent::OperationSuccess("constraint added".to_string()));
                } else if let Some(err) = resp.get("error") {
                    self.send_event(UiEvent::OperationError(
                        err.as_str().unwrap_or("constraint error").to_string(),
                    ));
                }
            }
            None => {
                self.send_event(UiEvent::OperationError(
                    "constraint request failed".to_string(),
                ));
            }
        }
    }

    async fn handle_subscribe_events(&mut self, entity: &str) {
        if self.broker_addr.is_empty() {
            self.send_event(UiEvent::OperationError("not connected".to_string()));
            return;
        }

        let client_id = format!("mqdb-gui-events-{}", uuid::Uuid::new_v4());
        let event_client = MqttClient::new(&client_id);
        let options = mqtt5::ConnectOptions::new(&client_id);

        if Box::pin(event_client.connect_with_options(&self.broker_addr, options))
            .await
            .is_err()
        {
            self.send_event(UiEvent::OperationError(
                "failed to connect event client".to_string(),
            ));
            return;
        }

        let topic = format!("$DB/{entity}/events/#");
        let ui_tx = self.ui_tx.clone();
        let entity_name = entity.to_string();
        let repaint = Arc::clone(&self.repaint);

        if let Err(e) = event_client
            .subscribe(&topic, move |msg| {
                let operation = msg
                    .topic
                    .rsplit('/')
                    .next()
                    .unwrap_or("unknown")
                    .to_string();
                let data: Value = serde_json::from_slice(&msg.payload).unwrap_or(Value::Null);
                let id = data
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let event = LiveEvent {
                    entity: entity_name.clone(),
                    operation,
                    id,
                    data,
                };
                let _ = ui_tx.send(UiEvent::EventReceived(event));
                repaint();
            })
            .await
        {
            self.send_event(UiEvent::OperationError(format!(
                "failed to subscribe to events: {e}"
            )));
            return;
        }

        self.event_client = Some(event_client);
        self.send_event(UiEvent::OperationSuccess(format!(
            "subscribed to {entity} events"
        )));
    }

    async fn handle_unsubscribe_events(&mut self) {
        if let Some(ec) = self.event_client.take() {
            let _ = ec.disconnect().await;
        }
    }
}

fn parse_filter_value(s: &str) -> Value {
    if let Ok(n) = s.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(n) = s.parse::<f64>()
        && let Some(n) = serde_json::Number::from_f64(n)
    {
        return Value::Number(n);
    }
    if s == "true" {
        return Value::Bool(true);
    }
    if s == "false" {
        return Value::Bool(false);
    }
    if s == "null" {
        return Value::Null;
    }
    Value::String(s.to_string())
}
