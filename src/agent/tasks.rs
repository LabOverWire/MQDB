use super::handlers::handle_message;
use super::{MqdbAgent, connect_mqtt_client, resolve_connect_address};
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::time::Duration;
use mqtt5::types::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

impl MqdbAgent {
    pub(super) fn spawn_handler_task(
        &self,
        bind_addr: SocketAddr,
        handler_username: Option<String>,
        handler_password: Option<String>,
        auth_providers: Option<Arc<ComprehensiveAuthProvider>>,
    ) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let backup_dir = self.backup_dir.clone();
        let ownership_config = Arc::clone(&self.ownership_config);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let client = MqttClient::new("mqdb-internal-handler");
            let addr = resolve_connect_address(bind_addr);

            let response_creds = (handler_username.clone(), handler_password.clone());
            let connect_result = if let Some(user) = handler_username {
                let options = mqtt5::types::ConnectOptions::new("mqdb-internal-handler")
                    .with_credentials(user, handler_password.as_deref().unwrap_or(""));
                Box::pin(client.connect_with_options(&addr, options))
                    .await
                    .map(|_| ())
            } else {
                client.connect(&addr).await
            };

            if let Err(e) = connect_result {
                error!("Failed to connect internal handler: {e}");
                return;
            }

            let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(256);

            let callback_tx = msg_tx.clone();
            if let Err(e) = client
                .subscribe("$DB/#", move |message| {
                    let _ = callback_tx.try_send(message);
                })
                .await
            {
                error!("Failed to subscribe to $DB/#: {e}");
                return;
            }

            info!("Internal handler subscribed to $DB/#");

            let response_client = MqttClient::new("mqdb-response-publisher");
            if let Err(e) = connect_mqtt_client(
                &response_client,
                "mqdb-response-publisher",
                &addr,
                response_creds.0,
                response_creds.1,
            )
            .await
            {
                error!("Failed to connect response publisher: {e}");
                return;
            }

            loop {
                tokio::select! {
                    msg = msg_rx.recv() => {
                        if let Some(message) = msg {
                            handle_message(&db, &response_client, message, &backup_dir, &ownership_config, auth_providers.as_deref()).await;
                        } else {
                            debug!("Message channel closed");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Internal handler shutting down");
                        break;
                    }
                }
            }
        })
    }

    pub(super) fn spawn_event_task(
        &self,
        event_addr: SocketAddr,
        event_service_username: Option<String>,
        event_service_password: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        let event_db = Arc::clone(&self.db);
        let mut event_shutdown_rx = self.shutdown_tx.subscribe();
        let num_partitions = self.db.num_partitions();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let client = MqttClient::new("mqdb-event-publisher");
            let addr = format!("{}:{}", event_addr.ip(), event_addr.port());

            if let Err(e) = connect_mqtt_client(
                &client,
                "mqdb-event-publisher",
                &addr,
                event_service_username,
                event_service_password,
            )
            .await
            {
                error!("Failed to connect event publisher: {e}");
                return;
            }

            let mut event_rx = event_db.event_receiver();

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(change_event) => {
                                let topic = if num_partitions > 0 {
                                    let partition = change_event.partition(num_partitions);
                                    format!("$DB/{}/events/p{}/{}", change_event.entity, partition, change_event.id)
                                } else {
                                    format!("$DB/{}/events/{}", change_event.entity, change_event.id)
                                };
                                let payload = match serde_json::to_vec(&change_event) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("Failed to serialize event: {e}");
                                        continue;
                                    }
                                };

                                if let Err(e) = client.publish_qos1(&topic, payload).await {
                                    warn!("Failed to publish event: {e}");
                                }
                            }
                            Err(e) => {
                                error!("Event channel error: {e}");
                                break;
                            }
                        }
                    }
                    _ = event_shutdown_rx.recv() => {
                        debug!("Event publisher shutting down");
                        break;
                    }
                }
            }
        })
    }

    pub(super) fn spawn_http_task(
        &self,
        bind_addr: SocketAddr,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let http_config = self
            .http_config
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())?;

        let http_bind = http_config.bind_address;
        let http_shutdown_rx = self.shutdown_tx.subscribe();
        let http_addr = resolve_connect_address(bind_addr);
        let http_creds = (service_username.cloned(), service_password.cloned());

        Some(tokio::spawn(async move {
            tokio::time::sleep(mqtt5::time::Duration::from_millis(300)).await;

            let http_mqtt_client = MqttClient::new("mqdb-http-oauth");
            if let Err(e) = connect_mqtt_client(
                &http_mqtt_client,
                "mqdb-http-oauth",
                &http_addr,
                http_creds.0,
                http_creds.1,
            )
            .await
            {
                error!("Failed to connect HTTP OAuth MQTT client: {e}");
                return;
            }

            info!(addr = %http_bind, "starting HTTP OAuth server");
            let server = crate::http::HttpServer::new(
                http_config,
                Arc::new(http_mqtt_client),
                http_shutdown_rx,
            );
            if let Err(e) = server.run().await {
                error!("HTTP server error: {e}");
            }
        }))
    }
}
