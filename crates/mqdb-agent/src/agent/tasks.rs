// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::handlers::handle_message;
use super::{MqdbAgent, connect_mqtt_client, resolve_connect_address};
use mqtt5::broker::auth::ComprehensiveAuthProvider;
use mqtt5::client::MqttClient;
use mqtt5::time::Duration;
use mqtt5::types::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

impl MqdbAgent {
    pub(super) fn spawn_license_check_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        let expires_at = self.license_expires_at?;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_tx = self.shutdown_tx.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if mqdb_core::license::LicenseInfo::check_runtime_expiry(expires_at) {
                            tracing::error!("license has expired — shutting down");
                            let _ = shutdown_tx.send(());
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        }))
    }

    pub(super) fn spawn_handler_task(
        &self,
        bind_addr: SocketAddr,
        handler_username: Option<String>,
        handler_password: Option<String>,
        auth_providers: Option<Arc<ComprehensiveAuthProvider>>,
        handler_ready_tx: Option<oneshot::Sender<()>>,
    ) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let backup_dir = self.backup_dir.clone();
        let ownership_config = if let Some(ref svc_user) = handler_username {
            let mut oc = (*self.ownership_config).clone();
            oc.add_admin_user(svc_user.clone());
            Arc::new(oc)
        } else {
            Arc::clone(&self.ownership_config)
        };
        let scope_config = Arc::clone(&self.scope_config);
        let vault_backend = Arc::clone(&self.vault_backend);
        #[cfg(feature = "http-api")]
        let auth_rate_limiter = Arc::clone(&self.auth_rate_limiter);
        #[cfg(feature = "http-api")]
        let identity_crypto = self.identity_crypto.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let client = MqttClient::new("mqdb-internal-handler");
            let addr = resolve_connect_address(bind_addr);

            let response_creds = (handler_username.clone(), handler_password.clone());
            let connect_result = connect_mqtt_client(
                &client,
                "mqdb-internal-handler",
                &addr,
                handler_username,
                handler_password,
            )
            .await;

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
            if let Some(tx) = handler_ready_tx {
                let _ = tx.send(());
            }

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
                            let ctx = super::handlers::MessageContext {
                                db: &db,
                                client: &response_client,
                                backup_dir: &backup_dir,
                                ownership: &ownership_config,
                                scope_config: &scope_config,
                                auth_providers: auth_providers.as_deref(),
                                vault_backend: &vault_backend,
                                #[cfg(feature = "http-api")]
                                auth_rate_limiter: &auth_rate_limiter,
                                #[cfg(feature = "http-api")]
                                identity_crypto: identity_crypto.as_ref(),
                            };
                            handle_message(&ctx, message).await;
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
                                let topic = change_event.event_topic(num_partitions);
                                let client_id = change_event.client_id.clone();

                                let payload = match serde_json::to_vec(&change_event) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("Failed to serialize event: {e}");
                                        continue;
                                    }
                                };

                                let mut options = mqtt5::types::PublishOptions {
                                    qos: mqtt5::QoS::AtLeastOnce,
                                    ..Default::default()
                                };
                                if let Some(ref cid) = client_id {
                                    options.properties.user_properties.push((
                                        "x-origin-client-id".to_string(),
                                        cid.clone(),
                                    ));
                                }
                                if let Err(e) = client.publish_with_options(&topic, payload, options).await {
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
        let mut http_config = self
            .http_config
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())?;

        http_config.vault_backend = Some(Arc::clone(&self.vault_backend));
        http_config.db = Some(Arc::clone(&self.db));
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
