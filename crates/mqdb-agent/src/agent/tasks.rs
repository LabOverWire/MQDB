// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::broker::BrokerFeeds;
use super::handlers::handle_message;
use super::{MqdbAgent, connect_mqtt_client, resolve_connect_address};
use crate::presence::PresenceEvent;
use crate::sparkplug::Certificate;
use mqtt5::broker::auth::{AuthProvider, ComprehensiveAuthProvider};
use mqtt5::client::MqttClient;
use mqtt5::time::Duration;
use mqtt5::types::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

pub(super) struct RetainedPublisher<T> {
    pub client_id: &'static str,
    pub qos: mqtt5::QoS,
    pub receiver: flume::Receiver<T>,
    pub encode: fn(T) -> Option<(String, Vec<u8>)>,
}

fn encode_presence(presence: PresenceEvent) -> Option<(String, Vec<u8>)> {
    match presence.payload() {
        Ok(payload) => Some((presence.topic(), payload)),
        Err(e) => {
            error!("Failed to serialize presence: {e}");
            None
        }
    }
}

fn encode_certificate(certificate: Certificate) -> Option<(String, Vec<u8>)> {
    Some((certificate.topic, certificate.payload))
}

pub(super) struct HandlerAuth {
    pub providers: Option<Arc<ComprehensiveAuthProvider>>,
    pub authorizer: Arc<dyn AuthProvider>,
}

impl MqdbAgent {
    pub(super) fn spawn_license_check_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        let expires_at = self.license_expires_at?;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_tx = self.shutdown_tx.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_hours(1));
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
        auth: HandlerAuth,
        mut broker_ready_rx: watch::Receiver<bool>,
        handler_ready_tx: Option<oneshot::Sender<()>>,
    ) -> tokio::task::JoinHandle<()> {
        let HandlerAuth {
            providers: auth_providers,
            authorizer,
        } = auth;
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
        #[cfg(feature = "http-api")]
        let session_store = self.session_store.clone();
        #[cfg(feature = "http-api")]
        let jti_revocation = self.jti_revocation.clone();

        tokio::spawn(async move {
            if !wait_for_broker(&mut broker_ready_rx, &mut shutdown_rx, "internal handler").await {
                return;
            }

            let client = MqttClient::new("mqdb-internal-handler");
            let addr = resolve_connect_address(bind_addr);

            let service_username = handler_username.clone();
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
                    if let Err(e) = callback_tx.try_send(message) {
                        let dropped = e.into_inner();
                        warn!(topic = %dropped.topic, "internal handler queue full, request dropped");
                    }
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

            let response_client = MqttClient::new(super::handlers::RESPONSE_PUBLISHER_CLIENT_ID);
            if let Err(e) = connect_mqtt_client(
                &response_client,
                super::handlers::RESPONSE_PUBLISHER_CLIENT_ID,
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
                                authorizer: authorizer.as_ref(),
                                service_username: service_username.as_deref(),
                                backup_dir: &backup_dir,
                                ownership: &ownership_config,
                                scope_config: &scope_config,
                                auth_providers: auth_providers.as_deref(),
                                vault_backend: &vault_backend,
                                #[cfg(feature = "http-api")]
                                auth_rate_limiter: &auth_rate_limiter,
                                #[cfg(feature = "http-api")]
                                identity_crypto: identity_crypto.as_ref(),
                                #[cfg(feature = "http-api")]
                                session_store: session_store.as_ref(),
                                #[cfg(feature = "http-api")]
                                jti_revocation: jti_revocation.as_ref(),
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
        mut broker_ready_rx: watch::Receiver<bool>,
        publisher_ready_tx: Option<oneshot::Sender<()>>,
    ) -> tokio::task::JoinHandle<()> {
        let event_db = Arc::clone(&self.db);
        let mut event_rx = self.db.event_receiver();
        let mut event_shutdown_rx = self.shutdown_tx.subscribe();
        let num_partitions = self.db.num_partitions();
        let scoped_events = self.scoped_events;
        let ownership_config = Arc::clone(&self.ownership_config);

        tokio::spawn(async move {
            if !wait_for_broker(
                &mut broker_ready_rx,
                &mut event_shutdown_rx,
                "event publisher",
            )
            .await
            {
                return;
            }

            let client = MqttClient::new("mqdb-event-publisher");
            let addr = resolve_connect_address(event_addr);

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
            if let Some(tx) = publisher_ready_tx {
                let _ = tx.send(());
            }

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(change_event) => {
                                let client_id = change_event.client_id.clone();

                                let payload = match serde_json::to_vec(&change_event) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("Failed to serialize event: {e}");
                                        continue;
                                    }
                                };

                                let Some(topics) = event_publish_topics(
                                    &event_db,
                                    &ownership_config,
                                    scoped_events,
                                    num_partitions,
                                    &change_event,
                                )
                                .await
                                else {
                                    continue;
                                };

                                for topic in topics {
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
                                    if let Err(e) = client
                                        .publish_with_options(&topic, payload.clone(), options)
                                        .await
                                    {
                                        warn!("Failed to publish event: {e}");
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                error!(skipped, "event publisher fell behind; change events were dropped");
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                debug!("change event channel closed");
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

    pub(super) fn spawn_feed_publishers(
        &self,
        feeds: BrokerFeeds,
        addr: SocketAddr,
        service_username: Option<&String>,
        service_password: Option<&String>,
        broker_ready_rx: &watch::Receiver<bool>,
    ) -> [Option<tokio::task::JoinHandle<()>>; 2] {
        let presence = feeds.presence.map(|receiver| {
            self.spawn_retained_publisher(
                RetainedPublisher {
                    client_id: "mqdb-presence-publisher",
                    qos: mqtt5::QoS::AtMostOnce,
                    receiver,
                    encode: encode_presence,
                },
                addr,
                service_username.cloned(),
                service_password.cloned(),
                broker_ready_rx.clone(),
            )
        });
        let certificates = feeds.certificates.map(|receiver| {
            self.spawn_retained_publisher(
                RetainedPublisher {
                    client_id: "mqdb-certificate-publisher",
                    qos: mqtt5::QoS::AtLeastOnce,
                    receiver,
                    encode: encode_certificate,
                },
                addr,
                service_username.cloned(),
                service_password.cloned(),
                broker_ready_rx.clone(),
            )
        });
        [presence, certificates]
    }

    pub(super) fn spawn_retained_publisher<T: Send + 'static>(
        &self,
        publisher: RetainedPublisher<T>,
        addr: SocketAddr,
        service_username: Option<String>,
        service_password: Option<String>,
        mut broker_ready_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let RetainedPublisher {
            client_id,
            qos,
            receiver,
            encode,
        } = publisher;

        tokio::spawn(async move {
            if !wait_for_broker(&mut broker_ready_rx, &mut shutdown_rx, client_id).await {
                return;
            }

            let client = MqttClient::new(client_id);
            let addr = resolve_connect_address(addr);

            if let Err(e) = connect_mqtt_client(
                &client,
                client_id,
                &addr,
                service_username,
                service_password,
            )
            .await
            {
                error!(client_id, "Failed to connect retained publisher: {e}");
                return;
            }

            loop {
                tokio::select! {
                    item = receiver.recv_async() => {
                        let Ok(item) = item else {
                            debug!(client_id, "retained publisher channel closed");
                            break;
                        };
                        let Some((topic, payload)) = encode(item) else {
                            continue;
                        };
                        let options = mqtt5::types::PublishOptions {
                            qos,
                            retain: true,
                            ..Default::default()
                        };
                        if let Err(e) = client.publish_with_options(&topic, payload, options).await {
                            warn!(client_id, topic, "Failed to publish retained message: {e}");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(client_id, "retained publisher shutting down");
                        break;
                    }
                }
            }
        })
    }

    #[cfg(feature = "http-api")]
    pub(super) fn spawn_http_task(
        &self,
        bind_addr: SocketAddr,
        service_username: Option<&String>,
        service_password: Option<&String>,
        mut broker_ready_rx: watch::Receiver<bool>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let mut http_config = self
            .http_config
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())?;

        http_config.vault_backend = Some(Arc::clone(&self.vault_backend));
        http_config.db_access = Arc::clone(&self.db) as Arc<dyn crate::vault_backend::DbAccess>;
        let http_bind = http_config.bind_address;
        let mut http_shutdown_rx = self.shutdown_tx.subscribe();
        let http_addr = resolve_connect_address(bind_addr);
        let http_creds = (service_username.cloned(), service_password.cloned());

        Some(tokio::spawn(async move {
            if !wait_for_broker(
                &mut broker_ready_rx,
                &mut http_shutdown_rx,
                "HTTP OAuth client",
            )
            .await
            {
                return;
            }

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

async fn event_publish_topics(
    db: &crate::database::Database,
    ownership: &mqdb_core::types::OwnershipConfig,
    scoped_events: bool,
    num_partitions: u8,
    event: &mqdb_core::events::ChangeEvent,
) -> Option<Vec<String>> {
    if !scoped_events {
        return Some(vec![event.event_topic(num_partitions)]);
    }
    let recipients = if let Some(precomputed) = event.recipients.clone() {
        Some(precomputed)
    } else {
        match db
            .event_recipients(
                ownership,
                &event.entity,
                &event.id,
                event.data.as_ref(),
                event.sender.as_deref(),
            )
            .await
        {
            Ok(recipients) => recipients,
            Err(e) => {
                warn!(
                    entity = %event.entity,
                    id = %event.id,
                    error = %e,
                    "failed to compute event recipients; dropping event"
                );
                return None;
            }
        }
    };
    match recipients {
        Some(recipients) => {
            if recipients.is_empty() {
                debug!(
                    entity = %event.entity,
                    id = %event.id,
                    "scoped event has no recipients; dropping"
                );
            }
            Some(
                recipients
                    .iter()
                    .map(|r| format!("$DB/u/{r}/events/{}/{}", event.entity, event.id))
                    .collect(),
            )
        }
        None => Some(vec![event.event_topic(num_partitions)]),
    }
}

async fn wait_for_broker(
    broker_ready_rx: &mut watch::Receiver<bool>,
    shutdown_rx: &mut broadcast::Receiver<()>,
    task: &str,
) -> bool {
    tokio::select! {
        ready = broker_ready_rx.wait_for(|ready| *ready) => {
            if ready.is_err() {
                error!("broker stopped before the {task} could connect");
            }
            ready.is_ok()
        }
        _ = shutdown_rx.recv() => {
            debug!("{task} shutting down before the broker became ready");
            false
        }
    }
}
