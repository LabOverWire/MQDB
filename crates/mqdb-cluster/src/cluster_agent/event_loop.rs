// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{AdminRequest, ClusteredAgent, MESH_CHECK_INTERVAL_SECS, RunningBroker};
use crate::cluster::{
    ClusterMessage, ClusterTransport, LocalPublishKind, LocalPublishRequest, LocalRetainedWriter,
    NodeController, NodeId, ProcessingBatch, RoutedDeliveryLanes, TopicSubscriptionBroadcast,
    WildcardBroadcast,
};
use mqtt5::broker::router::MessageRouter;
use mqtt5::time::Duration;
use std::sync::Arc;
use tokio::time::interval;
use tracing::{debug, info, warn};

use super::{
    CLEANUP_INTERVAL_SECS, RETAINED_ECHO_MAX_AGE_SECS, RETAINED_SYNC_CLEANUP_INTERVAL_SECS,
    TTL_CLEANUP_INTERVAL_SECS, UNIQUE_RECONCILE_INTERVAL_SECS,
};

impl ClusteredAgent {
    /// # Errors
    /// If broker setup, transport connection, or event loop execution fails.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (bridge_configs, use_external_bridge_manager) = self.prepare_bridges()?;

        let (broker_config, service_username, service_password, needs_composite, admin_users) =
            self.configure_broker_with_event_handler(&bridge_configs, use_external_bridge_manager)
                .await?;

        let (broker, auth_providers) = self
            .build_broker(
                broker_config,
                needs_composite,
                service_username.as_ref(),
                service_password.as_ref(),
                &admin_users,
            )
            .await?;
        self.auth_providers = auth_providers;

        let broker = self
            .start_broker(broker, &bridge_configs, use_external_bridge_manager)
            .await;

        let admin_client = self
            .connect_transport(service_username.as_ref(), service_password.as_ref())
            .await?;

        let (admin_tx, admin_rx) = flume::bounded::<AdminRequest>(32);
        super::broker::subscribe_admin_topics(&admin_client, &admin_tx).await?;

        self.setup_and_spawn_raft().await?;

        Box::pin(self.run_event_loop(
            broker,
            admin_client,
            admin_rx,
            service_username.as_ref(),
            service_password.as_ref(),
        ))
        .await
    }

    async fn run_event_loop(
        &mut self,
        broker: RunningBroker,
        admin_client: mqtt5::MqttClient,
        admin_rx: flume::Receiver<AdminRequest>,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tick_interval = interval(Duration::from_millis(10));
        let mut cleanup_interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        let mut ttl_cleanup_interval = interval(Duration::from_secs(TTL_CLEANUP_INTERVAL_SECS));
        let mut subscription_reconciliation_interval = interval(Duration::from_mins(5));
        let mut mesh_check_interval = interval(Duration::from_secs(MESH_CHECK_INTERVAL_SECS));
        let mut retained_sync_cleanup_interval =
            interval(Duration::from_secs(RETAINED_SYNC_CLEANUP_INTERVAL_SECS));
        let mut cascade_retry_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let mut license_check_interval = interval(Duration::from_hours(1));
        let mut unique_reconcile_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(UNIQUE_RECONCILE_INTERVAL_SECS),
            Duration::from_secs(UNIQUE_RECONCILE_INTERVAL_SECS),
        );
        let mut shutdown_rx = self
            .shutdown_rx
            .take()
            .unwrap_or_else(|| self.shutdown_tx.subscribe());
        let tx_tick = self
            .tx_tick
            .take()
            .ok_or("run() called twice: tx_tick already taken")?;
        let rx_main_queue = self
            .rx_main_queue
            .take()
            .ok_or("run() called twice: rx_main_queue already taken")?;
        let rx_batch = self
            .rx_batch
            .take()
            .ok_or("run() called twice: rx_batch already taken")?;

        let rx_local_publish = self.take_local_publish_receiver().await;

        let local_publish_task = if let Some(rx) = rx_local_publish {
            let retained_store = {
                let ctrl = self.controller.read().await;
                Arc::clone(&ctrl.stores().retained)
            };
            let retained_writer = LocalRetainedWriter::new(
                Arc::clone(&broker.router),
                retained_store,
                Arc::clone(&self.retained_echoes),
            );
            Some(tokio::spawn(Self::run_local_publish_loop(
                admin_client.clone(),
                Arc::clone(&broker.router),
                retained_writer,
                rx,
            )))
        } else {
            None
        };

        #[cfg(feature = "http-api")]
        self.spawn_http_task(service_username, service_password);
        #[cfg(not(feature = "http-api"))]
        let _ = (service_username, service_password);

        self.recover_pending_lwts().await;
        self.controller.write().await.load_stored_change_events();
        self.recover_pending_outbox().await;

        loop {
            tokio::select! {
                biased;

                _ = shutdown_rx.recv() => {
                    info!("cluster node shutting down");
                    break;
                }
                _ = tick_interval.tick() => {
                    Box::pin(self.handle_tick(&tx_tick)).await;
                }
                Ok(batch) = rx_batch.recv_async() => {
                    self.handle_processing_batch(batch).await;
                }
                Ok(msg) = rx_main_queue.recv_async() => {
                    self.handle_main_queue_message(msg, &rx_main_queue).await;
                }
                _ = ttl_cleanup_interval.tick() => {
                    self.handle_ttl_cleanup().await;
                }
                _ = cleanup_interval.tick() => {
                    self.handle_session_cleanup().await;
                }
                _ = subscription_reconciliation_interval.tick() => {
                    self.handle_subscription_reconciliation().await;
                }
                _ = mesh_check_interval.tick() => {
                    self.warn_on_unlinked_nodes().await;
                    self.redial_disconnected_peers().await;
                }
                _ = retained_sync_cleanup_interval.tick() => {
                    self.prune_retained_echoes();
                }
                _ = cascade_retry_interval.tick() => {
                    self.retry_pending_cascades().await;
                }
                Ok(req) = admin_rx.recv_async() => {
                    self.handle_admin_request(&admin_client, req).await;
                }
                _ = license_check_interval.tick() => {
                    self.handle_license_check();
                }
                _ = unique_reconcile_interval.tick() => {
                    self.handle_unique_reconcile().await;
                }
            }
        }

        if let Some(task) = local_publish_task {
            task.abort();
        }
        if let Some(manager) = broker.bridge_manager
            && let Err(e) = manager.stop_all().await
        {
            warn!(error = %e, "failed to stop bridges");
        }
        broker.handle.abort();
        match self.fatal_error.get() {
            Some(reason) => Err(reason.clone().into()),
            None => Ok(()),
        }
    }

    async fn handle_tick(&self, tx_tick: &flume::Sender<u64>) {
        let now = current_time_ms();
        let _ = tx_tick.try_send(now);

        let raft_partition_map = self.rx_partition_map.borrow().clone();

        let mut ctrl = self.controller.write().await;
        let became_primary = ctrl.apply_raft_partition_map(raft_partition_map).await;
        if became_primary {
            let stores = ctrl.stores();
            let result =
                stores
                    .subscriptions
                    .reconcile(&stores.topics, &stores.wildcards, |partition| {
                        ctrl.can_serve_reads(partition)
                    });
            if result.index_entries_restored > 0 {
                info!(
                    clients = result.clients_checked,
                    restored = result.index_entries_restored,
                    "reconciled subscriptions after partition takeover"
                );
            }
        }
        let raft_status = self.rx_raft_status.borrow().clone();
        ctrl.manage_unique_voters(raft_status.is_leader, raft_status.current_term, now);

        ctrl.transport().log_queue_stats();
        ctrl.pending_constraints().sweep_closed();
        ctrl.sweep_unique_quorum(now).await;
        ctrl.sweep_unique_seals(now);
        ctrl.drain_pending_seals().await;
        ctrl.drive_change_events(now).await;
    }

    async fn handle_processing_batch(&self, batch: ProcessingBatch) {
        if let Some(hb) = batch.heartbeat_to_send {
            let ctrl = self.controller.read().await;
            ctrl.broadcast_heartbeat(hb).await;
        }

        if !batch.heartbeat_updates.is_empty() {
            let mut ctrl = self.controller.write().await;
            ctrl.apply_heartbeat_updates(&batch.heartbeat_updates);
        }

        if !batch.dead_nodes.is_empty() {
            let mut ctrl = self.controller.write().await;
            ctrl.apply_dead_nodes(&batch.dead_nodes);
            let dead_nodes: Vec<NodeId> = ctrl.drain_dead_nodes_for_session_update().collect();

            for dead_node in &dead_nodes {
                let affected_sessions = ctrl.stores().sessions.sessions_on_node(*dead_node);
                if !affected_sessions.is_empty() {
                    info!(
                        node = dead_node.get(),
                        sessions = affected_sessions.len(),
                        "marking sessions disconnected due to node death"
                    );
                    let now = current_time_ms();
                    let mut session_count = 0u32;
                    for session in affected_sessions {
                        let client_id = session.client_id_str();
                        let result = ctrl.stores_mut().update_session_replicated(client_id, |s| {
                            s.set_connected(false, *dead_node, now);
                        });
                        if let Ok((_session, write)) = result {
                            ctrl.write_or_forward(write).await;
                        }
                        session_count += 1;
                        if session_count.is_multiple_of(8) {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }

        if !batch.dead_nodes.is_empty() {
            self.redial_disconnected_peers().await;
        }
    }

    fn try_resolve_constraint_response(&self, msg: &crate::cluster::InboundMessage) -> bool {
        match &msg.message {
            ClusterMessage::UniqueReserveResponse(resp) => {
                self.pending_constraints.resolve_unique(resp);
                true
            }
            ClusterMessage::FkCheckResponse(resp) => {
                self.pending_constraints.resolve_fk_check(resp);
                true
            }
            ClusterMessage::FkReverseLookupResponse(resp) => {
                self.pending_constraints.resolve_fk_lookup(resp);
                true
            }
            _ => false,
        }
    }

    async fn handle_main_queue_message(
        &self,
        msg: crate::cluster::InboundMessage,
        rx_main_queue: &flume::Receiver<crate::cluster::InboundMessage>,
    ) {
        const BATCH_SIZE: u32 = 8;

        if self.try_resolve_constraint_response(&msg) {
            let mut count = 1u32;
            while let Ok(msg) = rx_main_queue.try_recv() {
                if !self.try_resolve_constraint_response(&msg) {
                    let mut ctrl = self.controller.write().await;
                    if let Some(pending) = ctrl.handle_filtered_message(msg).await {
                        drop(ctrl);
                        Self::spawn_constraint_completion(self.controller.clone(), pending);
                    }
                }
                count += 1;
                if count.is_multiple_of(BATCH_SIZE) {
                    tokio::task::yield_now().await;
                }
            }
            return;
        }

        let mut ctrl = self.controller.write().await;
        if let Some(pending) = ctrl.handle_filtered_message(msg).await {
            drop(ctrl);
            Self::spawn_constraint_completion(self.controller.clone(), pending);
            let mut ctrl = self.controller.write().await;
            let mut count = 1u32;
            while let Ok(msg) = rx_main_queue.try_recv() {
                if self.try_resolve_constraint_response(&msg) {
                    continue;
                }
                if let Some(pending) = ctrl.handle_filtered_message(msg).await {
                    drop(ctrl);
                    Self::spawn_constraint_completion(self.controller.clone(), pending);
                    ctrl = self.controller.write().await;
                }
                count += 1;
                if count.is_multiple_of(BATCH_SIZE) {
                    tokio::task::yield_now().await;
                }
            }
            return;
        }
        let mut count = 1u32;
        while let Ok(msg) = rx_main_queue.try_recv() {
            if self.try_resolve_constraint_response(&msg) {
                continue;
            }
            if let Some(pending) = ctrl.handle_filtered_message(msg).await {
                drop(ctrl);
                Self::spawn_constraint_completion(self.controller.clone(), pending);
                ctrl = self.controller.write().await;
            }
            count += 1;
            if count.is_multiple_of(BATCH_SIZE) {
                tokio::task::yield_now().await;
            }
        }
    }

    fn spawn_constraint_completion(
        controller: Arc<tokio::sync::RwLock<NodeController<super::ClusterTransportKind>>>,
        pending: crate::cluster::node_controller::PendingConstraintWork,
    ) {
        use crate::cluster::node_controller::PendingConstraintWork;

        match pending {
            PendingConstraintWork::Unique(unique_work) => {
                Self::spawn_unique_completion(controller, unique_work);
            }
            PendingConstraintWork::Fk(fk_work) => {
                Self::spawn_fk_completion(controller, fk_work);
            }
            PendingConstraintWork::FkDelete(fk_delete_work) => {
                Self::spawn_fk_delete_completion(controller, fk_delete_work);
            }
        }
    }

    fn spawn_unique_completion(
        controller: Arc<tokio::sync::RwLock<NodeController<super::ClusterTransportKind>>>,
        pending: crate::cluster::node_controller::PendingUniqueWork,
    ) {
        use crate::cluster::node_controller::unique::{await_unique_quorum, await_unique_reserves};

        let local_reserved = pending.phase1.local_reserved;
        let pending_remote = pending.phase1.pending_remote;
        let pending_quorum = pending.phase1.pending_quorum;
        let continuation = pending.continuation;
        tokio::spawn(async move {
            use crate::cluster::node_controller::ReserveFailure;
            // Await the remote-reserve and local-quorum rounds concurrently so a create waits the
            // max of the two ~5s deadlines, not their sum.
            let remote_results = match tokio::join!(
                await_unique_reserves(pending_remote),
                await_unique_quorum(pending_quorum),
            ) {
                (Ok(confirmed), Ok(durable)) => {
                    tracing::debug!(?durable, "unique reserve is majority-durable");
                    Ok(confirmed)
                }
                (Ok(confirmed), Err(field)) => Err((ReserveFailure::NotDurable(field), confirmed)),
                (Err(e), _) => Err(e),
            };
            let mut ctrl = controller.write().await;
            ctrl.complete_pending_unique_work(local_reserved, remote_results, continuation)
                .await;
        });
    }

    fn spawn_fk_completion(
        controller: Arc<tokio::sync::RwLock<NodeController<super::ClusterTransportKind>>>,
        pending: crate::cluster::node_controller::PendingFkWork,
    ) {
        use crate::cluster::node_controller::fk::await_fk_checks;

        let pending_checks = pending.pending_checks;
        let continuation = pending.continuation;
        tokio::spawn(async move {
            use crate::cluster::node_controller::FkThenUniqueOutcome;
            let fk_result = await_fk_checks(pending_checks).await;
            let outcome = {
                let mut ctrl = controller.write().await;
                let (fk_ok, fk_error) = match fk_result {
                    Ok(()) => (true, None),
                    Err(msg) => (false, Some(msg)),
                };
                ctrl.complete_pending_fk_work(fk_ok, fk_error, continuation)
                    .await
                // controller write lock is released here, before the majority-await below.
            };
            if let FkThenUniqueOutcome::NeedUnique(pending) = outcome {
                Self::spawn_unique_completion(controller, *pending);
            }
        });
    }

    fn spawn_fk_delete_completion(
        controller: Arc<tokio::sync::RwLock<NodeController<super::ClusterTransportKind>>>,
        pending: crate::cluster::node_controller::PendingFkDeleteWork,
    ) {
        use crate::cluster::node_controller::fk::collect_recursive_cascade;

        let local_results = pending.local_results;
        let pending_lookups = pending.pending_lookups;
        let continuation = pending.continuation;
        let (del_entity, del_id) = continuation.entity_id();
        let (del_entity, del_id) = (del_entity.to_string(), del_id.to_string());
        let sender_owned = continuation.sender_ref().map(String::from);
        tokio::spawn(async move {
            let (restrict_error, side_effects) = collect_recursive_cascade(
                &controller,
                &del_entity,
                &del_id,
                local_results,
                pending_lookups,
                sender_owned.as_deref(),
            )
            .await;
            let mut ctrl = controller.write().await;
            ctrl.complete_pending_fk_delete_work(restrict_error, side_effects, continuation)
                .await;
        });
    }

    async fn handle_ttl_cleanup(&self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut ctrl = self.controller.write().await;
        let reaped = ctrl.reap_expired_ttl(now_secs).await;
        if reaped > 0 {
            info!(count = reaped, "cleaned up TTL-expired entities");
        }
    }

    async fn handle_session_cleanup(&self) {
        let now = current_time_ms();
        let expired_sessions = {
            let ctrl = self.controller.read().await;
            let expired_idempotency = ctrl.stores().idempotency.cleanup_expired(now);
            if expired_idempotency > 0 {
                info!(
                    expired_idempotency,
                    "cleaned up expired idempotency records"
                );
            }
            let stale_offsets = ctrl.stores().cleanup_stale_offsets(now);
            if stale_offsets > 0 {
                info!(stale_offsets, "cleaned up stale consumer offsets");
            }
            ctrl.stores().cleanup_expired_sessions(now)
        };
        if !expired_sessions.is_empty() {
            info!(
                count = expired_sessions.len(),
                "cleaning up expired sessions and subscriptions"
            );
            let mut ctrl = self.controller.write().await;
            for session in &expired_sessions {
                let client_id = session.client_id_str();
                clear_expired_session_subscriptions(&mut ctrl, client_id).await;
            }
        }
    }

    async fn handle_unique_reconcile(&self) {
        // Gather record keys (cheap, no JSON parse) under one brief read lock, then per chunk parse
        // the records under a read lock and apply under a write lock. The expensive parse and the
        // reassert sends are chunked, so no lock is held across them — only the cheap key-gather
        // scans the store under a read lock; otherwise every DB op on the node would stall for the
        // whole parse.
        const RECONCILE_CHUNK: usize = 64;

        let keys = {
            let ctrl = self.controller.read().await;
            ctrl.collect_unique_reconcile_keys()
        };
        for key_chunk in keys.chunks(RECONCILE_CHUNK) {
            let work = {
                let ctrl = self.controller.read().await;
                ctrl.project_unique_reconcile_work(key_chunk)
            };
            let mut ctrl = self.controller.write().await;
            ctrl.apply_unique_reconcile_chunk(&work).await;
        }

        let now = current_time_ms();
        let expired_unique = {
            let ctrl = self.controller.read().await;
            ctrl.stores().unique_cleanup_expired(now)
        };
        if expired_unique > 0 {
            info!(expired_unique, "reclaimed abandoned unique reservations");
        }
    }

    async fn redial_disconnected_peers(&self) {
        let (quic, linked) = {
            let ctrl = self.controller.read().await;
            (
                ctrl.transport().as_quic().cloned(),
                ctrl.alive_or_suspected_nodes(),
            )
        };
        if let Some(quic) = quic {
            tokio::spawn(async move {
                quic.redial_unlinked(&linked).await;
            });
        }
    }

    async fn warn_on_unlinked_nodes(&self) {
        let unlinked = {
            let ctrl = self.controller.read().await;
            ctrl.unlinked_nodes().await
        };
        if unlinked.is_empty() {
            return;
        }
        let nodes: Vec<u16> = unlinked.iter().map(|node| node.get()).collect();
        tracing::warn!(
            unlinked_nodes = ?nodes,
            "cluster mesh is incomplete: these known nodes have no direct connection to this node. \
             Broadcasts are a single hop and targeted sends are not routed, so subscriptions, \
             presence, client locations and cross-node publishes will not reach them. \
             Start every node with --peers listing all other nodes."
        );
    }

    async fn handle_subscription_reconciliation(&self) {
        let now = current_time_ms();
        let ctrl = self.controller.read().await;
        let stores = ctrl.stores();
        if stores.subscriptions.needs_reconciliation(now) {
            let result =
                stores
                    .subscriptions
                    .reconcile(&stores.topics, &stores.wildcards, |partition| {
                        ctrl.can_serve_reads(partition)
                    });
            if result.index_entries_restored > 0 {
                info!(
                    clients = result.clients_checked,
                    restored = result.index_entries_restored,
                    "reconciled subscription cache"
                );
            }
            stores.subscriptions.mark_reconciliation(now);
        }
    }

    fn prune_retained_echoes(&self) {
        let removed = self
            .retained_echoes
            .prune(Duration::from_secs(RETAINED_ECHO_MAX_AGE_SECS));
        if removed > 0 {
            warn!(
                removed,
                remaining = self.retained_echoes.pending_count(),
                "dropped retained echoes the broker never reported"
            );
        }
    }

    fn handle_license_check(&self) {
        if let Some(expires_at) = self.license_expires_at
            && mqdb_core::license::LicenseInfo::check_runtime_expiry(expires_at)
        {
            tracing::error!("license has expired — shutting down");
            let _ = self.shutdown_tx.send(());
        }
    }

    async fn run_local_publish_loop(
        admin_client: mqtt5::MqttClient,
        router: Arc<MessageRouter>,
        retained_writer: LocalRetainedWriter,
        rx: flume::Receiver<LocalPublishRequest>,
    ) {
        const MAX_CONCURRENT: usize = 256;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let routed = RoutedDeliveryLanes::spawn(&router);

        while let Ok(req) = rx.recv_async().await {
            Self::dispatch_local_publish(&admin_client, &routed, &retained_writer, &semaphore, req)
                .await;
        }
    }

    async fn dispatch_local_publish(
        admin_client: &mqtt5::MqttClient,
        routed: &RoutedDeliveryLanes,
        retained_writer: &LocalRetainedWriter,
        semaphore: &Arc<tokio::sync::Semaphore>,
        req: LocalPublishRequest,
    ) {
        match req.kind {
            LocalPublishKind::Plain => {
                Self::spawn_local_publish(admin_client, semaphore, req).await
            }
            LocalPublishKind::Routed => routed.deliver(req).await,
            LocalPublishKind::Retained => {
                retained_writer
                    .write(&req.topic, &req.payload, req.qos)
                    .await;
            }
            LocalPublishKind::RetainedRefresh => retained_writer.refresh(&req.topic).await,
        }
    }

    async fn spawn_local_publish(
        admin_client: &mqtt5::MqttClient,
        semaphore: &Arc<tokio::sync::Semaphore>,
        req: LocalPublishRequest,
    ) {
        let Ok(permit) = Arc::clone(semaphore).acquire_owned().await else {
            return;
        };
        let client = admin_client.clone();
        tokio::spawn(async move {
            let mut options = mqtt5::PublishOptions {
                qos: mqtt5::QoS::from(req.qos),
                ..Default::default()
            };
            options.properties.user_properties = req.user_properties;
            if let Err(e) = client
                .publish_with_options(&req.topic, req.payload, options)
                .await
            {
                warn!(error = %e, topic = %req.topic, "failed to publish local request");
            }
            drop(permit);
        });
    }

    async fn recover_pending_outbox(&self) {
        let ctrl = self.controller.read().await;
        let Some(outbox) = ctrl.stores().cluster_outbox() else {
            return;
        };
        let entries = match outbox.pending_events() {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "failed to scan outbox for pending events");
                return;
            }
        };
        if !entries.is_empty() {
            info!(count = entries.len(), "replaying pending outbox entries");
            for entry in &entries {
                ctrl.deliver_change_event(crate::cluster::RoutedPublish {
                    topic: entry.topic.clone(),
                    payload: entry.payload.clone(),
                    qos: 1,
                    user_properties: entry.user_properties.clone(),
                    order_key: None,
                })
                .await;
                let _ = outbox.mark_delivered(&entry.operation_id);
            }
        }

        let cascade_entries = match outbox.pending_cascade_ops() {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "failed to scan cascade outbox for pending ops");
                return;
            }
        };
        if cascade_entries.is_empty() {
            return;
        }
        info!(
            count = cascade_entries.len(),
            "replaying pending cascade operations"
        );
        drop(ctrl);
        self.controller
            .write()
            .await
            .replay_cascade_entries(&cascade_entries)
            .await;
    }

    async fn retry_pending_cascades(&self) {
        let ctrl = self.controller.read().await;
        let Some(outbox) = ctrl.stores().cluster_outbox() else {
            return;
        };
        let Ok(cascade_entries) = outbox.pending_cascade_ops() else {
            return;
        };
        if cascade_entries.is_empty() {
            return;
        }
        tracing::debug!(
            count = cascade_entries.len(),
            "retrying pending cascade operations"
        );
        drop(ctrl);
        self.controller
            .write()
            .await
            .replay_cascade_entries(&cascade_entries)
            .await;
    }

    async fn recover_pending_lwts(&self) {
        use crate::cluster::{ForwardTarget, ForwardedPublish, LwtPublisher, PublishRouter};

        let ctrl = self.controller.read().await;
        let lwt_publisher = LwtPublisher::new(&ctrl.stores().sessions);
        let pending = lwt_publisher.recover_pending_lwts();

        if pending.is_empty() {
            return;
        }

        info!(
            count = pending.len(),
            "recovering pending LWTs from previous crash"
        );

        let router = PublishRouter::new(&ctrl.stores().topics);
        let transport = ctrl.transport().clone();
        let node_id = self.node_id;

        for lwt in pending {
            let wildcards = ctrl.stores().wildcards.match_topic(&lwt.topic);
            let route = router.route_with_wildcards(&lwt.topic, &wildcards);

            let remote_nodes = ctrl.remote_targets(
                route
                    .targets
                    .into_iter()
                    .map(|target| ForwardTarget::new(target.client_id, target.qos)),
            );

            for (target_node, targets) in remote_nodes {
                let fwd = ForwardedPublish::new(
                    node_id,
                    lwt.topic.clone(),
                    lwt.qos,
                    lwt.retain,
                    lwt.payload.clone(),
                    targets,
                );
                let fwd_msg = ClusterMessage::ForwardedPublish(fwd);
                if let Err(e) = transport.send(target_node, fwd_msg).await {
                    warn!(target = target_node.get(), error = %e, "failed to forward recovered LWT");
                } else {
                    debug!(target = target_node.get(), topic = %lwt.topic, "forwarded recovered LWT to node");
                }
            }

            if let Err(e) = lwt_publisher.complete_lwt(&lwt.client_id, lwt.token) {
                warn!(client_id = %lwt.client_id, error = %e, "failed to mark recovered LWT as published");
            } else {
                info!(client_id = %lwt.client_id, topic = %lwt.topic, "recovered and published pending LWT");
            }
        }
    }
}

pub(super) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() * 1000 + u64::from(d.subsec_millis()))
}

pub(super) async fn clear_expired_session_subscriptions<T: ClusterTransport>(
    ctrl: &mut NodeController<T>,
    client_id: &str,
) {
    let snapshot = ctrl.stores().subscriptions.get_snapshot(client_id);
    if let Some(snapshot) = snapshot {
        for entry in &snapshot.topics {
            let topic = std::str::from_utf8(&entry.topic).unwrap_or("");
            if topic.is_empty() {
                continue;
            }

            let is_wildcard = entry.is_wildcard != 0;
            if is_wildcard {
                let result = ctrl
                    .stores_mut()
                    .unsubscribe_wildcard_replicated(topic, client_id);
                if result.is_ok() {
                    let broadcast = WildcardBroadcast::unsubscribe(topic, client_id);
                    let msg = ClusterMessage::WildcardBroadcast(broadcast);
                    let _ = ctrl.transport().broadcast(msg).await;
                }
            } else {
                let _ = ctrl.stores_mut().topics.unsubscribe(topic, client_id);
                let broadcast = TopicSubscriptionBroadcast::unsubscribe(topic, client_id);
                let msg = ClusterMessage::TopicSubscriptionBroadcast(broadcast);
                let _ = ctrl.transport().broadcast(msg).await;
            }

            let result = ctrl
                .stores_mut()
                .remove_subscription_replicated(client_id, topic);
            if let Ok((_snapshot, write)) = result {
                ctrl.write_or_forward(write).await;
            }
        }
    }

    for write in ctrl.stores_mut().clear_qos2_client_replicated(client_id) {
        ctrl.write_or_forward(write).await;
    }
    for write in ctrl
        .stores_mut()
        .clear_inflight_client_replicated(client_id)
    {
        ctrl.write_or_forward(write).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{RetainedEchoes, RetainedStore, subscribe_test_client};

    fn retained_request(topic: &str, payload: &[u8]) -> LocalPublishRequest {
        LocalPublishRequest {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos: 1,
            kind: LocalPublishKind::Retained,
            user_properties: Vec::new(),
            order_key: None,
        }
    }

    fn routed_request(topic: &str, payload: Vec<u8>) -> LocalPublishRequest {
        LocalPublishRequest {
            topic: topic.to_string(),
            payload,
            qos: 1,
            kind: LocalPublishKind::Routed,
            user_properties: Vec::new(),
            order_key: None,
        }
    }

    fn retained_writer_for(router: &Arc<MessageRouter>) -> LocalRetainedWriter {
        LocalRetainedWriter::new(
            Arc::clone(router),
            Arc::new(RetainedStore::new(NodeId::validated(1).unwrap())),
            Arc::new(RetainedEchoes::new()),
        )
    }

    #[tokio::test]
    async fn forwarded_publish_is_delivered_by_the_router_without_the_admin_client() {
        let admin_client = mqtt5::MqttClient::new("mqdb-admin-test");
        let router = Arc::new(MessageRouter::new());
        let mut receivers = subscribe_test_client(&router, "dashboard", "plant/temp", 8).await;
        let retained_writer = retained_writer_for(&router);
        let routed = RoutedDeliveryLanes::spawn(&router);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

        ClusteredAgent::dispatch_local_publish(
            &admin_client,
            &routed,
            &retained_writer,
            &semaphore,
            routed_request("plant/temp", b"21.5".to_vec()),
        )
        .await;

        let delivered = tokio::time::timeout(Duration::from_secs(1), receivers.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&delivered.publish.payload[..], b"21.5");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forwarded_publishes_on_one_topic_are_delivered_in_order() {
        const MESSAGES: usize = 2000;
        let admin_client = mqtt5::MqttClient::new("mqdb-admin-test");
        let router = Arc::new(MessageRouter::new());
        let mut receivers =
            subscribe_test_client(&router, "dashboard", "plant/temp", MESSAGES).await;
        let retained_writer = retained_writer_for(&router);
        let routed = RoutedDeliveryLanes::spawn(&router);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(256));

        for sequence in 0..MESSAGES {
            let sequence = u32::try_from(sequence).unwrap();
            ClusteredAgent::dispatch_local_publish(
                &admin_client,
                &routed,
                &retained_writer,
                &semaphore,
                routed_request("plant/temp", sequence.to_be_bytes().to_vec()),
            )
            .await;
        }

        let mut received = [0u32; MESSAGES];
        for slot in &mut received {
            let delivered = tokio::time::timeout(Duration::from_secs(5), receivers.recv())
                .await
                .unwrap()
                .unwrap();
            *slot = u32::from_be_bytes(delivered.publish.payload[..].try_into().unwrap());
        }
        let expected: [u32; MESSAGES] = std::array::from_fn(|i| u32::try_from(i).unwrap());
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn retained_local_publish_is_written_to_the_router_without_the_admin_client() {
        let admin_client = mqtt5::MqttClient::new("mqdb-admin-test");
        let router = Arc::new(MessageRouter::new());
        let retained_writer = retained_writer_for(&router);
        let routed = RoutedDeliveryLanes::spawn(&router);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

        ClusteredAgent::dispatch_local_publish(
            &admin_client,
            &routed,
            &retained_writer,
            &semaphore,
            retained_request("$DB/_presence/c1", b"connect"),
        )
        .await;

        let stored = router.get_retained_messages("$DB/_presence/c1").await;
        assert_eq!(stored.len(), 1);
        assert_eq!(&stored[0].payload[..], b"connect");

        ClusteredAgent::dispatch_local_publish(
            &admin_client,
            &routed,
            &retained_writer,
            &semaphore,
            retained_request("$DB/_presence/c1", b""),
        )
        .await;
        assert_eq!(router.retained_count().await, 0);
    }
}
