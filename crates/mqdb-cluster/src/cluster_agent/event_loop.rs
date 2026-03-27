// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{AdminRequest, ClusteredAgent};
use crate::cluster::{
    ClusterMessage, ClusterTransport, NodeController, NodeId, PartitionId, ProcessingBatch,
    TopicSubscriptionBroadcast, WildcardBroadcast,
};
use mqtt5::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::interval;
use tracing::{debug, info, warn};

use super::{
    CLEANUP_INTERVAL_SECS, RETAINED_SYNC_CLEANUP_INTERVAL_SECS, RETAINED_SYNC_TTL_SECS,
    TTL_CLEANUP_INTERVAL_SECS,
};

impl ClusteredAgent {
    /// # Errors
    /// If broker setup, transport connection, or event loop execution fails.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let synced_retained_topics = self.initialize_event_handler().await;

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

        let (broker_handle, _bridge_manager) = self
            .start_broker(broker, &bridge_configs, use_external_bridge_manager)
            .await;

        let admin_client = self
            .connect_transport(service_username.as_ref(), service_password.as_ref())
            .await?;

        let (admin_tx, admin_rx) = flume::bounded::<AdminRequest>(32);
        super::broker::subscribe_admin_topics(&admin_client, &admin_tx).await?;

        self.setup_and_spawn_raft().await?;

        Box::pin(self.run_event_loop(
            synced_retained_topics,
            broker_handle,
            admin_client,
            admin_rx,
            service_username.as_ref(),
            service_password.as_ref(),
        ))
        .await
    }

    async fn run_event_loop(
        &mut self,
        synced_retained_topics: Arc<tokio::sync::RwLock<HashMap<String, std::time::Instant>>>,
        broker_handle: tokio::task::JoinHandle<()>,
        admin_client: mqtt5::MqttClient,
        admin_rx: flume::Receiver<AdminRequest>,
        service_username: Option<&String>,
        service_password: Option<&String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tick_interval = interval(Duration::from_millis(10));
        let mut cleanup_interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        let mut ttl_cleanup_interval = interval(Duration::from_secs(TTL_CLEANUP_INTERVAL_SECS));
        let mut wildcard_reconciliation_interval = interval(Duration::from_secs(60));
        let mut subscription_reconciliation_interval = interval(Duration::from_secs(300));
        let mut retained_sync_cleanup_interval =
            interval(Duration::from_secs(RETAINED_SYNC_CLEANUP_INTERVAL_SECS));
        let mut cascade_retry_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let mut license_check_interval = interval(Duration::from_secs(3600));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
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
            let client = admin_client.clone();
            Some(tokio::spawn(Self::run_local_publish_loop(client, rx)))
        } else {
            None
        };

        let _http_task = self.spawn_http_task(service_username, service_password);

        self.recover_pending_lwts().await;
        self.recover_pending_outbox().await;

        loop {
            tokio::select! {
                biased;

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
                _ = wildcard_reconciliation_interval.tick() => {
                    self.handle_wildcard_reconciliation().await;
                }
                _ = subscription_reconciliation_interval.tick() => {
                    self.handle_subscription_reconciliation().await;
                }
                _ = retained_sync_cleanup_interval.tick() => {
                    Self::handle_retained_sync_cleanup(&synced_retained_topics).await;
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
                _ = shutdown_rx.recv() => {
                    info!("cluster node shutting down");
                    break;
                }
            }
        }

        if let Some(task) = local_publish_task {
            task.abort();
        }
        broker_handle.abort();
        Ok(())
    }

    async fn handle_tick(&self, tx_tick: &flume::Sender<u64>) {
        let now = current_time_ms();
        let _ = tx_tick.try_send(now);

        let raft_partition_map = self.rx_partition_map.borrow().clone();

        let mut ctrl = self.controller.write().await;
        let current_map = ctrl.partition_map().clone();
        let mut became_primary = false;
        if current_map != raft_partition_map {
            let mut changes = 0u32;
            for partition in PartitionId::all() {
                let new_assignment = raft_partition_map.get(partition);
                let old_assignment = current_map.get(partition);

                if new_assignment != old_assignment {
                    let is_primary = new_assignment.primary == Some(self.node_id);
                    let is_replica = new_assignment.replicas.contains(&self.node_id);
                    let was_primary = old_assignment.primary == Some(self.node_id);

                    if is_primary {
                        ctrl.become_primary(partition, new_assignment.epoch);
                        if !was_primary {
                            became_primary = true;
                        }
                    } else if is_replica {
                        let role = ctrl.role(partition);
                        let had_data = matches!(
                            role,
                            crate::cluster::ReplicaRole::Primary
                                | crate::cluster::ReplicaRole::Replica
                        );
                        if had_data {
                            let seq = ctrl.sequence(partition).unwrap_or(0);
                            ctrl.become_replica(partition, new_assignment.epoch, seq);
                        } else if let Some(primary) = new_assignment.primary {
                            ctrl.become_replica_with_snapshot(
                                partition,
                                new_assignment.epoch,
                                primary,
                            )
                            .await;
                        }
                    }
                    changes += 1;
                    if changes.is_multiple_of(8) {
                        tokio::task::yield_now().await;
                    }
                }
            }
            ctrl.update_partition_map(raft_partition_map);
        }
        if became_primary {
            let stores = ctrl.stores();
            let result = stores
                .subscriptions
                .reconcile(&stores.topics, &stores.wildcards);
            if result.subscriptions_added > 0 || result.subscriptions_removed > 0 {
                info!(
                    clients = result.clients_checked,
                    added = result.subscriptions_added,
                    removed = result.subscriptions_removed,
                    "reconciled subscriptions after partition takeover"
                );
            }
        }
        ctrl.transport().log_queue_stats();
        ctrl.pending_constraints().sweep_closed();
    }

    async fn handle_processing_batch(&self, batch: ProcessingBatch) {
        if let Some(hb) = batch.heartbeat_to_send {
            let ctrl = self.controller.read().await;
            let _ = ctrl.transport().broadcast(hb).await;
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
        use crate::cluster::node_controller::unique::await_unique_reserves;

        let local_reserved = pending.phase1.local_reserved;
        let pending_remote = pending.phase1.pending_remote;
        let continuation = pending.continuation;
        tokio::spawn(async move {
            let remote_results = await_unique_reserves(pending_remote).await;
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
            let fk_result = await_fk_checks(pending_checks).await;
            let mut ctrl = controller.write().await;
            let (fk_ok, fk_error) = match fk_result {
                Ok(()) => (true, None),
                Err(msg) => (false, Some(msg)),
            };
            ctrl.complete_pending_fk_work(fk_ok, fk_error, continuation)
                .await;
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
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ctrl = self.controller.read().await;
        let expired_ttl = ctrl.stores().db_data.cleanup_expired_ttl(now_secs);
        if !expired_ttl.is_empty() {
            info!(count = expired_ttl.len(), "cleaned up TTL-expired entities");
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
            let expired_unique = ctrl.stores().unique_cleanup_expired(now);
            if expired_unique > 0 {
                info!(expired_unique, "cleaned up expired unique reservations");
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

    async fn handle_wildcard_reconciliation(&self) {
        let now = current_time_ms();
        let ctrl = self.controller.read().await;
        let pending_store = &ctrl.stores().wildcard_pending;
        if pending_store.needs_reconciliation(now) {
            let pending = pending_store.get_pending_for_retry();
            if !pending.is_empty() {
                debug!(
                    count = pending.len(),
                    "retrying pending wildcard broadcasts"
                );
                for p in &pending {
                    let broadcast = p.to_broadcast();
                    let msg = ClusterMessage::WildcardBroadcast(broadcast);
                    let _ = ctrl.transport().broadcast(msg).await;
                    pending_store.mark_retried(&p.pattern, &p.client_id);
                }
                info!(
                    count = pending.len(),
                    "rebroadcast pending wildcard subscriptions"
                );
            }
            pending_store.mark_reconciliation(now);
            let max_age_ms = 5 * 60 * 1000;
            let removed = pending_store.clear_old_entries(now, max_age_ms);
            if removed > 0 {
                debug!(removed, "cleared old wildcard pending entries");
            }
        }
    }

    async fn handle_subscription_reconciliation(&self) {
        let now = current_time_ms();
        let ctrl = self.controller.read().await;
        let stores = ctrl.stores();
        if stores.subscriptions.needs_reconciliation(now) {
            let result = stores
                .subscriptions
                .reconcile(&stores.topics, &stores.wildcards);
            if result.subscriptions_added > 0 || result.subscriptions_removed > 0 {
                info!(
                    clients = result.clients_checked,
                    added = result.subscriptions_added,
                    removed = result.subscriptions_removed,
                    "reconciled subscription cache"
                );
            }
            stores.subscriptions.mark_reconciliation(now);
        }
    }

    async fn handle_retained_sync_cleanup(
        synced_retained_topics: &Arc<tokio::sync::RwLock<HashMap<String, std::time::Instant>>>,
    ) {
        let ttl = std::time::Duration::from_secs(RETAINED_SYNC_TTL_SECS);
        let mut synced = synced_retained_topics.write().await;
        let before_len = synced.len();
        synced.retain(|_, insert_time| insert_time.elapsed() < ttl);
        let removed = before_len - synced.len();
        if removed > 0 {
            debug!(
                removed,
                remaining = synced.len(),
                "cleaned up stale retained sync entries"
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
        rx: flume::Receiver<crate::cluster::LocalPublishRequest>,
    ) {
        const MAX_CONCURRENT: usize = 256;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));

        while let Ok(req) = rx.recv_async().await {
            Self::spawn_local_publish(&admin_client, &semaphore, req).await;

            while let Ok(req) = rx.try_recv() {
                Self::spawn_local_publish(&admin_client, &semaphore, req).await;
            }
        }
    }

    async fn spawn_local_publish(
        admin_client: &mqtt5::MqttClient,
        semaphore: &Arc<tokio::sync::Semaphore>,
        req: crate::cluster::LocalPublishRequest,
    ) {
        let Ok(permit) = Arc::clone(semaphore).acquire_owned().await else {
            return;
        };
        let client = admin_client.clone();
        tokio::spawn(async move {
            let mut options = mqtt5::PublishOptions {
                qos: mqtt5::QoS::from(req.qos),
                retain: req.retain,
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
                ctrl.transport()
                    .queue_local_publish_with_properties(
                        entry.topic.clone(),
                        entry.payload.clone(),
                        1,
                        entry.user_properties.clone(),
                    )
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
        let local_work = send_cascade_operations(&ctrl, outbox, &cascade_entries).await;
        drop(ctrl);
        if !local_work.is_empty() {
            execute_local_cascade_work(&self.controller, local_work).await;
        }
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
        let local_work = send_cascade_operations(&ctrl, outbox, &cascade_entries).await;
        drop(ctrl);
        if !local_work.is_empty() {
            execute_local_cascade_work(&self.controller, local_work).await;
        }
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

            let mut remote_nodes: HashMap<NodeId, Vec<ForwardTarget>> = HashMap::new();
            for target in route.targets {
                let connected_node = ctrl
                    .stores()
                    .client_locations
                    .get(&target.client_id)
                    .or_else(|| {
                        ctrl.stores()
                            .sessions
                            .get(&target.client_id)
                            .filter(|s| s.connected == 1)
                            .and_then(|s| NodeId::validated(s.connected_node))
                    });

                if let Some(target_node) = connected_node
                    && target_node != node_id
                {
                    remote_nodes
                        .entry(target_node)
                        .or_default()
                        .push(ForwardTarget::new(target.client_id, target.qos));
                }
            }

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

struct LocalCascadeWork {
    operation_id: String,
    ops: Vec<crate::cluster::CascadeRemoteOp>,
}

async fn send_cascade_operations<T: ClusterTransport>(
    ctrl: &NodeController<T>,
    outbox: &crate::cluster::ClusterOutbox,
    entries: &[crate::cluster::CascadePendingEntry],
) -> Vec<LocalCascadeWork> {
    let mut local_work = Vec::new();
    for cascade in entries {
        let mut ack_receivers = Vec::new();
        let mut local_ops = Vec::new();
        for op in &cascade.operations {
            let partition = match op {
                crate::cluster::CascadeRemoteOp::Delete { entity, id }
                | crate::cluster::CascadeRemoteOp::SetNull { entity, id, .. } => {
                    crate::cluster::data_partition(entity, id)
                }
            };
            if ctrl.is_primary_for_partition(partition) {
                local_ops.push(op.clone());
                continue;
            }
            match op {
                crate::cluster::CascadeRemoteOp::Delete { entity, id } => {
                    if let Some(rx) = ctrl
                        .send_cascade_request(
                            partition,
                            crate::cluster::JsonDbOp::Delete,
                            entity,
                            id,
                            &[],
                        )
                        .await
                    {
                        ack_receivers.push(rx);
                    }
                }
                crate::cluster::CascadeRemoteOp::SetNull {
                    entity,
                    id,
                    field,
                    expected_value,
                } => {
                    if let Some(rx) = ctrl
                        .send_cascade_set_null_request(partition, entity, id, field, expected_value)
                        .await
                    {
                        ack_receivers.push(rx);
                    }
                }
            }
        }
        if !local_ops.is_empty() {
            local_work.push(LocalCascadeWork {
                operation_id: cascade.operation_id.clone(),
                ops: local_ops,
            });
        }
        crate::cluster::node_controller::db_ops::spawn_cascade_ack_waiter(
            Some(outbox.clone()),
            cascade.operation_id.clone(),
            ack_receivers,
            false,
        );
    }
    local_work
}

async fn execute_local_cascade_work(
    controller: &Arc<tokio::sync::RwLock<NodeController<super::ClusterTransportKind>>>,
    work: Vec<LocalCascadeWork>,
) {
    let mut ctrl = controller.write().await;
    let now_ms = current_time_ms();
    for item in &work {
        for op in &item.ops {
            match op {
                crate::cluster::CascadeRemoteOp::Delete { entity, id } => {
                    if let Ok((db_entity, write)) = ctrl.db_delete_prepare(entity, id) {
                        let data: serde_json::Value = serde_json::from_slice(&db_entity.data)
                            .unwrap_or(serde_json::Value::Null);
                        let event = mqdb_core::events::ChangeEvent::delete(
                            entity.clone(),
                            id.clone(),
                            data,
                        );
                        let outbox =
                            crate::cluster::node_controller::db_ops::build_change_event_outbox(
                                &event,
                            );
                        ctrl.db_commit(write, outbox.clone()).await;
                        ctrl.publish_and_deliver_change_event(event, &outbox.operation_id)
                            .await;
                    }
                }
                crate::cluster::CascadeRemoteOp::SetNull {
                    entity,
                    id,
                    field,
                    expected_value,
                } => {
                    ctrl.execute_local_cascade_set_null(entity, id, field, expected_value, now_ms)
                        .await;
                }
            }
        }
        if let Some(outbox) = ctrl.stores().cluster_outbox() {
            let _ = outbox.mark_cascade_delivered(&item.operation_id);
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
