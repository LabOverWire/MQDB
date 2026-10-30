// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::super::event_delivery::{Shipment, TargetKind};
use super::super::protocol::{
    ChangeEventDelivery, ChangeEventMark, ChangeEventRecord, EventId, ForwardTarget,
    ForwardedPublish, ReplicationWrite,
};
use super::super::store_manager::StoreApplyError;
use super::super::store_manager::outbox::{ClusterOutbox, StoredChangeEvent};
use super::super::transport::{ClusterMessage, ClusterTransport};
use super::super::{NodeId, NodeStatus, PartitionId, PublishRouter, RoutedPublish};
use super::NodeController;

fn routed(record: ChangeEventRecord) -> RoutedPublish {
    RoutedPublish {
        topic: record.topic,
        payload: record.payload,
        qos: 1,
        user_properties: record.user_properties,
        order_key: Some(record.order_key),
        share_groups: None,
    }
}

pub(super) struct WriteChangeEvent {
    pub(super) id: EventId,
    pub(super) record: ChangeEventRecord,
    pub(super) storage_entry: (Vec<u8>, Vec<u8>),
}

impl<T: ClusterTransport> NodeController<T> {
    pub(crate) fn load_stored_change_events(&mut self) {
        let Some(outbox) = self.stores.cluster_outbox() else {
            return;
        };
        match outbox.stored_change_events() {
            Ok(events) => {
                for event in events {
                    self.event_delivery
                        .store(event.partition, event.id, event.record);
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to load stored change events"),
        }
    }

    fn change_event_targets(&self, record: &ChangeEventRecord) -> Vec<NodeId> {
        let wildcards = self.stores.wildcards.match_topic(&record.topic);
        let targets =
            PublishRouter::new(&self.stores.topics).route_targets(&record.topic, &wildcards);
        let mut nodes: Vec<NodeId> = self
            .remote_targets(
                targets
                    .into_iter()
                    .map(|target| ForwardTarget::new(target.client_id, target.qos)),
            )
            .into_keys()
            .collect();
        nodes.push(self.node_id);
        nodes.sort_unstable();
        nodes
    }

    pub(super) fn change_event_entry(write: &ReplicationWrite) -> Option<WriteChangeEvent> {
        let record = write.event.clone()?;
        let id = EventId::new(write.epoch.get(), write.sequence);
        let storage_entry = ClusterOutbox::change_event_entry(write.partition, id, &record)?;
        Some(WriteChangeEvent {
            id,
            record,
            storage_entry,
        })
    }

    pub(super) fn apply_replicated_write(
        &mut self,
        write: &ReplicationWrite,
    ) -> Result<(), StoreApplyError> {
        let Some(WriteChangeEvent {
            id,
            record,
            storage_entry,
        }) = Self::change_event_entry(write)
        else {
            return self.stores.apply_write(write);
        };
        self.stores
            .apply_write_with_outbox_entries(write, std::slice::from_ref(&storage_entry))?;
        if let Some(dropped) = self.event_delivery.store(write.partition, id, record) {
            tracing::error!(
                partition = write.partition.get(),
                epoch = dropped.id.epoch,
                sequence = dropped.id.sequence,
                "replica change-event copy full, dropping the oldest"
            );
        }
        Ok(())
    }

    pub(super) async fn take_on_change_event(
        &mut self,
        partition: PartitionId,
        id: EventId,
        record: ChangeEventRecord,
    ) {
        let targets = self.change_event_targets(&record);
        tracing::debug!(
            partition = partition.get(),
            epoch = id.epoch,
            sequence = id.sequence,
            ?targets,
            sending = self.event_delivery.is_sending(partition),
            "change event taken on"
        );
        if let Some(dropped) = self.event_delivery.take_on(partition, id, record, targets) {
            tracing::error!(
                partition = partition.get(),
                epoch = dropped.id.epoch,
                sequence = dropped.id.sequence,
                missed_by = ?dropped.missed_by,
                "too many undelivered change events, dropping the oldest"
            );
        }
        self.ship_change_events(partition, Self::current_time_ms())
            .await;
    }

    async fn ship_change_events(&mut self, partition: PartitionId, now: u64) {
        let local = self.node_id;
        loop {
            let heartbeat = &self.heartbeat;
            let shipments = self.event_delivery.shipments(partition, now, |node| {
                if node == local {
                    TargetKind::Local
                } else if heartbeat.delivers_acked_events(node) {
                    TargetKind::Acked
                } else {
                    TargetKind::Legacy
                }
            });
            let mut delivered_locally = false;
            for shipment in shipments {
                if shipment.kind == TargetKind::Local {
                    let id = shipment.id;
                    if self
                        .deliver_change_event_locally(partition, id, shipment.record)
                        .await
                    {
                        delivered_locally |= self.event_delivery.acked(partition, local, id);
                    }
                } else {
                    self.ship(partition, shipment).await;
                }
            }
            if !delivered_locally {
                break;
            }
        }
    }

    async fn ship(&mut self, partition: PartitionId, shipment: Shipment) {
        let Shipment {
            target,
            kind,
            id,
            record,
        } = shipment;
        let message = match kind {
            TargetKind::Local => return,
            TargetKind::Legacy => ClusterMessage::ForwardedPublish(
                ForwardedPublish::new(
                    self.node_id,
                    record.topic,
                    1,
                    false,
                    record.payload,
                    Vec::new(),
                )
                .with_user_properties(record.user_properties)
                .with_order_key(Some(record.order_key)),
            ),
            TargetKind::Acked => ClusterMessage::ChangeEvent(ChangeEventDelivery {
                partition,
                id,
                record,
            }),
        };
        if let Err(e) = self.transport.send(target, message).await {
            tracing::debug!(
                target = target.get(),
                partition = partition.get(),
                error = %e,
                "change event not sent, will retry"
            );
        }
    }

    async fn deliver_change_event_locally(
        &mut self,
        partition: PartitionId,
        id: EventId,
        record: ChangeEventRecord,
    ) -> bool {
        if !self.event_delivery.is_new(partition, id) {
            tracing::debug!(
                partition = partition.get(),
                epoch = id.epoch,
                sequence = id.sequence,
                "change event already delivered here"
            );
            return true;
        }
        if !self.transport.deliver_routed_publish(routed(record)).await {
            return false;
        }
        tracing::debug!(
            partition = partition.get(),
            epoch = id.epoch,
            sequence = id.sequence,
            "change event delivered here"
        );
        self.event_delivery.mark_delivered(partition, id);
        true
    }

    pub(super) async fn handle_change_event(
        &mut self,
        from: NodeId,
        delivery: ChangeEventDelivery,
    ) {
        let ChangeEventDelivery {
            partition,
            id,
            record,
        } = delivery;
        if !self
            .deliver_change_event_locally(partition, id, record)
            .await
        {
            return;
        }
        let _ = self
            .transport
            .send(
                from,
                ClusterMessage::ChangeEventAck(ChangeEventMark::new(partition, id)),
            )
            .await;
    }

    pub(super) async fn handle_change_event_ack(&mut self, from: NodeId, mark: ChangeEventMark) {
        if self.event_delivery.acked(mark.partition, from, mark.id) {
            self.ship_change_events(mark.partition, Self::current_time_ms())
                .await;
        }
    }

    pub(super) fn handle_change_event_prune(&mut self, mark: ChangeEventMark) {
        if self.event_delivery.is_sending(mark.partition) {
            return;
        }
        self.event_delivery.prune_through(mark.partition, mark.id);
        if let Some(outbox) = self.stores.cluster_outbox()
            && let Err(e) = outbox.remove_change_events_through(mark.partition, mark.id)
        {
            tracing::warn!(partition = mark.partition.get(), error = %e, "failed to prune change events");
        }
    }

    pub async fn drive_change_events(&mut self, now: u64) {
        for partition in self.event_delivery.sending_partitions() {
            self.ship_change_events(partition, now).await;
        }
        for (partition, through) in self.event_delivery.prunes_due(now) {
            if let Some(outbox) = self.stores.cluster_outbox()
                && let Err(e) = outbox.remove_change_events_through(partition, through)
            {
                tracing::warn!(partition = partition.get(), error = %e, "failed to prune change events");
            }
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            for replica in replicas {
                if self.heartbeat.delivers_acked_events(replica) {
                    let _ = self
                        .transport
                        .send(
                            replica,
                            ClusterMessage::ChangeEventPrune(ChangeEventMark::new(
                                partition, through,
                            )),
                        )
                        .await;
                }
            }
        }
    }

    pub(super) fn start_sending_change_events(&mut self, partition: PartitionId) {
        tracing::debug!(partition = partition.get(), "start sending change events");
        let mut delivery = std::mem::take(&mut self.event_delivery);
        delivery.activate(partition, |record| self.change_event_targets(record));
        self.event_delivery = delivery;
    }

    pub(super) fn stop_sending_change_events(&mut self, partition: PartitionId) {
        tracing::debug!(partition = partition.get(), "stop sending change events");
        self.event_delivery.deactivate(partition);
    }

    pub fn forget_change_events(&mut self, partition: PartitionId) {
        tracing::debug!(partition = partition.get(), "forget change events");
        self.event_delivery.forget(partition);
        if let Some(outbox) = self.stores.cluster_outbox()
            && let Err(e) = outbox.remove_change_events(partition)
        {
            tracing::warn!(partition = partition.get(), error = %e, "failed to remove change events");
        }
    }

    pub(super) fn set_aside_change_events(
        &mut self,
        partition: PartitionId,
    ) -> Vec<StoredChangeEvent> {
        let saved = match self.stores.cluster_outbox() {
            Some(outbox) => match outbox.stored_change_events_for(partition) {
                Ok(events) => events,
                Err(e) => {
                    tracing::warn!(partition = partition.get(), error = %e, "failed to read change events before snapshot import");
                    return Vec::new();
                }
            },
            None => Vec::new(),
        };
        self.forget_change_events(partition);
        saved
    }

    pub(super) fn restore_change_events(
        &mut self,
        partition: PartitionId,
        saved: &[StoredChangeEvent],
    ) {
        if let Some(outbox) = self.stores.cluster_outbox() {
            if let Err(e) = outbox.remove_change_events(partition) {
                tracing::warn!(partition = partition.get(), error = %e, "failed to clear change events after a failed snapshot import");
            }
            if let Err(e) = outbox.store_change_events(saved) {
                tracing::error!(partition = partition.get(), error = %e, "failed to restore change events after a failed snapshot import");
            }
        }
        self.reload_change_events(partition);
    }

    pub(super) fn change_events_node_gone(&mut self, node: NodeId) {
        self.event_delivery.node_gone(node);
    }

    pub(super) fn reload_change_events(&mut self, partition: PartitionId) {
        self.event_delivery.forget(partition);
        let Some(outbox) = self.stores.cluster_outbox() else {
            return;
        };
        match outbox.stored_change_events_for(partition) {
            Ok(events) => {
                for StoredChangeEvent {
                    partition,
                    id,
                    record,
                } in events
                {
                    self.event_delivery.store(partition, id, record);
                }
            }
            Err(e) => {
                tracing::warn!(partition = partition.get(), error = %e, "failed to reload change events")
            }
        }
    }

    pub(super) async fn publish_unowned_change_event(
        &self,
        partition: PartitionId,
        record: ChangeEventRecord,
    ) {
        tracing::warn!(
            partition = partition.get(),
            topic = %record.topic,
            "no primary role holds this write, publishing its change event untracked"
        );
        self.deliver_change_event(routed(record)).await;
    }

    pub(super) async fn forward_write_with_change_event(
        &mut self,
        primary: NodeId,
        write: ReplicationWrite,
        event: Option<ChangeEventRecord>,
    ) {
        let write_partition = write.partition;
        let hand_off = self.heartbeat.delivers_acked_events(primary)
            && self.heartbeat.node_status(primary) == NodeStatus::Alive;
        match event {
            Some(record) if hand_off => {
                let attached = write.with_event(Some(record.clone()));
                if self
                    .transport
                    .send(primary, ClusterMessage::WriteRequest(attached))
                    .await
                    .is_err()
                {
                    self.publish_unowned_change_event(write_partition, record)
                        .await;
                }
            }
            other => {
                let _ = self
                    .transport
                    .send(primary, ClusterMessage::WriteRequest(write))
                    .await;
                if let Some(record) = other {
                    self.deliver_change_event(routed(record)).await;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_change_events(&self, partition: PartitionId) -> usize {
        self.event_delivery.pending_count(partition)
    }
}
