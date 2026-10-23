// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::{ChangeEventRecord, EventId};
use super::{NUM_PARTITIONS, NodeId, PartitionId};
use std::collections::{BTreeMap, HashMap};

pub(crate) const MAX_PENDING_EVENTS: usize = 4096;
pub(crate) const RESEND_AFTER_MS: u64 = 500;
pub(crate) const PRUNE_EVERY_MS: u64 = 250;
pub(crate) const PRUNE_REPEAT_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetKind {
    Local,
    Legacy,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Shipment {
    pub(crate) target: NodeId,
    pub(crate) kind: TargetKind,
    pub(crate) id: EventId,
    pub(crate) record: ChangeEventRecord,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DroppedEvent {
    pub(crate) id: EventId,
    pub(crate) missed_by: Vec<NodeId>,
}

#[derive(Debug)]
struct PendingEvent {
    record: ChangeEventRecord,
    targets: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy)]
struct InFlight {
    id: EventId,
    sent_at_ms: u64,
}

#[derive(Debug, Default)]
struct PartitionEvents {
    events: BTreeMap<EventId, PendingEvent>,
    sending: bool,
    in_flight: HashMap<NodeId, InFlight>,
    removed_through: Option<EventId>,
    prune_announced: Option<EventId>,
    prune_announced_at_ms: u64,
}

impl PartitionEvents {
    fn insert(&mut self, id: EventId, event: PendingEvent) -> Option<DroppedEvent> {
        self.events.insert(id, event);
        if self.events.len() <= MAX_PENDING_EVENTS {
            return None;
        }
        let (dropped_id, dropped) = self.events.pop_first()?;
        self.in_flight.retain(|_, flight| flight.id != dropped_id);
        self.mark_removed(dropped_id);
        Some(DroppedEvent {
            id: dropped_id,
            missed_by: dropped.targets,
        })
    }

    fn mark_removed(&mut self, id: EventId) {
        if self.removed_through.is_none_or(|through| through < id) {
            self.removed_through = Some(id);
        }
    }

    fn remove_completed_front(&mut self) {
        if !self.sending {
            return;
        }
        while let Some(entry) = self.events.first_entry() {
            if !entry.get().targets.is_empty() {
                break;
            }
            let id = *entry.key();
            entry.remove();
            self.mark_removed(id);
        }
    }

    fn complete(&mut self, target: NodeId, id: EventId) {
        if let Some(event) = self.events.get_mut(&id) {
            event.targets.retain(|&t| t != target);
        }
    }

    fn shipments(&mut self, now_ms: u64, kind_of: &impl Fn(NodeId) -> TargetKind) -> Vec<Shipment> {
        let mut shipments = Vec::new();
        let mut waiting: Vec<NodeId> = Vec::new();
        let in_flight = &mut self.in_flight;
        for (&id, event) in &mut self.events {
            let record = &event.record;
            event.targets.retain(|&target| {
                if waiting.contains(&target) {
                    return true;
                }
                let kind = kind_of(target);
                if kind == TargetKind::Legacy {
                    shipments.push(Shipment {
                        target,
                        kind,
                        id,
                        record: record.clone(),
                    });
                    return false;
                }
                waiting.push(target);
                let due = in_flight.get(&target).is_none_or(|flight| {
                    now_ms.saturating_sub(flight.sent_at_ms) >= RESEND_AFTER_MS
                });
                if due {
                    shipments.push(Shipment {
                        target,
                        kind,
                        id,
                        record: record.clone(),
                    });
                    in_flight.insert(
                        target,
                        InFlight {
                            id,
                            sent_at_ms: now_ms,
                        },
                    );
                }
                true
            });
        }
        self.remove_completed_front();
        shipments
    }
}

#[derive(Debug)]
pub(crate) struct EventDelivery {
    partitions: HashMap<u16, PartitionEvents>,
    delivered: [Option<EventId>; NUM_PARTITIONS as usize],
    last_prune_ms: u64,
}

impl Default for EventDelivery {
    fn default() -> Self {
        Self {
            partitions: HashMap::new(),
            delivered: [None; NUM_PARTITIONS as usize],
            last_prune_ms: 0,
        }
    }
}

impl EventDelivery {
    pub(crate) fn store(
        &mut self,
        partition: PartitionId,
        id: EventId,
        record: ChangeEventRecord,
    ) -> Option<DroppedEvent> {
        let events = self.partitions.entry(partition.get()).or_default();
        if events.removed_through.is_some_and(|through| id <= through) {
            return None;
        }
        events.insert(
            id,
            PendingEvent {
                record,
                targets: Vec::new(),
            },
        )
    }

    pub(crate) fn take_on(
        &mut self,
        partition: PartitionId,
        id: EventId,
        record: ChangeEventRecord,
        targets: Vec<NodeId>,
    ) -> Option<DroppedEvent> {
        self.partitions
            .entry(partition.get())
            .or_default()
            .insert(id, PendingEvent { record, targets })
    }

    pub(crate) fn activate(
        &mut self,
        partition: PartitionId,
        targets_for: impl Fn(&ChangeEventRecord) -> Vec<NodeId>,
    ) {
        let events = self.partitions.entry(partition.get()).or_default();
        if events.sending {
            return;
        }
        events.sending = true;
        events.in_flight.clear();
        for event in events.events.values_mut() {
            event.targets = targets_for(&event.record);
        }
    }

    pub(crate) fn deactivate(&mut self, partition: PartitionId) {
        if let Some(events) = self.partitions.get_mut(&partition.get()) {
            events.sending = false;
            events.in_flight.clear();
            for event in events.events.values_mut() {
                event.targets.clear();
            }
        }
    }

    pub(crate) fn forget(&mut self, partition: PartitionId) {
        self.partitions.remove(&partition.get());
    }

    pub(crate) fn prune_through(&mut self, partition: PartitionId, through: EventId) {
        if let Some(events) = self.partitions.get_mut(&partition.get()) {
            events.events.retain(|&id, _| id > through);
            events.in_flight.retain(|_, flight| flight.id > through);
            events.mark_removed(through);
        }
    }

    pub(crate) fn is_sending(&self, partition: PartitionId) -> bool {
        self.partitions
            .get(&partition.get())
            .is_some_and(|events| events.sending)
    }

    pub(crate) fn sending_partitions(&self) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .filter(|(_, events)| events.sending)
            .filter_map(|(&p, _)| PartitionId::new(p))
            .collect()
    }

    pub(crate) fn shipments(
        &mut self,
        partition: PartitionId,
        now_ms: u64,
        kind_of: impl Fn(NodeId) -> TargetKind,
    ) -> Vec<Shipment> {
        match self.partitions.get_mut(&partition.get()) {
            Some(events) if events.sending => events.shipments(now_ms, &kind_of),
            _ => Vec::new(),
        }
    }

    pub(crate) fn acked(&mut self, partition: PartitionId, from: NodeId, id: EventId) -> bool {
        let Some(events) = self.partitions.get_mut(&partition.get()) else {
            return false;
        };
        if events
            .in_flight
            .get(&from)
            .is_none_or(|flight| flight.id != id)
        {
            return false;
        }
        events.in_flight.remove(&from);
        events.complete(from, id);
        events.remove_completed_front();
        true
    }

    pub(crate) fn node_gone(&mut self, node: NodeId) {
        for events in self.partitions.values_mut() {
            events.in_flight.remove(&node);
            for event in events.events.values_mut() {
                event.targets.retain(|&t| t != node);
            }
            events.remove_completed_front();
        }
    }

    pub(crate) fn prunes_due(&mut self, now_ms: u64) -> Vec<(PartitionId, EventId)> {
        if now_ms.saturating_sub(self.last_prune_ms) < PRUNE_EVERY_MS {
            return Vec::new();
        }
        self.last_prune_ms = now_ms;
        self.partitions
            .iter_mut()
            .filter(|(_, events)| events.sending)
            .filter_map(|(&p, events)| {
                let through = events.removed_through?;
                let repeat_due =
                    now_ms.saturating_sub(events.prune_announced_at_ms) >= PRUNE_REPEAT_MS;
                if events.prune_announced.is_some_and(|sent| sent >= through) && !repeat_due {
                    return None;
                }
                events.prune_announced = Some(through);
                events.prune_announced_at_ms = now_ms;
                Some((PartitionId::new(p)?, through))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_count(&self, partition: PartitionId) -> usize {
        self.partitions
            .get(&partition.get())
            .map_or(0, |events| events.events.len())
    }

    pub(crate) fn is_new(&self, partition: PartitionId, id: EventId) -> bool {
        self.delivered
            .get(usize::from(partition.get()))
            .is_some_and(|mark| mark.is_none_or(|last| last < id))
    }

    pub(crate) fn mark_delivered(&mut self, partition: PartitionId, id: EventId) {
        if let Some(mark) = self.delivered.get_mut(usize::from(partition.get()))
            && mark.is_none_or(|last| last < id)
        {
            *mark = Some(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    fn record(topic: &str) -> ChangeEventRecord {
        ChangeEventRecord {
            topic: topic.to_string(),
            payload: topic.as_bytes().to_vec(),
            user_properties: Vec::new(),
            order_key: "e/1".to_string(),
        }
    }

    fn acked_everywhere(_: NodeId) -> TargetKind {
        TargetKind::Acked
    }

    fn shipped(shipments: &[Shipment]) -> Vec<(u16, EventId)> {
        shipments.iter().map(|s| (s.target.get(), s.id)).collect()
    }

    #[test]
    fn a_target_gets_the_next_event_only_after_acking_the_previous_one() {
        let mut delivery = EventDelivery::default();
        let p = partition(7);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(2)]);

        let first = delivery.shipments(p, 0, acked_everywhere);
        assert_eq!(shipped(&first), vec![(2, EventId::new(1, 1))]);
        assert!(delivery.shipments(p, 10, acked_everywhere).is_empty());

        assert!(delivery.acked(p, node(2), EventId::new(1, 1)));
        let second = delivery.shipments(p, 20, acked_everywhere);
        assert_eq!(shipped(&second), vec![(2, EventId::new(1, 2))]);
    }

    #[test]
    fn an_unacked_event_is_resent_after_the_timeout() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(3)]);

        assert_eq!(delivery.shipments(p, 0, acked_everywhere).len(), 1);
        assert!(
            delivery
                .shipments(p, RESEND_AFTER_MS - 1, acked_everywhere)
                .is_empty()
        );
        let resent = delivery.shipments(p, RESEND_AFTER_MS, acked_everywhere);
        assert_eq!(shipped(&resent), vec![(3, EventId::new(1, 1))]);
    }

    #[test]
    fn an_ack_for_an_event_not_in_flight_changes_nothing() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(3)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(3)]);
        delivery.shipments(p, 0, acked_everywhere);

        assert!(!delivery.acked(p, node(3), EventId::new(1, 2)));
        assert_eq!(delivery.pending_count(p), 2);
    }

    #[test]
    fn legacy_targets_are_done_as_soon_as_they_are_shipped() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(4)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(4)]);

        let shipments = delivery.shipments(p, 0, |_| TargetKind::Legacy);
        assert_eq!(
            shipped(&shipments),
            vec![(4, EventId::new(1, 1)), (4, EventId::new(1, 2))]
        );
        assert_eq!(delivery.pending_count(p), 0);
        assert_eq!(
            delivery.prunes_due(PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 2))]
        );
    }

    #[test]
    fn the_local_target_waits_for_its_own_ack_like_a_remote_one() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(1)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(1)]);

        let first = delivery.shipments(p, 0, |_| TargetKind::Local);
        assert_eq!(shipped(&first), vec![(1, EventId::new(1, 1))]);
        assert!(delivery.shipments(p, 0, |_| TargetKind::Local).is_empty());
        assert!(delivery.acked(p, node(1), EventId::new(1, 1)));
        let second = delivery.shipments(p, 0, |_| TargetKind::Local);
        assert_eq!(shipped(&second), vec![(1, EventId::new(1, 2))]);
    }

    #[test]
    fn prunes_cover_only_the_completed_prefix() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(3)]);
        delivery.shipments(p, 0, acked_everywhere);

        assert!(delivery.acked(p, node(3), EventId::new(1, 2)));
        assert!(delivery.prunes_due(PRUNE_EVERY_MS).is_empty());

        assert!(delivery.acked(p, node(2), EventId::new(1, 1)));
        assert_eq!(
            delivery.prunes_due(2 * PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 2))]
        );
        assert!(delivery.prunes_due(3 * PRUNE_EVERY_MS).is_empty());
    }

    #[test]
    fn prunes_are_announced_at_most_every_interval() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(4)]);
        delivery.shipments(p, 0, |_| TargetKind::Legacy);
        assert_eq!(
            delivery.prunes_due(PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 1))]
        );

        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(4)]);
        delivery.shipments(p, 0, |_| TargetKind::Legacy);
        assert!(delivery.prunes_due(2 * PRUNE_EVERY_MS - 1).is_empty());
        assert_eq!(
            delivery.prunes_due(2 * PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 2))]
        );
    }

    #[test]
    fn an_unchanged_prune_is_announced_again_after_the_repeat_interval() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(4)]);
        delivery.shipments(p, 0, |_| TargetKind::Legacy);
        assert_eq!(
            delivery.prunes_due(PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 1))]
        );

        assert!(delivery.prunes_due(PRUNE_REPEAT_MS).is_empty());
        assert_eq!(
            delivery.prunes_due(PRUNE_EVERY_MS + PRUNE_REPEAT_MS),
            vec![(p, EventId::new(1, 1))]
        );
    }

    #[test]
    fn activating_a_partition_that_is_already_sending_keeps_acked_targets_done() {
        let mut delivery = EventDelivery::default();
        let p = partition(7);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2), node(3)]);
        delivery.shipments(p, 0, acked_everywhere);
        assert!(delivery.acked(p, node(2), EventId::new(1, 1)));

        delivery.activate(p, |_| vec![node(2), node(3)]);

        assert!(delivery.shipments(p, 10, acked_everywhere).is_empty());
    }

    #[test]
    fn a_legacy_target_gets_every_pending_event_in_one_pass_while_an_acked_one_waits() {
        let mut delivery = EventDelivery::default();
        let p = partition(7);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2), node(3)]);
        delivery.take_on(p, EventId::new(1, 2), record("b"), vec![node(2), node(3)]);

        let shipments = delivery.shipments(p, 0, |target| {
            if target == node(2) {
                TargetKind::Legacy
            } else {
                TargetKind::Acked
            }
        });

        assert_eq!(
            shipped(&shipments),
            vec![
                (2, EventId::new(1, 1)),
                (3, EventId::new(1, 1)),
                (2, EventId::new(1, 2))
            ]
        );
    }

    #[test]
    fn a_replica_copy_is_sent_only_after_activation() {
        let mut delivery = EventDelivery::default();
        let p = partition(9);
        delivery.store(p, EventId::new(1, 4), record("a"));
        assert!(delivery.shipments(p, 0, acked_everywhere).is_empty());
        assert!(delivery.prunes_due(PRUNE_EVERY_MS).is_empty());

        delivery.activate(p, |_| vec![node(2)]);
        let shipments = delivery.shipments(p, 0, acked_everywhere);
        assert_eq!(shipped(&shipments), vec![(2, EventId::new(1, 4))]);
    }

    #[test]
    fn deactivation_stops_sending_but_keeps_the_copy() {
        let mut delivery = EventDelivery::default();
        let p = partition(9);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2)]);
        delivery.deactivate(p);

        assert!(delivery.shipments(p, 0, acked_everywhere).is_empty());
        assert_eq!(delivery.pending_count(p), 1);
    }

    #[test]
    fn a_prune_removes_entries_up_to_and_including_its_id_across_epochs() {
        let mut delivery = EventDelivery::default();
        let p = partition(2);
        delivery.store(p, EventId::new(1, 5), record("a"));
        delivery.store(p, EventId::new(1, 9), record("b"));
        delivery.store(p, EventId::new(2, 1), record("c"));

        delivery.prune_through(p, EventId::new(2, 0));
        assert_eq!(delivery.pending_count(p), 1);

        delivery.store(p, EventId::new(1, 7), record("late"));
        assert_eq!(delivery.pending_count(p), 1);
    }

    #[test]
    fn a_dead_node_no_longer_holds_back_pruning() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        delivery.take_on(p, EventId::new(1, 1), record("a"), vec![node(2), node(3)]);
        delivery.shipments(p, 0, acked_everywhere);
        assert!(delivery.acked(p, node(2), EventId::new(1, 1)));

        delivery.node_gone(node(3));
        assert_eq!(delivery.pending_count(p), 0);
        assert_eq!(
            delivery.prunes_due(PRUNE_EVERY_MS),
            vec![(p, EventId::new(1, 1))]
        );
    }

    #[test]
    fn the_oldest_event_is_dropped_past_the_cap() {
        let mut delivery = EventDelivery::default();
        let p = partition(1);
        delivery.activate(p, |_| Vec::new());
        let max = u64::try_from(MAX_PENDING_EVENTS).unwrap();
        for seq in 1..=max {
            assert!(
                delivery
                    .take_on(p, EventId::new(1, seq), record("a"), vec![node(2)])
                    .is_none()
            );
        }
        let dropped = delivery.take_on(p, EventId::new(1, max + 1), record("a"), vec![node(2)]);
        assert_eq!(
            dropped,
            Some(DroppedEvent {
                id: EventId::new(1, 1),
                missed_by: vec![node(2)],
            })
        );
        assert_eq!(delivery.pending_count(p), MAX_PENDING_EVENTS);
    }

    #[test]
    fn the_receiver_delivers_only_ids_above_its_mark() {
        let mut delivery = EventDelivery::default();
        let p = partition(3);
        assert!(delivery.is_new(p, EventId::new(1, 2)));
        delivery.mark_delivered(p, EventId::new(1, 2));
        assert!(!delivery.is_new(p, EventId::new(1, 2)));
        assert!(!delivery.is_new(p, EventId::new(1, 1)));
        assert!(delivery.is_new(p, EventId::new(2, 1)));
        delivery.mark_delivered(p, EventId::new(2, 1));
        delivery.mark_delivered(p, EventId::new(1, 9));
        assert!(!delivery.is_new(p, EventId::new(1, 9)));
        assert!(delivery.is_new(partition(4), EventId::new(1, 1)));
    }
}
