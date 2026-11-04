// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::death_verdicts::DeathVerdicts;
use super::heartbeat::{HeartbeatManager, NodeStatus};
use super::protocol::{ForwardedPublish, Heartbeat};
use super::raft_task::RaftEvent;
use super::transport::{ClusterMessage, InboundMessage, TransportConfig};
use super::{NodeId, PartitionMap, RaftMessage};
use std::collections::{HashSet, VecDeque};

const FORWARD_DEDUP_CAPACITY: usize = 1000;

pub struct HeartbeatUpdate {
    pub from: NodeId,
    pub heartbeat: Heartbeat,
    pub received_at: u64,
}

#[derive(Default)]
pub struct ProcessingBatch {
    pub heartbeat_updates: Vec<HeartbeatUpdate>,
    pub dead_nodes: Vec<NodeId>,
    pub heartbeat_to_send: Option<Heartbeat>,
    pub forwarded_publishes: Vec<(String, Vec<u8>, u8)>,
}

pub struct MessageProcessor {
    node_id: NodeId,
    heartbeat_manager: HeartbeatManager,
    forward_dedup: HashSet<u64>,
    forward_dedup_order: VecDeque<u64>,
    pending_heartbeat_updates: Vec<HeartbeatUpdate>,
    tx_raft_messages: flume::Sender<RaftMessage>,
    tx_raft_events: flume::Sender<RaftEvent>,
    death_verdicts: DeathVerdicts,
    tx_main_queue: flume::Sender<InboundMessage>,
    rx_inbox: flume::Receiver<InboundMessage>,
    rx_tick: flume::Receiver<u64>,
    rx_expected_members: flume::Receiver<NodeId>,
    last_batch_time: u64,
}

impl MessageProcessor {
    #[must_use]
    pub fn new(
        node_id: NodeId,
        config: TransportConfig,
        tx_raft_messages: flume::Sender<RaftMessage>,
        tx_raft_events: flume::Sender<RaftEvent>,
        tx_main_queue: flume::Sender<InboundMessage>,
        rx_inbox: flume::Receiver<InboundMessage>,
        rx_tick: flume::Receiver<u64>,
    ) -> Self {
        Self {
            node_id,
            heartbeat_manager: HeartbeatManager::new(node_id, config),
            forward_dedup: HashSet::with_capacity(FORWARD_DEDUP_CAPACITY),
            forward_dedup_order: VecDeque::with_capacity(FORWARD_DEDUP_CAPACITY),
            pending_heartbeat_updates: Vec::new(),
            tx_raft_messages,
            tx_raft_events,
            death_verdicts: DeathVerdicts::default(),
            tx_main_queue,
            rx_inbox,
            rx_tick,
            rx_expected_members: flume::unbounded().1,
            last_batch_time: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn receive_heartbeat_for_test(&mut self, from: NodeId, at: u64) {
        self.heartbeat_manager
            .receive_heartbeat(from, &Heartbeat::create(from, at), at);
    }

    pub fn expect_members_from(&mut self, rx_expected_members: flume::Receiver<NodeId>) {
        self.rx_expected_members = rx_expected_members;
    }

    pub fn register_peer(&mut self, peer: NodeId) {
        self.heartbeat_manager.register_node(peer);
    }

    #[cfg(test)]
    pub(crate) fn check_timeouts(&mut self, now: u64) -> Vec<NodeId> {
        self.heartbeat_manager.check_timeouts(now)
    }

    pub fn expect_peer(&mut self, peer: NodeId) {
        self.heartbeat_manager.expect_node(peer);
    }

    pub fn update_partition_map(&mut self, map: PartitionMap) {
        self.heartbeat_manager.update_partition_map(map);
    }

    pub async fn run(mut self, tx_batch: flume::Sender<ProcessingBatch>) {
        tracing::info!(node = self.node_id.get(), "message processor started");

        loop {
            tokio::select! {
                biased;

                Ok(now) = self.rx_tick.recv_async() => {
                    let batch = self.process_tick(now);
                    if tx_batch.send_async(batch).await.is_err() {
                        tracing::warn!("batch channel closed, processor exiting");
                        break;
                    }
                }

                Ok(msg) = self.rx_inbox.recv_async() => {
                    const BATCH_SIZE: u32 = 64;
                    self.process_message(msg);
                    let mut count = 1u32;
                    while let Ok(msg) = self.rx_inbox.try_recv() {
                        self.process_message(msg);
                        count += 1;
                        if count.is_multiple_of(BATCH_SIZE) {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }

        tracing::info!(node = self.node_id.get(), "message processor stopped");
    }

    pub(crate) fn process_tick(&mut self, now: u64) -> ProcessingBatch {
        let mut batch = ProcessingBatch::default();
        self.last_batch_time = now;

        if self.heartbeat_manager.should_send(now) {
            batch.heartbeat_to_send = Some(self.heartbeat_manager.build_heartbeat(now));
        }

        for member in self.rx_expected_members.try_iter() {
            self.heartbeat_manager.expect_again(member);
        }

        let heartbeat_manager = &self.heartbeat_manager;
        self.death_verdicts.retry(&self.tx_raft_events, |node| {
            heartbeat_manager.node_status(node) != NodeStatus::Dead
        });

        batch.dead_nodes = self.heartbeat_manager.check_timeouts(now);
        batch.heartbeat_updates = std::mem::take(&mut self.pending_heartbeat_updates);

        for dead in &batch.dead_nodes {
            self.death_verdicts.report(&self.tx_raft_events, *dead);
        }

        batch
    }

    fn process_message(&mut self, msg: InboundMessage) {
        match &msg.message {
            ClusterMessage::Heartbeat(hb) => {
                self.heartbeat_manager
                    .receive_heartbeat(msg.from, hb, msg.received_at);
                if self.heartbeat_manager.node_status(msg.from) == NodeStatus::Alive {
                    let _ = self.tx_raft_events.try_send(RaftEvent::NodeAlive(msg.from));
                }
                self.pending_heartbeat_updates.push(HeartbeatUpdate {
                    from: msg.from,
                    heartbeat: hb.clone(),
                    received_at: msg.received_at,
                });
            }

            ClusterMessage::RequestVote(req) => {
                let _ = self.tx_raft_messages.try_send(RaftMessage::RequestVote {
                    from: msg.from,
                    request: *req,
                });
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                let _ = self
                    .tx_raft_messages
                    .try_send(RaftMessage::RequestVoteResponse {
                        from: msg.from,
                        response: *resp,
                    });
            }
            ClusterMessage::AppendEntries(req) => {
                let _ = self.tx_raft_messages.try_send(RaftMessage::AppendEntries {
                    from: msg.from,
                    request: req.clone(),
                });
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                let _ = self
                    .tx_raft_messages
                    .try_send(RaftMessage::AppendEntriesResponse {
                        from: msg.from,
                        response: *resp,
                    });
            }
            ClusterMessage::InstallSnapshot(request) => {
                let _ = self
                    .tx_raft_messages
                    .try_send(RaftMessage::InstallSnapshot {
                        from: msg.from,
                        request: request.clone(),
                    });
            }

            ClusterMessage::ForwardedPublish(fwd) => {
                if self.check_forward_dedup(fwd) {
                    let _ = self.tx_main_queue.send(msg);
                }
            }

            _ => {
                let _ = self.tx_main_queue.send(msg);
            }
        }
    }

    fn check_forward_dedup(&mut self, fwd: &ForwardedPublish) -> bool {
        let fingerprint = Self::forward_fingerprint(fwd);

        if self.forward_dedup.contains(&fingerprint) {
            tracing::trace!(
                origin = fwd.origin_node.get(),
                topic = %fwd.topic,
                "deduplicated forwarded publish in processor"
            );
            return false;
        }

        if self.forward_dedup.len() >= FORWARD_DEDUP_CAPACITY
            && let Some(old) = self.forward_dedup_order.pop_front()
        {
            self.forward_dedup.remove(&old);
        }
        self.forward_dedup.insert(fingerprint);
        self.forward_dedup_order.push_back(fingerprint);
        true
    }

    fn forward_fingerprint(fwd: &ForwardedPublish) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        fwd.origin_node.hash(&mut hasher);
        fwd.timestamp_us.hash(&mut hasher);
        fwd.topic.hash(&mut hasher);
        fwd.payload.hash(&mut hasher);
        hasher.finish()
    }

    #[must_use]
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.heartbeat_manager.alive_nodes()
    }

    #[must_use]
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        self.heartbeat_manager.dead_nodes()
    }

    #[must_use]
    pub fn partition_map(&self) -> &PartitionMap {
        self.heartbeat_manager.partition_map()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TransportConfig {
        TransportConfig {
            heartbeat_interval_ms: 100,
            heartbeat_timeout_ms: 500,
            ack_timeout_ms: 50,
        }
    }

    fn processor_with_a_full_event_queue(
        peer: NodeId,
    ) -> (MessageProcessor, flume::Receiver<RaftEvent>) {
        let (tx_raft, _rx_raft) = flume::unbounded();
        let (tx_events, rx_events) = flume::bounded(1);
        let (tx_main, _rx_main) = flume::unbounded();
        let (_tx_inbox, rx_inbox) = flume::unbounded();
        let (_tx_tick, rx_tick) = flume::unbounded();
        let mut processor = MessageProcessor::new(
            NodeId::validated(1).unwrap(),
            test_config(),
            tx_raft,
            tx_events.clone(),
            tx_main,
            rx_inbox,
            rx_tick,
        );
        processor.register_peer(peer);
        processor.heartbeat_manager.receive_heartbeat(
            peer,
            &crate::cluster::protocol::Heartbeat::create(peer, 0),
            1_000,
        );
        tx_events.try_send(RaftEvent::NodeAlive(peer)).unwrap();
        (processor, rx_events)
    }

    fn verdict_arrives(rx_events: &flume::Receiver<RaftEvent>, peer: NodeId) -> bool {
        rx_events
            .try_iter()
            .any(|event| matches!(event, RaftEvent::NodeDead(node) if node == peer))
    }

    #[test]
    fn a_death_verdict_refused_by_a_full_queue_is_sent_on_a_later_tick() {
        let peer = NodeId::validated(2).unwrap();
        let (mut processor, rx_events) = processor_with_a_full_event_queue(peer);

        assert_eq!(processor.process_tick(2_000).dead_nodes, [peer]);
        assert!(!verdict_arrives(&rx_events, peer));

        assert!(processor.process_tick(2_100).dead_nodes.is_empty());
        assert!(verdict_arrives(&rx_events, peer));
    }

    #[test]
    fn a_held_death_verdict_is_dropped_when_the_node_is_back() {
        let peer = NodeId::validated(2).unwrap();
        let (mut processor, rx_events) = processor_with_a_full_event_queue(peer);

        processor.process_tick(2_000);
        assert!(!verdict_arrives(&rx_events, peer));
        processor.heartbeat_manager.receive_heartbeat(
            peer,
            &crate::cluster::protocol::Heartbeat::create(peer, 0),
            2_050,
        );

        processor.process_tick(2_100);
        assert!(!verdict_arrives(&rx_events, peer));
    }

    #[test]
    fn a_held_verdict_gives_way_to_a_new_expectation_of_that_node() {
        let peer = NodeId::validated(2).unwrap();
        let (mut processor, rx_events) = processor_with_a_full_event_queue(peer);
        let (tx_expected, rx_expected) = flume::unbounded();
        processor.expect_members_from(rx_expected);

        assert_eq!(processor.process_tick(2_000).dead_nodes, [peer]);
        assert!(!verdict_arrives(&rx_events, peer));
        tx_expected.send(peer).unwrap();

        assert!(processor.process_tick(2_100).dead_nodes.is_empty());
        assert!(!verdict_arrives(&rx_events, peer));
    }

    #[test]
    fn processor_forward_dedup() {
        let node_id = NodeId::validated(1).unwrap();
        let (tx_raft, _rx_raft) = flume::unbounded();
        let (tx_events, _rx_events) = flume::unbounded();
        let (tx_main, _rx_main) = flume::unbounded();
        let (_tx_inbox, rx_inbox) = flume::unbounded();
        let (_tx_tick, rx_tick) = flume::unbounded();

        let mut processor = MessageProcessor::new(
            node_id,
            test_config(),
            tx_raft,
            tx_events,
            tx_main,
            rx_inbox,
            rx_tick,
        );

        let fwd = ForwardedPublish::new(
            NodeId::validated(2).unwrap(),
            "test/topic".to_string(),
            1,
            false,
            vec![1, 2, 3],
            vec![],
        );

        assert!(processor.check_forward_dedup(&fwd));
        assert!(!processor.check_forward_dedup(&fwd));

        let fwd2 = ForwardedPublish::new(
            NodeId::validated(2).unwrap(),
            "test/topic2".to_string(),
            1,
            false,
            vec![1, 2, 3],
            vec![],
        );
        assert!(processor.check_forward_dedup(&fwd2));
    }

    #[test]
    fn processor_dedup_capacity() {
        let node_id = NodeId::validated(1).unwrap();
        let (tx_raft, _rx_raft) = flume::unbounded();
        let (tx_events, _rx_events) = flume::unbounded();
        let (tx_main, _rx_main) = flume::unbounded();
        let (_tx_inbox, rx_inbox) = flume::unbounded();
        let (_tx_tick, rx_tick) = flume::unbounded();

        let mut processor = MessageProcessor::new(
            node_id,
            test_config(),
            tx_raft,
            tx_events,
            tx_main,
            rx_inbox,
            rx_tick,
        );

        for i in 0..FORWARD_DEDUP_CAPACITY {
            let fwd = ForwardedPublish::new(
                NodeId::validated(2).unwrap(),
                format!("test/topic/{i}"),
                1,
                false,
                vec![1, 2, 3],
                vec![],
            );
            assert!(processor.check_forward_dedup(&fwd));
        }

        assert_eq!(processor.forward_dedup.len(), FORWARD_DEDUP_CAPACITY);

        let fwd_new = ForwardedPublish::new(
            NodeId::validated(2).unwrap(),
            "test/topic/new".to_string(),
            1,
            false,
            vec![1, 2, 3],
            vec![],
        );
        assert!(processor.check_forward_dedup(&fwd_new));

        assert_eq!(processor.forward_dedup.len(), FORWARD_DEDUP_CAPACITY);
    }
}
