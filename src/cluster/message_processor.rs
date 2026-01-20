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
    pub heartbeat_to_send: Option<ClusterMessage>,
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
    tx_main_queue: flume::Sender<InboundMessage>,
    rx_inbox: flume::Receiver<InboundMessage>,
    rx_tick: flume::Receiver<u64>,
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
            tx_main_queue,
            rx_inbox,
            rx_tick,
            last_batch_time: 0,
        }
    }

    pub fn register_peer(&mut self, peer: NodeId) {
        self.heartbeat_manager.register_node(peer);
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
                    self.process_message(msg);
                }
            }
        }

        tracing::info!(node = self.node_id.get(), "message processor stopped");
    }

    fn process_tick(&mut self, now: u64) -> ProcessingBatch {
        let mut batch = ProcessingBatch::default();
        self.last_batch_time = now;

        if self.heartbeat_manager.should_send(now) {
            batch.heartbeat_to_send = Some(self.heartbeat_manager.create_heartbeat(now));
        }

        batch.dead_nodes = self.heartbeat_manager.check_timeouts(now);
        batch.heartbeat_updates = std::mem::take(&mut self.pending_heartbeat_updates);

        for dead in &batch.dead_nodes {
            let _ = self.tx_raft_events.send(RaftEvent::NodeDead(*dead));
        }

        batch
    }

    fn process_message(&mut self, msg: InboundMessage) {
        match &msg.message {
            ClusterMessage::Heartbeat(hb) => {
                self.heartbeat_manager
                    .receive_heartbeat(msg.from, hb, msg.received_at);
                if self.heartbeat_manager.node_status(msg.from) == NodeStatus::Alive {
                    let _ = self.tx_raft_events.send(RaftEvent::NodeAlive(msg.from));
                }
                self.pending_heartbeat_updates.push(HeartbeatUpdate {
                    from: msg.from,
                    heartbeat: hb.clone(),
                    received_at: msg.received_at,
                });
            }

            ClusterMessage::RequestVote(req) => {
                let _ = self.tx_raft_messages.send(RaftMessage::RequestVote {
                    from: msg.from,
                    request: *req,
                });
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                let _ = self
                    .tx_raft_messages
                    .send(RaftMessage::RequestVoteResponse {
                        from: msg.from,
                        response: *resp,
                    });
            }
            ClusterMessage::AppendEntries(req) => {
                let _ = self.tx_raft_messages.send(RaftMessage::AppendEntries {
                    from: msg.from,
                    request: req.clone(),
                });
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                let _ = self
                    .tx_raft_messages
                    .send(RaftMessage::AppendEntriesResponse {
                        from: msg.from,
                        response: *resp,
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
        fwd.timestamp_ms.hash(&mut hasher);
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
