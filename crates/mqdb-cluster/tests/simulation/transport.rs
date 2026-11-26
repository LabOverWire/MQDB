// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb_cluster::cluster::{
    BatchReadRequest, BatchReadResponse, ClusterMessage, ClusterTransport, Epoch, Heartbeat,
    InboundMessage, NodeId, PartitionId, QueryRequest, QueryResponse, ReplicationWrite,
    TransportError,
};

use super::framework::{VirtualClock, VirtualNetwork};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct TransportState {
    registered_nodes: Vec<u16>,
}

#[derive(Debug, Clone)]
pub struct SimulatedTransport {
    node_id: NodeId,
    network: VirtualNetwork,
    clock: VirtualClock,
    state: Arc<Mutex<TransportState>>,
}

impl SimulatedTransport {
    pub fn new(node_id: NodeId, network: VirtualNetwork, clock: VirtualClock) -> Self {
        network.register_node(node_id.get());
        Self {
            node_id,
            network,
            clock,
            state: Arc::new(Mutex::new(TransportState {
                registered_nodes: vec![node_id.get()],
            })),
        }
    }

    pub fn register_peer(&self, peer: NodeId) {
        self.network.register_node(peer.get());
        let mut state = self.state.lock().unwrap();
        if !state.registered_nodes.contains(&peer.get()) {
            state.registered_nodes.push(peer.get());
        }
    }

    fn serialize_message(msg: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(msg.message_type());
        msg.encode_payload(&mut buf);
        buf
    }

    fn deserialize_message(data: &[u8]) -> Option<ClusterMessage> {
        let (msg_type, payload) = data.split_first()?;
        ClusterMessage::decode_from_wire(*msg_type, payload)
    }
}

impl ClusterTransport for SimulatedTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    async fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        let data = Self::serialize_message(&message);
        if self.network.send(self.node_id.get(), to.get(), data) {
            Ok(())
        } else {
            Err(TransportError::NetworkPartitioned)
        }
    }

    async fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        let data = Self::serialize_message(&message);
        let nodes = self.state.lock().unwrap().registered_nodes.clone();

        for &peer in &nodes {
            if peer != self.node_id.get() {
                self.network.send(self.node_id.get(), peer, data.clone());
            }
        }

        Ok(())
    }

    async fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        _message: ClusterMessage,
    ) -> Result<(), TransportError> {
        Err(TransportError::PartitionNotFound(partition))
    }

    async fn direct_peers(&self) -> Option<Vec<NodeId>> {
        None
    }

    fn recv(&self) -> Option<InboundMessage> {
        let msg = self.network.receive(self.node_id.get())?;
        let message = Self::deserialize_message(&msg.payload)?;
        let from = NodeId::validated(msg.from)?;

        Some(InboundMessage {
            from,
            message,
            received_at: self.clock.now() / 1_000_000,
        })
    }

    fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
        self.recv()
    }

    fn pending_count(&self) -> usize {
        self.network.pending_count(self.node_id.get())
    }

    fn requeue(&self, msg: InboundMessage) {
        let data = Self::serialize_message(&msg.message);
        self.network
            .requeue(self.node_id.get(), msg.from.get(), data);
    }

    async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

    async fn queue_local_response(&self, _: String, _: Vec<u8>, _: u8, _: Option<Vec<u8>>) {}

    async fn queue_local_publish_with_properties(
        &self,
        _: String,
        _: Vec<u8>,
        _: u8,
        _: Vec<(String, String)>,
    ) {
    }

    async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
    async fn queue_retained_refresh(&self, _topic: String) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::framework::VirtualClock;
    use mqdb_cluster::cluster::Operation;
    use std::time::Duration;

    #[tokio::test]
    async fn send_and_receive_heartbeat() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let hb = Heartbeat::create(node1, 1000);
        t1.send(node2, ClusterMessage::Heartbeat(hb)).await.unwrap();

        assert!(t2.recv().is_none());

        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        assert_eq!(msg.from, node1);
        match msg.message {
            ClusterMessage::Heartbeat(h) => assert_eq!(h.node_id(), 1),
            _ => panic!("expected heartbeat"),
        }
    }

    #[tokio::test]
    async fn send_and_receive_write() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let write = ReplicationWrite::new(
            PartitionId::new(5).unwrap(),
            Operation::Insert,
            Epoch::new(1),
            42,
            "users".to_string(),
            "123".to_string(),
            b"test data".to_vec(),
        );

        t1.send(node2, ClusterMessage::Write(write)).await.unwrap();
        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        match msg.message {
            ClusterMessage::Write(w) => {
                assert_eq!(w.partition, PartitionId::new(5).unwrap());
                assert_eq!(w.sequence, 42);
                assert_eq!(w.entity, "users");
                assert_eq!(w.id, "123");
            }
            _ => panic!("expected write"),
        }
    }

    #[tokio::test]
    async fn broadcast_sends_to_all_peers() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());
        let t3 = SimulatedTransport::new(node3, network.clone(), clock.clone());

        t1.register_peer(node2);
        t1.register_peer(node3);

        let hb = Heartbeat::create(node1, 1000);
        t1.broadcast(ClusterMessage::Heartbeat(hb)).await.unwrap();

        clock.advance(Duration::from_millis(1));

        assert!(t2.recv().is_some());
        assert!(t3.recv().is_some());
        assert!(t1.recv().is_none());
    }

    #[tokio::test]
    async fn send_and_receive_query_request() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let partition = PartitionId::new(5).unwrap();
        let request = QueryRequest::new(
            12345,
            5000,
            "users".to_string(),
            Some("age > 30".to_string()),
            100,
            Some(b"cursor".to_vec()),
        );

        t1.send(node2, ClusterMessage::QueryRequest { partition, request })
            .await
            .unwrap();
        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        assert_eq!(msg.from, node1);
        match msg.message {
            ClusterMessage::QueryRequest {
                partition: p,
                request: r,
            } => {
                assert_eq!(p, PartitionId::new(5).unwrap());
                assert_eq!(r.query_id, 12345);
                assert_eq!(r.timeout_ms, 5000);
                assert_eq!(r.entity, "users");
                assert_eq!(r.filter, Some("age > 30".to_string()));
                assert_eq!(r.limit, 100);
                assert_eq!(r.cursor, Some(b"cursor".to_vec()));
            }
            _ => panic!("expected query request"),
        }
    }

    #[tokio::test]
    async fn send_and_receive_query_response() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let partition = PartitionId::new(10).unwrap();
        let response = QueryResponse::ok(
            999,
            partition,
            b"result-data".to_vec(),
            true,
            Some(b"next-cursor".to_vec()),
        );

        t1.send(node2, ClusterMessage::QueryResponse(response))
            .await
            .unwrap();
        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        match msg.message {
            ClusterMessage::QueryResponse(r) => {
                assert_eq!(r.query_id, 999);
                assert_eq!(r.partition, partition);
                assert!(r.status.is_ok());
                assert_eq!(r.results, b"result-data");
                assert!(r.has_more);
                assert_eq!(r.cursor, Some(b"next-cursor".to_vec()));
            }
            _ => panic!("expected query response"),
        }
    }

    #[tokio::test]
    async fn send_and_receive_batch_read_request() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let partition = PartitionId::new(7).unwrap();
        let request = BatchReadRequest::new(
            555,
            partition,
            "sessions".to_string(),
            vec!["id1".to_string(), "id2".to_string()],
        );

        t1.send(node2, ClusterMessage::BatchReadRequest(request))
            .await
            .unwrap();
        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        match msg.message {
            ClusterMessage::BatchReadRequest(r) => {
                assert_eq!(r.request_id, 555);
                assert_eq!(r.partition, partition);
                assert_eq!(r.entity, "sessions");
                assert_eq!(r.ids, vec!["id1", "id2"]);
            }
            _ => panic!("expected batch read request"),
        }
    }

    #[tokio::test]
    async fn send_and_receive_batch_read_response() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let partition = PartitionId::new(15).unwrap();
        let results = vec![
            ("id1".to_string(), Some(b"data1".to_vec())),
            ("id2".to_string(), None),
        ];
        let response = BatchReadResponse::new(777, partition, results);

        t1.send(node2, ClusterMessage::BatchReadResponse(response))
            .await
            .unwrap();
        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        match msg.message {
            ClusterMessage::BatchReadResponse(r) => {
                assert_eq!(r.request_id, 777);
                assert_eq!(r.partition, partition);
                assert_eq!(r.results.len(), 2);
                assert_eq!(r.results[0], ("id1".to_string(), Some(b"data1".to_vec())));
                assert_eq!(r.results[1], ("id2".to_string(), None));
            }
            _ => panic!("expected batch read response"),
        }
    }
}
