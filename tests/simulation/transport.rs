// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use mqdb::cluster::raft::{
    AppendEntriesRequest, AppendEntriesResponse, PartitionUpdate, RequestVoteRequest,
    RequestVoteResponse,
};
use mqdb::cluster::{
    BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ClusterMessage,
    ClusterTransport, Epoch, ForwardedPublish, Heartbeat, InboundMessage, JsonDbRequest,
    JsonDbResponse, NUM_PARTITIONS, NodeId, PartitionId, QueryRequest, QueryResponse,
    ReplicationAck, ReplicationWrite, SnapshotChunk, SnapshotComplete, SnapshotRequest,
    TopicSubscriptionBroadcast, TransportError, UniqueCommitRequest, UniqueCommitResponse,
    UniqueReleaseRequest, UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse,
    WildcardBroadcast,
};

use super::framework::{VirtualClock, VirtualNetwork};
use bebytes::BeBytes;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct TransportState {
    registered_nodes: Vec<u16>,
    partition_primaries: Vec<Option<u16>>,
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
                partition_primaries: vec![None; NUM_PARTITIONS as usize],
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

    #[allow(dead_code)]
    pub fn set_partition_primary(&self, partition: PartitionId, primary: Option<NodeId>) {
        let mut state = self.state.lock().unwrap();
        state.partition_primaries[partition.get() as usize] = primary.map(NodeId::get);
    }

    fn serialize_message(msg: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(msg.message_type());
        msg.encode_payload(&mut buf);
        buf
    }

    #[allow(clippy::too_many_lines)]
    fn deserialize_message(data: &[u8]) -> Option<ClusterMessage> {
        if data.is_empty() {
            return None;
        }

        let msg_type = data[0];
        let payload = &data[1..];

        match msg_type {
            0 => {
                let (hb, _) = Heartbeat::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::Heartbeat(hb))
            }
            10 => {
                let w = ReplicationWrite::from_bytes(payload)?;
                Some(ClusterMessage::Write(w))
            }
            11 => {
                let (ack, _) = ReplicationAck::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::Ack(ack))
            }
            15 => {
                let w = ReplicationWrite::from_bytes(payload)?;
                Some(ClusterMessage::WriteRequest(w))
            }
            2 => {
                if payload.len() < 2 {
                    return None;
                }
                let node_id = u16::from_be_bytes([payload[0], payload[1]]);
                let node = NodeId::validated(node_id)?;
                Some(ClusterMessage::DeathNotice { node_id: node })
            }
            3 => {
                if payload.len() < 2 {
                    return None;
                }
                let node_id = u16::from_be_bytes([payload[0], payload[1]]);
                let node = NodeId::validated(node_id)?;
                Some(ClusterMessage::DrainNotification { node_id: node })
            }
            20 => {
                let (req, _) = RequestVoteRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::RequestVote(req))
            }
            21 => {
                let (resp, _) = RequestVoteResponse::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::RequestVoteResponse(resp))
            }
            22 => {
                let req = AppendEntriesRequest::from_bytes(payload)?;
                Some(ClusterMessage::AppendEntries(req))
            }
            23 => {
                let (resp, _) = AppendEntriesResponse::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::AppendEntriesResponse(resp))
            }
            12 => {
                let (req, _) = CatchupRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::CatchupRequest(req))
            }
            13 => {
                let resp = CatchupResponse::from_bytes(payload)?;
                Some(ClusterMessage::CatchupResponse(resp))
            }
            30 => {
                let fwd = ForwardedPublish::from_bytes(payload)?;
                Some(ClusterMessage::ForwardedPublish(fwd))
            }
            40 => {
                let (req, _) = SnapshotRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::SnapshotRequest(req))
            }
            41 => {
                let chunk = SnapshotChunk::from_bytes(payload)?;
                Some(ClusterMessage::SnapshotChunk(chunk))
            }
            42 => {
                let (complete, _) = SnapshotComplete::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::SnapshotComplete(complete))
            }
            50 => {
                if payload.len() < 2 {
                    return None;
                }
                let partition_id = u16::from_be_bytes([payload[0], payload[1]]);
                let partition = PartitionId::new(partition_id)?;
                let request = QueryRequest::from_bytes(&payload[2..])?;
                Some(ClusterMessage::QueryRequest { partition, request })
            }
            51 => {
                let response = QueryResponse::from_bytes(payload)?;
                Some(ClusterMessage::QueryResponse(response))
            }
            52 => {
                let request = BatchReadRequest::from_bytes(payload)?;
                Some(ClusterMessage::BatchReadRequest(request))
            }
            53 => {
                let response = BatchReadResponse::from_bytes(payload)?;
                Some(ClusterMessage::BatchReadResponse(response))
            }
            60 => {
                let (broadcast, _) = WildcardBroadcast::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::WildcardBroadcast(broadcast))
            }
            61 => {
                let (broadcast, _) = TopicSubscriptionBroadcast::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::TopicSubscriptionBroadcast(broadcast))
            }
            70 => {
                let (update, _) = PartitionUpdate::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::PartitionUpdate(update))
            }
            54 => {
                if payload.len() < 2 {
                    return None;
                }
                let partition_id = u16::from_be_bytes([payload[0], payload[1]]);
                let partition = PartitionId::new(partition_id)?;
                let request = JsonDbRequest::from_bytes(&payload[2..])?;
                Some(ClusterMessage::JsonDbRequest { partition, request })
            }
            55 => {
                let response = JsonDbResponse::from_bytes(payload)?;
                Some(ClusterMessage::JsonDbResponse(response))
            }
            80 => {
                let (req, _) = UniqueReserveRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueReserveRequest(req))
            }
            81 => {
                let (resp, _) = UniqueReserveResponse::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueReserveResponse(resp))
            }
            82 => {
                let (req, _) = UniqueCommitRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueCommitRequest(req))
            }
            83 => {
                let (resp, _) = UniqueCommitResponse::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueCommitResponse(resp))
            }
            84 => {
                let (req, _) = UniqueReleaseRequest::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueReleaseRequest(req))
            }
            85 => {
                let (resp, _) = UniqueReleaseResponse::try_from_be_bytes(payload).ok()?;
                Some(ClusterMessage::UniqueReleaseResponse(resp))
            }
            _ => None,
        }
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
        message: ClusterMessage,
    ) -> Result<(), TransportError> {
        let primary = {
            let state = self.state.lock().unwrap();
            state.partition_primaries[partition.get() as usize]
        };

        match primary {
            Some(id) => {
                if let Some(node) = NodeId::validated(id) {
                    let data = Self::serialize_message(&message);
                    if self.network.send(self.node_id.get(), node.get(), data) {
                        Ok(())
                    } else {
                        Err(TransportError::NetworkPartitioned)
                    }
                } else {
                    Err(TransportError::PartitionNotFound(partition))
                }
            }
            None => Err(TransportError::PartitionNotFound(partition)),
        }
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

    async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::framework::VirtualClock;
    use mqdb::cluster::Operation;
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
