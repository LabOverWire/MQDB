use mqdb::cluster::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use mqdb::cluster::{
    ClusterMessage, ClusterTransport, Epoch, Heartbeat, InboundMessage, NodeId, PartitionId,
    ReplicationAck, ReplicationWrite, TransportError,
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
                partition_primaries: vec![None; 64],
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
        state.partition_primaries[partition.get() as usize] = primary.map(|n| n.get());
    }

    fn serialize_message(msg: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(msg.message_type());

        match msg {
            ClusterMessage::Heartbeat(hb) => {
                buf.extend_from_slice(&hb.to_be_bytes());
            }
            ClusterMessage::Write(w) => {
                buf.extend_from_slice(&w.to_bytes());
            }
            ClusterMessage::Ack(ack) => {
                buf.extend_from_slice(&ack.to_be_bytes());
            }
            ClusterMessage::DeathNotice { node_id } => {
                buf.extend_from_slice(&node_id.get().to_be_bytes());
            }
            ClusterMessage::RequestVote(req) => {
                buf.extend_from_slice(&req.to_be_bytes());
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
            ClusterMessage::AppendEntries(req) => {
                buf.extend_from_slice(&req.to_bytes());
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                buf.extend_from_slice(&resp.to_be_bytes());
            }
        }

        buf
    }

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
            2 => {
                if payload.len() < 2 {
                    return None;
                }
                let node_id = u16::from_be_bytes([payload[0], payload[1]]);
                let node = NodeId::validated(node_id)?;
                Some(ClusterMessage::DeathNotice { node_id: node })
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
            _ => None,
        }
    }
}

impl ClusterTransport for SimulatedTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        let data = Self::serialize_message(&message);
        if self.network.send(self.node_id.get(), to.get(), data) {
            Ok(())
        } else {
            Err(TransportError::NetworkPartitioned)
        }
    }

    fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        let data = Self::serialize_message(&message);
        let nodes = self.state.lock().unwrap().registered_nodes.clone();

        for &peer in &nodes {
            if peer != self.node_id.get() {
                self.network.send(self.node_id.get(), peer, data.clone());
            }
        }

        Ok(())
    }

    fn send_to_partition_primary(
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
                    self.send(node, message)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::framework::VirtualClock;
    use mqdb::cluster::Operation;
    use std::time::Duration;

    #[test]
    fn send_and_receive_heartbeat() {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let t1 = SimulatedTransport::new(node1, network.clone(), clock.clone());
        let t2 = SimulatedTransport::new(node2, network.clone(), clock.clone());

        t1.register_peer(node2);

        let hb = Heartbeat::create(node1, 1000);
        t1.send(node2, ClusterMessage::Heartbeat(hb)).unwrap();

        assert!(t2.recv().is_none());

        clock.advance(Duration::from_millis(1));

        let msg = t2.recv().unwrap();
        assert_eq!(msg.from, node1);
        match msg.message {
            ClusterMessage::Heartbeat(h) => assert_eq!(h.node_id(), 1),
            _ => panic!("expected heartbeat"),
        }
    }

    #[test]
    fn send_and_receive_write() {
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

        t1.send(node2, ClusterMessage::Write(write)).unwrap();
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

    #[test]
    fn broadcast_sends_to_all_peers() {
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
        t1.broadcast(ClusterMessage::Heartbeat(hb)).unwrap();

        clock.advance(Duration::from_millis(1));

        assert!(t2.recv().is_some());
        assert!(t3.recv().is_some());
        assert!(t1.recv().is_none());
    }
}
