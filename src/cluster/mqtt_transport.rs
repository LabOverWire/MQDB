use super::protocol::{Heartbeat, ReplicationAck, ReplicationWrite};
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportError};
use super::{NodeId, PartitionId};
use bebytes::BeBytes;
use mqtt5::client::MqttClient;
use mqtt5::QoS;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

const CLUSTER_TOPIC_PREFIX: &str = "$CLUSTER";

#[derive(Debug)]
struct MqttTransportState {
    inbox: VecDeque<InboundMessage>,
    connected: bool,
}

pub struct MqttTransport {
    node_id: NodeId,
    client: MqttClient,
    state: Arc<Mutex<MqttTransportState>>,
    message_tx: mpsc::UnboundedSender<InboundMessage>,
}

impl std::fmt::Debug for MqttTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttTransport")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl MqttTransport {
    pub fn new(node_id: NodeId) -> Self {
        let client_id = format!("mqdb-node-{}", node_id.get());
        let client = MqttClient::new(&client_id);
        let (tx, mut rx) = mpsc::unbounded_channel();

        let state = Arc::new(Mutex::new(MqttTransportState {
            inbox: VecDeque::new(),
            connected: false,
        }));

        let state_clone = state.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let mut s = state_clone.lock().unwrap();
                s.inbox.push_back(msg);
            }
        });

        Self {
            node_id,
            client,
            state,
            message_tx: tx,
        }
    }

    pub async fn connect(&self, broker_addr: &str) -> Result<(), TransportError> {
        self.client
            .connect(broker_addr)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        {
            let mut state = self.state.lock().unwrap();
            state.connected = true;
        }

        self.subscribe_to_cluster_topics().await?;
        Ok(())
    }

    async fn subscribe_to_cluster_topics(&self) -> Result<(), TransportError> {
        let node_topic = format!("{}/node/{}", CLUSTER_TOPIC_PREFIX, self.node_id.get());
        let broadcast_topic = format!("{}/broadcast", CLUSTER_TOPIC_PREFIX);
        let heartbeat_topic = format!("{}/heartbeat/+", CLUSTER_TOPIC_PREFIX);

        let tx = self.message_tx.clone();
        let node_id = self.node_id;

        self.client
            .subscribe(&node_topic, {
                let tx = tx.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.client
            .subscribe(&broadcast_topic, {
                let tx = tx.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.client
            .subscribe(&heartbeat_topic, {
                let tx = tx.clone();
                move |msg| {
                    if let Some(inbound) = Self::parse_message(&msg.payload, node_id) {
                        let _ = tx.send(inbound);
                    }
                }
            })
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    fn parse_message(payload: &[u8], local_node: NodeId) -> Option<InboundMessage> {
        if payload.len() < 3 {
            return None;
        }

        let from_id = u16::from_be_bytes([payload[0], payload[1]]);
        let from = NodeId::validated(from_id)?;

        if from == local_node {
            return None;
        }

        let msg_type = payload[2];
        let data = &payload[3..];

        let message = match msg_type {
            0 => {
                let (hb, _) = Heartbeat::try_from_be_bytes(data).ok()?;
                ClusterMessage::Heartbeat(hb)
            }
            10 => {
                let w = ReplicationWrite::from_bytes(data)?;
                ClusterMessage::Write(w)
            }
            11 => {
                let (ack, _) = ReplicationAck::try_from_be_bytes(data).ok()?;
                ClusterMessage::Ack(ack)
            }
            2 => {
                if data.len() < 2 {
                    return None;
                }
                let dead_id = u16::from_be_bytes([data[0], data[1]]);
                let dead_node = NodeId::validated(dead_id)?;
                ClusterMessage::DeathNotice { node_id: dead_node }
            }
            _ => return None,
        };

        Some(InboundMessage {
            from,
            message,
            received_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        })
    }

    fn serialize_message(&self, message: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.node_id.get().to_be_bytes());
        buf.push(message.message_type());

        match message {
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
        }

        buf
    }

    pub async fn send_async(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        let topic = format!("{}/node/{}", CLUSTER_TOPIC_PREFIX, to.get());
        let payload = self.serialize_message(&message);

        self.client
            .publish_qos(&topic, payload, QoS::AtLeastOnce)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    pub async fn broadcast_async(&self, message: ClusterMessage) -> Result<(), TransportError> {
        let topic = match &message {
            ClusterMessage::Heartbeat(_) => format!("{}/heartbeat/{}", CLUSTER_TOPIC_PREFIX, self.node_id.get()),
            _ => format!("{}/broadcast", CLUSTER_TOPIC_PREFIX),
        };

        let payload = self.serialize_message(&message);

        self.client
            .publish_qos(&topic, payload, QoS::AtLeastOnce)
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), TransportError> {
        self.client
            .disconnect()
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let mut state = self.state.lock().unwrap();
        state.connected = false;
        Ok(())
    }
}

impl ClusterTransport for MqttTransport {
    fn local_node(&self) -> NodeId {
        self.node_id
    }

    fn send(&self, to: NodeId, message: ClusterMessage) -> Result<(), TransportError> {
        let state = self.state.lock().unwrap();
        if !state.connected {
            return Err(TransportError::NotConnected);
        }
        drop(state);

        let topic = format!("{}/node/{}", CLUSTER_TOPIC_PREFIX, to.get());
        let payload = self.serialize_message(&message);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish_qos(&topic, payload, QoS::AtLeastOnce).await;
        });

        Ok(())
    }

    fn broadcast(&self, message: ClusterMessage) -> Result<(), TransportError> {
        let state = self.state.lock().unwrap();
        if !state.connected {
            return Err(TransportError::NotConnected);
        }
        drop(state);

        let topic = match &message {
            ClusterMessage::Heartbeat(_) => format!("{}/heartbeat/{}", CLUSTER_TOPIC_PREFIX, self.node_id.get()),
            _ => format!("{}/broadcast", CLUSTER_TOPIC_PREFIX),
        };

        let payload = self.serialize_message(&message);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish_qos(&topic, payload, QoS::AtLeastOnce).await;
        });

        Ok(())
    }

    fn send_to_partition_primary(
        &self,
        _partition: PartitionId,
        _message: ClusterMessage,
    ) -> Result<(), TransportError> {
        Err(TransportError::PartitionNotFound(_partition))
    }

    fn recv(&self) -> Option<InboundMessage> {
        let mut state = self.state.lock().unwrap();
        state.inbox.pop_front()
    }

    fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
        self.recv()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize_message_for_test(node_id: NodeId, message: &ClusterMessage) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&node_id.get().to_be_bytes());
        buf.push(message.message_type());

        match message {
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
        }

        buf
    }

    #[test]
    fn serialize_and_parse_heartbeat() {
        let node1 = NodeId::validated(1).unwrap();

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = serialize_message_for_test(node1, &msg);

        let node2 = NodeId::validated(2).unwrap();
        let parsed = MqttTransport::parse_message(&bytes, node2);
        assert!(parsed.is_some());

        match parsed.unwrap().message {
            ClusterMessage::Heartbeat(h) => {
                assert_eq!(h.node_id(), 1);
            }
            _ => panic!("expected heartbeat"),
        }
    }

    #[test]
    fn parse_ignores_own_messages() {
        let node1 = NodeId::validated(1).unwrap();

        let hb = Heartbeat::create(node1, 1000);
        let msg = ClusterMessage::Heartbeat(hb);
        let bytes = serialize_message_for_test(node1, &msg);

        let parsed = MqttTransport::parse_message(&bytes, node1);
        assert!(parsed.is_none());
    }
}
