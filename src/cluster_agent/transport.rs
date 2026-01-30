#[allow(deprecated)]
use crate::cluster::{
    ClusterMessage, ClusterTransport, InboundMessage, MqttTransport, NodeId, PartitionId,
    QuicDirectTransport,
};

#[derive(Debug, Clone)]
pub enum ClusterTransportKind {
    #[deprecated(since = "0.2.0", note = "use Quic variant instead")]
    Mqtt(#[allow(deprecated)] MqttTransport),
    Quic(QuicDirectTransport),
}

impl ClusterTransportKind {
    #[deprecated(since = "0.2.0", note = "MQTT transport is deprecated, use QUIC")]
    #[must_use]
    #[allow(deprecated)]
    pub fn as_mqtt(&self) -> Option<&MqttTransport> {
        match self {
            #[allow(deprecated)]
            Self::Mqtt(t) => Some(t),
            Self::Quic(_) => None,
        }
    }

    #[must_use]
    #[allow(deprecated)]
    pub fn as_quic(&self) -> Option<&QuicDirectTransport> {
        match self {
            Self::Mqtt(_) => None,
            Self::Quic(t) => Some(t),
        }
    }

    pub fn log_queue_stats(&self) {
        if let Self::Quic(t) = self {
            t.log_queue_stats();
        }
    }
}

#[allow(deprecated)]
impl ClusterTransport for ClusterTransportKind {
    fn local_node(&self) -> NodeId {
        match self {
            Self::Mqtt(t) => t.local_node(),
            Self::Quic(t) => t.local_node(),
        }
    }

    async fn send(
        &self,
        to: NodeId,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.send(to, message).await,
            Self::Quic(t) => t.send(to, message).await,
        }
    }

    async fn broadcast(
        &self,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.broadcast(message).await,
            Self::Quic(t) => t.broadcast(message).await,
        }
    }

    async fn send_to_partition_primary(
        &self,
        partition: PartitionId,
        message: ClusterMessage,
    ) -> Result<(), crate::cluster::TransportError> {
        match self {
            Self::Mqtt(t) => t.send_to_partition_primary(partition, message).await,
            Self::Quic(t) => t.send_to_partition_primary(partition, message).await,
        }
    }

    fn recv(&self) -> Option<InboundMessage> {
        match self {
            Self::Mqtt(t) => t.recv(),
            Self::Quic(t) => t.recv(),
        }
    }

    fn try_recv_timeout(&self, timeout_ms: u64) -> Option<InboundMessage> {
        match self {
            Self::Mqtt(t) => t.try_recv_timeout(timeout_ms),
            Self::Quic(t) => t.try_recv_timeout(timeout_ms),
        }
    }

    fn pending_count(&self) -> usize {
        match self {
            Self::Mqtt(t) => t.pending_count(),
            Self::Quic(t) => t.pending_count(),
        }
    }

    fn requeue(&self, msg: InboundMessage) {
        match self {
            Self::Mqtt(t) => t.requeue(msg),
            Self::Quic(t) => t.requeue(msg),
        }
    }

    async fn queue_local_publish(&self, topic: String, payload: Vec<u8>, qos: u8) {
        match self {
            Self::Mqtt(t) => t.queue_local_publish(topic, payload, qos).await,
            Self::Quic(t) => t.queue_local_publish(topic, payload, qos).await,
        }
    }

    async fn queue_local_publish_retained(&self, topic: String, payload: Vec<u8>, qos: u8) {
        match self {
            Self::Mqtt(t) => t.queue_local_publish_retained(topic, payload, qos).await,
            Self::Quic(t) => t.queue_local_publish_retained(topic, payload, qos).await,
        }
    }
}
