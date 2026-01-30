use crate::cluster::PartitionId;
use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WildcardOp {
    Subscribe = 0,
    Unsubscribe = 1,
}

impl WildcardOp {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Subscribe),
            1 => Some(Self::Unsubscribe),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct WildcardBroadcast {
    version: u8,
    operation: u8,
    timestamp_ms: u64,
    pattern_len: u16,
    #[FromField(pattern_len)]
    pattern: Vec<u8>,
    client_id_len: u8,
    #[FromField(client_id_len)]
    client_id: Vec<u8>,
    client_partition: u16,
    qos: u8,
    subscription_type: u8,
}

impl WildcardBroadcast {
    pub const VERSION: u8 = 2;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn subscribe(
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: u8,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            version: Self::VERSION,
            operation: WildcardOp::Subscribe as u8,
            timestamp_ms,
            pattern_len: pattern.len() as u16,
            pattern: pattern.as_bytes().to_vec(),
            client_id_len: client_id.len() as u8,
            client_id: client_id.as_bytes().to_vec(),
            client_partition: client_partition.get(),
            qos,
            subscription_type,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn unsubscribe(pattern: &str, client_id: &str) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            version: Self::VERSION,
            operation: WildcardOp::Unsubscribe as u8,
            timestamp_ms,
            pattern_len: pattern.len() as u16,
            pattern: pattern.as_bytes().to_vec(),
            client_id_len: client_id.len() as u8,
            client_id: client_id.as_bytes().to_vec(),
            client_partition: 0,
            qos: 0,
            subscription_type: 0,
        }
    }

    #[must_use]
    pub fn operation(&self) -> Option<WildcardOp> {
        WildcardOp::from_u8(self.operation)
    }

    #[must_use]
    pub fn pattern_str(&self) -> &str {
        std::str::from_utf8(&self.pattern).unwrap_or("")
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn client_partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.client_partition)
    }

    #[must_use]
    pub fn qos(&self) -> u8 {
        self.qos
    }

    #[must_use]
    pub fn subscription_type(&self) -> u8 {
        self.subscription_type
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct TopicSubscriptionBroadcast {
    version: u8,
    operation: u8,
    timestamp_ms: u64,
    topic_len: u16,
    #[FromField(topic_len)]
    topic: Vec<u8>,
    client_id_len: u8,
    #[FromField(client_id_len)]
    client_id: Vec<u8>,
    client_partition: u16,
    qos: u8,
}

impl TopicSubscriptionBroadcast {
    pub const VERSION: u8 = 2;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn subscribe(topic: &str, client_id: &str, client_partition: PartitionId, qos: u8) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        Self {
            version: Self::VERSION,
            operation: WildcardOp::Subscribe as u8,
            timestamp_ms,
            topic_len: topic.len() as u16,
            topic: topic.as_bytes().to_vec(),
            client_id_len: client_id.len() as u8,
            client_id: client_id.as_bytes().to_vec(),
            client_partition: client_partition.get(),
            qos,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn unsubscribe(topic: &str, client_id: &str) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        Self {
            version: Self::VERSION,
            operation: WildcardOp::Unsubscribe as u8,
            timestamp_ms,
            topic_len: topic.len() as u16,
            topic: topic.as_bytes().to_vec(),
            client_id_len: client_id.len() as u8,
            client_id: client_id.as_bytes().to_vec(),
            client_partition: 0,
            qos: 0,
        }
    }

    #[must_use]
    pub fn operation(&self) -> Option<WildcardOp> {
        WildcardOp::from_u8(self.operation)
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn client_partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.client_partition)
    }

    #[must_use]
    pub fn qos(&self) -> u8 {
        self.qos
    }
}
