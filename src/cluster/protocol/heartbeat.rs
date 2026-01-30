use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;

#[derive(Debug, Clone, BeBytes)]
pub struct Heartbeat {
    version: u8,
    node_id: u16,
    timestamp_ms: u64,
    primary_bitmap: u64,
    replica_bitmap: u64,
}

impl Heartbeat {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(node_id: NodeId, timestamp_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            node_id: node_id.get(),
            timestamp_ms,
            primary_bitmap: 0,
            replica_bitmap: 0,
        }
    }

    pub fn set_primary(&mut self, partition: PartitionId) {
        self.primary_bitmap |= 1u64 << partition.get();
    }

    pub fn set_replica(&mut self, partition: PartitionId) {
        self.replica_bitmap |= 1u64 << partition.get();
    }

    #[must_use]
    pub fn is_primary(&self, partition: PartitionId) -> bool {
        (self.primary_bitmap >> partition.get()) & 1 == 1
    }

    #[must_use]
    pub fn is_replica(&self, partition: PartitionId) -> bool {
        (self.replica_bitmap >> partition.get()) & 1 == 1
    }

    #[must_use]
    pub fn node_id(&self) -> u16 {
        self.node_id
    }

    #[must_use]
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}
