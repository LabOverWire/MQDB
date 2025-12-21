use super::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Heartbeat = 0,
    DetailedHeartbeat = 1,
    DeathNotice = 2,
    ReplicationWrite = 10,
    ReplicationAck = 11,
    CatchupRequest = 12,
    CatchupResponse = 13,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

impl Operation {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Insert),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckStatus {
    Ok = 0,
    StaleEpoch = 1,
    NotReplica = 2,
    SequenceGap = 3,
}

impl AckStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::StaleEpoch),
            2 => Some(Self::NotReplica),
            3 => Some(Self::SequenceGap),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::Ok
    }
}

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

#[derive(Debug, Clone)]
pub struct ReplicationWrite {
    pub partition: PartitionId,
    pub operation: Operation,
    pub epoch: Epoch,
    pub sequence: u64,
    pub entity: String,
    pub id: String,
    pub data: Vec<u8>,
}

impl ReplicationWrite {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        partition: PartitionId,
        operation: Operation,
        epoch: Epoch,
        sequence: u64,
        entity: String,
        id: String,
        data: Vec<u8>,
    ) -> Self {
        Self {
            partition,
            operation,
            epoch,
            sequence,
            entity,
            id,
            data,
        }
    }

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let id_bytes = self.id.as_bytes();

        let mut buf = Vec::with_capacity(20 + entity_bytes.len() + id_bytes.len() + self.data.len());

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(self.operation as u8);
        buf.extend_from_slice(&(self.epoch.get() as u32).to_be_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        buf.push(entity_bytes.len() as u8);
        buf.push(id_bytes.len() as u8);
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        buf.extend_from_slice(entity_bytes);
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(&self.data);

        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 20 {
            return None;
        }

        let _version = bytes[0];
        let partition = u16::from_be_bytes([bytes[1], bytes[2]]);
        let operation = Operation::from_u8(bytes[3])?;
        let epoch = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let sequence = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let entity_len = bytes[16] as usize;
        let id_len = bytes[17] as usize;
        let data_len = u32::from_be_bytes([bytes[18], bytes[19], bytes[20], bytes[21]]) as usize;

        let header_len = 22;
        if bytes.len() < header_len + entity_len + id_len + data_len {
            return None;
        }

        let entity_start = header_len;
        let id_start = entity_start + entity_len;
        let data_start = id_start + id_len;

        let entity = String::from_utf8(bytes[entity_start..id_start].to_vec()).ok()?;
        let id = String::from_utf8(bytes[id_start..data_start].to_vec()).ok()?;
        let data = bytes[data_start..data_start + data_len].to_vec();

        Some(Self {
            partition: PartitionId::new(partition)?,
            operation,
            epoch: Epoch::new(u64::from(epoch)),
            sequence,
            entity,
            id,
            data,
        })
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct ReplicationAck {
    version: u8,
    partition: u16,
    status: u8,
    epoch: u32,
    sequence: u64,
    node_id: u16,
}

impl ReplicationAck {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn ok(partition: PartitionId, epoch: Epoch, sequence: u64, node_id: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: AckStatus::Ok as u8,
            epoch: epoch.get() as u32,
            sequence,
            node_id: node_id.get(),
        }
    }

    #[must_use]
    pub fn stale_epoch(partition: PartitionId, current_epoch: Epoch, node_id: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: AckStatus::StaleEpoch as u8,
            epoch: current_epoch.get() as u32,
            sequence: 0,
            node_id: node_id.get(),
        }
    }

    #[must_use]
    pub fn not_replica(partition: PartitionId, node_id: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: AckStatus::NotReplica as u8,
            epoch: 0,
            sequence: 0,
            node_id: node_id.get(),
        }
    }

    #[must_use]
    pub fn sequence_gap(partition: PartitionId, epoch: Epoch, expected_seq: u64, node_id: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: AckStatus::SequenceGap as u8,
            epoch: epoch.get() as u32,
            sequence: expected_seq,
            node_id: node_id.get(),
        }
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        PartitionId::new(self.partition).unwrap_or_else(|| PartitionId::new(0).unwrap())
    }

    #[must_use]
    pub fn status(&self) -> Option<AckStatus> {
        AckStatus::from_u8(self.status)
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.status == AckStatus::Ok as u8
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        Epoch::new(u64::from(self.epoch))
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn node_id(&self) -> u16 {
        self.node_id
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct CatchupRequest {
    version: u8,
    partition: u16,
    from_sequence: u64,
    to_sequence: u64,
    requester_id: u16,
}

impl CatchupRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(partition: PartitionId, from_seq: u64, to_seq: u64, requester: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            from_sequence: from_seq,
            to_sequence: to_seq,
            requester_id: requester.get(),
        }
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        PartitionId::new(self.partition).unwrap_or_else(|| PartitionId::new(0).unwrap())
    }

    #[must_use]
    pub fn from_sequence(&self) -> u64 {
        self.from_sequence
    }

    #[must_use]
    pub fn to_sequence(&self) -> u64 {
        self.to_sequence
    }

    #[must_use]
    pub fn requester_id(&self) -> u16 {
        self.requester_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_bitmap_operations() {
        let node = NodeId::validated(1).unwrap();
        let mut hb = Heartbeat::create(node, 1000);

        let p0 = PartitionId::new(0).unwrap();
        let p5 = PartitionId::new(5).unwrap();
        let p63 = PartitionId::new(63).unwrap();

        hb.set_primary(p0);
        hb.set_primary(p63);
        hb.set_replica(p5);

        assert!(hb.is_primary(p0));
        assert!(hb.is_primary(p63));
        assert!(!hb.is_primary(p5));
        assert!(hb.is_replica(p5));
        assert!(!hb.is_replica(p0));
    }

    #[test]
    fn heartbeat_bebytes_roundtrip() {
        let node = NodeId::validated(42).unwrap();
        let mut hb = Heartbeat::create(node, 123456);
        hb.set_primary(PartitionId::new(10).unwrap());

        let bytes = hb.to_be_bytes();
        let (decoded, _) = Heartbeat::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.node_id(), 42);
        assert_eq!(decoded.timestamp_ms(), 123456);
        assert!(decoded.is_primary(PartitionId::new(10).unwrap()));
    }

    #[test]
    fn replication_write_roundtrip() {
        let partition = PartitionId::new(5).unwrap();
        let epoch = Epoch::new(10);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            epoch,
            100,
            "users".to_string(),
            "123".to_string(),
            b"{\"name\":\"Alice\"}".to_vec(),
        );

        let bytes = write.to_bytes();
        let decoded = ReplicationWrite::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition, partition);
        assert_eq!(decoded.operation, Operation::Insert);
        assert_eq!(decoded.epoch, epoch);
        assert_eq!(decoded.sequence, 100);
        assert_eq!(decoded.entity, "users");
        assert_eq!(decoded.id, "123");
        assert_eq!(decoded.data, b"{\"name\":\"Alice\"}");
    }

    #[test]
    fn replication_ack_statuses() {
        let partition = PartitionId::new(0).unwrap();
        let epoch = Epoch::new(5);
        let node = NodeId::validated(1).unwrap();

        let ok = ReplicationAck::ok(partition, epoch, 100, node);
        assert!(ok.is_ok());
        assert_eq!(ok.status(), Some(AckStatus::Ok));

        let stale = ReplicationAck::stale_epoch(partition, epoch, node);
        assert!(!stale.is_ok());
        assert_eq!(stale.status(), Some(AckStatus::StaleEpoch));

        let not_replica = ReplicationAck::not_replica(partition, node);
        assert_eq!(not_replica.status(), Some(AckStatus::NotReplica));

        let gap = ReplicationAck::sequence_gap(partition, epoch, 50, node);
        assert_eq!(gap.status(), Some(AckStatus::SequenceGap));
        assert_eq!(gap.sequence(), 50);
    }

    #[test]
    fn replication_ack_bebytes_roundtrip() {
        let partition = PartitionId::new(7).unwrap();
        let epoch = Epoch::new(3);
        let node = NodeId::validated(2).unwrap();

        let ack = ReplicationAck::ok(partition, epoch, 42, node);
        let bytes = ack.to_be_bytes();
        let (decoded, _) = ReplicationAck::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition(), partition);
        assert!(decoded.is_ok());
        assert_eq!(decoded.epoch(), epoch);
        assert_eq!(decoded.sequence(), 42);
        assert_eq!(decoded.node_id(), 2);
    }

    #[test]
    fn catchup_request_bebytes_roundtrip() {
        let partition = PartitionId::new(10).unwrap();
        let requester = NodeId::validated(5).unwrap();
        let req = CatchupRequest::create(partition, 100, 200, requester);

        let bytes = req.to_be_bytes();
        let (decoded, _) = CatchupRequest::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition(), partition);
        assert_eq!(decoded.from_sequence(), 100);
        assert_eq!(decoded.to_sequence(), 200);
        assert_eq!(decoded.requester_id(), 5);
    }
}
