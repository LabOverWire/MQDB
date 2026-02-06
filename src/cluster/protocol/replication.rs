use super::types::{AckStatus, Operation};
use crate::cluster::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;

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
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let id_bytes = self.id.as_bytes();

        let mut buf =
            Vec::with_capacity(20 + entity_bytes.len() + id_bytes.len() + self.data.len());

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

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 20 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }
        let partition = u16::from_be_bytes([bytes[1], bytes[2]]);
        let operation = Operation::from_u8(bytes[3])?;
        let epoch = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let sequence = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
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

#[derive(Debug, Clone, Copy, BeBytes)]
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
    #[allow(clippy::cast_possible_truncation)]
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
    #[allow(clippy::cast_possible_truncation)]
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
    #[allow(clippy::cast_possible_truncation)]
    pub fn sequence_gap(
        partition: PartitionId,
        epoch: Epoch,
        expected_seq: u64,
        node_id: NodeId,
    ) -> Self {
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
        PartitionId::new(self.partition).unwrap_or(PartitionId::ZERO)
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
        PartitionId::new(self.partition).unwrap_or(PartitionId::ZERO)
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

#[derive(Debug, Clone)]
pub struct CatchupResponse {
    pub partition: PartitionId,
    pub responder_id: NodeId,
    pub writes: Vec<ReplicationWrite>,
}

impl CatchupResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(
        partition: PartitionId,
        responder: NodeId,
        writes: Vec<ReplicationWrite>,
    ) -> Self {
        Self {
            partition,
            responder_id: responder,
            writes,
        }
    }

    #[must_use]
    pub fn empty(partition: PartitionId, responder: NodeId) -> Self {
        Self {
            partition,
            responder_id: responder,
            writes: Vec::new(),
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let write_bytes: Vec<Vec<u8>> =
            self.writes.iter().map(ReplicationWrite::to_bytes).collect();
        let total_write_len: usize = write_bytes.iter().map(Vec::len).sum();

        let mut buf = Vec::with_capacity(9 + total_write_len);
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&self.responder_id.get().to_be_bytes());
        buf.extend_from_slice(&(self.writes.len() as u32).to_be_bytes());

        for wb in write_bytes {
            buf.extend_from_slice(&(wb.len() as u32).to_be_bytes());
            buf.extend_from_slice(&wb);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let partition = u16::from_be_bytes([bytes[1], bytes[2]]);
        let responder = u16::from_be_bytes([bytes[3], bytes[4]]);
        let write_count = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;

        let mut offset = 9;
        let mut writes = Vec::with_capacity(write_count);

        for _ in 0..write_count {
            if offset + 4 > bytes.len() {
                return None;
            }
            let write_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + write_len > bytes.len() {
                return None;
            }
            let write = ReplicationWrite::from_bytes(&bytes[offset..offset + write_len])?;
            writes.push(write);
            offset += write_len;
        }

        Some(Self {
            partition: PartitionId::new(partition)?,
            responder_id: NodeId::validated(responder)?,
            writes,
        })
    }
}
