// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::change_event::ChangeEventRecord;
use super::types::{AckStatus, Operation};
use super::wire::WireReader;
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
    pub event: Option<ChangeEventRecord>,
}

impl ReplicationWrite {
    pub const VERSION: u8 = 1;
    pub const WIDE_VERSION: u8 = 2;
    const EVENT_TRAILER: u8 = 1;

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
            event: None,
        }
    }

    #[must_use]
    pub fn with_event(mut self, event: Option<ChangeEventRecord>) -> Self {
        self.event = event;
        self
    }

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let entity = self.entity.as_bytes();
        let id = self.id.as_bytes();
        let narrow_lengths = u8::try_from(entity.len())
            .ok()
            .zip(u8::try_from(id.len()).ok());

        let mut buf = Vec::with_capacity(30 + entity.len() + id.len() + self.data.len());
        buf.push(if narrow_lengths.is_some() {
            Self::VERSION
        } else {
            Self::WIDE_VERSION
        });
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(self.operation as u8);
        buf.extend_from_slice(&u32::try_from(self.epoch.get()).ok()?.to_be_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        match narrow_lengths {
            Some((entity_len, id_len)) => buf.extend_from_slice(&[entity_len, id_len]),
            None => {
                buf.extend_from_slice(&u32::try_from(entity.len()).ok()?.to_be_bytes());
                buf.extend_from_slice(&u32::try_from(id.len()).ok()?.to_be_bytes());
            }
        }
        buf.extend_from_slice(&u32::try_from(self.data.len()).ok()?.to_be_bytes());
        buf.extend_from_slice(entity);
        buf.extend_from_slice(id);
        buf.extend_from_slice(&self.data);
        if let Some(event) = &self.event {
            let event_bytes = event.to_bytes()?;
            buf.push(Self::EVENT_TRAILER);
            buf.extend_from_slice(&u32::try_from(event_bytes.len()).ok()?.to_be_bytes());
            buf.extend_from_slice(&event_bytes);
        }

        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        let version = reader.u8()?;
        if version != Self::VERSION && version != Self::WIDE_VERSION {
            return None;
        }
        let wide = version == Self::WIDE_VERSION;

        let partition = PartitionId::new(reader.u16()?)?;
        let operation = Operation::from_u8(reader.u8()?)?;
        let epoch = Epoch::new(u64::from(reader.u32()?));
        let sequence = reader.u64()?;
        let mut field_length = || {
            if wide {
                usize::try_from(reader.u32()?).ok()
            } else {
                reader.u8().map(usize::from)
            }
        };
        let entity_len = field_length()?;
        let id_len = field_length()?;
        let data_len = usize::try_from(reader.u32()?).ok()?;
        let entity = reader.text(entity_len)?;
        let id = reader.text(id_len)?;
        let data = reader.take(data_len)?.to_vec();
        let event = if !reader.is_exhausted() && reader.u8()? == Self::EVENT_TRAILER {
            let event_len = usize::try_from(reader.u32()?).ok()?;
            Some(ChangeEventRecord::from_bytes(reader.take(event_len)?)?)
        } else {
            None
        };

        Some(Self {
            partition,
            operation,
            epoch,
            sequence,
            entity,
            id,
            data,
            event,
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
    pub fn ok(partition: PartitionId, epoch: Epoch, sequence: u64, node_id: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: AckStatus::Ok as u8,
            epoch: wire_epoch(epoch),
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
            epoch: wire_epoch(current_epoch),
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
            epoch: wire_epoch(epoch),
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

fn wire_epoch(epoch: Epoch) -> u32 {
    u32::try_from(epoch.get()).unwrap_or(u32::MAX)
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
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let write_bytes = self
            .writes
            .iter()
            .map(ReplicationWrite::to_bytes)
            .collect::<Option<Vec<_>>>()?;
        let total_write_len: usize = write_bytes.iter().map(|wb| 4 + wb.len()).sum();

        let mut buf = Vec::with_capacity(9 + total_write_len);
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&self.responder_id.get().to_be_bytes());
        buf.extend_from_slice(&u32::try_from(write_bytes.len()).ok()?.to_be_bytes());

        for wb in write_bytes {
            buf.extend_from_slice(&u32::try_from(wb.len()).ok()?.to_be_bytes());
            buf.extend_from_slice(&wb);
        }

        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        if reader.u8()? != Self::VERSION {
            return None;
        }

        let partition = PartitionId::new(reader.u16()?)?;
        let responder_id = NodeId::validated(reader.u16()?)?;
        let write_count = reader.u32()?;
        let writes = (0..write_count)
            .map(|_| {
                let write_len = usize::try_from(reader.u32()?).ok()?;
                ReplicationWrite::from_bytes(reader.take(write_len)?)
            })
            .collect::<Option<Vec<_>>>()?;

        Some(Self {
            partition,
            responder_id,
            writes,
        })
    }
}
