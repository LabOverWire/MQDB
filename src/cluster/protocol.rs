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
    WriteRequest = 20,
    ForwardedPublish = 30,
    SnapshotRequest = 40,
    SnapshotChunk = 41,
    SnapshotComplete = 42,
    QueryRequest = 50,
    QueryResponse = 51,
    BatchReadRequest = 52,
    BatchReadResponse = 53,
    JsonDbRequest = 54,
    JsonDbResponse = 55,
    UniqueReserveRequest = 80,
    UniqueReserveResponse = 81,
    UniqueCommitRequest = 82,
    UniqueCommitResponse = 83,
    UniqueReleaseRequest = 84,
    UniqueReleaseResponse = 85,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryStatus {
    Ok = 0,
    Timeout = 1,
    Error = 2,
    NotPrimary = 3,
}

impl QueryStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::Timeout),
            2 => Some(Self::Error),
            3 => Some(Self::NotPrimary),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::Ok
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JsonDbOp {
    Read = 0,
    Update = 1,
    Delete = 2,
    List = 3,
    Create = 4,
}

impl JsonDbOp {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Read),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            3 => Some(Self::List),
            4 => Some(Self::Create),
            _ => None,
        }
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

    /// # Panics
    /// Panics if partition ID 0 is invalid (should not happen).
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

    /// # Panics
    /// Panics if partition ID 0 is invalid (should not happen).
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

#[derive(Debug, Clone)]
pub struct ForwardTarget {
    pub client_id: String,
    pub qos: u8,
}

impl ForwardTarget {
    #[must_use]
    pub fn new(client_id: String, qos: u8) -> Self {
        Self { client_id, qos }
    }
}

#[derive(Debug, Clone)]
pub struct ForwardedPublish {
    pub origin_node: NodeId,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub payload: Vec<u8>,
    pub targets: Vec<ForwardTarget>,
    pub timestamp_ms: u64,
}

impl ForwardedPublish {
    pub const VERSION: u8 = 2;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        origin_node: NodeId,
        topic: String,
        qos: u8,
        retain: bool,
        payload: Vec<u8>,
        targets: Vec<ForwardTarget>,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        Self {
            origin_node,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_ms,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let topic_bytes = self.topic.as_bytes();

        let targets_size: usize = self.targets.iter().map(|t| 2 + t.client_id.len()).sum();

        let mut buf =
            Vec::with_capacity(20 + topic_bytes.len() + self.payload.len() + targets_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.origin_node.get().to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic_bytes);
        buf.push(self.qos);
        buf.push(u8::from(self.retain));
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.push(self.targets.len() as u8);

        for target in &self.targets {
            let client_bytes = target.client_id.as_bytes();
            buf.push(client_bytes.len() as u8);
            buf.extend_from_slice(client_bytes);
            buf.push(target.qos);
        }

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

        let origin_node = u16::from_be_bytes([bytes[1], bytes[2]]);
        let timestamp_ms = u64::from_be_bytes([
            bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10],
        ]);
        let topic_len = u16::from_be_bytes([bytes[11], bytes[12]]) as usize;

        let mut offset = 13;
        if bytes.len() < offset + topic_len + 7 {
            return None;
        }

        let topic = String::from_utf8(bytes[offset..offset + topic_len].to_vec()).ok()?;
        offset += topic_len;

        let qos = bytes[offset];
        offset += 1;

        let retain = bytes[offset] != 0;
        offset += 1;

        if bytes.len() < offset + 4 {
            return None;
        }
        let payload_len = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + payload_len + 1 {
            return None;
        }
        let payload = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;

        let target_count = bytes[offset] as usize;
        offset += 1;

        let mut targets = Vec::with_capacity(target_count);
        for _ in 0..target_count {
            if offset >= bytes.len() {
                return None;
            }
            let client_id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + client_id_len + 1 {
                return None;
            }
            let client_id =
                String::from_utf8(bytes[offset..offset + client_id_len].to_vec()).ok()?;
            offset += client_id_len;

            let target_qos = bytes[offset];
            offset += 1;

            targets.push(ForwardTarget {
                client_id,
                qos: target_qos,
            });
        }

        Some(Self {
            origin_node: NodeId::validated(origin_node)?,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query_id: u64,
    pub timeout_ms: u32,
    pub entity: String,
    pub filter: Option<String>,
    pub limit: u32,
    pub cursor: Option<Vec<u8>>,
}

impl QueryRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        query_id: u64,
        timeout_ms: u32,
        entity: String,
        filter: Option<String>,
        limit: u32,
        cursor: Option<Vec<u8>>,
    ) -> Self {
        Self {
            query_id,
            timeout_ms,
            entity,
            filter,
            limit,
            cursor,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let filter_bytes = self.filter.as_ref().map(String::as_bytes);
        let filter_len = filter_bytes.map_or(0, <[u8]>::len);
        let cursor_len = self.cursor.as_ref().map_or(0, Vec::len);

        let mut buf = Vec::with_capacity(21 + entity_bytes.len() + filter_len + cursor_len);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.query_id.to_be_bytes());
        buf.extend_from_slice(&self.timeout_ms.to_be_bytes());
        buf.push(entity_bytes.len() as u8);
        buf.extend_from_slice(entity_bytes);
        buf.extend_from_slice(&(filter_len as u16).to_be_bytes());
        if let Some(fb) = filter_bytes {
            buf.extend_from_slice(fb);
        }
        buf.extend_from_slice(&self.limit.to_be_bytes());
        buf.extend_from_slice(&(cursor_len as u16).to_be_bytes());
        if let Some(c) = &self.cursor {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 21 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let query_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let timeout_ms = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);
        let entity_len = bytes[13] as usize;

        let mut offset = 14;
        if bytes.len() < offset + entity_len + 8 {
            return None;
        }

        let entity = String::from_utf8(bytes[offset..offset + entity_len].to_vec()).ok()?;
        offset += entity_len;

        let filter_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let filter = if filter_len > 0 {
            if bytes.len() < offset + filter_len {
                return None;
            }
            Some(String::from_utf8(bytes[offset..offset + filter_len].to_vec()).ok()?)
        } else {
            None
        };
        offset += filter_len;

        if bytes.len() < offset + 6 {
            return None;
        }

        let limit = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let cursor_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let cursor = if cursor_len > 0 {
            if bytes.len() < offset + cursor_len {
                return None;
            }
            Some(bytes[offset..offset + cursor_len].to_vec())
        } else {
            None
        };

        Some(Self {
            query_id,
            timeout_ms,
            entity,
            filter,
            limit,
            cursor,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub query_id: u64,
    pub partition: PartitionId,
    pub status: QueryStatus,
    pub results: Vec<u8>,
    pub has_more: bool,
    pub cursor: Option<Vec<u8>>,
}

impl QueryResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn ok(
        query_id: u64,
        partition: PartitionId,
        results: Vec<u8>,
        has_more: bool,
        cursor: Option<Vec<u8>>,
    ) -> Self {
        Self {
            query_id,
            partition,
            status: QueryStatus::Ok,
            results,
            has_more,
            cursor,
        }
    }

    #[must_use]
    pub fn error(query_id: u64, partition: PartitionId, status: QueryStatus) -> Self {
        Self {
            query_id,
            partition,
            status,
            results: Vec::new(),
            has_more: false,
            cursor: None,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let cursor_len = self.cursor.as_ref().map_or(0, Vec::len);
        let mut buf = Vec::with_capacity(18 + self.results.len() + cursor_len);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.query_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(self.status as u8);
        buf.extend_from_slice(&(self.results.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.results);
        buf.push(u8::from(self.has_more));
        buf.extend_from_slice(&(cursor_len as u16).to_be_bytes());
        if let Some(c) = &self.cursor {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 18 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let query_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let status = QueryStatus::from_u8(bytes[11])?;
        let results_len = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;

        let mut offset = 16;
        if bytes.len() < offset + results_len + 3 {
            return None;
        }

        let results = bytes[offset..offset + results_len].to_vec();
        offset += results_len;

        let has_more = bytes[offset] != 0;
        offset += 1;

        let cursor_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let cursor = if cursor_len > 0 {
            if bytes.len() < offset + cursor_len {
                return None;
            }
            Some(bytes[offset..offset + cursor_len].to_vec())
        } else {
            None
        };

        Some(Self {
            query_id,
            partition: PartitionId::new(partition)?,
            status,
            results,
            has_more,
            cursor,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JsonDbRequest {
    pub request_id: u64,
    pub op: JsonDbOp,
    pub entity: String,
    pub id: Option<String>,
    pub payload: Vec<u8>,
    pub response_topic: String,
    pub correlation_data: Option<Vec<u8>>,
}

impl JsonDbRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        request_id: u64,
        op: JsonDbOp,
        entity: String,
        id: Option<String>,
        payload: Vec<u8>,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            request_id,
            op,
            entity,
            id,
            payload,
            response_topic,
            correlation_data,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let id_bytes = self.id.as_ref().map(String::as_bytes);
        let id_len = id_bytes.map_or(0, <[u8]>::len);
        let response_topic_bytes = self.response_topic.as_bytes();
        let corr_len = self.correlation_data.as_ref().map_or(0, Vec::len);

        let mut buf = Vec::with_capacity(
            18 + entity_bytes.len()
                + id_len
                + self.payload.len()
                + response_topic_bytes.len()
                + corr_len,
        );

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.push(self.op as u8);
        buf.push(entity_bytes.len() as u8);
        buf.extend_from_slice(entity_bytes);
        buf.push(id_len as u8);
        if let Some(ib) = id_bytes {
            buf.extend_from_slice(ib);
        }
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&(response_topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(response_topic_bytes);
        buf.extend_from_slice(&(corr_len as u16).to_be_bytes());
        if let Some(c) = &self.correlation_data {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 18 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let op = JsonDbOp::from_u8(bytes[9])?;
        let entity_len = bytes[10] as usize;

        let mut offset = 11;
        if bytes.len() < offset + entity_len + 1 {
            return None;
        }

        let entity = String::from_utf8(bytes[offset..offset + entity_len].to_vec()).ok()?;
        offset += entity_len;

        let id_len = bytes[offset] as usize;
        offset += 1;

        let id = if id_len > 0 {
            if bytes.len() < offset + id_len + 4 {
                return None;
            }
            let i = String::from_utf8(bytes[offset..offset + id_len].to_vec()).ok()?;
            offset += id_len;
            Some(i)
        } else {
            None
        };

        if bytes.len() < offset + 4 {
            return None;
        }

        let payload_len = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + payload_len + 4 {
            return None;
        }

        let payload = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;

        let response_topic_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        if bytes.len() < offset + response_topic_len + 2 {
            return None;
        }

        let response_topic =
            String::from_utf8(bytes[offset..offset + response_topic_len].to_vec()).ok()?;
        offset += response_topic_len;

        let corr_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let correlation_data = if corr_len > 0 {
            if bytes.len() < offset + corr_len {
                return None;
            }
            Some(bytes[offset..offset + corr_len].to_vec())
        } else {
            None
        };

        Some(Self {
            request_id,
            op,
            entity,
            id,
            payload,
            response_topic,
            correlation_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JsonDbResponse {
    pub request_id: u64,
    pub payload: Vec<u8>,
    pub response_topic: String,
    pub correlation_data: Option<Vec<u8>>,
}

impl JsonDbResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        request_id: u64,
        payload: Vec<u8>,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            request_id,
            payload,
            response_topic,
            correlation_data,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let response_topic_bytes = self.response_topic.as_bytes();
        let corr_len = self.correlation_data.as_ref().map_or(0, Vec::len);

        let mut buf =
            Vec::with_capacity(15 + self.payload.len() + response_topic_bytes.len() + corr_len);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&(response_topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(response_topic_bytes);
        buf.extend_from_slice(&(corr_len as u16).to_be_bytes());
        if let Some(c) = &self.correlation_data {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 15 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);

        let payload_len = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
        let mut offset = 13;

        if bytes.len() < offset + payload_len + 4 {
            return None;
        }

        let payload = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;

        let response_topic_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        if bytes.len() < offset + response_topic_len + 2 {
            return None;
        }

        let response_topic =
            String::from_utf8(bytes[offset..offset + response_topic_len].to_vec()).ok()?;
        offset += response_topic_len;

        let corr_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let correlation_data = if corr_len > 0 {
            if bytes.len() < offset + corr_len {
                return None;
            }
            Some(bytes[offset..offset + corr_len].to_vec())
        } else {
            None
        };

        Some(Self {
            request_id,
            payload,
            response_topic,
            correlation_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BatchReadRequest {
    pub request_id: u64,
    pub partition: PartitionId,
    pub entity: String,
    pub ids: Vec<String>,
}

impl BatchReadRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(request_id: u64, partition: PartitionId, entity: String, ids: Vec<String>) -> Self {
        Self {
            request_id,
            partition,
            entity,
            ids,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let ids_size: usize = self.ids.iter().map(|id| 1 + id.len()).sum();

        let mut buf = Vec::with_capacity(14 + entity_bytes.len() + ids_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(entity_bytes.len() as u8);
        buf.extend_from_slice(entity_bytes);
        buf.extend_from_slice(&(self.ids.len() as u16).to_be_bytes());
        for id in &self.ids {
            let id_bytes = id.as_bytes();
            buf.push(id_bytes.len() as u8);
            buf.extend_from_slice(id_bytes);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 14 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let entity_len = bytes[11] as usize;

        let mut offset = 12;
        if bytes.len() < offset + entity_len + 2 {
            return None;
        }

        let entity = String::from_utf8(bytes[offset..offset + entity_len].to_vec()).ok()?;
        offset += entity_len;

        let id_count = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            if offset >= bytes.len() {
                return None;
            }
            let id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + id_len {
                return None;
            }
            let id = String::from_utf8(bytes[offset..offset + id_len].to_vec()).ok()?;
            ids.push(id);
            offset += id_len;
        }

        Some(Self {
            request_id,
            partition: PartitionId::new(partition)?,
            entity,
            ids,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BatchReadResponse {
    pub request_id: u64,
    pub partition: PartitionId,
    pub results: Vec<(String, Option<Vec<u8>>)>,
}

impl BatchReadResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        request_id: u64,
        partition: PartitionId,
        results: Vec<(String, Option<Vec<u8>>)>,
    ) -> Self {
        Self {
            request_id,
            partition,
            results,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let results_size: usize = self
            .results
            .iter()
            .map(|(id, data)| 1 + id.len() + 4 + data.as_ref().map_or(0, Vec::len))
            .sum();

        let mut buf = Vec::with_capacity(13 + results_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&(self.results.len() as u16).to_be_bytes());

        for (id, data) in &self.results {
            let id_bytes = id.as_bytes();
            buf.push(id_bytes.len() as u8);
            buf.extend_from_slice(id_bytes);

            match data {
                Some(d) => {
                    buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                    buf.extend_from_slice(d);
                }
                None => {
                    buf.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
                }
            }
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 13 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let result_count = u16::from_be_bytes([bytes[11], bytes[12]]) as usize;

        let mut offset = 13;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            if offset >= bytes.len() {
                return None;
            }
            let id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + id_len + 4 {
                return None;
            }
            let id = String::from_utf8(bytes[offset..offset + id_len].to_vec()).ok()?;
            offset += id_len;

            let data_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            offset += 4;

            let data = if data_len == 0xFFFF_FFFF {
                None
            } else {
                let len = data_len as usize;
                if bytes.len() < offset + len {
                    return None;
                }
                let d = bytes[offset..offset + len].to_vec();
                offset += len;
                Some(d)
            };

            results.push((id, data));
        }

        Some(Self {
            request_id,
            partition: PartitionId::new(partition)?,
            results,
        })
    }
}

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
    pub fn subscribe(
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UniqueReserveStatus {
    Reserved = 0,
    AlreadyReserved = 1,
    Conflict = 2,
    Error = 3,
}

impl UniqueReserveStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Reserved,
            1 => Self::AlreadyReserved,
            2 => Self::Conflict,
            _ => Self::Error,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        matches!(self, Self::Reserved | Self::AlreadyReserved)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReserveRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub record_id_len: u16,
    #[FromField(record_id_len)]
    pub record_id: Vec<u8>,
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
    pub data_partition: u16,
    pub ttl_ms: u64,
}

impl UniqueReserveRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        idempotency_key: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            record_id_len: record_id.len() as u16,
            record_id: record_id.as_bytes().to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
            data_partition: data_partition.get(),
            ttl_ms,
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn record_id_str(&self) -> &str {
        std::str::from_utf8(&self.record_id).unwrap_or("")
    }

    #[must_use]
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }

    #[must_use]
    pub fn data_partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.data_partition)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReserveResponse {
    pub version: u8,
    pub request_id: u64,
    pub result: u8,
}

impl UniqueReserveResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, result: UniqueReserveStatus) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            result: result as u8,
        }
    }

    #[must_use]
    pub fn status(&self) -> UniqueReserveStatus {
        UniqueReserveStatus::from_u8(self.result)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueCommitRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
}

impl UniqueCommitRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueCommitResponse {
    pub version: u8,
    pub request_id: u64,
    pub success: u8,
}

impl UniqueCommitResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, success: bool) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            success: u8::from(success),
        }
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.success != 0
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReleaseRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
}

impl UniqueReleaseRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReleaseResponse {
    pub version: u8,
    pub request_id: u64,
    pub success: u8,
}

impl UniqueReleaseResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, success: bool) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            success: u8::from(success),
        }
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.success != 0
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
        let mut hb = Heartbeat::create(node, 123_456);
        hb.set_primary(PartitionId::new(10).unwrap());

        let bytes = hb.to_be_bytes();
        let (decoded, _) = Heartbeat::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.node_id(), 42);
        assert_eq!(decoded.timestamp_ms(), 123_456);
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

    #[test]
    fn catchup_response_roundtrip() {
        let partition = PartitionId::new(7).unwrap();
        let responder = NodeId::validated(3).unwrap();
        let epoch = Epoch::new(5);

        let writes = vec![
            ReplicationWrite::new(
                partition,
                Operation::Insert,
                epoch,
                1,
                "_sessions".to_string(),
                "client1".to_string(),
                b"data1".to_vec(),
            ),
            ReplicationWrite::new(
                partition,
                Operation::Update,
                epoch,
                2,
                "_sessions".to_string(),
                "client2".to_string(),
                b"data2".to_vec(),
            ),
        ];

        let resp = CatchupResponse::create(partition, responder, writes);
        let bytes = resp.to_bytes();
        let decoded = CatchupResponse::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition, partition);
        assert_eq!(decoded.responder_id, responder);
        assert_eq!(decoded.writes.len(), 2);
        assert_eq!(decoded.writes[0].sequence, 1);
        assert_eq!(decoded.writes[0].id, "client1");
        assert_eq!(decoded.writes[1].sequence, 2);
        assert_eq!(decoded.writes[1].id, "client2");
    }

    #[test]
    fn catchup_response_empty() {
        let partition = PartitionId::new(0).unwrap();
        let responder = NodeId::validated(1).unwrap();

        let resp = CatchupResponse::empty(partition, responder);
        let bytes = resp.to_bytes();
        let decoded = CatchupResponse::from_bytes(&bytes).unwrap();

        assert!(decoded.writes.is_empty());
    }

    #[test]
    fn forwarded_publish_roundtrip() {
        let origin = NodeId::validated(2).unwrap();
        let targets = vec![
            ForwardTarget::new("client-a".to_string(), 1),
            ForwardTarget::new("client-b".to_string(), 2),
        ];

        let fwd = ForwardedPublish::new(
            origin,
            "sensors/temp".to_string(),
            1,
            false,
            b"25.5".to_vec(),
            targets,
        );

        let bytes = fwd.to_bytes();
        let decoded = ForwardedPublish::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.origin_node, origin);
        assert_eq!(decoded.topic, "sensors/temp");
        assert_eq!(decoded.qos, 1);
        assert!(!decoded.retain);
        assert_eq!(decoded.payload, b"25.5");
        assert_eq!(decoded.targets.len(), 2);
        assert_eq!(decoded.targets[0].client_id, "client-a");
        assert_eq!(decoded.targets[0].qos, 1);
        assert_eq!(decoded.targets[1].client_id, "client-b");
        assert_eq!(decoded.targets[1].qos, 2);
    }

    #[test]
    fn forwarded_publish_empty_targets() {
        let origin = NodeId::validated(1).unwrap();
        let fwd = ForwardedPublish::new(origin, "test/topic".to_string(), 0, true, vec![], vec![]);

        let bytes = fwd.to_bytes();
        let decoded = ForwardedPublish::from_bytes(&bytes).unwrap();

        assert!(decoded.retain);
        assert!(decoded.payload.is_empty());
        assert!(decoded.targets.is_empty());
    }

    #[test]
    fn query_request_roundtrip() {
        let req = QueryRequest::new(
            12345,
            5000,
            "users".to_string(),
            Some("age > 30".to_string()),
            100,
            Some(b"cursor-data".to_vec()),
        );

        let bytes = req.to_bytes();
        let decoded = QueryRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.query_id, 12345);
        assert_eq!(decoded.timeout_ms, 5000);
        assert_eq!(decoded.entity, "users");
        assert_eq!(decoded.filter, Some("age > 30".to_string()));
        assert_eq!(decoded.limit, 100);
        assert_eq!(decoded.cursor, Some(b"cursor-data".to_vec()));
    }

    #[test]
    fn query_request_no_filter_no_cursor() {
        let req = QueryRequest::new(1, 1000, "sessions".to_string(), None, 50, None);

        let bytes = req.to_bytes();
        let decoded = QueryRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.query_id, 1);
        assert_eq!(decoded.entity, "sessions");
        assert!(decoded.filter.is_none());
        assert!(decoded.cursor.is_none());
    }

    #[test]
    fn query_response_roundtrip() {
        let partition = PartitionId::new(17).unwrap();
        let resp = QueryResponse::ok(
            999,
            partition,
            b"result-data".to_vec(),
            true,
            Some(b"next-cursor".to_vec()),
        );

        let bytes = resp.to_bytes();
        let decoded = QueryResponse::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.query_id, 999);
        assert_eq!(decoded.partition, partition);
        assert!(decoded.status.is_ok());
        assert_eq!(decoded.results, b"result-data");
        assert!(decoded.has_more);
        assert_eq!(decoded.cursor, Some(b"next-cursor".to_vec()));
    }

    #[test]
    fn query_response_error() {
        let partition = PartitionId::new(5).unwrap();
        let resp = QueryResponse::error(123, partition, QueryStatus::Timeout);

        let bytes = resp.to_bytes();
        let decoded = QueryResponse::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.query_id, 123);
        assert_eq!(decoded.status, QueryStatus::Timeout);
        assert!(decoded.results.is_empty());
        assert!(!decoded.has_more);
    }

    #[test]
    fn batch_read_request_roundtrip() {
        let partition = PartitionId::new(10).unwrap();
        let req = BatchReadRequest::new(
            555,
            partition,
            "users".to_string(),
            vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
        );

        let bytes = req.to_bytes();
        let decoded = BatchReadRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 555);
        assert_eq!(decoded.partition, partition);
        assert_eq!(decoded.entity, "users");
        assert_eq!(decoded.ids.len(), 3);
        assert_eq!(decoded.ids[0], "id1");
        assert_eq!(decoded.ids[1], "id2");
        assert_eq!(decoded.ids[2], "id3");
    }

    #[test]
    fn batch_read_response_roundtrip() {
        let partition = PartitionId::new(20).unwrap();
        let results = vec![
            ("id1".to_string(), Some(b"data1".to_vec())),
            ("id2".to_string(), None),
            ("id3".to_string(), Some(b"data3".to_vec())),
        ];
        let resp = BatchReadResponse::new(777, partition, results);

        let bytes = resp.to_bytes();
        let decoded = BatchReadResponse::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 777);
        assert_eq!(decoded.partition, partition);
        assert_eq!(decoded.results.len(), 3);
        assert_eq!(
            decoded.results[0],
            ("id1".to_string(), Some(b"data1".to_vec()))
        );
        assert_eq!(decoded.results[1], ("id2".to_string(), None));
        assert_eq!(
            decoded.results[2],
            ("id3".to_string(), Some(b"data3".to_vec()))
        );
    }

    #[test]
    fn query_status_from_u8() {
        assert_eq!(QueryStatus::from_u8(0), Some(QueryStatus::Ok));
        assert_eq!(QueryStatus::from_u8(1), Some(QueryStatus::Timeout));
        assert_eq!(QueryStatus::from_u8(2), Some(QueryStatus::Error));
        assert_eq!(QueryStatus::from_u8(3), Some(QueryStatus::NotPrimary));
        assert_eq!(QueryStatus::from_u8(255), None);
    }

    #[test]
    fn unique_reserve_request_roundtrip() {
        let partition = PartitionId::new(10).unwrap();
        let req = UniqueReserveRequest::create(
            12345,
            "users",
            "email",
            b"test@example.com",
            "user-123",
            "req-abc",
            partition,
            30_000,
        );

        let bytes = req.to_be_bytes();
        let (decoded, _) = UniqueReserveRequest::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.entity_str(), "users");
        assert_eq!(decoded.field_str(), "email");
        assert_eq!(decoded.value, b"test@example.com");
        assert_eq!(decoded.record_id_str(), "user-123");
        assert_eq!(decoded.idempotency_key_str(), "req-abc");
        assert_eq!(decoded.data_partition(), Some(partition));
        assert_eq!(decoded.ttl_ms, 30_000);
    }

    #[test]
    fn unique_reserve_response_roundtrip() {
        let resp = UniqueReserveResponse::create(12345, UniqueReserveStatus::Reserved);

        let bytes = resp.to_be_bytes();
        let (decoded, _) = UniqueReserveResponse::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.status(), UniqueReserveStatus::Reserved);
        assert!(decoded.status().is_ok());
    }

    #[test]
    fn unique_commit_request_roundtrip() {
        let req =
            UniqueCommitRequest::create(999, "users", "email", b"test@example.com", "req-abc");

        let bytes = req.to_be_bytes();
        let (decoded, _) = UniqueCommitRequest::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 999);
        assert_eq!(decoded.entity_str(), "users");
        assert_eq!(decoded.field_str(), "email");
        assert_eq!(decoded.value, b"test@example.com");
        assert_eq!(decoded.idempotency_key_str(), "req-abc");
    }

    #[test]
    fn unique_commit_response_roundtrip() {
        let resp = UniqueCommitResponse::create(999, true);

        let bytes = resp.to_be_bytes();
        let (decoded, _) = UniqueCommitResponse::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 999);
        assert!(decoded.is_success());
    }

    #[test]
    fn unique_release_request_roundtrip() {
        let req = UniqueReleaseRequest::create(777, "orders", "order_id", b"ORD-001", "req-xyz");

        let bytes = req.to_be_bytes();
        let (decoded, _) = UniqueReleaseRequest::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 777);
        assert_eq!(decoded.entity_str(), "orders");
        assert_eq!(decoded.field_str(), "order_id");
        assert_eq!(decoded.value, b"ORD-001");
        assert_eq!(decoded.idempotency_key_str(), "req-xyz");
    }

    #[test]
    fn unique_release_response_roundtrip() {
        let resp = UniqueReleaseResponse::create(777, false);

        let bytes = resp.to_be_bytes();
        let (decoded, _) = UniqueReleaseResponse::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.request_id, 777);
        assert!(!decoded.is_success());
    }
}
