// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::wire::WireReader;
use crate::cluster::PartitionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    pub epoch: u64,
    pub sequence: u64,
}

impl EventId {
    #[must_use]
    pub fn new(epoch: u64, sequence: u64) -> Self {
        Self { epoch, sequence }
    }

    fn push(self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.epoch.to_be_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
    }

    fn read(reader: &mut WireReader<'_>) -> Option<Self> {
        Some(Self::new(reader.u64()?, reader.u64()?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEventRecord {
    pub topic: String,
    pub payload: Vec<u8>,
    pub user_properties: Vec<(String, String)>,
    pub order_key: String,
}

fn push_u32_prefixed(buf: &mut Vec<u8>, field: &[u8]) -> Option<()> {
    buf.extend_from_slice(&u32::try_from(field.len()).ok()?.to_be_bytes());
    buf.extend_from_slice(field);
    Some(())
}

fn read_u32_prefixed<'a>(reader: &mut WireReader<'a>) -> Option<&'a [u8]> {
    let len = usize::try_from(reader.u32()?).ok()?;
    reader.take(len)
}

fn read_u32_text(reader: &mut WireReader<'_>) -> Option<String> {
    String::from_utf8(read_u32_prefixed(reader)?.to_vec()).ok()
}

impl ChangeEventRecord {
    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut buf =
            Vec::with_capacity(14 + self.topic.len() + self.payload.len() + self.order_key.len());
        self.push(&mut buf)?;
        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        let record = Self::read(&mut reader)?;
        reader.is_exhausted().then_some(record)
    }

    fn push(&self, buf: &mut Vec<u8>) -> Option<()> {
        push_u32_prefixed(buf, self.topic.as_bytes())?;
        push_u32_prefixed(buf, &self.payload)?;
        buf.extend_from_slice(
            &u16::try_from(self.user_properties.len())
                .ok()?
                .to_be_bytes(),
        );
        for (key, value) in &self.user_properties {
            push_u32_prefixed(buf, key.as_bytes())?;
            push_u32_prefixed(buf, value.as_bytes())?;
        }
        push_u32_prefixed(buf, self.order_key.as_bytes())
    }

    fn read(reader: &mut WireReader<'_>) -> Option<Self> {
        let topic = read_u32_text(reader)?;
        let payload = read_u32_prefixed(reader)?.to_vec();
        let property_count = reader.u16()?;
        let user_properties = (0..property_count)
            .map(|_| Some((read_u32_text(reader)?, read_u32_text(reader)?)))
            .collect::<Option<Vec<_>>>()?;
        let order_key = read_u32_text(reader)?;
        Some(Self {
            topic,
            payload,
            user_properties,
            order_key,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEventDelivery {
    pub partition: PartitionId,
    pub id: EventId,
    pub record: ChangeEventRecord,
}

impl ChangeEventDelivery {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::with_capacity(
            33 + self.record.topic.len() + self.record.payload.len() + self.record.order_key.len(),
        );
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        self.id.push(&mut buf);
        self.record.push(&mut buf)?;
        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        if reader.u8()? != Self::VERSION {
            return None;
        }
        let partition = PartitionId::new(reader.u16()?)?;
        let id = EventId::read(&mut reader)?;
        let record = ChangeEventRecord::read(&mut reader)?;
        reader.is_exhausted().then_some(Self {
            partition,
            id,
            record,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeEventMark {
    pub partition: PartitionId,
    pub id: EventId,
}

impl ChangeEventMark {
    pub const VERSION: u8 = 1;
    const SIZE: usize = 19;

    #[must_use]
    pub fn new(partition: PartitionId, id: EventId) -> Self {
        Self { partition, id }
    }

    #[must_use]
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        self.id.push(&mut buf);
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        if reader.u8()? != Self::VERSION {
            return None;
        }
        let partition = PartitionId::new(reader.u16()?)?;
        let id = EventId::read(&mut reader)?;
        reader.is_exhausted().then_some(Self { partition, id })
    }
}
