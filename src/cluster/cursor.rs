use super::PartitionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCursor {
    pub partition: PartitionId,
    pub sequence: u64,
    pub last_key: Option<Vec<u8>>,
}

impl PartitionCursor {
    #[must_use]
    pub fn new(partition: PartitionId, sequence: u64, last_key: Option<Vec<u8>>) -> Self {
        Self {
            partition,
            sequence,
            last_key,
        }
    }

    #[must_use]
    pub fn initial(partition: PartitionId) -> Self {
        Self {
            partition,
            sequence: 0,
            last_key: None,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let key_len = self.last_key.as_ref().map_or(0, Vec::len);
        let mut buf = Vec::with_capacity(12 + key_len);

        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        buf.extend_from_slice(&(key_len as u16).to_be_bytes());
        if let Some(k) = &self.last_key {
            buf.extend_from_slice(k);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 12 {
            return None;
        }

        let partition = u16::from_be_bytes([bytes[0], bytes[1]]);
        let sequence = u64::from_be_bytes([
            bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9],
        ]);
        let key_len = u16::from_be_bytes([bytes[10], bytes[11]]) as usize;

        if bytes.len() < 12 + key_len {
            return None;
        }

        let last_key = if key_len > 0 {
            Some(bytes[12..12 + key_len].to_vec())
        } else {
            None
        };

        Some(Self {
            partition: PartitionId::new(partition)?,
            sequence,
            last_key,
        })
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        12 + self.last_key.as_ref().map_or(0, Vec::len)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScatterCursor {
    cursors: Vec<PartitionCursor>,
}

impl ScatterCursor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cursors: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cursors: Vec::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, cursor: PartitionCursor) {
        if let Some(existing) = self
            .cursors
            .iter_mut()
            .find(|c| c.partition == cursor.partition)
        {
            *existing = cursor;
        } else {
            self.cursors.push(cursor);
        }
    }

    #[must_use]
    pub fn get(&self, partition: PartitionId) -> Option<&PartitionCursor> {
        self.cursors.iter().find(|c| c.partition == partition)
    }

    pub fn update(&mut self, partition: PartitionId, sequence: u64, last_key: Option<Vec<u8>>) {
        if let Some(existing) = self
            .cursors
            .iter_mut()
            .find(|c| c.partition == partition)
        {
            existing.sequence = sequence;
            existing.last_key = last_key;
        } else {
            self.cursors
                .push(PartitionCursor::new(partition, sequence, last_key));
        }
    }

    pub fn partitions(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.cursors.iter().map(|c| c.partition)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode(&self) -> Vec<u8> {
        let total_size: usize = self.cursors.iter().map(PartitionCursor::byte_len).sum();
        let mut buf = Vec::with_capacity(2 + total_size);

        buf.extend_from_slice(&(self.cursors.len() as u16).to_be_bytes());
        for cursor in &self.cursors {
            buf.extend_from_slice(&cursor.to_bytes());
        }

        buf
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }

        let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        let mut offset = 2;
        let mut cursors = Vec::with_capacity(count);

        for _ in 0..count {
            if offset + 12 > bytes.len() {
                return None;
            }

            let key_len =
                u16::from_be_bytes([bytes[offset + 10], bytes[offset + 11]]) as usize;
            let cursor_len = 12 + key_len;

            if offset + cursor_len > bytes.len() {
                return None;
            }

            let cursor = PartitionCursor::from_bytes(&bytes[offset..offset + cursor_len])?;
            cursors.push(cursor);
            offset += cursor_len;
        }

        Some(Self { cursors })
    }

    pub fn iter(&self) -> impl Iterator<Item = &PartitionCursor> {
        self.cursors.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    #[test]
    fn partition_cursor_roundtrip() {
        let cursor = PartitionCursor::new(partition(17), 12345, Some(b"last-key".to_vec()));

        let bytes = cursor.to_bytes();
        let decoded = PartitionCursor::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition, partition(17));
        assert_eq!(decoded.sequence, 12345);
        assert_eq!(decoded.last_key, Some(b"last-key".to_vec()));
    }

    #[test]
    fn partition_cursor_no_key() {
        let cursor = PartitionCursor::initial(partition(5));

        let bytes = cursor.to_bytes();
        let decoded = PartitionCursor::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition, partition(5));
        assert_eq!(decoded.sequence, 0);
        assert!(decoded.last_key.is_none());
    }

    #[test]
    fn scatter_cursor_encode_decode() {
        let mut scatter = ScatterCursor::new();
        scatter.add(PartitionCursor::new(partition(10), 100, None));
        scatter.add(PartitionCursor::new(partition(20), 200, Some(b"key".to_vec())));
        scatter.add(PartitionCursor::new(partition(30), 300, None));

        let bytes = scatter.encode();
        let decoded = ScatterCursor::decode(&bytes).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.get(partition(10)).unwrap().sequence, 100);
        assert_eq!(decoded.get(partition(20)).unwrap().sequence, 200);
        assert_eq!(
            decoded.get(partition(20)).unwrap().last_key,
            Some(b"key".to_vec())
        );
        assert_eq!(decoded.get(partition(30)).unwrap().sequence, 300);
    }

    #[test]
    fn scatter_cursor_update() {
        let mut scatter = ScatterCursor::new();
        scatter.add(PartitionCursor::new(partition(5), 10, None));

        assert_eq!(scatter.get(partition(5)).unwrap().sequence, 10);

        scatter.update(partition(5), 20, Some(b"new".to_vec()));

        assert_eq!(scatter.get(partition(5)).unwrap().sequence, 20);
        assert_eq!(
            scatter.get(partition(5)).unwrap().last_key,
            Some(b"new".to_vec())
        );
        assert_eq!(scatter.len(), 1);
    }

    #[test]
    fn scatter_cursor_add_updates_existing() {
        let mut scatter = ScatterCursor::new();
        scatter.add(PartitionCursor::new(partition(5), 10, None));
        scatter.add(PartitionCursor::new(partition(5), 20, Some(b"new".to_vec())));

        assert_eq!(scatter.len(), 1);
        assert_eq!(scatter.get(partition(5)).unwrap().sequence, 20);
    }

    #[test]
    fn scatter_cursor_empty() {
        let scatter = ScatterCursor::new();
        assert!(scatter.is_empty());

        let bytes = scatter.encode();
        let decoded = ScatterCursor::decode(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn scatter_cursor_partitions_iterator() {
        let mut scatter = ScatterCursor::new();
        scatter.add(PartitionCursor::initial(partition(1)));
        scatter.add(PartitionCursor::initial(partition(2)));
        scatter.add(PartitionCursor::initial(partition(3)));

        let partitions: Vec<_> = scatter.partitions().collect();
        assert_eq!(partitions.len(), 3);
        assert!(partitions.contains(&partition(1)));
        assert!(partitions.contains(&partition(2)));
        assert!(partitions.contains(&partition(3)));
    }
}
