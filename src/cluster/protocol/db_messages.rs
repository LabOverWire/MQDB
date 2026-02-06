use super::types::JsonDbOp;

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
