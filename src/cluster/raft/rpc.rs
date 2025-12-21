use super::state::LogEntry;
use bebytes::BeBytes;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u16,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

impl RequestVoteRequest {
    pub fn create(term: u64, candidate_id: u16, last_log_index: u64, last_log_term: u64) -> Self {
        Self::new(term, candidate_id, last_log_index, last_log_term)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: u8,
}

impl RequestVoteResponse {
    pub fn granted(term: u64) -> Self {
        Self::new(term, 1)
    }

    pub fn rejected(term: u64) -> Self {
        Self::new(term, 0)
    }

    pub fn is_granted(&self) -> bool {
        self.vote_granted != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct AppendEntriesHeader {
    pub term: u64,
    pub leader_id: u16,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    pub entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u16,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

impl AppendEntriesRequest {
    pub fn create(
        term: u64,
        leader_id: u16,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    pub fn heartbeat(
        term: u64,
        leader_id: u16,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
    ) -> Self {
        Self::create(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            Vec::new(),
            leader_commit,
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let header = AppendEntriesHeader::new(
            self.term,
            self.leader_id,
            self.prev_log_index,
            self.prev_log_term,
            self.leader_commit,
            self.entries.len() as u32,
        );
        let mut buf = header.to_be_bytes();

        for entry in &self.entries {
            let entry_bytes = entry.to_be_bytes();
            buf.extend_from_slice(&(entry_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(&entry_bytes);
        }

        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (header, consumed) = AppendEntriesHeader::try_from_be_bytes(bytes).ok()?;

        let mut entries = Vec::with_capacity(header.entry_count as usize);
        let mut offset = consumed;

        for _ in 0..header.entry_count {
            if offset + 4 > bytes.len() {
                return None;
            }
            let entry_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + entry_len > bytes.len() {
                return None;
            }
            let (entry, _) =
                LogEntry::try_from_be_bytes(&bytes[offset..offset + entry_len]).ok()?;
            entries.push(entry);
            offset += entry_len;
        }

        Some(Self {
            term: header.term,
            leader_id: header.leader_id,
            prev_log_index: header.prev_log_index,
            prev_log_term: header.prev_log_term,
            entries,
            leader_commit: header.leader_commit,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: u8,
    pub match_index: u64,
}

impl AppendEntriesResponse {
    pub fn success(term: u64, match_index: u64) -> Self {
        Self::new(term, 1, match_index)
    }

    pub fn failure(term: u64) -> Self {
        Self::new(term, 0, 0)
    }

    pub fn is_success(&self) -> bool {
        self.success != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::raft::RaftCommand;

    #[test]
    fn request_vote_roundtrip() {
        let req = RequestVoteRequest::create(5, 2, 10, 3);
        let bytes = req.to_be_bytes();
        let (parsed, _) = RequestVoteRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn request_vote_response_roundtrip() {
        let resp = RequestVoteResponse::granted(5);
        let bytes = resp.to_be_bytes();
        let (parsed, _) = RequestVoteResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(resp, parsed);
        assert!(parsed.is_granted());
    }

    #[test]
    fn append_entries_empty_roundtrip() {
        let req = AppendEntriesRequest::heartbeat(5, 1, 10, 3, 8);
        let bytes = req.to_bytes();
        let parsed = AppendEntriesRequest::from_bytes(&bytes).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn append_entries_with_entries_roundtrip() {
        let entries = vec![
            LogEntry::create(11, 5, RaftCommand::Noop),
            LogEntry::create(12, 5, RaftCommand::AddNode { node_id: 4 }),
        ];
        let req = AppendEntriesRequest::create(5, 1, 10, 3, entries, 8);
        let bytes = req.to_bytes();
        let parsed = AppendEntriesRequest::from_bytes(&bytes).unwrap();
        assert_eq!(req.term, parsed.term);
        assert_eq!(req.entries.len(), parsed.entries.len());
    }

    #[test]
    fn append_entries_response_roundtrip() {
        let resp = AppendEntriesResponse::success(5, 12);
        let bytes = resp.to_be_bytes();
        let (parsed, _) = AppendEntriesResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(resp, parsed);
        assert!(parsed.is_success());
    }
}
