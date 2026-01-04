use super::NodeId;
use super::protocol::Operation;
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct ClientLocationEntry {
    pub version: u8,
    pub client_id_len: u16,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub connected_node: u16,
}

impl ClientLocationEntry {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(client_id: &str, connected_node: NodeId) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        Self {
            version: 1,
            client_id_len: client_bytes.len() as u16,
            client_id: client_bytes,
            connected_node: connected_node.get(),
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn node(&self) -> Option<NodeId> {
        NodeId::validated(self.connected_node)
    }
}

#[must_use]
pub fn client_location_key(client_id: &str) -> String {
    format!("_client_loc/{client_id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientLocationError {
    NotFound,
    SerializationError,
}

impl std::fmt::Display for ClientLocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "client location not found"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for ClientLocationError {}

pub struct ClientLocationStore {
    entries: RwLock<HashMap<String, NodeId>>,
}

impl ClientLocationStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn set(&self, client_id: &str, node: NodeId) {
        self.entries
            .write()
            .unwrap()
            .insert(client_id.to_string(), node);
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn remove(&self, client_id: &str) {
        self.entries.write().unwrap().remove(client_id);
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn get(&self, client_id: &str) -> Option<NodeId> {
        self.entries.read().unwrap().get(client_id).copied()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn count(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn apply_replicated(
        &self,
        operation: Operation,
        data: &[u8],
    ) -> Result<(), ClientLocationError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let (entry, _) = ClientLocationEntry::try_from_be_bytes(data)
                    .map_err(|_| ClientLocationError::SerializationError)?;
                if let Some(node) = entry.node() {
                    self.set(entry.client_id_str(), node);
                }
                Ok(())
            }
            Operation::Delete => {
                if !data.is_empty() {
                    let (entry, _) = ClientLocationEntry::try_from_be_bytes(data)
                        .map_err(|_| ClientLocationError::SerializationError)?;
                    self.remove(entry.client_id_str());
                }
                Ok(())
            }
        }
    }
}

impl Default for ClientLocationStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_location_roundtrip() {
        let entry = ClientLocationEntry::create("client123", NodeId::validated(5).unwrap());
        let bytes = entry.to_be_bytes();
        let (decoded, _) = ClientLocationEntry::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.client_id_str(), "client123");
        assert_eq!(decoded.connected_node, 5);
    }

    #[test]
    fn store_set_get_remove() {
        let store = ClientLocationStore::new();
        let node = NodeId::validated(3).unwrap();

        assert!(store.get("test").is_none());

        store.set("test", node);
        assert_eq!(store.get("test"), Some(node));

        store.remove("test");
        assert!(store.get("test").is_none());
    }
}
