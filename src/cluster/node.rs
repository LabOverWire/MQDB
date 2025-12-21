use bebytes::BeBytes;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, BeBytes)]
pub struct NodeId {
    id: u16,
}

impl NodeId {
    pub const INVALID: Self = Self { id: 0 };

    #[must_use]
    pub fn validated(id: u16) -> Option<Self> {
        if id == 0 { None } else { Some(Self::new(id)) }
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.id
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.id != 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.id)
    }
}

impl From<NodeId> for u16 {
    fn from(node: NodeId) -> Self {
        node.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_zero_is_invalid() {
        assert!(NodeId::validated(0).is_none());
        assert!(!NodeId::INVALID.is_valid());
    }

    #[test]
    fn node_id_non_zero_is_valid() {
        assert!(NodeId::validated(1).is_some());
        assert!(NodeId::validated(1).unwrap().is_valid());
        assert!(NodeId::validated(65535).is_some());
    }

    #[test]
    fn node_id_ordering() {
        let n1 = NodeId::validated(1).unwrap();
        let n2 = NodeId::validated(2).unwrap();
        assert!(n1 < n2);
    }

    #[test]
    fn node_id_bebytes_roundtrip() {
        let node = NodeId::validated(42).unwrap();
        let bytes = node.to_be_bytes();
        let (decoded, _) = NodeId::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(node, decoded);
    }
}
