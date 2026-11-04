// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tracing::warn;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Entry {
    addr: SocketAddr,
    configured: bool,
}

pub(super) struct AddressBook {
    entries: RwLock<HashMap<NodeId, Entry>>,
    path: Option<PathBuf>,
}

impl AddressBook {
    pub(super) fn in_memory() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            path: None,
        }
    }

    pub(super) fn load(path: PathBuf) -> Self {
        let entries = match std::fs::read_to_string(&path) {
            Ok(text) => parse(&text),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => {
                warn!(path = %path.display(), %error, "ignoring unreadable peer address book");
                HashMap::new()
            }
        };
        Self {
            entries: RwLock::new(entries),
            path: Some(path),
        }
    }

    pub(super) async fn configure(&self, node: NodeId, addr: SocketAddr) {
        let entry = Entry {
            addr,
            configured: true,
        };
        let mut entries = self.entries.write().await;
        if entries.insert(node, entry).map(|old| old.addr) != Some(addr) {
            self.persist(&entries);
        }
    }

    pub(super) async fn learn(&self, node: NodeId, addr: SocketAddr) {
        let mut entries = self.entries.write().await;
        match entries.get(&node) {
            Some(known) if known.configured || known.addr == addr => {}
            _ => {
                entries.insert(
                    node,
                    Entry {
                        addr,
                        configured: false,
                    },
                );
                self.persist(&entries);
            }
        }
    }

    pub(super) async fn forget(&self, node: NodeId, addr: SocketAddr) {
        let mut entries = self.entries.write().await;
        if entries.get(&node).is_some_and(|entry| entry.addr == addr) {
            entries.remove(&node);
            self.persist(&entries);
        }
    }

    pub(super) async fn addr(&self, node: NodeId) -> Option<SocketAddr> {
        self.entries.read().await.get(&node).map(|entry| entry.addr)
    }

    pub(super) async fn nodes(&self) -> Vec<NodeId> {
        self.entries.read().await.keys().copied().collect()
    }

    fn persist(&self, entries: &HashMap<NodeId, Entry>) {
        let Some(path) = &self.path else {
            return;
        };
        if let Err(error) = write_atomically(path, &render(entries)) {
            warn!(path = %path.display(), %error, "failed to persist peer address book");
        }
    }
}

fn parse(text: &str) -> HashMap<NodeId, Entry> {
    text.lines()
        .filter_map(|line| {
            let (node, addr) = line.split_once(' ')?;
            let node = NodeId::validated(node.parse().ok()?)?;
            let addr = addr.parse().ok()?;
            Some((
                node,
                Entry {
                    addr,
                    configured: false,
                },
            ))
        })
        .collect()
}

fn render(entries: &HashMap<NodeId, Entry>) -> String {
    entries
        .iter()
        .map(|(node, entry)| format!("{} {}\n", node.get(), entry.addr))
        .collect()
}

fn write_atomically(path: &Path, contents: &str) -> std::io::Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, contents)?;
    std::fs::rename(&temporary, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[tokio::test]
    async fn learned_addresses_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_addrs");

        let book = AddressBook::load(path.clone());
        book.learn(node(2), addr(2000)).await;
        book.configure(node(3), addr(3000)).await;

        let reloaded = AddressBook::load(path);
        assert_eq!(reloaded.addr(node(2)).await, Some(addr(2000)));
        assert_eq!(reloaded.addr(node(3)).await, Some(addr(3000)));
    }

    #[tokio::test]
    async fn an_inbound_address_never_replaces_a_configured_one() {
        let book = AddressBook::in_memory();
        book.configure(node(2), addr(2000)).await;
        book.learn(node(2), addr(9999)).await;
        assert_eq!(book.addr(node(2)).await, Some(addr(2000)));
    }

    #[tokio::test]
    async fn an_inbound_address_replaces_a_stale_remembered_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_addrs");
        std::fs::write(&path, "2 127.0.0.1:2000\n").unwrap();

        let book = AddressBook::load(path.clone());
        book.learn(node(2), addr(2500)).await;

        assert_eq!(
            AddressBook::load(path).addr(node(2)).await,
            Some(addr(2500))
        );
    }

    #[tokio::test]
    async fn a_configured_address_overrides_a_remembered_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_addrs");
        std::fs::write(&path, "2 127.0.0.1:2000\n").unwrap();

        let book = AddressBook::load(path);
        book.configure(node(2), addr(2500)).await;

        assert_eq!(book.addr(node(2)).await, Some(addr(2500)));
    }

    #[tokio::test]
    async fn damaged_lines_are_skipped_and_a_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_addrs");
        assert!(AddressBook::load(path.clone()).nodes().await.is_empty());

        std::fs::write(
            &path,
            "junk\n0 127.0.0.1:1\n2 not-an-addr\n3 127.0.0.1:3000\n",
        )
        .unwrap();
        let book = AddressBook::load(path);
        assert_eq!(book.nodes().await, vec![node(3)]);
    }
}
