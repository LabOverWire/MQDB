use crate::cluster::protocol::{Operation, ReplicationWrite};
use crate::cluster::store_manager::{StoreApplyError, StoreManager};
use crate::cluster::{Epoch, NodeId, PartitionId, entity};

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn partition() -> PartitionId {
    PartitionId::ZERO
}

#[test]
fn apply_session_insert() {
    let manager = StoreManager::new(node(1));

    let session = crate::cluster::SessionData::create("client1", node(1));
    let data = crate::cluster::SessionStore::serialize(&session);

    let write = ReplicationWrite::new(
        partition(),
        Operation::Insert,
        Epoch::new(1),
        1,
        entity::SESSIONS.to_string(),
        "client1".to_string(),
        data,
    );

    manager.apply_write(&write).unwrap();
    assert!(manager.sessions.get("client1").is_some());
}

#[test]
fn apply_session_delete() {
    let manager = StoreManager::new(node(1));
    manager.sessions.create_session("client1").unwrap();

    let session = crate::cluster::SessionData::create("client1", node(1));
    let data = crate::cluster::SessionStore::serialize(&session);

    let write = ReplicationWrite::new(
        partition(),
        Operation::Delete,
        Epoch::new(1),
        1,
        entity::SESSIONS.to_string(),
        "client1".to_string(),
        data,
    );

    manager.apply_write(&write).unwrap();
    assert!(manager.sessions.get("client1").is_none());
}

#[test]
fn apply_unknown_entity_fails() {
    let manager = StoreManager::new(node(1));

    let write = ReplicationWrite::new(
        partition(),
        Operation::Insert,
        Epoch::new(1),
        1,
        "_unknown".to_string(),
        "id".to_string(),
        vec![],
    );

    let result = manager.apply_write(&write);
    assert!(matches!(result, Err(StoreApplyError::UnknownEntity)));
}
