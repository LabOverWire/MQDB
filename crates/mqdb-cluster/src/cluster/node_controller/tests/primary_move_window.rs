use super::*;
use crate::cluster::db::data_partition;
use crate::cluster::{PartitionAssignment, PartitionMap};

const ENTITY: &str = "item";
const ID: &str = "n3-25";
const OLD: u16 = 3;
const NEW: u16 = 4;

fn node(id: u16) -> NodeId {
    NodeId::validated(id).unwrap()
}

fn controller(id: u16) -> NodeController<MockTransport> {
    create_test_controller(node(id), MockTransport::new(node(id)))
}

fn map_with(partition: PartitionId, primary: u16, replica: u16, epoch: u64) -> PartitionMap {
    let mut map = PartitionMap::default();
    map.set(
        partition,
        PartitionAssignment::new(node(primary), vec![node(replica)], Epoch::new(epoch)),
    );
    map
}

fn another_id_in(partition: PartitionId) -> String {
    (0u32..)
        .map(|n| format!("other-{n}"))
        .find(|id| id != ID && data_partition(ENTITY, id) == partition)
        .expect("some id hashes to the partition")
}

async fn deliver(
    from: &mut NodeController<MockTransport>,
    from_id: u16,
    to: &mut NodeController<MockTransport>,
    to_id: u16,
) -> usize {
    let messages: Vec<ClusterMessage> = from
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(target, message)| (target == node(to_id)).then_some(message))
        .collect();
    let count = messages.len();
    for message in messages {
        to.transport.inject_message(node(from_id), message);
    }
    Box::pin(to.process_messages()).await;
    count
}

async fn apply_moved_map(ctrl: &mut NodeController<MockTransport>, partition: PartitionId) {
    Box::pin(ctrl.apply_raft_partition_map(map_with(partition, NEW, OLD, 2))).await;
}

async fn create(ctrl: &mut NodeController<MockTransport>, id: &str, data: &[u8]) {
    Box::pin(ctrl.db_create(ENTITY, id, data, 1000))
        .await
        .unwrap();
}

async fn settle(old: &mut NodeController<MockTransport>, new: &mut NodeController<MockTransport>) {
    for _ in 0..32 {
        let to_new = deliver(old, OLD, new, NEW).await;
        let to_old = deliver(new, NEW, old, OLD).await;
        if to_new == 0 && to_old == 0 {
            return;
        }
    }
    panic!("the two nodes kept exchanging messages");
}

async fn pair_in_epoch_one(
    partition: PartitionId,
) -> (NodeController<MockTransport>, NodeController<MockTransport>) {
    let mut old = controller(OLD);
    old.become_primary(partition, Epoch::new(1));
    Box::pin(old.apply_raft_partition_map(map_with(partition, OLD, NEW, 1))).await;
    let mut new = controller(NEW);
    new.become_replica(partition, Epoch::new(1), 0);
    new.update_partition_map(map_with(partition, OLD, NEW, 1));
    old.transport.take_sent();
    (old, new)
}

async fn create_on_old_after_the_new_primary_switched(
    partition: PartitionId,
) -> (NodeController<MockTransport>, NodeController<MockTransport>) {
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    apply_moved_map(&mut new, partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    deliver(&mut old, OLD, &mut new, NEW).await;
    (old, new)
}

#[tokio::test]
async fn a_create_the_old_primary_acked_reaches_a_new_primary_that_switched_first() {
    let partition = data_partition(ENTITY, ID);
    let (mut old, mut new) = create_on_old_after_the_new_primary_switched(partition).await;

    deliver(&mut new, NEW, &mut old, OLD).await;
    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            old.stores().db_get(ENTITY, ID).is_some()
        ),
        (true, true),
        "new primary, old primary"
    );
}

#[tokio::test]
async fn a_refusal_that_arrives_after_the_old_primary_moved_still_forwards_the_create() {
    let partition = data_partition(ENTITY, ID);
    let (mut old, mut new) = create_on_old_after_the_new_primary_switched(partition).await;

    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            old.stores().db_get(ENTITY, ID).is_some()
        ),
        (true, true),
        "new primary, old primary"
    );
}

#[tokio::test]
async fn a_primary_does_not_apply_a_replication_write_from_a_newer_primary() {
    let partition = data_partition(ENTITY, ID);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    apply_moved_map(&mut new, partition).await;
    create(&mut new, ID, br#"{"n":2}"#).await;

    deliver(&mut new, NEW, &mut old, OLD).await;

    assert_eq!(
        (
            old.stores().db_get(ENTITY, ID).is_some(),
            old.epoch(partition),
            old.role(partition)
        ),
        (false, Some(Epoch::new(1)), ReplicaRole::Primary)
    );
}

#[tokio::test]
async fn both_primaries_writing_in_the_window_end_with_the_same_records() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    apply_moved_map(&mut new, partition).await;

    create(&mut old, ID, br#"{"n":1}"#).await;
    create(&mut new, &second, br#"{"n":2}"#).await;
    deliver(&mut old, OLD, &mut new, NEW).await;
    deliver(&mut new, NEW, &mut old, OLD).await;
    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    let holds = |ctrl: &NodeController<MockTransport>| {
        (
            ctrl.stores().db_get(ENTITY, ID).is_some(),
            ctrl.stores().db_get(ENTITY, &second).is_some(),
        )
    };
    assert_eq!(
        (holds(&new), holds(&old), old.role(partition)),
        ((true, true), (true, true), ReplicaRole::Replica),
        "new primary, old primary, old primary's role"
    );
}

#[tokio::test]
async fn a_demoted_primary_stores_the_new_primarys_first_write() {
    let partition = data_partition(ENTITY, ID);
    let (mut old, mut new) = create_on_old_after_the_new_primary_switched(partition).await;
    deliver(&mut new, NEW, &mut old, OLD).await;
    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    let second = another_id_in(partition);
    create(&mut new, &second, br#"{"n":2}"#).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, &second).is_some(),
            old.stores().db_get(ENTITY, &second).is_some()
        ),
        (true, true),
        "new primary, demoted primary"
    );
}

#[tokio::test]
async fn a_create_refused_after_the_new_primarys_write_replaced_it_in_the_log_survives() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    apply_moved_map(&mut new, partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    create(&mut new, &second, br#"{"n":2}"#).await;
    apply_moved_map(&mut old, partition).await;
    deliver(&mut new, NEW, &mut old, OLD).await;
    deliver(&mut old, OLD, &mut new, NEW).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            old.stores().db_get(ENTITY, ID).is_some()
        ),
        (true, true),
        "new primary, old primary"
    );
}

#[tokio::test]
async fn writes_a_replica_heals_by_catchup_are_no_longer_unanswered() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    old.transport.take_sent();
    create(&mut old, &second, br#"{"n":2}"#).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            old.holds_unanswered(partition)
        ),
        (true, false),
        "replica healed the gap, primary still waits for an answer"
    );
}

#[tokio::test]
async fn a_write_held_for_a_gap_when_the_replica_becomes_primary_survives() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    create(&mut old, &second, br#"{"n":2}"#).await;
    let mut writes: Vec<ClusterMessage> = old
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(target, message)| (target == node(NEW)).then_some(message))
        .collect();
    let later = writes.pop().expect("second write sent");
    let earlier = writes.pop().expect("first write sent");

    new.transport.inject_message(node(OLD), later);
    Box::pin(new.process_messages()).await;
    deliver(&mut new, NEW, &mut old, OLD).await;
    apply_moved_map(&mut new, partition).await;
    new.transport.inject_message(node(OLD), earlier);
    Box::pin(new.process_messages()).await;
    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            new.stores().db_get(ENTITY, &second).is_some()
        ),
        (true, true),
        "new primary holds the first write, the write it held for the gap"
    );
}

#[tokio::test]
async fn a_held_write_survives_when_the_old_primary_moves_before_the_gap_ack() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    create(&mut old, &second, br#"{"n":2}"#).await;
    let mut writes: Vec<ClusterMessage> = old
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(target, message)| (target == node(NEW)).then_some(message))
        .collect();
    let later = writes.pop().expect("second write sent");
    let earlier = writes.pop().expect("first write sent");

    new.transport.inject_message(node(OLD), later);
    Box::pin(new.process_messages()).await;
    let gap_ack = new.transport.take_sent();
    apply_moved_map(&mut new, partition).await;
    apply_moved_map(&mut old, partition).await;
    for (target, message) in gap_ack {
        if target == node(OLD) {
            old.transport.inject_message(node(NEW), message);
        }
    }
    Box::pin(old.process_messages()).await;
    new.transport.inject_message(node(OLD), earlier);
    Box::pin(new.process_messages()).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            new.stores().db_get(ENTITY, &second).is_some()
        ),
        (true, true),
        "new primary holds the first write, the write it held for the gap"
    );
}

#[tokio::test]
async fn a_dropped_write_survives_when_its_catch_up_reaches_the_new_primary() {
    let partition = data_partition(ENTITY, ID);
    let second = another_id_in(partition);
    let (mut old, mut new) = pair_in_epoch_one(partition).await;
    create(&mut old, ID, br#"{"n":1}"#).await;
    create(&mut old, &second, br#"{"n":2}"#).await;
    let later = old
        .transport
        .take_sent()
        .into_iter()
        .filter_map(|(target, message)| (target == node(NEW)).then_some(message))
        .next_back()
        .expect("second write sent");

    new.transport.inject_message(node(OLD), later);
    Box::pin(new.process_messages()).await;
    deliver(&mut new, NEW, &mut old, OLD).await;
    apply_moved_map(&mut new, partition).await;
    apply_moved_map(&mut old, partition).await;
    settle(&mut old, &mut new).await;

    assert_eq!(
        (
            new.stores().db_get(ENTITY, ID).is_some(),
            new.stores().db_get(ENTITY, &second).is_some()
        ),
        (true, true),
        "new primary holds the dropped write, the write it held for the gap"
    );
}
