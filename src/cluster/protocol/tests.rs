use super::*;
use crate::cluster::{Epoch, NodeId, PartitionId};
use bebytes::BeBytes;

#[test]
fn heartbeat_bitmap_operations() {
    let node = NodeId::validated(1).unwrap();
    let mut hb = Heartbeat::create(node, 1000);

    let p0 = PartitionId::ZERO;
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
    let partition = PartitionId::ZERO;
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
    let partition = PartitionId::ZERO;
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
    let req = UniqueCommitRequest::create(999, "users", "email", b"test@example.com", "req-abc");

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
