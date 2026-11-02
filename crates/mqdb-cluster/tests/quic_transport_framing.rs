use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqdb_cluster::{
    ClusterMessage, ClusterTransport, ForwardedPublish, Heartbeat, NodeId, PartitionId,
    QuicDirectTransport, SnapshotChunk, TransportError,
};
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::oneshot;

const STALL_WINDOW: u32 = 16 * 1024;
const SEND_ATTEMPTS: u32 = 8192;

struct Certs {
    ca_pem: String,
    leaf_pem: String,
    leaf_key_pem: String,
    leaf_der: CertificateDer<'static>,
    leaf_key_der: Vec<u8>,
}

fn generate_certs() -> Certs {
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();

    let leaf_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let leaf_key = KeyPair::generate().unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    Certs {
        ca_pem,
        leaf_pem: leaf_cert.pem(),
        leaf_key_pem: leaf_key.serialize_pem(),
        leaf_der: leaf_cert.der().clone(),
        leaf_key_der: leaf_key.serialize_der(),
    }
}

fn server_endpoint(certs: &Certs, stream_window: u32) -> Endpoint {
    let key = PrivateKeyDer::try_from(certs.leaf_key_der.clone()).unwrap();
    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certs.leaf_der.clone()], key)
        .unwrap();

    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
    ));

    let mut transport = TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(stream_window));
    server_config.transport_config(Arc::new(transport));

    Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap()
}

async fn far_end(
    endpoint: Endpoint,
    release: oneshot::Receiver<()>,
    frame_tx: flume::Sender<Vec<u8>>,
) {
    let connection: Connection = endpoint.accept().await.unwrap().await.unwrap();
    let (send_stream, mut recv): (SendStream, RecvStream) = connection.accept_bi().await.unwrap();

    let mut header = [0u8; 2];
    recv.read_exact(&mut header).await.unwrap();

    release.await.ok();

    loop {
        let mut len_buf = [0u8; 4];
        if recv.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        if recv.read_exact(&mut payload).await.is_err() {
            break;
        }
        if frame_tx.send(payload).is_err() {
            break;
        }
    }

    drop(send_stream);
    drop(connection);
}

async fn collect_frame(frame_rx: &flume::Receiver<Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(10), frame_rx.recv_async())
        .await
        .expect("timed out waiting for a framed message")
        .expect("far-end frame channel closed")
}

struct StalledPeer {
    transport: QuicDirectTransport,
    local: NodeId,
    peer: NodeId,
    frame_rx: flume::Receiver<Vec<u8>>,
    release_tx: oneshot::Sender<()>,
    far_handle: tokio::task::JoinHandle<()>,
}

async fn setup_stalled_peer() -> StalledPeer {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, STALL_WINDOW);
    let far_addr: SocketAddr = endpoint.local_addr().unwrap();

    let (release_tx, release_rx) = oneshot::channel();
    let (frame_tx, frame_rx) = flume::unbounded();
    let far_handle = tokio::spawn(far_end(endpoint, release_rx, frame_tx));

    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let transport = QuicDirectTransport::new(local);
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();

    StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    }
}

fn heartbeat(local: NodeId, tick: u64) -> ClusterMessage {
    ClusterMessage::Heartbeat(Heartbeat::create(local, tick))
}

fn bulk_message(local: NodeId) -> ClusterMessage {
    ClusterMessage::ForwardedPublish(ForwardedPublish::new(
        local,
        "bulk".to_string(),
        0,
        false,
        vec![0u8; 64],
        Vec::new(),
    ))
}

#[tokio::test]
async fn backpressure_drops_messages_but_keeps_the_stream_framed() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let mut ok_count = 0u32;
    let mut terminated_by_drop = false;
    let mut blocked = false;
    for tick in 0..SEND_ATTEMPTS {
        match tokio::time::timeout(
            Duration::from_millis(500),
            transport.send(peer, heartbeat(local, u64::from(tick))),
        )
        .await
        {
            Err(_) => {
                blocked = true;
                break;
            }
            Ok(Ok(())) => ok_count += 1,
            Ok(Err(TransportError::SendQueueFull(_))) => {
                terminated_by_drop = true;
                break;
            }
            Ok(Err(other)) => panic!("unexpected send error: {other}"),
        }
    }

    assert!(
        !blocked,
        "a send blocked under backpressure instead of returning immediately"
    );
    assert!(
        terminated_by_drop,
        "expected SendQueueFull once the per-peer queue filled"
    );
    assert!(
        ok_count > 0,
        "expected some sends to be accepted before the queue filled"
    );

    release_tx.send(()).ok();

    for _ in 0..ok_count {
        let frame = collect_frame(&frame_rx).await;
        assert!(
            frame.len() >= 3,
            "frame shorter than a cluster message header"
        );
        assert_eq!(
            &frame[0..2],
            &local.get().to_be_bytes(),
            "frame did not begin with the sender node id (stream mis-framed)"
        );
    }

    transport
        .send(peer, heartbeat(local, u64::MAX))
        .await
        .unwrap();
    let frame = collect_frame(&frame_rx).await;
    assert!(frame.len() >= 3);
    assert_eq!(
        &frame[0..2],
        &local.get().to_be_bytes(),
        "post-backpressure frame mis-framed"
    );

    far_handle.abort();
}

#[tokio::test]
async fn control_plane_survives_a_full_bulk_queue() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let mut bulk_ok = 0u32;
    let mut bulk_dropped = false;
    for _ in 0..SEND_ATTEMPTS {
        match tokio::time::timeout(
            Duration::from_millis(500),
            transport.send(peer, bulk_message(local)),
        )
        .await
        {
            Err(_) => panic!("bulk send blocked under backpressure"),
            Ok(Ok(())) => bulk_ok += 1,
            Ok(Err(TransportError::SendQueueFull(_))) => {
                bulk_dropped = true;
                break;
            }
            Ok(Err(other)) => panic!("unexpected bulk send error: {other}"),
        }
    }
    assert!(bulk_dropped, "bulk lane should saturate and start dropping");

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        transport.send(peer, heartbeat(local, 7)),
    )
    .await
    .expect("control-plane send blocked under backpressure");
    assert!(
        matches!(result, Ok(())),
        "control-plane heartbeat was dropped while the bulk lane was full: {result:?}"
    );

    release_tx.send(()).ok();

    let heartbeat_type = heartbeat(local, 0).message_type();
    let mut heartbeat_delivered = false;
    for _ in 0..bulk_ok + 2 {
        let frame = collect_frame(&frame_rx).await;
        assert!(
            frame.len() >= 3,
            "frame shorter than a cluster message header"
        );
        assert_eq!(
            &frame[0..2],
            &local.get().to_be_bytes(),
            "stream mis-framed under the lane writer"
        );
        if frame[2] == heartbeat_type {
            heartbeat_delivered = true;
            break;
        }
    }
    assert!(
        heartbeat_delivered,
        "control-plane heartbeat was accepted but never delivered"
    );

    far_handle.abort();
}

fn snapshot_chunk(index: u32) -> ClusterMessage {
    ClusterMessage::SnapshotChunk(
        SnapshotChunk::new(PartitionId::ZERO, index, u32::MAX, 0, vec![0u8; 1024])
            .with_transfer_id(1),
    )
}

#[tokio::test]
async fn bulk_messages_survive_a_full_snapshot_lane() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let mut chunks_dropped = false;
    for index in 0..SEND_ATTEMPTS {
        match tokio::time::timeout(
            Duration::from_millis(500),
            transport.send(peer, snapshot_chunk(index)),
        )
        .await
        {
            Err(_) => panic!("snapshot chunk send blocked under backpressure"),
            Ok(Ok(())) => {}
            Ok(Err(TransportError::SendQueueFull(_))) => {
                chunks_dropped = true;
                break;
            }
            Ok(Err(other)) => panic!("unexpected snapshot send error: {other}"),
        }
    }
    assert!(
        chunks_dropped,
        "snapshot chunks should eventually fill their lane"
    );

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        transport.send(peer, bulk_message(local)),
    )
    .await
    .expect("bulk send blocked under backpressure");
    assert!(
        matches!(result, Ok(())),
        "a bulk message was refused while snapshot chunks filled the queue: {result:?}"
    );

    release_tx.send(()).ok();

    let bulk_type = bulk_message(local).message_type();
    let mut bulk_delivered = false;
    for _ in 0..SEND_ATTEMPTS {
        let frame = collect_frame(&frame_rx).await;
        assert_eq!(
            &frame[0..2],
            &local.get().to_be_bytes(),
            "stream mis-framed"
        );
        if frame[2] == bulk_type {
            bulk_delivered = true;
            break;
        }
    }
    assert!(
        bulk_delivered,
        "the bulk message was accepted but never delivered"
    );

    far_handle.abort();
}

async fn reconnecting_far_end(
    endpoint: Endpoint,
    close_first: oneshot::Receiver<()>,
    accepted_tx: flume::Sender<()>,
) {
    let conn1: Connection = endpoint.accept().await.unwrap().await.unwrap();
    let (s1, mut r1): (SendStream, RecvStream) = conn1.accept_bi().await.unwrap();
    let mut header = [0u8; 2];
    r1.read_exact(&mut header).await.unwrap();
    accepted_tx.send(()).ok();

    close_first.await.ok();
    conn1.close(0u32.into(), b"test-close");
    drop(s1);
    drop(r1);
    drop(conn1);

    let conn2: Connection = endpoint.accept().await.unwrap().await.unwrap();
    let (s2, mut r2): (SendStream, RecvStream) = conn2.accept_bi().await.unwrap();
    let mut header2 = [0u8; 2];
    r2.read_exact(&mut header2).await.unwrap();
    accepted_tx.send(()).ok();

    std::future::pending::<()>().await;
    drop(s2);
    drop(conn2);
}

#[tokio::test]
async fn dead_peer_is_removed_and_redialled() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, 1024 * 1024);
    let far_addr: SocketAddr = endpoint.local_addr().unwrap();

    let (close_tx, close_rx) = oneshot::channel();
    let (accepted_tx, accepted_rx) = flume::unbounded();
    let far_handle = tokio::spawn(reconnecting_far_end(endpoint, close_rx, accepted_tx));

    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let transport = QuicDirectTransport::new(local);
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();

    tokio::time::timeout(Duration::from_secs(10), accepted_rx.recv_async())
        .await
        .expect("far end did not accept the initial connection")
        .unwrap();
    assert!(
        transport.direct_peers().await.unwrap().contains(&peer),
        "peer should be linked after the initial connect"
    );

    close_tx.send(()).ok();
    let mut removed = false;
    for _ in 0..200 {
        transport.send(peer, heartbeat(local, 1)).await.ok();
        if !transport.direct_peers().await.unwrap().contains(&peer) {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        removed,
        "a send-side failure should remove the dead peer from the map"
    );

    transport.redial_unlinked(&[]).await;
    tokio::time::timeout(Duration::from_secs(10), accepted_rx.recv_async())
        .await
        .expect("far end did not accept the redial")
        .unwrap();
    assert!(
        transport.direct_peers().await.unwrap().contains(&peer),
        "peer should be re-linked after redial_unlinked"
    );

    far_handle.abort();
}

async fn stream_stopping_far_end(endpoint: Endpoint, closed_tx: oneshot::Sender<()>) {
    let connection: Connection = endpoint.accept().await.unwrap().await.unwrap();
    let (send_stream, mut recv): (SendStream, RecvStream) = connection.accept_bi().await.unwrap();
    let mut header = [0u8; 2];
    recv.read_exact(&mut header).await.unwrap();
    recv.stop(0u32.into()).unwrap();
    connection.closed().await;
    closed_tx.send(()).ok();
    drop(send_stream);
}

#[tokio::test]
async fn a_failed_writer_closes_the_peer_connection() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, 1024 * 1024);
    let far_addr: SocketAddr = endpoint.local_addr().unwrap();
    let (closed_tx, closed_rx) = oneshot::channel();
    let far_handle = tokio::spawn(stream_stopping_far_end(endpoint, closed_tx));

    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let transport = QuicDirectTransport::new(local);
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();

    let mut removed = false;
    for tick in 0..200 {
        transport.send(peer, heartbeat(local, tick)).await.ok();
        if !transport.direct_peers().await.unwrap().contains(&peer) {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(removed, "a stopped stream should remove the peer");

    tokio::time::timeout(Duration::from_secs(10), closed_rx)
        .await
        .expect("the connection behind a failed writer was left open")
        .unwrap();

    far_handle.abort();
}

#[tokio::test]
async fn a_snapshot_chunk_is_not_held_behind_a_full_bulk_lane() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let mut bulk_full = false;
    let mut bulk_accepted = 0;
    for _ in 0..SEND_ATTEMPTS {
        match transport.send(peer, bulk_message(local)).await {
            Ok(()) => bulk_accepted += 1,
            Err(TransportError::SendQueueFull(_)) => {
                bulk_full = true;
                break;
            }
            Err(other) => panic!("unexpected bulk send error: {other}"),
        }
    }
    assert!(bulk_full, "bulk messages should eventually fill their lane");
    transport
        .send(peer, snapshot_chunk(1))
        .await
        .expect("snapshot chunk refused while only the bulk lane was full");

    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let mut frames_before = 0;
    loop {
        let frame = collect_frame(&frame_rx).await;
        if frame[2] == snapshot_type {
            break;
        }
        frames_before += 1;
    }
    assert!(
        frames_before < bulk_accepted,
        "the snapshot chunk waited behind all {bulk_accepted} queued bulk frames"
    );

    far_handle.abort();
}

#[tokio::test]
async fn bulk_gets_a_byte_share_while_snapshot_chunks_are_queued() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    for index in 0..SEND_ATTEMPTS {
        if transport.send(peer, snapshot_chunk(index)).await.is_err() {
            break;
        }
    }
    for _ in 0..SEND_ATTEMPTS {
        if transport.send(peer, bulk_message(local)).await.is_err() {
            break;
        }
    }
    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let mut frame = collect_frame(&frame_rx).await;
    while frame[2] == snapshot_type {
        frame = collect_frame(&frame_rx).await;
    }
    while frame[2] != snapshot_type {
        frame = collect_frame(&frame_rx).await;
    }
    let chunk_len = frame.len();
    let mut bulk_len = 0;
    frame = collect_frame(&frame_rx).await;
    while frame[2] != snapshot_type {
        bulk_len += frame.len();
        frame = collect_frame(&frame_rx).await;
    }
    assert!(
        bulk_len * 2 >= chunk_len,
        "only {bulk_len} bulk bytes went out between chunks of {chunk_len} bytes"
    );

    far_handle.abort();
}

#[tokio::test]
async fn a_frame_over_the_receive_limit_is_refused_at_send() {
    let StalledPeer {
        transport,
        local,
        peer,
        far_handle,
        ..
    } = setup_stalled_peer().await;

    let oversized = ClusterMessage::ForwardedPublish(ForwardedPublish::new(
        local,
        "oversized".to_string(),
        0,
        false,
        vec![0u8; 10 * 1024 * 1024 + 1],
        Vec::new(),
    ));
    let result = transport.send(peer, oversized).await;
    assert!(
        matches!(result, Err(TransportError::SendFailed(_))),
        "a frame the receiver would reject was accepted: {result:?}"
    );

    far_handle.abort();
}

#[tokio::test]
async fn a_first_chunk_is_written_after_the_bulk_frames_queued_before_it() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let queued_before = 300;
    for _ in 0..queued_before {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    transport.send(peer, snapshot_chunk(0)).await.unwrap();
    transport.send(peer, snapshot_chunk(1)).await.unwrap();
    for _ in 0..50 {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let mut bulk_before_first_chunk = 0;
    while collect_frame(&frame_rx).await[2] != snapshot_type {
        bulk_before_first_chunk += 1;
    }
    assert!(
        bulk_before_first_chunk >= queued_before,
        "the first chunk overtook bulk frames: only {bulk_before_first_chunk} of {queued_before} went first"
    );

    let mut bulk_before_second_chunk = 0;
    while collect_frame(&frame_rx).await[2] != snapshot_type {
        bulk_before_second_chunk += 1;
    }
    assert!(
        bulk_before_first_chunk + bulk_before_second_chunk < queued_before + 50,
        "the second chunk waited behind every later bulk frame"
    );

    far_handle.abort();
}

fn snapshot_chunk_of(transfer: u8, index: u32) -> ClusterMessage {
    ClusterMessage::SnapshotChunk(
        SnapshotChunk::new(PartitionId::ZERO, index, u32::MAX, 0, vec![transfer; 1024])
            .with_transfer_id(u64::from(transfer)),
    )
}

#[tokio::test]
async fn a_first_chunk_after_a_full_lane_is_not_held_by_refused_frames() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    let mut accepted = 0;
    let mut refused = 0;
    while refused <= 20 {
        match transport.send(peer, bulk_message(local)).await {
            Ok(()) => accepted += 1,
            Err(TransportError::SendQueueFull(_)) => refused += 1,
            Err(other) => panic!("unexpected bulk send error: {other}"),
        }
    }
    transport.send(peer, snapshot_chunk(0)).await.unwrap();
    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let mut bulk_before_chunk = 0;
    while collect_frame(&frame_rx).await[2] != snapshot_type {
        bulk_before_chunk += 1;
    }
    assert_eq!(bulk_before_chunk, accepted);

    far_handle.abort();
}

#[tokio::test]
async fn a_first_chunk_is_positioned_exactly_and_control_frames_do_not_count_as_bulk() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    for _ in 0..300 {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    transport.send(peer, snapshot_chunk_of(1, 0)).await.unwrap();
    transport.send(peer, snapshot_chunk_of(1, 1)).await.unwrap();
    for _ in 0..100 {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    transport.send(peer, snapshot_chunk_of(2, 0)).await.unwrap();
    for _ in 0..50 {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    transport.send(peer, heartbeat(local, 7)).await.unwrap();
    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let heartbeat_type = heartbeat(local, 0).message_type();
    let mut bulk = 0;
    let mut chunk_positions = Vec::new();
    let mut heartbeat_seen = false;
    while bulk < 450 || chunk_positions.len() < 3 {
        let frame = collect_frame(&frame_rx).await;
        if frame[2] == snapshot_type {
            chunk_positions.push(bulk);
        } else if frame[2] == heartbeat_type {
            heartbeat_seen = true;
        } else {
            bulk += 1;
        }
    }
    assert_eq!(chunk_positions[0], 300);
    assert!(chunk_positions[1] >= 300);
    assert!(
        chunk_positions[2] >= 400,
        "the second transfer overtook bulk queued before it: {chunk_positions:?}"
    );
    assert!(heartbeat_seen);

    far_handle.abort();
}

#[tokio::test]
async fn a_first_chunk_is_written_when_no_bulk_follows_it() {
    let StalledPeer {
        transport,
        local,
        peer,
        frame_rx,
        release_tx,
        far_handle,
    } = setup_stalled_peer().await;

    for _ in 0..5 {
        transport.send(peer, bulk_message(local)).await.unwrap();
    }
    transport.send(peer, snapshot_chunk(0)).await.unwrap();
    release_tx.send(()).ok();

    let snapshot_type = snapshot_chunk(0).message_type();
    let mut bulk_before_chunk = 0;
    while collect_frame(&frame_rx).await[2] != snapshot_type {
        bulk_before_chunk += 1;
    }
    assert_eq!(bulk_before_chunk, 5);

    far_handle.abort();
}

#[tokio::test]
async fn a_first_chunk_with_no_bulk_ever_queued_is_written() {
    let StalledPeer {
        transport,
        peer,
        frame_rx,
        release_tx,
        far_handle,
        ..
    } = setup_stalled_peer().await;

    transport.send(peer, snapshot_chunk(0)).await.unwrap();
    release_tx.send(()).ok();

    assert_eq!(
        collect_frame(&frame_rx).await[2],
        snapshot_chunk(0).message_type()
    );

    far_handle.abort();
}

async fn linked_transport(
    local: NodeId,
    peer: NodeId,
) -> (
    QuicDirectTransport,
    flume::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, 1024 * 1024);
    let far_addr: SocketAddr = endpoint.local_addr().unwrap();
    let (close_tx, close_rx) = oneshot::channel();
    let (accepted_tx, accepted_rx) = flume::unbounded();
    let far_handle = tokio::spawn(reconnecting_far_end(endpoint, close_rx, accepted_tx));

    let transport = QuicDirectTransport::new(local);
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), accepted_rx.recv_async())
        .await
        .expect("far end did not accept the initial connection")
        .unwrap();
    (transport, accepted_rx, close_tx, far_handle, dir)
}

#[tokio::test]
async fn a_fresh_link_is_not_redialled_until_it_has_had_time_to_prove_itself() {
    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let (transport, _accepted, _close, far_handle, _dir) = linked_transport(local, peer).await;

    assert!(transport.peers_needing_dial(&[]).await.is_empty());
    assert!(
        transport.peers_needing_dial(&[peer]).await.is_empty(),
        "a peer just back has not had time to send its first heartbeat"
    );

    tokio::time::sleep(Duration::from_millis(5_100)).await;
    assert!(
        transport.peers_needing_dial(&[]).await.is_empty(),
        "a link to a peer not declared dead is left alone"
    );
    assert_eq!(
        transport.peers_needing_dial(&[peer]).await,
        [peer],
        "an old link to a dead peer is stale"
    );
    far_handle.abort();
}

#[tokio::test]
async fn a_lost_link_needs_a_dial_before_failure_detection() {
    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let (transport, accepted, close, far_handle, _dir) = linked_transport(local, peer).await;

    close.send(()).ok();
    let mut removed = false;
    for _ in 0..200 {
        let _ = transport.send(peer, heartbeat(local, 1)).await;
        if !transport.direct_peers().await.unwrap().contains(&peer) {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(removed, "a send-side failure should remove the dead peer");
    assert_eq!(transport.peers_needing_dial(&[]).await, [peer]);

    transport.redial(&[peer]).await;
    tokio::time::timeout(Duration::from_secs(10), accepted.recv_async())
        .await
        .expect("far end did not accept the redial")
        .unwrap();
    assert!(transport.direct_peers().await.unwrap().contains(&peer));
    far_handle.abort();
}

#[tokio::test]
async fn a_redial_skips_a_peer_that_is_already_being_dialled() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let silent_peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let silent_addr = silent_peer.local_addr().unwrap();
    let book = dir.path().join("peer_addrs");
    std::fs::write(&book, format!("2 {silent_addr}\n")).unwrap();

    let peer = NodeId::validated(2).unwrap();
    let transport =
        QuicDirectTransport::remembering_addresses_in(NodeId::validated(1).unwrap(), book);
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();

    let first = transport.clone();
    let first_dial = tokio::spawn(async move { first.redial(&[peer]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = std::time::Instant::now();
    transport.redial(&[peer]).await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the second redial must return at once, not wait out its own dial timeout"
    );
    first_dial.await.unwrap();
}

fn free_udp_addr() -> SocketAddr {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test]
async fn a_node_restarted_without_peers_dials_the_peers_that_once_dialled_it() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();
    let book = dir.path().join("peer_addrs");

    let seed_id = NodeId::validated(1).unwrap();
    let joiner_id = NodeId::validated(2).unwrap();
    let seed_addr = free_udp_addr();
    let joiner_addr = free_udp_addr();

    let seed = QuicDirectTransport::remembering_addresses_in(seed_id, book.clone());
    seed.set_ca_file(ca_path.clone());
    seed.set_cert_key_files(leaf_path.clone(), leaf_key_path.clone());
    seed.bind(seed_addr, &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    assert!(seed.peers_needing_dial(&[]).await.is_empty());

    let joiner = QuicDirectTransport::new(joiner_id);
    joiner.set_ca_file(ca_path.clone());
    joiner.set_cert_key_files(leaf_path.clone(), leaf_key_path.clone());
    joiner
        .bind(joiner_addr, &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    joiner.connect_to_peer(seed_id, seed_addr).await.unwrap();

    for _ in 0..200 {
        if seed.direct_peers().await.unwrap().contains(&joiner_id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for _ in 0..200 {
        if std::fs::read_to_string(&book).is_ok_and(|text| !text.is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let restarted = QuicDirectTransport::remembering_addresses_in(seed_id, book);
    restarted.set_ca_file(ca_path);
    restarted.set_cert_key_files(leaf_path.clone(), leaf_key_path.clone());
    restarted
        .bind(free_udp_addr(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    assert_eq!(
        restarted.peers_needing_dial(&[]).await,
        [joiner_id],
        "the restarted node knows the joiner it never had in --peers"
    );

    restarted.redial(&[joiner_id]).await;
    assert!(
        restarted.direct_peers().await.unwrap().contains(&joiner_id),
        "the remembered address is the joiner's listening address"
    );
}

async fn stream_counting_far_end(endpoint: Endpoint, streams_tx: flume::Sender<()>) {
    let mut connections = Vec::new();
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connection) = incoming.await else {
            continue;
        };
        connections.push(connection.clone());
        let streams_tx = streams_tx.clone();
        tokio::spawn(async move {
            if let Ok((send, mut recv)) = connection.accept_bi().await {
                let mut header = [0u8; 2];
                if recv.read_exact(&mut header).await.is_ok() {
                    streams_tx.send(()).ok();
                }
                std::future::pending::<()>().await;
                drop((send, recv));
            }
        });
    }
}

#[tokio::test]
async fn a_redial_that_finishes_after_the_peer_linked_first_keeps_the_new_link() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, 1024 * 1024);
    let far_addr = endpoint.local_addr().unwrap();
    let (streams_tx, streams_rx) = flume::unbounded();
    let far_handle = tokio::spawn(stream_counting_far_end(endpoint, streams_tx));

    let peer = NodeId::validated(2).unwrap();
    let transport = QuicDirectTransport::new(NodeId::validated(1).unwrap());
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), streams_rx.recv_async())
        .await
        .expect("far end did not see the first stream")
        .unwrap();

    transport.redial(&[peer]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        streams_rx.is_empty(),
        "a redial must not open a stream that replaces a link younger than the settle time"
    );
    far_handle.abort();
}

#[tokio::test]
async fn dials_reuse_the_client_tls_config_loaded_by_the_first_one() {
    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let (transport, accepted, close, far_handle, dir) = linked_transport(local, peer).await;
    std::fs::remove_file(dir.path().join("ca.pem")).unwrap();

    close.send(()).ok();
    for _ in 0..200 {
        let _ = transport.send(peer, heartbeat(local, 1)).await;
        if !transport.direct_peers().await.unwrap().contains(&peer) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    transport.redial(&[peer]).await;

    tokio::time::timeout(Duration::from_secs(10), accepted.recv_async())
        .await
        .expect("the redial must not need the CA file again")
        .unwrap();
    far_handle.abort();
}

#[tokio::test]
async fn connecting_configured_peers_remembers_them_and_reports_each_failure() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = server_endpoint(&certs, 1024 * 1024);
    let live_addr = endpoint.local_addr().unwrap();
    let (streams_tx, streams_rx) = flume::unbounded();
    let far_handle = tokio::spawn(stream_counting_far_end(endpoint, streams_tx));
    let silent_peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let silent_addr = silent_peer.local_addr().unwrap();

    let live = NodeId::validated(2).unwrap();
    let silent = NodeId::validated(3).unwrap();
    let transport = QuicDirectTransport::new(NodeId::validated(1).unwrap());
    transport.set_ca_file(ca_path);
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();

    let failures = transport
        .connect_configured_peers(&[(live, live_addr), (silent, silent_addr)])
        .await;

    assert_eq!(
        failures.iter().map(|(node, _)| *node).collect::<Vec<_>>(),
        [silent]
    );
    tokio::time::timeout(Duration::from_secs(10), streams_rx.recv_async())
        .await
        .expect("the live peer must be dialled")
        .unwrap();
    assert_eq!(transport.peers_needing_dial(&[]).await, [silent]);
    far_handle.abort();
}

#[tokio::test]
async fn a_remembered_address_that_answers_as_another_node_is_forgotten() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let new_id = NodeId::validated(6).unwrap();
    let old_id = NodeId::validated(4).unwrap();
    let local_id = NodeId::validated(1).unwrap();
    let new_addr = free_udp_addr();
    let new_node = QuicDirectTransport::new(new_id);
    new_node.set_ca_file(ca_path.clone());
    new_node
        .bind(new_addr, &leaf_path, &leaf_key_path)
        .await
        .unwrap();

    let book = dir.path().join("peer_addrs");
    std::fs::write(&book, format!("4 {new_addr}\n")).unwrap();
    let local = QuicDirectTransport::remembering_addresses_in(local_id, book.clone());
    local.set_ca_file(ca_path);
    local.set_cert_key_files(leaf_path.clone(), leaf_key_path.clone());
    local
        .bind(free_udp_addr(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    local.redial(&[old_id]).await;
    assert!(local.direct_peers().await.unwrap().contains(&old_id));

    for _ in 0..200 {
        let _ = new_node.send(local_id, heartbeat(new_id, 1)).await;
        if !local.direct_peers().await.unwrap().contains(&old_id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        !local.direct_peers().await.unwrap().contains(&old_id),
        "the link is not to node 4"
    );
    assert!(local.peers_needing_dial(&[old_id]).await.is_empty());
    assert_eq!(std::fs::read_to_string(&book).unwrap(), "");
}
