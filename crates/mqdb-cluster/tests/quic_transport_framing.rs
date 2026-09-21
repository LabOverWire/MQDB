use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqdb_cluster::{
    ClusterMessage, ClusterTransport, Heartbeat, NodeId, QuicDirectTransport, TransportError,
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

fn stalling_server_endpoint(certs: &Certs) -> Endpoint {
    let key = PrivateKeyDer::try_from(certs.leaf_key_der.clone()).unwrap();
    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certs.leaf_der.clone()], key)
        .unwrap();

    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
    ));

    let mut transport = TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(STALL_WINDOW));
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

#[tokio::test]
async fn backpressure_drops_messages_but_keeps_the_stream_framed() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let leaf_path = dir.path().join("leaf.pem");
    let leaf_key_path = dir.path().join("leaf.key");
    std::fs::write(&ca_path, &certs.ca_pem).unwrap();
    std::fs::write(&leaf_path, &certs.leaf_pem).unwrap();
    std::fs::write(&leaf_key_path, &certs.leaf_key_pem).unwrap();

    let endpoint = stalling_server_endpoint(&certs);
    let far_addr: SocketAddr = endpoint.local_addr().unwrap();

    let (release_tx, release_rx) = oneshot::channel();
    let (frame_tx, frame_rx) = flume::unbounded();
    let far_handle = tokio::spawn(far_end(endpoint, release_rx, frame_tx));

    let local = NodeId::validated(1).unwrap();
    let peer = NodeId::validated(2).unwrap();
    let transport = QuicDirectTransport::new(local);
    transport.set_ca_file(ca_path.clone());
    transport
        .bind("127.0.0.1:0".parse().unwrap(), &leaf_path, &leaf_key_path)
        .await
        .unwrap();
    transport.connect_to_peer(peer, far_addr).await.unwrap();

    let mut ok_count = 0u32;
    let mut terminated_by_drop = false;
    let mut blocked = false;
    for tick in 0..SEND_ATTEMPTS {
        let message = ClusterMessage::Heartbeat(Heartbeat::create(local, u64::from(tick)));
        match tokio::time::timeout(Duration::from_millis(500), transport.send(peer, message)).await
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

    let follow_up = ClusterMessage::Heartbeat(Heartbeat::create(local, u64::MAX));
    transport.send(peer, follow_up).await.unwrap();
    let frame = collect_frame(&frame_rx).await;
    assert!(frame.len() >= 3);
    assert_eq!(
        &frame[0..2],
        &local.get().to_be_bytes(),
        "post-backpressure frame mis-framed"
    );

    far_handle.abort();
}
