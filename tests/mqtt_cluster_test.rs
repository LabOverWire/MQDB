use mqdb::cluster::{
    ClusterMessage, ClusterTransport, Epoch, Heartbeat, MqttTransport, NodeController, NodeId,
    Operation, PartitionId, ReplicationWrite, TransportConfig,
};
use mqtt5::broker::MqttBroker;
use mqtt5::broker::config::{BrokerConfig, StorageBackend, StorageConfig};
use mqtt5::{ConnectOptions, MqttClient, QoS, WillMessage};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

async fn start_broker_and_wait(port: u16) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let storage_config = StorageConfig {
        backend: StorageBackend::Memory,
        enable_persistence: true,
        ..Default::default()
    };
    let config = BrokerConfig::default()
        .with_bind_address(addr)
        .with_max_clients(10)
        .with_storage(storage_config);
    let mut broker = MqttBroker::with_config(config).await.unwrap();
    let mut ready_rx = broker.ready_receiver();

    tokio::spawn(async move {
        let _ = broker.run().await;
    });

    let _ = ready_rx.changed().await;
}

#[tokio::test]
async fn mqtt_transport_heartbeat_roundtrip() {
    let port = 11883;
    start_broker_and_wait(port).await;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let t1 = MqttTransport::new(node1);
    let t2 = MqttTransport::new(node2);

    let addr = format!("127.0.0.1:{port}");
    t1.connect(&addr).await.unwrap();
    t2.connect(&addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let hb = Heartbeat::create(node1, 1000);
    t1.broadcast(ClusterMessage::Heartbeat(hb))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let msg = t2.recv();
    assert!(msg.is_some(), "node2 should receive heartbeat from node1");

    match msg.unwrap().message {
        ClusterMessage::Heartbeat(h) => {
            assert_eq!(h.node_id(), 1);
        }
        _ => panic!("expected heartbeat"),
    }

    t1.disconnect().await.unwrap();
    t2.disconnect().await.unwrap();
}

#[tokio::test]
async fn mqtt_transport_replication_write() {
    let port = 11884;
    start_broker_and_wait(port).await;

    let node1 = NodeId::validated(1).unwrap();
    let node2 = NodeId::validated(2).unwrap();

    let t1 = MqttTransport::new(node1);
    let t2 = MqttTransport::new(node2);

    let addr = format!("127.0.0.1:{port}");
    t1.connect(&addr).await.unwrap();
    t2.connect(&addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let partition = PartitionId::new(5).unwrap();
    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        42,
        "users".to_string(),
        "123".to_string(),
        b"test data".to_vec(),
    );

    t1.send(node2, ClusterMessage::Write(write))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let msg = t2.recv();
    assert!(msg.is_some(), "node2 should receive write from node1");

    match msg.unwrap().message {
        ClusterMessage::Write(w) => {
            assert_eq!(w.partition, partition);
            assert_eq!(w.sequence, 42);
            assert_eq!(w.entity, "users");
            assert_eq!(w.id, "123");
        }
        _ => panic!("expected write"),
    }

    t1.disconnect().await.unwrap();
    t2.disconnect().await.unwrap();
}

#[tokio::test]
async fn mqtt_two_node_replication() {
    let port = 11885;
    start_broker_and_wait(port).await;

    let node1_id = NodeId::validated(1).unwrap();
    let node2_id = NodeId::validated(2).unwrap();

    let t1 = MqttTransport::new(node1_id);
    let t2 = MqttTransport::new(node2_id);

    let addr = format!("127.0.0.1:{port}");
    t1.connect(&addr).await.unwrap();
    t2.connect(&addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = TransportConfig::default();
    let mut ctrl1 = NodeController::new(node1_id, t1, config);
    let mut ctrl2 = NodeController::new(node2_id, t2, config);

    let partition = PartitionId::new(0).unwrap();

    ctrl1.become_primary(partition, Epoch::new(1));
    ctrl2.become_replica(partition, Epoch::new(1), 0);

    let write = ReplicationWrite::new(
        partition,
        Operation::Insert,
        Epoch::new(1),
        0,
        "users".to_string(),
        "123".to_string(),
        b"test data".to_vec(),
    );

    let seq = ctrl1.replicate_write(write, &[node2_id], 1).await.unwrap();
    assert_eq!(seq, 1);

    tokio::time::sleep(Duration::from_millis(200)).await;

    ctrl2.process_messages().await;
    assert_eq!(ctrl2.sequence(partition), Some(1));

    tokio::time::sleep(Duration::from_millis(100)).await;

    ctrl1.process_messages().await;
}

#[tokio::test]
async fn mqtt_lwt_death_detection() {
    let port = 11886;
    start_broker_and_wait(port).await;

    let addr = format!("mqtt://127.0.0.1:{port}");

    let node1_id: u16 = 1;
    let death_topic = "cluster/death";

    let death_notice_received = Arc::new(AtomicBool::new(false));
    let received_node_id = Arc::new(std::sync::Mutex::new(0u16));

    let observer = MqttClient::new("observer-node");
    observer.connect(&addr).await.unwrap();

    let death_flag = death_notice_received.clone();
    let node_id_ref = received_node_id.clone();
    observer
        .subscribe(death_topic, move |msg| {
            let death_flag = death_flag.clone();
            let node_id_ref = node_id_ref.clone();
            tokio::spawn(async move {
                if msg.payload.len() >= 2 {
                    let id = u16::from_be_bytes([msg.payload[0], msg.payload[1]]);
                    *node_id_ref.lock().unwrap() = id;
                    death_flag.store(true, Ordering::SeqCst);
                }
            });
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let will_payload = node1_id.to_be_bytes().to_vec();
    let will = WillMessage::new(death_topic, will_payload).with_qos(QoS::AtLeastOnce);
    let options = ConnectOptions::new(format!("mqdb-node-{node1_id}"))
        .with_will(will)
        .with_keep_alive(Duration::from_secs(1));

    let dying_node = MqttClient::with_options(options.clone());
    Box::pin(dying_node.connect_with_options(&addr, options))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    dying_node.disconnect_abnormally().await.unwrap();
    drop(dying_node);

    tokio::time::sleep(Duration::from_millis(2000)).await;

    assert!(
        death_notice_received.load(Ordering::SeqCst),
        "observer should receive death notice via LWT"
    );
    assert_eq!(
        *received_node_id.lock().unwrap(),
        node1_id,
        "death notice should contain the correct node ID"
    );

    observer.disconnect().await.unwrap();
}
