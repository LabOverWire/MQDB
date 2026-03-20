// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(23000);

pub fn next_test_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[allow(dead_code)]
pub async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for attempt in 1..=50u32 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        assert!(attempt != 50, "port {port} not ready after 5s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
