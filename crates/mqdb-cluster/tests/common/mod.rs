// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub fn next_test_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    listener.local_addr().expect("local_addr").port()
}
