// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_agent::MqdbAgent;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

const AGENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RunningAgent {
    pub port: u16,
    handle: JoinHandle<()>,
    ready: watch::Receiver<bool>,
    shutdown: broadcast::Sender<()>,
    dir: TempDir,
}

impl RunningAgent {
    pub async fn launch(agent: MqdbAgent, port: u16, dir: TempDir) -> Self {
        let (handle, ready, shutdown) = agent
            .with_bind_address(SocketAddr::from(([127, 0, 0, 1], port)))
            .start()
            .await
            .unwrap_or_else(|e| panic!("the agent on port {port} failed to start: {e}"));
        Self {
            port,
            handle,
            ready,
            shutdown,
            dir,
        }
    }

    pub async fn start(agent: MqdbAgent, port: u16, dir: TempDir) -> Self {
        let mut running = Self::launch(agent, port, dir).await;
        tokio::time::timeout(AGENT_TIMEOUT, running.ready.wait_for(|ready| *ready))
            .await
            .unwrap_or_else(|_| panic!("the agent on port {port} did not become ready in time"))
            .unwrap_or_else(|_| panic!("the agent on port {port} stopped before it became ready"));
        running
    }

    pub async fn stop(self) {
        let port = self.port;
        self.shutdown.send(()).unwrap_or_else(|_| {
            panic!(
                "the agent on port {port} stopped before the test did; \
                 its broker error is in the tracing output"
            )
        });
        tokio::time::timeout(AGENT_TIMEOUT, self.handle)
            .await
            .unwrap_or_else(|_| panic!("the agent on port {port} did not stop after shutdown"))
            .unwrap_or_else(|e| panic!("the agent task on port {port} panicked: {e}"));
        self.dir
            .close()
            .unwrap_or_else(|e| panic!("remove the data dir of the agent on port {port}: {e}"));
    }
}
