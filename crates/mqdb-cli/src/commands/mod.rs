// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub(crate) mod acl;
pub(crate) mod agent;
pub(crate) mod auth;
pub(crate) mod bench;
#[cfg(feature = "cluster")]
pub(crate) mod cluster;
pub(crate) mod consumer;
pub(crate) mod crud;
#[cfg(feature = "cluster")]
pub(crate) mod dev;
#[cfg(feature = "cluster")]
pub(crate) mod dev_bench;
pub(crate) mod env_secret;

/// Resolve once a shutdown signal arrives: Ctrl-C (SIGINT) on every platform,
/// plus SIGTERM on unix so `docker stop`/`systemctl stop` shut down gracefully.
pub(crate) async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!("failed to install SIGTERM handler: {e}; using SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
