// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

pub use std::time::{Duration, Instant};
pub use tokio::sync::Mutex;
pub use tokio::sync::RwLock;

pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}
