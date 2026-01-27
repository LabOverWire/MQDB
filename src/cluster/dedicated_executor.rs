use std::sync::Arc;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::oneshot;
use tracing::debug;

pub struct DedicatedExecutor {
    runtime: Arc<Runtime>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    name: String,
}

impl DedicatedExecutor {
    /// # Panics
    /// Panics if the Tokio runtime or runtime thread cannot be created.
    #[must_use]
    pub fn new(name: &str, worker_threads: usize) -> Self {
        let worker_count = worker_threads.clamp(2, 8);

        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_count)
            .thread_name(format!("{name}-worker"))
            .enable_all()
            .build()
            .expect("failed to create dedicated runtime");

        let runtime = Arc::new(runtime);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let rt_clone = runtime.clone();
        let thread_name = format!("{name}-main");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                rt_clone.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })
            .expect("failed to spawn runtime thread");

        debug!(
            name = %name,
            workers = worker_count,
            "created dedicated executor"
        );

        Self {
            runtime,
            shutdown_tx: Some(shutdown_tx),
            name: name.to_string(),
        }
    }

    #[must_use]
    pub fn handle(&self) -> Handle {
        self.runtime.handle().clone()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            debug!(name = %self.name, "dedicated executor shutdown initiated");
        }
    }
}

impl Drop for DedicatedExecutor {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            debug!(name = %self.name, "dedicated executor dropped");
        }
    }
}
