#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
mod native {
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
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    pub use std::sync::Mutex;
    pub use std::sync::RwLock;
    pub use web_time::{Duration, Instant};

    pub fn spawn<F>(future: F)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        wasm_bindgen_futures::spawn_local(future);
    }

    pub async fn sleep(duration: Duration) {
        let millis = duration.as_millis() as i32;
        let promise = js_sys::Promise::new(&mut |resolve, _| {
            if let Some(window) = web_sys::window() {
                let _ =
                    window.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, millis);
            }
        });
        let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use native::*;

#[cfg(target_arch = "wasm32")]
pub use wasm::*;
