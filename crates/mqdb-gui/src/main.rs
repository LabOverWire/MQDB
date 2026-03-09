mod app;
mod client;
mod panels;
mod state;
mod theme;

use eframe::egui;

fn main() -> eframe::Result {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let (cmd_tx, cmd_rx) = flume::bounded::<state::Command>(64);
    let (ui_tx, ui_rx) = flume::bounded::<state::UiEvent>(256);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([800.0, 500.0]),
        ..Default::default()
    };

    eframe::run_native(
        "MQDB",
        options,
        Box::new(move |cc| {
            theme::apply(&cc.egui_ctx);

            let repaint_ctx = cc.egui_ctx.clone();
            let repaint = Box::new(move || repaint_ctx.request_repaint());

            let backend = client::MqttBackend::new(cmd_rx, ui_tx, repaint);
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
                rt.block_on(backend.run());
            });

            Ok(Box::new(app::App::new(cmd_tx, ui_rx)))
        }),
    )
}
