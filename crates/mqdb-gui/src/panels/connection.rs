use crate::state::{AppState, Command, ConnectionStatus};
use crate::theme;
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let connected = state.connection == ConnectionStatus::Connected;
    let connecting = state.connection == ConnectionStatus::Connecting;

    ui.add_enabled(
        !connected,
        egui::TextEdit::singleline(&mut state.broker_host).desired_width(120.0),
    );
    ui.colored_label(theme::text_dim(), ":");
    ui.add_enabled(
        !connected,
        egui::TextEdit::singleline(&mut state.broker_port).desired_width(40.0),
    );
    ui.add_space(4.0);
    ui.add_enabled(
        !connected,
        egui::TextEdit::singleline(&mut state.username)
            .desired_width(80.0)
            .hint_text("user"),
    );
    ui.add_enabled(
        !connected,
        egui::TextEdit::singleline(&mut state.password)
            .password(true)
            .desired_width(80.0)
            .hint_text("pass"),
    );

    ui.add_space(4.0);

    if connected {
        if ui.button("Disconnect").clicked() {
            let _ = cmd_tx.send(Command::Disconnect);
        }
    } else if connecting {
        ui.add_enabled(false, egui::Button::new("Connecting..."));
        ui.spinner();
    } else if ui.button("Connect").clicked() {
        let port = state.broker_port.parse::<u16>().unwrap_or(1883);
        let username = if state.username.is_empty() {
            None
        } else {
            Some(state.username.clone())
        };
        let password = if state.password.is_empty() {
            None
        } else {
            Some(state.password.clone())
        };
        state.connection = ConnectionStatus::Connecting;
        let _ = cmd_tx.send(Command::Connect {
            host: state.broker_host.clone(),
            port,
            username,
            password,
        });
    }
}
