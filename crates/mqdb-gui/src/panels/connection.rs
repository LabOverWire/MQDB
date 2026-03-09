use crate::state::{AppState, Command, ConnectionStatus};
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    ui.horizontal(|ui| {
        let connected = state.connection == ConnectionStatus::Connected;
        let connecting = state.connection == ConnectionStatus::Connecting;

        ui.label("Host:");
        ui.add_enabled(
            !connected,
            egui::TextEdit::singleline(&mut state.broker_host).desired_width(120.0),
        );
        ui.label("Port:");
        ui.add_enabled(
            !connected,
            egui::TextEdit::singleline(&mut state.broker_port).desired_width(50.0),
        );
        ui.label("User:");
        ui.add_enabled(
            !connected,
            egui::TextEdit::singleline(&mut state.username).desired_width(80.0),
        );
        ui.label("Pass:");
        ui.add_enabled(
            !connected,
            egui::TextEdit::singleline(&mut state.password)
                .password(true)
                .desired_width(80.0),
        );

        if connected {
            if ui.button("Disconnect").clicked() {
                let _ = cmd_tx.send(Command::Disconnect);
            }
            ui.colored_label(egui::Color32::GREEN, "Connected");
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
    });
}
