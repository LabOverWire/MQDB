use crate::state::{ActivePanel, AppState, Command};
use crate::theme;
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    ui.horizontal(|ui| {
        ui.heading("Live Events");
        ui.add_space(8.0);
        if ui.button("Back").clicked() {
            let _ = cmd_tx.send(Command::UnsubscribeEvents);
            state.active_panel = ActivePanel::Records;
        }
        if ui.button("Clear").clicked() {
            state.events.clear();
        }
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.colored_label(theme::text_dim(), format!("{} events", state.events.len()));
        });
    });
    ui.separator();

    if state.events.is_empty() {
        ui.add_space(20.0);
        ui.centered_and_justified(|ui| {
            ui.colored_label(theme::text_dim(), "Waiting for events...");
        });
        return;
    }

    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .stick_to_bottom(true)
        .show(ui, |ui| {
            for event in &state.events {
                ui.group(|ui| {
                    ui.horizontal(|ui| {
                        let color = match event.operation.as_str() {
                            "create" => theme::CREATE_COLOR,
                            "delete" => theme::DELETE_COLOR,
                            "update" => theme::UPDATE_COLOR,
                            _ => theme::text_dim(),
                        };
                        ui.colored_label(color, egui::RichText::new(&event.operation).strong());
                        ui.colored_label(
                            theme::text_dim(),
                            format!("{}/{}", event.entity, event.id),
                        );
                    });
                    let data_str = serde_json::to_string_pretty(&event.data).unwrap_or_default();
                    ui.add(
                        egui::TextEdit::multiline(&mut data_str.as_str())
                            .code_editor()
                            .desired_width(f32::INFINITY)
                            .desired_rows(3),
                    );
                });
            }
        });
}
