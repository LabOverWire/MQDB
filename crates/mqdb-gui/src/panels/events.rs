use crate::state::{ActivePanel, AppState, Command};
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    ui.horizontal(|ui| {
        ui.heading("Live Events");
        if ui.button("Back").clicked() {
            let _ = cmd_tx.send(Command::UnsubscribeEvents);
            state.active_panel = ActivePanel::Records;
        }
        if ui.button("Clear").clicked() {
            state.events.clear();
        }
        ui.label(format!("{} events", state.events.len()));
    });
    ui.separator();

    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .stick_to_bottom(true)
        .show(ui, |ui| {
            for event in &state.events {
                ui.group(|ui| {
                    ui.horizontal(|ui| {
                        let color = match event.operation.as_str() {
                            "create" => egui::Color32::GREEN,
                            "delete" => egui::Color32::RED,
                            "update" => egui::Color32::YELLOW,
                            _ => egui::Color32::GRAY,
                        };
                        ui.colored_label(color, &event.operation);
                        ui.label(format!("{}/{}", event.entity, event.id));
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
