use crate::state::{ActivePanel, AppState, Command};
use eframe::egui;
use serde_json::Value;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        return;
    };

    ui.horizontal(|ui| {
        ui.heading(format!("Schema: {entity}"));
        if ui.button("Back").clicked() {
            state.active_panel = ActivePanel::Records;
        }
    });
    ui.separator();

    if let Some(catalog) = &state.catalog
        && let Some(info) = catalog.entities.iter().find(|e| e.name == entity)
    {
        if let Some(schema) = &info.schema {
            ui.label("Current schema:");
            let pretty = serde_json::to_string_pretty(schema).unwrap_or_default();
            ui.add(
                egui::TextEdit::multiline(&mut pretty.as_str())
                    .code_editor()
                    .desired_width(f32::INFINITY)
                    .desired_rows(8),
            );
            ui.separator();
        } else {
            ui.label("No schema defined");
            ui.separator();
        }
    }

    ui.label("Set schema (JSON):");
    egui::ScrollArea::vertical()
        .max_height(ui.available_height() - 40.0)
        .show(ui, |ui| {
            ui.add(
                egui::TextEdit::multiline(&mut state.schema_json)
                    .code_editor()
                    .desired_width(f32::INFINITY)
                    .desired_rows(15),
            );
        });

    ui.separator();
    if ui.button("Set Schema").clicked() {
        match serde_json::from_str::<Value>(&state.schema_json) {
            Ok(schema) => {
                let _ = cmd_tx.send(Command::SetSchema { entity, schema });
            }
            Err(e) => {
                state.status_message = Some(crate::state::StatusMessage {
                    text: format!("invalid JSON: {e}"),
                    is_error: true,
                });
            }
        }
    }
}
