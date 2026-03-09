use crate::state::{ActivePanel, AppState, Command};
use eframe::egui;
use serde_json::Value;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        return;
    };

    ui.horizontal(|ui| {
        ui.heading(format!("Constraints: {entity}"));
        if ui.button("Back").clicked() {
            state.active_panel = ActivePanel::Records;
        }
    });
    ui.separator();

    if let Some(catalog) = &state.catalog
        && let Some(info) = catalog.entities.iter().find(|e| e.name == entity)
    {
        if info.constraints.is_empty() {
            ui.label("No constraints defined");
        } else {
            for constraint in &info.constraints {
                let pretty = serde_json::to_string_pretty(constraint).unwrap_or_default();
                ui.group(|ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut pretty.as_str())
                            .code_editor()
                            .desired_width(f32::INFINITY)
                            .desired_rows(4),
                    );
                });
            }
        }
    }

    ui.separator();
    ui.label("Add constraint (JSON):");
    ui.add(
        egui::TextEdit::multiline(&mut state.constraint_json)
            .code_editor()
            .desired_width(f32::INFINITY)
            .desired_rows(6),
    );

    ui.separator();
    ui.horizontal(|ui| {
        if ui.button("Add Constraint").clicked() {
            match serde_json::from_str::<Value>(&state.constraint_json) {
                Ok(constraint) => {
                    let _ = cmd_tx.send(Command::AddConstraint { entity, constraint });
                }
                Err(e) => {
                    state.status_message = Some(crate::state::StatusMessage {
                        text: format!("invalid JSON: {e}"),
                        is_error: true,
                    });
                }
            }
        }
        if ui.button("Example: Unique").clicked() {
            state.constraint_json =
                "{\n  \"type\": \"unique\",\n  \"fields\": [\"email\"]\n}".to_string();
        }
        if ui.button("Example: Not Null").clicked() {
            state.constraint_json =
                "{\n  \"type\": \"not_null\",\n  \"field\": \"name\"\n}".to_string();
        }
    });
}
