use crate::state::{ActivePanel, AppState, Command};
use eframe::egui;
use serde_json::Value;

pub fn show_create(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        return;
    };

    ui.horizontal(|ui| {
        ui.heading(format!("Create {entity}"));
        if ui.button("Back").clicked() {
            state.active_panel = ActivePanel::Records;
        }
    });
    ui.separator();

    ui.label("JSON data:");
    egui::ScrollArea::vertical()
        .max_height(ui.available_height() - 40.0)
        .show(ui, |ui| {
            ui.add(
                egui::TextEdit::multiline(&mut state.create_json)
                    .code_editor()
                    .desired_width(f32::INFINITY)
                    .desired_rows(20),
            );
        });

    ui.separator();
    ui.horizontal(|ui| {
        if ui.button("Create").clicked() {
            match serde_json::from_str::<Value>(&state.create_json) {
                Ok(data) => {
                    let _ = cmd_tx.send(Command::CreateRecord {
                        entity: entity.clone(),
                        data,
                    });
                }
                Err(e) => {
                    state.status_message = Some(crate::state::StatusMessage {
                        text: format!("invalid JSON: {e}"),
                        is_error: true,
                    });
                }
            }
        }
        if ui.button("Clear").clicked() {
            state.create_json = "{\n  \n}".to_string();
        }
    });
}

pub fn show_edit(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        return;
    };
    let record_id = state
        .selected_record
        .as_ref()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let Some(id) = record_id else {
        ui.label("No record selected");
        return;
    };

    ui.horizontal(|ui| {
        ui.heading(format!("Edit {entity}/{id}"));
        if ui.button("Back").clicked() {
            state.active_panel = ActivePanel::Records;
        }
    });
    ui.separator();

    egui::ScrollArea::vertical()
        .max_height(ui.available_height() - 40.0)
        .show(ui, |ui| {
            ui.add(
                egui::TextEdit::multiline(&mut state.edit_json)
                    .code_editor()
                    .desired_width(f32::INFINITY)
                    .desired_rows(20),
            );
        });

    ui.separator();
    if ui.button("Update").clicked() {
        match serde_json::from_str::<Value>(&state.edit_json) {
            Ok(mut fields) => {
                if let Some(obj) = fields.as_object_mut() {
                    obj.remove("id");
                    obj.remove("_version");
                }
                let _ = cmd_tx.send(Command::UpdateRecord {
                    entity: entity.clone(),
                    id,
                    fields,
                });
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

pub fn show_detail(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        return;
    };

    ui.horizontal(|ui| {
        ui.heading("Record Detail");
        if ui.button("Back").clicked() {
            state.active_panel = ActivePanel::Records;
        }
    });
    ui.separator();

    if let Some(record) = &state.selected_record {
        let pretty = serde_json::to_string_pretty(record).unwrap_or_default();
        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.add(
                egui::TextEdit::multiline(&mut pretty.as_str())
                    .code_editor()
                    .desired_width(f32::INFINITY),
            );
        });

        ui.separator();
        ui.horizontal(|ui| {
            if ui.button("Edit").clicked() {
                state.edit_json = serde_json::to_string_pretty(record).unwrap_or_default();
                state.active_panel = ActivePanel::Edit;
            }
            if ui.button("Delete").clicked()
                && let Some(id) = record.get("id").and_then(|v| v.as_str())
            {
                let _ = cmd_tx.send(Command::DeleteRecord {
                    entity: entity.clone(),
                    id: id.to_string(),
                });
                state.active_panel = ActivePanel::Records;
            }
            if ui.button("Refresh").clicked()
                && let Some(id) = record.get("id").and_then(|v| v.as_str())
            {
                let _ = cmd_tx.send(Command::ReadRecord {
                    entity,
                    id: id.to_string(),
                });
            }
        });
    } else {
        ui.label("No record selected");
    }
}
