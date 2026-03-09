use crate::state::{ActivePanel, AppState, Command};
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    ui.heading("Entities");
    ui.separator();

    if let Some(catalog) = &state.catalog {
        if catalog.entities.is_empty() {
            ui.label("No entities found");
        }
        let entities = catalog.entities.clone();
        for entity in &entities {
            let selected = state.selected_entity.as_deref() == Some(&entity.name);
            let label = format!("{} ({})", entity.name, entity.record_count);
            if ui.selectable_label(selected, label).clicked() && !selected {
                state.selected_entity = Some(entity.name.clone());
                state.active_panel = ActivePanel::Records;
                state.records.clear();
                state.selected_record = None;
                let limit = state.record_limit.parse().unwrap_or(50);
                let offset = state.record_offset.parse().unwrap_or(0);
                let _ = cmd_tx.send(Command::ListRecords {
                    entity: entity.name.clone(),
                    filters: state.filter_rows.clone(),
                    sort: if state.sort_field.is_empty() {
                        vec![]
                    } else {
                        vec![crate::state::SortSpec {
                            field: state.sort_field.clone(),
                            direction: state.sort_direction.clone(),
                        }]
                    },
                    limit,
                    offset,
                });
            }
        }
    } else {
        ui.label("Not connected");
    }

    ui.separator();
    ui.horizontal(|ui| {
        ui.text_edit_singleline(&mut state.new_entity_name);
        if ui.button("Add").clicked() && !state.new_entity_name.is_empty() {
            if let Some(catalog) = &mut state.catalog {
                let name = state.new_entity_name.clone();
                if !catalog.entities.iter().any(|e| e.name == name) {
                    catalog.entities.push(crate::state::EntityInfo {
                        name: name.clone(),
                        ..Default::default()
                    });
                }
                state.selected_entity = Some(name);
                state.active_panel = ActivePanel::Records;
                state.records.clear();
            }
            state.new_entity_name.clear();
        }
    });

    ui.separator();
    if ui.button("Refresh Catalog").clicked() {
        let _ = cmd_tx.send(Command::FetchCatalog);
    }

    if let Some(catalog) = &state.catalog {
        ui.separator();
        ui.label(format!("Mode: {}", catalog.server.mode));
        if let Some(node_id) = catalog.server.node_id {
            ui.label(format!("Node: {node_id}"));
        }
    }
}
