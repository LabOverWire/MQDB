use crate::state::{ActivePanel, AppState, Command, ConnectionStatus};
use crate::theme;
use eframe::egui;

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    ui.colored_label(theme::text_dim(), "ENTITIES");
    ui.add_space(4.0);

    let connected = state.connection == ConnectionStatus::Connected;

    if let Some(catalog) = &state.catalog {
        if catalog.entities.is_empty() {
            ui.colored_label(theme::text_dim(), "no entities");
        }
        let entities = catalog.entities.clone();
        for entity in &entities {
            let selected = state.selected_entity.as_deref() == Some(&entity.name);
            let label_text = format!("{}  ({})", entity.name, entity.record_count);
            let label = if selected {
                egui::RichText::new(label_text).color(theme::accent())
            } else {
                egui::RichText::new(label_text)
            };
            if ui.selectable_label(selected, label).clicked() && !selected {
                select_entity(state, cmd_tx, &entity.name);
            }
        }
    } else if connected {
        ui.colored_label(theme::text_dim(), "enter entity name below");
    } else {
        ui.colored_label(theme::text_dim(), "not connected");
    }

    if let Some(name) = &state.selected_entity
        && (state.catalog.is_none()
            || state
                .catalog
                .as_ref()
                .is_some_and(|c| !c.entities.iter().any(|e| &e.name == name)))
    {
        let selected_name = name.clone();
        ui.horizontal(|ui| {
            ui.colored_label(theme::accent(), &selected_name);
        });
    }

    if connected {
        ui.add_space(8.0);
        ui.separator();
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            let response = ui.add(
                egui::TextEdit::singleline(&mut state.new_entity_name)
                    .desired_width(120.0)
                    .hint_text("new entity"),
            );
            let enter_pressed =
                response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if (ui.button("+").clicked() || enter_pressed) && !state.new_entity_name.is_empty() {
                let name = state.new_entity_name.clone();
                if let Some(catalog) = &mut state.catalog
                    && !catalog.entities.iter().any(|e| e.name == name)
                {
                    catalog.entities.push(crate::state::EntityInfo {
                        name: name.clone(),
                        ..Default::default()
                    });
                }
                select_entity(state, cmd_tx, &name);
                state.new_entity_name.clear();
            }
        });

    }
}

fn select_entity(state: &mut AppState, cmd_tx: &flume::Sender<Command>, name: &str) {
    state.selected_entity = Some(name.to_string());
    state.active_panel = ActivePanel::Records;
    state.records.clear();
    state.selected_record = None;
    let limit = state.record_limit.parse().unwrap_or(50);
    let offset = state.record_offset.parse().unwrap_or(0);
    let _ = cmd_tx.send(Command::ListRecords {
        entity: name.to_string(),
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
