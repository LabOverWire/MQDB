use crate::state::{ActivePanel, AppState, Command, FilterSpec, SortSpec};
use eframe::egui;
use egui_extras::{Column, TableBuilder};

pub fn show(ui: &mut egui::Ui, state: &mut AppState, cmd_tx: &flume::Sender<Command>) {
    let Some(entity) = state.selected_entity.clone() else {
        ui.centered_and_justified(|ui| {
            ui.label("Select an entity from the sidebar");
        });
        return;
    };

    ui.horizontal(|ui| {
        ui.heading(&entity);
        ui.separator();
        if ui.button("Create").clicked() {
            state.active_panel = ActivePanel::Create;
            state.create_json = "{\n  \n}".to_string();
        }
        if ui.button("Schema").clicked() {
            state.active_panel = ActivePanel::Schema;
            if let Some(catalog) = &state.catalog
                && let Some(info) = catalog.entities.iter().find(|e| e.name == entity)
            {
                state.schema_json = info.schema.as_ref().map_or_else(
                    || "{\n  \"fields\": {}\n}".to_string(),
                    |s| serde_json::to_string_pretty(s).unwrap_or_default(),
                );
            }
        }
        if ui.button("Constraints").clicked() {
            state.active_panel = ActivePanel::Constraints;
        }
        if ui.button("Events").clicked() {
            state.active_panel = ActivePanel::Events;
            let _ = cmd_tx.send(Command::SubscribeEvents {
                entity: entity.clone(),
            });
        }
    });

    ui.separator();
    show_filter_bar(ui, state, cmd_tx, &entity);
    ui.separator();

    if state.records.is_empty() {
        ui.label("No records");
        return;
    }

    let columns = collect_columns(&state.records);

    let available = ui.available_size();
    TableBuilder::new(ui)
        .striped(true)
        .resizable(true)
        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
        .min_scrolled_height(available.y - 20.0)
        .columns(Column::auto().at_least(60.0), columns.len() + 1)
        .header(20.0, |mut header| {
            for col in &columns {
                header.col(|ui| {
                    ui.strong(col);
                });
            }
            header.col(|ui| {
                ui.strong("Actions");
            });
        })
        .body(|body| {
            let records = state.records.clone();
            body.rows(20.0, records.len(), |mut row| {
                let idx = row.index();
                let record = &records[idx];
                for col in &columns {
                    row.col(|ui| {
                        let val = record.get(col).map(format_value).unwrap_or_default();
                        ui.label(&val);
                    });
                }
                row.col(|ui| {
                    if ui.small_button("View").clicked() {
                        state.selected_record = Some(record.clone());
                        state.active_panel = ActivePanel::Detail;
                    }
                    if ui.small_button("Edit").clicked() {
                        state.selected_record = Some(record.clone());
                        state.edit_json = serde_json::to_string_pretty(record).unwrap_or_default();
                        state.active_panel = ActivePanel::Edit;
                    }
                    if ui.small_button("Delete").clicked()
                        && let Some(id) = record.get("id").and_then(|v| v.as_str())
                    {
                        let _ = cmd_tx.send(Command::DeleteRecord {
                            entity: entity.clone(),
                            id: id.to_string(),
                        });
                    }
                });
            });
        });
}

#[allow(clippy::too_many_lines)]
fn show_filter_bar(
    ui: &mut egui::Ui,
    state: &mut AppState,
    cmd_tx: &flume::Sender<Command>,
    entity: &str,
) {
    egui::CollapsingHeader::new("Filters & Sort")
        .default_open(false)
        .show(ui, |ui| {
            let mut remove_idx = None;
            for (i, filter) in state.filter_rows.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    ui.label("Field:");
                    ui.text_edit_singleline(&mut filter.field);
                    ui.label("Op:");
                    egui::ComboBox::from_id_salt(format!("filter_op_{i}"))
                        .selected_text(&filter.op)
                        .width(50.0)
                        .show_ui(ui, |ui| {
                            for op in ["=", "!=", ">", ">=", "<", "<=", "~"] {
                                ui.selectable_value(&mut filter.op, op.to_string(), op);
                            }
                        });
                    ui.label("Value:");
                    ui.text_edit_singleline(&mut filter.value);
                    if ui.small_button("X").clicked() {
                        remove_idx = Some(i);
                    }
                });
            }
            if let Some(idx) = remove_idx {
                state.filter_rows.remove(idx);
            }
            if ui.small_button("+ Add Filter").clicked() {
                state.filter_rows.push(FilterSpec {
                    op: "=".to_string(),
                    ..Default::default()
                });
            }

            ui.horizontal(|ui| {
                ui.label("Sort:");
                ui.text_edit_singleline(&mut state.sort_field);
                egui::ComboBox::from_id_salt("sort_dir")
                    .selected_text(&state.sort_direction)
                    .width(60.0)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut state.sort_direction, "asc".to_string(), "asc");
                        ui.selectable_value(&mut state.sort_direction, "desc".to_string(), "desc");
                    });
            });

            ui.horizontal(|ui| {
                ui.label("Limit:");
                ui.add(egui::TextEdit::singleline(&mut state.record_limit).desired_width(50.0));
                ui.label("Offset:");
                ui.add(egui::TextEdit::singleline(&mut state.record_offset).desired_width(50.0));
            });
        });

    ui.horizontal(|ui| {
        if ui.button("Search").clicked() {
            let limit = state.record_limit.parse().unwrap_or(50);
            let offset = state.record_offset.parse().unwrap_or(0);
            let _ = cmd_tx.send(Command::ListRecords {
                entity: entity.to_string(),
                filters: state.filter_rows.clone(),
                sort: if state.sort_field.is_empty() {
                    vec![]
                } else {
                    vec![SortSpec {
                        field: state.sort_field.clone(),
                        direction: if state.sort_direction.is_empty() {
                            "asc".to_string()
                        } else {
                            state.sort_direction.clone()
                        },
                    }]
                },
                limit,
                offset,
            });
        }

        let offset: usize = state.record_offset.parse().unwrap_or(0);
        let limit: usize = state.record_limit.parse().unwrap_or(50);

        if offset > 0 && ui.button("< Prev").clicked() {
            let new_offset = offset.saturating_sub(limit);
            state.record_offset = new_offset.to_string();
            let _ = cmd_tx.send(Command::ListRecords {
                entity: entity.to_string(),
                filters: state.filter_rows.clone(),
                sort: vec![],
                limit,
                offset: new_offset,
            });
        }

        if state.records.len() >= limit && ui.button("Next >").clicked() {
            let new_offset = offset + limit;
            state.record_offset = new_offset.to_string();
            let _ = cmd_tx.send(Command::ListRecords {
                entity: entity.to_string(),
                filters: state.filter_rows.clone(),
                sort: vec![],
                limit,
                offset: new_offset,
            });
        }

        ui.label(format!("{} records", state.records.len()));
    });
}

fn collect_columns(records: &[serde_json::Value]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Some(serde_json::Value::Object(obj)) = records.first()
        && obj.contains_key("id")
    {
        cols.push("id".to_string());
        seen.insert("id".to_string());
    }

    for record in records {
        if let serde_json::Value::Object(obj) = record {
            for key in obj.keys() {
                if !key.starts_with('_') && seen.insert(key.clone()) {
                    cols.push(key.clone());
                }
            }
        }
    }

    cols
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(arr) => format!("[{} items]", arr.len()),
        serde_json::Value::Object(obj) => format!("{{{} fields}}", obj.len()),
    }
}
