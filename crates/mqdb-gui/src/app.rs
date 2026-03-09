use crate::panels;
use crate::state::{ActivePanel, AppState, Command, ConnectionStatus, StatusMessage, UiEvent};
use eframe::egui;

pub struct App {
    state: AppState,
    cmd_tx: flume::Sender<Command>,
    ui_rx: flume::Receiver<UiEvent>,
}

impl App {
    pub fn new(cmd_tx: flume::Sender<Command>, ui_rx: flume::Receiver<UiEvent>) -> Self {
        Self {
            state: AppState::new(),
            cmd_tx,
            ui_rx,
        }
    }

    fn poll_events(&mut self) {
        while let Ok(event) = self.ui_rx.try_recv() {
            match event {
                UiEvent::Connected => {
                    self.state.connection = ConnectionStatus::Connected;
                    self.state.status_message = Some(StatusMessage {
                        text: "connected".to_string(),
                        is_error: false,
                    });
                    let _ = self.cmd_tx.send(Command::FetchCatalog);
                }
                UiEvent::Disconnected => {
                    self.state.connection = ConnectionStatus::Disconnected;
                    self.state.catalog = None;
                    self.state.selected_entity = None;
                    self.state.records.clear();
                    self.state.selected_record = None;
                    self.state.events.clear();
                    self.state.status_message = Some(StatusMessage {
                        text: "disconnected".to_string(),
                        is_error: false,
                    });
                }
                UiEvent::ConnectionError(e) => {
                    self.state.connection = ConnectionStatus::Disconnected;
                    self.state.status_message = Some(StatusMessage {
                        text: e,
                        is_error: true,
                    });
                }
                UiEvent::CatalogReceived(catalog) => {
                    self.state.catalog = Some(catalog);
                    self.state.status_message = Some(StatusMessage {
                        text: "catalog loaded".to_string(),
                        is_error: false,
                    });
                }
                UiEvent::RecordsReceived { entity, records } => {
                    if self.state.selected_entity.as_deref() == Some(&entity) {
                        self.state.records = records;
                    }
                }
                UiEvent::RecordReceived { entity, record } => {
                    if self.state.selected_entity.as_deref() == Some(&entity) {
                        self.state.selected_record = Some(record);
                    }
                }
                UiEvent::OperationSuccess(msg) => {
                    self.state.status_message = Some(StatusMessage {
                        text: msg,
                        is_error: false,
                    });
                    if let Some(entity) = &self.state.selected_entity {
                        let limit = self.state.record_limit.parse().unwrap_or(50);
                        let offset = self.state.record_offset.parse().unwrap_or(0);
                        let _ = self.cmd_tx.send(Command::ListRecords {
                            entity: entity.clone(),
                            filters: self.state.filter_rows.clone(),
                            sort: vec![],
                            limit,
                            offset,
                        });
                        let _ = self.cmd_tx.send(Command::FetchCatalog);
                    }
                }
                UiEvent::OperationError(msg) => {
                    self.state.status_message = Some(StatusMessage {
                        text: msg,
                        is_error: true,
                    });
                }
                UiEvent::EventReceived(event) => {
                    const MAX_EVENTS: usize = 500;
                    if self.state.events.len() >= MAX_EVENTS {
                        self.state.events.remove(0);
                    }
                    self.state.events.push(event);
                }
            }
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_events();

        egui::TopBottomPanel::top("connection").show(ctx, |ui| {
            panels::connection::show(ui, &mut self.state, &self.cmd_tx);
        });

        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(msg) = &self.state.status_message {
                    let color = if msg.is_error {
                        egui::Color32::RED
                    } else {
                        egui::Color32::GREEN
                    };
                    ui.colored_label(color, &msg.text);
                }
            });
        });

        egui::SidePanel::left("entities_panel")
            .default_width(200.0)
            .resizable(true)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    panels::entities::show(ui, &mut self.state, &self.cmd_tx);
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| match self.state.active_panel {
            ActivePanel::Records => {
                panels::records::show(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Detail => {
                panels::record_edit::show_detail(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Create => {
                panels::record_edit::show_create(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Edit => {
                panels::record_edit::show_edit(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Schema => {
                panels::schema::show(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Constraints => {
                panels::constraints::show(ui, &mut self.state, &self.cmd_tx);
            }
            ActivePanel::Events => {
                panels::events::show(ui, &mut self.state, &self.cmd_tx);
            }
        });
    }
}
