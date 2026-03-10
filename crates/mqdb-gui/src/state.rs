use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum Command {
    Connect {
        host: String,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    },
    Disconnect,
    FetchCatalog,
    ListRecords {
        entity: String,
        filters: Vec<FilterSpec>,
        sort: Vec<SortSpec>,
        limit: usize,
        offset: usize,
    },
    ReadRecord {
        entity: String,
        id: String,
    },
    CreateRecord {
        entity: String,
        data: Value,
    },
    UpdateRecord {
        entity: String,
        id: String,
        fields: Value,
    },
    DeleteRecord {
        entity: String,
        id: String,
    },
    SetSchema {
        entity: String,
        schema: Value,
    },
    AddConstraint {
        entity: String,
        constraint: Value,
    },
    SubscribeEvents {
        entity: String,
    },
    UnsubscribeEvents,
}

#[derive(Debug, Clone)]
pub enum UiEvent {
    Connected,
    Disconnected,
    ConnectionError(String),
    CatalogReceived(CatalogData),
    RecordsReceived { entity: String, records: Vec<Value> },
    RecordReceived { entity: String, record: Value },
    OperationSuccess(String),
    OperationError(String),
    EventReceived(LiveEvent),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogData {
    pub entities: Vec<EntityInfo>,
    pub server: ServerInfo,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerInfo {
    pub mode: String,
    #[serde(default)]
    pub vault_enabled: bool,
    #[serde(default)]
    pub node_id: Option<u16>,
    #[serde(default)]
    pub is_leader: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntityInfo {
    pub name: String,
    #[serde(default)]
    pub record_count: usize,
    pub schema: Option<Value>,
    #[serde(default)]
    pub constraints: Vec<Value>,
    pub ownership: Option<Value>,
    pub scope: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct LiveEvent {
    pub entity: String,
    pub operation: String,
    pub id: String,
    pub data: Value,
}

#[derive(Debug, Clone, Default)]
pub struct FilterSpec {
    pub field: String,
    pub op: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct SortSpec {
    pub field: String,
    pub direction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected,
}

#[derive(Default)]
pub struct AppState {
    pub connection: ConnectionStatus,
    pub broker_host: String,
    pub broker_port: String,
    pub username: String,
    pub password: String,
    pub catalog: Option<CatalogData>,
    pub selected_entity: Option<String>,
    pub records: Vec<Value>,
    pub selected_record: Option<Value>,
    pub status_message: Option<StatusMessage>,
    pub events: Vec<LiveEvent>,
    pub filter_rows: Vec<FilterSpec>,
    pub sort_field: String,
    pub sort_direction: String,
    pub record_limit: String,
    pub record_offset: String,
    pub create_json: String,
    pub edit_json: String,
    pub schema_json: String,
    pub constraint_json: String,
    pub active_panel: ActivePanel,
    pub selected_row: Option<usize>,
    pub new_entity_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ActivePanel {
    #[default]
    Records,
    Detail,
    Create,
    Edit,
    Schema,
    Constraints,
    Events,
}

#[derive(Debug, Clone)]
pub struct StatusMessage {
    pub text: String,
    pub is_error: bool,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            broker_host: "127.0.0.1".to_string(),
            broker_port: "1883".to_string(),
            record_limit: "50".to_string(),
            record_offset: "0".to_string(),
            create_json: "{\n  \n}".to_string(),
            ..Default::default()
        }
    }
}
