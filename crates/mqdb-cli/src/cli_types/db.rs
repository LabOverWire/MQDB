// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::path::PathBuf;

use super::base::{ConnectionArgs, OutputFormat};

#[cfg(feature = "cluster")]
#[derive(Subcommand)]
pub(crate) enum DbAction {
    Create {
        #[arg(short, long, help = "Partition number")]
        partition: u16,
        #[arg(short, long, help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "JSON data")]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Read {
        #[arg(short, long, help = "Partition number")]
        partition: u16,
        #[arg(short, long, help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "Record ID")]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Update {
        #[arg(short, long, help = "Partition number")]
        partition: u16,
        #[arg(short, long, help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "Record ID")]
        id: String,
        #[arg(short, long, help = "JSON data")]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Delete {
        #[arg(short, long, help = "Partition number")]
        partition: u16,
        #[arg(short, long, help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "Record ID")]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
pub(crate) enum BackupAction {
    Create {
        #[arg(short, long, default_value = "backup", help = "Backup name")]
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
pub(crate) enum ConsumerGroupAction {
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    Show {
        #[arg(help = "Consumer group name")]
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
pub(crate) enum SchemaAction {
    #[command(after_long_help = "\
Examples:
  mqdb schema set users --file users_schema.json

Schema file format (JSON):
  {\"name\": {\"type\": \"string\", \"required\": true}, \"age\": {\"type\": \"number\", \"default\": 0}}

Supported types: string, number, boolean")]
    Set {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(short, long, help = "Path to JSON schema file")]
        file: PathBuf,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Get {
        #[arg(help = "Entity name")]
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
pub(crate) enum ConstraintAction {
    #[command(after_long_help = "\
Examples:
  mqdb constraint add users --unique email
  mqdb constraint add posts --fk author_id:users:id:cascade
  mqdb constraint add users --not-null name
  mqdb constraint add orders --name custom_name --unique product_id

Foreign key format: field:target_entity:target_field[:action]
  Actions: cascade, restrict, set_null (default: restrict)")]
    Add {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(long, help = "Custom constraint name (auto-generated if omitted)")]
        name: Option<String>,
        #[arg(long, help = "Field for unique constraint")]
        unique: Option<String>,
        #[arg(long, help = "Foreign key: field:target_entity:target_field[:action]")]
        fk: Option<String>,
        #[arg(long, help = "Field for not-null constraint")]
        not_null: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    List {
        #[arg(help = "Entity name")]
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
pub(crate) enum IndexAction {
    Add {
        #[arg(help = "Entity name")]
        entity: String,
        #[arg(long, num_args = 1.., required = true, help = "Fields to index")]
        fields: Vec<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}
