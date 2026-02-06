// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::path::PathBuf;

use super::base::{ConnectionArgs, OutputFormat};

#[derive(Subcommand)]
pub(crate) enum DbAction {
    Create {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Read {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Update {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        data: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Delete {
        #[arg(short, long)]
        partition: u16,
        #[arg(short, long)]
        entity: String,
        #[arg(short, long)]
        id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

#[derive(Subcommand)]
pub(crate) enum BackupAction {
    Create {
        #[arg(short, long, default_value = "backup")]
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
        name: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
pub(crate) enum SchemaAction {
    Set {
        entity: String,
        #[arg(short, long)]
        file: PathBuf,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    Get {
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Subcommand)]
pub(crate) enum ConstraintAction {
    Add {
        entity: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        unique: Option<String>,
        #[arg(long)]
        fk: Option<String>,
        #[arg(long)]
        not_null: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    List {
        entity: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
}
