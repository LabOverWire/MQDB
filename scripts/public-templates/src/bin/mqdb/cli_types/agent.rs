// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::Subcommand;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::auth::{AuthArgs, OAuthArgs};
use super::base::{ConnectionArgs, DurabilityArg};

#[derive(Subcommand)]
pub(crate) enum AgentAction {
    #[command(about = "Start a standalone MQTT broker agent")]
    Start {
        #[arg(
            long,
            default_value = "127.0.0.1:1883",
            help = "Address to bind MQTT listener"
        )]
        bind: SocketAddr,
        #[arg(long, help = "Path to database directory")]
        db: PathBuf,
        #[command(flatten)]
        auth: Box<AuthArgs>,
        #[arg(
            long,
            default_value = "periodic",
            help = "Durability mode: immediate (fsync every write), periodic (fsync periodically), none (no fsync)"
        )]
        durability: DurabilityArg,
        #[arg(
            long,
            default_value = "10",
            help = "Fsync interval in ms when using periodic durability"
        )]
        durability_ms: u64,
        #[arg(long, help = "Path to QUIC/TLS certificate file (PEM format)")]
        quic_cert: Option<PathBuf>,
        #[arg(long, help = "Path to QUIC/TLS private key file (PEM format)")]
        quic_key: Option<PathBuf>,
        #[arg(long, help = "WebSocket bind address (e.g. 0.0.0.0:8080)")]
        ws_bind: Option<SocketAddr>,
        #[command(flatten)]
        oauth: Box<OAuthArgs>,
        #[arg(
            long,
            help = "Ownership config: entity=field pairs (e.g. diagrams=userId)"
        )]
        ownership: Option<String>,
        #[arg(long, help = "Scope events by entity field (e.g. diagrams=diagramId)")]
        event_scope: Option<String>,
        #[arg(long, help = "Path to file containing encryption passphrase")]
        passphrase_file: Option<PathBuf>,
    },
    #[command(about = "Check broker connectivity status")]
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}
