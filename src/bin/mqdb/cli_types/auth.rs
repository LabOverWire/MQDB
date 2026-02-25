// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::{Args, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

use super::base::JwtAlgorithmArg;

#[derive(Args)]
pub(crate) struct AuthArgs {
    #[arg(long, help = "Path to password file")]
    pub(crate) passwd: Option<PathBuf>,
    #[arg(long, help = "Path to ACL file")]
    pub(crate) acl: Option<PathBuf>,
    #[cfg(feature = "dev-insecure")]
    #[arg(long, help = "Allow anonymous connections (dev only)")]
    pub(crate) anonymous: bool,
    #[arg(long, help = "Path to SCRAM-SHA-256 credentials file")]
    pub(crate) scram_file: Option<PathBuf>,
    #[arg(long, help = "JWT algorithm: hs256, rs256, es256")]
    pub(crate) jwt_algorithm: Option<JwtAlgorithmArg>,
    #[arg(long, requires = "jwt_algorithm", help = "Path to JWT secret/key file")]
    pub(crate) jwt_key: Option<PathBuf>,
    #[arg(long, help = "JWT issuer claim")]
    pub(crate) jwt_issuer: Option<String>,
    #[arg(long, help = "JWT audience claim")]
    pub(crate) jwt_audience: Option<String>,
    #[arg(
        long,
        default_value = "60",
        help = "JWT clock skew tolerance in seconds"
    )]
    pub(crate) jwt_clock_skew: u64,
    #[arg(long, conflicts_with_all = ["jwt_algorithm"], help = "Path to federated JWT config JSON")]
    pub(crate) federated_jwt_config: Option<PathBuf>,
    #[arg(long, help = "Path to certificate auth file")]
    pub(crate) cert_auth_file: Option<PathBuf>,
    #[arg(long, help = "Disable authentication rate limiting")]
    pub(crate) no_rate_limit: bool,
    #[arg(long, default_value = "5", help = "Rate limit max failed attempts")]
    pub(crate) rate_limit_max_attempts: u32,
    #[arg(long, default_value = "60", help = "Rate limit window in seconds")]
    pub(crate) rate_limit_window_secs: u64,
    #[arg(
        long,
        default_value = "300",
        help = "Rate limit lockout duration in seconds"
    )]
    pub(crate) rate_limit_lockout_secs: u64,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Comma-separated list of admin usernames"
    )]
    pub(crate) admin_users: Vec<String>,
}

#[derive(Args, Clone)]
pub(crate) struct OAuthArgs {
    #[arg(long, help = "HTTP server bind address for OAuth (e.g. 0.0.0.0:8081)")]
    pub(crate) http_bind: Option<SocketAddr>,
    #[arg(long, help = "Path to file containing Google OAuth client secret")]
    pub(crate) oauth_client_secret: Option<PathBuf>,
    #[arg(
        long,
        help = "OAuth redirect URI (default: http://localhost:{http_port}/oauth/callback)"
    )]
    pub(crate) oauth_redirect_uri: Option<String>,
    #[arg(long, help = "URI to redirect browser after OAuth completes")]
    pub(crate) oauth_frontend_redirect: Option<String>,
    #[arg(
        long,
        default_value = "30",
        help = "Ticket JWT expiry in seconds (default: 30)"
    )]
    pub(crate) ticket_expiry_secs: u64,
    #[arg(long, help = "Set Secure flag on session cookies (requires HTTPS)")]
    pub(crate) cookie_secure: bool,
    #[arg(
        long,
        help = "CORS allowed origin for auth endpoints (e.g. http://localhost:8000)"
    )]
    pub(crate) cors_origin: Option<String>,
    #[arg(
        long,
        default_value = "10",
        help = "Max ticket requests per minute per IP"
    )]
    pub(crate) ticket_rate_limit: u32,
    #[arg(
        long,
        help = "Trust X-Forwarded-For header for client IP (enable when behind a reverse proxy)"
    )]
    pub(crate) trust_proxy: bool,
}

#[derive(Subcommand)]
pub(crate) enum AclAction {
    #[command(about = "Add a user ACL rule")]
    Add {
        username: String,
        topic: String,
        permission: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Remove user ACL rules")]
    Remove {
        username: String,
        topic: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Add a role rule")]
    RoleAdd {
        role_name: String,
        topic: String,
        permission: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Remove a role or role rule")]
    RoleRemove {
        role_name: String,
        topic: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List roles")]
    RoleList {
        role_name: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Assign a role to a user")]
    Assign {
        username: String,
        role: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Unassign a role from a user")]
    Unassign {
        username: String,
        role: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List ACL rules")]
    List {
        #[arg(long)]
        user: Option<String>,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "Check if a user can perform an action on a topic")]
    Check {
        username: String,
        topic: String,
        action: String,
        #[arg(short, long)]
        file: PathBuf,
    },
    #[command(about = "List roles assigned to a user")]
    UserRoles {
        username: String,
        #[arg(short, long)]
        file: PathBuf,
    },
}
