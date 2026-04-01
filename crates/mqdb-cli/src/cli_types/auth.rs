// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use clap::{Args, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

use super::base::JwtAlgorithmArg;

#[derive(Args)]
pub(crate) struct AuthArgs {
    #[arg(long, env = "MQDB_PASSWD_FILE", help = "Path to password file")]
    pub(crate) passwd: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_PASSWD", hide = true)]
    pub(crate) passwd_data: Option<String>,
    #[arg(long, env = "MQDB_ACL_FILE", help = "Path to ACL file")]
    pub(crate) acl: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_ACL", hide = true)]
    pub(crate) acl_data: Option<String>,
    #[cfg(feature = "dev-insecure")]
    #[arg(long, help = "Allow anonymous connections (dev only)")]
    pub(crate) anonymous: bool,
    #[arg(
        long,
        env = "MQDB_SCRAM_FILE",
        help = "Path to SCRAM-SHA-256 credentials file"
    )]
    pub(crate) scram_file: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_SCRAM", hide = true)]
    pub(crate) scram_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_JWT_ALGORITHM",
        help = "JWT algorithm: hs256, rs256, es256"
    )]
    pub(crate) jwt_algorithm: Option<JwtAlgorithmArg>,
    #[arg(
        long,
        env = "MQDB_JWT_KEY_FILE",
        requires = "jwt_algorithm",
        help = "Path to JWT secret/key file"
    )]
    pub(crate) jwt_key: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_JWT_KEY", hide = true)]
    pub(crate) jwt_key_data: Option<String>,
    #[arg(long, env = "MQDB_JWT_ISSUER", help = "JWT issuer claim")]
    pub(crate) jwt_issuer: Option<String>,
    #[arg(long, env = "MQDB_JWT_AUDIENCE", help = "JWT audience claim")]
    pub(crate) jwt_audience: Option<String>,
    #[arg(
        long,
        env = "MQDB_JWT_CLOCK_SKEW",
        default_value = "60",
        help = "JWT clock skew tolerance in seconds"
    )]
    pub(crate) jwt_clock_skew: u64,
    #[arg(long, env = "MQDB_FEDERATED_JWT_CONFIG_FILE", conflicts_with_all = ["jwt_algorithm"], help = "Path to federated JWT config JSON")]
    pub(crate) federated_jwt_config: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_FEDERATED_JWT_CONFIG", hide = true)]
    pub(crate) federated_jwt_config_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_CERT_AUTH_FILE",
        help = "Path to certificate auth file"
    )]
    pub(crate) cert_auth_file: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_CERT_AUTH", hide = true)]
    pub(crate) cert_auth_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_NO_RATE_LIMIT",
        help = "Disable authentication rate limiting"
    )]
    pub(crate) no_rate_limit: bool,
    #[arg(
        long,
        env = "MQDB_RATE_LIMIT_MAX_ATTEMPTS",
        default_value = "5",
        help = "Rate limit max failed attempts"
    )]
    pub(crate) rate_limit_max_attempts: u32,
    #[arg(
        long,
        env = "MQDB_RATE_LIMIT_WINDOW_SECS",
        default_value = "60",
        help = "Rate limit window in seconds"
    )]
    pub(crate) rate_limit_window_secs: u64,
    #[arg(
        long,
        env = "MQDB_RATE_LIMIT_LOCKOUT_SECS",
        default_value = "300",
        help = "Rate limit lockout duration in seconds"
    )]
    pub(crate) rate_limit_lockout_secs: u64,
    #[arg(
        long,
        env = "MQDB_ADMIN_USERS",
        value_delimiter = ',',
        help = "Comma-separated list of admin usernames"
    )]
    pub(crate) admin_users: Vec<String>,
}

#[derive(Args, Clone)]
pub(crate) struct OAuthArgs {
    #[arg(
        long,
        env = "MQDB_HTTP_BIND",
        help = "HTTP server bind address for OAuth (e.g. 0.0.0.0:8081)"
    )]
    pub(crate) http_bind: Option<SocketAddr>,
    #[arg(
        long,
        env = "MQDB_OAUTH_CLIENT_SECRET_FILE",
        help = "Path to file containing Google OAuth client secret"
    )]
    pub(crate) oauth_client_secret: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_OAUTH_CLIENT_SECRET", hide = true)]
    pub(crate) oauth_client_secret_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_OAUTH_REDIRECT_URI",
        help = "OAuth redirect URI (default: http://localhost:{http_port}/oauth/callback)"
    )]
    pub(crate) oauth_redirect_uri: Option<String>,
    #[arg(
        long,
        env = "MQDB_OAUTH_FRONTEND_REDIRECT",
        help = "URI to redirect browser after OAuth completes"
    )]
    pub(crate) oauth_frontend_redirect: Option<String>,
    #[arg(
        long,
        env = "MQDB_TICKET_EXPIRY_SECS",
        default_value = "30",
        help = "Ticket JWT expiry in seconds (default: 30)"
    )]
    pub(crate) ticket_expiry_secs: u64,
    #[arg(
        long,
        env = "MQDB_COOKIE_SECURE",
        help = "Set Secure flag on session cookies (requires HTTPS)"
    )]
    pub(crate) cookie_secure: bool,
    #[arg(
        long,
        env = "MQDB_CORS_ORIGIN",
        help = "CORS allowed origin for auth endpoints (e.g. http://localhost:8000)"
    )]
    pub(crate) cors_origin: Option<String>,
    #[arg(
        long,
        env = "MQDB_TICKET_RATE_LIMIT",
        default_value = "10",
        help = "Max ticket requests per minute per IP"
    )]
    pub(crate) ticket_rate_limit: u32,
    #[arg(
        long,
        env = "MQDB_TRUST_PROXY",
        help = "Trust X-Forwarded-For header for client IP (enable when behind a reverse proxy)"
    )]
    pub(crate) trust_proxy: bool,
    #[arg(
        long,
        env = "MQDB_IDENTITY_KEY_FILE",
        help = "Path to 32-byte identity encryption key file (auto-generated if omitted)"
    )]
    pub(crate) identity_key_file: Option<PathBuf>,
    #[arg(long = "", env = "MQDB_IDENTITY_KEY", hide = true)]
    pub(crate) identity_key_data: Option<String>,
    #[arg(
        long,
        env = "MQDB_EMAIL_AUTH",
        help = "Enable email/password registration and login"
    )]
    pub(crate) email_auth: bool,
}

#[derive(Subcommand)]
pub(crate) enum AclAction {
    #[command(about = "Add a user ACL rule")]
    Add {
        #[arg(help = "Username")]
        username: String,
        #[arg(help = "MQTT topic pattern")]
        topic: String,
        #[arg(help = "Permission: read, write, readwrite, or deny")]
        permission: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Remove user ACL rules")]
    Remove {
        #[arg(help = "Username")]
        username: String,
        #[arg(help = "MQTT topic pattern (removes all rules if omitted)")]
        topic: Option<String>,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Add a role rule")]
    RoleAdd {
        #[arg(help = "Role name")]
        role_name: String,
        #[arg(help = "MQTT topic pattern")]
        topic: String,
        #[arg(help = "Permission: read, write, readwrite, or deny")]
        permission: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Remove a role or role rule")]
    RoleRemove {
        #[arg(help = "Role name")]
        role_name: String,
        #[arg(help = "MQTT topic pattern (removes entire role if omitted)")]
        topic: Option<String>,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "List roles")]
    RoleList {
        #[arg(help = "Role name (lists all roles if omitted)")]
        role_name: Option<String>,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Assign a role to a user")]
    Assign {
        #[arg(help = "Username")]
        username: String,
        #[arg(help = "Role name")]
        role: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Unassign a role from a user")]
    Unassign {
        #[arg(help = "Username")]
        username: String,
        #[arg(help = "Role name")]
        role: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "List ACL rules")]
    List {
        #[arg(long, help = "Filter by username")]
        user: Option<String>,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "Check if a user can perform an action on a topic")]
    Check {
        #[arg(help = "Username")]
        username: String,
        #[arg(help = "MQTT topic pattern")]
        topic: String,
        #[arg(help = "Action to check: read or write")]
        action: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
    #[command(about = "List roles assigned to a user")]
    UserRoles {
        #[arg(help = "Username")]
        username: String,
        #[arg(short, long, help = "ACL file path")]
        file: PathBuf,
    },
}
