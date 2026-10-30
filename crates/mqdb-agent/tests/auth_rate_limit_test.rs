// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::next_test_port;
use mqdb_agent::{Database, MqdbAgent};
use mqtt5::MqttError;
use mqtt5::broker::PasswordAuthProvider;
use mqtt5::broker::config::{JwtAlgorithm, JwtConfig, RateLimitConfig};
use mqtt5::client::MqttClient;
use mqtt5::protocol::v5::reason_codes::ReasonCode;
use mqtt5::types::ConnectOptions;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

const MAX_ATTEMPTS: u32 = 3;
const USER: &str = "paysvc";

struct TestAgent {
    dir: TempDir,
    port: u16,
    password: String,
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl TestAgent {
    async fn start(jwt: bool) -> Self {
        let dir = TempDir::new().expect("agent dir");
        let password_file = dir.path().join("passwd");
        let password = uuid::Uuid::new_v4().to_string();
        let hash = PasswordAuthProvider::hash_password(&password).expect("hash password");
        std::fs::write(&password_file, format!("{USER}:{hash}\n")).expect("write password file");
        let db = Database::open_without_background_tasks(dir.path().join("db"))
            .await
            .expect("open database");
        let port = next_test_port();
        let agent = MqdbAgent::new(db)
            .with_bind_address(SocketAddr::from(([127, 0, 0, 1], port)))
            .with_password_file(password_file)
            .with_rate_limit_config(RateLimitConfig {
                enabled: true,
                max_attempts: MAX_ATTEMPTS,
                window_secs: 60,
                lockout_secs: 300,
            });
        let agent = if jwt {
            let key_file = dir.path().join("jwt.key");
            std::fs::write(&key_file, "rate-limit-test-secret").expect("write jwt key");
            agent.with_jwt_config(JwtConfig {
                algorithm: JwtAlgorithm::HS256,
                secret_or_key_file: key_file,
                ..JwtConfig::default()
            })
        } else {
            agent
        };
        let (handle, mut ready_rx, shutdown) = agent.start().await.expect("the agent starts");
        tokio::time::timeout(Duration::from_secs(10), ready_rx.wait_for(|ready| *ready))
            .await
            .expect("the agent becomes ready in time")
            .expect("the agent signals readiness");
        Self {
            dir,
            port,
            password,
            handle,
            shutdown,
        }
    }

    async fn login(&self, password: &str) -> Result<(), MqttError> {
        let client_id = format!("rate-limit-{}", uuid::Uuid::new_v4());
        let client = MqttClient::new(client_id.clone());
        let options = ConnectOptions::new(client_id).with_credentials(USER, password);
        Box::pin(client.connect_with_options(&format!("127.0.0.1:{}", self.port), options)).await?;
        client.disconnect().await
    }

    async fn stop(self) {
        self.shutdown
            .send(())
            .expect("the agent listens for shutdown until it stops");
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("the agent should stop after shutdown")
            .expect("the agent task should not panic");
        drop(self.dir);
    }
}

fn is_rate_limited(result: &Result<(), MqttError>) -> bool {
    matches!(
        result,
        Err(MqttError::ConnectionRefused(
            ReasonCode::ConnectionRateExceeded
        ))
    )
}

async fn wrong_passwords_lock_out_the_user(jwt: bool) {
    let agent = TestAgent::start(jwt).await;
    for attempt in 1..=MAX_ATTEMPTS {
        let result = agent.login(&uuid::Uuid::new_v4().to_string()).await;
        assert!(
            result.is_err() && !is_rate_limited(&result),
            "wrong password {attempt} must be refused as a bad login: {result:?}"
        );
    }
    let result = agent.login(&agent.password).await;
    assert!(
        is_rate_limited(&result),
        "after {MAX_ATTEMPTS} wrong passwords the next login must be rate limited: {result:?}"
    );
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_password_logins_are_not_rate_limited_with_jwt_enabled() {
    let agent = TestAgent::start(true).await;
    for attempt in 1..=MAX_ATTEMPTS * 2 {
        let result = agent.login(&agent.password).await;
        assert!(
            result.is_ok(),
            "valid password login {attempt} must succeed: {result:?}"
        );
    }
    agent.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_passwords_are_rate_limited_with_jwt_enabled() {
    wrong_passwords_lock_out_the_user(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_passwords_are_rate_limited_with_password_auth_only() {
    wrong_passwords_lock_out_the_user(false).await;
}
