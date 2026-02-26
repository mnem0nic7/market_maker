//! Standalone gateway service binary.
//!
//! Starts the HTTP/WebSocket API server with empty state. In production this
//! service would receive state updates via NATS; for now it serves as a
//! standalone REST/WS API server that can be used for testing and development.

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

use mm_gateway::{ApiState, GatewayConfig, GatewayServer};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AppConfig {
    /// Host address to bind to (e.g. "0.0.0.0" or "127.0.0.1").
    #[serde(default = "default_host")]
    host: String,
    /// Port to listen on.
    #[serde(default = "default_port")]
    port: u16,
    /// Optional API bearer token for authentication.
    /// Can also be set via GATEWAY_API_TOKEN environment variable.
    #[serde(default)]
    api_token: Option<String>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            api_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing (structured JSON logging, respects RUST_LOG).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .json()
        .init();

    info!("gateway service starting");

    // Load configuration.
    let cfg = load_config().context("failed to load configuration")?;
    info!(host = %cfg.host, port = cfg.port, "configuration loaded");

    // Build the gateway config for the server.
    let gateway_config = GatewayConfig {
        host: cfg.host,
        port: cfg.port,
        cors_origins: vec![
            "http://127.0.0.1:3000".to_string(),
            "http://localhost:3000".to_string(),
        ],
        api_token: cfg.api_token,
    };

    // Create the shared API state with defaults (empty collections).
    //
    // In a full deployment this state would be populated via NATS subscriptions
    // that relay updates from the trading-core engine. For standalone operation
    // the REST and WebSocket endpoints are fully functional but return empty
    // data until state is populated (e.g. via the API itself or test scripts).
    let state = ApiState::new();

    info!(
        address = %gateway_config.bind_address(),
        "starting gateway server"
    );

    // Build and start the server. This blocks until a shutdown signal is
    // received (SIGINT / SIGTERM), at which point it performs a graceful
    // shutdown allowing in-flight requests to complete.
    let server = GatewayServer::new(gateway_config, state);
    server
        .start()
        .await
        .context("gateway server error")?;

    info!("gateway service stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Configuration loading
// ---------------------------------------------------------------------------

fn load_config() -> Result<AppConfig> {
    let settings = config::Config::builder()
        // Defaults
        .set_default("host", "127.0.0.1")?
        .set_default("port", 8080_i64)?
        // Config files (optional)
        .add_source(
            config::File::with_name("config/gateway")
                .required(false),
        )
        .add_source(
            config::File::with_name("config/default")
                .required(false),
        )
        // Environment variables (prefixed with GATEWAY_)
        .add_source(
            config::Environment::with_prefix("GATEWAY")
                .separator("_"),
        )
        .build()
        .context("failed to build config")?;

    settings
        .try_deserialize::<AppConfig>()
        .context("failed to deserialize config")
}
