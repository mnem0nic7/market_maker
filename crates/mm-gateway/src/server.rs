use std::sync::Arc;

use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::info;

use crate::rest::{self, ApiState};
use crate::ws::{self, DashboardBroadcaster};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the HTTP / WebSocket gateway server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Bind address (e.g. "0.0.0.0" or "127.0.0.1").
    pub host: String,

    /// TCP port to listen on.
    pub port: u16,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

impl GatewayConfig {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

// ---------------------------------------------------------------------------
// Gateway Server
// ---------------------------------------------------------------------------

/// The top-level gateway server that composes the REST API and WebSocket
/// endpoint, applies shared middleware, and manages graceful shutdown.
pub struct GatewayServer {
    config: GatewayConfig,
    state: Arc<ApiState>,
    broadcaster: Arc<DashboardBroadcaster>,
}

impl GatewayServer {
    /// Create a new server instance.
    ///
    /// The `DashboardBroadcaster` is created internally — callers can obtain
    /// its sender via [`GatewayServer::broadcaster`].
    pub fn new(config: GatewayConfig, state: Arc<ApiState>) -> Self {
        let broadcaster = DashboardBroadcaster::new(
            4096, // mpsc buffer
            1024, // broadcast capacity
        );

        Self {
            config,
            state,
            broadcaster,
        }
    }

    /// Create a server with an externally constructed broadcaster.
    pub fn with_broadcaster(
        config: GatewayConfig,
        state: Arc<ApiState>,
        broadcaster: Arc<DashboardBroadcaster>,
    ) -> Self {
        Self {
            config,
            state,
            broadcaster,
        }
    }

    /// Returns a reference to the broadcaster so that other system components
    /// can push events for live dashboard delivery.
    pub fn broadcaster(&self) -> &Arc<DashboardBroadcaster> {
        &self.broadcaster
    }

    /// Build the composed [`Router`] with all middleware applied.
    pub fn build_router(&self) -> Router {
        // CORS — permissive for dashboard dev; tighten in production.
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        // Tracing layer logs each request/response cycle.
        let trace_layer = TraceLayer::new_for_http();

        // Merge REST and WebSocket routers.
        let rest = rest::rest_router(self.state.clone());
        let ws = ws::ws_router(self.state.clone(), self.broadcaster.clone());

        rest.merge(ws)
            .layer(cors)
            .layer(trace_layer)
    }

    /// Start listening and serving requests.
    ///
    /// This method blocks until a SIGINT or SIGTERM signal is received, at
    /// which point it performs a graceful shutdown (allowing in-flight
    /// requests to complete).
    pub async fn start(self) -> anyhow::Result<()> {
        let addr = self.config.bind_address();
        let router = self.build_router();

        let listener = TcpListener::bind(&addr).await?;
        info!(address = %addr, "gateway server listening");

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        info!("gateway server shut down gracefully");
        Ok(())
    }
}

/// Stand-alone convenience function: build everything from a config and state,
/// then start serving.
pub async fn start(config: GatewayConfig, state: Arc<ApiState>) -> anyhow::Result<()> {
    let server = GatewayServer::new(config, state);
    server.start().await
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

/// Wait for either SIGINT (Ctrl-C) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received SIGINT, initiating graceful shutdown");
        }
        _ = terminate => {
            info!("received SIGTERM, initiating graceful shutdown");
        }
    }
}
