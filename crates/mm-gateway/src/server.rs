use std::sync::Arc;

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};

use crate::rest::{self, ApiState, RateLimitConfig, RateLimiter};
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

    /// Allowed CORS origins. If empty, falls back to wildcard (`*`).
    #[serde(default)]
    pub cors_origins: Vec<String>,

    /// Optional API bearer token. If set, all REST endpoints require
    /// `Authorization: Bearer <token>` header. If empty/None, auth is disabled.
    /// Health and WebSocket endpoints are exempt from authentication.
    #[serde(default)]
    pub api_token: Option<String>,

    /// Maximum API requests per second (global rate limit).
    /// `None` or `0` disables rate limiting. Defaults to 100.
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: Option<u32>,
}

fn default_rate_limit_rps() -> Option<u32> {
    Some(100)
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            cors_origins: vec![
                "http://127.0.0.1:3000".to_string(),
                "http://localhost:3000".to_string(),
            ],
            api_token: None,
            rate_limit_rps: Some(100),
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
    pub fn new(config: GatewayConfig, mut state: Arc<ApiState>) -> Self {
        // Configure rate limiting from the gateway config before the Arc is
        // shared with handler tasks.  `Arc::get_mut` is safe here because this
        // is the only reference at construction time.
        if let Some(rps) = config.rate_limit_rps {
            if rps > 0 {
                if let Some(inner) = Arc::get_mut(&mut state) {
                    let limiter = RateLimiter::from_config(&RateLimitConfig {
                        requests_per_second: rps,
                        burst_size: rps / 2, // default burst = half RPS
                    });
                    inner.rate_limit = Some(Arc::new(limiter));
                    info!(rps = rps, burst = rps / 2, "API rate limiting enabled");
                }
            }
        }

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
        // CORS — restrict to configured origins; fall back to wildcard if empty.
        let cors = if self.config.cors_origins.is_empty() {
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
        } else {
            let origins: Vec<HeaderValue> = self
                .config
                .cors_origins
                .iter()
                .filter_map(|o| o.parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods(Any)
                .allow_headers(Any)
        };

        // Tracing layer logs each request/response cycle.
        let trace_layer = TraceLayer::new_for_http();

        // Merge REST and WebSocket routers.
        let rest = rest::rest_router(self.state.clone());
        let ws = ws::ws_router(self.state.clone(), self.broadcaster.clone());

        // Layer order in axum: last added = first executed (outermost).
        // trace_layer → cors → auth_layer → handler
        // This ensures CORS preflight OPTIONS requests are handled before auth.
        if let Some(ref token) = self.config.api_token {
            let expected = format!("Bearer {}", token);
            info!("API bearer-token authentication enabled");
            let auth_layer = middleware::from_fn(move |req: Request, next: Next| {
                let expected = expected.clone();
                async move {
                    let path = req.uri().path().to_string();
                    // Skip auth for health check, metrics, and WebSocket upgrade
                    if path == "/api/v1/health" || path == "/api/v1/ws" || path == "/metrics" {
                        return next.run(req).await;
                    }
                    match req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                    {
                        Some(v) if v == expected => next.run(req).await,
                        _ => {
                            warn!(
                                path = %path,
                                method = %req.method(),
                                "Authentication failure: missing or invalid bearer token"
                            );
                            (
                                StatusCode::UNAUTHORIZED,
                                axum::Json(serde_json::json!({
                                    "status": "error",
                                    "error": "missing or invalid bearer token"
                                })),
                            )
                                .into_response()
                        }
                    }
                }
            });
            rest.merge(ws)
                .layer(auth_layer)
                .layer(cors)
                .layer(trace_layer)
        } else {
            rest.merge(ws).layer(cors).layer(trace_layer)
        }
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
