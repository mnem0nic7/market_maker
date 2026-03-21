use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};
use utoipa::OpenApi;

use crate::rest::{self, ApiDoc, ApiState, RateLimitConfig, RateLimiter};
use crate::ws::{self, DashboardBroadcaster};

// ---------------------------------------------------------------------------
// Bind retry constants
// ---------------------------------------------------------------------------

/// Maximum number of TCP bind retries before giving up.
pub const BIND_MAX_RETRIES: u32 = 5;

/// Base delay in milliseconds for exponential backoff between bind retries.
pub const BIND_BASE_DELAY_MS: u64 = 500;

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
    ready_tx: Option<oneshot::Sender<()>>,
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

        if let Some(inner) = Arc::get_mut(&mut state) {
            inner.dashboard_event_tx = Some(broadcaster.sender());
        }

        Self {
            config,
            state,
            broadcaster,
            ready_tx: None,
        }
    }

    /// Create a server with an externally constructed broadcaster.
    pub fn with_broadcaster(
        config: GatewayConfig,
        mut state: Arc<ApiState>,
        broadcaster: Arc<DashboardBroadcaster>,
    ) -> Self {
        if let Some(inner) = Arc::get_mut(&mut state) {
            inner.dashboard_event_tx = Some(broadcaster.sender());
        }

        Self {
            config,
            state,
            broadcaster,
            ready_tx: None,
        }
    }

    /// Attach a oneshot sender that will be signalled once the server has
    /// successfully bound to its TCP port and is ready to accept connections.
    pub fn with_ready_signal(mut self, tx: oneshot::Sender<()>) -> Self {
        self.ready_tx = Some(tx);
        self
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

        // OpenAPI spec and Swagger UI — served at /api/docs and /api/openapi.json.
        let openapi_routes = Router::new()
            .route("/api/openapi.json", get(serve_openapi_spec))
            .route("/api/docs", get(serve_swagger_ui));

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
                    // Skip auth for health check, metrics, WebSocket upgrade, and API docs
                    if path == "/api/v1/health"
                        || path == "/api/v1/ws"
                        || path == "/metrics"
                        || path.starts_with("/api/docs")
                        || path == "/api/openapi.json"
                    {
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
                .merge(openapi_routes)
                .layer(auth_layer)
                .layer(cors)
                .layer(trace_layer)
        } else {
            rest.merge(ws)
                .merge(openapi_routes)
                .layer(cors)
                .layer(trace_layer)
        }
    }

    /// Start listening and serving requests.
    ///
    /// TCP binding is retried with exponential backoff up to
    /// [`BIND_MAX_RETRIES`] times. Once bound, the optional readiness signal
    /// (see [`with_ready_signal`](Self::with_ready_signal)) is fired so that
    /// callers know the server is accepting connections.
    ///
    /// This method blocks until a SIGINT or SIGTERM signal is received, at
    /// which point it performs a graceful shutdown (allowing in-flight
    /// requests to complete).
    pub async fn start(mut self) -> anyhow::Result<()> {
        let addr = self.config.bind_address();
        let router = self.build_router();

        // Retry TCP bind with exponential backoff.
        let listener = 'bind: {
            for attempt in 0..=BIND_MAX_RETRIES {
                match TcpListener::bind(&addr).await {
                    Ok(l) => {
                        if attempt > 0 {
                            info!(attempt, "gateway bound successfully after retry");
                        }
                        break 'bind l;
                    }
                    Err(e) if attempt == BIND_MAX_RETRIES => return Err(e.into()),
                    Err(e) => {
                        let delay = (BIND_BASE_DELAY_MS * 2u64.pow(attempt)).min(10_000);
                        warn!(
                            attempt,
                            delay_ms = delay,
                            addr = %addr,
                            error = %e,
                            "gateway bind failed, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
            unreachable!()
        };

        info!(address = %addr, "gateway server listening");

        // Signal readiness to the caller (e.g. the main trading loop).
        if let Some(tx) = self.ready_tx.take() {
            let _ = tx.send(());
        }

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
// OpenAPI spec and Swagger UI handlers
// ---------------------------------------------------------------------------

/// Serve the machine-readable OpenAPI JSON specification.
async fn serve_openapi_spec() -> impl IntoResponse {
    let spec = ApiDoc::openapi().to_json().unwrap_or_else(|e| {
        serde_json::json!({"error": format!("failed to generate OpenAPI spec: {e}")}).to_string()
    });
    (StatusCode::OK, [("content-type", "application/json")], spec)
}

/// Serve an embedded Swagger UI page that loads the spec from /api/openapi.json.
///
/// This uses the official Swagger UI from a public CDN so that no additional
/// binary dependencies (utoipa-swagger-ui) are required.
async fn serve_swagger_ui() -> impl IntoResponse {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Market Maker API — Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: '/api/openapi.json',
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: 'BaseLayout'
    });
  </script>
</body>
</html>"#;

    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest::ApiState;

    #[test]
    fn test_bind_retry_constants() {
        // Sanity-check the retry constants are reasonable.
        assert!(BIND_MAX_RETRIES >= 1, "must retry at least once");
        assert!(BIND_MAX_RETRIES <= 10, "excessive retries");
        assert!(BIND_BASE_DELAY_MS >= 100, "base delay too short");
        assert!(BIND_BASE_DELAY_MS <= 5000, "base delay too long");
    }

    #[test]
    fn test_bind_retry_backoff() {
        // Verify the exponential backoff sequence with the 10s cap.
        let delays: Vec<u64> = (0..=BIND_MAX_RETRIES)
            .map(|attempt| (BIND_BASE_DELAY_MS * 2u64.pow(attempt)).min(10_000))
            .collect();

        // With BIND_BASE_DELAY_MS=500: 500, 1000, 2000, 4000, 8000, 10000
        assert_eq!(delays[0], 500);
        assert_eq!(delays[1], 1000);
        assert_eq!(delays[2], 2000);
        assert_eq!(delays[3], 4000);
        assert_eq!(delays[4], 8000);

        // All delays must be capped at 10_000.
        for d in &delays {
            assert!(*d <= 10_000, "delay {d} exceeds 10s cap");
        }
    }

    #[tokio::test]
    async fn test_gateway_ready_signal() {
        let config = GatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 0, // OS-assigned port avoids conflicts
            cors_origins: vec![],
            api_token: None,
            rate_limit_rps: None,
        };

        let state = ApiState::new();
        let broadcaster = DashboardBroadcaster::new(64, 16);

        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let server =
            GatewayServer::with_broadcaster(config, state, broadcaster).with_ready_signal(ready_tx);

        // Spawn the server; it will bind to an ephemeral port and signal ready.
        let handle = tokio::spawn(async move {
            let _ = server.start().await;
        });

        // The readiness signal should arrive well within 2 seconds.
        let result = tokio::time::timeout(Duration::from_secs(2), ready_rx).await;
        assert!(result.is_ok(), "timed out waiting for readiness signal");
        assert!(
            result.unwrap().is_ok(),
            "readiness channel closed without signal"
        );

        // Clean up — abort the server task (it's blocking on shutdown_signal).
        handle.abort();
    }
}
