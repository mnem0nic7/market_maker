use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::Utc;
use mm_core::{Exchange, MetricsRegistry, Order, OrderBook, PortfolioRisk, Position, StrategyConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// API Response wrapper
// ---------------------------------------------------------------------------

/// Consistent JSON envelope returned by every endpoint.
#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Json<ApiResponse<T>> {
        Json(ApiResponse {
            status: "ok",
            data: Some(data),
            error: None,
        })
    }
}

impl ApiResponse<()> {
    pub fn error(message: impl Into<String>) -> (StatusCode, Json<ApiResponse<()>>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                status: "error",
                data: None,
                error: Some(message.into()),
            }),
        )
    }

    pub fn not_found(message: impl Into<String>) -> (StatusCode, Json<ApiResponse<()>>) {
        (
            StatusCode::NOT_FOUND,
            Json(ApiResponse {
                status: "error",
                data: None,
                error: Some(message.into()),
            }),
        )
    }

    pub fn ok_message(message: impl Into<String>) -> Json<ApiResponse<String>> {
        Json(ApiResponse {
            status: "ok",
            data: Some(message.into()),
            error: None,
        })
    }

    pub fn conflict(message: impl Into<String>) -> (StatusCode, Json<ApiResponse<()>>) {
        (
            StatusCode::CONFLICT,
            Json(ApiResponse {
                status: "error",
                data: None,
                error: Some(message.into()),
            }),
        )
    }

    pub fn bad_request(message: impl Into<String>) -> (StatusCode, Json<ApiResponse<()>>) {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                status: "error",
                data: None,
                error: Some(message.into()),
            }),
        )
    }
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// Shared application state accessible from all API handlers.
///
/// In a full deployment, these fields hold references to the actual system
/// components (OMS, risk engine, market-data manager, etc.).  During early
/// development or testing the in-memory collections are populated directly.
pub struct ApiState {
    /// All tracked positions keyed by (exchange, symbol).
    pub positions: RwLock<Vec<Position>>,

    /// Open (non-terminal) orders.
    pub orders: RwLock<Vec<Order>>,

    /// Latest portfolio-level risk summary.
    pub portfolio_risk: RwLock<Option<PortfolioRisk>>,

    /// Registered strategy configurations and their running status.
    pub strategies: RwLock<Vec<StrategyStatus>>,

    /// Latest order book snapshots keyed by (exchange, symbol).
    pub order_books: RwLock<std::collections::HashMap<(Exchange, String), OrderBook>>,

    /// Whether the global kill-switch is currently active.
    pub kill_switch_active: Mutex<bool>,

    /// Optional Prometheus metrics registry.
    pub metrics: Option<Arc<MetricsRegistry>>,
}

/// Wraps a strategy config with its runtime status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyStatus {
    pub config: StrategyConfig,
    pub running: bool,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub stopped_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ApiState {
    /// Create empty state suitable for testing or bootstrapping.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            positions: RwLock::new(Vec::new()),
            orders: RwLock::new(Vec::new()),
            portfolio_risk: RwLock::new(None),
            strategies: RwLock::new(Vec::new()),
            order_books: RwLock::new(std::collections::HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: None,
        })
    }

    /// Create state with a Prometheus metrics registry attached.
    pub fn with_metrics(metrics: Arc<MetricsRegistry>) -> Arc<Self> {
        Arc::new(Self {
            positions: RwLock::new(Vec::new()),
            orders: RwLock::new(Vec::new()),
            portfolio_risk: RwLock::new(None),
            strategies: RwLock::new(Vec::new()),
            order_books: RwLock::new(std::collections::HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: Some(metrics),
        })
    }
}

impl Default for ApiState {
    fn default() -> Self {
        Self {
            positions: RwLock::new(Vec::new()),
            orders: RwLock::new(Vec::new()),
            portfolio_risk: RwLock::new(None),
            strategies: RwLock::new(Vec::new()),
            order_books: RwLock::new(std::collections::HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Router construction
// ---------------------------------------------------------------------------

/// Build the full REST API router with all v1 routes.
pub fn rest_router(state: Arc<ApiState>) -> Router {
    Router::new()
        // Prometheus metrics
        .route("/metrics", get(prometheus_metrics))
        // Health
        .route("/api/v1/health", get(health_check))
        // Positions
        .route("/api/v1/positions", get(list_positions))
        // Orders
        .route("/api/v1/orders", get(list_orders))
        .route("/api/v1/orders/{order_id}", get(get_order))
        // Portfolio risk
        .route("/api/v1/portfolio/risk", get(get_portfolio_risk))
        // Strategies
        .route("/api/v1/strategies", get(list_strategies).post(create_strategy))
        .route("/api/v1/strategies/{id}", put(update_strategy).delete(delete_strategy))
        .route("/api/v1/strategies/{id}/start", post(start_strategy))
        .route("/api/v1/strategies/{id}/stop", post(stop_strategy))
        // Risk controls
        .route("/api/v1/risk/kill-switch", post(activate_kill_switch))
        .route("/api/v1/risk/kill-switch", delete(reset_kill_switch))
        // Market data
        .route(
            "/api/v1/market-data/{exchange}/{symbol}/orderbook",
            get(get_orderbook),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Query parameter types
// ---------------------------------------------------------------------------

/// Query parameters for filtering the order list.
#[derive(Debug, Deserialize)]
pub struct OrderQuery {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub strategy_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    timestamp: String,
    version: &'static str,
}

async fn health_check() -> impl IntoResponse {
    ApiResponse::ok(HealthResponse {
        status: "healthy",
        timestamp: Utc::now().to_rfc3339(),
        version: env!("CARGO_PKG_VERSION"),
    })
}

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

async fn prometheus_metrics(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    match &state.metrics {
        Some(metrics) => {
            let body = metrics.encode();
            (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                body,
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics not configured",
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Positions
// ---------------------------------------------------------------------------

async fn list_positions(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let positions = state.positions.read().await;
    ApiResponse::ok(positions.clone())
}

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

async fn list_orders(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<OrderQuery>,
) -> impl IntoResponse {
    let orders = state.orders.read().await;

    let filtered: Vec<Order> = orders
        .iter()
        .filter(|o| {
            if let Some(ref exch) = params.exchange {
                let order_exchange = o.exchange.to_string().to_lowercase();
                if order_exchange != exch.to_lowercase() {
                    return false;
                }
            }
            if let Some(ref sym) = params.symbol {
                if o.symbol.canonical().to_lowercase() != sym.to_lowercase() {
                    return false;
                }
            }
            if let Some(ref sid) = params.strategy_id {
                if &o.strategy_id != sid {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect();

    ApiResponse::ok(filtered)
}

async fn get_order(
    State(state): State<Arc<ApiState>>,
    Path(order_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let orders = state.orders.read().await;

    let found = orders.iter().find(|o| {
        o.client_order_id == order_id
            || o.exchange_order_id.as_deref() == Some(order_id.as_str())
    });

    match found {
        Some(order) => Ok(ApiResponse::ok(order.clone())),
        None => Err(ApiResponse::not_found(format!(
            "order not found: {order_id}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Portfolio risk
// ---------------------------------------------------------------------------

async fn get_portfolio_risk(
    State(state): State<Arc<ApiState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let risk = state.portfolio_risk.read().await;

    match risk.as_ref() {
        Some(r) => Ok(ApiResponse::ok(r.clone())),
        None => Err(ApiResponse::not_found(
            "no portfolio risk data available yet",
        )),
    }
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Valid strategy types accepted by the platform.
const VALID_STRATEGY_TYPES: &[&str] = &[
    "spread",
    "avellaneda",
    "grid",
    "twap",
    "vwap",
    "momentum",
    "mean_reversion",
    "stat_arb",
];

async fn list_strategies(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let strategies = state.strategies.read().await;
    ApiResponse::ok(strategies.clone())
}

async fn create_strategy(
    State(state): State<Arc<ApiState>>,
    Json(config): Json<StrategyConfig>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    // Validate strategy_type
    if !VALID_STRATEGY_TYPES.contains(&config.strategy_type.as_str()) {
        return Err(ApiResponse::bad_request(format!(
            "invalid strategy_type: '{}'. Must be one of: {}",
            config.strategy_type,
            VALID_STRATEGY_TYPES.join(", ")
        )));
    }

    let mut strategies = state.strategies.write().await;

    // Check for duplicate strategy_id
    if strategies
        .iter()
        .any(|s| s.config.strategy_id == config.strategy_id)
    {
        return Err(ApiResponse::conflict(format!(
            "strategy already exists: {}",
            config.strategy_id
        )));
    }

    let strategy_id = config.strategy_id.clone();
    strategies.push(StrategyStatus {
        config,
        running: false,
        started_at: None,
        stopped_at: None,
    });

    info!(strategy_id = %strategy_id, "strategy created via API");
    Ok((StatusCode::CREATED, ApiResponse::ok_message(format!("strategy {strategy_id} created"))))
}

async fn update_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(config): Json<StrategyConfig>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let mut strategies = state.strategies.write().await;

    let strategy = strategies
        .iter_mut()
        .find(|s| s.config.strategy_id == id);

    match strategy {
        Some(s) => {
            if s.running {
                return Err(ApiResponse::conflict(format!(
                    "cannot update running strategy: {id}. Stop it first."
                )));
            }
            // Preserve the original strategy_id regardless of what was sent in the body
            let mut new_config = config;
            new_config.strategy_id = id.clone();
            s.config = new_config;
            info!(strategy_id = %id, "strategy updated via API");
            Ok(ApiResponse::ok_message(format!("strategy {id} updated")))
        }
        None => Err(ApiResponse::not_found(format!(
            "strategy not found: {id}"
        ))),
    }
}

async fn delete_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let mut strategies = state.strategies.write().await;

    let pos = strategies
        .iter()
        .position(|s| s.config.strategy_id == id);

    match pos {
        Some(idx) => {
            if strategies[idx].running {
                return Err(ApiResponse::conflict(format!(
                    "cannot delete running strategy: {id}. Stop it first."
                )));
            }
            strategies.remove(idx);
            info!(strategy_id = %id, "strategy deleted via API");
            Ok(ApiResponse::ok_message(format!("strategy {id} deleted")))
        }
        None => Err(ApiResponse::not_found(format!(
            "strategy not found: {id}"
        ))),
    }
}

async fn start_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let mut strategies = state.strategies.write().await;

    let strategy = strategies
        .iter_mut()
        .find(|s| s.config.strategy_id == id);

    match strategy {
        Some(s) => {
            if s.running {
                info!(strategy_id = %id, "strategy already running");
                Ok(ApiResponse::ok_message(format!(
                    "strategy {id} is already running"
                )))
            } else {
                s.running = true;
                s.started_at = Some(Utc::now());
                s.stopped_at = None;
                info!(strategy_id = %id, "strategy started via API");
                Ok(ApiResponse::ok_message(format!(
                    "strategy {id} started"
                )))
            }
        }
        None => Err(ApiResponse::not_found(format!(
            "strategy not found: {id}"
        ))),
    }
}

async fn stop_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let mut strategies = state.strategies.write().await;

    let strategy = strategies
        .iter_mut()
        .find(|s| s.config.strategy_id == id);

    match strategy {
        Some(s) => {
            if !s.running {
                info!(strategy_id = %id, "strategy already stopped");
                Ok(ApiResponse::ok_message(format!(
                    "strategy {id} is already stopped"
                )))
            } else {
                s.running = false;
                s.stopped_at = Some(Utc::now());
                info!(strategy_id = %id, "strategy stopped via API");
                Ok(ApiResponse::ok_message(format!(
                    "strategy {id} stopped"
                )))
            }
        }
        None => Err(ApiResponse::not_found(format!(
            "strategy not found: {id}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Risk kill-switch
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct KillSwitchResponse {
    active: bool,
    timestamp: String,
}

async fn activate_kill_switch(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let mut ks = state.kill_switch_active.lock().await;
    *ks = true;
    warn!("KILL SWITCH activated via REST API");

    // Also mark all strategies as stopped.
    let mut strategies = state.strategies.write().await;
    let now = Utc::now();
    for s in strategies.iter_mut() {
        if s.running {
            s.running = false;
            s.stopped_at = Some(now);
        }
    }

    ApiResponse::ok(KillSwitchResponse {
        active: true,
        timestamp: now.to_rfc3339(),
    })
}

async fn reset_kill_switch(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let mut ks = state.kill_switch_active.lock().await;
    *ks = false;
    info!("Kill switch reset via REST API");

    ApiResponse::ok(KillSwitchResponse {
        active: false,
        timestamp: Utc::now().to_rfc3339(),
    })
}

// ---------------------------------------------------------------------------
// Market data — order book
// ---------------------------------------------------------------------------

async fn get_orderbook(
    State(state): State<Arc<ApiState>>,
    Path((exchange, symbol)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let exchange_parsed: Exchange = exchange
        .to_lowercase()
        .parse()
        .map_err(|_| ApiResponse::not_found(format!("unknown exchange: {exchange}")))?;

    let books = state.order_books.read().await;

    // Symbol comes in as "BTC-USDT" or "BTC/USDT" — normalise to canonical.
    let normalised_symbol = symbol.replace('-', "/").to_uppercase();

    let found = books.get(&(exchange_parsed, normalised_symbol.clone()));

    match found {
        Some(book) => Ok(ApiResponse::ok(book.clone())),
        None => Err(ApiResponse::not_found(format!(
            "order book not found for {exchange_parsed} {normalised_symbol}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use mm_core::types::Symbol;
    use tower::ServiceExt;

    /// Helper to build a default StrategyConfig for tests.
    fn test_strategy_config(id: &str) -> StrategyConfig {
        StrategyConfig {
            strategy_id: id.to_string(),
            strategy_type: "spread".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            enabled: true,
            params: serde_json::json!({}),
        }
    }

    /// Helper to build a StrategyStatus (not running).
    fn test_strategy_status(id: &str) -> StrategyStatus {
        StrategyStatus {
            config: test_strategy_config(id),
            running: false,
            started_at: None,
            stopped_at: None,
        }
    }

    /// Helper to build a running StrategyStatus.
    fn test_running_strategy_status(id: &str) -> StrategyStatus {
        StrategyStatus {
            config: test_strategy_config(id),
            running: true,
            started_at: Some(Utc::now()),
            stopped_at: None,
        }
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let state = ApiState::new();
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn positions_returns_empty_list() {
        let state = ApiState::new();
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/positions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_order_returns_404() {
        let state = ApiState::new();
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/orders/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- Strategy CRUD tests ---

    #[tokio::test]
    async fn create_strategy_returns_201() {
        let state = ApiState::new();
        let app = rest_router(state);

        let config = test_strategy_config("test-create-1");
        let body = serde_json::to_string(&config).unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/strategies")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn create_duplicate_strategy_returns_409() {
        let state = ApiState::new();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(test_strategy_status("dup-1"));
        }
        let app = rest_router(state);

        let config = test_strategy_config("dup-1");
        let body = serde_json::to_string(&config).unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/strategies")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn create_strategy_invalid_type_returns_400() {
        let state = ApiState::new();
        let app = rest_router(state);

        let mut config = test_strategy_config("bad-type-1");
        config.strategy_type = "nonexistent".to_string();
        let body = serde_json::to_string(&config).unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/strategies")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn update_stopped_strategy_returns_ok() {
        let state = ApiState::new();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(test_strategy_status("update-1"));
        }
        let app = rest_router(state);

        let mut config = test_strategy_config("update-1");
        config.strategy_type = "grid".to_string();
        let body = serde_json::to_string(&config).unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/strategies/update-1")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn update_running_strategy_returns_409() {
        let state = ApiState::new();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(test_running_strategy_status("running-update-1"));
        }
        let app = rest_router(state);

        let config = test_strategy_config("running-update-1");
        let body = serde_json::to_string(&config).unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/strategies/running-update-1")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn delete_stopped_strategy_returns_ok() {
        let state = ApiState::new();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(test_strategy_status("delete-1"));
        }
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/strategies/delete-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_running_strategy_returns_409() {
        let state = ApiState::new();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(test_running_strategy_status("running-del-1"));
        }
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/strategies/running-del-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn delete_nonexistent_strategy_returns_404() {
        let state = ApiState::new();
        let app = rest_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/strategies/no-such-strategy")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
