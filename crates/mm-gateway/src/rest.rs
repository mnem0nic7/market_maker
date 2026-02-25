use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::Utc;
use futures::StreamExt;
use mm_backtest::{
    OptimizationConfig, OptimizationProgress, OptimizationResult, ParameterOptimizer,
};
use mm_core::{Exchange, MetricsRegistry, Order, OrderBook, PortfolioRisk, Position, StrategyConfig, StrategyMetrics};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use mm_core::traits::Strategy;
use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, GridConfig, GridStrategy,
    MeanReversionConfig, MeanReversionStrategy, MomentumConfig, MomentumStrategy, SpreadConfig,
    SpreadStrategy, StatArbConfig, StatArbStrategy, StrategyEngine, TwapConfig, TwapStrategy,
    VwapConfig, VwapStrategy,
};

use crate::assistant::{AssistantConfig, ChatRequest, ClaudeClient, build_system_context};

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
    pub order_books: RwLock<HashMap<(Exchange, String), OrderBook>>,

    /// Whether the global kill-switch is currently active.
    pub kill_switch_active: Mutex<bool>,

    /// Optional Prometheus metrics registry.
    pub metrics: Option<Arc<MetricsRegistry>>,

    /// Per-strategy metrics keyed by strategy_id.
    pub strategy_metrics: RwLock<HashMap<String, StrategyMetrics>>,

    /// Parameter optimisation jobs keyed by job_id.
    pub optimization_jobs: RwLock<HashMap<String, OptimizationJobState>>,

    /// Optional Claude AI assistant configuration.
    pub assistant_config: Option<AssistantConfig>,

    /// Shared strategy engine — when set, start/stop handlers register real
    /// strategy instances with the trading loop's engine.
    pub strategy_engine: Option<Arc<Mutex<StrategyEngine>>>,
}

/// State of an optimisation job (running, completed, or failed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationJobState {
    pub status: String,
    pub results: Option<Vec<OptimizationResult>>,
    pub progress: Option<OptimizationProgress>,
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
            order_books: RwLock::new(HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: None,
            strategy_metrics: RwLock::new(HashMap::new()),
            optimization_jobs: RwLock::new(HashMap::new()),
            assistant_config: AssistantConfig::from_env(),
            strategy_engine: None,
        })
    }

    /// Create state with a Prometheus metrics registry attached.
    pub fn with_metrics(metrics: Arc<MetricsRegistry>) -> Arc<Self> {
        Arc::new(Self {
            positions: RwLock::new(Vec::new()),
            orders: RwLock::new(Vec::new()),
            portfolio_risk: RwLock::new(None),
            strategies: RwLock::new(Vec::new()),
            order_books: RwLock::new(HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: Some(metrics),
            strategy_metrics: RwLock::new(HashMap::new()),
            optimization_jobs: RwLock::new(HashMap::new()),
            assistant_config: AssistantConfig::from_env(),
            strategy_engine: None,
        })
    }

    /// Create state with a Prometheus metrics registry and a shared strategy engine.
    pub fn with_engine(
        metrics: Arc<MetricsRegistry>,
        engine: Arc<Mutex<StrategyEngine>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            positions: RwLock::new(Vec::new()),
            orders: RwLock::new(Vec::new()),
            portfolio_risk: RwLock::new(None),
            strategies: RwLock::new(Vec::new()),
            order_books: RwLock::new(HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: Some(metrics),
            strategy_metrics: RwLock::new(HashMap::new()),
            optimization_jobs: RwLock::new(HashMap::new()),
            assistant_config: AssistantConfig::from_env(),
            strategy_engine: Some(engine),
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
            order_books: RwLock::new(HashMap::new()),
            kill_switch_active: Mutex::new(false),
            metrics: None,
            strategy_metrics: RwLock::new(HashMap::new()),
            optimization_jobs: RwLock::new(HashMap::new()),
            assistant_config: AssistantConfig::from_env(),
            strategy_engine: None,
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
        .route("/api/v1/strategies/{id}/metrics", get(get_strategy_metrics))
        // Risk controls
        .route("/api/v1/risk/kill-switch", post(activate_kill_switch))
        .route("/api/v1/risk/kill-switch", delete(reset_kill_switch))
        // Market data
        .route(
            "/api/v1/market-data/{exchange}/{symbol}/orderbook",
            get(get_orderbook),
        )
        // Backtest optimisation
        .route("/api/v1/backtest/optimize", post(start_optimization))
        .route("/api/v1/backtest/optimize/{job_id}", get(get_optimization_job))
        // AI assistant
        .route("/api/v1/assistant/chat", post(assistant_chat))
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

/// Build a boxed strategy instance from a StrategyConfig.
///
/// The strategy-specific config structs (e.g. `SpreadConfig`) require
/// `strategy_id`, `exchange`, and `symbol` fields for deserialization.
/// The dashboard sends those as top-level fields on `StrategyConfig` and
/// omits them from `params`, so we inject them before deserializing.
fn build_strategy(config: &StrategyConfig) -> Result<Box<dyn Strategy>, String> {
    let exchange = config.exchange;
    let symbol = config.symbol.clone();

    // Inject identity fields into params so the typed config structs can
    // deserialize regardless of whether the caller included them.
    let mut params = config.params.clone();
    if let Some(obj) = params.as_object_mut() {
        obj.entry("strategy_id")
            .or_insert_with(|| serde_json::json!(config.strategy_id));
        obj.entry("exchange")
            .or_insert_with(|| serde_json::json!(exchange));
        obj.entry("symbol")
            .or_insert_with(|| serde_json::json!(symbol));
    }

    match config.strategy_type.as_str() {
        "spread" => {
            let cfg: SpreadConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid spread params: {e}"))?;
            Ok(Box::new(SpreadStrategy::new(cfg)))
        }
        "avellaneda" => {
            let cfg: AvellanedaStoikovConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid avellaneda params: {e}"))?;
            Ok(Box::new(AvellanedaStoikovStrategy::new(cfg)))
        }
        "grid" => {
            let cfg: GridConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid grid params: {e}"))?;
            Ok(Box::new(GridStrategy::new(cfg)))
        }
        "twap" => {
            let cfg: TwapConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid twap params: {e}"))?;
            Ok(Box::new(TwapStrategy::new(cfg)))
        }
        "vwap" => {
            let cfg: VwapConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid vwap params: {e}"))?;
            Ok(Box::new(VwapStrategy::new(cfg)))
        }
        "momentum" => {
            let cfg: MomentumConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid momentum params: {e}"))?;
            Ok(Box::new(MomentumStrategy::new(cfg)))
        }
        "mean_reversion" => {
            let cfg: MeanReversionConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid mean_reversion params: {e}"))?;
            Ok(Box::new(MeanReversionStrategy::new(cfg)))
        }
        "stat_arb" => {
            let cfg: StatArbConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid stat_arb params: {e}"))?;
            Ok(Box::new(StatArbStrategy::new(cfg)))
        }
        other => Err(format!("unsupported strategy type: {other}")),
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
                // Build and register the strategy with the engine.
                if let Some(ref engine) = state.strategy_engine {
                    let strategy_instance = build_strategy(&s.config)
                        .map_err(|e| ApiResponse::error(format!("failed to build strategy: {e}")))?;

                    let mut engine_guard = engine.lock().await;
                    engine_guard.add_strategy(strategy_instance)
                        .map_err(|e| ApiResponse::error(format!("failed to register strategy: {e}")))?;
                }

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
                // Remove the strategy from the engine.
                if let Some(ref engine) = state.strategy_engine {
                    let mut engine_guard = engine.lock().await;
                    if let Some((_strat, cancel_actions)) = engine_guard.remove_strategy(&id) {
                        info!(
                            strategy_id = %id,
                            cancel_actions = cancel_actions.len(),
                            "strategy removed from engine"
                        );
                    }
                }

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
// Strategy metrics
// ---------------------------------------------------------------------------

async fn get_strategy_metrics(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let metrics = state.strategy_metrics.read().await;

    match metrics.get(&id) {
        Some(m) => Ok(ApiResponse::ok(m.clone())),
        None => Err(ApiResponse::not_found(format!(
            "strategy metrics not found: {id}"
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
// Backtest optimisation
// ---------------------------------------------------------------------------

/// Request body for starting an optimisation. Wraps [`OptimizationConfig`]
/// together with synthetic-data generation parameters since the gateway does
/// not have access to historical data files directly.
#[derive(Debug, Deserialize)]
struct StartOptimizationRequest {
    /// The full optimisation configuration.
    #[serde(flatten)]
    config: OptimizationConfig,
    /// Number of synthetic events to generate for the backtest. Defaults to 500.
    #[serde(default = "default_opt_num_events")]
    num_events: usize,
}

fn default_opt_num_events() -> usize {
    500
}

#[derive(Serialize)]
struct StartOptimizationResponse {
    job_id: String,
}

/// Generate simple synthetic market events for the optimizer (order book snapshots).
fn generate_optimizer_events(num_events: usize) -> Vec<mm_backtest::MarketEvent> {
    use mm_core::types::{PriceLevel, Symbol};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut events = Vec::with_capacity(num_events);
    let exchange = Exchange::Simulated;
    let symbol = Symbol::new("BTC", "USDT");
    let base_time = Utc::now() - chrono::Duration::seconds(num_events as i64);
    let mut mid_price: f64 = 50_000.0;
    let mut seed: u64 = 42;

    for i in 0..num_events {
        let ts = base_time + chrono::Duration::seconds(i as i64);
        seed = {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            i.hash(&mut hasher);
            hasher.finish()
        };
        let step_pct = ((seed % 1000) as f64 / 1000.0 - 0.5) * 0.005;
        mid_price *= 1.0 + step_pct;
        if mid_price < 100.0 {
            mid_price = 100.0;
        }

        let spread_bps = 1.0 + (seed % 10) as f64;
        let half_spread = mid_price * spread_bps / 10_000.0 / 2.0;
        let best_bid = mid_price - half_spread;
        let best_ask = mid_price + half_spread;

        let bids = vec![PriceLevel {
            price: rust_decimal::Decimal::from_f64_retain(best_bid)
                .unwrap_or(rust_decimal::Decimal::new(50000, 0))
                .round_dp(2),
            quantity: rust_decimal::Decimal::new(10, 0),
        }];
        let asks = vec![PriceLevel {
            price: rust_decimal::Decimal::from_f64_retain(best_ask)
                .unwrap_or(rust_decimal::Decimal::new(50000, 0))
                .round_dp(2),
            quantity: rust_decimal::Decimal::new(10, 0),
        }];

        if i % 3 == 0 && i > 0 {
            let trade_side = if seed % 2 == 0 {
                mm_core::Side::Buy
            } else {
                mm_core::Side::Sell
            };
            let trade_price = if trade_side == mm_core::Side::Buy {
                best_ask
            } else {
                best_bid
            };
            events.push(mm_backtest::MarketEvent::TradeEvent {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                price: rust_decimal::Decimal::from_f64_retain(trade_price)
                    .unwrap_or(rust_decimal::Decimal::new(50000, 0))
                    .round_dp(2),
                quantity: rust_decimal::Decimal::new(1, 2),
                side: trade_side,
                trade_id: format!("synth-{}", i),
            });
        } else {
            events.push(mm_backtest::MarketEvent::OrderBookSnapshot {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                bids,
                asks,
                sequence: i as u64,
            });
        }
    }

    events
}

async fn start_optimization(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<StartOptimizationRequest>,
) -> Result<(StatusCode, impl IntoResponse), (StatusCode, Json<ApiResponse<()>>)> {
    let job_id = uuid::Uuid::new_v4().to_string();

    // Insert job as running.
    {
        let mut jobs = state.optimization_jobs.write().await;
        jobs.insert(
            job_id.clone(),
            OptimizationJobState {
                status: "running".to_string(),
                results: None,
                progress: None,
            },
        );
    }

    info!(job_id = %job_id, "Starting optimisation job");

    let config = req.config;
    let num_events = req.num_events;
    let state_clone = Arc::clone(&state);
    let jid = job_id.clone();

    // Spawn the optimisation in a background task.
    tokio::task::spawn_blocking(move || {
        let events = generate_optimizer_events(num_events);

        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::channel::<mm_backtest::OptimizationProgress>(64);

        // Spawn a small async task to forward progress into state.
        let progress_state = Arc::clone(&state_clone);
        let progress_jid = jid.clone();
        let rt = tokio::runtime::Handle::current();
        rt.spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let mut jobs = progress_state.optimization_jobs.write().await;
                if let Some(job) = jobs.get_mut(&progress_jid) {
                    job.progress = Some(progress);
                }
            }
        });

        let results = ParameterOptimizer::run(&config, events, Some(progress_tx));

        // Update job to completed.
        let state_final = state_clone;
        let jid_final = jid;
        rt.block_on(async move {
            let mut jobs = state_final.optimization_jobs.write().await;
            if let Some(job) = jobs.get_mut(&jid_final) {
                job.status = "completed".to_string();
                job.results = Some(results);
            }
        });
    });

    Ok((
        StatusCode::CREATED,
        ApiResponse::ok(StartOptimizationResponse { job_id }),
    ))
}

async fn get_optimization_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let jobs = state.optimization_jobs.read().await;

    match jobs.get(&job_id) {
        Some(job) => Ok(ApiResponse::ok(job.clone())),
        None => Err(ApiResponse::not_found(format!(
            "optimisation job not found: {job_id}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// AI Assistant — SSE streaming chat
// ---------------------------------------------------------------------------

async fn assistant_chat(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<ChatRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let config = match &state.assistant_config {
        Some(config) => config.clone(),
        None => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse {
                    status: "error",
                    data: None,
                    error: Some("AI assistant not configured. Set MM_ASSISTANT__ANTHROPIC_API_KEY.".into()),
                }),
            ));
        }
    };

    let client = ClaudeClient::new(config);
    let system_ctx = build_system_context(&state).await;

    let stream = client
        .stream_chat(&system_ctx, &req.messages)
        .await
        .map_err(|e| {
            ApiResponse::error(format!("assistant error: {e}"))
        })?;

    // Convert text chunks into SSE-formatted events.
    let sse_stream = stream.map(|chunk| {
        match chunk {
            Ok(text) => Ok::<_, std::convert::Infallible>(
                format!("data: {}\n\n", serde_json::json!({"text": text})),
            ),
            Err(e) => Ok(format!(
                "data: {}\n\n",
                serde_json::json!({"error": e.to_string()})
            )),
        }
    });

    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        axum::body::Body::from_stream(sse_stream),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use crate::assistant::ChatMessage;
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

    // --- AI Assistant tests ---

    #[tokio::test]
    async fn assistant_returns_503_without_api_key() {
        // Ensure no API key is set so assistant_config will be None.
        std::env::remove_var("MM_ASSISTANT__ANTHROPIC_API_KEY");
        let state = ApiState::new();
        let app = rest_router(state);

        let body = serde_json::to_string(&ChatRequest {
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
        })
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/assistant/chat")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
