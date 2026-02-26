//! Prometheus metrics registry for the market maker platform.
//!
//! [`MetricsRegistry`] lives in `mm-core` so every crate in the workspace can
//! import and update metrics without circular dependencies.  A single instance
//! is created in the `trading-core` binary and shared via `Arc<MetricsRegistry>`.

use prometheus::{
    opts, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Registry,
    TextEncoder,
};

use crate::errors::MmError;

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// Central collection of Prometheus counters, gauges, and histograms used
/// across the platform.
///
/// All metric families are pre-registered in [`MetricsRegistry::new`] so that
/// callers only need to call `.with_label_values(…).inc()` / `.set()` etc.
pub struct MetricsRegistry {
    /// The underlying Prometheus registry (used to serialize /metrics output).
    pub registry: Registry,

    // -- Counters --
    /// Total orders submitted (labels: exchange, side, status).
    pub orders_total: GaugeVec,

    /// Total fills received (labels: exchange, side).
    pub fills_total: GaugeVec,

    /// Risk check evaluations (labels: check_name, result).
    pub risk_checks_total: GaugeVec,

    // -- Gauges --
    /// Current position quantity (labels: exchange, symbol).
    pub position_quantity: GaugeVec,

    /// Realized PnL (labels: exchange, symbol).
    pub pnl_realized: GaugeVec,

    /// Unrealized PnL (labels: exchange, symbol).
    pub pnl_unrealized: GaugeVec,

    /// Current order book spread in basis points (labels: exchange, symbol).
    pub order_book_spread_bps: GaugeVec,

    /// Whether the global kill switch is active (0 or 1).
    pub kill_switch_active: Gauge,

    /// WebSocket connection status per exchange (labels: exchange).
    pub websocket_connected: GaugeVec,

    // -- API metrics --
    /// Total API requests (labels: endpoint, method, status).
    pub api_requests_total: IntCounterVec,

    /// API request duration in seconds (labels: endpoint, method).
    pub api_request_duration_seconds: HistogramVec,

    /// Current number of active WebSocket connections.
    pub ws_active_connections: IntGauge,

    // -- Histograms --
    /// Strategy tick duration in seconds (labels: strategy_id).
    pub strategy_tick_duration: HistogramVec,

    /// Order round-trip latency in seconds (labels: exchange).
    pub order_latency: HistogramVec,
}

impl MetricsRegistry {
    /// Create a new registry with all metrics pre-registered.
    ///
    /// Returns an error if any metric fails to register with the Prometheus
    /// registry (e.g. due to a duplicate metric name).
    pub fn new() -> Result<Self, MmError> {
        let registry = Registry::new();

        // -- Counters (using GaugeVec so we can call .inc()) --

        let orders_total = GaugeVec::new(
            opts!("mm_orders_total", "Total orders submitted"),
            &["exchange", "side", "status"],
        )
        .expect("metric: orders_total");

        let fills_total = GaugeVec::new(
            opts!("mm_fills_total", "Total fills received"),
            &["exchange", "side"],
        )
        .expect("metric: fills_total");

        let risk_checks_total = GaugeVec::new(
            opts!("mm_risk_checks_total", "Risk check evaluations"),
            &["check_name", "result"],
        )
        .expect("metric: risk_checks_total");

        // -- Gauges --

        let position_quantity = GaugeVec::new(
            opts!("mm_position_quantity", "Current position quantity"),
            &["exchange", "symbol"],
        )
        .expect("metric: position_quantity");

        let pnl_realized = GaugeVec::new(
            opts!("mm_pnl_realized", "Realized PnL"),
            &["exchange", "symbol"],
        )
        .expect("metric: pnl_realized");

        let pnl_unrealized = GaugeVec::new(
            opts!("mm_pnl_unrealized", "Unrealized PnL"),
            &["exchange", "symbol"],
        )
        .expect("metric: pnl_unrealized");

        let order_book_spread_bps = GaugeVec::new(
            opts!(
                "mm_order_book_spread_bps",
                "Order book spread in basis points"
            ),
            &["exchange", "symbol"],
        )
        .expect("metric: order_book_spread_bps");

        let kill_switch_active = Gauge::with_opts(opts!(
            "mm_kill_switch_active",
            "Whether the global kill switch is active (0 or 1)"
        ))
        .expect("metric: kill_switch_active");

        let websocket_connected = GaugeVec::new(
            opts!("mm_websocket_connected", "WebSocket connection status"),
            &["exchange"],
        )
        .expect("metric: websocket_connected");

        // -- API metrics --

        let api_requests_total = IntCounterVec::new(
            opts!("mm_api_requests_total", "Total API requests"),
            &["endpoint", "method", "status"],
        )
        .expect("metric: api_requests_total");

        let api_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "mm_api_request_duration_seconds",
                "API request duration in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
            ]),
            &["endpoint", "method"],
        )
        .expect("metric: api_request_duration_seconds");

        let ws_active_connections = IntGauge::with_opts(opts!(
            "mm_ws_active_connections",
            "Current number of active WebSocket connections"
        ))
        .expect("metric: ws_active_connections");

        // -- Histograms --

        let strategy_tick_duration = HistogramVec::new(
            HistogramOpts::new(
                "mm_strategy_tick_duration_seconds",
                "Strategy tick duration in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
            ]),
            &["strategy_id"],
        )
        .expect("metric: strategy_tick_duration");

        let order_latency = HistogramVec::new(
            HistogramOpts::new(
                "mm_order_latency_seconds",
                "Order round-trip latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
            ]),
            &["exchange"],
        )
        .expect("metric: order_latency");

        // Register everything.
        registry
            .register(Box::new(orders_total.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register orders_total metric: {e}"))
            })?;
        registry
            .register(Box::new(fills_total.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register fills_total metric: {e}"))
            })?;
        registry
            .register(Box::new(risk_checks_total.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register risk_checks_total metric: {e}"))
            })?;
        registry
            .register(Box::new(position_quantity.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register position_quantity metric: {e}"))
            })?;
        registry
            .register(Box::new(pnl_realized.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register pnl_realized metric: {e}"))
            })?;
        registry
            .register(Box::new(pnl_unrealized.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register pnl_unrealized metric: {e}"))
            })?;
        registry
            .register(Box::new(order_book_spread_bps.clone()))
            .map_err(|e| {
                MmError::Internal(format!(
                    "failed to register order_book_spread_bps metric: {e}"
                ))
            })?;
        registry
            .register(Box::new(kill_switch_active.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register kill_switch_active metric: {e}"))
            })?;
        registry
            .register(Box::new(websocket_connected.clone()))
            .map_err(|e| {
                MmError::Internal(format!(
                    "failed to register websocket_connected metric: {e}"
                ))
            })?;
        registry
            .register(Box::new(api_requests_total.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register api_requests_total metric: {e}"))
            })?;
        registry
            .register(Box::new(api_request_duration_seconds.clone()))
            .map_err(|e| {
                MmError::Internal(format!(
                    "failed to register api_request_duration_seconds metric: {e}"
                ))
            })?;
        registry
            .register(Box::new(ws_active_connections.clone()))
            .map_err(|e| {
                MmError::Internal(format!(
                    "failed to register ws_active_connections metric: {e}"
                ))
            })?;
        registry
            .register(Box::new(strategy_tick_duration.clone()))
            .map_err(|e| {
                MmError::Internal(format!(
                    "failed to register strategy_tick_duration metric: {e}"
                ))
            })?;
        registry
            .register(Box::new(order_latency.clone()))
            .map_err(|e| {
                MmError::Internal(format!("failed to register order_latency metric: {e}"))
            })?;

        Ok(Self {
            registry,
            orders_total,
            fills_total,
            risk_checks_total,
            position_quantity,
            pnl_realized,
            pnl_unrealized,
            order_book_spread_bps,
            kill_switch_active,
            websocket_connected,
            api_requests_total,
            api_request_duration_seconds,
            ws_active_connections,
            strategy_tick_duration,
            order_latency,
        })
    }

    /// Encode all metrics as Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        if let Err(_e) = encoder.encode(&metric_families, &mut buffer) {
            eprintln!("metrics: failed to encode Prometheus metrics: {}", _e);
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("failed to initialize default MetricsRegistry")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_registry_creates_successfully() {
        let registry = MetricsRegistry::new().unwrap();

        // Increment a counter and verify it serializes.
        registry
            .orders_total
            .with_label_values(&["binance", "buy", "submitted"])
            .inc();

        let output = registry.encode();
        assert!(output.contains("mm_orders_total"));
        assert!(output.contains("binance"));
    }

    #[test]
    fn metrics_encode_contains_all_families() {
        let registry = MetricsRegistry::new().unwrap();

        // GaugeVec / HistogramVec families only appear in the text output
        // once at least one label set has been initialised.
        registry
            .orders_total
            .with_label_values(&["test", "buy", "submitted"])
            .inc();
        registry
            .fills_total
            .with_label_values(&["test", "buy"])
            .inc();
        registry
            .risk_checks_total
            .with_label_values(&["test", "pass"])
            .inc();
        registry
            .position_quantity
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        registry
            .pnl_realized
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        registry
            .pnl_unrealized
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        registry
            .order_book_spread_bps
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        registry
            .websocket_connected
            .with_label_values(&["test"])
            .set(0.0);
        registry
            .strategy_tick_duration
            .with_label_values(&["test"])
            .observe(0.001);
        registry
            .order_latency
            .with_label_values(&["test"])
            .observe(0.01);
        registry
            .api_requests_total
            .with_label_values(&["/api/v1/health", "GET", "200"])
            .inc();
        registry
            .api_request_duration_seconds
            .with_label_values(&["/api/v1/health", "GET"])
            .observe(0.005);
        registry.ws_active_connections.inc();

        let output = registry.encode();

        assert!(output.contains("mm_orders_total"));
        assert!(output.contains("mm_fills_total"));
        assert!(output.contains("mm_risk_checks_total"));
        assert!(output.contains("mm_kill_switch_active"));
        assert!(output.contains("mm_strategy_tick_duration_seconds"));
        assert!(output.contains("mm_order_latency_seconds"));
        assert!(output.contains("mm_api_requests_total"));
        assert!(output.contains("mm_api_request_duration_seconds"));
        assert!(output.contains("mm_ws_active_connections"));
    }

    #[test]
    fn test_metrics_registry_creation() {
        // Verify that MetricsRegistry::new() succeeds and returns Ok.
        let result = MetricsRegistry::new();
        assert!(
            result.is_ok(),
            "MetricsRegistry::new() should succeed, got error: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_metrics_registry_has_expected_metrics() {
        // Verify key metrics exist by gathering from the registry
        // and checking that the expected metric families are present.
        //
        // Prometheus only includes vector metrics in gather() output once
        // at least one label set has been initialised, so we touch each one.
        let m = MetricsRegistry::new().unwrap();

        m.orders_total
            .with_label_values(&["test", "buy", "submitted"])
            .inc();
        m.fills_total.with_label_values(&["test", "buy"]).inc();
        m.risk_checks_total
            .with_label_values(&["test", "pass"])
            .inc();
        m.position_quantity
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        m.pnl_realized
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        m.pnl_unrealized
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        m.order_book_spread_bps
            .with_label_values(&["test", "BTC/USD"])
            .set(0.0);
        m.websocket_connected.with_label_values(&["test"]).set(0.0);
        m.api_requests_total
            .with_label_values(&["/health", "GET", "200"])
            .inc();
        m.api_request_duration_seconds
            .with_label_values(&["/health", "GET"])
            .observe(0.001);
        m.strategy_tick_duration
            .with_label_values(&["test"])
            .observe(0.001);
        m.order_latency.with_label_values(&["test"]).observe(0.001);

        let families = m.registry.gather();
        let family_names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();

        let expected = [
            "mm_orders_total",
            "mm_fills_total",
            "mm_risk_checks_total",
            "mm_position_quantity",
            "mm_pnl_realized",
            "mm_pnl_unrealized",
            "mm_order_book_spread_bps",
            "mm_kill_switch_active",
            "mm_websocket_connected",
            "mm_api_requests_total",
            "mm_api_request_duration_seconds",
            "mm_ws_active_connections",
            "mm_strategy_tick_duration_seconds",
            "mm_order_latency_seconds",
        ];

        for name in &expected {
            assert!(
                family_names.contains(name),
                "expected metric family '{}' not found in registry; found: {:?}",
                name,
                family_names
            );
        }
    }
}
