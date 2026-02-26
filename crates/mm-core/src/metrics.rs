//! Prometheus metrics registry for the market maker platform.
//!
//! [`MetricsRegistry`] lives in `mm-core` so every crate in the workspace can
//! import and update metrics without circular dependencies.  A single instance
//! is created in the `trading-core` binary and shared via `Arc<MetricsRegistry>`.

use prometheus::{
    opts, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Registry, TextEncoder,
};

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

    // -- Histograms --
    /// Strategy tick duration in seconds (labels: strategy_id).
    pub strategy_tick_duration: HistogramVec,

    /// Order round-trip latency in seconds (labels: exchange).
    pub order_latency: HistogramVec,
}

impl MetricsRegistry {
    /// Create a new registry with all metrics pre-registered.
    pub fn new() -> Self {
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
        registry.register(Box::new(orders_total.clone())).unwrap();
        registry.register(Box::new(fills_total.clone())).unwrap();
        registry
            .register(Box::new(risk_checks_total.clone()))
            .unwrap();
        registry
            .register(Box::new(position_quantity.clone()))
            .unwrap();
        registry.register(Box::new(pnl_realized.clone())).unwrap();
        registry.register(Box::new(pnl_unrealized.clone())).unwrap();
        registry
            .register(Box::new(order_book_spread_bps.clone()))
            .unwrap();
        registry
            .register(Box::new(kill_switch_active.clone()))
            .unwrap();
        registry
            .register(Box::new(websocket_connected.clone()))
            .unwrap();
        registry
            .register(Box::new(strategy_tick_duration.clone()))
            .unwrap();
        registry.register(Box::new(order_latency.clone())).unwrap();

        Self {
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
            strategy_tick_duration,
            order_latency,
        }
    }

    /// Encode all metrics as Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
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
        let registry = MetricsRegistry::new();

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
        let registry = MetricsRegistry::new();

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

        let output = registry.encode();

        assert!(output.contains("mm_orders_total"));
        assert!(output.contains("mm_fills_total"));
        assert!(output.contains("mm_risk_checks_total"));
        assert!(output.contains("mm_kill_switch_active"));
        assert!(output.contains("mm_strategy_tick_duration_seconds"));
        assert!(output.contains("mm_order_latency_seconds"));
    }
}
