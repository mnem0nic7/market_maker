//! Per-strategy metrics tracking.
//!
//! [`StrategyMetrics`] captures key performance indicators for a single
//! strategy instance.  [`StrategyMetricsTracker`] aggregates fill / order
//! events and exposes the current snapshot for each registered strategy.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::{Fill, Side};

// ---------------------------------------------------------------------------
// StrategyMetrics
// ---------------------------------------------------------------------------

/// Snapshot of key performance metrics for a single strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    /// Unique identifier of the strategy.
    pub strategy_id: String,

    /// Cumulative realized profit/loss.
    pub realized_pnl: Decimal,

    /// Current unrealized profit/loss (set externally from position data).
    pub unrealized_pnl: Decimal,

    /// Current signed strategy position (positive = long, negative = short).
    pub current_position: Decimal,

    /// High-water mark for total PnL observed so far.
    pub peak_total_pnl: Decimal,

    /// Maximum drawdown in total PnL terms observed so far.
    pub max_drawdown: Decimal,

    /// Total number of fills received.
    pub total_fills: u64,

    /// Total number of orders placed.
    pub total_orders_placed: u64,

    /// Total number of orders cancelled.
    pub total_orders_cancelled: u64,

    /// Total number of orders rejected by risk or exchange.
    pub total_orders_rejected: u64,

    /// Average spread captured in basis points.
    pub avg_spread_captured_bps: Decimal,

    /// Cumulative traded volume (quantity * price summed over fills).
    pub volume_traded: Decimal,

    /// Seconds since the strategy was registered / started.
    pub uptime_secs: u64,

    /// Timestamp of the most recent fill, if any.
    pub last_fill_at: Option<DateTime<Utc>>,
}

impl StrategyMetrics {
    /// Create a new, zeroed-out metrics instance for the given strategy.
    fn new(strategy_id: impl Into<String>) -> Self {
        Self {
            strategy_id: strategy_id.into(),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            current_position: Decimal::ZERO,
            peak_total_pnl: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            total_fills: 0,
            total_orders_placed: 0,
            total_orders_cancelled: 0,
            total_orders_rejected: 0,
            avg_spread_captured_bps: Decimal::ZERO,
            volume_traded: Decimal::ZERO,
            uptime_secs: 0,
            last_fill_at: None,
        }
    }
}

// ---------------------------------------------------------------------------
// StrategyMetricsTracker
// ---------------------------------------------------------------------------

/// Accumulates per-strategy metrics from order/fill events.
pub struct StrategyMetricsTracker {
    metrics: HashMap<String, StrategyMetrics>,
    start_times: HashMap<String, DateTime<Utc>>,
    /// Running average entry price per strategy (used to compute realized PnL).
    avg_entry_prices: HashMap<String, Decimal>,
    /// Current signed position per strategy (positive = long, negative = short).
    positions: HashMap<String, Decimal>,
}

impl StrategyMetricsTracker {
    /// Create a new, empty tracker.
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            start_times: HashMap::new(),
            avg_entry_prices: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    /// Register a strategy so that metrics collection begins for it.
    pub fn register_strategy(&mut self, strategy_id: &str) {
        let now = Utc::now();
        self.metrics
            .entry(strategy_id.to_string())
            .or_insert_with(|| StrategyMetrics::new(strategy_id));
        self.start_times
            .entry(strategy_id.to_string())
            .or_insert(now);
        self.avg_entry_prices
            .entry(strategy_id.to_string())
            .or_insert(Decimal::ZERO);
        self.positions
            .entry(strategy_id.to_string())
            .or_insert(Decimal::ZERO);
    }

    /// Record a fill event for the given strategy.
    ///
    /// Increments `total_fills`, adds to `volume_traded`, computes realized
    /// PnL using a running average entry price, and sets `last_fill_at`.
    pub fn on_fill(&mut self, strategy_id: &str, fill: &Fill) {
        // Ensure the strategy is registered.
        if !self.metrics.contains_key(strategy_id) {
            self.register_strategy(strategy_id);
        }

        let Some(metrics) = self.metrics.get_mut(strategy_id) else {
            return;
        };
        let Some(avg_entry) = self.avg_entry_prices.get_mut(strategy_id) else {
            return;
        };
        let Some(position) = self.positions.get_mut(strategy_id) else {
            return;
        };

        metrics.total_fills += 1;
        metrics.volume_traded += fill.quantity * fill.price;
        metrics.last_fill_at = Some(fill.timestamp);

        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let new_position = *position + signed_qty;

        // Determine if this fill is adding to or reducing the position.
        let adding = (*position >= Decimal::ZERO && signed_qty > Decimal::ZERO)
            || (*position <= Decimal::ZERO && signed_qty < Decimal::ZERO);

        if adding {
            // Increasing position: update average entry price.
            let total_cost = *avg_entry * position.abs() + fill.price * fill.quantity;
            let total_qty = position.abs() + fill.quantity;
            if total_qty != Decimal::ZERO {
                *avg_entry = total_cost / total_qty;
            }
        } else {
            // Reducing position: realize PnL on the portion being closed.
            let closed_qty = fill.quantity.min(position.abs());
            let pnl_per_unit = match fill.side {
                Side::Sell => fill.price - *avg_entry,
                Side::Buy => *avg_entry - fill.price,
            };
            metrics.realized_pnl += pnl_per_unit * closed_qty;

            // If the position flipped, reset avg entry to fill price.
            if (*position > Decimal::ZERO && new_position < Decimal::ZERO)
                || (*position < Decimal::ZERO && new_position > Decimal::ZERO)
            {
                *avg_entry = fill.price;
            }
        }

        *position = new_position;
        metrics.current_position = new_position;
        self.update_peak_values(strategy_id);
    }

    /// Record that an order was placed for the given strategy.
    pub fn on_order_placed(&mut self, strategy_id: &str) {
        if !self.metrics.contains_key(strategy_id) {
            self.register_strategy(strategy_id);
        }
        if let Some(m) = self.metrics.get_mut(strategy_id) {
            m.total_orders_placed += 1;
        }
    }

    /// Record that an order was cancelled for the given strategy.
    pub fn on_order_cancelled(&mut self, strategy_id: &str) {
        if !self.metrics.contains_key(strategy_id) {
            self.register_strategy(strategy_id);
        }
        if let Some(m) = self.metrics.get_mut(strategy_id) {
            m.total_orders_cancelled += 1;
        }
    }

    /// Record that an order was rejected for the given strategy.
    pub fn on_order_rejected(&mut self, strategy_id: &str) {
        if !self.metrics.contains_key(strategy_id) {
            self.register_strategy(strategy_id);
        }
        if let Some(m) = self.metrics.get_mut(strategy_id) {
            m.total_orders_rejected += 1;
        }
    }

    /// Set the current unrealized PnL for a strategy (computed externally
    /// from position and market price data).
    pub fn update_unrealized_pnl(&mut self, strategy_id: &str, pnl: Decimal) {
        if let Some(m) = self.metrics.get_mut(strategy_id) {
            m.unrealized_pnl = pnl;
            if let Some(position) = self.positions.get(strategy_id) {
                m.current_position = *position;
            }
        }
        self.update_peak_values(strategy_id);
    }

    fn update_peak_values(&mut self, strategy_id: &str) {
        let Some(m) = self.metrics.get_mut(strategy_id) else {
            return;
        };

        let total_pnl = m.realized_pnl + m.unrealized_pnl;
        if total_pnl > m.peak_total_pnl {
            m.peak_total_pnl = total_pnl;
        }

        let drawdown = m.peak_total_pnl - total_pnl;
        if drawdown > m.max_drawdown {
            m.max_drawdown = drawdown;
        }
    }

    /// Recompute `uptime_secs` for all registered strategies based on the
    /// given timestamp.
    pub fn update_uptime(&mut self, now: DateTime<Utc>) {
        for (id, m) in &mut self.metrics {
            if let Some(start) = self.start_times.get(id) {
                m.uptime_secs = (now - *start).num_seconds().max(0) as u64;
            }
        }
    }

    /// Get a reference to the metrics for a specific strategy.
    pub fn get(&self, strategy_id: &str) -> Option<&StrategyMetrics> {
        self.metrics.get(strategy_id)
    }

    /// Return a snapshot of all registered strategy metrics.
    pub fn all(&self) -> Vec<StrategyMetrics> {
        self.metrics.values().cloned().collect()
    }
}

impl Default for StrategyMetricsTracker {
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
    use crate::types::{Exchange, Symbol};
    use rust_decimal_macros::dec;

    /// Helper to build a fill.
    fn make_fill(side: Side, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            fill_id: "f1".into(),
            client_order_id: "c1".into(),
            exchange_order_id: "e1".into(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".into(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn on_fill_increments_total_fills_and_volume() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        let fill = make_fill(Side::Buy, dec!(50000), dec!(0.1));
        tracker.on_fill("s1", &fill);

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.total_fills, 1);
        assert_eq!(m.volume_traded, dec!(5000));
        assert!(m.last_fill_at.is_some());
    }

    #[test]
    fn on_fill_computes_realized_pnl() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        // Buy 1 BTC at 50000
        let buy = make_fill(Side::Buy, dec!(50000), dec!(1));
        tracker.on_fill("s1", &buy);

        // Sell 1 BTC at 51000 => realized PnL = 1000
        let sell = make_fill(Side::Sell, dec!(51000), dec!(1));
        tracker.on_fill("s1", &sell);

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.realized_pnl, dec!(1000));
        assert_eq!(m.current_position, dec!(0));
        assert_eq!(m.total_fills, 2);
    }

    #[test]
    fn on_order_placed_increments_counter() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        tracker.on_order_placed("s1");
        tracker.on_order_placed("s1");

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.total_orders_placed, 2);
    }

    #[test]
    fn on_order_cancelled_increments_counter() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        tracker.on_order_cancelled("s1");

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.total_orders_cancelled, 1);
    }

    #[test]
    fn on_order_rejected_increments_counter() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        tracker.on_order_rejected("s1");

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.total_orders_rejected, 1);
    }

    #[test]
    fn update_uptime_computes_correct_seconds() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        // Override the start time to a known value.
        let start = Utc::now() - chrono::Duration::seconds(120);
        tracker.start_times.insert("s1".to_string(), start);

        let now = start + chrono::Duration::seconds(120);
        tracker.update_uptime(now);

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.uptime_secs, 120);
    }

    #[test]
    fn all_returns_all_registered_metrics() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");
        tracker.register_strategy("s2");
        tracker.register_strategy("s3");

        let all = tracker.all();
        assert_eq!(all.len(), 3);

        let ids: Vec<&str> = all.iter().map(|m| m.strategy_id.as_str()).collect();
        assert!(ids.contains(&"s1"));
        assert!(ids.contains(&"s2"));
        assert!(ids.contains(&"s3"));
    }

    #[test]
    fn unregistered_strategy_auto_registers_on_fill() {
        let mut tracker = StrategyMetricsTracker::new();

        let fill = make_fill(Side::Buy, dec!(100), dec!(1));
        tracker.on_fill("auto", &fill);

        assert!(tracker.get("auto").is_some());
        assert_eq!(tracker.get("auto").unwrap().total_fills, 1);
    }

    #[test]
    fn update_unrealized_pnl_sets_value() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        tracker.update_unrealized_pnl("s1", dec!(500));

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.unrealized_pnl, dec!(500));
        assert_eq!(m.peak_total_pnl, dec!(500));
        assert_eq!(m.max_drawdown, dec!(0));
    }

    #[test]
    fn update_peak_values_tracks_drawdown() {
        let mut tracker = StrategyMetricsTracker::new();
        tracker.register_strategy("s1");

        tracker.update_unrealized_pnl("s1", dec!(300));
        tracker.update_unrealized_pnl("s1", dec!(100));

        let m = tracker.get("s1").unwrap();
        assert_eq!(m.peak_total_pnl, dec!(300));
        assert_eq!(m.max_drawdown, dec!(200));
    }
}
