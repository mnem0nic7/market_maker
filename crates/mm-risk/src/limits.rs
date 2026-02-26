use std::collections::VecDeque;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use mm_core::{traits::RiskCheck, Exchange, NewOrder, PortfolioRisk, RiskAction, Side};

// ---------------------------------------------------------------------------
// Position Limit Check
// ---------------------------------------------------------------------------

/// Rejects orders that would cause a single-symbol position or total gross
/// exposure to exceed configured thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionLimitConfig {
    /// Maximum absolute position size for any single symbol.
    pub max_position_per_symbol: Decimal,
    /// Maximum total gross exposure across all positions.
    pub max_gross_exposure: Decimal,
}

pub struct PositionLimitCheck {
    config: PositionLimitConfig,
}

impl PositionLimitCheck {
    pub fn new(config: PositionLimitConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RiskCheck for PositionLimitCheck {
    fn name(&self) -> &str {
        "position_limit"
    }

    async fn check_order(&self, order: &NewOrder, portfolio: &PortfolioRisk) -> RiskAction {
        // Reduce-only orders decrease risk — always allow them.
        if order.reduce_only {
            return RiskAction::Allow;
        }

        // Find the current position for this symbol (if any).
        let current_position_qty = portfolio
            .positions
            .iter()
            .find(|p| p.symbol == order.symbol)
            .map(|p| p.quantity)
            .unwrap_or(Decimal::ZERO);

        let signed_order_qty = match order.side {
            Side::Buy => order.quantity,
            Side::Sell => -order.quantity,
        };

        let projected_position = (current_position_qty + signed_order_qty).abs();

        // If the order reduces the absolute position (moves toward zero), allow it.
        if projected_position < current_position_qty.abs() {
            return RiskAction::Allow;
        }

        // Per-symbol position limit.
        if projected_position > self.config.max_position_per_symbol {
            let headroom = self.config.max_position_per_symbol - current_position_qty.abs();
            if headroom <= Decimal::ZERO {
                return RiskAction::Reject {
                    reason: format!(
                        "position limit: {} position already at {} (max {})",
                        order.symbol,
                        current_position_qty.abs(),
                        self.config.max_position_per_symbol,
                    ),
                };
            }
            return RiskAction::ReduceSize {
                max_quantity: headroom,
                reason: format!(
                    "position limit: reducing order to stay within {} limit for {}",
                    self.config.max_position_per_symbol, order.symbol,
                ),
            };
        }

        // Gross exposure limit.
        // Estimate the new gross exposure: current gross + notional of this order.
        let order_notional = order.quantity * order.price.unwrap_or(Decimal::ZERO);
        let projected_gross = portfolio.gross_exposure + order_notional;

        if projected_gross > self.config.max_gross_exposure {
            let headroom_notional = self.config.max_gross_exposure - portfolio.gross_exposure;
            if headroom_notional <= Decimal::ZERO {
                return RiskAction::Reject {
                    reason: format!(
                        "gross exposure limit: current {} exceeds max {}",
                        portfolio.gross_exposure, self.config.max_gross_exposure,
                    ),
                };
            }
            // Convert notional headroom back to quantity.
            let price = order.price.unwrap_or(Decimal::ONE);
            if price > Decimal::ZERO {
                let max_qty = headroom_notional / price;
                return RiskAction::ReduceSize {
                    max_quantity: max_qty,
                    reason: format!(
                        "gross exposure limit: reducing to stay within {} gross exposure",
                        self.config.max_gross_exposure,
                    ),
                };
            }
        }

        RiskAction::Allow
    }

    async fn check_portfolio(&self, portfolio: &PortfolioRisk) -> RiskAction {
        if portfolio.gross_exposure > self.config.max_gross_exposure {
            return RiskAction::Reject {
                reason: format!(
                    "portfolio gross exposure {} exceeds limit {}",
                    portfolio.gross_exposure, self.config.max_gross_exposure,
                ),
            };
        }

        for pos in &portfolio.positions {
            if pos.quantity.abs() > self.config.max_position_per_symbol {
                return RiskAction::Reject {
                    reason: format!(
                        "position {} for {} exceeds per-symbol limit {}",
                        pos.quantity.abs(),
                        pos.symbol,
                        self.config.max_position_per_symbol,
                    ),
                };
            }
        }

        RiskAction::Allow
    }
}

// ---------------------------------------------------------------------------
// Order Size Check
// ---------------------------------------------------------------------------

/// Rejects or reduces individual orders that are too large by quantity or
/// notional value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSizeConfig {
    /// Maximum quantity for a single order.
    pub max_order_quantity: Decimal,
    /// Maximum notional value (price * quantity) for a single order.
    pub max_order_notional: Decimal,
}

pub struct OrderSizeCheck {
    config: OrderSizeConfig,
}

impl OrderSizeCheck {
    pub fn new(config: OrderSizeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RiskCheck for OrderSizeCheck {
    fn name(&self) -> &str {
        "order_size"
    }

    async fn check_order(&self, order: &NewOrder, _portfolio: &PortfolioRisk) -> RiskAction {
        // Quantity check.
        if order.quantity > self.config.max_order_quantity {
            return RiskAction::ReduceSize {
                max_quantity: self.config.max_order_quantity,
                reason: format!(
                    "order quantity {} exceeds max {}",
                    order.quantity, self.config.max_order_quantity,
                ),
            };
        }

        // Notional check.
        let price = order.price.unwrap_or(Decimal::ZERO);
        let notional = order.quantity * price;
        if price > Decimal::ZERO && notional > self.config.max_order_notional {
            let max_qty = self.config.max_order_notional / price;
            return RiskAction::ReduceSize {
                max_quantity: max_qty,
                reason: format!(
                    "order notional {} exceeds max {}",
                    notional, self.config.max_order_notional,
                ),
            };
        }

        RiskAction::Allow
    }

    async fn check_portfolio(&self, _portfolio: &PortfolioRisk) -> RiskAction {
        // Order-size is a per-order check; nothing to do at portfolio level.
        RiskAction::Allow
    }
}

// ---------------------------------------------------------------------------
// PnL Limit Check
// ---------------------------------------------------------------------------

/// Monitors daily PnL and drawdown, rejecting new orders when thresholds are
/// breached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlLimitConfig {
    /// Maximum daily loss (positive number, e.g. 10000 means -$10,000 loss
    /// threshold). When `daily_pnl <= -max_daily_loss`, orders are rejected.
    pub max_daily_loss: Decimal,
    /// Maximum drawdown as a percentage (e.g. 0.05 = 5%).
    /// When portfolio drawdown exceeds this, a kill-switch is recommended.
    pub max_drawdown_pct: Decimal,
}

pub struct PnlLimitCheck {
    config: PnlLimitConfig,
    /// Internally tracked cumulative daily PnL. Updated by callers via
    /// [`PnlLimitCheck::record_pnl`].
    daily_pnl: Mutex<DailyPnlState>,
}

struct DailyPnlState {
    /// Cumulative realized + unrealized PnL for the current trading day.
    pnl: Decimal,
    /// High-water mark used for drawdown tracking.
    high_water_mark: Decimal,
    /// The date of the current PnL tracking window.
    date: chrono::NaiveDate,
    /// Whether at least one PnL delta has been recorded for the current day.
    has_recorded: bool,
}

impl PnlLimitCheck {
    pub fn new(config: PnlLimitConfig) -> Self {
        let today = Utc::now().date_naive();
        Self {
            config,
            daily_pnl: Mutex::new(DailyPnlState {
                pnl: Decimal::ZERO,
                high_water_mark: Decimal::ZERO,
                date: today,
                has_recorded: false,
            }),
        }
    }

    /// Record a PnL change. Automatically resets at the start of a new
    /// calendar day (UTC).
    pub fn record_pnl(&self, delta: Decimal) {
        let mut state = self.daily_pnl.lock();
        let today = Utc::now().date_naive();

        if state.date != today {
            debug!(old_date = %state.date, new_date = %today, "daily PnL reset");
            state.pnl = Decimal::ZERO;
            state.high_water_mark = Decimal::ZERO;
            state.date = today;
            state.has_recorded = false;
        }

        state.pnl += delta;
        state.has_recorded = true;
        if state.pnl > state.high_water_mark {
            state.high_water_mark = state.pnl;
        }
    }

    /// Return the internally-tracked daily PnL.
    pub fn current_daily_pnl(&self) -> Decimal {
        self.daily_pnl.lock().pnl
    }

    /// Return the current drawdown from intra-day high-water mark.
    pub fn current_drawdown(&self) -> Decimal {
        let state = self.daily_pnl.lock();
        state.high_water_mark - state.pnl
    }
}

#[async_trait]
impl RiskCheck for PnlLimitCheck {
    fn name(&self) -> &str {
        "pnl_limit"
    }

    async fn check_order(&self, _order: &NewOrder, portfolio: &PortfolioRisk) -> RiskAction {
        // Prefer the internally tracked PnL if available, fall back to
        // portfolio snapshot.
        let state = self.daily_pnl.lock();
        let effective_pnl = if state.has_recorded {
            state.pnl
        } else {
            portfolio.daily_pnl
        };
        let drawdown = state.high_water_mark - state.pnl;
        drop(state);

        // Daily loss threshold (max_daily_loss is a positive number).
        if effective_pnl <= -self.config.max_daily_loss {
            warn!(
                daily_pnl = %effective_pnl,
                limit = %self.config.max_daily_loss,
                "daily loss limit breached"
            );
            return RiskAction::KillSwitch {
                reason: format!(
                    "daily loss {} exceeds limit -{}",
                    effective_pnl, self.config.max_daily_loss,
                ),
            };
        }

        // Drawdown check (compared against high-water mark of gross exposure).
        if portfolio.gross_exposure > Decimal::ZERO {
            let drawdown_pct = drawdown / portfolio.gross_exposure;
            if drawdown_pct > self.config.max_drawdown_pct {
                warn!(
                    drawdown_pct = %drawdown_pct,
                    limit = %self.config.max_drawdown_pct,
                    "drawdown limit breached"
                );
                return RiskAction::Reject {
                    reason: format!(
                        "drawdown {:.4}% exceeds max {:.4}%",
                        drawdown_pct * Decimal::from(100),
                        self.config.max_drawdown_pct * Decimal::from(100),
                    ),
                };
            }
        }

        RiskAction::Allow
    }

    async fn check_portfolio(&self, portfolio: &PortfolioRisk) -> RiskAction {
        if portfolio.daily_pnl <= -self.config.max_daily_loss {
            return RiskAction::KillSwitch {
                reason: format!(
                    "portfolio daily PnL {} exceeds loss limit -{}",
                    portfolio.daily_pnl, self.config.max_daily_loss,
                ),
            };
        }

        if portfolio.gross_exposure > Decimal::ZERO {
            let drawdown_pct = portfolio.max_drawdown.abs() / portfolio.gross_exposure;
            if drawdown_pct > self.config.max_drawdown_pct {
                return RiskAction::KillSwitch {
                    reason: format!(
                        "portfolio drawdown {:.4}% exceeds max {:.4}%",
                        drawdown_pct * Decimal::from(100),
                        self.config.max_drawdown_pct * Decimal::from(100),
                    ),
                };
            }
        }

        RiskAction::Allow
    }
}

// ---------------------------------------------------------------------------
// Rate Limit Check
// ---------------------------------------------------------------------------

/// Enforces order submission rate limits using sliding window counters. Each
/// exchange is tracked independently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum number of orders per second per exchange.
    pub max_orders_per_second: u32,
    /// Maximum number of orders per minute per exchange.
    pub max_orders_per_minute: u32,
}

pub struct RateLimitCheck {
    config: RateLimitConfig,
    /// Per-exchange sliding window of order timestamps.
    windows: Mutex<std::collections::HashMap<Exchange, OrderWindow>>,
}

struct OrderWindow {
    timestamps: VecDeque<DateTime<Utc>>,
}

impl OrderWindow {
    fn new() -> Self {
        Self {
            timestamps: VecDeque::new(),
        }
    }

    /// Prune entries older than the given duration.
    fn prune(&mut self, cutoff: DateTime<Utc>) {
        while let Some(front) = self.timestamps.front() {
            if *front < cutoff {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }
    }

    /// Count entries within the last `window` duration.
    fn count_since(&self, cutoff: DateTime<Utc>) -> usize {
        self.timestamps.iter().filter(|t| **t >= cutoff).count()
    }

    /// Record a new timestamp.
    fn record(&mut self, ts: DateTime<Utc>) {
        self.timestamps.push_back(ts);
    }
}

impl RateLimitCheck {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            windows: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Record that an order was submitted. Must be called after the engine
    /// decides to allow the order so the window stays accurate.
    pub fn record_order(&self, exchange: Exchange) {
        let now = Utc::now();
        let mut windows = self.windows.lock();
        let window = windows.entry(exchange).or_insert_with(OrderWindow::new);
        window.record(now);
    }
}

#[async_trait]
impl RiskCheck for RateLimitCheck {
    fn name(&self) -> &str {
        "rate_limit"
    }

    async fn check_order(&self, order: &NewOrder, _portfolio: &PortfolioRisk) -> RiskAction {
        let now = Utc::now();
        let one_second_ago = now - chrono::Duration::seconds(1);
        let one_minute_ago = now - chrono::Duration::minutes(1);

        let mut windows = self.windows.lock();
        let window = windows
            .entry(order.exchange)
            .or_insert_with(OrderWindow::new);

        // Prune entries older than one minute.
        window.prune(one_minute_ago);

        let orders_last_second = window.count_since(one_second_ago);
        let orders_last_minute = window.timestamps.len();

        if orders_last_second >= self.config.max_orders_per_second as usize {
            return RiskAction::Reject {
                reason: format!(
                    "rate limit: {} orders/s on {} (max {})",
                    orders_last_second, order.exchange, self.config.max_orders_per_second,
                ),
            };
        }

        if orders_last_minute >= self.config.max_orders_per_minute as usize {
            return RiskAction::Reject {
                reason: format!(
                    "rate limit: {} orders/min on {} (max {})",
                    orders_last_minute, order.exchange, self.config.max_orders_per_minute,
                ),
            };
        }

        // Record this order in the window so future checks account for it.
        window.record(now);

        RiskAction::Allow
    }

    async fn check_portfolio(&self, _portfolio: &PortfolioRisk) -> RiskAction {
        // Rate limiting is per-order only.
        RiskAction::Allow
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{Exchange, OrderType, Position, Symbol, TimeInForce};
    use rust_decimal_macros::dec;

    fn sample_order() -> NewOrder {
        NewOrder {
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50000)),
            quantity: dec!(1),
            strategy_id: "test".into(),
            reduce_only: false,
        }
    }

    fn portfolio_with_position(qty: Decimal, gross: Decimal) -> PortfolioRisk {
        PortfolioRisk {
            total_var_95: Decimal::ZERO,
            total_var_99: Decimal::ZERO,
            net_exposure: qty * dec!(50000),
            gross_exposure: gross,
            max_drawdown: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            positions: vec![Position {
                exchange: Exchange::Binance,
                symbol: Symbol::new("BTC", "USDT"),
                quantity: qty,
                average_entry_price: dec!(50000),
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                updated_at: Utc::now(),
            }],
            timestamp: Utc::now(),
        }
    }

    fn empty_portfolio() -> PortfolioRisk {
        PortfolioRisk {
            total_var_95: Decimal::ZERO,
            total_var_99: Decimal::ZERO,
            net_exposure: Decimal::ZERO,
            gross_exposure: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            positions: vec![],
            timestamp: Utc::now(),
        }
    }

    // -- PositionLimitCheck --

    #[tokio::test]
    async fn position_limit_allows_within_bounds() {
        let check = PositionLimitCheck::new(PositionLimitConfig {
            max_position_per_symbol: dec!(10),
            max_gross_exposure: dec!(1_000_000),
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }

    #[tokio::test]
    async fn position_limit_rejects_when_full() {
        let check = PositionLimitCheck::new(PositionLimitConfig {
            max_position_per_symbol: dec!(5),
            max_gross_exposure: dec!(1_000_000),
        });
        let portfolio = portfolio_with_position(dec!(5), dec!(250_000));
        let result = check.check_order(&sample_order(), &portfolio).await;
        assert!(matches!(result, RiskAction::Reject { .. }));
    }

    #[tokio::test]
    async fn position_limit_reduces_size() {
        let check = PositionLimitCheck::new(PositionLimitConfig {
            max_position_per_symbol: dec!(5),
            max_gross_exposure: dec!(1_000_000),
        });
        let portfolio = portfolio_with_position(dec!(4.5), dec!(225_000));
        let mut order = sample_order();
        order.quantity = dec!(2);
        let result = check.check_order(&order, &portfolio).await;
        match result {
            RiskAction::ReduceSize { max_quantity, .. } => {
                assert_eq!(max_quantity, dec!(0.5));
            }
            other => panic!("expected ReduceSize, got {:?}", other),
        }
    }

    // -- OrderSizeCheck --

    #[tokio::test]
    async fn order_size_allows_within_bounds() {
        let check = OrderSizeCheck::new(OrderSizeConfig {
            max_order_quantity: dec!(10),
            max_order_notional: dec!(1_000_000),
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }

    #[tokio::test]
    async fn order_size_reduces_quantity() {
        let check = OrderSizeCheck::new(OrderSizeConfig {
            max_order_quantity: dec!(0.5),
            max_order_notional: dec!(1_000_000),
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        match result {
            RiskAction::ReduceSize { max_quantity, .. } => {
                assert_eq!(max_quantity, dec!(0.5));
            }
            other => panic!("expected ReduceSize, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn order_size_reduces_notional() {
        let check = OrderSizeCheck::new(OrderSizeConfig {
            max_order_quantity: dec!(100),
            max_order_notional: dec!(25_000),
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        match result {
            RiskAction::ReduceSize { max_quantity, .. } => {
                assert_eq!(max_quantity, dec!(0.5));
            }
            other => panic!("expected ReduceSize, got {:?}", other),
        }
    }

    // -- PnlLimitCheck --

    #[tokio::test]
    async fn pnl_limit_allows_when_within_bounds() {
        let check = PnlLimitCheck::new(PnlLimitConfig {
            max_daily_loss: dec!(10_000),
            max_drawdown_pct: dec!(0.05),
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }

    #[tokio::test]
    async fn pnl_limit_triggers_kill_switch_on_daily_loss() {
        let check = PnlLimitCheck::new(PnlLimitConfig {
            max_daily_loss: dec!(10_000),
            max_drawdown_pct: dec!(0.05),
        });
        check.record_pnl(dec!(-15_000));
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::KillSwitch { .. }));
    }

    // -- RateLimitCheck --

    #[tokio::test]
    async fn rate_limit_allows_within_bounds() {
        let check = RateLimitCheck::new(RateLimitConfig {
            max_orders_per_second: 10,
            max_orders_per_minute: 100,
        });
        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }

    #[tokio::test]
    async fn rate_limit_rejects_when_exceeded() {
        let check = RateLimitCheck::new(RateLimitConfig {
            max_orders_per_second: 2,
            max_orders_per_minute: 100,
        });
        // Simulate 2 orders already placed.
        check.record_order(Exchange::Binance);
        check.record_order(Exchange::Binance);

        let result = check.check_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Reject { .. }));
    }
}
