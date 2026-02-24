use std::collections::VecDeque;
use std::fmt;

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use mm_core::{NewOrder, PortfolioRisk, RiskAction};

// ---------------------------------------------------------------------------
// Circuit Breaker Levels
// ---------------------------------------------------------------------------

/// Multi-level circuit breaker for progressive risk mitigation.
///
/// | Level | Name     | Trigger                                 | Action                          |
/// |-------|----------|-----------------------------------------|---------------------------------|
/// | L1    | Order    | Single order too large                  | Reject the order                |
/// | L2    | Rate     | Order rate exceeds threshold            | Throttle (reject temporarily)   |
/// | L3    | PnL      | Intra-day loss exceeds threshold        | Pause strategy (reject orders)  |
/// | L4    | Position | Order would exceed position limit       | Reject orders increasing risk   |
/// | L5    | System   | Catastrophic failure / manual trigger   | Cancel everything and halt      |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BreakerLevel {
    /// L1: Single order size check.
    Order,
    /// L2: Order submission rate check.
    Rate,
    /// L3: PnL / drawdown check.
    PnL,
    /// L4: Position limit check.
    Position,
    /// L5: System-wide kill switch.
    System,
}

impl fmt::Display for BreakerLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BreakerLevel::Order => write!(f, "L1-Order"),
            BreakerLevel::Rate => write!(f, "L2-Rate"),
            BreakerLevel::PnL => write!(f, "L3-PnL"),
            BreakerLevel::Position => write!(f, "L4-Position"),
            BreakerLevel::System => write!(f, "L5-System"),
        }
    }
}

/// State of a single breaker level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Normal operation.
    Active,
    /// Breaker has tripped; orders at or above this level are blocked.
    Triggered,
    /// Breaker is in a cooldown period after being triggered. Orders are still
    /// blocked until the cooldown expires.
    Cooldown,
}

impl fmt::Display for BreakerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BreakerState::Active => write!(f, "Active"),
            BreakerState::Triggered => write!(f, "Triggered"),
            BreakerState::Cooldown => write!(f, "Cooldown"),
        }
    }
}

/// Result of a circuit breaker evaluation.
#[derive(Debug, Clone)]
pub struct CircuitBreakerResult {
    /// The highest-severity level that was triggered (if any).
    pub level: Option<BreakerLevel>,
    /// The action the caller should take.
    pub action: RiskAction,
}

// ---------------------------------------------------------------------------
// Per-level configuration
// ---------------------------------------------------------------------------

/// Thresholds for the L1 (Order) breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1Config {
    /// Maximum allowed single-order quantity.
    pub max_order_quantity: Decimal,
    /// Maximum allowed single-order notional.
    pub max_order_notional: Decimal,
}

/// Thresholds for the L2 (Rate) breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Config {
    /// Maximum orders per second across all exchanges.
    pub max_orders_per_second: u32,
    /// Cooldown duration after the breaker trips.
    pub cooldown_seconds: u64,
}

/// Thresholds for the L3 (PnL) breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L3Config {
    /// Maximum intra-day loss (positive number).
    pub max_daily_loss: Decimal,
    /// Maximum drawdown percentage (e.g. 0.05 = 5%).
    pub max_drawdown_pct: Decimal,
    /// Cooldown duration after the breaker trips.
    pub cooldown_seconds: u64,
}

/// Thresholds for the L4 (Position) breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L4Config {
    /// Maximum gross exposure.
    pub max_gross_exposure: Decimal,
    /// Maximum position size per symbol.
    pub max_position_per_symbol: Decimal,
}

/// Thresholds for the L5 (System) breaker. L5 has no automatic reset;
/// [`CircuitBreaker::reset`] must be called explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L5Config {
    /// Maximum cumulative daily loss before system halt.
    pub max_catastrophic_loss: Decimal,
}

/// Aggregated circuit breaker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub l1: L1Config,
    pub l2: L2Config,
    pub l3: L3Config,
    pub l4: L4Config,
    pub l5: L5Config,
}

// ---------------------------------------------------------------------------
// Internal mutable state
// ---------------------------------------------------------------------------

struct LevelState {
    state: BreakerState,
    triggered_at: Option<DateTime<Utc>>,
    trigger_count: u64,
}

impl LevelState {
    fn new() -> Self {
        Self {
            state: BreakerState::Active,
            triggered_at: None,
            trigger_count: 0,
        }
    }

    fn trip(&mut self) {
        self.state = BreakerState::Triggered;
        self.triggered_at = Some(Utc::now());
        self.trigger_count += 1;
    }

    fn cooldown(&mut self) {
        self.state = BreakerState::Cooldown;
    }

    fn reset(&mut self) {
        self.state = BreakerState::Active;
        self.triggered_at = None;
    }

    fn is_blocking(&self) -> bool {
        matches!(self.state, BreakerState::Triggered | BreakerState::Cooldown)
    }
}

struct InnerState {
    l1: LevelState,
    l2: LevelState,
    l3: LevelState,
    l4: LevelState,
    l5: LevelState,
    /// Sliding window of order timestamps for L2 rate check.
    order_timestamps: VecDeque<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

/// Multi-level circuit breaker with configurable thresholds per level.
///
/// Thread-safe: all mutable state is behind a `parking_lot::Mutex`.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<InnerState>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(InnerState {
                l1: LevelState::new(),
                l2: LevelState::new(),
                l3: LevelState::new(),
                l4: LevelState::new(),
                l5: LevelState::new(),
                order_timestamps: VecDeque::new(),
            }),
        }
    }

    /// Evaluate a proposed order against all breaker levels.
    ///
    /// Levels are checked in ascending severity (L1 through L5). The highest
    /// triggered level determines the action.
    pub fn check(
        &self,
        order: &NewOrder,
        portfolio: &PortfolioRisk,
    ) -> CircuitBreakerResult {
        let mut state = self.state.lock();
        let now = Utc::now();

        // ------------------------------------------------------------------
        // Before evaluating, transition any expired cooldowns back to Active.
        // ------------------------------------------------------------------
        self.maybe_expire_cooldown(&mut state.l2, self.config.l2.cooldown_seconds, now);
        self.maybe_expire_cooldown(&mut state.l3, self.config.l3.cooldown_seconds, now);

        // ------------------------------------------------------------------
        // L5: System halt (sticky -- must be manually reset)
        // ------------------------------------------------------------------
        if state.l5.is_blocking() {
            return CircuitBreakerResult {
                level: Some(BreakerLevel::System),
                action: RiskAction::KillSwitch {
                    reason: "L5 system circuit breaker active -- manual reset required".into(),
                },
            };
        }

        // Check if L5 should trip based on catastrophic loss.
        if portfolio.daily_pnl <= -self.config.l5.max_catastrophic_loss {
            warn!(
                daily_pnl = %portfolio.daily_pnl,
                threshold = %self.config.l5.max_catastrophic_loss,
                "L5 SYSTEM circuit breaker TRIGGERED"
            );
            state.l5.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::System),
                action: RiskAction::KillSwitch {
                    reason: format!(
                        "catastrophic loss: daily PnL {} exceeds -{}",
                        portfolio.daily_pnl, self.config.l5.max_catastrophic_loss,
                    ),
                },
            };
        }

        // ------------------------------------------------------------------
        // L4: Position limit
        // ------------------------------------------------------------------
        if state.l4.is_blocking() {
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Position),
                action: RiskAction::Reject {
                    reason: "L4 position circuit breaker active".into(),
                },
            };
        }

        if portfolio.gross_exposure > self.config.l4.max_gross_exposure {
            warn!(
                gross_exposure = %portfolio.gross_exposure,
                threshold = %self.config.l4.max_gross_exposure,
                "L4 position circuit breaker TRIGGERED"
            );
            state.l4.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Position),
                action: RiskAction::Reject {
                    reason: format!(
                        "gross exposure {} exceeds L4 limit {}",
                        portfolio.gross_exposure, self.config.l4.max_gross_exposure,
                    ),
                },
            };
        }

        // Per-symbol position check.
        for pos in &portfolio.positions {
            if pos.quantity.abs() > self.config.l4.max_position_per_symbol {
                warn!(
                    symbol = %pos.symbol,
                    position = %pos.quantity,
                    threshold = %self.config.l4.max_position_per_symbol,
                    "L4 per-symbol position circuit breaker TRIGGERED"
                );
                state.l4.trip();
                return CircuitBreakerResult {
                    level: Some(BreakerLevel::Position),
                    action: RiskAction::Reject {
                        reason: format!(
                            "position {} for {} exceeds L4 per-symbol limit {}",
                            pos.quantity.abs(),
                            pos.symbol,
                            self.config.l4.max_position_per_symbol,
                        ),
                    },
                };
            }
        }

        // ------------------------------------------------------------------
        // L3: PnL / drawdown
        // ------------------------------------------------------------------
        if state.l3.is_blocking() {
            return CircuitBreakerResult {
                level: Some(BreakerLevel::PnL),
                action: RiskAction::Reject {
                    reason: "L3 PnL circuit breaker active (strategy paused)".into(),
                },
            };
        }

        if portfolio.daily_pnl <= -self.config.l3.max_daily_loss {
            warn!(
                daily_pnl = %portfolio.daily_pnl,
                threshold = %self.config.l3.max_daily_loss,
                "L3 PnL circuit breaker TRIGGERED"
            );
            state.l3.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::PnL),
                action: RiskAction::Reject {
                    reason: format!(
                        "daily loss {} exceeds L3 limit -{}",
                        portfolio.daily_pnl, self.config.l3.max_daily_loss,
                    ),
                },
            };
        }

        if portfolio.gross_exposure > Decimal::ZERO {
            let drawdown_pct = portfolio.max_drawdown.abs() / portfolio.gross_exposure;
            if drawdown_pct > self.config.l3.max_drawdown_pct {
                warn!(
                    drawdown_pct = %drawdown_pct,
                    threshold = %self.config.l3.max_drawdown_pct,
                    "L3 drawdown circuit breaker TRIGGERED"
                );
                state.l3.trip();
                return CircuitBreakerResult {
                    level: Some(BreakerLevel::PnL),
                    action: RiskAction::Reject {
                        reason: format!(
                            "drawdown {}% exceeds L3 limit {}%",
                            drawdown_pct * Decimal::from(100),
                            self.config.l3.max_drawdown_pct * Decimal::from(100),
                        ),
                    },
                };
            }
        }

        // ------------------------------------------------------------------
        // L2: Rate limit
        // ------------------------------------------------------------------
        if state.l2.is_blocking() {
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Rate),
                action: RiskAction::Reject {
                    reason: "L2 rate circuit breaker active (throttling)".into(),
                },
            };
        }

        // Prune timestamps older than 1 second.
        let one_second_ago = now - Duration::seconds(1);
        while let Some(front) = state.order_timestamps.front() {
            if *front < one_second_ago {
                state.order_timestamps.pop_front();
            } else {
                break;
            }
        }

        if state.order_timestamps.len() >= self.config.l2.max_orders_per_second as usize {
            warn!(
                orders_per_second = state.order_timestamps.len(),
                threshold = self.config.l2.max_orders_per_second,
                "L2 rate circuit breaker TRIGGERED"
            );
            state.l2.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Rate),
                action: RiskAction::Reject {
                    reason: format!(
                        "order rate {} /s exceeds L2 limit {}",
                        state.order_timestamps.len(),
                        self.config.l2.max_orders_per_second,
                    ),
                },
            };
        }

        // ------------------------------------------------------------------
        // L1: Single order size
        // ------------------------------------------------------------------
        if order.quantity > self.config.l1.max_order_quantity {
            debug!(
                quantity = %order.quantity,
                threshold = %self.config.l1.max_order_quantity,
                "L1 order size circuit breaker TRIGGERED"
            );
            state.l1.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Order),
                action: RiskAction::Reject {
                    reason: format!(
                        "order quantity {} exceeds L1 limit {}",
                        order.quantity, self.config.l1.max_order_quantity,
                    ),
                },
            };
        }

        let notional = order.quantity * order.price.unwrap_or(Decimal::ZERO);
        if notional > self.config.l1.max_order_notional {
            debug!(
                notional = %notional,
                threshold = %self.config.l1.max_order_notional,
                "L1 order notional circuit breaker TRIGGERED"
            );
            state.l1.trip();
            return CircuitBreakerResult {
                level: Some(BreakerLevel::Order),
                action: RiskAction::Reject {
                    reason: format!(
                        "order notional {} exceeds L1 limit {}",
                        notional, self.config.l1.max_order_notional,
                    ),
                },
            };
        }

        // ------------------------------------------------------------------
        // All clear -- record this order timestamp for L2 rate tracking.
        // ------------------------------------------------------------------
        state.order_timestamps.push_back(now);

        CircuitBreakerResult {
            level: None,
            action: RiskAction::Allow,
        }
    }

    /// Record an external order submission for L2 rate tracking (useful when
    /// orders bypass the circuit breaker check path).
    pub fn record_order(&self) {
        let mut state = self.state.lock();
        state.order_timestamps.push_back(Utc::now());
    }

    /// Manually reset a specific breaker level.
    ///
    /// For L5, this is the *only* way to restore normal operation after a
    /// system halt.
    pub fn reset(&self, level: BreakerLevel) {
        let mut state = self.state.lock();
        let ls = match level {
            BreakerLevel::Order => &mut state.l1,
            BreakerLevel::Rate => &mut state.l2,
            BreakerLevel::PnL => &mut state.l3,
            BreakerLevel::Position => &mut state.l4,
            BreakerLevel::System => &mut state.l5,
        };
        warn!(%level, "circuit breaker manually reset");
        ls.reset();
    }

    /// Reset all breaker levels. Use with caution.
    pub fn reset_all(&self) {
        warn!("ALL circuit breaker levels manually reset");
        let mut state = self.state.lock();
        state.l1.reset();
        state.l2.reset();
        state.l3.reset();
        state.l4.reset();
        state.l5.reset();
    }

    /// Query the current state of a specific level.
    pub fn level_state(&self, level: BreakerLevel) -> BreakerState {
        let state = self.state.lock();
        match level {
            BreakerLevel::Order => state.l1.state,
            BreakerLevel::Rate => state.l2.state,
            BreakerLevel::PnL => state.l3.state,
            BreakerLevel::Position => state.l4.state,
            BreakerLevel::System => state.l5.state,
        }
    }

    /// Return the cumulative trigger count for a specific level.
    pub fn trigger_count(&self, level: BreakerLevel) -> u64 {
        let state = self.state.lock();
        match level {
            BreakerLevel::Order => state.l1.trigger_count,
            BreakerLevel::Rate => state.l2.trigger_count,
            BreakerLevel::PnL => state.l3.trigger_count,
            BreakerLevel::Position => state.l4.trigger_count,
            BreakerLevel::System => state.l5.trigger_count,
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Transition a level from `Triggered` to `Cooldown`, and from `Cooldown`
    /// back to `Active` once the cooldown duration expires.
    fn maybe_expire_cooldown(
        &self,
        ls: &mut LevelState,
        cooldown_secs: u64,
        now: DateTime<Utc>,
    ) {
        match ls.state {
            BreakerState::Triggered => {
                // Move to cooldown immediately after trigger.
                ls.cooldown();
            }
            BreakerState::Cooldown => {
                if let Some(triggered_at) = ls.triggered_at {
                    let elapsed = now - triggered_at;
                    if elapsed >= Duration::seconds(cooldown_secs as i64) {
                        info!("circuit breaker cooldown expired, resetting to Active");
                        ls.reset();
                    }
                }
            }
            BreakerState::Active => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{Exchange, OrderType, Side, Symbol, TimeInForce};
    use rust_decimal_macros::dec;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            l1: L1Config {
                max_order_quantity: dec!(10),
                max_order_notional: dec!(500_000),
            },
            l2: L2Config {
                max_orders_per_second: 5,
                cooldown_seconds: 5,
            },
            l3: L3Config {
                max_daily_loss: dec!(10_000),
                max_drawdown_pct: dec!(0.05),
                cooldown_seconds: 60,
            },
            l4: L4Config {
                max_gross_exposure: dec!(1_000_000),
                max_position_per_symbol: dec!(50),
            },
            l5: L5Config {
                max_catastrophic_loss: dec!(50_000),
            },
        }
    }

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

    #[test]
    fn allows_normal_order() {
        let cb = CircuitBreaker::new(test_config());
        let result = cb.check(&sample_order(), &empty_portfolio());
        assert!(result.level.is_none());
        assert!(matches!(result.action, RiskAction::Allow));
    }

    #[test]
    fn l1_rejects_oversized_order() {
        let cb = CircuitBreaker::new(test_config());
        let mut order = sample_order();
        order.quantity = dec!(20); // exceeds max_order_quantity of 10
        let result = cb.check(&order, &empty_portfolio());
        assert_eq!(result.level, Some(BreakerLevel::Order));
        assert!(matches!(result.action, RiskAction::Reject { .. }));
    }

    #[test]
    fn l1_rejects_high_notional_order() {
        let cb = CircuitBreaker::new(test_config());
        let mut order = sample_order();
        order.quantity = dec!(10);
        order.price = Some(dec!(60000)); // notional = 600_000 > 500_000
        let result = cb.check(&order, &empty_portfolio());
        assert_eq!(result.level, Some(BreakerLevel::Order));
        assert!(matches!(result.action, RiskAction::Reject { .. }));
    }

    #[test]
    fn l3_rejects_on_daily_loss() {
        let cb = CircuitBreaker::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-15_000); // exceeds max_daily_loss of 10_000
        let result = cb.check(&sample_order(), &portfolio);
        assert_eq!(result.level, Some(BreakerLevel::PnL));
        assert!(matches!(result.action, RiskAction::Reject { .. }));
    }

    #[test]
    fn l5_triggers_kill_switch() {
        let cb = CircuitBreaker::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-60_000); // exceeds max_catastrophic_loss of 50_000
        let result = cb.check(&sample_order(), &portfolio);
        assert_eq!(result.level, Some(BreakerLevel::System));
        assert!(matches!(result.action, RiskAction::KillSwitch { .. }));
    }

    #[test]
    fn l5_requires_manual_reset() {
        let cb = CircuitBreaker::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-60_000);

        // Trip L5
        let _ = cb.check(&sample_order(), &portfolio);

        // Even with a healthy portfolio, L5 stays triggered.
        let result = cb.check(&sample_order(), &empty_portfolio());
        assert_eq!(result.level, Some(BreakerLevel::System));
        assert!(matches!(result.action, RiskAction::KillSwitch { .. }));

        // Manual reset clears it.
        cb.reset(BreakerLevel::System);
        let result = cb.check(&sample_order(), &empty_portfolio());
        assert!(result.level.is_none());
        assert!(matches!(result.action, RiskAction::Allow));
    }

    #[test]
    fn reset_all_clears_everything() {
        let cb = CircuitBreaker::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-60_000);
        let _ = cb.check(&sample_order(), &portfolio);

        cb.reset_all();
        let result = cb.check(&sample_order(), &empty_portfolio());
        assert!(matches!(result.action, RiskAction::Allow));
    }

    #[test]
    fn trigger_count_increments() {
        let cb = CircuitBreaker::new(test_config());
        let mut order = sample_order();
        order.quantity = dec!(20);

        let _ = cb.check(&order, &empty_portfolio());
        assert_eq!(cb.trigger_count(BreakerLevel::Order), 1);

        cb.reset(BreakerLevel::Order);
        let _ = cb.check(&order, &empty_portfolio());
        assert_eq!(cb.trigger_count(BreakerLevel::Order), 2);
    }
}
