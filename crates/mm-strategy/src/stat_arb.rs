use std::collections::{HashSet, VecDeque};

use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderStatus, OrderType,
    Side, Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Approximate square root for `Decimal` using Newton's method (20 iterations).
fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut guess = value / dec!(2);
    for _ in 0..20 {
        guess = (guess + value / guess) / dec!(2);
    }
    guess
}

/// Compute rolling mean and standard deviation over the given spread window.
///
/// Returns `(mean, std_dev)`.
fn rolling_stats(spreads: &VecDeque<Decimal>) -> (Decimal, Decimal) {
    let count = Decimal::from(spreads.len());
    let mean = spreads.iter().copied().sum::<Decimal>() / count;
    let variance = spreads
        .iter()
        .map(|s| (*s - mean) * (*s - mean))
        .sum::<Decimal>()
        / count;
    let std_dev = decimal_sqrt(variance);
    (mean, std_dev)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Statistical Arbitrage trading strategy.
///
/// Trades the spread between two correlated instruments across exchanges.
/// Tracks the price ratio/spread, computes z-score, enters when |z| exceeds
/// the entry threshold, and exits when |z| falls below the exit threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatArbConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Exchange for leg A.
    pub leg_a_exchange: Exchange,
    /// Symbol for leg A.
    pub leg_a_symbol: Symbol,
    /// Exchange for leg B.
    pub leg_b_exchange: Exchange,
    /// Symbol for leg B.
    pub leg_b_symbol: Symbol,
    /// Number of spread observations in the rolling window.
    pub lookback_periods: usize,
    /// Z-score threshold to enter a position.
    pub entry_z_score: Decimal,
    /// Z-score threshold to exit a position (must be less than entry).
    pub exit_z_score: Decimal,
    /// Order size for each entry/exit order.
    pub order_size: Decimal,
    /// Maximum absolute position size.
    pub max_position: Decimal,
}

impl StatArbConfig {
    /// Validate logical constraints on the configuration parameters.
    pub fn validate(&self) -> Result<(), String> {
        if self.lookback_periods < 2 {
            return Err("lookback_periods must be at least 2".to_string());
        }
        if self.entry_z_score <= Decimal::ZERO {
            return Err("entry_z_score must be positive".to_string());
        }
        if self.exit_z_score < Decimal::ZERO {
            return Err("exit_z_score must be non-negative".to_string());
        }
        if self.exit_z_score >= self.entry_z_score {
            return Err("exit_z_score must be less than entry_z_score".to_string());
        }
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be positive".to_string());
        }
        if self.max_position <= Decimal::ZERO {
            return Err("max_position must be positive".to_string());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

/// Statistical Arbitrage trading strategy.
///
/// Maintains a sliding window of spread values (leg_a - leg_b) and computes
/// z-scores to detect when the spread deviates from its historical mean.
/// Enters when |z| > `entry_z_score` and exits when |z| < `exit_z_score`.
///
/// For simplicity, orders are only placed on leg A. In a production stat arb
/// strategy, both legs would be traded simultaneously.
pub struct StatArbStrategy {
    config: StatArbConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Sliding window of recent spread values (leg_a - leg_b).
    spread_history: VecDeque<Decimal>,
    /// Latest mid price for leg A.
    leg_a_price: Option<Decimal>,
    /// Latest mid price for leg B.
    leg_b_price: Option<Decimal>,
    /// Whether a position is currently open.
    position_open: bool,
    /// Side of leg_a in the open position (Buy = long spread, Sell = short spread).
    position_side: Option<Side>,
    /// Set of client order IDs currently in-flight.
    live_order_ids: HashSet<String>,
}

impl StatArbStrategy {
    /// Create a new Statistical Arbitrage strategy from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `lookback_periods` is less than 2 or if `exit_z_score >= entry_z_score`.
    pub fn new(config: StatArbConfig) -> Self {
        assert!(
            config.lookback_periods >= 2,
            "lookback_periods must be >= 2"
        );
        assert!(
            config.exit_z_score < config.entry_z_score,
            "exit_z_score must be less than entry_z_score"
        );

        info!(
            strategy_id = %config.strategy_id,
            leg_a_exchange = %config.leg_a_exchange,
            leg_a_symbol = %config.leg_a_symbol,
            leg_b_exchange = %config.leg_b_exchange,
            leg_b_symbol = %config.leg_b_symbol,
            lookback_periods = config.lookback_periods,
            entry_z_score = %config.entry_z_score,
            exit_z_score = %config.exit_z_score,
            order_size = %config.order_size,
            max_position = %config.max_position,
            "initializing Statistical Arbitrage trading strategy"
        );

        Self {
            config,
            active: true,
            spread_history: VecDeque::new(),
            leg_a_price: None,
            leg_b_price: None,
            position_open: false,
            position_side: None,
            live_order_ids: HashSet::new(),
        }
    }

    /// Determine which leg a snapshot belongs to.
    ///
    /// Returns `Some('A')` for leg A, `Some('B')` for leg B, or `None` if it
    /// matches neither.
    fn identify_leg(&self, snapshot: &MarketSnapshot) -> Option<char> {
        if snapshot.exchange == self.config.leg_a_exchange
            && snapshot.symbol == self.config.leg_a_symbol
        {
            Some('A')
        } else if snapshot.exchange == self.config.leg_b_exchange
            && snapshot.symbol == self.config.leg_b_symbol
        {
            Some('B')
        } else {
            None
        }
    }

    /// Build the cancel-all action for this strategy's orders on leg A.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.leg_a_exchange),
            symbol: Some(self.config.leg_a_symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Build a new limit order action on leg A with the given side and price.
    fn build_order(&self, side: Side, price: Decimal, reduce_only: bool) -> OrderAction {
        let client_order_id = Uuid::new_v4().to_string();
        debug!(
            strategy_id = %self.config.strategy_id,
            side = %side,
            quantity = %self.config.order_size,
            price = %price,
            client_order_id = %client_order_id,
            reduce_only = reduce_only,
            "building stat arb order on leg A"
        );

        OrderAction::Place(NewOrder {
            exchange: self.config.leg_a_exchange,
            symbol: self.config.leg_a_symbol.clone(),
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::IOC,
            price: Some(price),
            quantity: self.config.order_size,
            strategy_id: self.config.strategy_id.clone(),
            reduce_only,
        })
    }
}

impl Strategy for StatArbStrategy {
    fn name(&self) -> &str {
        "stat_arb"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn exchange(&self) -> Exchange {
        self.config.leg_a_exchange
    }

    fn symbol(&self) -> Symbol {
        self.config.leg_a_symbol.clone()
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        // Step 1: Determine which leg this tick is for.
        let leg = match self.identify_leg(snapshot) {
            Some(l) => l,
            None => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    exchange = %snapshot.exchange,
                    symbol = %snapshot.symbol,
                    "tick does not match either leg, skipping"
                );
                return Vec::new();
            }
        };

        // Step 2: Get mid price and update the appropriate leg.
        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    leg = %leg,
                    "no mid price available, skipping tick"
                );
                return Vec::new();
            }
        };

        match leg {
            'A' => self.leg_a_price = Some(mid),
            'B' => self.leg_b_price = Some(mid),
            _ => unreachable!(),
        }

        // Step 3: If both legs don't have prices yet, return empty.
        let (price_a, price_b) = match (self.leg_a_price, self.leg_b_price) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    leg_a_price = ?self.leg_a_price,
                    leg_b_price = ?self.leg_b_price,
                    "waiting for both legs to have prices"
                );
                return Vec::new();
            }
        };

        // Step 4: Compute spread = leg_a_price - leg_b_price
        let spread = price_a - price_b;

        // Step 5: Push spread to history (capped at lookback_periods).
        self.spread_history.push_back(spread);
        while self.spread_history.len() > self.config.lookback_periods {
            self.spread_history.pop_front();
        }

        // Step 6: If history not full, return empty.
        if self.spread_history.len() < self.config.lookback_periods {
            debug!(
                strategy_id = %self.config.strategy_id,
                history_len = self.spread_history.len(),
                lookback = self.config.lookback_periods,
                "spread history not full, waiting"
            );
            return Vec::new();
        }

        // Step 7: Compute z-score = (current_spread - mean) / std_dev
        let (mean, std_dev) = rolling_stats(&self.spread_history);
        if std_dev == Decimal::ZERO {
            debug!(
                strategy_id = %self.config.strategy_id,
                "std_dev is zero, cannot compute z-score"
            );
            return Vec::new();
        }
        let z_score = (spread - mean) / std_dev;

        debug!(
            strategy_id = %self.config.strategy_id,
            spread = %spread,
            mean = %mean,
            std_dev = %std_dev,
            z_score = %z_score,
            position_open = self.position_open,
            position_side = ?self.position_side,
            "stat arb tick"
        );

        // Step 9: If position open and |z| < exit_z_score -> exit.
        if self.position_open {
            if z_score.abs() < self.config.exit_z_score {
                let exit_side = match self.position_side {
                    Some(Side::Buy) => Side::Sell,
                    Some(Side::Sell) => Side::Buy,
                    None => return Vec::new(),
                };
                info!(
                    strategy_id = %self.config.strategy_id,
                    z_score = %z_score,
                    exit_z_score = %self.config.exit_z_score,
                    exit_side = %exit_side,
                    "stat arb exit triggered, z-score reverted"
                );
                return vec![self.build_order(exit_side, price_a, true)];
            }
            // In position but no exit signal -- hold.
            return Vec::new();
        }

        // Step 8 & 10: No position -- check entry conditions, respecting max_position.
        let current_position_abs = snapshot.position.quantity.abs();
        if current_position_abs >= self.config.max_position {
            debug!(
                strategy_id = %self.config.strategy_id,
                current_position = %snapshot.position.quantity,
                max_position = %self.config.max_position,
                "max position reached, skipping entry"
            );
            return Vec::new();
        }

        if z_score > self.config.entry_z_score {
            // Spread is high -> sell leg_a (expect spread to narrow).
            info!(
                strategy_id = %self.config.strategy_id,
                z_score = %z_score,
                entry_z_score = %self.config.entry_z_score,
                "spread too high, selling leg A (short spread)"
            );
            return vec![self.build_order(Side::Sell, price_a, false)];
        }

        if z_score < -self.config.entry_z_score {
            // Spread is low -> buy leg_a (expect spread to widen).
            info!(
                strategy_id = %self.config.strategy_id,
                z_score = %z_score,
                entry_z_score = %self.config.entry_z_score,
                "spread too low, buying leg A (long spread)"
            );
            return vec![self.build_order(Side::Buy, price_a, false)];
        }

        Vec::new()
    }

    fn on_fill(&mut self, fill: &Fill) {
        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            position_open = self.position_open,
            "Stat Arb strategy received fill"
        );

        // Remove the order from live tracking.
        self.live_order_ids.remove(&fill.client_order_id);

        if self.position_open {
            // Exit fill -- clear position state.
            info!(
                strategy_id = %self.config.strategy_id,
                "exit fill received, clearing position state"
            );
            self.position_open = false;
            self.position_side = None;
        } else {
            // Entry fill -- set position state.
            let side = fill.side;
            self.position_open = true;
            self.position_side = Some(side);
            info!(
                strategy_id = %self.config.strategy_id,
                side = %side,
                entry_price = %fill.price,
                "entry fill received, position established"
            );
        }
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "Stat Arb strategy received order update"
        );

        match update.status {
            OrderStatus::Pending | OrderStatus::Submitted | OrderStatus::New => {
                self.live_order_ids.insert(update.client_order_id.clone());
            }
            OrderStatus::Filled
            | OrderStatus::Cancelled
            | OrderStatus::Rejected
            | OrderStatus::Expired => {
                self.live_order_ids.remove(&update.client_order_id);
            }
            OrderStatus::PartiallyFilled | OrderStatus::PendingCancel => {
                // Order is still live.
            }
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            position_open = self.position_open,
            position_side = ?self.position_side,
            live_orders = self.live_order_ids.len(),
            "stopping Statistical Arbitrage trading strategy"
        );
        self.active = false;
        vec![self.cancel_all_action()]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::types::{OrderBook, Position, PriceLevel};

    /// Helper: build a default StatArbConfig for testing.
    fn test_config() -> StatArbConfig {
        StatArbConfig {
            strategy_id: "stat-arb-test-1".to_string(),
            leg_a_exchange: Exchange::Binance,
            leg_a_symbol: Symbol::new("BTC", "USDT"),
            leg_b_exchange: Exchange::Coinbase,
            leg_b_symbol: Symbol::new("BTC", "USD"),
            lookback_periods: 5,
            entry_z_score: dec!(2),
            exit_z_score: dec!(0.5),
            order_size: dec!(1),
            max_position: dec!(5),
        }
    }

    /// Helper: build a MarketSnapshot for a specific exchange/symbol with a
    /// given mid price and optional position quantity.
    fn make_snapshot(
        exchange: Exchange,
        symbol: Symbol,
        mid: Decimal,
        position_qty: Decimal,
    ) -> MarketSnapshot {
        let half_spread = dec!(0.5);
        let bid = mid - half_spread;
        let ask = mid + half_spread;
        let ts = chrono::Utc::now();

        let mut position = Position::new(exchange, symbol.clone());
        position.quantity = position_qty;

        MarketSnapshot {
            exchange,
            symbol: symbol.clone(),
            order_book: std::sync::Arc::new(OrderBook {
                exchange,
                symbol,
                bids: vec![PriceLevel {
                    price: bid,
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: ask,
                    quantity: dec!(10),
                }],
                timestamp: ts,
                sequence: 1,
            }),
            last_trade: None,
            position,
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Helper: build a snapshot for leg A.
    fn make_leg_a_snapshot(mid: Decimal, position_qty: Decimal) -> MarketSnapshot {
        make_snapshot(
            Exchange::Binance,
            Symbol::new("BTC", "USDT"),
            mid,
            position_qty,
        )
    }

    /// Helper: build a snapshot for leg B.
    fn make_leg_b_snapshot(mid: Decimal) -> MarketSnapshot {
        make_snapshot(
            Exchange::Coinbase,
            Symbol::new("BTC", "USD"),
            mid,
            Decimal::ZERO,
        )
    }

    /// Helper: build a Fill with the given side, price, and quantity.
    fn make_fill(side: Side, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            fill_id: "fill-1".to_string(),
            client_order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Helper: seed the strategy's spread history directly for controlled tests.
    ///
    /// Sets both leg prices so the strategy is ready to compute new spreads,
    /// and pre-populates `spread_history` with the given values.
    fn seed_spread_history(
        strategy: &mut StatArbStrategy,
        spreads: &[Decimal],
        leg_a_price: Decimal,
        leg_b_price: Decimal,
    ) {
        strategy.leg_a_price = Some(leg_a_price);
        strategy.leg_b_price = Some(leg_b_price);
        strategy.spread_history.clear();
        for &s in spreads {
            strategy.spread_history.push_back(s);
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: Constructor initializes correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_new_initializes_correctly() {
        let config = test_config();
        let strategy = StatArbStrategy::new(config.clone());

        assert!(strategy.active);
        assert!(strategy.spread_history.is_empty());
        assert!(strategy.leg_a_price.is_none());
        assert!(strategy.leg_b_price.is_none());
        assert!(!strategy.position_open);
        assert!(strategy.position_side.is_none());
        assert!(strategy.live_order_ids.is_empty());
        assert_eq!(strategy.name(), "stat_arb");
        assert_eq!(strategy.strategy_id(), "stat-arb-test-1");
    }

    // -----------------------------------------------------------------------
    // Test 2: No action when only one leg has price
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_only_one_leg_has_price() {
        let config = test_config();
        let mut strategy = StatArbStrategy::new(config);

        // Only feed leg A
        let snap_a = make_leg_a_snapshot(dec!(100), Decimal::ZERO);
        let actions = strategy.on_tick(&snap_a);
        assert!(actions.is_empty());
        assert!(strategy.leg_a_price.is_some());
        assert!(strategy.leg_b_price.is_none());

        // Spread history should still be empty
        assert!(strategy.spread_history.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 3: No action when spread_history not full
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_spread_history_not_full() {
        let config = test_config(); // lookback_periods = 5
        let mut strategy = StatArbStrategy::new(config);

        // Seed with only 3 spreads (need 5 to be full).
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1)],
            dec!(100),
            dec!(99),
        );

        // Feed a leg B tick to add one more spread (total 4, still < 5).
        let snap_b = make_leg_b_snapshot(dec!(99));
        let actions = strategy.on_tick(&snap_b);
        assert!(actions.is_empty());
        assert_eq!(strategy.spread_history.len(), 4);
    }

    // -----------------------------------------------------------------------
    // Test 4: Enter short spread (sell leg_a) when z > entry_z_score
    // -----------------------------------------------------------------------
    #[test]
    fn test_enter_short_spread_when_z_positive() {
        let mut config = test_config();
        config.lookback_periods = 5;
        config.entry_z_score = dec!(1.5);
        let mut strategy = StatArbStrategy::new(config);

        // Pre-seed 4 spreads of 1, with leg_a=110, leg_b=100 so the next
        // tick will compute spread = 10.
        // After the tick: history = [1, 1, 1, 1, 10]
        // Mean = (1+1+1+1+10)/5 = 14/5 = 2.8
        // Variance = ((1-2.8)^2*4 + (10-2.8)^2)/5 = (12.96+51.84)/5 = 12.96
        // StdDev = sqrt(12.96) ~= 3.6
        // z = (10 - 2.8) / 3.6 = 2.0 > 1.5 -> sell leg A
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1), dec!(1)],
            dec!(110),
            dec!(100),
        );

        // Feed a leg B tick that keeps spread = 110 - 100 = 10
        let snap_b = make_leg_b_snapshot(dec!(100));
        let actions = strategy.on_tick(&snap_b);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell);
                assert_eq!(order.quantity, dec!(1));
                assert_eq!(order.exchange, Exchange::Binance);
                assert_eq!(order.symbol, Symbol::new("BTC", "USDT"));
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.time_in_force, TimeInForce::IOC);
                assert!(!order.reduce_only);
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: Enter long spread (buy leg_a) when z < -entry_z_score
    // -----------------------------------------------------------------------
    #[test]
    fn test_enter_long_spread_when_z_negative() {
        let mut config = test_config();
        config.lookback_periods = 5;
        config.entry_z_score = dec!(1.5);
        let mut strategy = StatArbStrategy::new(config);

        // Pre-seed 4 spreads of 1, with leg_a=91, leg_b=99 so the next
        // tick will compute spread = 91 - 99 = -8.
        // After the tick: history = [1, 1, 1, 1, -8]
        // Mean = (1+1+1+1-8)/5 = -0.8
        // Variance = ((1.8)^2*4 + (-7.2)^2)/5 = (12.96+51.84)/5 = 12.96
        // StdDev ~= 3.6
        // z = (-8 - (-0.8)) / 3.6 = -2.0 < -1.5 -> buy leg A
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1), dec!(1)],
            dec!(91),
            dec!(99),
        );

        // Feed a leg B tick; spread = 91 - 99 = -8
        let snap_b = make_leg_b_snapshot(dec!(99));
        let actions = strategy.on_tick(&snap_b);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.quantity, dec!(1));
                assert_eq!(order.exchange, Exchange::Binance);
                assert!(!order.reduce_only);
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: Exit when z reverts within exit_z_score
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_when_z_reverts() {
        let mut config = test_config();
        config.lookback_periods = 5;
        config.entry_z_score = dec!(1.5);
        config.exit_z_score = dec!(0.5);
        let mut strategy = StatArbStrategy::new(config);

        // Simulate an open short-spread position (sold leg A).
        strategy.position_open = true;
        strategy.position_side = Some(Side::Sell);

        // Pre-seed spread history: [1, 1, 1, 1, 10]
        // with leg_a=101.5, leg_b=100 so next tick spread = 1.5.
        // After tick: history = [1, 1, 1, 10, 1.5]
        // Mean = (1+1+1+10+1.5)/5 = 14.5/5 = 2.9
        // Var = ((1-2.9)^2*3+(10-2.9)^2+(1.5-2.9)^2)/5
        //     = (3.61*3+50.41+1.96)/5 = (10.83+50.41+1.96)/5 = 63.2/5 = 12.64
        // StdDev = sqrt(12.64) ~= 3.556
        // z = (1.5 - 2.9) / 3.556 = -1.4/3.556 ~= -0.394
        // |z| = 0.394 < 0.5 -> EXIT!
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1), dec!(1), dec!(10)],
            dec!(101.5),
            dec!(100),
        );

        // Feed a leg B tick to trigger the exit check.
        let snap_b = make_leg_b_snapshot(dec!(100));
        let actions = strategy.on_tick(&snap_b);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy); // Exit short spread -> buy leg A
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for exit"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 7: Max position check prevents entry
    // -----------------------------------------------------------------------
    #[test]
    fn test_max_position_prevents_entry() {
        let mut config = test_config();
        config.lookback_periods = 5;
        config.entry_z_score = dec!(1.5);
        config.max_position = dec!(5);
        let mut strategy = StatArbStrategy::new(config);

        // Pre-seed 4 spreads of 1, with leg_a=110, leg_b=100
        // so the next tick will compute spread = 10 -> z > entry.
        // But the snapshot will have position = 5 (at max).
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1), dec!(1)],
            dec!(110),
            dec!(100),
        );

        // Feed a leg A tick with position_qty = 5 (at max_position).
        // Spread = 110 - 100 = 10, z would be 2.0 > 1.5, but max_position blocks.
        let snap_a = make_leg_a_snapshot(dec!(110), dec!(5));
        let actions = strategy.on_tick(&snap_a);

        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 8: stop() cancels all and deactivates
    // -----------------------------------------------------------------------
    #[test]
    fn test_stop_cancels_all_and_deactivates() {
        let config = test_config();
        let mut strategy = StatArbStrategy::new(config);
        assert!(strategy.is_active());

        let actions = strategy.stop();
        assert!(!strategy.is_active());
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Binance));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("stat-arb-test-1".to_string()));
            }
            _ => panic!("expected CancelAll action"),
        }

        // Verify on_tick returns empty when inactive
        let snap = make_leg_a_snapshot(dec!(100), Decimal::ZERO);
        let actions = strategy.on_tick(&snap);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 9: z-score computation correctness
    // -----------------------------------------------------------------------
    #[test]
    fn test_z_score_computation() {
        // Verify that rolling_stats produces correct values.
        // Spreads: [10, 20, 30]
        // Mean = 20
        // Variance = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 3 = 200/3 = 66.666...
        // StdDev = sqrt(66.666...) ~= 8.165
        let spreads: VecDeque<Decimal> = vec![dec!(10), dec!(20), dec!(30)].into_iter().collect();
        let (mean, std_dev) = rolling_stats(&spreads);

        assert_eq!(mean, dec!(20));
        let expected_std = dec!(8.165);
        let diff = (std_dev - expected_std).abs();
        assert!(
            diff < dec!(0.01),
            "std_dev {std_dev} should be close to {expected_std}, diff = {diff}"
        );

        // z-score for current value 30: (30 - 20) / 8.165 ~= 1.225
        let z = (dec!(30) - mean) / std_dev;
        let expected_z = dec!(1.225);
        let z_diff = (z - expected_z).abs();
        assert!(
            z_diff < dec!(0.01),
            "z-score {z} should be close to {expected_z}, diff = {z_diff}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: on_fill entry sets position state, exit clears it
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_fill_entry_and_exit_lifecycle() {
        let config = test_config();
        let mut strategy = StatArbStrategy::new(config);

        // Entry fill
        let entry_fill = make_fill(Side::Sell, dec!(110), dec!(1));
        strategy.on_fill(&entry_fill);
        assert!(strategy.position_open);
        assert_eq!(strategy.position_side, Some(Side::Sell));

        // Exit fill
        let exit_fill = make_fill(Side::Buy, dec!(100), dec!(1));
        strategy.on_fill(&exit_fill);
        assert!(!strategy.position_open);
        assert!(strategy.position_side.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 11: on_order_update tracks live order IDs
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_order_update_tracks_live_orders() {
        let config = test_config();
        let mut strategy = StatArbStrategy::new(config);

        let update_new = OrderUpdate {
            client_order_id: "order-1".to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };

        strategy.on_order_update(&update_new);
        assert!(strategy.live_order_ids.contains("order-1"));

        // Filled -- should be removed
        let update_filled = OrderUpdate {
            client_order_id: "order-1".to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(100),
            last_fill_price: Some(dec!(100)),
            last_fill_quantity: Some(dec!(1)),
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };

        strategy.on_order_update(&update_filled);
        assert!(!strategy.live_order_ids.contains("order-1"));
    }

    // -----------------------------------------------------------------------
    // Test 12: Tick for unrelated exchange/symbol is ignored
    // -----------------------------------------------------------------------
    #[test]
    fn test_unrelated_tick_ignored() {
        let config = test_config();
        let mut strategy = StatArbStrategy::new(config);

        // Feed a tick for an exchange/symbol that doesn't match either leg
        let snap = make_snapshot(
            Exchange::Kraken,
            Symbol::new("ETH", "USD"),
            dec!(3000),
            Decimal::ZERO,
        );
        let actions = strategy.on_tick(&snap);
        assert!(actions.is_empty());
        assert!(strategy.leg_a_price.is_none());
        assert!(strategy.leg_b_price.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 13: Spread history caps at lookback_periods
    // -----------------------------------------------------------------------
    #[test]
    fn test_spread_history_caps_at_lookback() {
        let config = test_config(); // lookback_periods = 5
        let mut strategy = StatArbStrategy::new(config);

        // Seed with 5 spreads and then add 3 more via ticks.
        // After adding, history should still be capped at 5.
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(2), dec!(3), dec!(4), dec!(5)],
            dec!(100),
            dec!(99),
        );

        // Feed 3 more leg B ticks (each produces spread = 100 - 99 = 1)
        for _ in 0..3 {
            let snap_b = make_leg_b_snapshot(dec!(99));
            strategy.on_tick(&snap_b);
        }

        assert_eq!(strategy.spread_history.len(), 5);
        // The oldest entries (1, 2, 3) should have been evicted.
        // Remaining: [4, 5, 1, 1, 1]
        let history: Vec<Decimal> = strategy.spread_history.iter().copied().collect();
        assert_eq!(history, vec![dec!(4), dec!(5), dec!(1), dec!(1), dec!(1)]);
    }

    // -----------------------------------------------------------------------
    // Test 14: No entry when z-score is within bands
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_entry_when_z_within_bands() {
        let config = test_config(); // entry_z_score = 2
        let mut strategy = StatArbStrategy::new(config);

        // Pre-seed 5 identical spreads -> std_dev = 0.
        // With std_dev = 0, z-score cannot be computed, so no action.
        seed_spread_history(
            &mut strategy,
            &[dec!(1), dec!(1), dec!(1), dec!(1), dec!(1)],
            dec!(100),
            dec!(99),
        );

        // Feed a tick that keeps the same spread.
        let snap_b = make_leg_b_snapshot(dec!(99));
        let actions = strategy.on_tick(&snap_b);

        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 15: decimal_sqrt helper correctness
    // -----------------------------------------------------------------------
    #[test]
    fn test_decimal_sqrt() {
        assert_eq!(decimal_sqrt(Decimal::ZERO), Decimal::ZERO);

        let result = decimal_sqrt(dec!(4));
        assert!((result - dec!(2)).abs() < dec!(0.0001));

        let result = decimal_sqrt(dec!(9));
        assert!((result - dec!(3)).abs() < dec!(0.0001));

        let result = decimal_sqrt(dec!(2));
        assert!((result - dec!(1.4142)).abs() < dec!(0.001));

        assert_eq!(decimal_sqrt(dec!(-5)), Decimal::ZERO);
    }

    // -----------------------------------------------------------------------
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_valid_config() {
        let config = test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_lookback_too_small() {
        let mut config = test_config();
        config.lookback_periods = 1;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("lookback_periods must be at least 2"));
    }

    #[test]
    fn test_validate_exit_z_score_gte_entry() {
        let mut config = test_config();
        config.exit_z_score = dec!(2); // equal to entry_z_score
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("exit_z_score must be less than entry_z_score"));
    }

    #[test]
    fn test_validate_zero_order_size() {
        let mut config = test_config();
        config.order_size = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("order_size must be positive"));
    }

    #[test]
    fn test_validate_zero_max_position() {
        let mut config = test_config();
        config.max_position = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("max_position must be positive"));
    }
}
