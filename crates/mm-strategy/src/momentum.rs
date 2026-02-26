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
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Momentum trading strategy.
///
/// Trades in the direction of recent price trends. Enters long when momentum
/// exceeds a configurable threshold, short when momentum falls below the
/// negative threshold. Exits via trailing stop or mean reversion (momentum
/// returning within the exit threshold).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Number of price observations in the lookback window.
    pub lookback_periods: usize,
    /// Momentum threshold in basis points to trigger entry. A Buy is placed
    /// when momentum > entry_threshold_bps; a Sell when momentum <
    /// -entry_threshold_bps.
    pub entry_threshold_bps: Decimal,
    /// Momentum threshold in basis points for mean-reversion exit. If the
    /// absolute momentum falls back within this range, the position is closed.
    pub exit_threshold_bps: Decimal,
    /// Order size for each entry/exit order.
    pub order_size: Decimal,
    /// Trailing stop distance in basis points. For a long position the exit
    /// triggers when the current price drops below
    /// `highest_since_entry * (1 - trailing_stop_bps / 10_000)`. Symmetric
    /// logic applies for short positions.
    pub trailing_stop_bps: Decimal,
    /// Maximum absolute position size. No new entry is placed if the current
    /// position would exceed this limit.
    pub max_position: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

/// Momentum trading strategy.
///
/// Maintains a sliding window of mid prices and computes the rate of change
/// (momentum) in basis points. When momentum exceeds the entry threshold a
/// directional order is placed. Open positions are managed with a trailing
/// stop and a mean-reversion exit.
pub struct MomentumStrategy {
    config: MomentumConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Sliding window of recent mid prices.
    price_history: VecDeque<Decimal>,
    /// Side of the current open position (None when flat).
    current_position_side: Option<Side>,
    /// Entry price of the current position.
    entry_price: Option<Decimal>,
    /// Highest mid price observed since entry (used for long trailing stop).
    highest_since_entry: Option<Decimal>,
    /// Lowest mid price observed since entry (used for short trailing stop).
    lowest_since_entry: Option<Decimal>,
    /// Set of client order IDs currently in-flight.
    live_order_ids: HashSet<String>,
}

impl MomentumStrategy {
    /// Create a new Momentum strategy from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `lookback_periods` is less than 2.
    pub fn new(config: MomentumConfig) -> Self {
        assert!(
            config.lookback_periods >= 2,
            "lookback_periods must be >= 2"
        );

        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            lookback_periods = config.lookback_periods,
            entry_threshold_bps = %config.entry_threshold_bps,
            exit_threshold_bps = %config.exit_threshold_bps,
            order_size = %config.order_size,
            trailing_stop_bps = %config.trailing_stop_bps,
            max_position = %config.max_position,
            "initializing Momentum trading strategy"
        );

        Self {
            config,
            active: true,
            price_history: VecDeque::new(),
            current_position_side: None,
            entry_price: None,
            highest_since_entry: None,
            lowest_since_entry: None,
            live_order_ids: HashSet::new(),
        }
    }

    /// Build the cancel-all action for this strategy's orders.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.exchange),
            symbol: Some(self.config.symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Compute momentum in basis points: (current - oldest) / oldest * 10_000.
    fn compute_momentum(&self) -> Option<Decimal> {
        if self.price_history.len() < self.config.lookback_periods {
            return None;
        }

        let oldest = *self.price_history.front()?;
        let current = *self.price_history.back()?;

        if oldest.is_zero() {
            return None;
        }

        Some((current - oldest) / oldest * dec!(10_000))
    }

    /// Build a new order action with the given side.
    fn build_order(&self, side: Side, reduce_only: bool) -> OrderAction {
        let client_order_id = Uuid::new_v4().to_string();
        debug!(
            strategy_id = %self.config.strategy_id,
            side = %side,
            quantity = %self.config.order_size,
            client_order_id = %client_order_id,
            reduce_only = reduce_only,
            "building momentum order"
        );

        OrderAction::Place(NewOrder {
            exchange: self.config.exchange,
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            price: None,
            quantity: self.config.order_size,
            strategy_id: self.config.strategy_id.clone(),
            reduce_only,
        })
    }

    /// Check whether the trailing stop has been hit for the current position.
    fn trailing_stop_triggered(&self, current_price: Decimal) -> bool {
        match self.current_position_side {
            Some(Side::Buy) => {
                if let Some(highest) = self.highest_since_entry {
                    let stop_level =
                        highest * (Decimal::ONE - self.config.trailing_stop_bps / dec!(10_000));
                    current_price < stop_level
                } else {
                    false
                }
            }
            Some(Side::Sell) => {
                if let Some(lowest) = self.lowest_since_entry {
                    let stop_level =
                        lowest * (Decimal::ONE + self.config.trailing_stop_bps / dec!(10_000));
                    current_price > stop_level
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Check whether the mean-reversion exit condition is met.
    fn mean_reversion_exit(&self, momentum: Decimal) -> bool {
        match self.current_position_side {
            Some(Side::Buy) => momentum <= self.config.exit_threshold_bps,
            Some(Side::Sell) => momentum >= -self.config.exit_threshold_bps,
            None => false,
        }
    }

    /// Return the side needed to close the current position.
    fn exit_side(&self) -> Option<Side> {
        match self.current_position_side {
            Some(Side::Buy) => Some(Side::Sell),
            Some(Side::Sell) => Some(Side::Buy),
            None => None,
        }
    }

    /// Clear all position-related state.
    fn clear_position_state(&mut self) {
        self.current_position_side = None;
        self.entry_price = None;
        self.highest_since_entry = None;
        self.lowest_since_entry = None;
    }
}

impl Strategy for MomentumStrategy {
    fn name(&self) -> &str {
        "momentum"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        // Step 1: Get mid price and push to history
        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping tick"
                );
                return Vec::new();
            }
        };

        self.price_history.push_back(mid);
        while self.price_history.len() > self.config.lookback_periods {
            self.price_history.pop_front();
        }

        // Step 2: Need full window before acting
        if self.price_history.len() < self.config.lookback_periods {
            debug!(
                strategy_id = %self.config.strategy_id,
                history_len = self.price_history.len(),
                lookback = self.config.lookback_periods,
                "price history not full, waiting"
            );
            return Vec::new();
        }

        // Step 3: Compute momentum
        let momentum = match self.compute_momentum() {
            Some(m) => m,
            None => return Vec::new(),
        };

        debug!(
            strategy_id = %self.config.strategy_id,
            momentum_bps = %momentum,
            mid_price = %mid,
            position_side = ?self.current_position_side,
            "momentum tick"
        );

        // Step 5: If in position, check exits first
        if self.current_position_side.is_some() {
            // Update highest/lowest since entry
            if let Some(Side::Buy) = self.current_position_side {
                let highest = self.highest_since_entry.unwrap_or(mid);
                self.highest_since_entry = Some(highest.max(mid));
            }
            if let Some(Side::Sell) = self.current_position_side {
                let lowest = self.lowest_since_entry.unwrap_or(mid);
                self.lowest_since_entry = Some(lowest.min(mid));
            }

            // Check trailing stop
            if self.trailing_stop_triggered(mid) {
                let exit = match self.exit_side() {
                    Some(s) => s,
                    None => return Vec::new(),
                };
                info!(
                    strategy_id = %self.config.strategy_id,
                    exit_side = %exit,
                    mid_price = %mid,
                    highest_since_entry = ?self.highest_since_entry,
                    lowest_since_entry = ?self.lowest_since_entry,
                    "trailing stop triggered, exiting position"
                );
                return vec![self.build_order(exit, true)];
            }

            // Check mean reversion exit
            if self.mean_reversion_exit(momentum) {
                let exit = match self.exit_side() {
                    Some(s) => s,
                    None => return Vec::new(),
                };
                info!(
                    strategy_id = %self.config.strategy_id,
                    exit_side = %exit,
                    momentum_bps = %momentum,
                    "mean reversion exit triggered"
                );
                return vec![self.build_order(exit, true)];
            }

            // In position but no exit signal — hold
            return Vec::new();
        }

        // Step 4/6: No position — check entry conditions
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

        if momentum > self.config.entry_threshold_bps {
            info!(
                strategy_id = %self.config.strategy_id,
                momentum_bps = %momentum,
                threshold = %self.config.entry_threshold_bps,
                "momentum above threshold, entering long"
            );
            return vec![self.build_order(Side::Buy, false)];
        }

        if momentum < -self.config.entry_threshold_bps {
            info!(
                strategy_id = %self.config.strategy_id,
                momentum_bps = %momentum,
                threshold = %self.config.entry_threshold_bps,
                "momentum below negative threshold, entering short"
            );
            return vec![self.build_order(Side::Sell, false)];
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
            position_side = ?self.current_position_side,
            "Momentum strategy received fill"
        );

        // Remove the order from live tracking
        self.live_order_ids.remove(&fill.client_order_id);

        if self.current_position_side.is_some() {
            // Exit fill — clear position state
            info!(
                strategy_id = %self.config.strategy_id,
                "exit fill received, clearing position state"
            );
            self.clear_position_state();
        } else {
            // Entry fill — set position state
            let side = fill.side;
            self.current_position_side = Some(side);
            self.entry_price = Some(fill.price);
            self.highest_since_entry = Some(fill.price);
            self.lowest_since_entry = Some(fill.price);
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
            "Momentum strategy received order update"
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
                // Order is still live
            }
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            position_side = ?self.current_position_side,
            live_orders = self.live_order_ids.len(),
            "stopping Momentum trading strategy"
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

    /// Helper: build a default MomentumConfig for testing.
    fn test_config() -> MomentumConfig {
        MomentumConfig {
            strategy_id: "momentum-test-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            lookback_periods: 5,
            entry_threshold_bps: dec!(50), // 50 bps = 0.5%
            exit_threshold_bps: dec!(10),  // 10 bps for mean-reversion exit
            order_size: dec!(1),
            trailing_stop_bps: dec!(30), // 30 bps trailing stop
            max_position: dec!(5),
        }
    }

    /// Helper: build a MarketSnapshot with a given mid price (bid/ask
    /// centered around it) and optional position quantity.
    fn make_snapshot(mid: Decimal, position_qty: Decimal) -> MarketSnapshot {
        let half_spread = dec!(0.5);
        let bid = mid - half_spread;
        let ask = mid + half_spread;
        let ts = chrono::Utc::now();

        let mut position = Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT"));
        position.quantity = position_qty;

        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: OrderBook {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
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
            },
            last_trade: None,
            position,
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Helper: build a Fill with the given side, price, and quantity.
    fn make_fill(side: Side, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            fill_id: "fill-1".to_string(),
            client_order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Helper: feed a series of mid prices into the strategy and return the
    /// actions from the last tick. All snapshots use a flat position.
    fn feed_prices(strategy: &mut MomentumStrategy, prices: &[Decimal]) -> Vec<OrderAction> {
        let mut actions = Vec::new();
        for &price in prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            actions = strategy.on_tick(&snap);
        }
        actions
    }

    /// Helper: feed prices with a custom position quantity for each tick.
    fn feed_prices_with_position(
        strategy: &mut MomentumStrategy,
        prices: &[Decimal],
        position_qty: Decimal,
    ) -> Vec<OrderAction> {
        let mut actions = Vec::new();
        for &price in prices {
            let snap = make_snapshot(price, position_qty);
            actions = strategy.on_tick(&snap);
        }
        actions
    }

    // -----------------------------------------------------------------------
    // Test 1: Constructor initializes correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_new_initializes_correctly() {
        let config = test_config();
        let strategy = MomentumStrategy::new(config.clone());

        assert!(strategy.active);
        assert!(strategy.price_history.is_empty());
        assert!(strategy.current_position_side.is_none());
        assert!(strategy.entry_price.is_none());
        assert!(strategy.highest_since_entry.is_none());
        assert!(strategy.lowest_since_entry.is_none());
        assert!(strategy.live_order_ids.is_empty());
        assert_eq!(strategy.name(), "momentum");
        assert_eq!(strategy.strategy_id(), "momentum-test-1");
    }

    // -----------------------------------------------------------------------
    // Test 2: No action when price_history not full
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_history_not_full() {
        let config = test_config(); // lookback_periods = 5
        let mut strategy = MomentumStrategy::new(config);

        // Feed only 3 prices (need 5)
        let prices = [dec!(100), dec!(101), dec!(102)];
        let actions = feed_prices(&mut strategy, &prices);

        assert!(actions.is_empty());
        assert_eq!(strategy.price_history.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 3: Long entry when momentum > threshold
    // -----------------------------------------------------------------------
    #[test]
    fn test_long_entry_when_momentum_above_threshold() {
        let config = test_config(); // entry_threshold = 50 bps, lookback = 5
        let mut strategy = MomentumStrategy::new(config);

        // Prices rising: momentum = (105 - 100) / 100 * 10000 = 500 bps > 50 bps
        let prices = [dec!(100), dec!(101), dec!(102), dec!(103), dec!(105)];
        let actions = feed_prices(&mut strategy, &prices);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.quantity, dec!(1));
                assert_eq!(order.order_type, OrderType::Market);
                assert_eq!(order.time_in_force, TimeInForce::IOC);
                assert!(!order.reduce_only);
                assert_eq!(order.strategy_id, "momentum-test-1");
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: Short entry when momentum < -threshold
    // -----------------------------------------------------------------------
    #[test]
    fn test_short_entry_when_momentum_below_negative_threshold() {
        let config = test_config(); // entry_threshold = 50 bps, lookback = 5
        let mut strategy = MomentumStrategy::new(config);

        // Prices falling: momentum = (95 - 100) / 100 * 10000 = -500 bps < -50 bps
        let prices = [dec!(100), dec!(99), dec!(98), dec!(97), dec!(95)];
        let actions = feed_prices(&mut strategy, &prices);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell);
                assert_eq!(order.quantity, dec!(1));
                assert!(!order.reduce_only);
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: Trailing stop triggers exit for long position
    // -----------------------------------------------------------------------
    #[test]
    fn test_trailing_stop_triggers_exit_long() {
        let config = test_config(); // trailing_stop = 30 bps
        let mut strategy = MomentumStrategy::new(config);

        // First, establish a long position
        let entry_prices = [dec!(100), dec!(101), dec!(102), dec!(103), dec!(105)];
        let actions = feed_prices(&mut strategy, &entry_prices);
        assert_eq!(actions.len(), 1); // Buy order placed

        // Simulate fill
        let fill = make_fill(Side::Buy, dec!(105), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Buy));
        assert_eq!(strategy.entry_price, Some(dec!(105)));

        // Price rises further, then drops to trigger trailing stop
        // highest will be set to 110
        // Trailing stop level = 110 * (1 - 30/10000) = 110 * 0.997 = 109.67
        // Price drops to 109 which is below 109.67 -> trailing stop triggered

        // Feed rising prices first (these update highest_since_entry)
        let snap_up = make_snapshot(dec!(110), Decimal::ZERO);
        strategy.on_tick(&snap_up);
        assert_eq!(strategy.highest_since_entry, Some(dec!(110)));

        // Now feed a price that triggers the trailing stop
        let snap_drop = make_snapshot(dec!(109), Decimal::ZERO);
        let actions = strategy.on_tick(&snap_drop);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell); // Exit long = sell
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for trailing stop exit"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: Trailing stop triggers exit for short position
    // -----------------------------------------------------------------------
    #[test]
    fn test_trailing_stop_triggers_exit_short() {
        let config = test_config(); // trailing_stop = 30 bps
        let mut strategy = MomentumStrategy::new(config);

        // Establish a short position via falling prices
        let entry_prices = [dec!(100), dec!(99), dec!(98), dec!(97), dec!(95)];
        let actions = feed_prices(&mut strategy, &entry_prices);
        assert_eq!(actions.len(), 1); // Sell order placed

        // Simulate fill
        let fill = make_fill(Side::Sell, dec!(95), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Sell));

        // Price drops further, setting lowest to 90
        let snap_down = make_snapshot(dec!(90), Decimal::ZERO);
        strategy.on_tick(&snap_down);
        assert_eq!(strategy.lowest_since_entry, Some(dec!(90)));

        // Trailing stop level = 90 * (1 + 30/10000) = 90 * 1.003 = 90.27
        // Price rises to 91 which is above 90.27 -> trailing stop triggered
        let snap_up = make_snapshot(dec!(91), Decimal::ZERO);
        let actions = strategy.on_tick(&snap_up);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy); // Exit short = buy
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for trailing stop exit"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 7: Mean reversion exit for long position
    // -----------------------------------------------------------------------
    #[test]
    fn test_mean_reversion_exit_long() {
        let mut config = test_config();
        // Use wide trailing stop so it does not trigger first
        config.trailing_stop_bps = dec!(5000);
        config.exit_threshold_bps = dec!(10);
        let mut strategy = MomentumStrategy::new(config);

        // Enter long with strong momentum
        let entry_prices = [dec!(100), dec!(101), dec!(102), dec!(103), dec!(105)];
        let actions = feed_prices(&mut strategy, &entry_prices);
        assert_eq!(actions.len(), 1);

        let fill = make_fill(Side::Buy, dec!(105), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Buy));

        // Now feed prices that bring momentum back to within exit_threshold
        // We need momentum <= 10 bps.
        // After filling the window with roughly flat prices, momentum will be small.
        // The window slides, so we need 5 prices where newest ~ oldest.
        // Feed: 105, 105, 105, 105, 105 -> momentum = (105-105)/105 * 10000 = 0 bps <= 10
        let flat_prices = [dec!(105), dec!(105), dec!(105), dec!(105), dec!(105)];
        let mut actions = Vec::new();
        for &price in &flat_prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            actions = strategy.on_tick(&snap);
        }

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell); // Exit long
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for mean reversion exit"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 8: Mean reversion exit for short position
    // -----------------------------------------------------------------------
    #[test]
    fn test_mean_reversion_exit_short() {
        let mut config = test_config();
        config.trailing_stop_bps = dec!(5000); // wide trailing stop
        config.exit_threshold_bps = dec!(10);
        let mut strategy = MomentumStrategy::new(config);

        // Enter short with negative momentum
        let entry_prices = [dec!(100), dec!(99), dec!(98), dec!(97), dec!(95)];
        let actions = feed_prices(&mut strategy, &entry_prices);
        assert_eq!(actions.len(), 1);

        let fill = make_fill(Side::Sell, dec!(95), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Sell));

        // Feed flat prices so momentum returns to ~0 bps (>= -10)
        let flat_prices = [dec!(95), dec!(95), dec!(95), dec!(95), dec!(95)];
        let mut actions = Vec::new();
        for &price in &flat_prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            actions = strategy.on_tick(&snap);
        }

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy); // Exit short
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for mean reversion exit"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: No entry when max_position would be exceeded
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_entry_when_max_position_exceeded() {
        let config = test_config(); // max_position = 5
        let mut strategy = MomentumStrategy::new(config);

        // Feed rising prices to trigger long entry, but with position already at max
        let prices = [dec!(100), dec!(101), dec!(102), dec!(103), dec!(105)];
        let actions = feed_prices_with_position(&mut strategy, &prices, dec!(5));

        // Should NOT place an order because position is at max
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 10: No entry when max_position exceeded (short side)
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_entry_when_max_position_exceeded_short() {
        let config = test_config(); // max_position = 5
        let mut strategy = MomentumStrategy::new(config);

        // Feed falling prices to trigger short entry, but position is at -5
        let prices = [dec!(100), dec!(99), dec!(98), dec!(97), dec!(95)];
        let actions = feed_prices_with_position(&mut strategy, &prices, dec!(-5));

        // Should NOT place an order because abs(position) is at max
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 11: stop() cancels all and deactivates
    // -----------------------------------------------------------------------
    #[test]
    fn test_stop_cancels_all_and_deactivates() {
        let config = test_config();
        let mut strategy = MomentumStrategy::new(config);
        assert!(strategy.is_active());

        let actions = strategy.stop();
        assert!(!strategy.is_active());
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("momentum-test-1".to_string()));
            }
            _ => panic!("expected CancelAll action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 12: on_tick returns empty when strategy is inactive
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_returns_empty_when_inactive() {
        let config = test_config();
        let mut strategy = MomentumStrategy::new(config);
        strategy.stop();

        let snap = make_snapshot(dec!(100), Decimal::ZERO);
        let actions = strategy.on_tick(&snap);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 13: on_fill entry sets position state, exit clears it
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_fill_entry_and_exit_lifecycle() {
        let config = test_config();
        let mut strategy = MomentumStrategy::new(config);

        // Entry fill
        let entry_fill = make_fill(Side::Buy, dec!(105), dec!(1));
        strategy.on_fill(&entry_fill);

        assert_eq!(strategy.current_position_side, Some(Side::Buy));
        assert_eq!(strategy.entry_price, Some(dec!(105)));
        assert_eq!(strategy.highest_since_entry, Some(dec!(105)));
        assert_eq!(strategy.lowest_since_entry, Some(dec!(105)));

        // Exit fill
        let exit_fill = make_fill(Side::Sell, dec!(110), dec!(1));
        strategy.on_fill(&exit_fill);

        assert!(strategy.current_position_side.is_none());
        assert!(strategy.entry_price.is_none());
        assert!(strategy.highest_since_entry.is_none());
        assert!(strategy.lowest_since_entry.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 14: on_order_update tracks live order IDs
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_order_update_tracks_live_orders() {
        let config = test_config();
        let mut strategy = MomentumStrategy::new(config);

        let update_new = OrderUpdate {
            client_order_id: "order-1".to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Simulated,
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

        // Filled — should be removed
        let update_filled = OrderUpdate {
            client_order_id: "order-1".to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(105),
            last_fill_price: Some(dec!(105)),
            last_fill_quantity: Some(dec!(1)),
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };

        strategy.on_order_update(&update_filled);
        assert!(!strategy.live_order_ids.contains("order-1"));
    }

    // -----------------------------------------------------------------------
    // Test 15: No action when momentum is within threshold (no signal)
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_momentum_within_threshold() {
        let config = test_config(); // entry_threshold = 50 bps, lookback = 5
        let mut strategy = MomentumStrategy::new(config);

        // Flat prices: momentum = (100.01 - 100) / 100 * 10000 = 1 bps (within threshold)
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(100.01)];
        let actions = feed_prices(&mut strategy, &prices);

        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 16: Price history caps at lookback_periods
    // -----------------------------------------------------------------------
    #[test]
    fn test_price_history_caps_at_lookback() {
        let config = test_config(); // lookback = 5
        let mut strategy = MomentumStrategy::new(config);

        // Feed 10 prices — history should be capped at 5
        let prices = [
            dec!(100),
            dec!(101),
            dec!(102),
            dec!(103),
            dec!(104),
            dec!(105),
            dec!(106),
            dec!(107),
            dec!(108),
            dec!(109),
        ];
        for &price in &prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            strategy.on_tick(&snap);
        }

        assert_eq!(strategy.price_history.len(), 5);
        // The oldest should be prices[5] = 105
        assert_eq!(*strategy.price_history.front().unwrap(), dec!(105));
        assert_eq!(*strategy.price_history.back().unwrap(), dec!(109));
    }

    // -----------------------------------------------------------------------
    // Test 17: on_tick returns empty when no mid price (empty order book)
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_no_mid_price() {
        let config = test_config();
        let mut strategy = MomentumStrategy::new(config);

        let mut snapshot = make_snapshot(dec!(100), Decimal::ZERO);
        snapshot.order_book.bids.clear();
        snapshot.order_book.asks.clear();

        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
        assert!(strategy.price_history.is_empty());
    }
}
