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

/// Compute rolling mean and standard deviation over the given price window.
///
/// Returns `(mean, std_dev)`.
fn rolling_stats(prices: &VecDeque<Decimal>) -> (Decimal, Decimal) {
    let count = Decimal::from(prices.len());
    let mean = prices.iter().copied().sum::<Decimal>() / count;
    let variance = prices
        .iter()
        .map(|p| (*p - mean) * (*p - mean))
        .sum::<Decimal>()
        / count;
    let std_dev = decimal_sqrt(variance);
    (mean, std_dev)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Mean Reversion trading strategy.
///
/// Trades against price extremes using Bollinger Band-style logic. Enters long
/// when the mid price falls below the lower band and enters short when it rises
/// above the upper band. Exits when price reverts to the mean (or the opposite
/// band, depending on `exit_at_mean`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Number of price observations in the rolling window.
    pub window_periods: usize,
    /// Number of standard deviations for the upper/lower bands.
    pub num_std_dev: Decimal,
    /// Order size for each entry/exit order.
    pub order_size: Decimal,
    /// Maximum absolute position size. No new entry is placed if the current
    /// position would exceed this limit.
    pub max_position: Decimal,
    /// When `true`, exit positions when price reverts to the mean.
    /// When `false`, exit only when price reaches the opposite band.
    pub exit_at_mean: bool,
}

impl MeanReversionConfig {
    /// Validate logical constraints on the configuration parameters.
    pub fn validate(&self) -> Result<(), String> {
        if self.window_periods < 2 {
            return Err("window_periods must be at least 2".to_string());
        }
        if self.num_std_dev <= Decimal::ZERO {
            return Err("num_std_dev must be positive".to_string());
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

/// Mean Reversion trading strategy.
///
/// Maintains a sliding window of mid prices and computes Bollinger-style bands
/// (rolling mean +/- `num_std_dev` * standard deviation). Entries occur when
/// price moves outside the bands; exits occur on mean reversion or at the
/// opposite band.
pub struct MeanReversionStrategy {
    config: MeanReversionConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Sliding window of recent mid prices.
    price_history: VecDeque<Decimal>,
    /// Side of the current open position (None when flat).
    current_position_side: Option<Side>,
    /// Set of client order IDs currently in-flight.
    live_order_ids: HashSet<String>,
}

impl MeanReversionStrategy {
    /// Create a new Mean Reversion strategy from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `window_periods` is less than 2.
    pub fn new(config: MeanReversionConfig) -> Self {
        assert!(config.window_periods >= 2, "window_periods must be >= 2");

        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            window_periods = config.window_periods,
            num_std_dev = %config.num_std_dev,
            order_size = %config.order_size,
            max_position = %config.max_position,
            exit_at_mean = config.exit_at_mean,
            "initializing Mean Reversion trading strategy"
        );

        Self {
            config,
            active: true,
            price_history: VecDeque::new(),
            current_position_side: None,
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

    /// Build a new limit order action with the given side and price.
    fn build_order(&self, side: Side, price: Decimal, reduce_only: bool) -> OrderAction {
        let client_order_id = Uuid::new_v4().to_string();
        debug!(
            strategy_id = %self.config.strategy_id,
            side = %side,
            quantity = %self.config.order_size,
            price = %price,
            client_order_id = %client_order_id,
            reduce_only = reduce_only,
            "building mean reversion order"
        );

        OrderAction::Place(NewOrder {
            exchange: self.config.exchange,
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::IOC,
            price: Some(price),
            quantity: self.config.order_size,
            strategy_id: self.config.strategy_id.clone(),
            reduce_only,
        })
    }

    /// Return the side needed to close the current position.
    fn exit_side(&self) -> Option<Side> {
        match self.current_position_side {
            Some(Side::Buy) => Some(Side::Sell),
            Some(Side::Sell) => Some(Side::Buy),
            None => None,
        }
    }
}

impl Strategy for MeanReversionStrategy {
    fn name(&self) -> &str {
        "mean_reversion"
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
        while self.price_history.len() > self.config.window_periods {
            self.price_history.pop_front();
        }

        // Step 2: Need full window before acting
        if self.price_history.len() < self.config.window_periods {
            debug!(
                strategy_id = %self.config.strategy_id,
                history_len = self.price_history.len(),
                window = self.config.window_periods,
                "price history not full, waiting"
            );
            return Vec::new();
        }

        // Step 3-6: Compute Bollinger Bands
        let (mean, std_dev) = rolling_stats(&self.price_history);
        let lower_band = mean - self.config.num_std_dev * std_dev;
        let upper_band = mean + self.config.num_std_dev * std_dev;

        debug!(
            strategy_id = %self.config.strategy_id,
            mid_price = %mid,
            mean = %mean,
            std_dev = %std_dev,
            lower_band = %lower_band,
            upper_band = %upper_band,
            position_side = ?self.current_position_side,
            "mean reversion tick"
        );

        // Step 8: If in position, check exit conditions first
        if self.current_position_side.is_some() {
            let should_exit = match self.current_position_side {
                Some(Side::Buy) => {
                    if self.config.exit_at_mean {
                        mid >= mean
                    } else {
                        mid >= upper_band
                    }
                }
                Some(Side::Sell) => {
                    if self.config.exit_at_mean {
                        mid <= mean
                    } else {
                        mid <= lower_band
                    }
                }
                None => false,
            };

            if should_exit {
                let exit = match self.exit_side() {
                    Some(s) => s,
                    None => return Vec::new(),
                };
                info!(
                    strategy_id = %self.config.strategy_id,
                    exit_side = %exit,
                    mid_price = %mid,
                    mean = %mean,
                    exit_at_mean = self.config.exit_at_mean,
                    "mean reversion exit triggered"
                );
                return vec![self.build_order(exit, mid, true)];
            }

            // In position but no exit signal -- hold
            return Vec::new();
        }

        // Step 7: No position -- check entry conditions
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

        if mid < lower_band {
            info!(
                strategy_id = %self.config.strategy_id,
                mid_price = %mid,
                lower_band = %lower_band,
                "price below lower band, entering long"
            );
            return vec![self.build_order(Side::Buy, mid, false)];
        }

        if mid > upper_band {
            info!(
                strategy_id = %self.config.strategy_id,
                mid_price = %mid,
                upper_band = %upper_band,
                "price above upper band, entering short"
            );
            return vec![self.build_order(Side::Sell, mid, false)];
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
            "Mean Reversion strategy received fill"
        );

        // Remove the order from live tracking
        self.live_order_ids.remove(&fill.client_order_id);

        if self.current_position_side.is_some() {
            // Exit fill -- clear position state
            info!(
                strategy_id = %self.config.strategy_id,
                "exit fill received, clearing position state"
            );
            self.current_position_side = None;
        } else {
            // Entry fill -- set position state
            let side = fill.side;
            self.current_position_side = Some(side);
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
            "Mean Reversion strategy received order update"
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
            "stopping Mean Reversion trading strategy"
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

    /// Helper: build a default MeanReversionConfig for testing.
    fn test_config() -> MeanReversionConfig {
        MeanReversionConfig {
            strategy_id: "mean-rev-test-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            window_periods: 5,
            num_std_dev: dec!(2),
            order_size: dec!(1),
            max_position: dec!(5),
            exit_at_mean: true,
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
            order_book: std::sync::Arc::new(OrderBook {
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
            }),
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
    fn feed_prices(strategy: &mut MeanReversionStrategy, prices: &[Decimal]) -> Vec<OrderAction> {
        let mut actions = Vec::new();
        for &price in prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            actions = strategy.on_tick(&snap);
        }
        actions
    }

    /// Helper: feed prices with a custom position quantity for each tick.
    fn feed_prices_with_position(
        strategy: &mut MeanReversionStrategy,
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
        let strategy = MeanReversionStrategy::new(config.clone());

        assert!(strategy.active);
        assert!(strategy.price_history.is_empty());
        assert!(strategy.current_position_side.is_none());
        assert!(strategy.live_order_ids.is_empty());
        assert_eq!(strategy.name(), "mean_reversion");
        assert_eq!(strategy.strategy_id(), "mean-rev-test-1");
    }

    // -----------------------------------------------------------------------
    // Test 2: No action when history not full
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_history_not_full() {
        let config = test_config(); // window_periods = 5
        let mut strategy = MeanReversionStrategy::new(config);

        // Feed only 3 prices (need 5)
        let prices = [dec!(100), dec!(101), dec!(102)];
        let actions = feed_prices(&mut strategy, &prices);

        assert!(actions.is_empty());
        assert_eq!(strategy.price_history.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 3: Buy when price < lower band
    // -----------------------------------------------------------------------
    #[test]
    fn test_buy_when_price_below_lower_band() {
        // Use num_std_dev = 1 so the lower band is closer to the mean,
        // making it easier to engineer a price below it.
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        let mut strategy = MeanReversionStrategy::new(config);

        // Prices: [100, 100, 100, 100, 80]
        // Mean = 96, StdDev = 8, Lower = 96 - 8 = 88
        // mid = 80 < 88 -> Buy
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(80)];
        let actions = feed_prices(&mut strategy, &prices);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.quantity, dec!(1));
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.time_in_force, TimeInForce::IOC);
                assert!(order.price.is_some());
                assert!(!order.reduce_only);
                assert_eq!(order.strategy_id, "mean-rev-test-1");
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: Sell when price > upper band
    // -----------------------------------------------------------------------
    #[test]
    fn test_sell_when_price_above_upper_band() {
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        let mut strategy = MeanReversionStrategy::new(config);

        // Prices: [100, 100, 100, 100, 120]
        // Mean = 104, StdDev = 8, Upper = 104 + 8 = 112
        // mid = 120 > 112 -> Sell
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(120)];
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
    // Test 5: Exit at mean (exit_at_mean = true)
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_at_mean_long() {
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        config.exit_at_mean = true;
        let mut strategy = MeanReversionStrategy::new(config);

        // Enter long: prices [100, 100, 100, 100, 80]
        // Mean=96, StdDev=8, Lower=88, mid=80 < 88 -> Buy
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(80)];
        let actions = feed_prices(&mut strategy, &prices);
        assert_eq!(actions.len(), 1);

        // Simulate fill
        let fill = make_fill(Side::Buy, dec!(80), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Buy));

        // Now feed prices that bring price back to mean or above.
        // Feed prices that stabilize, then one at/above the new mean.
        // Window: [100, 100, 80, 100, 100]
        // Mean = (100+100+80+100+100)/5 = 96
        // mid = 100 >= 96 -> exit
        let recovery_prices = [dec!(100), dec!(100)];
        let mut actions = Vec::new();
        for &price in &recovery_prices {
            let snap = make_snapshot(price, Decimal::ZERO);
            actions = strategy.on_tick(&snap);
        }

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell); // Exit long
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for exit at mean"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: Exit at opposite band (exit_at_mean = false)
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_at_opposite_band_long() {
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        config.exit_at_mean = false;
        let mut strategy = MeanReversionStrategy::new(config);

        // Enter long: prices [100, 100, 100, 100, 80]
        // Mean=96, StdDev=8, Lower=88, mid=80 < 88 -> Buy
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(80)];
        let actions = feed_prices(&mut strategy, &prices);
        assert_eq!(actions.len(), 1);

        // Simulate fill
        let fill = make_fill(Side::Buy, dec!(80), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Buy));

        // exit_at_mean=false: long exits when price >= upper_band.
        // Feed prices that push the current mid to/above the upper band.
        // After entering, window is [100, 100, 100, 80, ?]
        // We need mid >= upper_band.
        // Feed 120: window = [100, 100, 80, 100, 120]
        //   Wait, after entry the history already has [100,100,100,100,80].
        //   Next tick with 100: window = [100,100,100,80,100]
        //     Mean=96, Var = (16+16+16+256+16)/5=320/5=64, Std=8, Upper=104
        //     mid=100 < 104 -> no exit
        //   Next tick with 100: window = [100,100,80,100,100]
        //     Mean=96, Var=(16+16+256+16+16)/5=64, Std=8, Upper=104
        //     mid=100 < 104 -> no exit
        //   Next tick with 120: window = [100,80,100,100,120]
        //     Mean=100, Var=(0+400+0+0+400)/5=160, Std~=12.6, Upper~=112.6
        //     mid=120 > 112.6 -> exit!
        let snap1 = make_snapshot(dec!(100), Decimal::ZERO);
        strategy.on_tick(&snap1);
        let snap2 = make_snapshot(dec!(100), Decimal::ZERO);
        strategy.on_tick(&snap2);
        let snap3 = make_snapshot(dec!(120), Decimal::ZERO);
        let actions = strategy.on_tick(&snap3);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell); // Exit long
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for exit at opposite band"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 7: No entry when max_position exceeded
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_entry_when_max_position_exceeded() {
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        let mut strategy = MeanReversionStrategy::new(config);

        // Prices that would trigger a Buy: [100, 100, 100, 100, 80]
        // But position is already at max_position = 5
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(80)];
        let actions = feed_prices_with_position(&mut strategy, &prices, dec!(5));

        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 8: stop() cancels all and deactivates
    // -----------------------------------------------------------------------
    #[test]
    fn test_stop_cancels_all_and_deactivates() {
        let config = test_config();
        let mut strategy = MeanReversionStrategy::new(config);
        assert!(strategy.is_active());

        let actions = strategy.stop();
        assert!(!strategy.is_active());
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("mean-rev-test-1".to_string()));
            }
            _ => panic!("expected CancelAll action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: rolling_stats computes correct mean and std_dev
    // -----------------------------------------------------------------------
    #[test]
    fn test_rolling_stats_computes_correctly() {
        // Prices: [10, 20, 30]
        // Mean = 20
        // Variance = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 3 = (100+0+100)/3 = 200/3 = 66.666...
        // StdDev = sqrt(66.666...) ~= 8.165
        let prices: VecDeque<Decimal> = vec![dec!(10), dec!(20), dec!(30)].into_iter().collect();
        let (mean, std_dev) = rolling_stats(&prices);

        assert_eq!(mean, dec!(20));
        // Check std_dev is approximately 8.165
        let expected_std_dev = dec!(8.165);
        let diff = (std_dev - expected_std_dev).abs();
        assert!(
            diff < dec!(0.01),
            "std_dev {std_dev} should be close to {expected_std_dev}, diff = {diff}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: on_fill entry sets position state, exit clears it
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_fill_entry_and_exit_lifecycle() {
        let config = test_config();
        let mut strategy = MeanReversionStrategy::new(config);

        // Entry fill
        let entry_fill = make_fill(Side::Buy, dec!(80), dec!(1));
        strategy.on_fill(&entry_fill);
        assert_eq!(strategy.current_position_side, Some(Side::Buy));

        // Exit fill
        let exit_fill = make_fill(Side::Sell, dec!(100), dec!(1));
        strategy.on_fill(&exit_fill);
        assert!(strategy.current_position_side.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 11: on_order_update tracks live order IDs
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_order_update_tracks_live_orders() {
        let config = test_config();
        let mut strategy = MeanReversionStrategy::new(config);

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

        // Filled -- should be removed
        let update_filled = OrderUpdate {
            client_order_id: "order-1".to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(80),
            last_fill_price: Some(dec!(80)),
            last_fill_quantity: Some(dec!(1)),
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };

        strategy.on_order_update(&update_filled);
        assert!(!strategy.live_order_ids.contains("order-1"));
    }

    // -----------------------------------------------------------------------
    // Test 12: on_tick returns empty when strategy is inactive
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_returns_empty_when_inactive() {
        let config = test_config();
        let mut strategy = MeanReversionStrategy::new(config);
        strategy.stop();

        let snap = make_snapshot(dec!(100), Decimal::ZERO);
        let actions = strategy.on_tick(&snap);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 13: on_tick returns empty when no mid price (empty order book)
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_no_mid_price() {
        let config = test_config();
        let mut strategy = MeanReversionStrategy::new(config);

        let mut snapshot = make_snapshot(dec!(100), Decimal::ZERO);
        // Arc::make_mut gives mutable access in tests
        std::sync::Arc::make_mut(&mut snapshot.order_book)
            .bids
            .clear();
        std::sync::Arc::make_mut(&mut snapshot.order_book)
            .asks
            .clear();

        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
        assert!(strategy.price_history.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 14: Price history caps at window_periods
    // -----------------------------------------------------------------------
    #[test]
    fn test_price_history_caps_at_window_periods() {
        let config = test_config(); // window = 5
        let mut strategy = MeanReversionStrategy::new(config);

        // Feed 10 prices -- history should be capped at 5
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
        assert_eq!(*strategy.price_history.front().unwrap(), dec!(105));
        assert_eq!(*strategy.price_history.back().unwrap(), dec!(109));
    }

    // -----------------------------------------------------------------------
    // Test 15: decimal_sqrt returns correct values
    // -----------------------------------------------------------------------
    #[test]
    fn test_decimal_sqrt() {
        // sqrt(0) = 0
        assert_eq!(decimal_sqrt(Decimal::ZERO), Decimal::ZERO);

        // sqrt(4) = 2
        let result = decimal_sqrt(dec!(4));
        assert!((result - dec!(2)).abs() < dec!(0.0001));

        // sqrt(9) = 3
        let result = decimal_sqrt(dec!(9));
        assert!((result - dec!(3)).abs() < dec!(0.0001));

        // sqrt(2) ~= 1.4142
        let result = decimal_sqrt(dec!(2));
        assert!((result - dec!(1.4142)).abs() < dec!(0.001));

        // sqrt of negative returns 0
        assert_eq!(decimal_sqrt(dec!(-5)), Decimal::ZERO);
    }

    // -----------------------------------------------------------------------
    // Test 16: No action when price is within bands (no signal)
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_action_when_price_within_bands() {
        let config = test_config(); // num_std_dev = 2, window = 5
        let mut strategy = MeanReversionStrategy::new(config);

        // All prices the same -> std_dev = 0, bands collapse to mean.
        // Actually with std_dev=0, upper=lower=mean and mid==mean, so no
        // signal (not strictly less or greater).
        // Use slightly varying prices that stay within bands.
        let prices = [dec!(100), dec!(101), dec!(100), dec!(101), dec!(100)];
        let actions = feed_prices(&mut strategy, &prices);

        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 17: Exit at mean for short position
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_at_mean_short() {
        let mut config = test_config();
        config.num_std_dev = dec!(1);
        config.exit_at_mean = true;
        let mut strategy = MeanReversionStrategy::new(config);

        // Enter short: prices [100, 100, 100, 100, 120]
        // Mean=104, StdDev=8, Upper=112, mid=120 > 112 -> Sell
        let prices = [dec!(100), dec!(100), dec!(100), dec!(100), dec!(120)];
        let actions = feed_prices(&mut strategy, &prices);
        assert_eq!(actions.len(), 1);

        // Simulate fill
        let fill = make_fill(Side::Sell, dec!(120), dec!(1));
        strategy.on_fill(&fill);
        assert_eq!(strategy.current_position_side, Some(Side::Sell));

        // Feed prices that bring mid back to or below mean.
        // Window: [100, 100, 100, 120, 100]
        // Mean = (100+100+100+120+100)/5 = 104
        // mid = 100 <= 104 -> exit
        let snap = make_snapshot(dec!(100), Decimal::ZERO);
        let actions = strategy.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy); // Exit short
                assert!(order.reduce_only);
            }
            _ => panic!("expected Place action for exit at mean (short)"),
        }
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
    fn test_validate_window_too_small() {
        let mut config = test_config();
        config.window_periods = 1;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("window_periods must be at least 2"));
    }

    #[test]
    fn test_validate_zero_num_std_dev() {
        let mut config = test_config();
        config.num_std_dev = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("num_std_dev must be positive"));
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
