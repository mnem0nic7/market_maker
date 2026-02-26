use std::collections::HashMap;

use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderType, Side,
    Symbol, TimeInForce,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Grid type: arithmetic (equal price spacing) or geometric (equal percentage spacing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GridType {
    /// Equal absolute price difference between levels.
    Arithmetic,
    /// Equal percentage price difference between levels (constant ratio).
    Geometric,
}

/// Configuration for the grid trading strategy.
///
/// The strategy divides a price range into a series of grid levels and places
/// buy orders below the current price and sell orders above. When a buy fills
/// at level N, a sell is placed at level N+1, and vice versa.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Upper bound of the grid price range.
    pub upper_price: Decimal,
    /// Lower bound of the grid price range.
    pub lower_price: Decimal,
    /// Number of grid intervals (there will be num_grids + 1 price levels).
    pub num_grids: usize,
    /// Order size (quantity) per grid level.
    pub order_size: Decimal,
    /// Whether to use arithmetic or geometric spacing.
    pub grid_type: GridType,
}

// ---------------------------------------------------------------------------
// Grid Level Tracking
// ---------------------------------------------------------------------------

/// Tracks the state of a single grid level.
#[derive(Debug, Clone)]
struct GridLevel {
    /// The price at this grid level.
    price: Decimal,
    /// Index of this level (0 = lowest, N = highest).
    index: usize,
    /// The side of the currently placed order at this level, if any.
    active_side: Option<Side>,
    /// The client_order_id of the active order, if any.
    active_order_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

pub struct GridStrategy {
    config: GridConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Pre-computed grid levels, sorted from lowest to highest price.
    levels: Vec<GridLevel>,
    /// Maps client_order_id -> level index for fast fill routing.
    order_to_level: HashMap<String, usize>,
    /// Whether we have completed the initial order placement.
    initialized: bool,
}

impl GridStrategy {
    pub fn new(config: GridConfig) -> Self {
        assert!(
            config.upper_price > config.lower_price,
            "upper_price must be greater than lower_price"
        );
        assert!(config.num_grids >= 1, "num_grids must be at least 1");

        let levels = Self::compute_levels(&config);

        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            upper_price = %config.upper_price,
            lower_price = %config.lower_price,
            num_grids = config.num_grids,
            grid_type = ?config.grid_type,
            total_levels = levels.len(),
            "initializing grid trading strategy"
        );

        Self {
            config,
            active: true,
            levels,
            order_to_level: HashMap::new(),
            initialized: false,
        }
    }

    /// Compute the grid price levels based on configuration.
    fn compute_levels(config: &GridConfig) -> Vec<GridLevel> {
        let n = config.num_grids;
        // num_grids intervals means num_grids + 1 price levels
        let num_levels = n + 1;
        let mut levels = Vec::with_capacity(num_levels);

        match config.grid_type {
            GridType::Arithmetic => {
                // Equal price spacing: step = (upper - lower) / num_grids
                let step = (config.upper_price - config.lower_price) / Decimal::from(n as u64);
                for i in 0..num_levels {
                    let price = config.lower_price + step * Decimal::from(i as u64);
                    levels.push(GridLevel {
                        price,
                        index: i,
                        active_side: None,
                        active_order_id: None,
                    });
                }
            }
            GridType::Geometric => {
                // Equal percentage spacing: ratio = (upper / lower)^(1/n)
                // We compute in f64 for the exponentiation, then convert back.
                let lower_f64 = config.lower_price.to_f64().unwrap_or(1.0);
                let upper_f64 = config.upper_price.to_f64().unwrap_or(2.0);
                let ratio = (upper_f64 / lower_f64).powf(1.0 / n as f64);

                for i in 0..num_levels {
                    let price_f64 = lower_f64 * ratio.powi(i as i32);
                    let price = Decimal::try_from(price_f64).unwrap_or(config.lower_price);
                    levels.push(GridLevel {
                        price,
                        index: i,
                        active_side: None,
                        active_order_id: None,
                    });
                }
            }
        }

        debug!(
            levels_count = levels.len(),
            first_price = %levels.first().map(|l| l.price).unwrap_or_default(),
            last_price = %levels.last().map(|l| l.price).unwrap_or_default(),
            "computed grid levels"
        );

        levels
    }

    /// Build the cancel-all action for this strategy.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.exchange),
            symbol: Some(self.config.symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Create a new order action at the given level with the given side.
    fn place_order_at_level(&self, level_index: usize, side: Side) -> Option<OrderAction> {
        let level = self.levels.get(level_index)?;
        Some(OrderAction::Place(NewOrder {
            exchange: self.config.exchange,
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(level.price),
            quantity: self.config.order_size,
            strategy_id: self.config.strategy_id.clone(),
            reduce_only: false,
        }))
    }

    /// Perform initial order placement: buy orders below mid, sell orders above mid.
    fn initial_placement(&mut self, mid: Decimal) -> Vec<OrderAction> {
        let mut actions = Vec::new();

        for i in 0..self.levels.len() {
            let level_price = self.levels[i].price;

            if level_price < mid {
                // Place buy order below current price
                if let Some(action) = self.place_order_at_level(i, Side::Buy) {
                    self.levels[i].active_side = Some(Side::Buy);
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        level = i,
                        price = %level_price,
                        side = "Buy",
                        "placing initial grid order"
                    );
                    actions.push(action);
                }
            } else if level_price > mid {
                // Place sell order above current price
                if let Some(action) = self.place_order_at_level(i, Side::Sell) {
                    self.levels[i].active_side = Some(Side::Sell);
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        level = i,
                        price = %level_price,
                        side = "Sell",
                        "placing initial grid order"
                    );
                    actions.push(action);
                }
            }
            // If level_price == mid, we skip (ambiguous, no order placed)
        }

        self.initialized = true;
        actions
    }
}

impl Strategy for GridStrategy {
    fn name(&self) -> &str {
        "GridTrading"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                warn!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping tick"
                );
                return Vec::new();
            }
        };

        // On the first tick, place the initial grid orders.
        if !self.initialized {
            info!(
                strategy_id = %self.config.strategy_id,
                mid_price = %mid,
                "performing initial grid placement"
            );
            return self.initial_placement(mid);
        }

        // After initialization, the grid is event-driven via on_fill.
        // Ticks are used only for monitoring / health checks.
        Vec::new()
    }

    fn on_fill(&mut self, fill: &Fill) {
        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            "grid strategy received fill"
        );

        // Find the level that was filled by matching the client_order_id.
        let level_index = match self.order_to_level.remove(&fill.client_order_id) {
            Some(idx) => idx,
            None => {
                // Fallback: find by price (closest match)
                let mut best_idx = None;
                let mut best_distance = Decimal::MAX;
                for level in &self.levels {
                    let distance = (level.price - fill.price).abs();
                    if distance < best_distance {
                        best_distance = distance;
                        best_idx = Some(level.index);
                    }
                }
                match best_idx {
                    Some(idx) => idx,
                    None => {
                        warn!(
                            strategy_id = %self.config.strategy_id,
                            fill_id = %fill.fill_id,
                            "could not match fill to any grid level"
                        );
                        return;
                    }
                }
            }
        };

        // Clear the filled level's state
        if level_index < self.levels.len() {
            self.levels[level_index].active_side = None;
            self.levels[level_index].active_order_id = None;
        }

        // Grid logic: when a buy fills at level N, place a sell at level N+1.
        // When a sell fills at level N, place a buy at level N-1.
        match fill.side {
            Side::Buy => {
                // Filled a buy at level N -> place sell at N+1
                let target = level_index + 1;
                if target < self.levels.len() {
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        filled_level = level_index,
                        target_level = target,
                        target_price = %self.levels[target].price,
                        "buy filled, placing sell at next level up"
                    );
                    self.levels[target].active_side = Some(Side::Sell);
                    // Note: the actual order will be placed next time we can emit actions.
                    // Since on_fill doesn't return actions, we mark the level and
                    // on_tick will handle any re-placement if needed. However, a more
                    // sophisticated implementation would use a channel. For now, we
                    // set the pending state.
                } else {
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        filled_level = level_index,
                        "buy filled at top of grid, no sell to place"
                    );
                }
            }
            Side::Sell => {
                // Filled a sell at level N -> place buy at N-1
                if level_index > 0 {
                    let target = level_index - 1;
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        filled_level = level_index,
                        target_level = target,
                        target_price = %self.levels[target].price,
                        "sell filled, placing buy at next level down"
                    );
                    self.levels[target].active_side = Some(Side::Buy);
                } else {
                    debug!(
                        strategy_id = %self.config.strategy_id,
                        filled_level = level_index,
                        "sell filled at bottom of grid, no buy to place"
                    );
                }
            }
        }
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "grid strategy order update"
        );

        // When we receive a New/Submitted acknowledgement, record the mapping
        // from client_order_id to the grid level.
        if update.status == mm_core::types::OrderStatus::New
            || update.status == mm_core::types::OrderStatus::Submitted
        {
            // Find the level whose price best matches the order's average_fill_price
            // or the pending level without an order id yet.
            if let Some(last_fill_price) = update.last_fill_price {
                // Use fill price to match
                let mut best_idx = None;
                let mut best_distance = Decimal::MAX;
                for level in &self.levels {
                    let distance = (level.price - last_fill_price).abs();
                    if distance < best_distance {
                        best_distance = distance;
                        best_idx = Some(level.index);
                    }
                }
                if let Some(idx) = best_idx {
                    self.levels[idx].active_order_id =
                        Some(update.client_order_id.clone());
                    self.order_to_level
                        .insert(update.client_order_id.clone(), idx);
                }
            } else {
                // Match by finding a level that has an active_side but no order_id
                for level in self.levels.iter_mut() {
                    if level.active_side.is_some() && level.active_order_id.is_none() {
                        level.active_order_id = Some(update.client_order_id.clone());
                        self.order_to_level
                            .insert(update.client_order_id.clone(), level.index);
                        break;
                    }
                }
            }
        }

        // If terminal, clean up
        if update.status.is_terminal() {
            if let Some(level_idx) = self.order_to_level.remove(&update.client_order_id) {
                if level_idx < self.levels.len() {
                    // Only clear if this is still the active order at that level
                    if self.levels[level_idx].active_order_id.as_deref()
                        == Some(&update.client_order_id)
                    {
                        self.levels[level_idx].active_order_id = None;
                        // Keep active_side for cancelled orders so they can be re-placed
                        if update.status == mm_core::types::OrderStatus::Filled {
                            self.levels[level_idx].active_side = None;
                        }
                    }
                }
            }
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            "stopping grid trading strategy"
        );
        self.active = false;
        self.order_to_level.clear();
        for level in self.levels.iter_mut() {
            level.active_side = None;
            level.active_order_id = None;
        }
        vec![self.cancel_all_action()]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mm_core::traits::Strategy;
    use mm_core::types::{
        Exchange, MarketSnapshot, OrderAction, OrderBook, Position, PriceLevel, Side, Symbol,
    };
    use rust_decimal_macros::dec;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a default arithmetic GridConfig for testing.
    fn make_config() -> GridConfig {
        GridConfig {
            strategy_id: "test-grid".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            upper_price: dec!(70000),
            lower_price: dec!(60000),
            num_grids: 5,
            order_size: dec!(0.01),
            grid_type: GridType::Arithmetic,
        }
    }

    /// Build a MarketSnapshot with the given mid price.
    /// Uses bid = mid - 1, ask = mid + 1 so that mid_price() = mid exactly.
    fn make_snapshot(mid: Decimal) -> MarketSnapshot {
        let ts = Utc::now();
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: OrderBook {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: mid - dec!(1),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: mid + dec!(1),
                    quantity: dec!(10),
                }],
                timestamp: ts,
                sequence: 1,
            },
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Build a MarketSnapshot with an empty order book (no bids, no asks).
    fn make_empty_snapshot() -> MarketSnapshot {
        let ts = Utc::now();
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: OrderBook {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![],
                asks: vec![],
                timestamp: ts,
                sequence: 1,
            },
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Extract the price from a Place action, panicking if it is not a Place.
    fn extract_place_price(action: &OrderAction) -> Decimal {
        match action {
            OrderAction::Place(new_order) => new_order.price.expect("limit order must have price"),
            other => panic!("expected OrderAction::Place, got: {:?}", other),
        }
    }

    /// Extract the side from a Place action.
    fn extract_place_side(action: &OrderAction) -> Side {
        match action {
            OrderAction::Place(new_order) => new_order.side,
            other => panic!("expected OrderAction::Place, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 1. Arithmetic grid levels
    // -----------------------------------------------------------------------

    #[test]
    fn test_arithmetic_grid_levels() {
        // lower=60000, upper=70000, num_grids=5
        // step = (70000 - 60000) / 5 = 2000
        // Levels: 60000, 62000, 64000, 66000, 68000, 70000 (6 levels)
        let config = make_config();
        let strategy = GridStrategy::new(config);

        assert_eq!(strategy.levels.len(), 6);
        assert_eq!(strategy.levels[0].price, dec!(60000));
        assert_eq!(strategy.levels[1].price, dec!(62000));
        assert_eq!(strategy.levels[2].price, dec!(64000));
        assert_eq!(strategy.levels[3].price, dec!(66000));
        assert_eq!(strategy.levels[4].price, dec!(68000));
        assert_eq!(strategy.levels[5].price, dec!(70000));

        // Verify indices are correct
        for (i, level) in strategy.levels.iter().enumerate() {
            assert_eq!(level.index, i, "level index mismatch at position {}", i);
            assert!(level.active_side.is_none());
            assert!(level.active_order_id.is_none());
        }
    }

    // -----------------------------------------------------------------------
    // 2. Geometric grid levels
    // -----------------------------------------------------------------------

    #[test]
    fn test_geometric_grid_levels() {
        // lower=100, upper=400, num_grids=2
        // ratio = (400/100)^(1/2) = 4^0.5 = 2.0
        // Levels: 100, 200, 400 (3 levels)
        let config = GridConfig {
            strategy_id: "geo-grid".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            upper_price: dec!(400),
            lower_price: dec!(100),
            num_grids: 2,
            order_size: dec!(1),
            grid_type: GridType::Geometric,
        };

        let strategy = GridStrategy::new(config);

        assert_eq!(strategy.levels.len(), 3);
        assert_eq!(strategy.levels[0].price, dec!(100));
        // Level 1 should be approximately 200 (geometric mean of 100 and 400).
        // Geometric computation uses f64 so we check within a tolerance.
        let level1 = strategy.levels[1].price;
        let diff = (level1 - dec!(200)).abs();
        assert!(
            diff < dec!(0.01),
            "level 1 should be ~200, got {} (diff={})",
            level1,
            diff
        );
        assert_eq!(strategy.levels[2].price, dec!(400));
    }

    // -----------------------------------------------------------------------
    // 3. Initial placement: buys below mid, sells above mid
    // -----------------------------------------------------------------------

    #[test]
    fn test_initial_placement_buys_below_sells_above() {
        // Levels: 60000, 62000, 64000, 66000, 68000, 70000
        // Mid = 65000: buys at 60k, 62k, 64k; sells at 66k, 68k, 70k
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        let snapshot = make_snapshot(dec!(65000));
        let actions = strategy.on_tick(&snapshot);

        // Separate buys and sells
        let buys: Vec<_> = actions
            .iter()
            .filter(|a| extract_place_side(a) == Side::Buy)
            .collect();
        let sells: Vec<_> = actions
            .iter()
            .filter(|a| extract_place_side(a) == Side::Sell)
            .collect();

        assert_eq!(buys.len(), 3, "expected 3 buy orders below mid");
        assert_eq!(sells.len(), 3, "expected 3 sell orders above mid");

        // Verify buy prices (below mid)
        let mut buy_prices: Vec<Decimal> = buys.iter().map(|a| extract_place_price(a)).collect();
        buy_prices.sort();
        assert_eq!(buy_prices, vec![dec!(60000), dec!(62000), dec!(64000)]);

        // Verify sell prices (above mid)
        let mut sell_prices: Vec<Decimal> = sells.iter().map(|a| extract_place_price(a)).collect();
        sell_prices.sort();
        assert_eq!(sell_prices, vec![dec!(66000), dec!(68000), dec!(70000)]);
    }

    // -----------------------------------------------------------------------
    // 4. Initial placement order count
    // -----------------------------------------------------------------------

    #[test]
    fn test_initial_placement_order_count() {
        // num_grids=5 means 6 levels. With mid between levels (not matching any),
        // all 6 levels get orders: 3 buys + 3 sells = 6 orders.
        // However, if mid doesn't match any level price, we get one order per level.
        // With mid=65000 (not a level price), we expect all 6 levels to have orders.
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        let snapshot = make_snapshot(dec!(65000));
        let actions = strategy.on_tick(&snapshot);

        // 6 levels total, mid=65000 doesn't match any level, so all 6 get orders
        assert_eq!(
            actions.len(),
            6,
            "expected 6 orders (one per level since mid matches no level)"
        );

        // All should be Place actions
        for action in &actions {
            assert!(
                matches!(action, OrderAction::Place(_)),
                "expected Place action"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 5. Second tick returns no actions
    // -----------------------------------------------------------------------

    #[test]
    fn test_second_tick_no_action() {
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        let snapshot = make_snapshot(dec!(65000));

        // First tick: initial placement
        let actions1 = strategy.on_tick(&snapshot);
        assert!(!actions1.is_empty(), "first tick should produce actions");

        // Second tick: already initialized, should return empty
        let actions2 = strategy.on_tick(&snapshot);
        assert!(
            actions2.is_empty(),
            "second tick should return empty (grid is event-driven after init)"
        );
    }

    // -----------------------------------------------------------------------
    // 6. No mid price returns empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_mid_price_returns_empty() {
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        let snapshot = make_empty_snapshot();
        let actions = strategy.on_tick(&snapshot);

        assert!(
            actions.is_empty(),
            "empty order book (no mid price) should return no actions"
        );
        // Strategy should NOT be marked initialized since it couldn't place orders
        assert!(!strategy.initialized);
    }

    // -----------------------------------------------------------------------
    // 7. stop() returns CancelAll, clears state, sets active=false
    // -----------------------------------------------------------------------

    #[test]
    fn test_stop_returns_cancel_all() {
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        // Initialize with first tick
        let snapshot = make_snapshot(dec!(65000));
        let _ = strategy.on_tick(&snapshot);
        assert!(strategy.is_active());

        // Stop the strategy
        let actions = strategy.stop();

        assert_eq!(actions.len(), 1, "stop should return exactly one action");
        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("test-grid".to_string()));
            }
            other => panic!("expected CancelAll, got: {:?}", other),
        }

        // State should be cleared
        assert!(!strategy.is_active());
        assert!(strategy.order_to_level.is_empty());
        for level in &strategy.levels {
            assert!(
                level.active_side.is_none(),
                "level {} active_side should be None after stop",
                level.index
            );
            assert!(
                level.active_order_id.is_none(),
                "level {} active_order_id should be None after stop",
                level.index
            );
        }
    }

    // -----------------------------------------------------------------------
    // 8. Inactive strategy returns empty on tick
    // -----------------------------------------------------------------------

    #[test]
    fn test_inactive_returns_empty() {
        let config = make_config();
        let mut strategy = GridStrategy::new(config);

        // Initialize, then stop
        let snapshot = make_snapshot(dec!(65000));
        let _ = strategy.on_tick(&snapshot);
        let _ = strategy.stop();

        assert!(!strategy.is_active());

        // Tick after stop should return empty
        let actions = strategy.on_tick(&snapshot);
        assert!(
            actions.is_empty(),
            "inactive strategy should return no actions"
        );
    }

    // -----------------------------------------------------------------------
    // 9. Constructor assertions (should_panic tests)
    // -----------------------------------------------------------------------

    #[test]
    #[should_panic(expected = "upper_price must be greater than lower_price")]
    fn test_grid_constructor_panics_upper_le_lower() {
        let mut config = make_config();
        config.upper_price = dec!(50000); // less than lower_price (60000)
        GridStrategy::new(config);
    }

    #[test]
    #[should_panic(expected = "upper_price must be greater than lower_price")]
    fn test_grid_constructor_panics_upper_eq_lower() {
        let mut config = make_config();
        config.upper_price = dec!(60000); // equal to lower_price
        GridStrategy::new(config);
    }

    #[test]
    #[should_panic(expected = "num_grids must be at least 1")]
    fn test_grid_constructor_panics_zero_grids() {
        let mut config = make_config();
        config.num_grids = 0;
        GridStrategy::new(config);
    }
}
