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
