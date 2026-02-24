use std::collections::HashMap;

use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{Fill, MarketSnapshot, OrderAction};
use tracing::{debug, info, warn};

/// The strategy engine manages a collection of strategies and routes market data,
/// fills, and order updates to the appropriate strategy instances.
pub struct StrategyEngine {
    /// Active strategies indexed by their strategy_id for O(1) lookup.
    strategies: Vec<Box<dyn Strategy>>,
    /// Maps strategy_id -> index into the strategies vec for routing.
    id_index: HashMap<String, usize>,
}

impl StrategyEngine {
    /// Create a new empty strategy engine.
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            id_index: HashMap::new(),
        }
    }

    /// Add a strategy to the engine. Returns an error if a strategy with the
    /// same strategy_id already exists.
    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) -> Result<(), String> {
        let id = strategy.strategy_id().to_string();
        if self.id_index.contains_key(&id) {
            return Err(format!(
                "strategy with id '{}' already exists in the engine",
                id
            ));
        }

        let index = self.strategies.len();
        info!(
            strategy_id = %id,
            strategy_name = %strategy.name(),
            "adding strategy to engine"
        );
        self.strategies.push(strategy);
        self.id_index.insert(id, index);
        Ok(())
    }

    /// Remove a strategy by its strategy_id. Returns the removed strategy, or None
    /// if no strategy with that id was found. Before removal the strategy is stopped,
    /// and its final cancel actions are returned alongside the boxed strategy.
    pub fn remove_strategy(
        &mut self,
        strategy_id: &str,
    ) -> Option<(Box<dyn Strategy>, Vec<OrderAction>)> {
        let index = self.id_index.remove(strategy_id)?;

        info!(strategy_id = %strategy_id, "removing strategy from engine");

        // Swap-remove from the vec. If the removed element wasn't the last one,
        // we need to update the index of the element that was swapped in.
        let mut strategy = self.strategies.swap_remove(index);
        let cancel_actions = strategy.stop();

        // If swap_remove moved an element, update its index mapping.
        if index < self.strategies.len() {
            let swapped_id = self.strategies[index].strategy_id().to_string();
            self.id_index.insert(swapped_id, index);
        }

        Some((strategy, cancel_actions))
    }

    /// Run a single tick across all active strategies. Each strategy receives the
    /// market snapshot and returns order actions. All actions are collected and
    /// returned in a flat list.
    pub fn tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        let mut all_actions = Vec::new();

        for strategy in self.strategies.iter_mut() {
            if !strategy.is_active() {
                debug!(
                    strategy_id = %strategy.strategy_id(),
                    "skipping inactive strategy"
                );
                continue;
            }

            let actions = strategy.on_tick(snapshot);
            if !actions.is_empty() {
                debug!(
                    strategy_id = %strategy.strategy_id(),
                    action_count = actions.len(),
                    "strategy produced order actions"
                );
            }
            all_actions.extend(actions);
        }

        all_actions
    }

    /// Route a fill to the strategy that owns the order (identified by
    /// matching the strategy_id on open orders). The caller is responsible for
    /// determining which strategy_id the fill belongs to and passing it here.
    pub fn route_fill(&mut self, strategy_id: &str, fill: &Fill) {
        if let Some(&index) = self.id_index.get(strategy_id) {
            debug!(
                strategy_id = %strategy_id,
                fill_id = %fill.fill_id,
                side = %fill.side,
                price = %fill.price,
                quantity = %fill.quantity,
                "routing fill to strategy"
            );
            self.strategies[index].on_fill(fill);
        } else {
            warn!(
                strategy_id = %strategy_id,
                fill_id = %fill.fill_id,
                "no strategy found for fill, ignoring"
            );
        }
    }

    /// Route an order update to the appropriate strategy.
    pub fn route_order_update(&mut self, strategy_id: &str, update: &OrderUpdate) {
        if let Some(&index) = self.id_index.get(strategy_id) {
            debug!(
                strategy_id = %strategy_id,
                client_order_id = %update.client_order_id,
                status = ?update.status,
                "routing order update to strategy"
            );
            self.strategies[index].on_order_update(update);
        } else {
            warn!(
                strategy_id = %strategy_id,
                client_order_id = %update.client_order_id,
                "no strategy found for order update, ignoring"
            );
        }
    }

    /// Stop all strategies gracefully. Returns all cancel actions from every strategy.
    pub fn stop_all(&mut self) -> Vec<OrderAction> {
        let mut all_actions = Vec::new();
        for strategy in self.strategies.iter_mut() {
            info!(
                strategy_id = %strategy.strategy_id(),
                "stopping strategy"
            );
            let actions = strategy.stop();
            all_actions.extend(actions);
        }
        all_actions
    }

    /// Return the number of registered strategies.
    pub fn strategy_count(&self) -> usize {
        self.strategies.len()
    }

    /// Return the number of active (non-stopped) strategies.
    pub fn active_count(&self) -> usize {
        self.strategies.iter().filter(|s| s.is_active()).count()
    }

    /// Check whether a strategy with the given id exists in the engine.
    pub fn has_strategy(&self, strategy_id: &str) -> bool {
        self.id_index.contains_key(strategy_id)
    }

    /// Get a list of all strategy ids currently registered.
    pub fn strategy_ids(&self) -> Vec<String> {
        self.id_index.keys().cloned().collect()
    }
}

impl Default for StrategyEngine {
    fn default() -> Self {
        Self::new()
    }
}
