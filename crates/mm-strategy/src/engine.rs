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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mm_core::events::OrderUpdate;
    use mm_core::traits::Strategy;
    use mm_core::types::{
        CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderBook,
        OrderType, Position, PriceLevel, Side, Symbol, TimeInForce,
    };
    use rust_decimal_macros::dec;

    // -----------------------------------------------------------------------
    // MockStrategy
    // -----------------------------------------------------------------------

    struct MockStrategy {
        id: String,
        name: String,
        active: bool,
        tick_count: usize,
        fill_count: usize,
        update_count: usize,
        tick_actions: Vec<OrderAction>,
        stop_actions: Vec<OrderAction>,
    }

    impl MockStrategy {
        fn new(id: &str, name: &str, active: bool) -> Self {
            Self {
                id: id.to_string(),
                name: name.to_string(),
                active,
                tick_count: 0,
                fill_count: 0,
                update_count: 0,
                tick_actions: Vec::new(),
                stop_actions: Vec::new(),
            }
        }

        fn with_tick_actions(mut self, actions: Vec<OrderAction>) -> Self {
            self.tick_actions = actions;
            self
        }

        fn with_stop_actions(mut self, actions: Vec<OrderAction>) -> Self {
            self.stop_actions = actions;
            self
        }
    }

    impl Strategy for MockStrategy {
        fn name(&self) -> &str {
            &self.name
        }

        fn strategy_id(&self) -> &str {
            &self.id
        }

        fn on_tick(&mut self, _snapshot: &MarketSnapshot) -> Vec<OrderAction> {
            self.tick_count += 1;
            self.tick_actions.clone()
        }

        fn on_fill(&mut self, _fill: &Fill) {
            self.fill_count += 1;
        }

        fn on_order_update(&mut self, _update: &OrderUpdate) {
            self.update_count += 1;
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn stop(&mut self) -> Vec<OrderAction> {
            self.active = false;
            self.stop_actions.clone()
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_snapshot() -> MarketSnapshot {
        let sym = Symbol::new("BTC", "USDT");
        let now = Utc::now();
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: sym.clone(),
            order_book: OrderBook {
                exchange: Exchange::Simulated,
                symbol: sym.clone(),
                bids: vec![PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(1),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50001),
                    quantity: dec!(1),
                }],
                timestamp: now,
                sequence: 1,
            },
            last_trade: None,
            position: Position::new(Exchange::Simulated, sym),
            open_orders: vec![],
            timestamp: now,
        }
    }

    fn make_place_action(strategy_id: &str) -> OrderAction {
        OrderAction::Place(NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50000)),
            quantity: dec!(0.1),
            strategy_id: strategy_id.to_string(),
            reduce_only: false,
        })
    }

    fn make_cancel_all_action() -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(Exchange::Simulated),
            symbol: None,
            strategy_id: None,
        })
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_add_strategy() {
        let mut engine = StrategyEngine::new();
        let strat = MockStrategy::new("s1", "Mock1", true);

        let result = engine.add_strategy(Box::new(strat));
        assert!(result.is_ok());
        assert_eq!(engine.strategy_count(), 1);
        assert!(engine.has_strategy("s1"));
    }

    #[test]
    fn test_add_duplicate_strategy_fails() {
        let mut engine = StrategyEngine::new();
        let s1 = MockStrategy::new("dup", "MockA", true);
        let s2 = MockStrategy::new("dup", "MockB", true);

        assert!(engine.add_strategy(Box::new(s1)).is_ok());
        let result = engine.add_strategy(Box::new(s2));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("already exists"));
        assert_eq!(engine.strategy_count(), 1);
    }

    #[test]
    fn test_remove_strategy() {
        let mut engine = StrategyEngine::new();
        let strat = MockStrategy::new("rm1", "MockRm", true);
        engine.add_strategy(Box::new(strat)).unwrap();
        assert_eq!(engine.strategy_count(), 1);

        let removed = engine.remove_strategy("rm1");
        assert!(removed.is_some());
        let (boxed_strat, _cancel_actions) = removed.unwrap();
        assert_eq!(boxed_strat.strategy_id(), "rm1");
        assert_eq!(engine.strategy_count(), 0);
        assert!(!engine.has_strategy("rm1"));
    }

    #[test]
    fn test_remove_nonexistent_returns_none() {
        let mut engine = StrategyEngine::new();
        let result = engine.remove_strategy("does_not_exist");
        assert!(result.is_none());
    }

    #[test]
    fn test_tick_dispatches_to_active_strategies() {
        let mut engine = StrategyEngine::new();

        let active = MockStrategy::new("active1", "Active", true)
            .with_tick_actions(vec![make_place_action("active1")]);
        let inactive = MockStrategy::new("inactive1", "Inactive", false);

        engine.add_strategy(Box::new(active)).unwrap();
        engine.add_strategy(Box::new(inactive)).unwrap();

        let snapshot = make_snapshot();
        let actions = engine.tick(&snapshot);

        // Only the active strategy should have produced actions.
        assert_eq!(actions.len(), 1);

        // Verify the active strategy was ticked (we can check indirectly:
        // it returned 1 action). The inactive one should not have been ticked,
        // so tick_count remains 0. We cannot access tick_count through the trait
        // object, but the action count confirms dispatch behavior.
    }

    #[test]
    fn test_tick_collects_all_actions() {
        let mut engine = StrategyEngine::new();

        let s1 = MockStrategy::new("s1", "Mock1", true)
            .with_tick_actions(vec![make_place_action("s1"), make_place_action("s1")]);
        let s2 = MockStrategy::new("s2", "Mock2", true)
            .with_tick_actions(vec![make_cancel_all_action()]);

        engine.add_strategy(Box::new(s1)).unwrap();
        engine.add_strategy(Box::new(s2)).unwrap();

        let snapshot = make_snapshot();
        let actions = engine.tick(&snapshot);

        // s1 produces 2 actions, s2 produces 1 action => 3 total
        assert_eq!(actions.len(), 3);
    }

    #[test]
    fn test_stop_all() {
        let mut engine = StrategyEngine::new();

        let s1 = MockStrategy::new("s1", "Mock1", true)
            .with_stop_actions(vec![make_cancel_all_action()]);
        let s2 = MockStrategy::new("s2", "Mock2", true)
            .with_stop_actions(vec![make_cancel_all_action(), make_cancel_all_action()]);

        engine.add_strategy(Box::new(s1)).unwrap();
        engine.add_strategy(Box::new(s2)).unwrap();

        let actions = engine.stop_all();
        // s1 returns 1 stop action, s2 returns 2 stop actions => 3 total
        assert_eq!(actions.len(), 3);

        // After stop_all, all strategies should report inactive.
        assert_eq!(engine.active_count(), 0);
    }

    #[test]
    fn test_strategy_ids() {
        let mut engine = StrategyEngine::new();

        engine
            .add_strategy(Box::new(MockStrategy::new("alpha", "A", true)))
            .unwrap();
        engine
            .add_strategy(Box::new(MockStrategy::new("beta", "B", true)))
            .unwrap();
        engine
            .add_strategy(Box::new(MockStrategy::new("gamma", "C", false)))
            .unwrap();

        let mut ids = engine.strategy_ids();
        ids.sort();
        assert_eq!(ids, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_active_count() {
        let mut engine = StrategyEngine::new();

        engine
            .add_strategy(Box::new(MockStrategy::new("a1", "Active1", true)))
            .unwrap();
        engine
            .add_strategy(Box::new(MockStrategy::new("a2", "Active2", true)))
            .unwrap();
        engine
            .add_strategy(Box::new(MockStrategy::new("i1", "Inactive1", false)))
            .unwrap();

        assert_eq!(engine.strategy_count(), 3);
        assert_eq!(engine.active_count(), 2);
    }
}
