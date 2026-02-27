use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};

use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{Fill, MarketSnapshot, OrderAction};
use mm_core::StrategyConfig;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

use crate::build_strategy_from_config;

/// A request to hot-update a running strategy's parameters.
#[derive(Debug, Clone)]
pub struct StrategyParamUpdate {
    /// The strategy_id of the strategy to update.
    pub strategy_id: String,
    /// The full StrategyConfig with the new parameters applied.
    pub config: StrategyConfig,
}

/// The strategy engine manages a collection of strategies and routes market data,
/// fills, and order updates to the appropriate strategy instances.
pub struct StrategyEngine {
    /// Active strategies indexed by their strategy_id for O(1) lookup.
    strategies: Vec<Box<dyn Strategy>>,
    /// Maps strategy_id -> index into the strategies vec for routing.
    id_index: HashMap<String, usize>,
    /// Receiver for hot parameter updates from the API layer.
    param_update_rx: Option<mpsc::Receiver<StrategyParamUpdate>>,
    /// Strategy IDs that have panicked during `on_tick`. The caller can inspect
    /// this set to decide whether to deactivate or remove the affected strategies.
    panicked_strategies: HashSet<String>,
}

impl StrategyEngine {
    /// Create a new empty strategy engine.
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            id_index: HashMap::new(),
            param_update_rx: None,
            panicked_strategies: HashSet::new(),
        }
    }

    /// Attach a parameter update receiver to enable hot updates.
    pub fn set_param_update_receiver(&mut self, rx: mpsc::Receiver<StrategyParamUpdate>) {
        self.param_update_rx = Some(rx);
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
    #[instrument(skip(self, snapshot), fields(strategy_count = self.strategies.len()))]
    pub fn tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        let mut all_actions = Vec::new();

        for strategy in self.strategies.iter_mut() {
            // Skip strategies that don't match this snapshot's exchange+symbol
            if strategy.exchange() != snapshot.exchange || strategy.symbol() != snapshot.symbol {
                continue;
            }

            if !strategy.is_active() {
                debug!(
                    strategy_id = %strategy.strategy_id(),
                    "skipping inactive strategy"
                );
                continue;
            }

            let strategy_id = strategy.strategy_id().to_string();

            let result = catch_unwind(AssertUnwindSafe(|| strategy.on_tick(snapshot)));

            match result {
                Ok(actions) => {
                    if !actions.is_empty() {
                        debug!(
                            strategy_id = %strategy_id,
                            action_count = actions.len(),
                            "strategy produced order actions"
                        );
                    }
                    all_actions.extend(actions);
                }
                Err(panic_info) => {
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };

                    error!(
                        strategy_id = %strategy_id,
                        panic_message = %panic_msg,
                        "strategy panicked during on_tick, skipping"
                    );

                    self.panicked_strategies.insert(strategy_id);
                }
            }
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

    /// Check the parameter update channel (non-blocking) and apply any pending
    /// updates. For each update, builds a new strategy instance from the config.
    /// If the build succeeds, the old strategy is replaced. If it fails, the old
    /// strategy is kept and a warning is logged.
    #[instrument(skip(self))]
    pub fn try_apply_updates(&mut self) {
        let rx = match self.param_update_rx.as_mut() {
            Some(rx) => rx,
            None => return,
        };

        // Drain all pending updates in a non-blocking fashion.
        loop {
            match rx.try_recv() {
                Ok(update) => {
                    let strategy_id = &update.strategy_id;

                    let index = match self.id_index.get(strategy_id) {
                        Some(&idx) => idx,
                        None => {
                            warn!(
                                strategy_id = %strategy_id,
                                "param update for unknown strategy, ignoring"
                            );
                            continue;
                        }
                    };

                    match build_strategy_from_config(&update.config) {
                        Ok(new_strategy) => {
                            info!(
                                strategy_id = %strategy_id,
                                strategy_type = %update.config.strategy_type,
                                "hot-updating strategy parameters"
                            );
                            self.strategies[index] = new_strategy;
                        }
                        Err(e) => {
                            warn!(
                                strategy_id = %strategy_id,
                                error = %e,
                                "failed to build strategy with new params, keeping old strategy"
                            );
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    debug!("param update channel disconnected");
                    break;
                }
            }
        }
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

    /// Return the set of strategy IDs that have panicked during `on_tick`.
    /// The caller can use this to decide whether to deactivate or remove
    /// the affected strategies.
    pub fn panicked_strategies(&self) -> &HashSet<String> {
        &self.panicked_strategies
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
        exchange: Exchange,
        symbol: Symbol,
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
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
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

        fn with_exchange_symbol(mut self, exchange: Exchange, symbol: Symbol) -> Self {
            self.exchange = exchange;
            self.symbol = symbol;
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

        fn exchange(&self) -> Exchange {
            self.exchange
        }

        fn symbol(&self) -> Symbol {
            self.symbol.clone()
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
            order_book: std::sync::Arc::new(OrderBook {
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
            }),
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
        assert!(result.unwrap_err().contains("already exists"));
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

    // -----------------------------------------------------------------------
    // Hot parameter update tests
    // -----------------------------------------------------------------------

    /// Helper to create a valid StrategyConfig with spread params.
    fn make_spread_config(strategy_id: &str, spread_bps: u32) -> mm_core::StrategyConfig {
        mm_core::StrategyConfig {
            strategy_id: strategy_id.to_string(),
            strategy_type: "spread".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            enabled: true,
            params: serde_json::json!({
                "spread_bps": spread_bps,
                "order_size": "0.1",
                "num_levels": 1,
                "refresh_interval_ms": 1000,
                "min_spread_bps": "1",
                "reprice_threshold_bps": "100"
            }),
        }
    }

    #[tokio::test]
    async fn test_try_apply_updates_replaces_strategy() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let mut engine = StrategyEngine::new();
        engine.set_param_update_receiver(rx);

        // Build and add a strategy from config.
        let config = make_spread_config("s1", 10);
        let strategy = crate::build_strategy_from_config(&config).unwrap();
        engine.add_strategy(strategy).unwrap();

        assert_eq!(engine.strategy_count(), 1);
        assert!(engine.has_strategy("s1"));

        // Send an update with new params.
        let new_config = make_spread_config("s1", 20);
        tx.send(super::StrategyParamUpdate {
            strategy_id: "s1".to_string(),
            config: new_config,
        })
        .await
        .unwrap();

        // Apply updates — should replace the strategy.
        engine.try_apply_updates();

        // Strategy should still exist with the same id.
        assert_eq!(engine.strategy_count(), 1);
        assert!(engine.has_strategy("s1"));
    }

    #[tokio::test]
    async fn test_invalid_update_keeps_old_strategy() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let mut engine = StrategyEngine::new();
        engine.set_param_update_receiver(rx);

        // Build and add a valid strategy.
        let config = make_spread_config("s1", 10);
        let strategy = crate::build_strategy_from_config(&config).unwrap();
        engine.add_strategy(strategy).unwrap();

        // Send an update with invalid params (unsupported strategy type).
        let bad_config = mm_core::StrategyConfig {
            strategy_id: "s1".to_string(),
            strategy_type: "nonexistent_type".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            enabled: true,
            params: serde_json::json!({}),
        };

        tx.send(super::StrategyParamUpdate {
            strategy_id: "s1".to_string(),
            config: bad_config,
        })
        .await
        .unwrap();

        // Apply updates — should fail and keep the old strategy.
        engine.try_apply_updates();

        // Strategy should still exist (old one kept).
        assert_eq!(engine.strategy_count(), 1);
        assert!(engine.has_strategy("s1"));
    }

    #[tokio::test]
    async fn test_update_unknown_strategy_ignored() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let mut engine = StrategyEngine::new();
        engine.set_param_update_receiver(rx);

        // Send an update for a strategy that doesn't exist.
        let config = make_spread_config("nonexistent", 10);
        tx.send(super::StrategyParamUpdate {
            strategy_id: "nonexistent".to_string(),
            config,
        })
        .await
        .unwrap();

        // Should not panic — just ignore the update.
        engine.try_apply_updates();

        assert_eq!(engine.strategy_count(), 0);
    }

    #[test]
    fn test_try_apply_updates_no_receiver() {
        let mut engine = StrategyEngine::new();
        // No receiver attached — should be a no-op.
        engine.try_apply_updates();
    }

    // -----------------------------------------------------------------------
    // Panic isolation mocks
    // -----------------------------------------------------------------------

    /// A strategy that unconditionally panics in `on_tick`. Used to verify
    /// that the engine's `catch_unwind` isolation works correctly.
    struct PanickingStrategy {
        id: String,
    }

    impl PanickingStrategy {
        fn new(id: &str) -> Self {
            Self { id: id.to_string() }
        }
    }

    impl Strategy for PanickingStrategy {
        fn name(&self) -> &str {
            "panicking"
        }

        fn strategy_id(&self) -> &str {
            &self.id
        }

        fn exchange(&self) -> Exchange {
            Exchange::Simulated
        }

        fn symbol(&self) -> Symbol {
            Symbol::new("BTC", "USDT")
        }

        fn on_tick(&mut self, _snapshot: &MarketSnapshot) -> Vec<OrderAction> {
            panic!("test panic");
        }

        fn on_fill(&mut self, _fill: &Fill) {}

        fn on_order_update(&mut self, _update: &OrderUpdate) {}

        fn is_active(&self) -> bool {
            true
        }

        fn stop(&mut self) -> Vec<OrderAction> {
            Vec::new()
        }
    }

    /// A strategy that returns a known action on every tick. Used alongside
    /// `PanickingStrategy` to verify that the engine continues processing
    /// after a panic.
    struct WorkingStrategy {
        id: String,
        tick_actions: Vec<OrderAction>,
    }

    impl WorkingStrategy {
        fn new(id: &str, actions: Vec<OrderAction>) -> Self {
            Self {
                id: id.to_string(),
                tick_actions: actions,
            }
        }
    }

    impl Strategy for WorkingStrategy {
        fn name(&self) -> &str {
            "working"
        }

        fn strategy_id(&self) -> &str {
            &self.id
        }

        fn exchange(&self) -> Exchange {
            Exchange::Simulated
        }

        fn symbol(&self) -> Symbol {
            Symbol::new("BTC", "USDT")
        }

        fn on_tick(&mut self, _snapshot: &MarketSnapshot) -> Vec<OrderAction> {
            self.tick_actions.clone()
        }

        fn on_fill(&mut self, _fill: &Fill) {}

        fn on_order_update(&mut self, _update: &OrderUpdate) {}

        fn is_active(&self) -> bool {
            true
        }

        fn stop(&mut self) -> Vec<OrderAction> {
            Vec::new()
        }
    }

    // -----------------------------------------------------------------------
    // Panic isolation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tick_isolates_panicking_strategy() {
        let mut engine = StrategyEngine::new();

        // Add a panicking strategy first, then a working one.
        let panicker = PanickingStrategy::new("panicker");
        let worker = WorkingStrategy::new(
            "worker",
            vec![make_place_action("worker"), make_place_action("worker")],
        );

        engine.add_strategy(Box::new(panicker)).unwrap();
        engine.add_strategy(Box::new(worker)).unwrap();

        let snapshot = make_snapshot();

        // This must not crash — the panicking strategy is caught.
        let actions = engine.tick(&snapshot);

        // The working strategy's 2 actions should still be returned.
        assert_eq!(actions.len(), 2);

        // The panicking strategy should be recorded.
        assert!(engine.panicked_strategies().contains("panicker"));
        assert!(!engine.panicked_strategies().contains("worker"));
    }

    #[test]
    fn test_panicking_strategy_logged() {
        let mut engine = StrategyEngine::new();

        // Add two working strategies and one panicking strategy in the middle.
        let w1 = WorkingStrategy::new("w1", vec![make_place_action("w1")]);
        let panicker = PanickingStrategy::new("bad_strat");
        let w2 = WorkingStrategy::new("w2", vec![make_cancel_all_action()]);

        engine.add_strategy(Box::new(w1)).unwrap();
        engine.add_strategy(Box::new(panicker)).unwrap();
        engine.add_strategy(Box::new(w2)).unwrap();

        let snapshot = make_snapshot();

        // Engine should return the actions from both working strategies
        // and gracefully handle the panic from the middle one.
        let actions = engine.tick(&snapshot);

        // w1 produces 1 action, w2 produces 1 action => 2 total.
        // The panicking strategy's actions are skipped.
        assert_eq!(actions.len(), 2);

        // Verify the panicked strategy is tracked.
        assert_eq!(engine.panicked_strategies().len(), 1);
        assert!(engine.panicked_strategies().contains("bad_strat"));

        // A second tick should still work — the panicked strategy panics again
        // but the engine remains stable.
        let actions2 = engine.tick(&snapshot);
        assert_eq!(actions2.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Exchange+symbol filtering tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tick_filters_by_exchange_symbol() {
        let mut engine = StrategyEngine::new();

        // Strategy for BTC/USDT on Simulated exchange
        let btc_strategy = MockStrategy::new("btc_strat", "BtcMM", true)
            .with_exchange_symbol(Exchange::Simulated, Symbol::new("BTC", "USDT"))
            .with_tick_actions(vec![make_place_action("btc_strat")]);

        // Strategy for ETH/USDT on Simulated exchange
        let eth_strategy = MockStrategy::new("eth_strat", "EthMM", true)
            .with_exchange_symbol(Exchange::Simulated, Symbol::new("ETH", "USDT"))
            .with_tick_actions(vec![make_place_action("eth_strat")]);

        engine.add_strategy(Box::new(btc_strategy)).unwrap();
        engine.add_strategy(Box::new(eth_strategy)).unwrap();

        // Tick with a BTC/USDT snapshot (which make_snapshot() produces).
        let snapshot = make_snapshot(); // Exchange::Simulated, BTC/USDT
        let actions = engine.tick(&snapshot);

        // Only the BTC strategy should have been ticked.
        assert_eq!(
            actions.len(),
            1,
            "Only the BTC strategy should produce actions for a BTC snapshot"
        );

        // Verify the action came from the BTC strategy.
        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.strategy_id, "btc_strat");
            }
            _ => panic!("expected Place action from btc_strat"),
        }

        // Now tick with an ETH/USDT snapshot.
        let eth_sym = Symbol::new("ETH", "USDT");
        let now = Utc::now();
        let eth_snapshot = MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: eth_sym.clone(),
            order_book: std::sync::Arc::new(OrderBook {
                exchange: Exchange::Simulated,
                symbol: eth_sym.clone(),
                bids: vec![PriceLevel {
                    price: dec!(3000),
                    quantity: dec!(1),
                }],
                asks: vec![PriceLevel {
                    price: dec!(3001),
                    quantity: dec!(1),
                }],
                timestamp: now,
                sequence: 1,
            }),
            last_trade: None,
            position: Position::new(Exchange::Simulated, eth_sym),
            open_orders: vec![],
            timestamp: now,
        };

        let eth_actions = engine.tick(&eth_snapshot);

        // Only the ETH strategy should have been ticked.
        assert_eq!(
            eth_actions.len(),
            1,
            "Only the ETH strategy should produce actions for an ETH snapshot"
        );

        match &eth_actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.strategy_id, "eth_strat");
            }
            _ => panic!("expected Place action from eth_strat"),
        }
    }

    #[test]
    fn test_tick_filters_by_exchange() {
        let mut engine = StrategyEngine::new();

        // Two strategies for BTC/USDT but on different exchanges.
        let binance_strategy = MockStrategy::new("binance_strat", "BinanceMM", true)
            .with_exchange_symbol(Exchange::Binance, Symbol::new("BTC", "USDT"))
            .with_tick_actions(vec![make_place_action("binance_strat")]);

        let simulated_strategy = MockStrategy::new("sim_strat", "SimMM", true)
            .with_exchange_symbol(Exchange::Simulated, Symbol::new("BTC", "USDT"))
            .with_tick_actions(vec![make_place_action("sim_strat")]);

        engine.add_strategy(Box::new(binance_strategy)).unwrap();
        engine.add_strategy(Box::new(simulated_strategy)).unwrap();

        // Tick with a Simulated exchange BTC/USDT snapshot.
        let snapshot = make_snapshot(); // Exchange::Simulated, BTC/USDT
        let actions = engine.tick(&snapshot);

        // Only the Simulated strategy should fire.
        assert_eq!(
            actions.len(),
            1,
            "Only the Simulated exchange strategy should produce actions"
        );

        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.strategy_id, "sim_strat");
            }
            _ => panic!("expected Place action from sim_strat"),
        }
    }
}
