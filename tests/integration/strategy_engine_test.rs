//! Integration test: strategy engine lifecycle.
//!
//! Verifies the interaction between StrategyEngine, RiskEngine, and KillSwitch
//! in realistic end-to-end scenarios — all in-memory with no network or external
//! dependencies.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_core::{
    types::{
        Exchange, MarketSnapshot, OrderAction, OrderBook, Position, PriceLevel, Side, Symbol, Trade,
    },
    PortfolioRisk, RiskAction,
};
use mm_risk::{
    KillSwitch, KillSwitchConfig, KillSwitchTrigger, OrderSizeCheck, OrderSizeConfig, RiskEngine,
};
use mm_strategy::{SpreadConfig, SpreadStrategy, StrategyEngine};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

fn make_order_book(mid: Decimal) -> OrderBook {
    let spread = dec!(5);
    OrderBook {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        bids: vec![
            PriceLevel {
                price: mid - spread,
                quantity: dec!(1.0),
            },
            PriceLevel {
                price: mid - spread * dec!(2),
                quantity: dec!(2.0),
            },
        ],
        asks: vec![
            PriceLevel {
                price: mid + spread,
                quantity: dec!(1.0),
            },
            PriceLevel {
                price: mid + spread * dec!(2),
                quantity: dec!(2.0),
            },
        ],
        timestamp: Utc::now(),
        sequence: 1,
    }
}

fn make_snapshot(mid: Decimal) -> MarketSnapshot {
    MarketSnapshot {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        order_book: make_order_book(mid),
        last_trade: Some(Trade {
            exchange: Exchange::Simulated,
            symbol: make_symbol(),
            price: mid,
            quantity: dec!(0.1),
            side: Side::Buy,
            timestamp: Utc::now(),
            trade_id: "t1".into(),
        }),
        position: Position::new(Exchange::Simulated, make_symbol()),
        open_orders: Vec::new(),
        timestamp: Utc::now(),
    }
}

fn make_spread_config(strategy_id: &str, spread_bps: Decimal) -> SpreadConfig {
    SpreadConfig {
        strategy_id: strategy_id.into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        spread_bps,
        order_size: dec!(0.01),
        num_levels: 1,
        refresh_interval_ms: 0,
        min_spread_bps: dec!(5),
        reprice_threshold_bps: dec!(3),
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
        positions: Vec::new(),
        timestamp: Utc::now(),
    }
}

/// Extract the strategy_id from Place actions.
fn extract_strategy_ids(actions: &[OrderAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|a| match a {
            OrderAction::Place(order) => Some(order.strategy_id.clone()),
            _ => None,
        })
        .collect()
}

/// Check whether an action list contains a CancelAll.
fn has_cancel_all(actions: &[OrderAction]) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, OrderAction::CancelAll(_)))
}

// ---------------------------------------------------------------------------
// Test 1: Multi-strategy tick produces actions from both strategies
// ---------------------------------------------------------------------------

#[test]
fn strategy_engine_multi_strategy_tick() {
    let mut engine = StrategyEngine::new();

    // Add two SpreadStrategy instances with different configs.
    let config_a = make_spread_config("spread-alpha", dec!(10));
    let config_b = make_spread_config("spread-beta", dec!(15));

    engine
        .add_strategy(Box::new(SpreadStrategy::new(config_a)))
        .expect("should add spread-alpha");
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config_b)))
        .expect("should add spread-beta");

    assert_eq!(engine.strategy_count(), 2);
    assert_eq!(engine.active_count(), 2);

    // Build a market snapshot and tick the engine.
    let snapshot = make_snapshot(dec!(50000));
    let actions = engine.tick(&snapshot);

    // Each strategy with num_levels=1 produces 2 Place actions (1 bid + 1 ask).
    // Two strategies => at least 4 Place actions total.
    let place_actions: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, OrderAction::Place(_)))
        .collect();
    assert!(
        place_actions.len() >= 4,
        "expected at least 4 place actions from 2 strategies, got {}",
        place_actions.len()
    );

    // Verify both strategy_ids are present in the actions.
    let strategy_ids = extract_strategy_ids(&actions);
    assert!(
        strategy_ids.contains(&"spread-alpha".to_string()),
        "actions should include orders from spread-alpha"
    );
    assert!(
        strategy_ids.contains(&"spread-beta".to_string()),
        "actions should include orders from spread-beta"
    );

    // Verify each strategy contributed at least 2 actions (1 bid + 1 ask).
    let alpha_count = strategy_ids
        .iter()
        .filter(|id| *id == "spread-alpha")
        .count();
    let beta_count = strategy_ids
        .iter()
        .filter(|id| *id == "spread-beta")
        .count();
    assert!(
        alpha_count >= 2,
        "spread-alpha should produce at least 2 actions, got {alpha_count}"
    );
    assert!(
        beta_count >= 2,
        "spread-beta should produce at least 2 actions, got {beta_count}"
    );
}

// ---------------------------------------------------------------------------
// Test 2: stop_all cancels all orders and deactivates strategies
// ---------------------------------------------------------------------------

#[test]
fn strategy_engine_stop_cancels_all_orders() {
    let mut engine = StrategyEngine::new();

    let config = make_spread_config("spread-stop-test", dec!(10));
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config)))
        .expect("should add strategy");

    // Tick once to generate some orders (verifies the strategy is active).
    let snapshot = make_snapshot(dec!(50000));
    let tick_actions = engine.tick(&snapshot);
    assert!(
        !tick_actions.is_empty(),
        "strategy should produce actions on first tick"
    );

    // Stop all strategies.
    let stop_actions = engine.stop_all();

    // stop_all should return CancelAll actions (one per strategy).
    assert!(
        has_cancel_all(&stop_actions),
        "stop_all should produce CancelAll actions"
    );

    // All strategies should now be inactive.
    assert_eq!(
        engine.active_count(),
        0,
        "no strategies should be active after stop_all"
    );

    // The strategy is still registered (just inactive).
    assert_eq!(engine.strategy_count(), 1);

    // A subsequent tick should produce no actions (all strategies inactive).
    let snapshot2 = make_snapshot(dec!(50000));
    let post_stop_actions = engine.tick(&snapshot2);
    assert!(
        post_stop_actions.is_empty(),
        "tick after stop_all should produce no actions"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Dynamically add and remove strategies
// ---------------------------------------------------------------------------

#[test]
fn strategy_engine_add_remove_dynamic() {
    let mut engine = StrategyEngine::new();

    // Phase 1: Add a strategy, tick it, collect actions.
    let config_a = make_spread_config("spread-dynamic-a", dec!(10));
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config_a)))
        .expect("should add spread-dynamic-a");

    let snapshot = make_snapshot(dec!(50000));
    let actions_a = engine.tick(&snapshot);
    assert!(
        !actions_a.is_empty(),
        "spread-dynamic-a should produce actions"
    );

    // Phase 2: Remove the strategy. Should return the boxed strategy + cancel actions.
    let removed = engine.remove_strategy("spread-dynamic-a");
    assert!(removed.is_some(), "remove should return the strategy");

    let (removed_strategy, cancel_actions) = removed.unwrap();
    assert_eq!(removed_strategy.strategy_id(), "spread-dynamic-a");
    assert!(
        has_cancel_all(&cancel_actions),
        "removing a strategy should produce CancelAll actions"
    );

    // Engine should now be empty.
    assert_eq!(
        engine.strategy_count(),
        0,
        "engine should have no strategies"
    );
    assert!(!engine.has_strategy("spread-dynamic-a"));

    // Phase 3: Add a new strategy with a different config, tick again.
    let config_b = make_spread_config("spread-dynamic-b", dec!(20));
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config_b)))
        .expect("should add spread-dynamic-b");

    assert_eq!(engine.strategy_count(), 1);
    assert!(engine.has_strategy("spread-dynamic-b"));

    let snapshot2 = make_snapshot(dec!(50000));
    let actions_b = engine.tick(&snapshot2);
    assert!(
        !actions_b.is_empty(),
        "spread-dynamic-b should produce actions"
    );

    // Verify the new strategy's actions carry the correct strategy_id.
    let ids = extract_strategy_ids(&actions_b);
    assert!(
        ids.iter().all(|id| id == "spread-dynamic-b"),
        "all actions should belong to spread-dynamic-b, got: {:?}",
        ids
    );
}

// ---------------------------------------------------------------------------
// Test 4: Kill switch stops the strategy pipeline
// ---------------------------------------------------------------------------

#[test]
fn kill_switch_stops_strategy_pipeline() {
    // Create a KillSwitch, StrategyEngine, and a RiskEngine.
    let kill_switch = KillSwitch::new(KillSwitchConfig {
        max_daily_loss: dec!(10_000),
        max_position_per_symbol: dec!(100),
        exchange_disconnect_timeout_secs: 30,
        heartbeat_timeout_secs: 10,
        exchanges: vec![Exchange::Simulated],
    });

    let mut engine = StrategyEngine::new();
    let config = make_spread_config("spread-ks-test", dec!(10));
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config)))
        .expect("should add strategy");

    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(1.0),
        max_order_notional: dec!(1_000_000),
    })));

    // Phase 1: Normal operation — strategy produces actions, kill switch is inactive.
    assert!(
        !kill_switch.is_active(),
        "kill switch should start inactive"
    );

    let snapshot = make_snapshot(dec!(50000));

    // Simulate the main loop pattern: check kill switch before ticking.
    let actions = if !kill_switch.is_active() {
        engine.tick(&snapshot)
    } else {
        Vec::new()
    };

    assert!(
        !actions.is_empty(),
        "strategy should produce actions when kill switch is inactive"
    );

    // Phase 2: Activate the kill switch manually.
    let activation = kill_switch.activate(KillSwitchTrigger::ManualActivation {
        operator: "integration-test".into(),
    });

    assert!(kill_switch.is_active(), "kill switch should now be active");
    assert!(
        !activation.cancel_messages.is_empty(),
        "activation should produce cancel messages"
    );

    // Phase 3: The main loop should skip ticking when kill switch is active.
    let snapshot2 = make_snapshot(dec!(50000));
    let actions_after_ks = if !kill_switch.is_active() {
        engine.tick(&snapshot2)
    } else {
        Vec::new()
    };

    assert!(
        actions_after_ks.is_empty(),
        "no actions should be produced when kill switch is active"
    );

    // Phase 4: Verify the shared flag pattern works (another component polling).
    let flag = kill_switch.active_flag();
    assert!(
        flag.load(std::sync::atomic::Ordering::SeqCst),
        "shared flag should reflect active kill switch"
    );

    // Phase 5: Reset the kill switch and verify trading can resume.
    kill_switch.reset();
    assert!(
        !kill_switch.is_active(),
        "kill switch should be inactive after reset"
    );

    let snapshot3 = make_snapshot(dec!(50000));
    let actions_after_reset = if !kill_switch.is_active() {
        engine.tick(&snapshot3)
    } else {
        Vec::new()
    };

    assert!(
        !actions_after_reset.is_empty(),
        "strategy should resume producing actions after kill switch reset"
    );
}

// ---------------------------------------------------------------------------
// Test 5 (bonus): Full pipeline — strategy tick through risk evaluation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn strategy_engine_actions_pass_risk_checks() {
    let mut engine = StrategyEngine::new();

    let config = make_spread_config("spread-risk-pipeline", dec!(10));
    engine
        .add_strategy(Box::new(SpreadStrategy::new(config)))
        .expect("should add strategy");

    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(1.0),
        max_order_notional: dec!(1_000_000),
    })));

    let snapshot = make_snapshot(dec!(50000));
    let actions = engine.tick(&snapshot);
    assert!(!actions.is_empty(), "engine should produce actions");

    let portfolio = empty_portfolio();

    // Every Place action from the strategy should pass risk checks.
    for action in &actions {
        if let OrderAction::Place(new_order) = action {
            let risk_result = risk_engine.evaluate_order(new_order, &portfolio).await;
            assert!(
                matches!(risk_result, RiskAction::Allow),
                "risk should allow strategy order (size=0.01): {:?}",
                risk_result
            );
        }
    }
}
