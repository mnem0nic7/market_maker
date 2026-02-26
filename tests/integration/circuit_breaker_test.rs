//! Integration tests for the multi-level circuit breaker (L1 through L5).
//!
//! These tests exercise the full cascade of breaker levels through realistic
//! trading scenarios, complementing the unit tests in `mm-risk`.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_core::{
    types::{Exchange, NewOrder, OrderType, Position, Side, Symbol, TimeInForce},
    PortfolioRisk, RiskAction,
};
use mm_risk::{
    BreakerLevel, BreakerState, CircuitBreaker, CircuitBreakerConfig, L1Config, L2Config, L3Config,
    L4Config, L5Config, OrderSizeCheck, OrderSizeConfig, PositionLimitCheck, PositionLimitConfig,
    RiskEngine,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

/// A circuit breaker config with generous thresholds that should not trigger
/// during normal operation.
fn relaxed_config() -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        l1: L1Config {
            max_order_quantity: dec!(100),
            max_order_notional: dec!(5_000_000),
        },
        l2: L2Config {
            max_orders_per_second: 50,
            cooldown_seconds: 5,
        },
        l3: L3Config {
            max_daily_loss: dec!(100_000),
            max_drawdown_pct: dec!(0.10),
            cooldown_seconds: 60,
        },
        l4: L4Config {
            max_gross_exposure: dec!(10_000_000),
            max_position_per_symbol: dec!(500),
        },
        l5: L5Config {
            max_catastrophic_loss: dec!(500_000),
        },
    }
}

/// A circuit breaker config with very tight thresholds designed to trip easily
/// for cascade testing.
fn tight_config() -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        l1: L1Config {
            max_order_quantity: dec!(5),
            max_order_notional: dec!(250_000),
        },
        l2: L2Config {
            max_orders_per_second: 3,
            cooldown_seconds: 60,
        },
        l3: L3Config {
            max_daily_loss: dec!(1_000),
            max_drawdown_pct: dec!(0.02),
            cooldown_seconds: 60,
        },
        l4: L4Config {
            max_gross_exposure: dec!(500_000),
            max_position_per_symbol: dec!(10),
        },
        l5: L5Config {
            max_catastrophic_loss: dec!(5_000),
        },
    }
}

fn small_order() -> NewOrder {
    NewOrder {
        exchange: Exchange::Binance,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(50_000)),
        quantity: dec!(0.1),
        strategy_id: "integration_test".into(),
        reduce_only: false,
    }
}

fn order_with_quantity(qty: Decimal) -> NewOrder {
    NewOrder {
        exchange: Exchange::Binance,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(50_000)),
        quantity: qty,
        strategy_id: "integration_test".into(),
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

fn portfolio_with_loss(daily_pnl: Decimal) -> PortfolioRisk {
    PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: Decimal::ZERO,
        gross_exposure: dec!(100_000),
        max_drawdown: Decimal::ZERO,
        daily_pnl,
        positions: vec![],
        timestamp: Utc::now(),
    }
}

fn portfolio_with_exposure(gross_exposure: Decimal) -> PortfolioRisk {
    PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: gross_exposure,
        gross_exposure,
        max_drawdown: Decimal::ZERO,
        daily_pnl: Decimal::ZERO,
        positions: vec![Position {
            exchange: Exchange::Binance,
            symbol: make_symbol(),
            quantity: gross_exposure / dec!(50_000),
            average_entry_price: dec!(50_000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        }],
        timestamp: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// Test 1: Normal trading flow
// ---------------------------------------------------------------------------

#[test]
fn circuit_breaker_allows_normal_trading_flow() {
    let cb = CircuitBreaker::new(relaxed_config());
    let portfolio = empty_portfolio();

    // Submit several small orders -- all should be allowed.
    for _ in 0..10 {
        let result = cb.check(&small_order(), &portfolio);
        assert!(
            result.level.is_none(),
            "expected no breaker level triggered, got {:?}",
            result.level,
        );
        assert!(
            matches!(result.action, RiskAction::Allow),
            "expected Allow, got {:?}",
            result.action,
        );
    }

    // All trigger counts should remain at zero.
    assert_eq!(
        cb.trigger_count(BreakerLevel::Order),
        0,
        "L1 trigger count should be 0"
    );
    assert_eq!(
        cb.trigger_count(BreakerLevel::Rate),
        0,
        "L2 trigger count should be 0"
    );
    assert_eq!(
        cb.trigger_count(BreakerLevel::PnL),
        0,
        "L3 trigger count should be 0"
    );
    assert_eq!(
        cb.trigger_count(BreakerLevel::Position),
        0,
        "L4 trigger count should be 0"
    );
    assert_eq!(
        cb.trigger_count(BreakerLevel::System),
        0,
        "L5 trigger count should be 0"
    );

    // All levels should still be in Active state.
    assert_eq!(cb.level_state(BreakerLevel::Order), BreakerState::Active);
    assert_eq!(cb.level_state(BreakerLevel::Rate), BreakerState::Active);
    assert_eq!(cb.level_state(BreakerLevel::PnL), BreakerState::Active);
    assert_eq!(cb.level_state(BreakerLevel::Position), BreakerState::Active);
    assert_eq!(cb.level_state(BreakerLevel::System), BreakerState::Active);
}

// ---------------------------------------------------------------------------
// Test 2: L1 through L5 cascade
// ---------------------------------------------------------------------------

#[test]
fn circuit_breaker_l1_through_l5_cascade() {
    let cb = CircuitBreaker::new(tight_config());
    let portfolio = empty_portfolio();

    // --- Step 1: A valid small order should be allowed ---
    let result = cb.check(&small_order(), &portfolio);
    assert!(
        matches!(result.action, RiskAction::Allow),
        "small order should be allowed, got {:?}",
        result.action,
    );

    // --- Step 2: Oversized order trips L1 ---
    let oversized = order_with_quantity(dec!(10)); // exceeds max_order_quantity of 5
    let result = cb.check(&oversized, &portfolio);
    assert_eq!(
        result.level,
        Some(BreakerLevel::Order),
        "expected L1 trigger"
    );
    assert!(matches!(result.action, RiskAction::Reject { .. }));
    assert_eq!(cb.trigger_count(BreakerLevel::Order), 1);
    assert_eq!(cb.level_state(BreakerLevel::Order), BreakerState::Triggered);

    // --- Reset L1 ---
    cb.reset(BreakerLevel::Order);
    assert_eq!(cb.level_state(BreakerLevel::Order), BreakerState::Active);

    // --- Step 3: Rapid orders trip L2 ---
    // The tight config allows max 3 orders/second. We already placed 1 small order above
    // (which recorded a timestamp). Place 2 more to fill the window, then the 4th should trip.
    let _ = cb.check(&small_order(), &portfolio); // order #2
    let _ = cb.check(&small_order(), &portfolio); // order #3
    let result = cb.check(&small_order(), &portfolio); // order #4 -- should trip L2
    assert_eq!(
        result.level,
        Some(BreakerLevel::Rate),
        "expected L2 trigger, got {:?}",
        result.level,
    );
    assert!(matches!(result.action, RiskAction::Reject { .. }));
    assert_eq!(cb.trigger_count(BreakerLevel::Rate), 1);

    // --- Reset L2 ---
    cb.reset(BreakerLevel::Rate);

    // --- Step 4: Portfolio with daily losses trips L3 ---
    let loss_portfolio = portfolio_with_loss(dec!(-2_000)); // exceeds max_daily_loss of 1_000
    let result = cb.check(&small_order(), &loss_portfolio);
    assert_eq!(
        result.level,
        Some(BreakerLevel::PnL),
        "expected L3 trigger, got {:?}",
        result.level,
    );
    assert!(matches!(result.action, RiskAction::Reject { .. }));
    assert_eq!(cb.trigger_count(BreakerLevel::PnL), 1);

    // --- Reset L3 ---
    cb.reset(BreakerLevel::PnL);

    // --- Step 5: High exposure trips L4 ---
    let high_exposure_portfolio = portfolio_with_exposure(dec!(600_000)); // exceeds 500_000 limit
    let result = cb.check(&small_order(), &high_exposure_portfolio);
    assert_eq!(
        result.level,
        Some(BreakerLevel::Position),
        "expected L4 trigger, got {:?}",
        result.level,
    );
    assert!(matches!(result.action, RiskAction::Reject { .. }));
    assert_eq!(cb.trigger_count(BreakerLevel::Position), 1);

    // --- Reset L4 ---
    cb.reset(BreakerLevel::Position);

    // --- Step 6: Catastrophic loss trips L5 ---
    let catastrophic_portfolio = portfolio_with_loss(dec!(-10_000)); // exceeds max_catastrophic_loss of 5_000
    let result = cb.check(&small_order(), &catastrophic_portfolio);
    assert_eq!(
        result.level,
        Some(BreakerLevel::System),
        "expected L5 trigger, got {:?}",
        result.level,
    );
    assert!(matches!(result.action, RiskAction::KillSwitch { .. }));
    assert_eq!(cb.trigger_count(BreakerLevel::System), 1);

    // --- Step 7: L5 persists even with a healthy portfolio ---
    let result = cb.check(&small_order(), &empty_portfolio());
    assert_eq!(
        result.level,
        Some(BreakerLevel::System),
        "L5 should persist after trigger"
    );
    assert!(
        matches!(result.action, RiskAction::KillSwitch { .. }),
        "L5 should still return KillSwitch"
    );

    // --- Step 8: Manual reset of L5 restores normal operation ---
    cb.reset(BreakerLevel::System);
    assert_eq!(cb.level_state(BreakerLevel::System), BreakerState::Active);

    // Note: the original CircuitBreaker has accumulated order timestamps in its
    // L2 rate window from all the successful checks above (timestamps persist
    // even after reset). To verify that L5 reset truly restores normal operation
    // without L2 interference, create a fresh breaker that simulates the state
    // after L5 was the only triggered level.
    let cb_fresh = CircuitBreaker::new(tight_config());

    // Trip and reset L5 on the fresh breaker to verify the reset flow.
    let catastrophic = portfolio_with_loss(dec!(-10_000));
    let result = cb_fresh.check(&small_order(), &catastrophic);
    assert_eq!(result.level, Some(BreakerLevel::System));
    assert!(matches!(result.action, RiskAction::KillSwitch { .. }));

    // L5 persists with healthy portfolio.
    let result = cb_fresh.check(&small_order(), &empty_portfolio());
    assert!(matches!(result.action, RiskAction::KillSwitch { .. }));

    // Manual reset clears L5.
    cb_fresh.reset(BreakerLevel::System);
    let result = cb_fresh.check(&small_order(), &empty_portfolio());
    assert!(
        matches!(result.action, RiskAction::Allow),
        "after L5 reset, orders should be allowed again, got {:?}",
        result.action,
    );
}

// ---------------------------------------------------------------------------
// Test 3: Cooldown expiry
// ---------------------------------------------------------------------------

#[test]
fn circuit_breaker_cooldown_expiry() {
    // Use a config with 0-second cooldown for L2 and L3 so they expire
    // immediately on the next check.
    let config = CircuitBreakerConfig {
        l1: L1Config {
            max_order_quantity: dec!(100),
            max_order_notional: dec!(5_000_000),
        },
        l2: L2Config {
            max_orders_per_second: 2,
            cooldown_seconds: 0, // immediate cooldown expiry
        },
        l3: L3Config {
            max_daily_loss: dec!(1_000),
            max_drawdown_pct: dec!(0.02),
            cooldown_seconds: 0, // immediate cooldown expiry
        },
        l4: L4Config {
            max_gross_exposure: dec!(10_000_000),
            max_position_per_symbol: dec!(500),
        },
        l5: L5Config {
            max_catastrophic_loss: dec!(500_000),
        },
    };

    let cb = CircuitBreaker::new(config);
    let portfolio = empty_portfolio();

    // Submit orders to fill the L2 rate window (2 orders/sec max).
    let result = cb.check(&small_order(), &portfolio);
    assert!(matches!(result.action, RiskAction::Allow));
    let result = cb.check(&small_order(), &portfolio);
    assert!(matches!(result.action, RiskAction::Allow));

    // Third order should trip L2.
    let result = cb.check(&small_order(), &portfolio);
    assert_eq!(
        result.level,
        Some(BreakerLevel::Rate),
        "expected L2 trigger"
    );
    assert_eq!(cb.level_state(BreakerLevel::Rate), BreakerState::Triggered);

    // On the next check, the cooldown mechanism runs:
    //   Triggered -> Cooldown (first call of maybe_expire_cooldown).
    // Since Cooldown is blocking, the check returns Reject before evaluating
    // the L2 rate condition again.
    let result = cb.check(&small_order(), &portfolio);
    assert!(
        matches!(result.action, RiskAction::Reject { .. }),
        "should reject during cooldown transition, got {:?}",
        result.action,
    );

    // On the second check after trip, Cooldown -> Active (0s cooldown expired).
    // The breaker then re-evaluates L2 from Active state. Since the rate window
    // still has timestamps from earlier, L2 may re-trip or allow depending on
    // window pruning. Either way, the cooldown cycle completed.
    let _result = cb.check(&small_order(), &portfolio);

    // The cooldown expired, so the breaker re-evaluated from Active state.
    // If the rate window still has too many orders, L2 re-trips and we get Reject.
    // If the window cleared, we get Allow. Either way, the cooldown mechanism worked.
    // The important assertion is that we are no longer stuck in Cooldown state --
    // the breaker either returned to Active and re-evaluated, or re-tripped.
    let l2_state = cb.level_state(BreakerLevel::Rate);
    assert!(
        l2_state == BreakerState::Active || l2_state == BreakerState::Triggered,
        "after 0s cooldown, L2 should be Active or re-Triggered, not Cooldown; got {:?}",
        l2_state,
    );

    // Verify the trigger count incremented at least once (initial trip).
    assert!(
        cb.trigger_count(BreakerLevel::Rate) >= 1,
        "L2 should have been triggered at least once"
    );

    // Now verify L3 cooldown: use a fresh CircuitBreaker to avoid L2 rate
    // window interference from the earlier L2 tests (timestamps persist even
    // after reset).
    let l3_config = CircuitBreakerConfig {
        l1: L1Config {
            max_order_quantity: dec!(100),
            max_order_notional: dec!(5_000_000),
        },
        l2: L2Config {
            max_orders_per_second: 100, // high limit so L2 doesn't interfere
            cooldown_seconds: 0,
        },
        l3: L3Config {
            max_daily_loss: dec!(1_000),
            max_drawdown_pct: dec!(0.02),
            cooldown_seconds: 0, // immediate cooldown expiry
        },
        l4: L4Config {
            max_gross_exposure: dec!(10_000_000),
            max_position_per_symbol: dec!(500),
        },
        l5: L5Config {
            max_catastrophic_loss: dec!(500_000),
        },
    };
    let cb3 = CircuitBreaker::new(l3_config);

    let loss_portfolio = portfolio_with_loss(dec!(-2_000)); // exceeds L3 max_daily_loss of 1_000
    let result = cb3.check(&small_order(), &loss_portfolio);
    assert_eq!(result.level, Some(BreakerLevel::PnL));
    assert_eq!(cb3.level_state(BreakerLevel::PnL), BreakerState::Triggered);

    // First check after trip: Triggered -> Cooldown.
    let result = cb3.check(&small_order(), &empty_portfolio());
    assert!(matches!(result.action, RiskAction::Reject { .. }));

    // Second check: Cooldown -> Active (0s cooldown expired).
    // With a healthy portfolio, L3 should not re-trip.
    let result = cb3.check(&small_order(), &empty_portfolio());
    assert_eq!(cb3.level_state(BreakerLevel::PnL), BreakerState::Active);
    assert!(
        matches!(result.action, RiskAction::Allow),
        "after L3 cooldown expiry with healthy portfolio, should Allow, got {:?}",
        result.action,
    );
}

// ---------------------------------------------------------------------------
// Test 4: Circuit breaker + RiskEngine integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn circuit_breaker_with_risk_engine_integration() {
    // Set up a RiskEngine with OrderSizeCheck and PositionLimitCheck.
    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(5),
        max_order_notional: dec!(250_000),
    })));
    risk_engine.add_check(Box::new(PositionLimitCheck::new(PositionLimitConfig {
        max_position_per_symbol: dec!(10),
        max_gross_exposure: dec!(500_000),
    })));

    // Set up a CircuitBreaker with similar thresholds.
    let cb = CircuitBreaker::new(tight_config());
    let portfolio = empty_portfolio();

    // --- Scenario A: Both allow a valid small order ---
    let order = small_order();
    let risk_result = risk_engine.evaluate_order(&order, &portfolio).await;
    let cb_result = cb.check(&order, &portfolio);

    assert!(
        matches!(risk_result, RiskAction::Allow),
        "RiskEngine should allow small order, got {:?}",
        risk_result,
    );
    assert!(
        matches!(cb_result.action, RiskAction::Allow),
        "CircuitBreaker should allow small order, got {:?}",
        cb_result.action,
    );

    // --- Scenario B: Both reject an oversized order ---
    let oversized = order_with_quantity(dec!(20));

    let risk_result = risk_engine.evaluate_order(&oversized, &portfolio).await;
    let cb_result = cb.check(&oversized, &portfolio);

    // RiskEngine's OrderSizeCheck returns ReduceSize (not Reject) for oversized orders.
    assert!(
        matches!(
            risk_result,
            RiskAction::ReduceSize { .. } | RiskAction::Reject { .. }
        ),
        "RiskEngine should reduce or reject oversized order, got {:?}",
        risk_result,
    );

    // CircuitBreaker's L1 should trigger and reject.
    assert_eq!(
        cb_result.level,
        Some(BreakerLevel::Order),
        "CircuitBreaker L1 should trigger on oversized order"
    );
    assert!(
        matches!(cb_result.action, RiskAction::Reject { .. }),
        "CircuitBreaker should reject oversized order, got {:?}",
        cb_result.action,
    );

    // --- Scenario C: Both reject based on position limits ---
    cb.reset(BreakerLevel::Order); // clear L1
    let high_position_portfolio = PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: dec!(600_000),
        gross_exposure: dec!(600_000),
        max_drawdown: Decimal::ZERO,
        daily_pnl: Decimal::ZERO,
        positions: vec![Position {
            exchange: Exchange::Binance,
            symbol: make_symbol(),
            quantity: dec!(12), // exceeds per-symbol limit of 10
            average_entry_price: dec!(50_000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        }],
        timestamp: Utc::now(),
    };

    let order = small_order();
    let risk_result = risk_engine
        .evaluate_order(&order, &high_position_portfolio)
        .await;
    let cb_result = cb.check(&order, &high_position_portfolio);

    // RiskEngine's PositionLimitCheck should reject (position at limit, buying more).
    assert!(
        matches!(risk_result, RiskAction::Reject { .. }),
        "RiskEngine should reject when position exceeds limit, got {:?}",
        risk_result,
    );

    // CircuitBreaker L4 should trigger on gross exposure (600_000 > 500_000).
    assert_eq!(
        cb_result.level,
        Some(BreakerLevel::Position),
        "CircuitBreaker L4 should trigger on high exposure, got {:?}",
        cb_result.level,
    );
    assert!(
        matches!(cb_result.action, RiskAction::Reject { .. }),
        "CircuitBreaker should reject on high exposure, got {:?}",
        cb_result.action,
    );

    // --- Verify both are complementary safety layers ---
    // The RiskEngine and CircuitBreaker operate independently. Both flagged
    // the same dangerous orders/portfolios, demonstrating that they provide
    // overlapping but independently-evaluated protection.
    assert!(
        risk_engine.check_count() > 0,
        "RiskEngine should have active checks"
    );
    assert_eq!(
        cb.trigger_count(BreakerLevel::Order) + cb.trigger_count(BreakerLevel::Position),
        2,
        "CircuitBreaker should have triggered L1 and L4"
    );
}
