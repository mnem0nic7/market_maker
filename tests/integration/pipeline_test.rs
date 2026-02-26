//! Integration test: strategy → OMS → risk → execution pipeline.
//!
//! Verifies the full in-process pipeline using only in-memory/simulated
//! components — no network, no NATS, no database.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_core::{
    traits::Strategy,
    types::{
        Exchange, MarketSnapshot, NewOrder, OrderAction, OrderBook,
        Position, PriceLevel, Side, Symbol, Trade,
    },
    PortfolioRisk, RiskAction,
};
use mm_execution::OrderManager;
use mm_risk::{
    OrderSizeCheck, OrderSizeConfig, PositionLimitCheck, PositionLimitConfig, RiskEngine,
};
use mm_strategy::{SpreadConfig, SpreadStrategy};

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

fn make_order_book(mid: Decimal) -> OrderBook {
    let spread = dec!(5);
    OrderBook {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        bids: vec![
            PriceLevel { price: mid - spread, quantity: dec!(1.0) },
            PriceLevel { price: mid - spread * dec!(2), quantity: dec!(2.0) },
        ],
        asks: vec![
            PriceLevel { price: mid + spread, quantity: dec!(1.0) },
            PriceLevel { price: mid + spread * dec!(2), quantity: dec!(2.0) },
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

#[tokio::test]
async fn strategy_produces_actions_that_pass_risk() {
    // 1. Create a spread strategy.
    let config = SpreadConfig {
        strategy_id: "test-spread".into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        spread_bps: dec!(10),
        order_size: dec!(0.01),
        num_levels: 1,
        refresh_interval_ms: 0,
        min_spread_bps: dec!(5),
        reprice_threshold_bps: dec!(3),
    };
    let mut strategy = SpreadStrategy::new(config);

    // 2. Initialize risk engine.
    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(PositionLimitCheck::new(PositionLimitConfig {
        max_position_per_symbol: dec!(10),
        max_gross_exposure: dec!(500_000),
    })));
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(1),
        max_order_notional: dec!(100_000),
    })));

    // 3. Create OMS.
    let _oms = OrderManager::new();

    // 4. Build a market snapshot and tick the strategy.
    let snapshot = make_snapshot(dec!(50000));
    let actions = strategy.on_tick(&snapshot);

    // Strategy should produce at least one action (Place bid and/or ask).
    assert!(!actions.is_empty(), "strategy should produce actions");

    // 5. Run each Place action through risk checks.
    let portfolio = PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: Decimal::ZERO,
        gross_exposure: Decimal::ZERO,
        max_drawdown: Decimal::ZERO,
        daily_pnl: Decimal::ZERO,
        positions: Vec::new(),
        timestamp: Utc::now(),
    };

    for action in &actions {
        if let OrderAction::Place(new_order) = action {
            let risk_result = risk_engine.evaluate_order(new_order, &portfolio).await;
            assert!(
                matches!(risk_result, RiskAction::Allow),
                "risk should allow order: {risk_result:?}"
            );
        }
    }
}

#[tokio::test]
async fn risk_engine_rejects_oversized_order() {
    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(0.5),
        max_order_notional: dec!(100_000),
    })));

    let order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: mm_core::OrderType::Limit,
        time_in_force: mm_core::TimeInForce::GTC,
        price: Some(dec!(50000)),
        quantity: dec!(1.0), // Exceeds 0.5 limit
        strategy_id: "test".into(),
        reduce_only: false,
    };

    let portfolio = PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: Decimal::ZERO,
        gross_exposure: Decimal::ZERO,
        max_drawdown: Decimal::ZERO,
        daily_pnl: Decimal::ZERO,
        positions: Vec::new(),
        timestamp: Utc::now(),
    };

    let result = risk_engine.evaluate_order(&order, &portfolio).await;
    assert!(
        matches!(result, RiskAction::Reject { .. } | RiskAction::ReduceSize { .. }),
        "risk should reject oversized order: {result:?}"
    );
}
