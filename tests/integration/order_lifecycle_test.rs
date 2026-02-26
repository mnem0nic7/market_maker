//! Integration test: end-to-end order lifecycle.
//!
//! Verifies the full order lifecycle using the SimulatedExchange, OrderManager,
//! Position tracking, and RiskEngine — all in-memory with no network or
//! external dependencies.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_backtest::{FeeModel, SimulatedExchange, SlippageModel};
use mm_core::{
    types::{
        Exchange, NewOrder, OrderBook, OrderStatus, OrderType, Position,
        PriceLevel, Side, Symbol, TimeInForce,
    },
    OrderUpdate, PortfolioRisk, RiskAction,
};
use mm_execution::OrderManager;
use mm_risk::{OrderSizeCheck, OrderSizeConfig, RiskEngine};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

/// Build a simple order book centered around a mid price.
fn make_order_book(best_bid: Decimal, best_ask: Decimal) -> OrderBook {
    OrderBook {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        bids: vec![
            PriceLevel { price: best_bid, quantity: dec!(2.0) },
            PriceLevel { price: best_bid - dec!(10), quantity: dec!(5.0) },
        ],
        asks: vec![
            PriceLevel { price: best_ask, quantity: dec!(2.0) },
            PriceLevel { price: best_ask + dec!(10), quantity: dec!(5.0) },
        ],
        timestamp: Utc::now(),
        sequence: 1,
    }
}

/// Create a zero-fee, zero-slippage simulated exchange for deterministic tests.
fn make_exchange(initial_capital: Decimal) -> SimulatedExchange {
    SimulatedExchange::new(
        initial_capital,
        FeeModel {
            maker_fee_rate: Decimal::ZERO,
            taker_fee_rate: Decimal::ZERO,
        },
        SlippageModel {
            slippage_bps: Decimal::ZERO,
        },
    )
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

// ---------------------------------------------------------------------------
// Test 1: Full order lifecycle — place, fill, position update
// ---------------------------------------------------------------------------

#[test]
fn test_full_order_lifecycle() {
    // Set up a simulated exchange with an order book.
    // Best bid = 50000, best ask = 50010.
    let mut exchange = make_exchange(dec!(1_000_000));
    let book = make_order_book(dec!(50000), dec!(50010));

    // 1. Register the order in the OMS (status = Pending).
    let oms = OrderManager::new();
    let new_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(49990)),   // Below best ask — should rest on the book
        quantity: dec!(1.0),
        strategy_id: "lifecycle-test".into(),
        reduce_only: false,
    };

    let oms_order = oms.register_order(&new_order);
    assert_eq!(oms_order.status, OrderStatus::Pending);

    // 2. Mark as Submitted (simulating the router sending to exchange).
    let submitted = oms.mark_submitted(&oms_order.client_order_id);
    assert!(submitted.is_some());
    assert_eq!(submitted.unwrap().status, OrderStatus::Submitted);

    // 3. Submit to the simulated exchange.
    let sim_order = exchange.submit_order(&new_order, &book).unwrap();
    assert_eq!(sim_order.status, OrderStatus::New);
    assert_eq!(exchange.open_orders().len(), 1);

    // No fills yet with the current book (our order is at 49990, best ask is 50010).
    let fills = exchange.check_fills(&book);
    assert!(fills.is_empty(), "order should not fill when price is below best ask");

    // 4. Move the market: ask drops to our limit price, triggering a fill.
    let new_book = make_order_book(dec!(49980), dec!(49990));
    let fills = exchange.check_fills(&new_book);
    assert_eq!(fills.len(), 1, "resting buy limit should fill when ask drops to limit price");
    assert_eq!(exchange.open_orders().len(), 0, "filled order should be removed from open orders");

    let fill = &fills[0];
    assert_eq!(fill.side, Side::Buy);
    assert_eq!(fill.quantity, dec!(1.0));
    assert_eq!(fill.price, dec!(49990)); // Filled at best ask = our limit
    assert_eq!(fill.exchange, Exchange::Simulated);
    assert_eq!(fill.symbol, make_symbol());

    // 5. Update OMS to reflect the fill (Submitted -> Filled).
    let update = OrderUpdate {
        client_order_id: oms_order.client_order_id.clone(),
        exchange_order_id: sim_order.exchange_order_id.clone(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        status: OrderStatus::Filled,
        filled_quantity: fill.quantity,
        remaining_quantity: Decimal::ZERO,
        average_fill_price: fill.price,
        last_fill_price: Some(fill.price),
        last_fill_quantity: Some(fill.quantity),
        fee: Some(fill.fee),
        fee_currency: Some(fill.fee_currency.clone()),
        timestamp: Utc::now(),
    };

    let updated_order = oms.update_order(&update);
    assert!(updated_order.is_some());
    let updated_order = updated_order.unwrap();
    assert_eq!(updated_order.status, OrderStatus::Filled);
    assert_eq!(updated_order.filled_quantity, dec!(1.0));

    // 6. Update position and verify PnL.
    let mut position = Position::new(Exchange::Simulated, make_symbol());
    assert!(position.is_flat());

    position.update_on_fill(fill);
    assert!(position.is_long());
    assert_eq!(position.quantity, dec!(1.0));
    assert_eq!(position.average_entry_price, dec!(49990));
    // No realized PnL yet (we only bought, haven't sold).
    assert_eq!(position.realized_pnl, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// Test 2: Round-trip PnL — buy then sell
// ---------------------------------------------------------------------------

#[test]
fn test_round_trip_pnl() {
    let mut exchange = make_exchange(dec!(1_000_000));

    // Phase 1: Buy 1.0 BTC at 50000 (market order, fills at best ask).
    let book = make_order_book(dec!(49990), dec!(50000));

    let buy_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
        price: None,
        quantity: dec!(1.0),
        strategy_id: "pnl-test".into(),
        reduce_only: false,
    };

    let order = exchange.submit_order(&buy_order, &book).unwrap();
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.average_fill_price, dec!(50000)); // Filled at best ask

    let buy_fills = exchange.check_fills(&book);
    assert_eq!(buy_fills.len(), 1);
    let buy_fill = &buy_fills[0];
    assert_eq!(buy_fill.price, dec!(50000));
    assert_eq!(buy_fill.quantity, dec!(1.0));

    // Update position after buying.
    let mut position = Position::new(Exchange::Simulated, make_symbol());
    position.update_on_fill(buy_fill);

    assert_eq!(position.quantity, dec!(1.0));
    assert_eq!(position.average_entry_price, dec!(50000));
    assert_eq!(position.realized_pnl, Decimal::ZERO);

    // Phase 2: Sell 1.0 BTC at 51000 (market order, fills at best bid).
    let sell_book = make_order_book(dec!(51000), dec!(51010));

    let sell_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Sell,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
        price: None,
        quantity: dec!(1.0),
        strategy_id: "pnl-test".into(),
        reduce_only: false,
    };

    let order = exchange.submit_order(&sell_order, &sell_book).unwrap();
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.average_fill_price, dec!(51000)); // Filled at best bid

    let sell_fills = exchange.check_fills(&sell_book);
    assert_eq!(sell_fills.len(), 1);
    let sell_fill = &sell_fills[0];
    assert_eq!(sell_fill.price, dec!(51000));
    assert_eq!(sell_fill.quantity, dec!(1.0));

    // Update position after selling.
    position.update_on_fill(sell_fill);

    // Position should be flat.
    assert!(position.is_flat(), "position should be flat after round trip");
    assert_eq!(position.quantity, Decimal::ZERO);

    // Realized PnL = (sell_price - buy_price) * quantity = (51000 - 50000) * 1.0 = 1000
    assert_eq!(
        position.realized_pnl,
        dec!(1000),
        "realized PnL should be 1000 after buy@50000 sell@51000"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Cancel order lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_cancel_order_lifecycle() {
    let mut exchange = make_exchange(dec!(1_000_000));
    let book = make_order_book(dec!(50000), dec!(50010));

    // Register and submit a limit order far from the market.
    let oms = OrderManager::new();
    let new_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(48000)),   // Far below market — should rest.
        quantity: dec!(0.5),
        strategy_id: "cancel-test".into(),
        reduce_only: false,
    };

    let oms_order = oms.register_order(&new_order);
    oms.mark_submitted(&oms_order.client_order_id);

    // Submit to exchange and verify it rests on the book.
    let sim_order = exchange.submit_order(&new_order, &book).unwrap();
    assert_eq!(sim_order.status, OrderStatus::New);
    assert_eq!(exchange.open_orders().len(), 1);

    // Verify no fills are produced.
    let fills_before = exchange.check_fills(&book);
    assert!(fills_before.is_empty(), "no fills should occur for a far-away limit order");

    // Cancel the order on the simulated exchange.
    let cancel_result = exchange.cancel_order(&sim_order.client_order_id);
    assert!(cancel_result.is_ok(), "cancel should succeed");
    assert_eq!(exchange.open_orders().len(), 0, "no open orders after cancel");

    // Verify no fills were produced during the entire lifecycle.
    let fills_after = exchange.check_fills(&book);
    assert!(fills_after.is_empty(), "no fills should exist after cancellation");

    // Update OMS to reflect the cancellation (Submitted -> Cancelled).
    let update = OrderUpdate {
        client_order_id: oms_order.client_order_id.clone(),
        exchange_order_id: sim_order.exchange_order_id.clone(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        status: OrderStatus::Cancelled,
        filled_quantity: Decimal::ZERO,
        remaining_quantity: dec!(0.5),
        average_fill_price: Decimal::ZERO,
        last_fill_price: None,
        last_fill_quantity: None,
        fee: None,
        fee_currency: None,
        timestamp: Utc::now(),
    };

    let updated = oms.update_order(&update);
    assert!(updated.is_some());
    assert_eq!(updated.unwrap().status, OrderStatus::Cancelled);

    // Verify the OMS shows no open orders.
    let open_orders = oms.get_open_orders();
    assert!(open_orders.is_empty(), "OMS should have no open orders after cancellation");
}

// ---------------------------------------------------------------------------
// Test 4: Order rejection by risk engine (oversized quantity)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_order_rejection_invalid_size() {
    // Create a RiskEngine with an OrderSizeCheck (max_quantity = 1.0).
    let mut risk_engine = RiskEngine::new();
    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(1.0),
        max_order_notional: dec!(1_000_000), // High enough to not trigger
    })));

    // Submit an order for 2.0 BTC — exceeds the 1.0 limit.
    let oversized_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(50000)),
        quantity: dec!(2.0),
        strategy_id: "risk-test".into(),
        reduce_only: false,
    };

    let result = risk_engine
        .evaluate_order(&oversized_order, &empty_portfolio())
        .await;

    // The risk engine should request a size reduction (not a hard reject).
    match result {
        RiskAction::ReduceSize { max_quantity, reason } => {
            assert_eq!(
                max_quantity,
                dec!(1.0),
                "risk engine should reduce quantity to max allowed"
            );
            assert!(
                reason.contains("max"),
                "reason should mention the limit: {reason}"
            );
        }
        other => panic!("expected ReduceSize, got: {other:?}"),
    }

    // Verify that an order within limits passes.
    let valid_order = NewOrder {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(50000)),
        quantity: dec!(0.5),
        strategy_id: "risk-test".into(),
        reduce_only: false,
    };

    let result = risk_engine
        .evaluate_order(&valid_order, &empty_portfolio())
        .await;
    assert!(
        matches!(result, RiskAction::Allow),
        "order within limits should be allowed: {result:?}"
    );
}
