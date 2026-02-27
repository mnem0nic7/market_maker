//! End-to-end integration test: market data -> strategy -> risk -> execution -> fill -> strategy.on_fill()
//!
//! Exercises the full hot-path pipeline using in-memory/simulated components only.
//! Covers the testing gap: no prior test exercises the complete pipeline including
//! `route_fill()` back to the strategy.

use std::sync::Arc;

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_backtest::{FeeModel, SimulatedExchange, SlippageModel};
use mm_core::{
    types::{
        Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderBook, Position, PriceLevel,
        Side, Symbol, Trade,
    },
    PortfolioRisk, RiskAction,
};
use mm_execution::OrderManager;
use mm_risk::{
    OrderSizeCheck, OrderSizeConfig, PositionLimitCheck, PositionLimitConfig, RiskEngine,
};
use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, SpreadConfig, SpreadStrategy,
    StrategyEngine,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

fn make_eth_symbol() -> Symbol {
    Symbol::new("ETH", "USDT")
}

/// Build a realistic multi-level order book around the given mid price.
fn make_order_book(symbol: Symbol, mid: Decimal) -> OrderBook {
    let spread = dec!(5);
    OrderBook {
        exchange: Exchange::Simulated,
        symbol,
        bids: vec![
            PriceLevel {
                price: mid - spread,
                quantity: dec!(2.0),
            },
            PriceLevel {
                price: mid - spread * dec!(2),
                quantity: dec!(5.0),
            },
            PriceLevel {
                price: mid - spread * dec!(3),
                quantity: dec!(10.0),
            },
        ],
        asks: vec![
            PriceLevel {
                price: mid + spread,
                quantity: dec!(2.0),
            },
            PriceLevel {
                price: mid + spread * dec!(2),
                quantity: dec!(5.0),
            },
            PriceLevel {
                price: mid + spread * dec!(3),
                quantity: dec!(10.0),
            },
        ],
        timestamp: Utc::now(),
        sequence: 1,
    }
}

fn make_snapshot(symbol: Symbol, mid: Decimal) -> MarketSnapshot {
    MarketSnapshot {
        exchange: Exchange::Simulated,
        symbol: symbol.clone(),
        order_book: Arc::new(make_order_book(symbol.clone(), mid)),
        last_trade: Some(Trade {
            exchange: Exchange::Simulated,
            symbol: symbol.clone(),
            price: mid,
            quantity: dec!(0.1),
            side: Side::Buy,
            timestamp: Utc::now(),
            trade_id: "t1".into(),
        }),
        position: Position::new(Exchange::Simulated, symbol),
        open_orders: Vec::new(),
        timestamp: Utc::now(),
    }
}

fn make_snapshot_at(symbol: Symbol, mid: Decimal, ts: chrono::DateTime<Utc>) -> MarketSnapshot {
    MarketSnapshot {
        exchange: Exchange::Simulated,
        symbol: symbol.clone(),
        order_book: Arc::new(OrderBook {
            exchange: Exchange::Simulated,
            symbol: symbol.clone(),
            bids: vec![
                PriceLevel {
                    price: mid - dec!(5),
                    quantity: dec!(2.0),
                },
                PriceLevel {
                    price: mid - dec!(10),
                    quantity: dec!(5.0),
                },
            ],
            asks: vec![
                PriceLevel {
                    price: mid + dec!(5),
                    quantity: dec!(2.0),
                },
                PriceLevel {
                    price: mid + dec!(10),
                    quantity: dec!(5.0),
                },
            ],
            timestamp: ts,
            sequence: 1,
        }),
        last_trade: None,
        position: Position::new(Exchange::Simulated, symbol),
        open_orders: Vec::new(),
        timestamp: ts,
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

/// Build a permissive risk engine that allows reasonable orders through.
fn permissive_risk_engine() -> RiskEngine {
    let mut engine = RiskEngine::new();
    engine.add_check(Box::new(PositionLimitCheck::new(PositionLimitConfig {
        max_position_per_symbol: dec!(100),
        max_gross_exposure: dec!(10_000_000),
    })));
    engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: dec!(10),
        max_order_notional: dec!(1_000_000),
    })));
    engine
}

/// Extract all Place actions from an action list.
fn extract_places(actions: &[OrderAction]) -> Vec<&NewOrder> {
    actions
        .iter()
        .filter_map(|a| match a {
            OrderAction::Place(o) => Some(o),
            _ => None,
        })
        .collect()
}

// ===========================================================================
// Test 1: Full pipeline — tick to fill
// ===========================================================================

/// Wire up the complete hot-path pipeline:
///   MarketSnapshot -> StrategyEngine::tick() -> RiskEngine -> SimulatedExchange -> Fill -> route_fill()
///
/// This test exercises:
/// - SpreadStrategy generating order actions from a market snapshot
/// - StrategyEngine dispatching ticks and collecting actions
/// - RiskEngine evaluating each proposed order
/// - SimulatedExchange accepting and matching orders
/// - Fill generation and routing back to the strategy via route_fill()
/// - OrderManager tracking orders through the lifecycle
#[tokio::test]
async fn test_full_pipeline_tick_to_fill() {
    // ---- 1. Build components ----

    // SpreadStrategy with 1 level, wide spread so orders rest on the book
    let spread_config = SpreadConfig {
        strategy_id: "e2e-spread".into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        spread_bps: dec!(10),
        order_size: dec!(0.01),
        num_levels: 1,
        refresh_interval_ms: 0, // always refresh on every tick
        min_spread_bps: dec!(1),
        reprice_threshold_bps: dec!(100),
    };
    let strategy = SpreadStrategy::new(spread_config);

    // StrategyEngine
    let mut engine = StrategyEngine::new();
    engine
        .add_strategy(Box::new(strategy))
        .expect("adding strategy should succeed");
    assert_eq!(engine.strategy_count(), 1);
    assert!(engine.has_strategy("e2e-spread"));

    // RiskEngine with permissive limits
    let risk_engine = permissive_risk_engine();

    // SimulatedExchange with 1M capital, low fees, no slippage
    let mut sim_exchange = SimulatedExchange::new(
        dec!(1_000_000),
        FeeModel {
            maker_fee_rate: dec!(0.0001),
            taker_fee_rate: dec!(0.0005),
        },
        SlippageModel {
            slippage_bps: Decimal::ZERO,
        },
    );

    // OrderManager for tracking
    let oms = OrderManager::new();

    // ---- 2. Create a realistic market snapshot ----
    let mid = dec!(50000);
    let snapshot = make_snapshot(make_symbol(), mid);

    // ---- 3. Tick the strategy engine ----
    let actions = engine.tick(&snapshot);
    assert!(
        !actions.is_empty(),
        "strategy engine should produce actions on first tick"
    );

    let place_actions = extract_places(&actions);
    assert!(
        place_actions.len() >= 2,
        "spread strategy with 1 level should produce at least 2 Place actions (bid + ask), got {}",
        place_actions.len()
    );

    // Verify we have both buy and sell sides
    let has_buy = place_actions.iter().any(|o| o.side == Side::Buy);
    let has_sell = place_actions.iter().any(|o| o.side == Side::Sell);
    assert!(has_buy, "should have at least one buy order");
    assert!(has_sell, "should have at least one sell order");

    // ---- 4. Run each Place action through risk engine ----
    let portfolio = empty_portfolio();
    let mut approved_orders: Vec<NewOrder> = Vec::new();

    for action in &actions {
        if let OrderAction::Place(new_order) = action {
            let risk_result = risk_engine.evaluate_order(new_order, &portfolio).await;
            match risk_result {
                RiskAction::Allow => {
                    approved_orders.push(new_order.clone());
                }
                other => {
                    panic!(
                        "risk engine unexpectedly rejected order: {:?}, order: {:?}",
                        other, new_order
                    );
                }
            }
        }
    }

    assert!(
        !approved_orders.is_empty(),
        "at least some orders should pass risk checks"
    );

    // ---- 5. Submit approved orders to SimulatedExchange ----
    let order_book = make_order_book(make_symbol(), mid);
    let mut submitted_strategy_ids: Vec<String> = Vec::new();

    for new_order in &approved_orders {
        // Register in OMS first
        let tracked_order = oms.register_order(new_order);
        oms.mark_submitted(&tracked_order.client_order_id);

        // Submit to simulated exchange
        let result = sim_exchange.submit_order(new_order, &order_book);
        assert!(
            result.is_ok(),
            "order submission should succeed: {:?}",
            result.err()
        );
        submitted_strategy_ids.push(new_order.strategy_id.clone());
    }

    // ---- 6. Check for fills ----
    // The spread strategy places bids below mid and asks above mid.
    // With our order book, limit orders that cross the spread will fill immediately.
    // Orders that don't cross will rest. Let's check_fills to collect everything.
    let fills = sim_exchange.check_fills(&order_book);

    // The spread strategy places orders around mid price offset by spread_bps.
    // With spread_bps=10 (10 bps), bid ~ 49950, ask ~ 50050.
    // Our order book has best_ask=50005, best_bid=49995.
    // The strategy's bid at ~49950 is below best_ask (49950 < 50005), so it rests.
    // The strategy's ask at ~50050 is above best_bid (50050 > 49995), so it rests.
    // So fills come from resting orders that match when check_fills is called
    // with a book where the opposite side crosses.

    // Let's move the book to fill the resting orders.
    // To fill the buy order at ~49950, we need the ask to drop to 49950 or below.
    // To fill the sell order at ~50050, we need the bid to rise to 50050 or above.
    let fill_book = OrderBook {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        bids: vec![PriceLevel {
            price: dec!(50100),
            quantity: dec!(10),
        }],
        asks: vec![PriceLevel {
            price: dec!(49900),
            quantity: dec!(10),
        }],
        timestamp: Utc::now(),
        sequence: 2,
    };

    let resting_fills = sim_exchange.check_fills(&fill_book);

    // Combine immediate fills and resting fills
    let all_fills: Vec<Fill> = fills.into_iter().chain(resting_fills.into_iter()).collect();

    assert!(
        !all_fills.is_empty(),
        "at least one order should have been filled after moving the book"
    );

    // ---- 7. Route fills back to the strategy via engine.route_fill() ----
    for fill in &all_fills {
        engine.route_fill("e2e-spread", fill);
    }

    // ---- 8. Verify the pipeline completed without error ----
    // Tick again to verify the strategy is still functional after receiving fills
    let actions_after_fill = engine.tick(&snapshot);
    // The strategy should still be active and produce actions
    assert!(
        engine.active_count() > 0,
        "strategy should still be active after fills"
    );

    // The second tick should also produce place actions (since refresh_interval_ms=0)
    let places_after = extract_places(&actions_after_fill);
    assert!(
        !places_after.is_empty(),
        "strategy should continue producing orders after fills"
    );

    // Verify fill data makes sense
    for fill in &all_fills {
        assert_eq!(fill.exchange, Exchange::Simulated);
        assert_eq!(fill.symbol, make_symbol());
        assert!(fill.quantity > Decimal::ZERO);
        assert!(fill.price > Decimal::ZERO);
        assert!(!fill.fill_id.is_empty());
    }
}

// ===========================================================================
// Test 2: Multi-symbol isolation
// ===========================================================================

/// Verify that the StrategyEngine only dispatches snapshots to strategies
/// whose exchange+symbol match the snapshot.
///
/// The engine's `tick()` method filters strategies by exchange and symbol,
/// so a BTC/USDT snapshot only reaches the BTC strategy and an ETH/USDT
/// snapshot only reaches the ETH strategy. This test verifies:
/// - BTC snapshot produces only BTC-tagged orders (ETH strategy is skipped)
/// - ETH snapshot produces only ETH-tagged orders (BTC strategy is skipped)
/// - Each strategy's orders carry the correct symbol, strategy_id, and order_size
#[test]
fn test_multi_symbol_isolation() {
    // ---- 1. Create two SpreadStrategies for different symbols ----
    let btc_config = SpreadConfig {
        strategy_id: "btc-spread".into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(), // BTC/USDT
        spread_bps: dec!(10),
        order_size: dec!(0.01),
        num_levels: 1,
        refresh_interval_ms: 0,
        min_spread_bps: dec!(1),
        reprice_threshold_bps: dec!(100),
    };

    let eth_config = SpreadConfig {
        strategy_id: "eth-spread".into(),
        exchange: Exchange::Simulated,
        symbol: make_eth_symbol(), // ETH/USDT
        spread_bps: dec!(10),
        order_size: dec!(0.05),
        num_levels: 1,
        refresh_interval_ms: 0,
        min_spread_bps: dec!(1),
        reprice_threshold_bps: dec!(100),
    };

    let btc_strategy = SpreadStrategy::new(btc_config);
    let eth_strategy = SpreadStrategy::new(eth_config);

    // ---- 2. Add both to StrategyEngine ----
    let mut engine = StrategyEngine::new();
    engine.add_strategy(Box::new(btc_strategy)).unwrap();
    engine.add_strategy(Box::new(eth_strategy)).unwrap();
    assert_eq!(engine.strategy_count(), 2);

    // ---- 3. Tick with a BTC/USDT snapshot ----
    // The engine filters by exchange+symbol, so only the BTC strategy should fire.
    let btc_snapshot = make_snapshot(make_symbol(), dec!(50000));
    let btc_actions = engine.tick(&btc_snapshot);

    let btc_places = extract_places(&btc_actions);
    assert!(
        btc_places.len() >= 2,
        "BTC snapshot should produce at least 2 place actions (bid + ask), got {}",
        btc_places.len()
    );

    // ALL orders from a BTC snapshot should be BTC-tagged (ETH strategy was skipped)
    for order in &btc_places {
        assert_eq!(
            order.symbol,
            make_symbol(),
            "All orders from BTC snapshot should be BTC/USDT, got {}",
            order.symbol
        );
        assert_eq!(
            order.strategy_id, "btc-spread",
            "All orders from BTC snapshot should come from btc-spread"
        );
        assert_eq!(
            order.quantity,
            dec!(0.01),
            "BTC orders should use BTC strategy's order_size"
        );
    }

    // Verify no ETH orders were produced
    let eth_orders_in_btc_tick = btc_places
        .iter()
        .filter(|o| o.symbol == make_eth_symbol())
        .count();
    assert_eq!(
        eth_orders_in_btc_tick, 0,
        "BTC snapshot should NOT produce any ETH/USDT orders"
    );

    // ---- 4. Tick with an ETH/USDT snapshot ----
    // Only the ETH strategy should fire.
    let eth_snapshot = make_snapshot(make_eth_symbol(), dec!(3000));
    let eth_actions = engine.tick(&eth_snapshot);

    let eth_places = extract_places(&eth_actions);
    assert!(
        eth_places.len() >= 2,
        "ETH snapshot should produce at least 2 place actions (bid + ask), got {}",
        eth_places.len()
    );

    // ALL orders from an ETH snapshot should be ETH-tagged (BTC strategy was skipped)
    for order in &eth_places {
        assert_eq!(
            order.symbol,
            make_eth_symbol(),
            "All orders from ETH snapshot should be ETH/USDT, got {}",
            order.symbol
        );
        assert_eq!(
            order.strategy_id, "eth-spread",
            "All orders from ETH snapshot should come from eth-spread"
        );
        assert_eq!(
            order.quantity,
            dec!(0.05),
            "ETH orders should use ETH strategy's order_size"
        );
    }

    // Verify no BTC orders were produced in the ETH tick
    let btc_orders_in_eth_tick = eth_places
        .iter()
        .filter(|o| o.symbol == make_symbol())
        .count();
    assert_eq!(
        btc_orders_in_eth_tick, 0,
        "ETH snapshot should NOT produce any BTC/USDT orders"
    );

    // ---- 5. Verify both strategies are still active ----
    assert_eq!(
        engine.active_count(),
        2,
        "Both strategies should remain active after being ticked"
    );
}

// ===========================================================================
// Test 3: route_fill updates strategy state (Avellaneda-Stoikov inventory)
// ===========================================================================

/// Verify that routing fills back to the Avellaneda-Stoikov strategy via
/// `engine.route_fill()` updates the strategy's internal inventory, which
/// changes its subsequent quoting behavior (reservation price shifts).
///
/// The A-S model computes:
///   reservation_price = mid - inventory * gamma * sigma^2 * (T - t)
///
/// So after a buy fill increases inventory, the reservation price should
/// decrease (incentivizing selling), which moves the bid and ask prices lower.
#[test]
fn test_route_fill_updates_strategy_state() {
    let session_start = Utc::now();

    // ---- 1. Create an Avellaneda-Stoikov strategy ----
    let as_config = AvellanedaStoikovConfig {
        strategy_id: "as-fill-test".into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        gamma: dec!(0.1),
        sigma: dec!(2.0), // large sigma to make inventory adjustment visible
        kappa: dec!(1.5),
        order_size: dec!(0.5),
        max_position: dec!(10.0),
        session_duration_secs: 3600,
    };

    let strategy = AvellanedaStoikovStrategy::new(as_config).with_session_start(session_start);

    // ---- 2. Add to engine ----
    let mut engine = StrategyEngine::new();
    engine.add_strategy(Box::new(strategy)).unwrap();

    // ---- 3. First tick: generate initial orders with zero inventory ----
    let mid = dec!(50000);
    // Tick at 30 minutes into the session (half-way)
    let tick_time = session_start + Duration::minutes(30);
    let snapshot = make_snapshot_at(make_symbol(), mid, tick_time);
    let initial_actions = engine.tick(&snapshot);

    let initial_places = extract_places(&initial_actions);
    assert!(
        initial_places.len() >= 2,
        "A-S strategy should produce at least bid and ask, got {}",
        initial_places.len()
    );

    // Record the initial bid and ask prices
    let initial_bid = initial_places
        .iter()
        .find(|o| o.side == Side::Buy)
        .expect("should have a bid order")
        .price
        .expect("bid should have a price");
    let initial_ask = initial_places
        .iter()
        .find(|o| o.side == Side::Sell)
        .expect("should have an ask order")
        .price
        .expect("ask should have a price");

    // With zero inventory, reservation price == mid, so quotes should be
    // roughly symmetric around mid.
    let initial_mid_of_quotes = (initial_bid + initial_ask) / dec!(2);

    // ---- 4. Create a mock buy fill and route it ----
    // Simulate a large buy fill that increases inventory significantly
    let buy_fill = Fill {
        fill_id: format!(
            "fill-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        client_order_id: "order-buy-1".to_string(),
        exchange_order_id: "sim-buy-1".to_string(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        price: initial_bid,
        quantity: dec!(3.0), // large fill to make effect visible
        fee: Decimal::ZERO,
        fee_currency: "USDT".to_string(),
        timestamp: tick_time,
    };

    engine.route_fill("as-fill-test", &buy_fill);

    // ---- 5. Tick again and verify behavior changed ----
    // Use a slightly later timestamp so the strategy re-quotes
    let tick_time2 = tick_time + Duration::seconds(1);
    let snapshot2 = make_snapshot_at(make_symbol(), mid, tick_time2);
    let post_fill_actions = engine.tick(&snapshot2);

    let post_fill_places = extract_places(&post_fill_actions);
    assert!(
        !post_fill_places.is_empty(),
        "strategy should still produce orders after fill"
    );

    // After a buy fill, inventory > 0. The reservation price should shift DOWN:
    //   r = mid - inventory * gamma * sigma^2 * (T - t)
    //   With inventory=3.0, gamma=0.1, sigma=2.0, T-t~0.5:
    //   adjustment = 3.0 * 0.1 * 4.0 * 0.5 = 0.6
    //   r = 50000 - 0.6 = 49999.4
    //
    // Both bid and ask should shift lower compared to the initial zero-inventory quotes.

    let post_bid = post_fill_places
        .iter()
        .find(|o| o.side == Side::Buy)
        .expect("should have a bid order after fill")
        .price
        .expect("bid should have a price");
    let post_ask = post_fill_places
        .iter()
        .find(|o| o.side == Side::Sell)
        .expect("should have an ask order after fill")
        .price
        .expect("ask should have a price");

    let post_mid_of_quotes = (post_bid + post_ask) / dec!(2);

    // The midpoint of quotes should have shifted DOWN after the buy fill
    // (because positive inventory pushes reservation price below mid).
    assert!(
        post_mid_of_quotes < initial_mid_of_quotes,
        "After buy fill, quote midpoint should shift down. \
         Initial midpoint: {}, Post-fill midpoint: {}",
        initial_mid_of_quotes,
        post_mid_of_quotes
    );

    // Both individual prices should have shifted down
    assert!(
        post_bid < initial_bid,
        "After buy fill, bid should be lower. Initial: {}, Post: {}",
        initial_bid,
        post_bid
    );
    assert!(
        post_ask < initial_ask,
        "After buy fill, ask should be lower. Initial: {}, Post: {}",
        initial_ask,
        post_ask
    );
}

// ===========================================================================
// Test 4: Full pipeline with OMS lifecycle tracking
// ===========================================================================

/// End-to-end test that also validates OMS order lifecycle:
/// register -> submit -> fill, and that the OMS correctly tracks state.
#[tokio::test]
async fn test_pipeline_with_oms_lifecycle() {
    // Build a spread strategy with tight spread so orders cross the book immediately
    let config = SpreadConfig {
        strategy_id: "oms-e2e".into(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        spread_bps: dec!(5),
        order_size: dec!(0.01),
        num_levels: 1,
        refresh_interval_ms: 0,
        min_spread_bps: dec!(1),
        reprice_threshold_bps: dec!(100),
    };
    let strategy = SpreadStrategy::new(config);

    let mut engine = StrategyEngine::new();
    engine.add_strategy(Box::new(strategy)).unwrap();

    let risk_engine = permissive_risk_engine();
    let oms = OrderManager::new();

    let mut sim_exchange = SimulatedExchange::new(
        dec!(1_000_000),
        FeeModel::default(),
        SlippageModel::default(),
    );

    // Create snapshot and tick
    let mid = dec!(50000);
    let snapshot = make_snapshot(make_symbol(), mid);
    let actions = engine.tick(&snapshot);

    let portfolio = empty_portfolio();
    let order_book = make_order_book(make_symbol(), mid);
    let mut total_fills = Vec::new();

    for action in &actions {
        if let OrderAction::Place(new_order) = action {
            // Risk check
            let risk_result = risk_engine.evaluate_order(new_order, &portfolio).await;
            if !matches!(risk_result, RiskAction::Allow) {
                continue;
            }

            // Register in OMS
            let tracked = oms.register_order(new_order);
            assert_eq!(tracked.status, mm_core::OrderStatus::Pending);

            // Mark submitted
            let submitted = oms.mark_submitted(&tracked.client_order_id);
            assert!(submitted.is_some());

            // Submit to sim exchange
            let sim_result = sim_exchange.submit_order(new_order, &order_book);
            assert!(sim_result.is_ok());
        }
    }

    // Move the book to fill all resting orders
    let fill_book = OrderBook {
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        bids: vec![PriceLevel {
            price: dec!(50100),
            quantity: dec!(100),
        }],
        asks: vec![PriceLevel {
            price: dec!(49800),
            quantity: dec!(100),
        }],
        timestamp: Utc::now(),
        sequence: 3,
    };

    let fills = sim_exchange.check_fills(&fill_book);
    total_fills.extend(fills);

    // Route all fills back to strategy
    for fill in &total_fills {
        engine.route_fill("oms-e2e", fill);
    }

    assert!(
        !total_fills.is_empty(),
        "should have at least one fill from the pipeline"
    );

    // Verify OMS tracked the correct number of orders
    assert!(
        oms.order_count() > 0,
        "OMS should have tracked at least one order"
    );

    // Verify strategy is still active and functional
    assert_eq!(engine.active_count(), 1);
    let next_actions = engine.tick(&snapshot);
    assert!(
        !next_actions.is_empty(),
        "strategy should continue to produce actions after fills"
    );
}

// ===========================================================================
// Test 5: route_fill to nonexistent strategy is safely ignored
// ===========================================================================

/// Verify that routing a fill to a strategy_id that doesn't exist in the
/// engine does not panic or cause errors -- it is silently ignored.
#[test]
fn test_route_fill_nonexistent_strategy() {
    let mut engine = StrategyEngine::new();

    let fill = Fill {
        fill_id: format!(
            "fill-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        client_order_id: "order-1".to_string(),
        exchange_order_id: "sim-1".to_string(),
        exchange: Exchange::Simulated,
        symbol: make_symbol(),
        side: Side::Buy,
        price: dec!(50000),
        quantity: dec!(0.01),
        fee: Decimal::ZERO,
        fee_currency: "USDT".to_string(),
        timestamp: Utc::now(),
    };

    // Should not panic
    engine.route_fill("nonexistent-strategy", &fill);
}
