//! Integration test: full backtest with spread strategy on synthetic data.

use rust_decimal_macros::dec;

use mm_backtest::{BacktestConfig, MarketEvent, ReplayEngine};
use mm_core::types::{Exchange, PriceLevel, Side, Symbol};
use mm_strategy::{SpreadConfig, SpreadStrategy};

use chrono::{Duration, Utc};

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

/// Generate a small synthetic dataset for the test.
fn generate_test_data(n: usize) -> Vec<MarketEvent> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let exchange = Exchange::Simulated;
    let symbol = make_symbol();
    let base_time = Utc::now() - Duration::seconds(n as i64);
    let mut mid_price: f64 = 50_000.0;
    let mut seed: u64 = 42;
    let mut events = Vec::with_capacity(n);

    for i in 0..n {
        let ts = base_time + Duration::seconds(i as i64);

        seed = {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            i.hash(&mut hasher);
            hasher.finish()
        };

        let step_pct = ((seed % 1000) as f64 / 1000.0 - 0.5) * 0.005;
        mid_price *= 1.0 + step_pct;
        mid_price = (mid_price / 0.01).round() * 0.01;
        if mid_price < 100.0 {
            mid_price = 100.0;
        }

        let spread_bps = 1.0 + (seed % 10) as f64;
        let half_spread = mid_price * spread_bps / 10_000.0 / 2.0;

        let best_bid = mid_price - half_spread;
        let best_ask = mid_price + half_spread;

        if i % 3 == 0 && i > 0 {
            let side = if seed % 2 == 0 { Side::Buy } else { Side::Sell };
            let price = if side == Side::Buy {
                best_ask
            } else {
                best_bid
            };
            let qty = 0.01 + (seed % 50) as f64 / 1000.0;

            events.push(MarketEvent::TradeEvent {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                price: rust_decimal::Decimal::from_f64_retain(price)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: rust_decimal::Decimal::from_f64_retain(qty)
                    .unwrap_or(dec!(0.01))
                    .round_dp(6),
                side,
                trade_id: format!("t-{i}"),
            });
        } else {
            let bids: Vec<PriceLevel> = (0..3)
                .map(|j| PriceLevel {
                    price: rust_decimal::Decimal::from_f64_retain(
                        best_bid - (j as f64 * half_spread * 0.5),
                    )
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                    quantity: dec!(0.5),
                })
                .collect();

            let asks: Vec<PriceLevel> = (0..3)
                .map(|j| PriceLevel {
                    price: rust_decimal::Decimal::from_f64_retain(
                        best_ask + (j as f64 * half_spread * 0.5),
                    )
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                    quantity: dec!(0.5),
                })
                .collect();

            events.push(MarketEvent::OrderBookSnapshot {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                bids,
                asks,
                sequence: i as u64,
            });
        }
    }

    events
}

#[test]
fn backtest_spread_strategy_completes_with_valid_report() {
    let events = generate_test_data(200);

    let config = SpreadConfig {
        strategy_id: "backtest-test".into(),
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

    let bt_config = BacktestConfig {
        initial_capital: dec!(100_000),
        fee_rate: dec!(0.001),
        slippage_bps: dec!(0),
        start_time: None,
        end_time: None,
    };

    let engine = ReplayEngine::new(bt_config);
    let result = engine.run(&mut strategy, events);

    // Basic sanity checks on the report.
    assert!(result.num_trades >= 0);
    assert!(result.num_fills >= 0);
    // Net PnL should equal total_pnl minus total_fees.
    assert_eq!(result.net_pnl, result.total_pnl - result.total_fees);
}

#[test]
fn backtest_with_no_events_produces_zero_report() {
    let config = SpreadConfig {
        strategy_id: "empty-test".into(),
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

    let bt_config = BacktestConfig::default();
    let engine = ReplayEngine::new(bt_config);
    let result = engine.run(&mut strategy, vec![]);

    assert_eq!(result.num_trades, 0);
    assert_eq!(result.num_fills, 0);
}
