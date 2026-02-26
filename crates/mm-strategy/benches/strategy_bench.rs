//! Criterion benchmarks for the strategy engine hot-path operations.
//!
//! The strategy engine's `tick()` method is called on every market data update
//! in the main trading loop, making it one of the most latency-sensitive paths
//! in the system. Benchmarks cover:
//! - Single SpreadStrategy tick with a realistic 20-level order book
//! - StrategyEngine tick dispatching to 3 concurrent strategies

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mm_core::traits::Strategy;
use mm_core::types::{Exchange, MarketSnapshot, OrderBook, Position, PriceLevel, Symbol};
use mm_strategy::engine::StrategyEngine;
use mm_strategy::spread::{SpreadConfig, SpreadStrategy};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a realistic order book with `num_levels` price levels on each side,
/// centered around `mid_price` with `tick_size` spacing and random-ish quantities.
fn make_realistic_order_book(
    mid_price: Decimal,
    num_levels: usize,
    tick_size: Decimal,
) -> OrderBook {
    let mut bids = Vec::with_capacity(num_levels);
    let mut asks = Vec::with_capacity(num_levels);

    for i in 1..=num_levels {
        let offset = tick_size * Decimal::from(i as u64);

        // Bids: descending from just below mid
        bids.push(PriceLevel {
            price: mid_price - offset,
            // Vary quantity per level to simulate real book shape
            quantity: dec!(0.5) + Decimal::from(i as u64) * dec!(0.1),
        });

        // Asks: ascending from just above mid
        asks.push(PriceLevel {
            price: mid_price + offset,
            quantity: dec!(0.5) + Decimal::from(i as u64) * dec!(0.1),
        });
    }

    OrderBook {
        exchange: Exchange::Simulated,
        symbol: Symbol::new("BTC", "USDT"),
        bids,
        asks,
        timestamp: chrono::Utc::now(),
        sequence: 1,
    }
}

/// Build a MarketSnapshot with a realistic order book.
fn make_snapshot(num_levels: usize) -> MarketSnapshot {
    let sym = Symbol::new("BTC", "USDT");
    let order_book = make_realistic_order_book(dec!(50000), num_levels, dec!(0.50));

    MarketSnapshot {
        exchange: Exchange::Simulated,
        symbol: sym.clone(),
        order_book: Arc::new(order_book),
        last_trade: None,
        position: Position::new(Exchange::Simulated, sym),
        open_orders: vec![],
        timestamp: chrono::Utc::now(),
    }
}

/// Create a SpreadStrategy with the given id and level count.
fn make_spread_strategy(id: &str, num_levels: usize) -> SpreadStrategy {
    let config = SpreadConfig {
        strategy_id: id.to_string(),
        exchange: Exchange::Simulated,
        symbol: Symbol::new("BTC", "USDT"),
        spread_bps: dec!(10),
        order_size: dec!(0.01),
        num_levels,
        refresh_interval_ms: 1000,
        min_spread_bps: dec!(5),
        reprice_threshold_bps: dec!(3),
    };
    SpreadStrategy::new(config)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_spread_strategy_tick(c: &mut Criterion) {
    let snapshot = make_snapshot(20);

    c.bench_function("strategy_spread_tick_20_levels", |b| {
        // Each iteration gets a fresh strategy so every tick is the "first tick"
        // (forces full quote generation, the most expensive path).
        b.iter(|| {
            let mut strategy = make_spread_strategy("bench-spread", 3);
            let actions = strategy.on_tick(black_box(&snapshot));
            black_box(actions);
        });
    });
}

fn bench_spread_strategy_reprice(c: &mut Criterion) {
    // Benchmark the reprice path: first tick sets baseline, second tick with
    // a moved price forces cancel-and-replace.
    let snapshot1 = make_snapshot(20);
    // Second snapshot with mid price moved beyond reprice_threshold_bps (3 bps)
    let mut snapshot2 = make_snapshot(20);
    // Shift order book: move mid by ~25 (5 bps of 50000 = 25) to trigger reprice
    let shifted_book = make_realistic_order_book(dec!(50025), 20, dec!(0.50));
    snapshot2.order_book = Arc::new(shifted_book);
    snapshot2.timestamp = chrono::Utc::now();

    c.bench_function("strategy_spread_reprice", |b| {
        b.iter(|| {
            let mut strategy = make_spread_strategy("bench-spread", 3);
            // First tick: establishes baseline
            let _ = strategy.on_tick(black_box(&snapshot1));
            // Second tick: triggers reprice due to mid price movement
            let actions = strategy.on_tick(black_box(&snapshot2));
            black_box(actions);
        });
    });
}

fn bench_engine_tick_multi_strategy(c: &mut Criterion) {
    let snapshot = make_snapshot(20);

    c.bench_function("engine_tick_3_strategies", |b| {
        b.iter(|| {
            // Fresh engine with 3 strategies each iteration to measure full tick cost
            let mut engine = StrategyEngine::new();
            engine
                .add_strategy(Box::new(make_spread_strategy("spread-1", 3)))
                .unwrap();
            engine
                .add_strategy(Box::new(make_spread_strategy("spread-2", 3)))
                .unwrap();
            engine
                .add_strategy(Box::new(make_spread_strategy("spread-3", 3)))
                .unwrap();

            let actions = engine.tick(black_box(&snapshot));
            black_box(actions);
        });
    });
}

fn bench_engine_tick_steady_state(c: &mut Criterion) {
    // Benchmark the steady-state case where the engine already has strategies
    // that have been ticked once, and now must decide whether to reprice.
    // With the same mid price and within the refresh interval, this should
    // exercise the fast "no action needed" path.
    let snapshot = make_snapshot(20);

    let mut engine = StrategyEngine::new();
    engine
        .add_strategy(Box::new(make_spread_strategy("spread-1", 3)))
        .unwrap();
    engine
        .add_strategy(Box::new(make_spread_strategy("spread-2", 3)))
        .unwrap();
    engine
        .add_strategy(Box::new(make_spread_strategy("spread-3", 3)))
        .unwrap();

    // Initial tick to set baseline mid price and refresh time
    let _ = engine.tick(&snapshot);

    c.bench_function("engine_tick_3_strategies_no_reprice", |b| {
        b.iter(|| {
            // Same snapshot, within refresh interval => strategies return empty
            let actions = engine.tick(black_box(&snapshot));
            black_box(actions);
        });
    });
}

criterion_group!(
    benches,
    bench_spread_strategy_tick,
    bench_spread_strategy_reprice,
    bench_engine_tick_multi_strategy,
    bench_engine_tick_steady_state,
);
criterion_main!(benches);
