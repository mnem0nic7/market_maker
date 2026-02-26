//! Criterion benchmarks for symbol normalization hot-path operations.
//!
//! Symbol normalization runs on every market data message and every order
//! submission, so it is squarely on the hot path. Benchmarks cover:
//! - to_exchange_symbol (canonical -> exchange-native) for Binance, Coinbase
//! - from_exchange_symbol (exchange-native -> canonical) for Binance, Kraken
//!
//! Kraken's `from_exchange_symbol` is the most complex case due to legacy
//! X/Z prefix mapping and hash-map lookups.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mm_core::types::{Exchange, Symbol};
use mm_market_data::normalizer::{from_exchange_symbol, to_exchange_symbol};

// ---------------------------------------------------------------------------
// to_exchange_symbol benchmarks
// ---------------------------------------------------------------------------

fn bench_to_exchange_symbol_binance(c: &mut Criterion) {
    let sym = Symbol::new("BTC", "USDT");

    c.bench_function("normalizer_to_exchange_binance", |b| {
        b.iter(|| {
            let result = to_exchange_symbol(black_box(Exchange::Binance), black_box(&sym));
            black_box(result);
        });
    });
}

fn bench_to_exchange_symbol_coinbase(c: &mut Criterion) {
    let sym = Symbol::new("BTC", "USD");

    c.bench_function("normalizer_to_exchange_coinbase", |b| {
        b.iter(|| {
            let result = to_exchange_symbol(black_box(Exchange::Coinbase), black_box(&sym));
            black_box(result);
        });
    });
}

// ---------------------------------------------------------------------------
// from_exchange_symbol benchmarks
// ---------------------------------------------------------------------------

fn bench_from_exchange_symbol_binance(c: &mut Criterion) {
    let raw = "BTCUSDT";

    c.bench_function("normalizer_from_exchange_binance", |b| {
        b.iter(|| {
            let result = from_exchange_symbol(black_box(Exchange::Binance), black_box(raw));
            black_box(result);
        });
    });
}

fn bench_from_exchange_symbol_kraken(c: &mut Criterion) {
    let mut group = c.benchmark_group("normalizer_from_exchange_kraken");

    // Kraken v2 slash-separated form
    group.bench_function("v2_slash_form", |b| {
        let raw = "XBT/USD";
        b.iter(|| {
            let result = from_exchange_symbol(black_box(Exchange::Kraken), black_box(raw));
            black_box(result);
        });
    });

    // Kraken legacy concatenated form (requires prefix scanning)
    group.bench_function("legacy_concatenated", |b| {
        let raw = "XXBTZUSD";
        b.iter(|| {
            let result = from_exchange_symbol(black_box(Exchange::Kraken), black_box(raw));
            black_box(result);
        });
    });

    // Kraken canonical pass-through (no prefix mapping needed)
    group.bench_function("canonical_passthrough", |b| {
        let raw = "SOL/USD";
        b.iter(|| {
            let result = from_exchange_symbol(black_box(Exchange::Kraken), black_box(raw));
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Additional: batch normalization (simulates processing a burst of market data)
// ---------------------------------------------------------------------------

fn bench_batch_normalization(c: &mut Criterion) {
    // Simulate normalizing 100 different symbols as would happen in a market data burst
    let symbols: Vec<(&str, Exchange)> = vec![
        ("BTCUSDT", Exchange::Binance),
        ("ETHUSDT", Exchange::Binance),
        ("SOLUSDT", Exchange::Binance),
        ("DOGEUSDT", Exchange::Binance),
        ("XRPUSDT", Exchange::Binance),
        ("BTC-USD", Exchange::Coinbase),
        ("ETH-USD", Exchange::Coinbase),
        ("SOL-USD", Exchange::Coinbase),
        ("XBT/USD", Exchange::Kraken),
        ("ETH/EUR", Exchange::Kraken),
    ];

    c.bench_function("normalizer_batch_10_symbols", |b| {
        b.iter(|| {
            for &(raw, exchange) in black_box(&symbols) {
                let result = from_exchange_symbol(exchange, raw);
                black_box(result);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_to_exchange_symbol_binance,
    bench_to_exchange_symbol_coinbase,
    bench_from_exchange_symbol_binance,
    bench_from_exchange_symbol_kraken,
    bench_batch_normalization,
);
criterion_main!(benches);
