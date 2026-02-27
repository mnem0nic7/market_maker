//! Criterion benchmarks for the Order Management System (OMS) hot-path operations.
//!
//! Benchmarks cover:
//! - Order registration (insert into primary index)
//! - Order status update (state transition with exchange index linkage)
//! - Open order queries (filtered scan)
//! - Dual-index lookup (client_order_id and exchange_order_id)

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mm_core::types::{Exchange, NewOrder, OrderStatus, OrderType, Side, Symbol, TimeInForce};
use mm_core::OrderUpdate;
use mm_execution::OrderManager;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sample_new_order(exchange: Exchange, strategy_id: &str) -> NewOrder {
    NewOrder {
        exchange,
        symbol: Symbol::new("BTC", "USDT"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        price: Some(dec!(50000)),
        quantity: dec!(0.1),
        strategy_id: strategy_id.to_string(),
        reduce_only: false,
    }
}

/// Pre-populate an OMS with `count` orders in the New state on the given exchange,
/// each linked to an exchange_order_id. Returns the OMS and vectors of
/// (client_order_id, exchange_order_id) pairs.
fn populated_oms(count: usize, exchange: Exchange) -> (OrderManager, Vec<(String, String)>) {
    let oms = OrderManager::new();
    let mut ids = Vec::with_capacity(count);

    for i in 0..count {
        let new_order = sample_new_order(exchange, "bench-strategy");
        let order = oms.register_order(&new_order);
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        oms.mark_submitted(&cid);

        // Submitted -> New (with exchange_order_id)
        let exch_id = format!("EXCH-{}", i);
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: Some(exch_id.clone()),
            exchange,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };
        oms.update_order(&update);

        ids.push((cid, exch_id));
    }

    (oms, ids)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_register_order(c: &mut Criterion) {
    let new_order = sample_new_order(Exchange::Binance, "bench-strategy");

    c.bench_function("oms_register_order", |b| {
        let oms = OrderManager::new();
        b.iter(|| {
            let order = oms.register_order(black_box(&new_order));
            black_box(order);
        });
    });
}

fn bench_update_order_status(c: &mut Criterion) {
    c.bench_function("oms_update_order_status", |b| {
        b.iter_custom(|iters| {
            // Setup: create a fresh OMS and register+submit orders for each iteration batch
            let oms = OrderManager::new();
            let mut cids = Vec::with_capacity(iters as usize);

            for _ in 0..iters {
                let new_order = sample_new_order(Exchange::Binance, "bench-strategy");
                let order = oms.register_order(&new_order);
                oms.mark_submitted(&order.client_order_id);
                cids.push(order.client_order_id);
            }

            // Measure: transition Submitted -> New with exchange_order_id linkage
            let start = std::time::Instant::now();
            for (i, cid) in cids.iter().enumerate() {
                let update = OrderUpdate {
                    client_order_id: cid.clone(),
                    exchange_order_id: Some(format!("EXCH-{}", i)),
                    exchange: Exchange::Binance,
                    symbol: Symbol::new("BTC", "USDT"),
                    status: OrderStatus::New,
                    filled_quantity: Decimal::ZERO,
                    remaining_quantity: dec!(0.1),
                    average_fill_price: Decimal::ZERO,
                    last_fill_price: None,
                    last_fill_quantity: None,
                    fee: None,
                    fee_currency: None,
                    timestamp: chrono::Utc::now(),
                };
                black_box(oms.update_order(black_box(&update)));
            }
            start.elapsed()
        });
    });
}

fn bench_get_open_orders(c: &mut Criterion) {
    // Pre-populate with 500 orders: 250 active (New), 250 terminal (Filled)
    let oms = OrderManager::new();
    for _ in 0..250 {
        let new_order = sample_new_order(Exchange::Binance, "bench-strategy");
        let order = oms.register_order(&new_order);
        oms.mark_submitted(&order.client_order_id);
        let update = OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: Some(format!("EXCH-ACTIVE-{}", order.client_order_id)),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };
        oms.update_order(&update);
    }
    for _ in 0..250 {
        let new_order = sample_new_order(Exchange::Binance, "bench-strategy");
        let order = oms.register_order(&new_order);
        oms.mark_submitted(&order.client_order_id);
        let update = OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(0.1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(50000),
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: chrono::Utc::now(),
        };
        oms.update_order(&update);
    }

    c.bench_function("oms_get_open_orders_500_total", |b| {
        b.iter(|| {
            let open = oms.get_open_orders();
            black_box(open);
        });
    });
}

fn bench_dual_index_lookup(c: &mut Criterion) {
    let (oms, ids) = populated_oms(1000, Exchange::Binance);

    // Pick a stable pair from the middle of the population for lookup
    let (ref client_id, ref exchange_id) = ids[500];

    let mut group = c.benchmark_group("oms_dual_index_lookup");

    group.bench_function("by_client_order_id", |b| {
        b.iter(|| {
            let order = oms.get_order(black_box(client_id));
            black_box(order);
        });
    });

    group.bench_function("by_exchange_order_id", |b| {
        b.iter(|| {
            let order = oms.get_by_exchange_id(black_box(exchange_id));
            black_box(order);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_register_order,
    bench_update_order_status,
    bench_get_open_orders,
    bench_dual_index_lookup,
);
criterion_main!(benches);
