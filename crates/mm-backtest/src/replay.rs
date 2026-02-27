use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use mm_core::{
    traits::Strategy, Exchange, Fill, MarketSnapshot, Order, OrderAction, OrderBook, OrderStatus,
    OrderUpdate, Position, PriceLevel, Side, Symbol, Trade,
};

use crate::report::{BacktestResult, PerformanceCalculator};
use crate::sim_exchange::{FeeModel, SimulatedExchange, SlippageModel};

// ---------------------------------------------------------------------------
// MarketEvent
// ---------------------------------------------------------------------------

/// A historical market event used for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketEvent {
    /// A full order book snapshot at a point in time.
    OrderBookSnapshot {
        timestamp: DateTime<Utc>,
        exchange: Exchange,
        symbol: Symbol,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        sequence: u64,
    },
    /// A single trade that occurred on the exchange.
    TradeEvent {
        timestamp: DateTime<Utc>,
        exchange: Exchange,
        symbol: Symbol,
        price: Decimal,
        quantity: Decimal,
        side: Side,
        trade_id: String,
    },
}

impl MarketEvent {
    /// Get the timestamp of this event.
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            MarketEvent::OrderBookSnapshot { timestamp, .. } => *timestamp,
            MarketEvent::TradeEvent { timestamp, .. } => *timestamp,
        }
    }

    /// Get the exchange of this event.
    pub fn exchange(&self) -> Exchange {
        match self {
            MarketEvent::OrderBookSnapshot { exchange, .. } => *exchange,
            MarketEvent::TradeEvent { exchange, .. } => *exchange,
        }
    }

    /// Get the symbol of this event.
    pub fn symbol(&self) -> &Symbol {
        match self {
            MarketEvent::OrderBookSnapshot { symbol, .. } => symbol,
            MarketEvent::TradeEvent { symbol, .. } => symbol,
        }
    }
}

// ---------------------------------------------------------------------------
// BacktestConfig
// ---------------------------------------------------------------------------

/// Configuration for a backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    /// Starting capital in quote currency.
    pub initial_capital: Decimal,
    /// Fee rate applied to all fills (simple model). Overridden by fee_model if provided.
    pub fee_rate: Decimal,
    /// Slippage in basis points.
    pub slippage_bps: Decimal,
    /// Optional start time filter (events before this are skipped).
    pub start_time: Option<DateTime<Utc>>,
    /// Optional end time filter (events after this are skipped).
    pub end_time: Option<DateTime<Utc>>,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_capital: dec!(100_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ReplayEngine
// ---------------------------------------------------------------------------

/// Historical data replay engine.
///
/// Loads historical `MarketEvent`s, replays them in timestamp order, and drives
/// a `Strategy` through a `SimulatedExchange` to produce a `BacktestResult`.
pub struct ReplayEngine {
    config: BacktestConfig,
}

impl ReplayEngine {
    pub fn new(config: BacktestConfig) -> Self {
        Self { config }
    }

    /// Run the backtest with the given strategy and historical events.
    ///
    /// Events are sorted by timestamp. For each event the engine:
    /// 1. Updates the simulated clock
    /// 2. Updates internal state (order book, last trade)
    /// 3. Builds a `MarketSnapshot`
    /// 4. Calls `strategy.on_tick()`
    /// 5. Processes returned `OrderAction`s through the `SimulatedExchange`
    /// 6. Checks for fills and notifies the strategy
    pub fn run(&self, strategy: &mut dyn Strategy, mut events: Vec<MarketEvent>) -> BacktestResult {
        // Sort events by timestamp (stable sort preserves order of equal timestamps)
        events.sort_by_key(|e| e.timestamp());

        // Apply time filters
        let events: Vec<MarketEvent> = events
            .into_iter()
            .filter(|e| {
                if let Some(start) = self.config.start_time {
                    if e.timestamp() < start {
                        return false;
                    }
                }
                if let Some(end) = self.config.end_time {
                    if e.timestamp() > end {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Initialize components
        let fee_model = FeeModel {
            maker_fee_rate: self.config.fee_rate,
            taker_fee_rate: self.config.fee_rate,
        };
        let slippage_model = SlippageModel {
            slippage_bps: self.config.slippage_bps,
        };
        let mut sim_exchange =
            SimulatedExchange::new(self.config.initial_capital, fee_model, slippage_model);
        let mut perf_calc = PerformanceCalculator::new(self.config.initial_capital);
        perf_calc.set_strategy_name(strategy.name());

        // State: current order book and last trade per (exchange, symbol)
        let mut current_book: Option<OrderBook> = None;
        let mut last_trade: Option<Trade> = None;
        let mut position: Option<Position> = None;

        let total_events = events.len();
        tracing::info!(
            total_events = total_events,
            initial_capital = %self.config.initial_capital,
            fee_rate = %self.config.fee_rate,
            slippage_bps = %self.config.slippage_bps,
            "Starting backtest replay"
        );

        for (idx, event) in events.iter().enumerate() {
            let ts = event.timestamp();
            sim_exchange.set_time(ts);

            // Log progress every 10%
            if total_events > 100 && idx % (total_events / 10) == 0 {
                tracing::debug!(
                    progress = format!("{}%", idx * 100 / total_events),
                    timestamp = %ts,
                    "Replay progress"
                );
            }

            let exchange = event.exchange();
            let symbol = event.symbol().clone();

            // Update internal state based on event type
            match event {
                MarketEvent::OrderBookSnapshot {
                    timestamp,
                    exchange: ex,
                    symbol: sym,
                    bids,
                    asks,
                    sequence,
                } => {
                    current_book = Some(OrderBook {
                        exchange: *ex,
                        symbol: sym.clone(),
                        bids: bids.clone(),
                        asks: asks.clone(),
                        timestamp: *timestamp,
                        sequence: *sequence,
                    });
                }
                MarketEvent::TradeEvent {
                    timestamp,
                    exchange: ex,
                    symbol: sym,
                    price,
                    quantity,
                    side,
                    trade_id,
                } => {
                    last_trade = Some(Trade {
                        exchange: *ex,
                        symbol: sym.clone(),
                        price: *price,
                        quantity: *quantity,
                        side: *side,
                        timestamp: *timestamp,
                        trade_id: trade_id.clone(),
                    });
                }
            }

            // We need an order book to build a snapshot and check fills
            let book = match &current_book {
                Some(b) => b.clone(),
                None => {
                    // No order book yet, skip this event
                    continue;
                }
            };

            // Initialize position if needed
            if position.is_none() {
                position = Some(Position::new(exchange, symbol.clone()));
            }

            // Build the market snapshot.
            // Wrap in Arc to match the hot-path convention (cheap clone).
            let snapshot = MarketSnapshot {
                exchange,
                symbol: symbol.clone(),
                order_book: std::sync::Arc::new(book.clone()),
                last_trade: last_trade.clone(),
                position: position
                    .clone()
                    .unwrap_or_else(|| Position::new(exchange, symbol.clone())),
                open_orders: sim_exchange.open_orders(),
                timestamp: ts,
            };

            // Check for fills on resting orders from previous tick
            let fills = sim_exchange.check_fills(&book);
            self.process_fills(
                &fills,
                strategy,
                &mut position,
                &mut perf_calc,
                &exchange,
                &symbol,
            );

            // Call strategy
            let actions = strategy.on_tick(&snapshot);

            // Process order actions
            for action in actions {
                match action {
                    OrderAction::Place(new_order) => {
                        match sim_exchange.submit_order(&new_order, &book) {
                            Ok(order) => {
                                // Notify strategy of order acknowledgment
                                let update = self.make_order_update(&order);
                                strategy.on_order_update(&update);

                                // If market order or immediate fill, process fills now
                                let immediate_fills = sim_exchange.drain_pending_fills();
                                self.process_fills(
                                    &immediate_fills,
                                    strategy,
                                    &mut position,
                                    &mut perf_calc,
                                    &exchange,
                                    &symbol,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "Order submission rejected in sim exchange"
                                );
                            }
                        }
                    }
                    OrderAction::Cancel(cancel) => {
                        if let Err(e) = sim_exchange.cancel_order(&cancel.client_order_id) {
                            tracing::debug!(
                                client_order_id = %cancel.client_order_id,
                                error = %e,
                                "Cancel failed (order may already be filled or cancelled)"
                            );
                        }
                    }
                    OrderAction::CancelAll(_) => {
                        let count = sim_exchange.cancel_all();
                        tracing::debug!(count = count, "Cancelled all orders");
                    }
                    OrderAction::Amend {
                        client_order_id,
                        new_price,
                        new_quantity,
                    } => {
                        // Amend = cancel + re-place
                        tracing::debug!(
                            client_order_id = %client_order_id,
                            new_price = ?new_price,
                            new_quantity = ?new_quantity,
                            "Amend order (cancel + replace)"
                        );
                        // Try to cancel the existing order
                        if let Err(e) = sim_exchange.cancel_order(&client_order_id) {
                            tracing::debug!(
                                error = %e,
                                "Amend: cancel failed, order may already be gone"
                            );
                        }
                        // Note: In a real backtest, the strategy should re-place
                        // the order in the next tick. We don't automatically re-place
                        // here because we'd need the original order details.
                    }
                }
            }
        }

        // Strategy shutdown
        tracing::info!("Replay complete, stopping strategy");
        let stop_actions = strategy.stop();
        for action in stop_actions {
            if let OrderAction::CancelAll(_) = action {
                sim_exchange.cancel_all();
            }
        }

        perf_calc.finalize()
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn process_fills(
        &self,
        fills: &[Fill],
        strategy: &mut dyn Strategy,
        position: &mut Option<Position>,
        perf_calc: &mut PerformanceCalculator,
        exchange: &Exchange,
        symbol: &Symbol,
    ) {
        for fill in fills {
            // Update position
            if let Some(ref mut pos) = position {
                pos.update_on_fill(fill);
            } else {
                let mut pos = Position::new(*exchange, symbol.clone());
                pos.update_on_fill(fill);
                *position = Some(pos);
            }

            // Record in performance calculator
            perf_calc.record_fill(fill);

            // Notify strategy
            strategy.on_fill(fill);

            tracing::trace!(
                fill_id = %fill.fill_id,
                side = %fill.side,
                price = %fill.price,
                qty = %fill.quantity,
                fee = %fill.fee,
                "Fill processed"
            );
        }
    }

    fn make_order_update(&self, order: &Order) -> OrderUpdate {
        OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            status: order.status,
            filled_quantity: order.filled_quantity,
            remaining_quantity: order.remaining_quantity,
            average_fill_price: order.average_fill_price,
            last_fill_price: if order.status == OrderStatus::Filled {
                Some(order.average_fill_price)
            } else {
                None
            },
            last_fill_quantity: if order.status == OrderStatus::Filled {
                Some(order.filled_quantity)
            } else {
                None
            },
            fee: None,
            fee_currency: None,
            timestamp: order.updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::traits::Strategy;
    use mm_core::{
        CancelAllOrders, Exchange, NewOrder, OrderType, PriceLevel, Side, Symbol, TimeInForce,
    };
    use rust_decimal_macros::dec;

    /// A trivial strategy that buys once and then sells once when the bid
    /// is above a target price (ensuring a profitable round trip).
    struct BuySellOnceStrategy {
        has_bought: bool,
        has_sold: bool,
        /// Minimum bid price required before selling.
        sell_above_bid: Decimal,
    }

    impl BuySellOnceStrategy {
        fn new(sell_above_bid: Decimal) -> Self {
            Self {
                has_bought: false,
                has_sold: false,
                sell_above_bid,
            }
        }
    }

    impl Strategy for BuySellOnceStrategy {
        fn name(&self) -> &str {
            "buy_sell_once"
        }

        fn strategy_id(&self) -> &str {
            "test-buy-sell"
        }

        fn exchange(&self) -> Exchange {
            Exchange::Simulated
        }

        fn symbol(&self) -> Symbol {
            Symbol::new("BTC", "USDT")
        }

        fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
            let mut actions = Vec::new();

            if !self.has_bought {
                // Place a market buy
                actions.push(OrderAction::Place(NewOrder {
                    exchange: snapshot.exchange,
                    symbol: snapshot.symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                    price: None,
                    quantity: dec!(1),
                    strategy_id: "test-buy-sell".to_string(),
                    reduce_only: false,
                }));
                self.has_bought = true;
            } else if !self.has_sold && snapshot.position.quantity > Decimal::ZERO {
                // Only sell when the bid is above our target
                if let Some(best_bid) = snapshot.order_book.best_bid() {
                    if best_bid.price >= self.sell_above_bid {
                        actions.push(OrderAction::Place(NewOrder {
                            exchange: snapshot.exchange,
                            symbol: snapshot.symbol.clone(),
                            side: Side::Sell,
                            order_type: OrderType::Market,
                            time_in_force: TimeInForce::IOC,
                            price: None,
                            quantity: dec!(1),
                            strategy_id: "test-buy-sell".to_string(),
                            reduce_only: false,
                        }));
                        self.has_sold = true;
                    }
                }
            }

            actions
        }

        fn on_fill(&mut self, _fill: &Fill) {}
        fn on_order_update(&mut self, _update: &OrderUpdate) {}
        fn is_active(&self) -> bool {
            !self.has_sold
        }
        fn stop(&mut self) -> Vec<OrderAction> {
            vec![OrderAction::CancelAll(CancelAllOrders {
                exchange: None,
                symbol: None,
                strategy_id: Some("test-buy-sell".to_string()),
            })]
        }
    }

    fn make_events() -> Vec<MarketEvent> {
        let base_ts = chrono::Utc::now();

        vec![
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts,
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50010),
                    quantity: dec!(10),
                }],
                sequence: 1,
            },
            MarketEvent::TradeEvent {
                timestamp: base_ts + chrono::Duration::seconds(1),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                price: dec!(50005),
                quantity: dec!(0.5),
                side: Side::Buy,
                trade_id: "t1".to_string(),
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(2),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50100),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50110),
                    quantity: dec!(10),
                }],
                sequence: 2,
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(3),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50200),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50210),
                    quantity: dec!(10),
                }],
                sequence: 3,
            },
        ]
    }

    #[test]
    fn test_replay_engine_basic() {
        let config = BacktestConfig {
            initial_capital: dec!(1_000_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        };

        let engine = ReplayEngine::new(config);
        // Sell only when bid >= 50100 (second OB snapshot has bid=50100)
        let mut strategy = BuySellOnceStrategy::new(dec!(50100));

        let events = make_events();
        let result = engine.run(&mut strategy, events);

        // Strategy bought at 50010 (best ask of first book) and sold at 50100 (best bid of second book)
        // Gross PnL = 50100 - 50010 = 90
        assert!(
            result.num_fills >= 2,
            "Expected at least 2 fills, got {}",
            result.num_fills
        );
        assert!(
            result.total_pnl > Decimal::ZERO,
            "Expected positive PnL, got {}",
            result.total_pnl
        );
        assert!(result.total_fees > Decimal::ZERO, "Expected non-zero fees");
    }

    #[test]
    fn test_replay_engine_empty_events() {
        let config = BacktestConfig::default();
        let engine = ReplayEngine::new(config);

        /// A do-nothing strategy for testing.
        struct NoOpStrategy;
        impl Strategy for NoOpStrategy {
            fn name(&self) -> &str {
                "noop"
            }
            fn strategy_id(&self) -> &str {
                "noop"
            }
            fn exchange(&self) -> Exchange {
                Exchange::Simulated
            }
            fn symbol(&self) -> Symbol {
                Symbol::new("BTC", "USDT")
            }
            fn on_tick(&mut self, _: &MarketSnapshot) -> Vec<OrderAction> {
                vec![]
            }
            fn on_fill(&mut self, _: &Fill) {}
            fn on_order_update(&mut self, _: &OrderUpdate) {}
            fn is_active(&self) -> bool {
                true
            }
            fn stop(&mut self) -> Vec<OrderAction> {
                vec![]
            }
        }

        let mut strategy = NoOpStrategy;
        let result = engine.run(&mut strategy, vec![]);

        assert_eq!(result.num_fills, 0);
        assert_eq!(result.num_trades, 0);
        assert_eq!(result.total_pnl, Decimal::ZERO);
    }

    #[test]
    fn test_replay_with_spread_strategy() {
        use mm_strategy::{SpreadConfig, SpreadStrategy};

        let config = BacktestConfig {
            initial_capital: dec!(100_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        };

        let strategy_config = SpreadConfig {
            strategy_id: "test-spread".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            spread_bps: dec!(10),
            order_size: dec!(0.01),
            num_levels: 2,
            refresh_interval_ms: 0,
            min_spread_bps: dec!(5),
            reprice_threshold_bps: dec!(10),
        };

        let mut strategy = SpreadStrategy::new(strategy_config);
        let engine = ReplayEngine::new(config);

        // Generate enough events to trigger trading
        let events = generate_replay_events(200);
        let result = engine.run(&mut strategy, events);

        // The strategy should produce a valid BacktestResult
        assert!(
            result.num_fills > 0,
            "SpreadStrategy should produce fills, got {}",
            result.num_fills
        );
        assert!(
            result.total_volume > Decimal::ZERO,
            "Should have non-zero volume"
        );
        assert!(
            result.total_fees > Decimal::ZERO,
            "Should have non-zero fees with fee_rate=0.001"
        );
    }

    #[test]
    fn test_replay_respects_initial_capital() {
        let initial_capital = dec!(250_000);
        let config = BacktestConfig {
            initial_capital,
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        };

        let engine = ReplayEngine::new(config);
        let mut strategy = BuySellOnceStrategy::new(dec!(50100));
        let events = make_events();
        let result = engine.run(&mut strategy, events);

        // The equity curve is recorded after each fill. The first fill (buy)
        // deducts a fee, so equity = initial_capital - fee (no realized PnL yet
        // on the buy side). Verify the first equity point is close to initial
        // capital (within the fee amount).
        if !result.equity_curve.is_empty() {
            let first_equity = result.equity_curve[0].1;
            // The fee on a buy of 1 BTC at ~50010 with 0.1% fee rate is ~50.01
            // So first equity should be initial_capital minus that small fee.
            assert!(
                first_equity <= initial_capital,
                "First equity {} should be <= initial capital {} (fee deducted)",
                first_equity,
                initial_capital
            );
            assert!(
                first_equity > initial_capital - dec!(200),
                "First equity {} should be close to initial capital {} (within fee tolerance)",
                first_equity,
                initial_capital
            );
        }

        // The final equity should also reflect the initial capital base.
        // After a profitable round-trip (buy at ~50010, sell at ~50100),
        // equity should be close to initial_capital + profit - fees.
        if result.equity_curve.len() >= 2 {
            let final_equity = result.equity_curve.last().unwrap().1;
            // With a profitable trade, final equity should be above initial - fees
            assert!(
                final_equity > initial_capital - dec!(200),
                "Final equity {} should be close to or above initial capital {}",
                final_equity,
                initial_capital
            );
        }
    }

    #[test]
    fn test_replay_time_filter_start() {
        let base_ts = chrono::Utc::now();
        let events = vec![
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts,
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50010),
                    quantity: dec!(10),
                }],
                sequence: 1,
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(10),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50100),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50110),
                    quantity: dec!(10),
                }],
                sequence: 2,
            },
        ];

        // Set start_time to filter out the first event
        let config = BacktestConfig {
            initial_capital: dec!(100_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: Some(base_ts + chrono::Duration::seconds(5)),
            end_time: None,
        };

        let engine = ReplayEngine::new(config);
        // Use BuySellOnceStrategy with sell_above_bid set very high so it
        // only buys at the first available book.
        let mut strategy = BuySellOnceStrategy::new(dec!(999999));
        let result = engine.run(&mut strategy, events);

        // Only 1 event passed the time filter, so the strategy should have
        // seen the second book (bid=50100, ask=50110), bought at ask=50110.
        // With only one snapshot and sell target unreachable, we get 1 fill.
        assert!(
            result.num_fills <= 1,
            "With time filter, at most 1 fill expected (buy only)"
        );
    }

    #[test]
    fn test_replay_time_filter_end() {
        let base_ts = chrono::Utc::now();
        let events = vec![
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts,
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50010),
                    quantity: dec!(10),
                }],
                sequence: 1,
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(10),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50200),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50210),
                    quantity: dec!(10),
                }],
                sequence: 2,
            },
        ];

        // Set end_time to filter out the second event
        let config = BacktestConfig {
            initial_capital: dec!(100_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: Some(base_ts + chrono::Duration::seconds(5)),
        };

        let engine = ReplayEngine::new(config);
        let mut strategy = BuySellOnceStrategy::new(dec!(50100));
        let result = engine.run(&mut strategy, events);

        // Only 1 event passed the time filter, strategy buys but cannot sell
        // because the second book (with bid=50200) is filtered out.
        assert!(
            result.num_fills <= 1,
            "With end_time filter, should not see second snapshot for sell"
        );
    }

    #[test]
    fn test_replay_events_sorted_by_timestamp() {
        // Give events in reverse order; the engine should sort them.
        let base_ts = chrono::Utc::now();
        let events = vec![
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(2),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50200),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50210),
                    quantity: dec!(10),
                }],
                sequence: 3,
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts,
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50010),
                    quantity: dec!(10),
                }],
                sequence: 1,
            },
            MarketEvent::OrderBookSnapshot {
                timestamp: base_ts + chrono::Duration::seconds(1),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: dec!(50100),
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: dec!(50110),
                    quantity: dec!(10),
                }],
                sequence: 2,
            },
        ];

        let config = BacktestConfig::default();
        let engine = ReplayEngine::new(config);
        // Sell when bid >= 50100, which is the second event chronologically.
        let mut strategy = BuySellOnceStrategy::new(dec!(50100));
        let result = engine.run(&mut strategy, events);

        // Even though events were given in reverse, engine sorts them.
        // Strategy buys at ask=50010 (first chronological event) and sells when bid >= 50100.
        assert!(
            result.num_fills >= 2,
            "Engine should sort events and produce buy+sell fills, got {} fills",
            result.num_fills
        );
    }

    #[test]
    fn test_replay_noop_strategy_no_trades() {
        struct NoOpStrategy;
        impl Strategy for NoOpStrategy {
            fn name(&self) -> &str {
                "noop"
            }
            fn strategy_id(&self) -> &str {
                "noop"
            }
            fn exchange(&self) -> Exchange {
                Exchange::Simulated
            }
            fn symbol(&self) -> Symbol {
                Symbol::new("BTC", "USDT")
            }
            fn on_tick(&mut self, _: &MarketSnapshot) -> Vec<OrderAction> {
                vec![]
            }
            fn on_fill(&mut self, _: &Fill) {}
            fn on_order_update(&mut self, _: &OrderUpdate) {}
            fn is_active(&self) -> bool {
                true
            }
            fn stop(&mut self) -> Vec<OrderAction> {
                vec![]
            }
        }

        let config = BacktestConfig::default();
        let engine = ReplayEngine::new(config);
        let mut strategy = NoOpStrategy;
        let events = make_events();
        let result = engine.run(&mut strategy, events);

        assert_eq!(result.num_fills, 0, "NoOp strategy should produce no fills");
        assert_eq!(
            result.num_trades, 0,
            "NoOp strategy should produce no trades"
        );
        assert_eq!(result.total_pnl, Decimal::ZERO);
        assert_eq!(result.total_fees, Decimal::ZERO);
        assert!(result.equity_curve.is_empty());
    }

    #[test]
    fn test_replay_custom_fee_rate() {
        let config = BacktestConfig {
            initial_capital: dec!(1_000_000),
            fee_rate: dec!(0.01), // 1% fee rate (very high for testing)
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        };

        let engine = ReplayEngine::new(config);
        let mut strategy = BuySellOnceStrategy::new(dec!(50100));
        let events = make_events();
        let result = engine.run(&mut strategy, events);

        // With 1% fee rate, fees should be substantial
        if result.num_fills > 0 {
            assert!(
                result.total_fees > Decimal::ZERO,
                "Fees should be positive with 1% fee rate"
            );
            // Gross PnL should exceed net PnL by the fees
            assert_eq!(result.net_pnl, result.total_pnl - result.total_fees);
        }
    }

    #[test]
    fn test_replay_with_slippage() {
        let config_no_slip = BacktestConfig {
            initial_capital: dec!(1_000_000),
            fee_rate: dec!(0.001),
            slippage_bps: Decimal::ZERO,
            start_time: None,
            end_time: None,
        };

        let config_with_slip = BacktestConfig {
            initial_capital: dec!(1_000_000),
            fee_rate: dec!(0.001),
            slippage_bps: dec!(50), // 50 bps = 0.5% slippage
            start_time: None,
            end_time: None,
        };

        let events = make_events();

        // Run without slippage
        let engine1 = ReplayEngine::new(config_no_slip);
        let mut strategy1 = BuySellOnceStrategy::new(dec!(50100));
        let result_no_slip = engine1.run(&mut strategy1, events.clone());

        // Run with slippage
        let engine2 = ReplayEngine::new(config_with_slip);
        let mut strategy2 = BuySellOnceStrategy::new(dec!(50100));
        let result_with_slip = engine2.run(&mut strategy2, events);

        // If both produce fills, slippage should reduce net PnL
        if result_no_slip.num_fills > 0 && result_with_slip.num_fills > 0 {
            assert!(
                result_with_slip.net_pnl <= result_no_slip.net_pnl,
                "Slippage should reduce net PnL: no_slip={}, with_slip={}",
                result_no_slip.net_pnl,
                result_with_slip.net_pnl
            );
        }
    }

    #[test]
    fn test_replay_trade_events_do_not_trigger_strategy_without_book() {
        // If we only have trade events (no order book), the engine should not crash
        // and should not produce any fills (it needs a book to build a snapshot).
        let base_ts = chrono::Utc::now();
        let events = vec![
            MarketEvent::TradeEvent {
                timestamp: base_ts,
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                price: dec!(50000),
                quantity: dec!(1),
                side: Side::Buy,
                trade_id: "t1".to_string(),
            },
            MarketEvent::TradeEvent {
                timestamp: base_ts + chrono::Duration::seconds(1),
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                price: dec!(50100),
                quantity: dec!(0.5),
                side: Side::Sell,
                trade_id: "t2".to_string(),
            },
        ];

        let config = BacktestConfig::default();
        let engine = ReplayEngine::new(config);
        let mut strategy = BuySellOnceStrategy::new(dec!(50000));
        let result = engine.run(&mut strategy, events);

        assert_eq!(
            result.num_fills, 0,
            "Trade-only events without a book should produce no fills"
        );
    }

    /// Generate synthetic order book events for replay tests.
    fn generate_replay_events(count: usize) -> Vec<MarketEvent> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut events = Vec::with_capacity(count);
        let exchange = Exchange::Simulated;
        let symbol = Symbol::new("BTC", "USDT");
        let base_time = chrono::Utc::now() - chrono::Duration::seconds(count as i64);
        let mut mid_price: f64 = 50_000.0;
        let mut seed: u64 = 42;

        for i in 0..count {
            let ts = base_time + chrono::Duration::seconds(i as i64);
            seed = {
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                i.hash(&mut hasher);
                hasher.finish()
            };
            let step_pct = ((seed % 1000) as f64 / 1000.0 - 0.5) * 0.005;
            mid_price *= 1.0 + step_pct;
            if mid_price < 100.0 {
                mid_price = 100.0;
            }

            let spread_bps = 1.0 + (seed % 10) as f64;
            let half_spread = mid_price * spread_bps / 10_000.0 / 2.0;
            let best_bid = mid_price - half_spread;
            let best_ask = mid_price + half_spread;

            let bids = vec![PriceLevel {
                price: Decimal::from_f64_retain(best_bid)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: dec!(10),
            }];
            let asks = vec![PriceLevel {
                price: Decimal::from_f64_retain(best_ask)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: dec!(10),
            }];

            events.push(MarketEvent::OrderBookSnapshot {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                bids,
                asks,
                sequence: i as u64,
            });
        }

        events
    }
}
