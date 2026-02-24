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
    pub fn run(
        &self,
        strategy: &mut dyn Strategy,
        mut events: Vec<MarketEvent>,
    ) -> BacktestResult {
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

            // Build the market snapshot
            let snapshot = MarketSnapshot {
                exchange,
                symbol: symbol.clone(),
                order_book: book.clone(),
                last_trade: last_trade.clone(),
                position: position.clone().unwrap(),
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
        assert!(result.num_fills >= 2, "Expected at least 2 fills, got {}", result.num_fills);
        assert!(result.total_pnl > Decimal::ZERO, "Expected positive PnL, got {}", result.total_pnl);
        assert!(result.total_fees > Decimal::ZERO, "Expected non-zero fees");
    }

    #[test]
    fn test_replay_engine_empty_events() {
        let config = BacktestConfig::default();
        let engine = ReplayEngine::new(config);

        /// A do-nothing strategy for testing.
        struct NoOpStrategy;
        impl Strategy for NoOpStrategy {
            fn name(&self) -> &str { "noop" }
            fn strategy_id(&self) -> &str { "noop" }
            fn on_tick(&mut self, _: &MarketSnapshot) -> Vec<OrderAction> { vec![] }
            fn on_fill(&mut self, _: &Fill) {}
            fn on_order_update(&mut self, _: &OrderUpdate) {}
            fn is_active(&self) -> bool { true }
            fn stop(&mut self) -> Vec<OrderAction> { vec![] }
        }

        let mut strategy = NoOpStrategy;
        let result = engine.run(&mut strategy, vec![]);

        assert_eq!(result.num_fills, 0);
        assert_eq!(result.num_trades, 0);
        assert_eq!(result.total_pnl, Decimal::ZERO);
    }
}
