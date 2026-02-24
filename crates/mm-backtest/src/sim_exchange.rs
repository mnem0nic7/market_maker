use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

use mm_core::{
    Exchange, Fill, MmError, MmResult, NewOrder, Order, OrderBook, OrderStatus, OrderType, Side,
};

// ---------------------------------------------------------------------------
// Fee & Slippage Models
// ---------------------------------------------------------------------------

/// Configurable fee model applied to every fill.
#[derive(Debug, Clone)]
pub struct FeeModel {
    /// Fee rate for maker (limit) orders, e.g. 0.001 = 10 bps.
    pub maker_fee_rate: Decimal,
    /// Fee rate for taker (market / crossing) orders.
    pub taker_fee_rate: Decimal,
}

impl Default for FeeModel {
    fn default() -> Self {
        Self {
            maker_fee_rate: dec!(0.001),
            taker_fee_rate: dec!(0.001),
        }
    }
}

/// Configurable slippage model applied to fill prices.
#[derive(Debug, Clone)]
pub struct SlippageModel {
    /// Slippage in basis points (1 bps = 0.01%).
    pub slippage_bps: Decimal,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self {
            slippage_bps: Decimal::ZERO,
        }
    }
}

impl SlippageModel {
    /// Apply slippage to a price. Buy orders pay more, sell orders receive less.
    pub fn apply(&self, price: Decimal, side: Side) -> Decimal {
        if self.slippage_bps == Decimal::ZERO {
            return price;
        }
        let factor = self.slippage_bps / dec!(10_000);
        match side {
            Side::Buy => price * (Decimal::ONE + factor),
            Side::Sell => price * (Decimal::ONE - factor),
        }
    }
}

// ---------------------------------------------------------------------------
// Balance Tracker
// ---------------------------------------------------------------------------

/// Tracks the quote-currency balance for the simulated account.
#[derive(Debug, Clone)]
pub struct BalanceTracker {
    pub initial_capital: Decimal,
    pub available: Decimal,
    pub locked: Decimal,
}

impl BalanceTracker {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            initial_capital,
            available: initial_capital,
            locked: Decimal::ZERO,
        }
    }

    pub fn total(&self) -> Decimal {
        self.available + self.locked
    }

    /// Lock funds when a buy limit order is placed.
    pub fn lock(&mut self, amount: Decimal) -> MmResult<()> {
        if amount > self.available {
            return Err(MmError::InsufficientBalance {
                required: amount.to_string(),
                available: self.available.to_string(),
                currency: "QUOTE".to_string(),
            });
        }
        self.available -= amount;
        self.locked += amount;
        Ok(())
    }

    /// Unlock funds when an order is cancelled.
    pub fn unlock(&mut self, amount: Decimal) {
        let to_unlock = amount.min(self.locked);
        self.locked -= to_unlock;
        self.available += to_unlock;
    }

    /// Settle a fill: release the locked amount and credit/debit the actual cost.
    /// `locked_amount` is what was originally reserved.
    /// `actual_cost` is the actual price * quantity + fees (positive = cost, negative = proceeds).
    pub fn settle_fill(&mut self, locked_amount: Decimal, actual_cost: Decimal) {
        // Release the lock
        let to_unlock = locked_amount.min(self.locked);
        self.locked -= to_unlock;
        // Credit back the locked amount, then debit the actual cost
        self.available += to_unlock - actual_cost;
    }
}

// ---------------------------------------------------------------------------
// Simulated Exchange
// ---------------------------------------------------------------------------

/// A simulated matching engine for backtesting.
///
/// Maintains open orders and matches them against the current order book.
/// Supports limit and market orders with configurable fees and slippage.
#[derive(Debug)]
pub struct SimulatedExchange {
    /// Open orders indexed by client_order_id.
    open_orders: HashMap<String, Order>,
    /// Amount of quote currency locked per order (for buy limit orders).
    locked_amounts: HashMap<String, Decimal>,
    /// Fee model for fills.
    fee_model: FeeModel,
    /// Slippage model for fill prices.
    slippage_model: SlippageModel,
    /// Balance tracker.
    balance: BalanceTracker,
    /// Current simulated time (set by the replay engine).
    current_time: DateTime<Utc>,
    /// Monotonic sequence number for exchange order IDs.
    next_order_seq: u64,
    /// Pending fills generated during submit_order (for market orders).
    pending_fills: Vec<Fill>,
}

impl SimulatedExchange {
    pub fn new(
        initial_capital: Decimal,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
    ) -> Self {
        Self {
            open_orders: HashMap::new(),
            locked_amounts: HashMap::new(),
            fee_model,
            slippage_model,
            balance: BalanceTracker::new(initial_capital),
            current_time: Utc::now(),
            next_order_seq: 1,
            pending_fills: Vec::new(),
        }
    }

    /// Update the simulated clock. Called by the replay engine before processing events.
    pub fn set_time(&mut self, time: DateTime<Utc>) {
        self.current_time = time;
    }

    /// Get a reference to the current balance tracker.
    pub fn balance(&self) -> &BalanceTracker {
        &self.balance
    }

    /// Get a snapshot of all currently open orders.
    pub fn open_orders(&self) -> Vec<Order> {
        self.open_orders.values().cloned().collect()
    }

    /// Get a reference to the open orders map.
    pub fn open_orders_map(&self) -> &HashMap<String, Order> {
        &self.open_orders
    }

    // -----------------------------------------------------------------------
    // Order Submission
    // -----------------------------------------------------------------------

    /// Submit a new order. Market orders are filled immediately if possible.
    /// Limit orders are placed in the book and will be checked on subsequent ticks.
    pub fn submit_order(
        &mut self,
        new_order: &NewOrder,
        current_book: &OrderBook,
    ) -> MmResult<Order> {
        let exchange_order_id = format!("SIM-{}", self.next_order_seq);
        self.next_order_seq += 1;

        let mut order = Order {
            client_order_id: Uuid::new_v4().to_string(),
            exchange_order_id: Some(exchange_order_id),
            exchange: Exchange::Simulated,
            symbol: new_order.symbol.clone(),
            side: new_order.side,
            order_type: new_order.order_type,
            time_in_force: new_order.time_in_force,
            price: new_order.price,
            quantity: new_order.quantity,
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: new_order.quantity,
            average_fill_price: Decimal::ZERO,
            strategy_id: new_order.strategy_id.clone(),
            reduce_only: new_order.reduce_only,
            created_at: self.current_time,
            updated_at: self.current_time,
        };

        match new_order.order_type {
            OrderType::Market => {
                self.try_fill_market_order(&mut order, current_book)?;
            }
            OrderType::Limit => {
                // Lock funds for buy limit orders
                if new_order.side == Side::Buy {
                    if let Some(price) = new_order.price {
                        let lock_amount = price * new_order.quantity;
                        self.balance.lock(lock_amount)?;
                        self.locked_amounts
                            .insert(order.client_order_id.clone(), lock_amount);
                    }
                }

                // Check if the limit order can be immediately matched (crossing the book)
                if self.try_fill_limit_order_immediately(&mut order, current_book) {
                    // Order was fully filled
                    tracing::debug!(
                        client_order_id = %order.client_order_id,
                        "Limit order immediately filled (crossed the book)"
                    );
                } else if order.status != OrderStatus::Filled {
                    // Place on the book as resting order
                    tracing::debug!(
                        client_order_id = %order.client_order_id,
                        side = %order.side,
                        price = %order.price.unwrap_or_default(),
                        qty = %order.remaining_quantity,
                        "Limit order placed on simulated book"
                    );
                    self.open_orders
                        .insert(order.client_order_id.clone(), order.clone());
                }
            }
            _ => {
                // StopLimit and StopMarket are not yet supported in the sim
                tracing::warn!(
                    order_type = %new_order.order_type,
                    "Unsupported order type in simulated exchange, rejecting"
                );
                order.status = OrderStatus::Rejected;
                order.updated_at = self.current_time;
            }
        }

        Ok(order)
    }

    // -----------------------------------------------------------------------
    // Market Order Matching
    // -----------------------------------------------------------------------

    fn try_fill_market_order(
        &mut self,
        order: &mut Order,
        book: &OrderBook,
    ) -> MmResult<()> {
        let fill_price = match order.side {
            Side::Buy => book.best_ask().map(|l| l.price),
            Side::Sell => book.best_bid().map(|l| l.price),
        };

        match fill_price {
            Some(price) => {
                let slipped_price = self.slippage_model.apply(price, order.side);
                let fee_rate = self.fee_model.taker_fee_rate;
                let fee = slipped_price * order.quantity * fee_rate;

                // For market buy orders, debit cost from available balance
                if order.side == Side::Buy {
                    let total_cost = slipped_price * order.quantity + fee;
                    if total_cost > self.balance.available {
                        order.status = OrderStatus::Rejected;
                        order.updated_at = self.current_time;
                        return Err(MmError::InsufficientBalance {
                            required: total_cost.to_string(),
                            available: self.balance.available.to_string(),
                            currency: "QUOTE".to_string(),
                        });
                    }
                    self.balance.available -= total_cost;
                } else {
                    // Sell: credit proceeds minus fees
                    let proceeds = slipped_price * order.quantity - fee;
                    self.balance.available += proceeds;
                }

                order.filled_quantity = order.quantity;
                order.remaining_quantity = Decimal::ZERO;
                order.average_fill_price = slipped_price;
                order.status = OrderStatus::Filled;
                order.updated_at = self.current_time;

                let fill = Fill {
                    fill_id: Uuid::new_v4().to_string(),
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id: order
                        .exchange_order_id
                        .clone()
                        .unwrap_or_default(),
                    exchange: Exchange::Simulated,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    price: slipped_price,
                    quantity: order.quantity,
                    fee,
                    fee_currency: order.symbol.quote.clone(),
                    timestamp: self.current_time,
                };

                tracing::debug!(
                    client_order_id = %order.client_order_id,
                    side = %order.side,
                    price = %slipped_price,
                    qty = %order.quantity,
                    fee = %fee,
                    "Market order filled"
                );

                self.pending_fills.push(fill);
                Ok(())
            }
            None => {
                // No liquidity on the opposite side
                order.status = OrderStatus::Rejected;
                order.updated_at = self.current_time;
                Err(MmError::OrderRejected {
                    reason: "No liquidity available for market order".to_string(),
                })
            }
        }
    }

    // -----------------------------------------------------------------------
    // Immediate Limit Fill (crossing the spread)
    // -----------------------------------------------------------------------

    /// Attempt to immediately fill a limit order that crosses the spread.
    /// Returns true if the order was fully filled.
    fn try_fill_limit_order_immediately(
        &mut self,
        order: &mut Order,
        book: &OrderBook,
    ) -> bool {
        let can_fill = match order.side {
            Side::Buy => {
                if let (Some(order_price), Some(best_ask)) = (order.price, book.best_ask()) {
                    order_price >= best_ask.price
                } else {
                    false
                }
            }
            Side::Sell => {
                if let (Some(order_price), Some(best_bid)) = (order.price, book.best_bid()) {
                    order_price <= best_bid.price
                } else {
                    false
                }
            }
        };

        if !can_fill {
            return false;
        }

        let fill_price = match order.side {
            Side::Buy => book.best_ask().unwrap().price,
            Side::Sell => book.best_bid().unwrap().price,
        };

        let slipped_price = self.slippage_model.apply(fill_price, order.side);
        // Crossing limit orders are taker fills
        let fee_rate = self.fee_model.taker_fee_rate;
        let fee = slipped_price * order.quantity * fee_rate;

        // Settle the balance
        if order.side == Side::Buy {
            let locked = self
                .locked_amounts
                .remove(&order.client_order_id)
                .unwrap_or_default();
            let actual_cost = slipped_price * order.quantity + fee;
            self.balance.settle_fill(locked, actual_cost);
        } else {
            let proceeds = slipped_price * order.quantity - fee;
            self.balance.available += proceeds;
        }

        order.filled_quantity = order.quantity;
        order.remaining_quantity = Decimal::ZERO;
        order.average_fill_price = slipped_price;
        order.status = OrderStatus::Filled;
        order.updated_at = self.current_time;

        let fill = Fill {
            fill_id: Uuid::new_v4().to_string(),
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone().unwrap_or_default(),
            exchange: Exchange::Simulated,
            symbol: order.symbol.clone(),
            side: order.side,
            price: slipped_price,
            quantity: order.quantity,
            fee,
            fee_currency: order.symbol.quote.clone(),
            timestamp: self.current_time,
        };

        self.pending_fills.push(fill);
        true
    }

    // -----------------------------------------------------------------------
    // Resting Limit Order Matching (called each tick)
    // -----------------------------------------------------------------------

    /// Check all open limit orders against the current order book.
    /// Returns fills for any orders that can be matched.
    ///
    /// - Buy limit: fills if order price >= best ask
    /// - Sell limit: fills if order price <= best bid
    pub fn check_fills(&mut self, current_book: &OrderBook) -> Vec<Fill> {
        // Drain any pending fills from submit_order (market orders, immediate limit fills)
        let mut fills: Vec<Fill> = self.pending_fills.drain(..).collect();

        let mut filled_ids = Vec::new();

        for (client_order_id, order) in self.open_orders.iter() {
            if order.status.is_terminal() {
                filled_ids.push(client_order_id.clone());
                continue;
            }

            let should_fill = match order.side {
                Side::Buy => {
                    if let (Some(order_price), Some(best_ask)) =
                        (order.price, current_book.best_ask())
                    {
                        order_price >= best_ask.price
                    } else {
                        false
                    }
                }
                Side::Sell => {
                    if let (Some(order_price), Some(best_bid)) =
                        (order.price, current_book.best_bid())
                    {
                        order_price <= best_bid.price
                    } else {
                        false
                    }
                }
            };

            if should_fill {
                filled_ids.push(client_order_id.clone());
            }
        }

        for client_order_id in &filled_ids {
            if let Some(mut order) = self.open_orders.remove(client_order_id) {
                if order.status.is_terminal() {
                    continue;
                }

                // Determine fill price from the book
                let book_price = match order.side {
                    Side::Buy => current_book.best_ask().map(|l| l.price),
                    Side::Sell => current_book.best_bid().map(|l| l.price),
                };

                if let Some(price) = book_price {
                    let slipped_price = self.slippage_model.apply(price, order.side);
                    // Resting limit orders are maker fills
                    let fee_rate = self.fee_model.maker_fee_rate;
                    let fee = slipped_price * order.remaining_quantity * fee_rate;

                    // Settle balance
                    if order.side == Side::Buy {
                        let locked = self
                            .locked_amounts
                            .remove(&order.client_order_id)
                            .unwrap_or_default();
                        let actual_cost = slipped_price * order.remaining_quantity + fee;
                        self.balance.settle_fill(locked, actual_cost);
                    } else {
                        let proceeds = slipped_price * order.remaining_quantity - fee;
                        self.balance.available += proceeds;
                    }

                    let fill_qty = order.remaining_quantity;
                    order.filled_quantity += fill_qty;
                    order.remaining_quantity = Decimal::ZERO;
                    order.average_fill_price = slipped_price;
                    order.status = OrderStatus::Filled;
                    order.updated_at = self.current_time;

                    let fill = Fill {
                        fill_id: Uuid::new_v4().to_string(),
                        client_order_id: order.client_order_id.clone(),
                        exchange_order_id: order
                            .exchange_order_id
                            .clone()
                            .unwrap_or_default(),
                        exchange: Exchange::Simulated,
                        symbol: order.symbol.clone(),
                        side: order.side,
                        price: slipped_price,
                        quantity: fill_qty,
                        fee,
                        fee_currency: order.symbol.quote.clone(),
                        timestamp: self.current_time,
                    };

                    tracing::debug!(
                        client_order_id = %order.client_order_id,
                        side = %order.side,
                        price = %slipped_price,
                        qty = %fill_qty,
                        fee = %fee,
                        "Resting limit order filled"
                    );

                    fills.push(fill);
                }
            }
        }

        fills
    }

    // -----------------------------------------------------------------------
    // Cancel
    // -----------------------------------------------------------------------

    /// Cancel a single open order by client_order_id.
    pub fn cancel_order(&mut self, client_order_id: &str) -> MmResult<()> {
        match self.open_orders.remove(client_order_id) {
            Some(order) => {
                // Unlock reserved funds
                if order.side == Side::Buy {
                    if let Some(locked) = self.locked_amounts.remove(client_order_id) {
                        self.balance.unlock(locked);
                    }
                }
                tracing::debug!(
                    client_order_id = %client_order_id,
                    "Order cancelled"
                );
                Ok(())
            }
            None => Err(MmError::OrderNotFound {
                order_id: client_order_id.to_string(),
            }),
        }
    }

    /// Cancel all open orders. Returns the number of orders cancelled.
    pub fn cancel_all(&mut self) -> u32 {
        let count = self.open_orders.len() as u32;

        // Unlock all reserved funds
        for (id, order) in self.open_orders.drain() {
            if order.side == Side::Buy {
                if let Some(locked) = self.locked_amounts.remove(&id) {
                    self.balance.unlock(locked);
                }
            }
        }

        tracing::debug!(count = count, "All orders cancelled");
        count
    }

    /// Drain any pending fills (from market orders / immediate limit fills).
    /// This is used by the replay engine to collect fills that occurred during submit_order.
    pub fn drain_pending_fills(&mut self) -> Vec<Fill> {
        self.pending_fills.drain(..).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{OrderType, PriceLevel, Symbol, TimeInForce};
    use rust_decimal_macros::dec;

    fn test_book() -> OrderBook {
        OrderBook {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            bids: vec![
                PriceLevel {
                    price: dec!(50000),
                    quantity: dec!(1),
                },
                PriceLevel {
                    price: dec!(49990),
                    quantity: dec!(2),
                },
            ],
            asks: vec![
                PriceLevel {
                    price: dec!(50010),
                    quantity: dec!(1),
                },
                PriceLevel {
                    price: dec!(50020),
                    quantity: dec!(2),
                },
            ],
            timestamp: Utc::now(),
            sequence: 1,
        }
    }

    fn test_exchange() -> SimulatedExchange {
        SimulatedExchange::new(
            dec!(1_000_000),
            FeeModel {
                maker_fee_rate: dec!(0.0001),
                taker_fee_rate: dec!(0.0005),
            },
            SlippageModel {
                slippage_bps: Decimal::ZERO,
            },
        )
    }

    #[test]
    fn test_market_buy_fill() {
        let mut exchange = test_exchange();
        let book = test_book();

        let new_order = NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            price: None,
            quantity: dec!(0.5),
            strategy_id: "test".to_string(),
            reduce_only: false,
        };

        let order = exchange.submit_order(&new_order, &book).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.average_fill_price, dec!(50010)); // best ask
        assert_eq!(order.filled_quantity, dec!(0.5));

        let fills = exchange.check_fills(&book);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(50010));
    }

    #[test]
    fn test_market_sell_fill() {
        let mut exchange = test_exchange();
        let book = test_book();

        let new_order = NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            price: None,
            quantity: dec!(0.5),
            strategy_id: "test".to_string(),
            reduce_only: false,
        };

        let order = exchange.submit_order(&new_order, &book).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.average_fill_price, dec!(50000)); // best bid
    }

    #[test]
    fn test_limit_order_resting_then_fill() {
        let mut exchange = test_exchange();
        let book = test_book();

        // Place a buy limit below best ask (resting)
        let new_order = NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(49995)),
            quantity: dec!(1),
            strategy_id: "test".to_string(),
            reduce_only: false,
        };

        let order = exchange.submit_order(&new_order, &book).unwrap();
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(exchange.open_orders().len(), 1);

        // No fills yet with current book
        let fills = exchange.check_fills(&book);
        assert!(fills.is_empty());

        // Now move the ask down to match
        let new_book = OrderBook {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            bids: vec![PriceLevel {
                price: dec!(49990),
                quantity: dec!(1),
            }],
            asks: vec![PriceLevel {
                price: dec!(49995),
                quantity: dec!(2),
            }],
            timestamp: Utc::now(),
            sequence: 2,
        };

        let fills = exchange.check_fills(&new_book);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].side, Side::Buy);
        assert_eq!(exchange.open_orders().len(), 0);
    }

    #[test]
    fn test_limit_order_crossing_immediate_fill() {
        let mut exchange = test_exchange();
        let book = test_book();

        // Buy limit at or above best ask -> should immediately fill
        let new_order = NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50015)),
            quantity: dec!(0.5),
            strategy_id: "test".to_string(),
            reduce_only: false,
        };

        let order = exchange.submit_order(&new_order, &book).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        // Filled at best ask, not the limit price
        assert_eq!(order.average_fill_price, dec!(50010));
        assert_eq!(exchange.open_orders().len(), 0);
    }

    #[test]
    fn test_cancel_order() {
        let mut exchange = test_exchange();
        let book = test_book();

        let new_order = NewOrder {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(49000)),
            quantity: dec!(1),
            strategy_id: "test".to_string(),
            reduce_only: false,
        };

        let order = exchange.submit_order(&new_order, &book).unwrap();
        let coid = order.client_order_id.clone();
        assert_eq!(exchange.open_orders().len(), 1);

        exchange.cancel_order(&coid).unwrap();
        assert_eq!(exchange.open_orders().len(), 0);
    }

    #[test]
    fn test_cancel_all() {
        let mut exchange = test_exchange();
        let book = test_book();

        for price in [dec!(49000), dec!(49100), dec!(49200)] {
            let new_order = NewOrder {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                side: Side::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                price: Some(price),
                quantity: dec!(1),
                strategy_id: "test".to_string(),
                reduce_only: false,
            };
            exchange.submit_order(&new_order, &book).unwrap();
        }

        assert_eq!(exchange.open_orders().len(), 3);
        let cancelled = exchange.cancel_all();
        assert_eq!(cancelled, 3);
        assert_eq!(exchange.open_orders().len(), 0);
    }

    #[test]
    fn test_slippage_applied() {
        let slippage = SlippageModel {
            slippage_bps: dec!(10), // 10 bps = 0.1%
        };

        let buy_price = slippage.apply(dec!(50000), Side::Buy);
        assert!(buy_price > dec!(50000));

        let sell_price = slippage.apply(dec!(50000), Side::Sell);
        assert!(sell_price < dec!(50000));
    }
}
