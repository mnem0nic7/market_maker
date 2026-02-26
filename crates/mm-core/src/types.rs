use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Exchange & Symbol
// ---------------------------------------------------------------------------

/// Supported exchanges and venues.
///
/// Covers centralized exchanges (Binance, Coinbase, Kraken), decentralized
/// protocols (Uniswap, Raydium), equity brokers (Interactive Brokers, Alpaca),
/// and a simulated exchange for backtesting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum Exchange {
    Binance,
    Coinbase,
    Kraken,
    Uniswap,
    Raydium,
    InteractiveBrokers,
    Alpaca,
    Simulated,
}

/// Canonical symbol representation: "BTC/USDT", "ETH/USD", etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
}

impl Symbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into().to_uppercase(),
            quote: quote.into().to_uppercase(),
        }
    }

    pub fn canonical(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

// ---------------------------------------------------------------------------
// Side, OrderType, TimeInForce
// ---------------------------------------------------------------------------

/// Trade direction: buy or sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum Side {
    /// Buy / go long.
    Buy,
    /// Sell / go short.
    Sell,
}

impl Side {
    pub fn opposite(&self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Order execution type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum OrderType {
    /// Execute immediately at the best available price.
    Market,
    /// Execute at the specified price or better.
    Limit,
    /// Limit order activated when the stop price is reached.
    StopLimit,
    /// Market order activated when the stop price is reached.
    StopMarket,
}

/// Time-in-force policy controlling how long an order remains active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum TimeInForce {
    /// Good till cancelled
    GTC,
    /// Immediate or cancel
    IOC,
    /// Fill or kill
    FOK,
    /// Good till date
    GTD,
}

// ---------------------------------------------------------------------------
// Order Status (lifecycle state machine)
// ---------------------------------------------------------------------------

/// Order lifecycle state machine.
///
/// Transitions: Pending -> Submitted -> New -> PartiallyFilled -> Filled.
/// An order may also move to PendingCancel -> Cancelled, or be Rejected/Expired
/// at any non-terminal state. Terminal states: Filled, Cancelled, Rejected, Expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum OrderStatus {
    /// Created locally, not yet sent to exchange
    Pending,
    /// Sent to exchange, awaiting acknowledgment
    Submitted,
    /// Acknowledged by exchange
    New,
    /// Partially filled
    PartiallyFilled,
    /// Fully filled (terminal)
    Filled,
    /// Cancel request sent
    PendingCancel,
    /// Cancelled (terminal)
    Cancelled,
    /// Rejected by exchange (terminal)
    Rejected,
    /// Expired (terminal)
    Expired,
}

impl OrderStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Cancelled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self,
            OrderStatus::New | OrderStatus::PartiallyFilled | OrderStatus::Submitted
        )
    }
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

/// A trading order with full lifecycle tracking.
///
/// Each order is identified by a locally generated `client_order_id` and, once
/// acknowledged by the exchange, also by an `exchange_order_id`. The
/// [`OrderManager`](crate::types::OrderStatus) state machine governs valid status transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Locally generated UUID identifying this order.
    pub client_order_id: String,
    /// Exchange-assigned order ID, populated after acknowledgment.
    pub exchange_order_id: Option<String>,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    /// Limit price. `None` for market orders.
    pub price: Option<Decimal>,
    /// Requested order quantity.
    pub quantity: Decimal,
    /// Current lifecycle status.
    pub status: OrderStatus,
    /// Cumulative quantity filled so far.
    pub filled_quantity: Decimal,
    /// Quantity still outstanding (`quantity - filled_quantity`).
    pub remaining_quantity: Decimal,
    /// Volume-weighted average fill price across all partial fills.
    pub average_fill_price: Decimal,
    /// ID of the strategy that placed this order.
    pub strategy_id: String,
    /// If true, this order may only reduce an existing position.
    pub reduce_only: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    pub fn new_limit(
        exchange: Exchange,
        symbol: Symbol,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        strategy_id: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            client_order_id: Uuid::new_v4().to_string(),
            exchange_order_id: None,
            exchange,
            symbol,
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(price),
            quantity,
            status: OrderStatus::Pending,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: quantity,
            average_fill_price: Decimal::ZERO,
            strategy_id: strategy_id.into(),
            reduce_only: false,
            created_at: now,
            updated_at: now,
        }
    }
}

// ---------------------------------------------------------------------------
// NewOrder (request to place an order)
// ---------------------------------------------------------------------------

/// Request to place a new order on an exchange.
///
/// Passed through risk checks before being submitted via an [`ExecutionConnector`](crate::traits::ExecutionConnector).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    /// Limit price. `None` for market orders.
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    /// ID of the strategy requesting this order.
    pub strategy_id: String,
    pub reduce_only: bool,
}

// ---------------------------------------------------------------------------
// Cancel request
// ---------------------------------------------------------------------------

/// Request to cancel a specific open order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrder {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
}

/// Request to cancel all open orders, optionally filtered by exchange, symbol, or strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAllOrders {
    pub exchange: Option<Exchange>,
    pub symbol: Option<Symbol>,
    pub strategy_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Fill
// ---------------------------------------------------------------------------

/// Execution report for a completed (or partial) trade.
///
/// Fills are produced by exchange connectors and forwarded to the strategy via
/// [`Strategy::on_fill`](crate::traits::Strategy::on_fill) and to position tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    /// Unique identifier for this fill event.
    pub fill_id: String,
    /// Client-side order ID that produced this fill.
    pub client_order_id: String,
    /// Exchange-assigned order ID.
    pub exchange_order_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub side: Side,
    /// Execution price.
    pub price: Decimal,
    /// Quantity filled in this execution.
    pub quantity: Decimal,
    /// Fee charged for this fill.
    pub fee: Decimal,
    /// Currency in which the fee was charged.
    pub fee_currency: String,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

/// Current holding for a single instrument on one exchange.
///
/// Tracks entry price (volume-weighted average), realized and unrealized P&L.
/// Positive `quantity` = long, negative = short, zero = flat.
/// Updated incrementally via [`Position::update_on_fill`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// Signed quantity: positive for long, negative for short.
    pub quantity: Decimal,
    /// Volume-weighted average entry price across all fills that built this position.
    pub average_entry_price: Decimal,
    /// Mark-to-market unrealized P&L based on latest price.
    pub unrealized_pnl: Decimal,
    /// Cumulative realized P&L from closed portions of this position.
    pub realized_pnl: Decimal,
    pub updated_at: DateTime<Utc>,
}

impl Position {
    pub fn new(exchange: Exchange, symbol: Symbol) -> Self {
        Self {
            exchange,
            symbol,
            quantity: Decimal::ZERO,
            average_entry_price: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        }
    }

    pub fn is_long(&self) -> bool {
        self.quantity > Decimal::ZERO
    }

    pub fn is_short(&self) -> bool {
        self.quantity < Decimal::ZERO
    }

    pub fn is_flat(&self) -> bool {
        self.quantity == Decimal::ZERO
    }

    pub fn notional(&self, current_price: Decimal) -> Decimal {
        self.quantity.abs() * current_price
    }

    pub fn update_on_fill(&mut self, fill: &Fill) {
        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let new_quantity = self.quantity + signed_qty;

        // If adding to position, update average entry
        if (self.quantity >= Decimal::ZERO && signed_qty > Decimal::ZERO)
            || (self.quantity <= Decimal::ZERO && signed_qty < Decimal::ZERO)
        {
            let total_cost =
                self.average_entry_price * self.quantity.abs() + fill.price * fill.quantity;
            let total_qty = self.quantity.abs() + fill.quantity;
            if total_qty != Decimal::ZERO {
                self.average_entry_price = total_cost / total_qty;
            }
        } else {
            // Reducing position — realize PnL
            let pnl_per_unit = match fill.side {
                Side::Sell => fill.price - self.average_entry_price,
                Side::Buy => self.average_entry_price - fill.price,
            };
            let realized = pnl_per_unit * fill.quantity.min(self.quantity.abs());
            self.realized_pnl += realized;

            // If flipping sides, reset avg entry to fill price
            if (self.quantity > Decimal::ZERO && new_quantity < Decimal::ZERO)
                || (self.quantity < Decimal::ZERO && new_quantity > Decimal::ZERO)
            {
                self.average_entry_price = fill.price;
            }
        }

        self.quantity = new_quantity;
        self.updated_at = Utc::now();
    }
}

// ---------------------------------------------------------------------------
// Market Data Types
// ---------------------------------------------------------------------------

/// A single price level in an order book (price + aggregate quantity).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    /// Total quantity available at this price level.
    pub quantity: Decimal,
}

/// Snapshot of an order book for a single instrument.
///
/// Bids are sorted highest-first; asks are sorted lowest-first.
/// Provides convenience methods for best bid/ask, mid price, and spread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// Bid levels sorted by price descending (best bid first).
    pub bids: Vec<PriceLevel>,
    /// Ask levels sorted by price ascending (best ask first).
    pub asks: Vec<PriceLevel>,
    pub timestamp: DateTime<Utc>,
    /// Exchange-provided sequence number for detecting gaps.
    pub sequence: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::from(2)),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > Decimal::ZERO => {
                Some(spread / mid * Decimal::from(10_000))
            }
            _ => None,
        }
    }
}

/// Real-time ticker data with best bid/ask and 24-hour volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub bid: Decimal,
    pub ask: Decimal,
    pub last_price: Decimal,
    pub volume_24h: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// A single executed trade observed on the public trade feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: Side,
    pub timestamp: DateTime<Utc>,
    pub trade_id: String,
}

/// OHLCV candlestick bar for a given time interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub timestamp: DateTime<Utc>,
    pub interval: CandleInterval,
}

/// Supported candlestick time intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum CandleInterval {
    #[strum(serialize = "1m")]
    OneMinute,
    #[strum(serialize = "5m")]
    FiveMinutes,
    #[strum(serialize = "15m")]
    FifteenMinutes,
    #[strum(serialize = "1h")]
    OneHour,
    #[strum(serialize = "4h")]
    FourHours,
    #[strum(serialize = "1d")]
    OneDay,
}

// ---------------------------------------------------------------------------
// Market Snapshot (aggregated view for strategies)
// ---------------------------------------------------------------------------

/// Point-in-time market state delivered to strategies on each tick.
///
/// Aggregates the current order book, last trade, the strategy's position, and
/// its open orders into a single read-only view. Strategies receive this via
/// [`Strategy::on_tick`](crate::traits::Strategy::on_tick).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// Wrapped in `Arc` to avoid deep-cloning the order book on every strategy
    /// tick in the hot path. Strategies only read the book via `&MarketSnapshot`,
    /// so shared ownership is safe and eliminates per-tick Vec<PriceLevel> copies.
    pub order_book: std::sync::Arc<OrderBook>,
    /// Most recent public trade, if available.
    pub last_trade: Option<Trade>,
    /// Strategy's current position on this instrument.
    pub position: Position,
    /// Strategy's currently open orders on this instrument.
    pub open_orders: Vec<Order>,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Strategy Actions (output from strategies)
// ---------------------------------------------------------------------------

/// Actions a strategy can emit in response to a market tick.
///
/// Returned from [`Strategy::on_tick`](crate::traits::Strategy::on_tick) and
/// routed through risk checks before reaching the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderAction {
    /// Place a new order.
    Place(NewOrder),
    /// Cancel a specific order.
    Cancel(CancelOrder),
    /// Cancel all matching orders.
    CancelAll(CancelAllOrders),
    /// Amend an existing order's price and/or quantity.
    Amend {
        client_order_id: String,
        new_price: Option<Decimal>,
        new_quantity: Option<Decimal>,
    },
}

// ---------------------------------------------------------------------------
// Risk
// ---------------------------------------------------------------------------

/// Aggregate portfolio-level risk metrics used by [`RiskCheck`](crate::traits::RiskCheck) evaluations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioRisk {
    /// Value-at-Risk at 95% confidence level.
    pub total_var_95: Decimal,
    /// Value-at-Risk at 99% confidence level.
    pub total_var_99: Decimal,
    /// Net directional exposure (longs minus shorts).
    pub net_exposure: Decimal,
    /// Gross exposure (sum of absolute position values).
    pub gross_exposure: Decimal,
    /// Peak-to-trough drawdown since inception or last reset.
    pub max_drawdown: Decimal,
    /// Running daily P&L.
    pub daily_pnl: Decimal,
    /// All open positions in the portfolio.
    pub positions: Vec<Position>,
    pub timestamp: DateTime<Utc>,
}

/// Outcome of a risk check evaluation on a proposed order or portfolio state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskAction {
    /// Order passes all checks and may proceed.
    Allow,
    /// Order is rejected outright.
    Reject { reason: String },
    /// Order is allowed but size must be reduced to `max_quantity`.
    ReduceSize {
        max_quantity: Decimal,
        reason: String,
    },
    /// Severe risk event: activate the kill switch and cancel all activity.
    KillSwitch { reason: String },
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for instantiating a strategy at runtime.
///
/// The `strategy_type` field selects the concrete strategy implementation
/// (e.g. "spread", "avellaneda", "grid"), and `params` carries the
/// strategy-specific configuration as a JSON value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Strategy type key (e.g. "spread", "avellaneda", "grid", "twap").
    pub strategy_type: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// Whether this strategy should be active on startup.
    pub enabled: bool,
    /// Strategy-specific parameters, deserialized into the concrete config struct.
    pub params: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Helper to create a Fill with minimal boilerplate.
    fn make_fill(side: Side, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            fill_id: Uuid::new_v4().to_string(),
            client_order_id: "test-order".to_string(),
            exchange_order_id: "exch-order".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            timestamp: Utc::now(),
        }
    }

    /// Helper to create a flat position on the simulated exchange.
    fn flat_position() -> Position {
        Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT"))
    }

    // 1. Buy from flat: verify quantity and avg entry price.
    #[test]
    fn test_open_long_position() {
        let mut pos = flat_position();
        let fill = make_fill(Side::Buy, dec!(50000), dec!(1));

        pos.update_on_fill(&fill);

        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.average_entry_price, dec!(50000));
        assert_eq!(pos.realized_pnl, dec!(0));
        assert!(pos.is_long());
        assert!(!pos.is_flat());
        assert!(!pos.is_short());
    }

    // 2. Two buys — verify weighted average entry price.
    #[test]
    fn test_add_to_long_position() {
        let mut pos = flat_position();

        // Buy 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(1)));
        // Buy 1.0 @ 200
        pos.update_on_fill(&make_fill(Side::Buy, dec!(200), dec!(1)));

        assert_eq!(pos.quantity, dec!(2));
        // Weighted avg = (100*1 + 200*1) / 2 = 150
        assert_eq!(pos.average_entry_price, dec!(150));
        assert_eq!(pos.realized_pnl, dec!(0));
    }

    // 3. Buy then sell same qty — verify realized PnL and position is flat.
    #[test]
    fn test_close_long_position() {
        let mut pos = flat_position();

        // Buy 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(1)));
        // Sell 1.0 @ 110
        pos.update_on_fill(&make_fill(Side::Sell, dec!(110), dec!(1)));

        assert_eq!(pos.quantity, dec!(0));
        assert!(pos.is_flat());
        // PnL = (110 - 100) * 1 = 10
        assert_eq!(pos.realized_pnl, dec!(10));
    }

    // 4. Buy 1.0, sell 0.3 — verify remaining qty, avg entry unchanged, partial PnL.
    #[test]
    fn test_partial_close_long() {
        let mut pos = flat_position();

        // Buy 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(1)));
        // Sell 0.3 @ 120
        pos.update_on_fill(&make_fill(Side::Sell, dec!(120), dec!(0.3)));

        assert_eq!(pos.quantity, dec!(0.7));
        // Avg entry should remain 100 (partial close doesn't change avg entry)
        assert_eq!(pos.average_entry_price, dec!(100));
        // Realized PnL = (120 - 100) * 0.3 = 6
        assert_eq!(pos.realized_pnl, dec!(6));
    }

    // 5. Buy 1.0 @ 100, sell 2.0 @ 110 — flip long to short.
    //    Realized PnL on the closed 1.0 portion, new qty=-1.0, avg entry reset to 110.
    #[test]
    fn test_flip_long_to_short() {
        let mut pos = flat_position();

        // Buy 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(1)));
        // Sell 2.0 @ 110 (closes 1.0 long, opens 1.0 short)
        pos.update_on_fill(&make_fill(Side::Sell, dec!(110), dec!(2)));

        assert_eq!(pos.quantity, dec!(-1));
        assert!(pos.is_short());
        // PnL realized on the closed 1.0: (110 - 100) * 1 = 10
        assert_eq!(pos.realized_pnl, dec!(10));
        // Avg entry resets to the fill price for the new short side
        assert_eq!(pos.average_entry_price, dec!(110));
    }

    // 6. Sell from flat — open a short position.
    #[test]
    fn test_open_short_position() {
        let mut pos = flat_position();
        let fill = make_fill(Side::Sell, dec!(50000), dec!(1));

        pos.update_on_fill(&fill);

        assert_eq!(pos.quantity, dec!(-1));
        assert_eq!(pos.average_entry_price, dec!(50000));
        assert_eq!(pos.realized_pnl, dec!(0));
        assert!(pos.is_short());
        assert!(!pos.is_flat());
        assert!(!pos.is_long());
    }

    // 7. Two sells — verify weighted average entry price.
    #[test]
    fn test_add_to_short_position() {
        let mut pos = flat_position();

        // Sell 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Sell, dec!(100), dec!(1)));
        // Sell 1.0 @ 200
        pos.update_on_fill(&make_fill(Side::Sell, dec!(200), dec!(1)));

        assert_eq!(pos.quantity, dec!(-2));
        // Weighted avg = (100*1 + 200*1) / 2 = 150
        assert_eq!(pos.average_entry_price, dec!(150));
        assert_eq!(pos.realized_pnl, dec!(0));
    }

    // 8. Sell then buy — close short, verify realized PnL.
    #[test]
    fn test_close_short_position() {
        let mut pos = flat_position();

        // Sell 1.0 @ 100 (short entry)
        pos.update_on_fill(&make_fill(Side::Sell, dec!(100), dec!(1)));
        // Buy 1.0 @ 90 (close short at profit)
        pos.update_on_fill(&make_fill(Side::Buy, dec!(90), dec!(1)));

        assert_eq!(pos.quantity, dec!(0));
        assert!(pos.is_flat());
        // Short PnL = (entry - exit) * qty = (100 - 90) * 1 = 10
        assert_eq!(pos.realized_pnl, dec!(10));
    }

    // 9. Sell 1.0 @ 100, buy 2.0 @ 90 — flip short to long.
    //    PnL = 10 on the closed 1.0, qty = 1.0, avg entry = 90.
    #[test]
    fn test_flip_short_to_long() {
        let mut pos = flat_position();

        // Sell 1.0 @ 100
        pos.update_on_fill(&make_fill(Side::Sell, dec!(100), dec!(1)));
        // Buy 2.0 @ 90 (closes 1.0 short, opens 1.0 long)
        pos.update_on_fill(&make_fill(Side::Buy, dec!(90), dec!(2)));

        assert_eq!(pos.quantity, dec!(1));
        assert!(pos.is_long());
        // Short PnL on closed portion: (100 - 90) * 1 = 10
        assert_eq!(pos.realized_pnl, dec!(10));
        // Avg entry resets to fill price for the new long side
        assert_eq!(pos.average_entry_price, dec!(90));
    }

    // 10. Multiple fills — verify PnL accumulates correctly across trades.
    #[test]
    fn test_multiple_fills_pnl_accumulation() {
        let mut pos = flat_position();

        // Trade 1: Buy 2.0 @ 100
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(2)));
        assert_eq!(pos.quantity, dec!(2));
        assert_eq!(pos.average_entry_price, dec!(100));

        // Trade 2: Sell 1.0 @ 110 (partial close, realize +10)
        pos.update_on_fill(&make_fill(Side::Sell, dec!(110), dec!(1)));
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.realized_pnl, dec!(10));
        assert_eq!(pos.average_entry_price, dec!(100));

        // Trade 3: Sell 1.0 @ 120 (close remaining, realize +20)
        pos.update_on_fill(&make_fill(Side::Sell, dec!(120), dec!(1)));
        assert_eq!(pos.quantity, dec!(0));
        assert!(pos.is_flat());
        // Total realized: 10 + 20 = 30
        assert_eq!(pos.realized_pnl, dec!(30));

        // Trade 4: Sell 1.0 @ 200 (new short)
        pos.update_on_fill(&make_fill(Side::Sell, dec!(200), dec!(1)));
        assert_eq!(pos.quantity, dec!(-1));
        assert_eq!(pos.average_entry_price, dec!(200));
        // Realized PnL unchanged
        assert_eq!(pos.realized_pnl, dec!(30));

        // Trade 5: Buy 1.0 @ 180 (close short, realize +20)
        pos.update_on_fill(&make_fill(Side::Buy, dec!(180), dec!(1)));
        assert_eq!(pos.quantity, dec!(0));
        assert!(pos.is_flat());
        // Total realized: 30 + 20 = 50
        assert_eq!(pos.realized_pnl, dec!(50));
    }

    // 11. Edge case: fill with zero quantity should not panic.
    #[test]
    fn test_zero_quantity_fill_no_panic() {
        let mut pos = flat_position();

        // Zero-qty fill from flat
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(0)));
        assert_eq!(pos.quantity, dec!(0));
        assert!(pos.is_flat());

        // Open a position, then apply zero-qty fill
        pos.update_on_fill(&make_fill(Side::Buy, dec!(100), dec!(1)));
        pos.update_on_fill(&make_fill(Side::Sell, dec!(110), dec!(0)));
        assert_eq!(pos.quantity, dec!(1));
        assert_eq!(pos.realized_pnl, dec!(0));
    }
}
