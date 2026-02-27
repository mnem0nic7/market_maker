use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::errors::MmResult;
use crate::events::OrderUpdate;
use crate::types::*;

// ---------------------------------------------------------------------------
// Market Data Connector
// ---------------------------------------------------------------------------

/// Adapter for streaming normalized market data from a specific exchange.
///
/// Lifecycle: call [`connect`](Self::connect) with the desired symbols and `mpsc`
/// channels, then consume `OrderBook` and `Trade` updates. Use
/// [`disconnect`](Self::disconnect) for graceful teardown.
#[async_trait]
pub trait MarketDataConnector: Send + Sync {
    /// Returns the exchange this connector is bound to.
    fn exchange(&self) -> Exchange;

    /// Connect to the exchange and begin streaming data.
    /// Order book updates and trades are sent to the provided channels.
    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()>;

    /// Disconnect from the exchange.
    async fn disconnect(&mut self) -> MmResult<()>;

    /// Fetch a snapshot of the order book (REST fallback).
    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook>;

    /// Check if the connection is alive.
    fn is_connected(&self) -> bool;
}

// ---------------------------------------------------------------------------
// Execution Connector
// ---------------------------------------------------------------------------

/// Adapter for submitting and managing orders on a specific exchange.
///
/// Lifecycle: call [`connect`](Self::connect) to start receiving real-time
/// `OrderUpdate` events, then use [`submit_order`](Self::submit_order),
/// [`cancel_order`](Self::cancel_order), etc. for order management.
#[async_trait]
pub trait ExecutionConnector: Send + Sync {
    /// Returns the exchange this connector is bound to.
    fn exchange(&self) -> Exchange;

    /// Connect and start receiving order updates via the provided channel.
    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()>;

    /// Disconnect from the exchange and stop receiving updates.
    async fn disconnect(&mut self) -> MmResult<()>;

    /// Submit a new order. Returns the client_order_id.
    async fn submit_order(&self, order: &NewOrder) -> MmResult<String>;

    /// Cancel an existing order.
    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()>;

    /// Cancel all open orders, optionally filtered by symbol.
    async fn cancel_all_orders(&self, request: &CancelAllOrders) -> MmResult<u32>;

    /// Fetch current balances.
    async fn fetch_balances(&self) -> MmResult<Vec<Balance>>;

    /// Check if the connection is alive.
    fn is_connected(&self) -> bool;
}

/// Exchange account balance for a single currency.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Balance {
    pub currency: String,
    /// Available (unlocked) balance.
    pub free: Decimal,
    /// Balance locked in open orders.
    pub locked: Decimal,
    /// Total balance (`free + locked`).
    pub total: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

/// Core trading strategy interface.
///
/// All strategies -- live and backtest -- implement this same trait. The main
/// loop calls [`on_tick`](Self::on_tick) with each new [`MarketSnapshot`] and
/// routes the returned [`OrderAction`]s through risk checks to execution.
pub trait Strategy: Send + Sync {
    /// Human-readable strategy name (e.g. "avellaneda_stoikov").
    fn name(&self) -> &str;
    /// Unique identifier for this running strategy instance.
    fn strategy_id(&self) -> &str;

    /// The exchange this strategy is configured to trade on.
    fn exchange(&self) -> Exchange;
    /// The symbol (trading pair) this strategy is configured to trade.
    fn symbol(&self) -> Symbol;

    /// Called on each tick with the current market state.
    /// Returns a list of order actions to execute.
    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction>;

    /// Called when a fill occurs on one of this strategy's orders.
    fn on_fill(&mut self, fill: &Fill);

    /// Called when an order update is received.
    fn on_order_update(&mut self, update: &OrderUpdate);

    /// Whether this strategy is currently active.
    fn is_active(&self) -> bool;

    /// Stop the strategy gracefully. Should cancel outstanding orders.
    fn stop(&mut self) -> Vec<OrderAction>;
}

// ---------------------------------------------------------------------------
// Risk Check
// ---------------------------------------------------------------------------

/// Pre-trade risk check run on every order before it reaches the exchange.
///
/// Risk checks are evaluated sequentially by the `RiskEngine`. The first
/// non-[`Allow`](RiskAction::Allow) result short-circuits the pipeline.
#[async_trait]
pub trait RiskCheck: Send + Sync {
    /// Human-readable name of this risk check (used in rejection reasons and logs).
    fn name(&self) -> &str;

    /// Evaluate whether a proposed order should be allowed, rejected, or resized.
    async fn check_order(&self, order: &NewOrder, portfolio: &PortfolioRisk) -> RiskAction;

    /// Periodic portfolio-level risk evaluation (called independently of order flow).
    async fn check_portfolio(&self, portfolio: &PortfolioRisk) -> RiskAction;
}

// ---------------------------------------------------------------------------
// Event Publisher
// ---------------------------------------------------------------------------

/// Abstraction over event publishing (NATS JetStream in production, in-memory for tests).
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, envelope: crate::events::Envelope) -> MmResult<()>;
}
