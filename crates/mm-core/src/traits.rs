use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::errors::MmResult;
use crate::events::OrderUpdate;
use crate::types::*;

// ---------------------------------------------------------------------------
// Market Data Connector
// ---------------------------------------------------------------------------

/// Trait for exchange-specific market data connections.
/// Each exchange implements this to provide normalized market data.
#[async_trait]
pub trait MarketDataConnector: Send + Sync {
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

/// Trait for exchange-specific order execution.
#[async_trait]
pub trait ExecutionConnector: Send + Sync {
    fn exchange(&self) -> Exchange;

    /// Connect and start receiving order updates via the provided channel.
    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()>;

    async fn disconnect(&mut self) -> MmResult<()>;

    /// Submit a new order. Returns the client_order_id.
    async fn submit_order(&self, order: &NewOrder) -> MmResult<String>;

    /// Cancel an existing order.
    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()>;

    /// Cancel all open orders, optionally filtered by symbol.
    async fn cancel_all_orders(&self, request: &CancelAllOrders) -> MmResult<u32>;

    /// Fetch current balances.
    async fn fetch_balances(&self) -> MmResult<Vec<Balance>>;

    fn is_connected(&self) -> bool;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Balance {
    pub currency: String,
    pub free: Decimal,
    pub locked: Decimal,
    pub total: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

/// Core strategy trait. Strategies receive market snapshots and emit order actions.
/// All strategies — live and backtest — implement this same interface.
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn strategy_id(&self) -> &str;

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

/// Pre-trade risk check. Every order passes through risk checks before execution.
#[async_trait]
pub trait RiskCheck: Send + Sync {
    fn name(&self) -> &str;

    /// Evaluate whether an order should be allowed, rejected, or modified.
    async fn check_order(
        &self,
        order: &NewOrder,
        portfolio: &PortfolioRisk,
    ) -> RiskAction;

    /// Periodic portfolio-level risk evaluation.
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
