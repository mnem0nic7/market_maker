//! Market data manager and connector re-exports.
//!
//! [`MarketDataManager`] orchestrates multiple exchange-specific
//! [`MarketDataConnector`] implementations, routing subscribe/unsubscribe
//! requests to the correct connector and providing a unified interface for
//! querying order books.

use std::collections::HashMap;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use mm_core::errors::{MmError, MmResult};
use mm_core::traits::MarketDataConnector;
use mm_core::types::{Exchange, OrderBook, Symbol, Trade};

use crate::order_book::LocalOrderBook;

// Re-export the trait so callers only need to depend on mm-market-data.
pub use mm_core::traits::MarketDataConnector as MarketDataConnectorTrait;

// ---------------------------------------------------------------------------
// MarketDataManager
// ---------------------------------------------------------------------------

/// Manages multiple exchange market data connectors.
///
/// Provides a unified interface to subscribe to market data across exchanges
/// and maintains local order books that are kept up-to-date by background
/// WebSocket reader tasks.
pub struct MarketDataManager {
    /// One connector per exchange.
    connectors: HashMap<Exchange, Box<dyn MarketDataConnector>>,

    /// Local order books keyed by `(Exchange, canonical_symbol_string)`.
    books: DashMap<(Exchange, String), LocalOrderBook>,

    /// Channel senders for order book updates (downstream consumers).
    book_tx: mpsc::Sender<OrderBook>,
    /// Channel senders for trade updates (downstream consumers).
    trade_tx: mpsc::Sender<Trade>,
}

impl MarketDataManager {
    /// Create a new `MarketDataManager`.
    ///
    /// * `book_tx` / `trade_tx` - channels that receive normalized updates
    ///   from all exchanges. The caller is responsible for reading from the
    ///   corresponding receivers.
    pub fn new(book_tx: mpsc::Sender<OrderBook>, trade_tx: mpsc::Sender<Trade>) -> Self {
        Self {
            connectors: HashMap::new(),
            books: DashMap::new(),
            book_tx,
            trade_tx,
        }
    }

    /// Register an exchange-specific connector.
    pub fn register_connector(&mut self, connector: Box<dyn MarketDataConnector>) {
        let exchange = connector.exchange();
        info!(%exchange, "registering market data connector");
        self.connectors.insert(exchange, connector);
    }

    /// Subscribe to market data for the given symbols on the specified exchange.
    ///
    /// This connects to the exchange (if not already connected) and begins
    /// streaming order book and trade data.
    pub async fn subscribe(&mut self, exchange: Exchange, symbols: &[Symbol]) -> MmResult<()> {
        let connector = self
            .connectors
            .get_mut(&exchange)
            .ok_or_else(|| MmError::Exchange {
                exchange,
                message: "no connector registered for this exchange".to_string(),
            })?;

        // Initialize local books for each symbol.
        for symbol in symbols {
            let key = (exchange, symbol.canonical());
            if !self.books.contains_key(&key) {
                self.books
                    .insert(key, LocalOrderBook::new(exchange, symbol.clone()));
            }
        }

        info!(%exchange, symbols = ?symbols.iter().map(|s| s.canonical()).collect::<Vec<_>>(), "subscribing to market data");

        connector
            .connect(symbols, self.book_tx.clone(), self.trade_tx.clone())
            .await?;

        Ok(())
    }

    /// Disconnect from the specified exchange.
    pub async fn unsubscribe(&mut self, exchange: Exchange) -> MmResult<()> {
        let connector = self
            .connectors
            .get_mut(&exchange)
            .ok_or_else(|| MmError::Exchange {
                exchange,
                message: "no connector registered for this exchange".to_string(),
            })?;

        info!(%exchange, "unsubscribing / disconnecting market data connector");

        connector.disconnect().await?;

        // Remove all local books for this exchange.
        self.books.retain(|k, _| k.0 != exchange);

        Ok(())
    }

    /// Retrieve the current local order book for a given exchange and symbol.
    ///
    /// Returns the top `depth` levels from each side. Returns `None` if the
    /// book has not been initialized yet.
    pub fn get_order_book(
        &self,
        exchange: Exchange,
        symbol: &Symbol,
        depth: usize,
    ) -> Option<OrderBook> {
        let key = (exchange, symbol.canonical());
        self.books.get(&key).and_then(|book| {
            if book.is_initialized() {
                Some(book.to_order_book(depth))
            } else {
                warn!(
                    %exchange,
                    symbol = %symbol.canonical(),
                    "order book not yet initialized"
                );
                None
            }
        })
    }

    /// Update the local order book from a received [`OrderBook`] snapshot.
    ///
    /// This is typically called by the internal pipeline that reads from the
    /// channels populated by connectors.
    pub fn update_local_book(&self, ob: &OrderBook) {
        let key = (ob.exchange, ob.symbol.canonical());
        let bids: Vec<_> = ob.bids.iter().map(|l| (l.price, l.quantity)).collect();
        let asks: Vec<_> = ob.asks.iter().map(|l| (l.price, l.quantity)).collect();

        if let Some(mut book) = self.books.get_mut(&key) {
            book.apply_snapshot(&bids, &asks, ob.timestamp, ob.sequence);
        } else {
            let mut book = LocalOrderBook::new(ob.exchange, ob.symbol.clone());
            book.apply_snapshot(&bids, &asks, ob.timestamp, ob.sequence);
            self.books.insert(key, book);
        }
    }

    /// Check whether a specific exchange connector is currently connected.
    pub fn is_connected(&self, exchange: Exchange) -> bool {
        self.connectors
            .get(&exchange)
            .is_some_and(|c| c.is_connected())
    }

    /// List all registered exchanges.
    pub fn exchanges(&self) -> Vec<Exchange> {
        self.connectors.keys().copied().collect()
    }

    /// Disconnect all connectors.
    pub async fn disconnect_all(&mut self) {
        let exchanges: Vec<_> = self.connectors.keys().copied().collect();
        for exchange in exchanges {
            if let Err(e) = self.unsubscribe(exchange).await {
                error!(%exchange, error = %e, "failed to disconnect connector");
            }
        }
    }
}
