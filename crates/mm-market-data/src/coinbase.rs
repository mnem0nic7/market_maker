//! Coinbase Advanced Trade market data connector.
//!
//! Connects to the Coinbase Advanced Trade WebSocket API and subscribes to
//! the `level2` (order book) and `market_trades` channels.
//!
//! # Subscribe message format
//!
//! ```json
//! {
//!   "type": "subscribe",
//!   "product_ids": ["BTC-USD", "ETH-USD"],
//!   "channel": "level2"
//! }
//! ```
//!
//! # Level2 update message
//!
//! ```json
//! {
//!   "channel": "l2_data",
//!   "events": [{
//!     "type": "snapshot" | "update",
//!     "product_id": "BTC-USD",
//!     "updates": [
//!       {"side": "bid", "price_level": "25000.10", "new_quantity": "1.5"},
//!       {"side": "offer", "price_level": "25000.20", "new_quantity": "0.0"}
//!     ]
//!   }]
//! }
//! ```
//!
//! # Market trades message
//!
//! ```json
//! {
//!   "channel": "market_trades",
//!   "events": [{
//!     "type": "snapshot" | "update",
//!     "trades": [{
//!       "trade_id": "123",
//!       "product_id": "BTC-USD",
//!       "price": "25000.15",
//!       "size": "0.1",
//!       "side": "BUY",
//!       "time": "2023-01-01T00:00:00Z"
//!     }]
//!   }]
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use futures::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use mm_core::errors::{MmError, MmResult};
use mm_core::traits::MarketDataConnector;
use mm_core::types::{Exchange, OrderBook, PriceLevel, Side, Symbol, Trade};

use crate::normalizer;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COINBASE_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const COINBASE_REST_URL: &str = "https://api.coinbase.com";
const WS_RECONNECT_DELAY_SECS: u64 = 5;
const WS_MAX_BACKOFF_SECS: u64 = 300; // cap at 5 minutes
const WS_READ_TIMEOUT_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// Subscribe / unsubscribe messages
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct CoinbaseSubscribeMsg {
    #[serde(rename = "type")]
    msg_type: String,
    product_ids: Vec<String>,
    channel: String,
}

// ---------------------------------------------------------------------------
// Coinbase WebSocket message types (deserialization)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CoinbaseWsMessage {
    channel: Option<String>,
    events: Option<Vec<serde_json::Value>>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
}

/// A single level2 event.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CoinbaseL2Event {
    #[serde(rename = "type")]
    event_type: String,
    product_id: String,
    updates: Vec<CoinbaseL2Update>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseL2Update {
    side: String,
    price_level: String,
    new_quantity: String,
}

/// Market trades event.
#[derive(Debug, Deserialize)]
struct CoinbaseTradeEvent {
    #[serde(rename = "type")]
    event_type: String,
    trades: Vec<CoinbaseTradeEntry>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseTradeEntry {
    trade_id: String,
    product_id: String,
    price: String,
    size: String,
    side: String,
    time: String,
}

/// REST order book response.
#[derive(Debug, Deserialize)]
struct CoinbaseOrderBookResponse {
    pricebook: CoinbasePriceBook,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CoinbasePriceBook {
    product_id: String,
    bids: Vec<CoinbasePriceBookLevel>,
    asks: Vec<CoinbasePriceBookLevel>,
    time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CoinbasePriceBookLevel {
    price: String,
    size: String,
}

// ---------------------------------------------------------------------------
// CoinbaseConnector
// ---------------------------------------------------------------------------

/// Coinbase Advanced Trade market data connector.
pub struct CoinbaseConnector {
    connected: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    http: reqwest::Client,
}

impl CoinbaseConnector {
    pub fn new() -> Self {
        Self {
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

impl Default for CoinbaseConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataConnector for CoinbaseConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Coinbase
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Coinbase connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        let product_ids: Vec<String> = symbols
            .iter()
            .map(|s| normalizer::to_exchange_symbol(Exchange::Coinbase, s))
            .collect();

        info!(products = ?product_ids, "connecting to Coinbase WebSocket");

        let connected = self.connected.clone();
        let products = product_ids.clone();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(COINBASE_WS_URL).await {
                    Ok((ws_stream, _response)) => {
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("Coinbase WebSocket connected");

                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to level2 channel.
                        let l2_sub = CoinbaseSubscribeMsg {
                            msg_type: "subscribe".to_string(),
                            product_ids: products.clone(),
                            channel: "level2".to_string(),
                        };
                        if let Err(e) = send_json(&mut write, &l2_sub).await {
                            error!(error = %e, "failed to send level2 subscribe");
                            connected.store(false, Ordering::SeqCst);
                            break;
                        }

                        // Subscribe to market_trades channel.
                        let trades_sub = CoinbaseSubscribeMsg {
                            msg_type: "subscribe".to_string(),
                            product_ids: products.clone(),
                            channel: "market_trades".to_string(),
                        };
                        if let Err(e) = send_json(&mut write, &trades_sub).await {
                            error!(error = %e, "failed to send market_trades subscribe");
                            connected.store(false, Ordering::SeqCst);
                            break;
                        }

                        info!("Coinbase subscriptions sent");

                        loop {
                            match tokio::time::timeout(
                                Duration::from_secs(WS_READ_TIMEOUT_SECS),
                                read.next(),
                            )
                            .await
                            {
                                Ok(Some(Ok(Message::Text(text)))) => {
                                    reconnect_attempts = 0;
                                    if let Err(e) =
                                        handle_coinbase_message(&text, &book_tx, &trade_tx).await
                                    {
                                        warn!(error = %e, "failed to handle Coinbase message");
                                    }
                                }
                                Ok(Some(Ok(Message::Ping(data)))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "failed to send pong to Coinbase");
                                        break;
                                    }
                                }
                                Ok(Some(Ok(Message::Close(frame)))) => {
                                    info!(?frame, "Coinbase WebSocket closed by server");
                                    break;
                                }
                                Ok(Some(Err(e))) => {
                                    error!(error = %e, "Coinbase WebSocket error");
                                    break;
                                }
                                Ok(None) => {
                                    info!("Coinbase WebSocket stream ended");
                                    break;
                                }
                                Err(_) => {
                                    warn!("WebSocket read timeout after 30s, reconnecting...");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to connect to Coinbase WebSocket");
                    }
                }

                // Reconnection logic with exponential backoff and jitter.
                reconnect_attempts += 1;

                let base_delay = WS_RECONNECT_DELAY_SECS.min(WS_MAX_BACKOFF_SECS);
                let exp_delay = base_delay.saturating_mul(1u64 << reconnect_attempts.min(6));
                let capped_delay = exp_delay.min(WS_MAX_BACKOFF_SECS);
                // Add jitter: +/-25%
                let jitter_range = capped_delay / 4;
                let delay = if jitter_range > 0 {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .subsec_nanos() as u64;
                    capped_delay - jitter_range + nanos % (jitter_range * 2)
                } else {
                    capped_delay
                };

                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to Coinbase WebSocket"
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("disconnecting Coinbase connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook> {
        let product_id = normalizer::to_exchange_symbol(Exchange::Coinbase, symbol);
        let url = format!(
            "{}/api/v3/brokerage/market/product_book?product_id={}&limit={}",
            COINBASE_REST_URL, product_id, depth
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("REST request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("REST request returned status {}", resp.status()),
            });
        }

        let body: CoinbaseOrderBookResponse = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("failed to parse order book response: {}", e),
        })?;

        let bids: Vec<PriceLevel> = body
            .pricebook
            .bids
            .iter()
            .filter_map(|l| {
                let price = l.price.parse::<Decimal>().ok()?;
                let quantity = l.size.parse::<Decimal>().ok()?;
                Some(PriceLevel { price, quantity })
            })
            .collect();

        let asks: Vec<PriceLevel> = body
            .pricebook
            .asks
            .iter()
            .filter_map(|l| {
                let price = l.price.parse::<Decimal>().ok()?;
                let quantity = l.size.parse::<Decimal>().ok()?;
                Some(PriceLevel { price, quantity })
            })
            .collect();

        let timestamp = body
            .pricebook
            .time
            .as_deref()
            .and_then(|t| t.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now);

        Ok(OrderBook {
            exchange: Exchange::Coinbase,
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp,
            sequence: 0,
        })
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

async fn handle_coinbase_message(
    text: &str,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    let msg: CoinbaseWsMessage = serde_json::from_str(text).map_err(|e| MmError::Exchange {
        exchange: Exchange::Coinbase,
        message: format!("failed to parse Coinbase WS message: {}", e),
    })?;

    let channel = msg.channel.as_deref().unwrap_or("");
    let events = match msg.events {
        Some(events) => events,
        None => return Ok(()), // Subscription confirmations etc.
    };

    match channel {
        "l2_data" => {
            for event_val in events {
                if let Ok(event) = serde_json::from_value::<CoinbaseL2Event>(event_val) {
                    handle_l2_event(event, book_tx).await?;
                }
            }
        }
        "market_trades" => {
            for event_val in events {
                if let Ok(event) = serde_json::from_value::<CoinbaseTradeEvent>(event_val) {
                    handle_trade_event(event, trade_tx).await?;
                }
            }
        }
        "subscriptions" | "heartbeats" => {
            // Ignore subscription confirmations and heartbeats.
        }
        other => {
            warn!(channel = other, "unknown Coinbase channel, ignoring");
        }
    }

    Ok(())
}

async fn handle_l2_event(
    event: CoinbaseL2Event,
    book_tx: &mpsc::Sender<OrderBook>,
) -> Result<(), MmError> {
    let symbol = normalizer::from_exchange_symbol(Exchange::Coinbase, &event.product_id)
        .unwrap_or_else(|| Symbol::new(&event.product_id, "UNKNOWN"));

    let mut bids = Vec::new();
    let mut asks = Vec::new();

    for update in &event.updates {
        let price = update
            .price_level
            .parse::<Decimal>()
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse price '{}': {}", update.price_level, e),
            })?;
        let quantity = update
            .new_quantity
            .parse::<Decimal>()
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse quantity '{}': {}", update.new_quantity, e),
            })?;

        let level = PriceLevel { price, quantity };

        match update.side.as_str() {
            "bid" => bids.push(level),
            "offer" => asks.push(level),
            other => {
                warn!(side = other, "unexpected Coinbase L2 side");
            }
        }
    }

    // Sort bids descending, asks ascending.
    bids.sort_by(|a, b| b.price.cmp(&a.price));
    asks.sort_by(|a, b| a.price.cmp(&b.price));

    let order_book = OrderBook {
        exchange: Exchange::Coinbase,
        symbol,
        bids,
        asks,
        timestamp: Utc::now(),
        sequence: 0,
    };

    book_tx
        .send(order_book)
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("failed to send order book update: {}", e),
        })?;

    Ok(())
}

async fn handle_trade_event(
    event: CoinbaseTradeEvent,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    // Only forward "update" events, not the initial snapshot.
    if event.event_type == "snapshot" {
        return Ok(());
    }

    for entry in &event.trades {
        let symbol = normalizer::from_exchange_symbol(Exchange::Coinbase, &entry.product_id)
            .unwrap_or_else(|| Symbol::new(&entry.product_id, "UNKNOWN"));

        let price = entry
            .price
            .parse::<Decimal>()
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse trade price '{}': {}", entry.price, e),
            })?;

        let quantity = entry
            .size
            .parse::<Decimal>()
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse trade size '{}': {}", entry.size, e),
            })?;

        let side = match entry.side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => Side::Buy, // Default assumption.
        };

        let timestamp: DateTime<Utc> = entry.time.parse().unwrap_or_else(|_| Utc::now());

        let trade = Trade {
            exchange: Exchange::Coinbase,
            symbol,
            price,
            quantity,
            side,
            timestamp,
            trade_id: entry.trade_id.clone(),
        };

        trade_tx.send(trade).await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("failed to send trade: {}", e),
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn send_json<S, T>(sink: &mut S, msg: &T) -> Result<(), MmError>
where
    S: futures::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
    T: Serialize,
{
    let json = serde_json::to_string(msg).map_err(|e| MmError::Exchange {
        exchange: Exchange::Coinbase,
        message: format!("failed to serialize subscribe message: {}", e),
    })?;

    sink.send(Message::Text(json))
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("failed to send WebSocket message: {}", e),
        })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // -- Connector construction tests ----------------------------------------

    #[test]
    fn connector_default_is_not_connected() {
        let connector = CoinbaseConnector::default();
        assert!(!connector.is_connected());
        assert_eq!(connector.exchange(), Exchange::Coinbase);
    }

    #[test]
    fn connector_new_is_not_connected() {
        let connector = CoinbaseConnector::new();
        assert!(!connector.is_connected());
    }

    // -- Deserialization tests -----------------------------------------------

    #[test]
    fn deserialize_l2_event() {
        let json = r#"{
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [
                {"side": "bid", "price_level": "25000.10", "new_quantity": "1.5"},
                {"side": "offer", "price_level": "25000.20", "new_quantity": "2.0"}
            ]
        }"#;
        let event: CoinbaseL2Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "update");
        assert_eq!(event.product_id, "BTC-USD");
        assert_eq!(event.updates.len(), 2);
        assert_eq!(event.updates[0].side, "bid");
        assert_eq!(event.updates[0].price_level, "25000.10");
        assert_eq!(event.updates[0].new_quantity, "1.5");
        assert_eq!(event.updates[1].side, "offer");
    }

    #[test]
    fn deserialize_l2_snapshot_event() {
        let json = r#"{
            "type": "snapshot",
            "product_id": "ETH-USD",
            "updates": [
                {"side": "bid", "price_level": "1800.00", "new_quantity": "10.0"},
                {"side": "bid", "price_level": "1799.50", "new_quantity": "5.0"},
                {"side": "offer", "price_level": "1800.50", "new_quantity": "3.0"}
            ]
        }"#;
        let event: CoinbaseL2Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "snapshot");
        assert_eq!(event.product_id, "ETH-USD");
        assert_eq!(event.updates.len(), 3);
    }

    #[test]
    fn deserialize_trade_event() {
        let json = r#"{
            "type": "update",
            "trades": [{
                "trade_id": "123",
                "product_id": "BTC-USD",
                "price": "25000.15",
                "size": "0.1",
                "side": "BUY",
                "time": "2023-01-01T00:00:00Z"
            }]
        }"#;
        let event: CoinbaseTradeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "update");
        assert_eq!(event.trades.len(), 1);
        assert_eq!(event.trades[0].trade_id, "123");
        assert_eq!(event.trades[0].product_id, "BTC-USD");
        assert_eq!(event.trades[0].price, "25000.15");
        assert_eq!(event.trades[0].size, "0.1");
        assert_eq!(event.trades[0].side, "BUY");
    }

    #[test]
    fn deserialize_trade_event_multiple_trades() {
        let json = r#"{
            "type": "update",
            "trades": [
                {
                    "trade_id": "100",
                    "product_id": "BTC-USD",
                    "price": "25000.00",
                    "size": "0.5",
                    "side": "BUY",
                    "time": "2023-01-01T00:00:00Z"
                },
                {
                    "trade_id": "101",
                    "product_id": "BTC-USD",
                    "price": "25001.00",
                    "size": "1.0",
                    "side": "SELL",
                    "time": "2023-01-01T00:00:01Z"
                }
            ]
        }"#;
        let event: CoinbaseTradeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.trades.len(), 2);
        assert_eq!(event.trades[0].side, "BUY");
        assert_eq!(event.trades[1].side, "SELL");
    }

    #[test]
    fn deserialize_subscribe_msg_serialization() {
        let msg = CoinbaseSubscribeMsg {
            msg_type: "subscribe".to_string(),
            product_ids: vec!["BTC-USD".to_string(), "ETH-USD".to_string()],
            channel: "level2".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"subscribe\""));
        assert!(json.contains("\"product_ids\":[\"BTC-USD\",\"ETH-USD\"]"));
        assert!(json.contains("\"channel\":\"level2\""));
    }

    #[test]
    fn deserialize_ws_message_with_channel() {
        let json = r#"{
            "channel": "l2_data",
            "events": [{"type": "update", "product_id": "BTC-USD", "updates": []}]
        }"#;
        let msg: CoinbaseWsMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.channel.as_deref(), Some("l2_data"));
        assert!(msg.events.is_some());
    }

    #[test]
    fn deserialize_ws_message_subscription_confirm() {
        let json = r#"{
            "type": "subscriptions",
            "channels": [{"name": "level2", "product_ids": ["BTC-USD"]}]
        }"#;
        let msg: CoinbaseWsMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.msg_type.as_deref(), Some("subscriptions"));
        // No channel field in subscription confirmations.
        assert!(msg.channel.is_none());
    }

    #[test]
    fn deserialize_order_book_response() {
        let json = r#"{
            "pricebook": {
                "product_id": "BTC-USD",
                "bids": [
                    {"price": "25000.00", "size": "1.5"},
                    {"price": "24999.00", "size": "2.0"}
                ],
                "asks": [
                    {"price": "25001.00", "size": "0.5"}
                ],
                "time": "2023-01-01T00:00:00Z"
            }
        }"#;
        let resp: CoinbaseOrderBookResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.pricebook.product_id, "BTC-USD");
        assert_eq!(resp.pricebook.bids.len(), 2);
        assert_eq!(resp.pricebook.asks.len(), 1);
        assert_eq!(resp.pricebook.bids[0].price, "25000.00");
        assert_eq!(resp.pricebook.bids[0].size, "1.5");
    }

    // -- Message handling tests (async) --------------------------------------

    #[tokio::test]
    async fn handle_l2_data_message() {
        let json = r#"{
            "channel": "l2_data",
            "events": [{
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "price_level": "25000.10", "new_quantity": "1.500"},
                    {"side": "offer", "price_level": "25000.20", "new_quantity": "2.100"}
                ]
            }]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        handle_coinbase_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let book = book_rx.recv().await.unwrap();
        assert_eq!(book.exchange, Exchange::Coinbase);
        assert_eq!(book.symbol.base, "BTC");
        assert_eq!(book.symbol.quote, "USD");
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price, dec!(25000.10));
        assert_eq!(book.bids[0].quantity, dec!(1.500));
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.asks[0].price, dec!(25000.20));
        assert_eq!(book.asks[0].quantity, dec!(2.100));
    }

    #[tokio::test]
    async fn handle_l2_data_sorts_bids_desc_asks_asc() {
        let json = r#"{
            "channel": "l2_data",
            "events": [{
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "price_level": "24999.00", "new_quantity": "1.0"},
                    {"side": "bid", "price_level": "25000.00", "new_quantity": "2.0"},
                    {"side": "offer", "price_level": "25002.00", "new_quantity": "0.5"},
                    {"side": "offer", "price_level": "25001.00", "new_quantity": "1.0"}
                ]
            }]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        handle_coinbase_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let book = book_rx.recv().await.unwrap();
        // Bids should be sorted descending by price.
        assert_eq!(book.bids[0].price, dec!(25000.00));
        assert_eq!(book.bids[1].price, dec!(24999.00));
        // Asks should be sorted ascending by price.
        assert_eq!(book.asks[0].price, dec!(25001.00));
        assert_eq!(book.asks[1].price, dec!(25002.00));
    }

    #[tokio::test]
    async fn handle_market_trades_update_message() {
        let json = r#"{
            "channel": "market_trades",
            "events": [{
                "type": "update",
                "trades": [{
                    "trade_id": "456",
                    "product_id": "BTC-USD",
                    "price": "25000.15",
                    "size": "0.100",
                    "side": "BUY",
                    "time": "2023-01-01T00:00:00Z"
                }]
            }]
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        handle_coinbase_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.exchange, Exchange::Coinbase);
        assert_eq!(trade.symbol.base, "BTC");
        assert_eq!(trade.symbol.quote, "USD");
        assert_eq!(trade.price, dec!(25000.15));
        assert_eq!(trade.quantity, dec!(0.100));
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.trade_id, "456");
    }

    #[tokio::test]
    async fn handle_market_trades_sell_side() {
        let json = r#"{
            "channel": "market_trades",
            "events": [{
                "type": "update",
                "trades": [{
                    "trade_id": "789",
                    "product_id": "ETH-USD",
                    "price": "1800.00",
                    "size": "5.0",
                    "side": "SELL",
                    "time": "2023-06-15T12:30:00Z"
                }]
            }]
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        handle_coinbase_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.symbol.base, "ETH");
    }

    #[tokio::test]
    async fn handle_market_trades_snapshot_is_ignored() {
        let json = r#"{
            "channel": "market_trades",
            "events": [{
                "type": "snapshot",
                "trades": [{
                    "trade_id": "1",
                    "product_id": "BTC-USD",
                    "price": "25000.00",
                    "size": "1.0",
                    "side": "BUY",
                    "time": "2023-01-01T00:00:00Z"
                }]
            }]
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        handle_coinbase_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        // Snapshot trades are intentionally not forwarded.
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_subscription_confirmation_is_ignored() {
        let json = r#"{
            "channel": "subscriptions",
            "events": [{"subscriptions": {"level2": ["BTC-USD"]}}]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        let result = handle_coinbase_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());

        // No data should be emitted.
        assert!(book_rx.try_recv().is_err());
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_heartbeat_is_ignored() {
        let json = r#"{
            "channel": "heartbeats",
            "events": [{"current_time": "2023-01-01T00:00:00Z"}]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        let result = handle_coinbase_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());
        assert!(book_rx.try_recv().is_err());
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_invalid_json_returns_error() {
        let json = "not valid json at all";

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        let result = handle_coinbase_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_message_without_events_is_ok() {
        // Some messages (like initial connection acks) have no events field.
        let json = r#"{"type": "connected"}"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        let result = handle_coinbase_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());
    }

    // -- Symbol normalization (connector-specific usage) ----------------------

    #[test]
    fn coinbase_symbol_normalization_btcusd() {
        let sym = Symbol::new("BTC", "USD");
        let raw = normalizer::to_exchange_symbol(Exchange::Coinbase, &sym);
        assert_eq!(raw, "BTC-USD");
    }

    #[test]
    fn coinbase_symbol_normalization_ethusd() {
        let sym = Symbol::new("ETH", "USD");
        let raw = normalizer::to_exchange_symbol(Exchange::Coinbase, &sym);
        assert_eq!(raw, "ETH-USD");
    }

    #[test]
    fn coinbase_from_exchange_symbol() {
        let sym = normalizer::from_exchange_symbol(Exchange::Coinbase, "BTC-USD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn coinbase_from_exchange_symbol_eth_usdc() {
        let sym = normalizer::from_exchange_symbol(Exchange::Coinbase, "ETH-USDC").unwrap();
        assert_eq!(sym.base, "ETH");
        assert_eq!(sym.quote, "USDC");
    }
}
