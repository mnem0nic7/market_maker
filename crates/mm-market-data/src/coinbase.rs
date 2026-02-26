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
const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

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
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) =
                                        handle_coinbase_message(&text, &book_tx, &trade_tx).await
                                    {
                                        warn!(error = %e, "failed to handle Coinbase message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "failed to send pong to Coinbase");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Coinbase WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Coinbase WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Coinbase WebSocket stream ended");
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

                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "exceeded max reconnection attempts for Coinbase, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to Coinbase WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
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
