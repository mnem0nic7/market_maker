//! Kraken v2 WebSocket market data connector.
//!
//! Connects to the Kraken v2 WebSocket API at `wss://ws.kraken.com/v2` and
//! subscribes to the `book` and `trade` channels.
//!
//! # Subscribe message format (v2)
//!
//! ```json
//! {
//!   "method": "subscribe",
//!   "params": {
//!     "channel": "book",
//!     "symbol": ["BTC/USD"],
//!     "depth": 25
//!   }
//! }
//! ```
//!
//! # Book snapshot message
//!
//! ```json
//! {
//!   "channel": "book",
//!   "type": "snapshot",
//!   "data": [{
//!     "symbol": "BTC/USD",
//!     "bids": [{"price": 25000.10, "qty": 1.5}],
//!     "asks": [{"price": 25000.20, "qty": 2.0}],
//!     "checksum": 123456
//!   }]
//! }
//! ```
//!
//! # Book update message
//!
//! ```json
//! {
//!   "channel": "book",
//!   "type": "update",
//!   "data": [{
//!     "symbol": "BTC/USD",
//!     "bids": [{"price": 25000.10, "qty": 0.0}],
//!     "asks": [{"price": 25000.30, "qty": 3.0}],
//!     "checksum": 654321,
//!     "timestamp": "2023-01-01T00:00:00.000000Z"
//!   }]
//! }
//! ```
//!
//! # Trade message
//!
//! ```json
//! {
//!   "channel": "trade",
//!   "type": "update",
//!   "data": [{
//!     "symbol": "BTC/USD",
//!     "side": "buy",
//!     "price": 25000.15,
//!     "qty": 0.1,
//!     "ord_type": "limit",
//!     "trade_id": 12345,
//!     "timestamp": "2023-01-01T00:00:00.000000Z"
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

const KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v2";
const KRAKEN_REST_URL: &str = "https://api.kraken.com";
const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const DEFAULT_BOOK_DEPTH: u32 = 25;

// ---------------------------------------------------------------------------
// Kraken v2 subscribe messages
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct KrakenSubscribeMsg {
    method: String,
    params: KrakenSubscribeParams,
}

#[derive(Debug, Serialize)]
struct KrakenSubscribeParams {
    channel: String,
    symbol: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    depth: Option<u32>,
}

// ---------------------------------------------------------------------------
// Kraken v2 WebSocket message types (deserialization)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KrakenWsMessage {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<Vec<serde_json::Value>>,
    /// Method responses (subscribe confirmations, errors).
    method: Option<String>,
    result: Option<serde_json::Value>,
    error: Option<String>,
    success: Option<bool>,
}

/// A single book data entry.
#[derive(Debug, Deserialize)]
struct KrakenBookData {
    symbol: String,
    #[serde(default)]
    bids: Vec<KrakenBookLevel>,
    #[serde(default)]
    asks: Vec<KrakenBookLevel>,
    #[serde(default)]
    checksum: u64,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenBookLevel {
    price: Decimal,
    qty: Decimal,
}

/// A single trade data entry.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KrakenTradeData {
    symbol: String,
    side: String,
    price: Decimal,
    qty: Decimal,
    trade_id: u64,
    timestamp: Option<String>,
    #[serde(default)]
    ord_type: Option<String>,
}

/// REST order book response.
#[derive(Debug, Deserialize)]
struct KrakenRestResponse {
    error: Vec<String>,
    result: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// KrakenConnector
// ---------------------------------------------------------------------------

/// Kraken v2 WebSocket market data connector.
pub struct KrakenConnector {
    connected: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    http: reqwest::Client,
}

impl KrakenConnector {
    pub fn new() -> Self {
        Self {
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

impl Default for KrakenConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataConnector for KrakenConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Kraken
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Kraken connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        // Kraken v2 WS uses "BTC/USD" format via the normalizer.
        let kraken_symbols: Vec<String> = symbols
            .iter()
            .map(|s| normalizer::to_exchange_symbol(Exchange::Kraken, s))
            .collect();

        info!(symbols = ?kraken_symbols, "connecting to Kraken v2 WebSocket");

        let connected = self.connected.clone();
        let ws_symbols = kraken_symbols.clone();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(KRAKEN_WS_URL).await {
                    Ok((ws_stream, _response)) => {
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("Kraken v2 WebSocket connected");

                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to book channel.
                        let book_sub = KrakenSubscribeMsg {
                            method: "subscribe".to_string(),
                            params: KrakenSubscribeParams {
                                channel: "book".to_string(),
                                symbol: ws_symbols.clone(),
                                depth: Some(DEFAULT_BOOK_DEPTH),
                            },
                        };
                        if let Err(e) = send_json(&mut write, &book_sub).await {
                            error!(error = %e, "failed to send Kraken book subscribe");
                            connected.store(false, Ordering::SeqCst);
                            break;
                        }

                        // Subscribe to trade channel.
                        let trade_sub = KrakenSubscribeMsg {
                            method: "subscribe".to_string(),
                            params: KrakenSubscribeParams {
                                channel: "trade".to_string(),
                                symbol: ws_symbols.clone(),
                                depth: None,
                            },
                        };
                        if let Err(e) = send_json(&mut write, &trade_sub).await {
                            error!(error = %e, "failed to send Kraken trade subscribe");
                            connected.store(false, Ordering::SeqCst);
                            break;
                        }

                        info!("Kraken subscriptions sent");

                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) =
                                        handle_kraken_message(&text, &book_tx, &trade_tx).await
                                    {
                                        warn!(error = %e, "failed to handle Kraken message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "failed to send pong to Kraken");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Kraken WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Kraken WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Kraken WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to connect to Kraken WebSocket");
                    }
                }

                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "exceeded max reconnection attempts for Kraken, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to Kraken WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("disconnecting Kraken connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook> {
        // Kraken REST uses the legacy pair format for the Depth endpoint.
        // We'll use the pair without slash, e.g. "XBTUSD".
        let kraken_pair = normalizer::to_exchange_symbol(Exchange::Kraken, symbol)
            .replace('/', "");
        let url = format!(
            "{}/0/public/Depth?pair={}&count={}",
            KRAKEN_REST_URL, kraken_pair, depth
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("REST request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("REST request returned status {}", resp.status()),
            });
        }

        let body: KrakenRestResponse = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Kraken,
            message: format!("failed to parse REST response: {}", e),
        })?;

        if !body.error.is_empty() {
            return Err(MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("Kraken API error: {:?}", body.error),
            });
        }

        let result = body.result.ok_or_else(|| MmError::Exchange {
            exchange: Exchange::Kraken,
            message: "no result in Kraken REST response".to_string(),
        })?;

        // The result is a map with the pair name as key.
        let pair_data = result
            .as_object()
            .and_then(|m| m.values().next())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: "unexpected Kraken REST response format".to_string(),
            })?;

        let bids = parse_kraken_rest_levels(pair_data.get("bids"));
        let asks = parse_kraken_rest_levels(pair_data.get("asks"));

        Ok(OrderBook {
            exchange: Exchange::Kraken,
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp: Utc::now(),
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

async fn handle_kraken_message(
    text: &str,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    let msg: KrakenWsMessage = serde_json::from_str(text).map_err(|e| MmError::Exchange {
        exchange: Exchange::Kraken,
        message: format!("failed to parse Kraken WS message: {}", e),
    })?;

    // Handle method responses (subscribe confirmations / errors).
    if msg.method.is_some() {
        if let Some(false) = msg.success {
            let err_msg = msg.error.unwrap_or_default();
            warn!(error = %err_msg, "Kraken subscription error");
        }
        return Ok(());
    }

    let channel = msg.channel.as_deref().unwrap_or("");
    let msg_type = msg.msg_type.as_deref().unwrap_or("");
    let data = match msg.data {
        Some(data) => data,
        None => return Ok(()),
    };

    match channel {
        "book" => {
            for entry_val in data {
                if let Ok(book_data) = serde_json::from_value::<KrakenBookData>(entry_val) {
                    handle_book_data(book_data, msg_type, book_tx).await?;
                }
            }
        }
        "trade" => {
            for entry_val in data {
                if let Ok(trade_data) = serde_json::from_value::<KrakenTradeData>(entry_val) {
                    handle_trade_data(trade_data, trade_tx).await?;
                }
            }
        }
        "heartbeat" | "status" => {
            // Ignore heartbeats and status messages.
        }
        other => {
            warn!(channel = other, "unknown Kraken channel, ignoring");
        }
    }

    Ok(())
}

async fn handle_book_data(
    data: KrakenBookData,
    msg_type: &str,
    book_tx: &mpsc::Sender<OrderBook>,
) -> Result<(), MmError> {
    let symbol = normalizer::from_exchange_symbol(Exchange::Kraken, &data.symbol)
        .unwrap_or_else(|| Symbol::new(&data.symbol, "UNKNOWN"));

    let timestamp = data
        .timestamp
        .as_deref()
        .and_then(|t| t.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);

    let bids: Vec<PriceLevel> = data
        .bids
        .iter()
        .map(|l| PriceLevel {
            price: l.price,
            quantity: l.qty,
        })
        .collect();

    let asks: Vec<PriceLevel> = data
        .asks
        .iter()
        .map(|l| PriceLevel {
            price: l.price,
            quantity: l.qty,
        })
        .collect();

    let sequence = data.checksum;

    let _ = msg_type; // Both snapshot and update are forwarded; the local
                       // book manager distinguishes them based on context.

    let order_book = OrderBook {
        exchange: Exchange::Kraken,
        symbol,
        bids,
        asks,
        timestamp,
        sequence,
    };

    book_tx.send(order_book).await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Kraken,
        message: format!("failed to send order book update: {}", e),
    })?;

    Ok(())
}

async fn handle_trade_data(
    data: KrakenTradeData,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    let symbol = normalizer::from_exchange_symbol(Exchange::Kraken, &data.symbol)
        .unwrap_or_else(|| Symbol::new(&data.symbol, "UNKNOWN"));

    let side = match data.side.to_lowercase().as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => Side::Buy,
    };

    let timestamp = data
        .timestamp
        .as_deref()
        .and_then(|t| t.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(Utc::now);

    let trade = Trade {
        exchange: Exchange::Kraken,
        symbol,
        price: data.price,
        quantity: data.qty,
        side,
        timestamp,
        trade_id: data.trade_id.to_string(),
    };

    trade_tx.send(trade).await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Kraken,
        message: format!("failed to send trade: {}", e),
    })?;

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
        exchange: Exchange::Kraken,
        message: format!("failed to serialize message: {}", e),
    })?;

    sink.send(Message::Text(json)).await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Kraken,
        message: format!("failed to send WebSocket message: {}", e),
    })?;

    Ok(())
}

/// Parse Kraken REST order book levels.
///
/// Kraken REST returns levels as `[[price, volume, timestamp], ...]`.
fn parse_kraken_rest_levels(val: Option<&serde_json::Value>) -> Vec<PriceLevel> {
    let arr = match val.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };

    arr.iter()
        .filter_map(|entry| {
            let items = entry.as_array()?;
            if items.len() < 2 {
                return None;
            }
            let price = items[0]
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())?;
            let quantity = items[1]
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())?;
            Some(PriceLevel { price, quantity })
        })
        .collect()
}
