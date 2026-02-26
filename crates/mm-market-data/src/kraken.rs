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
        let kraken_pair = normalizer::to_exchange_symbol(Exchange::Kraken, symbol).replace('/', "");
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

    book_tx
        .send(order_book)
        .await
        .map_err(|e| MmError::Exchange {
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

    sink.send(Message::Text(json))
        .await
        .map_err(|e| MmError::Exchange {
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
            let price = items[0].as_str().and_then(|s| s.parse::<Decimal>().ok())?;
            let quantity = items[1].as_str().and_then(|s| s.parse::<Decimal>().ok())?;
            Some(PriceLevel { price, quantity })
        })
        .collect()
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
        let connector = KrakenConnector::default();
        assert!(!connector.is_connected());
        assert_eq!(connector.exchange(), Exchange::Kraken);
    }

    #[test]
    fn connector_new_is_not_connected() {
        let connector = KrakenConnector::new();
        assert!(!connector.is_connected());
    }

    // -- parse_kraken_rest_levels tests --------------------------------------

    #[test]
    fn parse_rest_levels_valid_input() {
        let json: serde_json::Value = serde_json::from_str(
            r#"[["25000.10", "1.500", 1672531200], ["24999.90", "0.750", 1672531201]]"#,
        )
        .unwrap();
        let levels = parse_kraken_rest_levels(Some(&json));
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].price, dec!(25000.10));
        assert_eq!(levels[0].quantity, dec!(1.500));
        assert_eq!(levels[1].price, dec!(24999.90));
        assert_eq!(levels[1].quantity, dec!(0.750));
    }

    #[test]
    fn parse_rest_levels_empty_array() {
        let json: serde_json::Value = serde_json::from_str("[]").unwrap();
        let levels = parse_kraken_rest_levels(Some(&json));
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_rest_levels_none_input() {
        let levels = parse_kraken_rest_levels(None);
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_rest_levels_non_array_input() {
        let json: serde_json::Value = serde_json::from_str("\"not an array\"").unwrap();
        let levels = parse_kraken_rest_levels(Some(&json));
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_rest_levels_skips_short_entries() {
        let json: serde_json::Value =
            serde_json::from_str(r#"[["25000.10", "1.500", 100], ["only_one_element"]]"#).unwrap();
        let levels = parse_kraken_rest_levels(Some(&json));
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, dec!(25000.10));
    }

    // -- Deserialization tests -----------------------------------------------

    #[test]
    fn deserialize_book_data() {
        let json = r#"{
            "symbol": "BTC/USD",
            "bids": [{"price": 25000.10, "qty": 1.5}],
            "asks": [{"price": 25000.20, "qty": 2.0}],
            "checksum": 123456,
            "timestamp": "2023-01-01T00:00:00.000000Z"
        }"#;
        let data: KrakenBookData = serde_json::from_str(json).unwrap();
        assert_eq!(data.symbol, "BTC/USD");
        assert_eq!(data.bids.len(), 1);
        assert_eq!(data.bids[0].price, dec!(25000.10));
        assert_eq!(data.bids[0].qty, dec!(1.5));
        assert_eq!(data.asks.len(), 1);
        assert_eq!(data.asks[0].price, dec!(25000.20));
        assert_eq!(data.asks[0].qty, dec!(2.0));
        assert_eq!(data.checksum, 123456);
        assert_eq!(
            data.timestamp.as_deref(),
            Some("2023-01-01T00:00:00.000000Z")
        );
    }

    #[test]
    fn deserialize_book_data_no_timestamp() {
        let json = r#"{
            "symbol": "ETH/USD",
            "bids": [],
            "asks": [],
            "checksum": 0
        }"#;
        let data: KrakenBookData = serde_json::from_str(json).unwrap();
        assert_eq!(data.symbol, "ETH/USD");
        assert!(data.timestamp.is_none());
        assert!(data.bids.is_empty());
        assert!(data.asks.is_empty());
    }

    #[test]
    fn deserialize_trade_data() {
        let json = r#"{
            "symbol": "BTC/USD",
            "side": "buy",
            "price": 25000.15,
            "qty": 0.1,
            "trade_id": 12345,
            "timestamp": "2023-01-01T00:00:00.000000Z",
            "ord_type": "limit"
        }"#;
        let data: KrakenTradeData = serde_json::from_str(json).unwrap();
        assert_eq!(data.symbol, "BTC/USD");
        assert_eq!(data.side, "buy");
        assert_eq!(data.price, dec!(25000.15));
        assert_eq!(data.qty, dec!(0.1));
        assert_eq!(data.trade_id, 12345);
        assert_eq!(data.ord_type.as_deref(), Some("limit"));
    }

    #[test]
    fn deserialize_trade_data_sell_side() {
        let json = r#"{
            "symbol": "ETH/USD",
            "side": "sell",
            "price": 1800.00,
            "qty": 5.0,
            "trade_id": 99999,
            "timestamp": "2023-06-15T12:00:00Z"
        }"#;
        let data: KrakenTradeData = serde_json::from_str(json).unwrap();
        assert_eq!(data.side, "sell");
        assert_eq!(data.price, dec!(1800.00));
        assert!(data.ord_type.is_none());
    }

    #[test]
    fn deserialize_ws_message_with_data() {
        let json = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 25000.0, "qty": 1.0}],
                "asks": [{"price": 25001.0, "qty": 2.0}],
                "checksum": 111
            }]
        }"#;
        let msg: KrakenWsMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.channel.as_deref(), Some("book"));
        assert_eq!(msg.msg_type.as_deref(), Some("snapshot"));
        assert!(msg.data.is_some());
        assert_eq!(msg.data.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn deserialize_ws_message_subscribe_confirm() {
        let json = r#"{
            "method": "subscribe",
            "result": {"channel": "book", "symbol": "BTC/USD"},
            "success": true
        }"#;
        let msg: KrakenWsMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.method.as_deref(), Some("subscribe"));
        assert_eq!(msg.success, Some(true));
        assert!(msg.data.is_none());
    }

    #[test]
    fn deserialize_ws_message_subscribe_error() {
        let json = r#"{
            "method": "subscribe",
            "success": false,
            "error": "Invalid symbol"
        }"#;
        let msg: KrakenWsMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.success, Some(false));
        assert_eq!(msg.error.as_deref(), Some("Invalid symbol"));
    }

    // -- Subscribe message serialization tests -------------------------------

    #[test]
    fn serialize_book_subscribe_msg() {
        let msg = KrakenSubscribeMsg {
            method: "subscribe".to_string(),
            params: KrakenSubscribeParams {
                channel: "book".to_string(),
                symbol: vec!["BTC/USD".to_string(), "ETH/USD".to_string()],
                depth: Some(25),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"method\":\"subscribe\""));
        assert!(json.contains("\"channel\":\"book\""));
        assert!(json.contains("\"BTC/USD\""));
        assert!(json.contains("\"ETH/USD\""));
        assert!(json.contains("\"depth\":25"));
    }

    #[test]
    fn serialize_trade_subscribe_msg_no_depth() {
        let msg = KrakenSubscribeMsg {
            method: "subscribe".to_string(),
            params: KrakenSubscribeParams {
                channel: "trade".to_string(),
                symbol: vec!["BTC/USD".to_string()],
                depth: None,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"channel\":\"trade\""));
        // depth should be omitted when None (skip_serializing_if).
        assert!(!json.contains("depth"));
    }

    // -- Message handling tests (async) --------------------------------------

    #[tokio::test]
    async fn handle_book_snapshot_message() {
        let json = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 25000.10, "qty": 1.5}, {"price": 24999.00, "qty": 3.0}],
                "asks": [{"price": 25000.20, "qty": 2.0}],
                "checksum": 123456,
                "timestamp": "2023-01-01T00:00:00.000000Z"
            }]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        handle_kraken_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let book = book_rx.recv().await.unwrap();
        assert_eq!(book.exchange, Exchange::Kraken);
        assert_eq!(book.symbol.base, "BTC");
        assert_eq!(book.symbol.quote, "USD");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[0].price, dec!(25000.10));
        assert_eq!(book.bids[0].quantity, dec!(1.5));
        assert_eq!(book.bids[1].price, dec!(24999.00));
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.asks[0].price, dec!(25000.20));
        assert_eq!(book.sequence, 123456);
    }

    #[tokio::test]
    async fn handle_book_update_message() {
        let json = r#"{
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "ETH/USD",
                "bids": [{"price": 1800.50, "qty": 0.0}],
                "asks": [{"price": 1801.00, "qty": 10.0}],
                "checksum": 654321,
                "timestamp": "2023-06-15T12:00:00Z"
            }]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        handle_kraken_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let book = book_rx.recv().await.unwrap();
        assert_eq!(book.symbol.base, "ETH");
        assert_eq!(book.symbol.quote, "USD");
        // qty 0.0 means level removed, but it's still forwarded as a PriceLevel.
        assert_eq!(book.bids[0].quantity, dec!(0.0));
        assert_eq!(book.sequence, 654321);
    }

    #[tokio::test]
    async fn handle_trade_message() {
        let json = r#"{
            "channel": "trade",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "side": "buy",
                "price": 25000.15,
                "qty": 0.1,
                "trade_id": 42,
                "timestamp": "2023-01-01T00:00:00Z"
            }]
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        handle_kraken_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.exchange, Exchange::Kraken);
        assert_eq!(trade.symbol.base, "BTC");
        assert_eq!(trade.symbol.quote, "USD");
        assert_eq!(trade.price, dec!(25000.15));
        assert_eq!(trade.quantity, dec!(0.1));
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.trade_id, "42");
    }

    #[tokio::test]
    async fn handle_trade_message_sell_side() {
        let json = r#"{
            "channel": "trade",
            "type": "update",
            "data": [{
                "symbol": "ETH/USD",
                "side": "sell",
                "price": 1800.00,
                "qty": 5.0,
                "trade_id": 99,
                "timestamp": "2023-06-15T12:00:00Z"
            }]
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        handle_kraken_message(json, &book_tx, &trade_tx)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.symbol.base, "ETH");
    }

    #[tokio::test]
    async fn handle_subscribe_confirmation_is_ignored() {
        let json = r#"{
            "method": "subscribe",
            "result": {"channel": "book", "symbol": "BTC/USD"},
            "success": true
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        let result = handle_kraken_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());
        assert!(book_rx.try_recv().is_err());
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_heartbeat_is_ignored() {
        let json = r#"{
            "channel": "heartbeat"
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        let result = handle_kraken_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());
        assert!(book_rx.try_recv().is_err());
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_status_message_is_ignored() {
        let json = r#"{
            "channel": "status",
            "type": "update",
            "data": [{"system": "online", "version": "2.0.0"}]
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);

        let result = handle_kraken_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_ok());
        assert!(book_rx.try_recv().is_err());
        assert!(trade_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_invalid_json_returns_error() {
        let json = "{broken json";

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);

        let result = handle_kraken_message(json, &book_tx, &trade_tx).await;
        assert!(result.is_err());
    }

    // -- Symbol normalization (connector-specific usage) ----------------------

    #[test]
    fn kraken_symbol_normalization_btc_usd() {
        let sym = Symbol::new("BTC", "USD");
        let raw = normalizer::to_exchange_symbol(Exchange::Kraken, &sym);
        assert_eq!(raw, "BTC/USD");
    }

    #[test]
    fn kraken_symbol_normalization_eth_eur() {
        let sym = Symbol::new("ETH", "EUR");
        let raw = normalizer::to_exchange_symbol(Exchange::Kraken, &sym);
        assert_eq!(raw, "ETH/EUR");
    }

    #[test]
    fn kraken_from_exchange_symbol_xbt_usd() {
        // Kraken uses XBT for Bitcoin in some responses.
        let sym = normalizer::from_exchange_symbol(Exchange::Kraken, "XBT/USD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn kraken_from_exchange_symbol_btc_usd() {
        let sym = normalizer::from_exchange_symbol(Exchange::Kraken, "BTC/USD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn kraken_from_exchange_symbol_legacy_xxbtzusd() {
        let sym = normalizer::from_exchange_symbol(Exchange::Kraken, "XXBTZUSD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }
}
