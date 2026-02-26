//! Binance market data connector.
//!
//! Connects to the Binance combined WebSocket stream and subscribes to
//! `<symbol>@depth@100ms` (order book deltas) and `<symbol>@trade` channels.
//!
//! # Wire format reference
//!
//! **depthUpdate** (partial book depth stream):
//! ```json
//! {
//!   "e": "depthUpdate",
//!   "E": 1672515782136,
//!   "s": "BTCUSDT",
//!   "U": 157,
//!   "u": 160,
//!   "b": [["25000.10", "1.500"], ["24999.90", "0.000"]],
//!   "a": [["25000.20", "2.100"]]
//! }
//! ```
//!
//! **trade**:
//! ```json
//! {
//!   "e": "trade",
//!   "E": 1672515782136,
//!   "s": "BTCUSDT",
//!   "t": 1234567,
//!   "p": "25000.15",
//!   "q": "0.100",
//!   "T": 1672515782136,
//!   "m": true
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::StreamExt;
use futures::SinkExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use mm_core::errors::{MmError, MmResult};
use mm_core::traits::MarketDataConnector;
use mm_core::types::{Exchange, OrderBook, PriceLevel, Side, Symbol, Trade};

use crate::normalizer;

// ---------------------------------------------------------------------------
// Constants & Configuration
// ---------------------------------------------------------------------------

const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

// ---------------------------------------------------------------------------
// Binance WebSocket message types (deserialization)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceWsMessage {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceDepthUpdate {
    /// Event time
    #[serde(rename = "E")]
    event_time: u64,
    /// Symbol
    #[serde(rename = "s")]
    symbol: String,
    /// First update ID in event
    #[serde(rename = "U")]
    first_update_id: u64,
    /// Final update ID in event
    #[serde(rename = "u")]
    last_update_id: u64,
    /// Bids: [[price, quantity], ...]
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    /// Asks: [[price, quantity], ...]
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceTrade {
    /// Event time
    #[serde(rename = "E")]
    event_time: u64,
    /// Symbol
    #[serde(rename = "s")]
    symbol: String,
    /// Trade ID
    #[serde(rename = "t")]
    trade_id: u64,
    /// Price
    #[serde(rename = "p")]
    price: String,
    /// Quantity
    #[serde(rename = "q")]
    quantity: String,
    /// Trade time
    #[serde(rename = "T")]
    trade_time: u64,
    /// Is the buyer the market maker? (true = seller-initiated = Sell)
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

/// REST order book snapshot response.
#[derive(Debug, Deserialize)]
struct BinanceOrderBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

// ---------------------------------------------------------------------------
// BinanceConnector
// ---------------------------------------------------------------------------

/// Configuration for the Binance market data connector.
#[derive(Debug, Clone)]
pub struct BinanceMarketDataConfig {
    /// Base REST URL (e.g. `https://api.binance.us`).
    pub rest_url: String,
    /// Base WebSocket URL (e.g. `wss://stream.binance.us:9443`).
    pub ws_url: String,
}

impl Default for BinanceMarketDataConfig {
    fn default() -> Self {
        Self {
            rest_url: "https://api.binance.us".into(),
            ws_url: "wss://stream.binance.us:9443".into(),
        }
    }
}

/// Binance market data connector.
///
/// Spawns a background task that maintains a WebSocket connection to the
/// Binance combined streams endpoint and forwards normalized data through
/// the provided channels.
pub struct BinanceConnector {
    config: BinanceMarketDataConfig,
    connected: Arc<AtomicBool>,
    /// Handle to the background reader task so we can abort on disconnect.
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// HTTP client for REST snapshot fallback.
    http: reqwest::Client,
}

impl BinanceConnector {
    pub fn new(config: BinanceMarketDataConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

impl Default for BinanceConnector {
    fn default() -> Self {
        Self::new(BinanceMarketDataConfig::default())
    }
}

#[async_trait]
impl MarketDataConnector for BinanceConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Binance connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        // Build the combined stream URL.
        // Format: wss://stream.binance.us:9443/stream?streams=btcusdt@depth@100ms/btcusdt@trade
        let streams: Vec<String> = symbols
            .iter()
            .flat_map(|sym| {
                let raw = normalizer::to_exchange_symbol(Exchange::Binance, sym).to_lowercase();
                vec![format!("{}@depth@100ms", raw), format!("{}@trade", raw)]
            })
            .collect();

        let url = format!(
            "{}/stream?streams={}",
            self.config.ws_url,
            streams.join("/")
        );

        info!(url = %url, "connecting to Binance WebSocket");

        let connected = self.connected.clone();
        let symbols_owned: Vec<Symbol> = symbols.to_vec();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(&url).await {
                    Ok((ws_stream, _response)) => {
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("Binance WebSocket connected");

                        let (mut _write, mut read) = ws_stream.split();

                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = handle_binance_message(
                                        &text,
                                        &book_tx,
                                        &trade_tx,
                                        &symbols_owned,
                                    )
                                    .await
                                    {
                                        warn!(error = %e, "failed to handle Binance message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = _write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "failed to send pong to Binance");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Binance WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Binance WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Binance WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to connect to Binance WebSocket");
                    }
                }

                // Reconnection logic.
                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "exceeded max reconnection attempts for Binance, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to Binance WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("disconnecting Binance connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook> {
        let raw = normalizer::to_exchange_symbol(Exchange::Binance, symbol);
        let limit = depth.min(5000); // Binance max is 5000
        let url = format!(
            "{}/api/v3/depth?symbol={}&limit={}",
            self.config.rest_url, raw, limit
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("REST request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("REST request returned status {}", resp.status()),
            });
        }

        let snapshot: BinanceOrderBookSnapshot =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("failed to parse order book snapshot: {}", e),
            })?;

        let bids = parse_price_levels(&snapshot.bids);
        let asks = parse_price_levels(&snapshot.asks);

        Ok(OrderBook {
            exchange: Exchange::Binance,
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp: Utc::now(),
            sequence: snapshot.last_update_id,
        })
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

/// Binance combined-stream wrapper envelope.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceCombinedStream {
    stream: String,
    data: serde_json::Value,
}

async fn handle_binance_message(
    text: &str,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
    symbols: &[Symbol],
) -> Result<(), MmError> {
    // Combined streams wrap messages in {"stream": "...", "data": {...}}.
    let combined: BinanceCombinedStream =
        serde_json::from_str(text).map_err(|e| MmError::Exchange {
            exchange: Exchange::Binance,
            message: format!("failed to parse combined stream envelope: {}", e),
        })?;

    // Determine message type from the data payload.
    let event_type = combined
        .data
        .get("e")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "depthUpdate" => {
            let depth: BinanceDepthUpdate =
                serde_json::from_value(combined.data).map_err(|e| MmError::Exchange {
                    exchange: Exchange::Binance,
                    message: format!("failed to parse depthUpdate: {}", e),
                })?;
            handle_depth_update(depth, book_tx, symbols).await?;
        }
        "trade" => {
            let trade: BinanceTrade =
                serde_json::from_value(combined.data).map_err(|e| MmError::Exchange {
                    exchange: Exchange::Binance,
                    message: format!("failed to parse trade: {}", e),
                })?;
            handle_trade(trade, trade_tx).await?;
        }
        other => {
            warn!(event_type = other, "unknown Binance event type, ignoring");
        }
    }

    Ok(())
}

async fn handle_depth_update(
    depth: BinanceDepthUpdate,
    book_tx: &mpsc::Sender<OrderBook>,
    _symbols: &[Symbol],
) -> Result<(), MmError> {
    let symbol = normalizer::from_exchange_symbol(Exchange::Binance, &depth.symbol)
        .unwrap_or_else(|| Symbol::new(&depth.symbol, "UNKNOWN"));

    let timestamp = millis_to_datetime(depth.event_time);
    let bids = parse_price_levels(&depth.bids);
    let asks = parse_price_levels(&depth.asks);

    let order_book = OrderBook {
        exchange: Exchange::Binance,
        symbol,
        bids,
        asks,
        timestamp,
        sequence: depth.last_update_id,
    };

    book_tx
        .send(order_book)
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Binance,
            message: format!("failed to send order book update: {}", e),
        })?;

    Ok(())
}

async fn handle_trade(trade: BinanceTrade, trade_tx: &mpsc::Sender<Trade>) -> Result<(), MmError> {
    let symbol = normalizer::from_exchange_symbol(Exchange::Binance, &trade.symbol)
        .unwrap_or_else(|| Symbol::new(&trade.symbol, "UNKNOWN"));

    let price: Decimal = trade.price.parse().map_err(|e| MmError::Exchange {
        exchange: Exchange::Binance,
        message: format!("failed to parse trade price '{}': {}", trade.price, e),
    })?;

    let quantity: Decimal = trade.quantity.parse().map_err(|e| MmError::Exchange {
        exchange: Exchange::Binance,
        message: format!("failed to parse trade quantity '{}': {}", trade.quantity, e),
    })?;

    // In Binance: `m = true` means the buyer is the maker, i.e. the trade
    // was initiated by a sell order (taker sells).
    let side = if trade.is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };

    let timestamp = millis_to_datetime(trade.trade_time);

    let normalized = Trade {
        exchange: Exchange::Binance,
        symbol,
        price,
        quantity,
        side,
        timestamp,
        trade_id: trade.trade_id.to_string(),
    };

    trade_tx
        .send(normalized)
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Binance,
            message: format!("failed to send trade: {}", e),
        })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_price_levels(raw: &[[String; 2]]) -> Vec<PriceLevel> {
    raw.iter()
        .filter_map(|pair| {
            let price = pair[0].parse::<Decimal>().ok()?;
            let quantity = pair[1].parse::<Decimal>().ok()?;
            Some(PriceLevel { price, quantity })
        })
        .collect()
}

fn millis_to_datetime(millis: u64) -> DateTime<Utc> {
    let secs = (millis / 1000) as i64;
    let nsecs = ((millis % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs)
        .single()
        .unwrap_or_else(Utc::now)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // -- Helper tests --------------------------------------------------------

    #[test]
    fn parse_price_levels_valid_input() {
        let raw = vec![
            ["25000.10".to_string(), "1.500".to_string()],
            ["24999.90".to_string(), "0.750".to_string()],
        ];
        let levels = parse_price_levels(&raw);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].price, dec!(25000.10));
        assert_eq!(levels[0].quantity, dec!(1.500));
        assert_eq!(levels[1].price, dec!(24999.90));
        assert_eq!(levels[1].quantity, dec!(0.750));
    }

    #[test]
    fn parse_price_levels_empty_input() {
        let raw: Vec<[String; 2]> = vec![];
        let levels = parse_price_levels(&raw);
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_price_levels_skips_invalid_entries() {
        let raw = vec![
            ["25000.10".to_string(), "1.500".to_string()],
            ["not_a_number".to_string(), "1.0".to_string()],
            ["25001.00".to_string(), "bad".to_string()],
        ];
        let levels = parse_price_levels(&raw);
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, dec!(25000.10));
    }

    #[test]
    fn millis_to_datetime_known_value() {
        // 2023-01-01 00:00:00.000 UTC = 1672531200000 ms
        let dt = millis_to_datetime(1672531200000);
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn millis_to_datetime_with_fractional_millis() {
        // 1672531200123 => 2023-01-01T00:00:00.123Z
        let dt = millis_to_datetime(1672531200123);
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.nanosecond(), 123_000_000);
    }

    #[test]
    fn millis_to_datetime_zero() {
        let dt = millis_to_datetime(0);
        assert_eq!(dt.year(), 1970);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }

    // -- Config tests --------------------------------------------------------

    #[test]
    fn default_config_has_expected_urls() {
        let config = BinanceMarketDataConfig::default();
        assert!(config.rest_url.contains("binance"));
        assert!(config.ws_url.starts_with("wss://"));
        assert!(config.ws_url.contains("binance"));
    }

    #[test]
    fn custom_config_overrides() {
        let config = BinanceMarketDataConfig {
            rest_url: "https://testnet.binance.vision".into(),
            ws_url: "wss://testnet.binance.vision/ws".into(),
        };
        assert_eq!(config.rest_url, "https://testnet.binance.vision");
        assert_eq!(config.ws_url, "wss://testnet.binance.vision/ws");
    }

    // -- Connector construction tests ----------------------------------------

    #[test]
    fn connector_default_is_not_connected() {
        let connector = BinanceConnector::default();
        assert!(!connector.is_connected());
        assert_eq!(connector.exchange(), Exchange::Binance);
    }

    #[test]
    fn connector_with_custom_config() {
        let config = BinanceMarketDataConfig {
            rest_url: "https://custom.api".into(),
            ws_url: "wss://custom.ws".into(),
        };
        let connector = BinanceConnector::new(config);
        assert!(!connector.is_connected());
        assert_eq!(connector.exchange(), Exchange::Binance);
    }

    // -- Deserialization tests -----------------------------------------------

    #[test]
    fn deserialize_depth_update() {
        let json = r#"{
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "U": 157,
            "u": 160,
            "b": [["25000.10", "1.500"], ["24999.90", "0.000"]],
            "a": [["25000.20", "2.100"]]
        }"#;
        let depth: BinanceDepthUpdate = serde_json::from_str(json).unwrap();
        assert_eq!(depth.symbol, "BTCUSDT");
        assert_eq!(depth.event_time, 1672515782136);
        assert_eq!(depth.first_update_id, 157);
        assert_eq!(depth.last_update_id, 160);
        assert_eq!(depth.bids.len(), 2);
        assert_eq!(depth.asks.len(), 1);
        assert_eq!(depth.bids[0][0], "25000.10");
        assert_eq!(depth.bids[0][1], "1.500");
        assert_eq!(depth.asks[0][0], "25000.20");
        assert_eq!(depth.asks[0][1], "2.100");
    }

    #[test]
    fn deserialize_trade() {
        let json = r#"{
            "e": "trade",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "t": 1234567,
            "p": "25000.15",
            "q": "0.100",
            "T": 1672515782136,
            "m": true
        }"#;
        let trade: BinanceTrade = serde_json::from_str(json).unwrap();
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.trade_id, 1234567);
        assert_eq!(trade.price, "25000.15");
        assert_eq!(trade.quantity, "0.100");
        assert!(trade.is_buyer_maker);
    }

    #[test]
    fn deserialize_trade_buyer_is_taker() {
        let json = r#"{
            "e": "trade",
            "E": 1672515782136,
            "s": "ETHUSDT",
            "t": 99999,
            "p": "1800.00",
            "q": "5.500",
            "T": 1672515782136,
            "m": false
        }"#;
        let trade: BinanceTrade = serde_json::from_str(json).unwrap();
        assert!(!trade.is_buyer_maker);
        assert_eq!(trade.symbol, "ETHUSDT");
    }

    #[test]
    fn deserialize_combined_stream_envelope() {
        let json = r#"{
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1672515782136,
                "s": "BTCUSDT",
                "t": 100,
                "p": "25000.00",
                "q": "1.0",
                "T": 1672515782136,
                "m": false
            }
        }"#;
        let combined: BinanceCombinedStream = serde_json::from_str(json).unwrap();
        assert_eq!(combined.stream, "btcusdt@trade");
        assert_eq!(combined.data["e"], "trade");
        assert_eq!(combined.data["s"], "BTCUSDT");
    }

    #[test]
    fn deserialize_order_book_snapshot() {
        let json = r#"{
            "lastUpdateId": 12345,
            "bids": [["25000.00", "1.5"], ["24999.00", "2.0"]],
            "asks": [["25001.00", "0.5"], ["25002.00", "1.0"]]
        }"#;
        let snapshot: BinanceOrderBookSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.last_update_id, 12345);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
    }

    // -- Message handling tests (async) --------------------------------------

    #[tokio::test]
    async fn handle_depth_update_message() {
        let json = r#"{
            "stream": "btcusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1672531200000,
                "s": "BTCUSDT",
                "U": 100,
                "u": 105,
                "b": [["25000.10", "1.500"]],
                "a": [["25000.20", "2.100"], ["25001.00", "0.500"]]
            }
        }"#;

        let (book_tx, mut book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);
        let symbols = vec![Symbol::new("BTC", "USDT")];

        handle_binance_message(json, &book_tx, &trade_tx, &symbols)
            .await
            .unwrap();

        let book = book_rx.recv().await.unwrap();
        assert_eq!(book.exchange, Exchange::Binance);
        assert_eq!(book.symbol.base, "BTC");
        assert_eq!(book.symbol.quote, "USDT");
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price, dec!(25000.10));
        assert_eq!(book.bids[0].quantity, dec!(1.500));
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.asks[0].price, dec!(25000.20));
        assert_eq!(book.sequence, 105);
    }

    #[tokio::test]
    async fn handle_trade_message() {
        let json = r#"{
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1672531200000,
                "s": "BTCUSDT",
                "t": 42,
                "p": "25000.15",
                "q": "0.100",
                "T": 1672531200000,
                "m": true
            }
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);
        let symbols = vec![Symbol::new("BTC", "USDT")];

        handle_binance_message(json, &book_tx, &trade_tx, &symbols)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.exchange, Exchange::Binance);
        assert_eq!(trade.symbol.base, "BTC");
        assert_eq!(trade.symbol.quote, "USDT");
        assert_eq!(trade.price, dec!(25000.15));
        assert_eq!(trade.quantity, dec!(0.100));
        assert_eq!(trade.side, Side::Sell); // is_buyer_maker = true => Sell
        assert_eq!(trade.trade_id, "42");
    }

    #[tokio::test]
    async fn handle_trade_message_buy_side() {
        let json = r#"{
            "stream": "ethusdt@trade",
            "data": {
                "e": "trade",
                "E": 1672531200000,
                "s": "ETHUSDT",
                "t": 99,
                "p": "1800.50",
                "q": "10.0",
                "T": 1672531200000,
                "m": false
            }
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, mut trade_rx) = mpsc::channel(10);
        let symbols = vec![Symbol::new("ETH", "USDT")];

        handle_binance_message(json, &book_tx, &trade_tx, &symbols)
            .await
            .unwrap();

        let trade = trade_rx.recv().await.unwrap();
        assert_eq!(trade.side, Side::Buy); // is_buyer_maker = false => Buy
        assert_eq!(trade.symbol.base, "ETH");
        assert_eq!(trade.price, dec!(1800.50));
    }

    #[tokio::test]
    async fn handle_unknown_event_type_does_not_error() {
        let json = r#"{
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1672531200000,
                "s": "BTCUSDT"
            }
        }"#;

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);
        let symbols = vec![Symbol::new("BTC", "USDT")];

        // Unknown event types should be silently ignored (just a warning log).
        let result = handle_binance_message(json, &book_tx, &trade_tx, &symbols).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_invalid_json_returns_error() {
        let json = "this is not valid json";

        let (book_tx, _book_rx) = mpsc::channel(10);
        let (trade_tx, _trade_rx) = mpsc::channel(10);
        let symbols = vec![Symbol::new("BTC", "USDT")];

        let result = handle_binance_message(json, &book_tx, &trade_tx, &symbols).await;
        assert!(result.is_err());
    }

    // -- Symbol normalization (connector-specific usage) ----------------------

    #[test]
    fn binance_symbol_normalization_btcusdt() {
        let sym = Symbol::new("BTC", "USDT");
        let raw = normalizer::to_exchange_symbol(Exchange::Binance, &sym);
        assert_eq!(raw, "BTCUSDT");
    }

    #[test]
    fn binance_symbol_normalization_lowercase_input() {
        // The connect method lowercases the symbol for stream URLs.
        let sym = Symbol::new("BTC", "USDT");
        let raw = normalizer::to_exchange_symbol(Exchange::Binance, &sym).to_lowercase();
        assert_eq!(raw, "btcusdt");
    }

    #[test]
    fn binance_from_exchange_symbol_btcusdt() {
        let sym = normalizer::from_exchange_symbol(Exchange::Binance, "BTCUSDT").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USDT");
    }

    #[test]
    fn binance_from_exchange_symbol_ethbtc() {
        let sym = normalizer::from_exchange_symbol(Exchange::Binance, "ETHBTC").unwrap();
        assert_eq!(sym.base, "ETH");
        assert_eq!(sym.quote, "BTC");
    }

    use chrono::Datelike;
    use chrono::Timelike;
}
