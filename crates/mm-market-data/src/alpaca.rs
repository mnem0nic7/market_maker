//! Alpaca market data connector.
//!
//! Connects to the Alpaca real-time data WebSocket and subscribes to
//! quote and trade streams for US equities.
//!
//! # Wire format reference
//!
//! **Authentication** (sent on connect):
//! ```json
//! {"action": "auth", "key": "...", "secret": "..."}
//! ```
//!
//! **Subscription**:
//! ```json
//! {"action": "subscribe", "quotes": ["AAPL"], "trades": ["AAPL"]}
//! ```
//!
//! **Quote message** (type "q"):
//! ```json
//! {
//!   "T": "q",
//!   "S": "AAPL",
//!   "bp": 150.10,
//!   "bs": 100,
//!   "ap": 150.20,
//!   "as": 200,
//!   "t": "2023-01-01T00:00:00.000000Z"
//! }
//! ```
//!
//! **Trade message** (type "t"):
//! ```json
//! {
//!   "T": "t",
//!   "S": "AAPL",
//!   "p": 150.15,
//!   "s": 50,
//!   "t": "2023-01-01T00:00:00.000000Z",
//!   "i": 123456
//! }
//! ```
//!
//! # Symbol mapping
//!
//! Alpaca uses plain ticker symbols (e.g. "AAPL"). The canonical
//! `Symbol { base: "AAPL", quote: "USD" }` maps to "AAPL" on the wire.

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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_DATA_WS_URL: &str = "wss://stream.data.alpaca.markets/v2/iex";
const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Alpaca market data connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_data_ws_url")]
    pub data_ws_url: String,
}

fn default_data_ws_url() -> String {
    DEFAULT_DATA_WS_URL.into()
}

// ---------------------------------------------------------------------------
// Alpaca WebSocket message types (deserialization)
// ---------------------------------------------------------------------------

/// A single message element from the Alpaca data stream.
/// Alpaca sends arrays of messages; each element has a `T` field indicating
/// the message type.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AlpacaQuoteMsg {
    /// Message type ("q")
    #[serde(rename = "T")]
    msg_type: String,
    /// Symbol
    #[serde(rename = "S")]
    symbol: String,
    /// Bid price
    #[serde(rename = "bp")]
    bid_price: Decimal,
    /// Bid size
    #[serde(rename = "bs")]
    bid_size: Decimal,
    /// Ask price
    #[serde(rename = "ap")]
    ask_price: Decimal,
    /// Ask size
    #[serde(rename = "as")]
    ask_size: Decimal,
    /// Timestamp (RFC 3339)
    #[serde(rename = "t")]
    timestamp: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AlpacaTradeMsg {
    /// Message type ("t")
    #[serde(rename = "T")]
    msg_type: String,
    /// Symbol
    #[serde(rename = "S")]
    symbol: String,
    /// Trade price
    #[serde(rename = "p")]
    price: Decimal,
    /// Trade size
    #[serde(rename = "s")]
    size: Decimal,
    /// Timestamp (RFC 3339)
    #[serde(rename = "t")]
    timestamp: String,
    /// Trade ID
    #[serde(rename = "i")]
    trade_id: u64,
}

// ---------------------------------------------------------------------------
// AlpacaConnector
// ---------------------------------------------------------------------------

/// Alpaca market data connector for US equities.
///
/// Spawns a background task that maintains a WebSocket connection to the
/// Alpaca real-time data API and forwards normalized order book (from quotes)
/// and trade data through the provided channels.
pub struct AlpacaConnector {
    config: AlpacaConfig,
    connected: Arc<AtomicBool>,
    /// Handle to the background reader task so we can abort on disconnect.
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// HTTP client for REST snapshot fallback.
    http: reqwest::Client,
}

impl AlpacaConnector {
    pub fn new(config: AlpacaConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

/// Convert a canonical Symbol to the Alpaca ticker.
/// Symbol { base: "AAPL", quote: "USD" } -> "AAPL"
fn to_alpaca_symbol(symbol: &Symbol) -> String {
    symbol.base.clone()
}

/// Convert an Alpaca ticker to our canonical Symbol.
/// "AAPL" -> Symbol { base: "AAPL", quote: "USD" }
fn from_alpaca_symbol(ticker: &str) -> Symbol {
    Symbol::new(ticker, "USD")
}

// ---------------------------------------------------------------------------
// MarketDataConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl MarketDataConnector for AlpacaConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Alpaca
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Alpaca connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        let tickers: Vec<String> = symbols.iter().map(to_alpaca_symbol).collect();

        info!(tickers = ?tickers, "connecting to Alpaca data WebSocket");

        let connected = self.connected.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(&config.data_ws_url).await {
                    Ok((ws_stream, _response)) => {
                        info!("Alpaca data WebSocket connected");

                        let (mut write, mut read) = ws_stream.split();

                        // --- Wait for the server welcome message ---
                        // Alpaca sends [{"T":"success","msg":"connected"}] upon connection.
                        match read.next().await {
                            Some(Ok(Message::Text(_))) => {
                                // Welcome received, proceed with auth.
                            }
                            Some(Err(e)) => {
                                error!(error = %e, "Alpaca: error reading welcome message");
                                continue;
                            }
                            _ => {
                                error!("Alpaca: unexpected message or stream ended before welcome");
                                continue;
                            }
                        }

                        // --- Authenticate ---
                        let auth_msg = serde_json::json!({
                            "action": "auth",
                            "key": config.api_key,
                            "secret": config.secret_key,
                        });

                        if let Err(e) = write.send(Message::Text(auth_msg.to_string())).await {
                            error!(error = %e, "Alpaca: failed to send auth message");
                            continue;
                        }

                        // Wait for auth response.
                        match read.next().await {
                            Some(Ok(Message::Text(text))) => {
                                // Check for auth success: [{"T":"success","msg":"authenticated"}]
                                let parsed: Result<Vec<serde_json::Value>, _> =
                                    serde_json::from_str(&text);
                                if let Ok(msgs) = parsed {
                                    let is_auth_success = msgs.iter().any(|m| {
                                        m["T"].as_str() == Some("success")
                                            && m["msg"].as_str() == Some("authenticated")
                                    });
                                    if !is_auth_success {
                                        let is_error =
                                            msgs.iter().any(|m| m["T"].as_str() == Some("error"));
                                        if is_error {
                                            error!(
                                                response = %text,
                                                "Alpaca: authentication failed"
                                            );
                                            break; // Don't retry auth failures.
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                error!(error = %e, "Alpaca: error reading auth response");
                                continue;
                            }
                            _ => {
                                error!("Alpaca: stream ended before auth response");
                                continue;
                            }
                        }

                        // --- Subscribe ---
                        let subscribe_msg = serde_json::json!({
                            "action": "subscribe",
                            "quotes": tickers,
                            "trades": tickers,
                        });

                        if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                            error!(error = %e, "Alpaca: failed to send subscribe message");
                            continue;
                        }

                        info!(tickers = ?tickers, "Alpaca: subscriptions sent");
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;

                        // --- Main read loop ---
                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) =
                                        handle_alpaca_message(&text, &book_tx, &trade_tx).await
                                    {
                                        warn!(error = %e, "failed to handle Alpaca message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "failed to send pong to Alpaca");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Alpaca WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Alpaca WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Alpaca WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to connect to Alpaca WebSocket");
                    }
                }

                // Reconnection logic.
                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "exceeded max reconnection attempts for Alpaca, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to Alpaca WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("disconnecting Alpaca connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, _depth: usize) -> MmResult<OrderBook> {
        // Alpaca does not provide a REST order book endpoint. We return the
        // latest quote as a single-level book via the snapshot endpoint.
        let ticker = to_alpaca_symbol(symbol);
        let url = format!("https://data.alpaca.markets/v2/stocks/{}/snapshot", ticker);

        let resp = self
            .http
            .get(&url)
            .header("APCA-API-KEY-ID", &self.config.api_key)
            .header("APCA-API-SECRET-KEY", &self.config.secret_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("REST snapshot request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("REST snapshot returned status {}", resp.status()),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to parse snapshot response: {}", e),
        })?;

        // Parse the latest quote from the snapshot.
        let quote = &body["latestQuote"];
        let bid_price: Decimal = parse_decimal_field(quote, "bp", "bid price")?;
        let bid_size: Decimal = parse_decimal_field(quote, "bs", "bid size")?;
        let ask_price: Decimal = parse_decimal_field(quote, "ap", "ask price")?;
        let ask_size: Decimal = parse_decimal_field(quote, "as", "ask size")?;

        let timestamp = quote["t"]
            .as_str()
            .and_then(|t| t.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now);

        Ok(OrderBook {
            exchange: Exchange::Alpaca,
            symbol: symbol.clone(),
            bids: vec![PriceLevel {
                price: bid_price,
                quantity: bid_size,
            }],
            asks: vec![PriceLevel {
                price: ask_price,
                quantity: ask_size,
            }],
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

/// Alpaca sends messages as JSON arrays of objects, where each object has a
/// `T` field indicating the message type.
async fn handle_alpaca_message(
    text: &str,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    let messages: Vec<serde_json::Value> =
        serde_json::from_str(text).map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to parse Alpaca message array: {}", e),
        })?;

    for msg in messages {
        let msg_type = msg["T"].as_str().unwrap_or("");

        match msg_type {
            "q" => {
                let quote: AlpacaQuoteMsg =
                    serde_json::from_value(msg).map_err(|e| MmError::Exchange {
                        exchange: Exchange::Alpaca,
                        message: format!("failed to parse quote message: {}", e),
                    })?;
                handle_quote(quote, book_tx).await?;
            }
            "t" => {
                let trade: AlpacaTradeMsg =
                    serde_json::from_value(msg).map_err(|e| MmError::Exchange {
                        exchange: Exchange::Alpaca,
                        message: format!("failed to parse trade message: {}", e),
                    })?;
                handle_trade(trade, trade_tx).await?;
            }
            "success" | "subscription" => {
                // Control messages (auth success, subscription confirmation).
                // Already handled during connection setup; ignore here.
            }
            "error" => {
                let error_msg = msg["msg"].as_str().unwrap_or("unknown error");
                warn!(error = error_msg, "Alpaca: error message received");
            }
            _ => {
                // Ignore unknown message types.
            }
        }
    }

    Ok(())
}

async fn handle_quote(
    quote: AlpacaQuoteMsg,
    book_tx: &mpsc::Sender<OrderBook>,
) -> Result<(), MmError> {
    let symbol = from_alpaca_symbol(&quote.symbol);

    let timestamp: DateTime<Utc> = quote.timestamp.parse().unwrap_or_else(|_| Utc::now());

    // Alpaca quotes represent a single level (NBBO). We model this as a
    // single-level order book snapshot.
    let order_book = OrderBook {
        exchange: Exchange::Alpaca,
        symbol,
        bids: vec![PriceLevel {
            price: quote.bid_price,
            quantity: quote.bid_size,
        }],
        asks: vec![PriceLevel {
            price: quote.ask_price,
            quantity: quote.ask_size,
        }],
        timestamp,
        sequence: 0,
    };

    book_tx
        .send(order_book)
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to send order book update: {}", e),
        })?;

    Ok(())
}

async fn handle_trade(
    trade: AlpacaTradeMsg,
    trade_tx: &mpsc::Sender<Trade>,
) -> Result<(), MmError> {
    let symbol = from_alpaca_symbol(&trade.symbol);

    let timestamp: DateTime<Utc> = trade.timestamp.parse().unwrap_or_else(|_| Utc::now());

    // Alpaca trade messages do not include a taker side. Default to Buy as
    // the side is not determinable from the consolidated feed.
    let normalized = Trade {
        exchange: Exchange::Alpaca,
        symbol,
        price: trade.price,
        quantity: trade.size,
        side: Side::Buy,
        timestamp,
        trade_id: trade.trade_id.to_string(),
    };

    trade_tx
        .send(normalized)
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to send trade: {}", e),
        })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a numeric field from a JSON value, handling both number and string
/// representations. Alpaca snapshot responses may encode numbers directly.
fn parse_decimal_field(
    obj: &serde_json::Value,
    field: &str,
    label: &str,
) -> Result<Decimal, MmError> {
    if let Some(s) = obj[field].as_str() {
        s.parse::<Decimal>().map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to parse {} '{}': {}", label, s, e),
        })
    } else if let Some(n) = obj[field].as_f64() {
        // Convert via string to preserve precision with Decimal.
        let s = n.to_string();
        s.parse::<Decimal>().map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("failed to parse {} '{}': {}", label, s, e),
        })
    } else {
        Err(MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("{} field '{}' not found or not a number", label, field),
        })
    }
}
