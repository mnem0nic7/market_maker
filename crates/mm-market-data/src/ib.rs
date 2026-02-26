//! Interactive Brokers market data connector.
//!
//! Connects to the IB Client Portal API gateway for market data via both
//! REST snapshots and WebSocket streaming.
//!
//! # IB Client Portal API
//!
//! The Client Portal gateway exposes a local REST/WS API at
//! `https://localhost:5000/v1/api`. It uses session-based authentication
//! managed by the gateway itself (no HMAC signing needed from callers).
//!
//! ## Market data snapshot (REST)
//!
//! ```text
//! GET /md/snapshot?conids={conid}&fields=31,84,85,86,88
//! ```
//!
//! Returns a JSON array of objects with numeric field keys:
//! - `31` — last price
//! - `84` — bid price
//! - `85` — ask price
//! - `86` — ask size
//! - `87` — ask size (displayed)
//! - `88` — bid size
//!
//! ## WebSocket streaming
//!
//! ```text
//! wss://localhost:5000/v1/api/ws
//! ```
//!
//! Subscribe by sending: `smd+{conid}+{"fields":["31","84","85","86"]}`
//!
//! The gateway sends back JSON messages with field updates keyed by conid.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::StreamExt;
use futures::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use mm_core::errors::{MmError, MmResult};
use mm_core::traits::MarketDataConnector;
use mm_core::types::{Exchange, OrderBook, PriceLevel, Side, Symbol, Trade};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_BASE_URL: &str = "https://localhost:5000/v1/api";
const DEFAULT_WS_URL: &str = "wss://localhost:5000/v1/api/ws";
const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the IB market data connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbConfig {
    /// Base URL for the IB Client Portal REST API.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// WebSocket URL for streaming market data.
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
    /// IB account identifier.
    pub account_id: String,
    /// Mapping of canonical symbol strings (e.g., "AAPL") to IB contract IDs.
    pub conid_map: HashMap<String, String>,
}

fn default_base_url() -> String {
    DEFAULT_BASE_URL.into()
}

fn default_ws_url() -> String {
    DEFAULT_WS_URL.into()
}

// ---------------------------------------------------------------------------
// IB market data snapshot response types
// ---------------------------------------------------------------------------

/// A single instrument snapshot from the IB `/md/snapshot` endpoint.
///
/// Field keys are numeric strings. We capture the fields we care about.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct IbSnapshotEntry {
    /// Contract ID
    conid: Option<u64>,
    /// Last price (field 31)
    #[serde(rename = "31", default)]
    last_price: Option<String>,
    /// Bid price (field 84)
    #[serde(rename = "84", default)]
    bid_price: Option<String>,
    /// Ask price (field 85)
    #[serde(rename = "85", default)]
    ask_price: Option<String>,
    /// Volume (field 86)
    #[serde(rename = "86", default)]
    volume: Option<String>,
    /// Ask size (field 87)
    #[serde(rename = "87", default)]
    ask_size: Option<String>,
    /// Bid size (field 88)
    #[serde(rename = "88", default)]
    bid_size: Option<String>,
}

// ---------------------------------------------------------------------------
// IbConnector
// ---------------------------------------------------------------------------

/// Interactive Brokers market data connector.
///
/// Connects to the IB Client Portal gateway's WebSocket endpoint for
/// streaming market data and uses the REST API for order book snapshots.
/// IB identifies instruments by contract IDs (conids) rather than ticker
/// symbols; the mapping is provided via [`IbConfig::conid_map`].
pub struct IbConnector {
    config: IbConfig,
    connected: Arc<AtomicBool>,
    /// Handle to the background WebSocket reader task.
    task_handle: Option<JoinHandle<()>>,
    /// HTTP client configured to accept self-signed certificates from the
    /// IB Client Portal gateway.
    http: reqwest::Client,
}

impl IbConnector {
    pub fn new(config: IbConfig) -> Self {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client for IB");

        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http,
        }
    }

    /// Resolve a canonical [`Symbol`] to an IB contract ID using the configured mapping.
    ///
    /// IB uses the base symbol (e.g., "AAPL") as the lookup key.
    fn resolve_conid(&self, symbol: &Symbol) -> MmResult<String> {
        self.config
            .conid_map
            .get(&symbol.base)
            .cloned()
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("no conid mapping for symbol {}", symbol),
            })
    }

    /// Reverse-lookup: find the canonical [`Symbol`] for a given contract ID.
    #[allow(dead_code)]
    fn symbol_for_conid(&self, conid: &str) -> Option<Symbol> {
        for (base, cid) in &self.config.conid_map {
            if cid == conid {
                // For equities the quote currency is USD by convention.
                return Some(Symbol::new(base.as_str(), "USD"));
            }
        }
        None
    }
}

#[async_trait]
impl MarketDataConnector for IbConnector {
    fn exchange(&self) -> Exchange {
        Exchange::InteractiveBrokers
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("IB connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        // Build the list of conids we need to subscribe to.
        let mut conid_symbol_map: HashMap<String, Symbol> = HashMap::new();
        for sym in symbols {
            let conid = self.resolve_conid(sym)?;
            conid_symbol_map.insert(conid, sym.clone());
        }

        let ws_url = self.config.ws_url.clone();
        let connected = Arc::clone(&self.connected);

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                info!(url = %ws_url, "connecting to IB Client Portal WebSocket");

                // NOTE: IB Client Portal uses self-signed certificates.
                // If `native-tls` is available as a direct dependency, you can
                // use `connect_async_tls_with_config` with
                // `danger_accept_invalid_certs(true)` instead.
                //
                // For production, either:
                // 1. Add the IB gateway cert to the system trust store, or
                // 2. Add `native-tls` to Cargo.toml and build a custom
                //    TLS connector with `danger_accept_invalid_certs(true)`.
                match tokio_tungstenite::connect_async(&ws_url).await {
                    Ok((ws_stream, _response)) => {
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("IB Client Portal WebSocket connected");

                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to market data for each conid.
                        for conid in conid_symbol_map.keys() {
                            let sub_msg = format!(
                                "smd+{}+{{\"fields\":[\"31\",\"84\",\"85\",\"86\",\"87\",\"88\"]}}",
                                conid
                            );
                            debug!(msg = %sub_msg, "IB: subscribing to market data");
                            if let Err(e) = write.send(Message::Text(sub_msg)).await {
                                error!(error = %e, "IB: failed to send subscription message");
                                break;
                            }
                        }

                        // Read loop.
                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = handle_ib_ws_message(
                                        &text,
                                        &book_tx,
                                        &trade_tx,
                                        &conid_symbol_map,
                                    )
                                    .await
                                    {
                                        warn!(error = %e, "IB: failed to handle WS message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "IB: failed to send pong");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "IB WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "IB WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("IB WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "failed to connect to IB Client Portal WebSocket");
                    }
                }

                // Reconnection logic.
                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "exceeded max reconnection attempts for IB, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "reconnecting to IB Client Portal WebSocket"
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("disconnecting IB market data connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, _depth: usize) -> MmResult<OrderBook> {
        let conid = self.resolve_conid(symbol)?;

        // IB snapshot returns at most 1-level depth (best bid/ask).
        let url = format!(
            "{}/md/snapshot?conids={}&fields=31,84,85,86,87,88",
            self.config.base_url, conid
        );

        let resp = self
            .http
            .get(&url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("REST snapshot request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("REST snapshot returned status {}", resp.status()),
            });
        }

        let entries: Vec<IbSnapshotEntry> =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("failed to parse IB snapshot response: {}", e),
            })?;

        let entry = entries.first().ok_or_else(|| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("no snapshot data returned for conid {}", conid),
        })?;

        let bids = parse_level(entry.bid_price.as_deref(), entry.bid_size.as_deref());
        let asks = parse_level(entry.ask_price.as_deref(), entry.ask_size.as_deref());

        Ok(OrderBook {
            exchange: Exchange::InteractiveBrokers,
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
// WebSocket message handling
// ---------------------------------------------------------------------------

/// Handle an incoming IB Client Portal WebSocket message.
///
/// IB WS messages for market data arrive as JSON objects keyed by conid,
/// with numeric field keys containing the updated values:
///
/// ```json
/// {
///   "conid": 265598,
///   "84": "150.25",
///   "85": "150.26",
///   "88": "100",
///   "87": "200",
///   "31": "150.25"
/// }
/// ```
async fn handle_ib_ws_message(
    text: &str,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
    conid_symbol_map: &HashMap<String, Symbol>,
) -> MmResult<()> {
    let msg: serde_json::Value =
        serde_json::from_str(text).map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("failed to parse IB WS message: {}", e),
        })?;

    // IB sends various message types. Market data updates contain a "conid" field.
    let conid = if let Some(c) = msg.get("conid") {
        // conid can be numeric or string
        if let Some(n) = c.as_u64() {
            n.to_string()
        } else if let Some(s) = c.as_str() {
            s.to_string()
        } else {
            // Not a market data message (e.g., system status, heartbeat).
            return Ok(());
        }
    } else {
        // Not a market data message.
        return Ok(());
    };

    let symbol = match conid_symbol_map.get(&conid) {
        Some(s) => s.clone(),
        None => {
            debug!(conid = %conid, "IB: received data for unknown conid, ignoring");
            return Ok(());
        }
    };

    // Extract bid/ask to build a 1-level order book.
    let bid_price = msg
        .get("84")
        .and_then(|v| v.as_str())
        .and_then(parse_ib_decimal);
    let ask_price = msg
        .get("85")
        .and_then(|v| v.as_str())
        .and_then(parse_ib_decimal);
    let bid_size = msg
        .get("88")
        .and_then(|v| v.as_str())
        .and_then(parse_ib_decimal);
    let ask_size = msg
        .get("87")
        .and_then(|v| v.as_str())
        .and_then(parse_ib_decimal);

    // If we have bid/ask data, send an order book update.
    if bid_price.is_some() || ask_price.is_some() {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        if let Some(bp) = bid_price {
            bids.push(PriceLevel {
                price: bp,
                quantity: bid_size.unwrap_or(Decimal::ZERO),
            });
        }

        if let Some(ap) = ask_price {
            asks.push(PriceLevel {
                price: ap,
                quantity: ask_size.unwrap_or(Decimal::ZERO),
            });
        }

        let order_book = OrderBook {
            exchange: Exchange::InteractiveBrokers,
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp: Utc::now(),
            sequence: 0,
        };

        book_tx.send(order_book).await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("failed to send order book update: {}", e),
        })?;
    }

    // If we have a last price (field 31), emit a synthetic trade event.
    // IB Client Portal does not provide a dedicated trade stream, so we
    // approximate with last-price updates.
    if let Some(last_price) = msg
        .get("31")
        .and_then(|v| v.as_str())
        .and_then(parse_ib_decimal)
    {
        let volume = msg
            .get("86")
            .and_then(|v| v.as_str())
            .and_then(parse_ib_decimal)
            .unwrap_or(Decimal::ZERO);

        // We don't know the true trade side from a last-price tick;
        // infer from bid/ask when available, default to Buy.
        let side = match (bid_price, ask_price) {
            (Some(bp), _) if last_price <= bp => Side::Sell,
            (_, Some(ap)) if last_price >= ap => Side::Buy,
            _ => Side::Buy,
        };

        let trade = Trade {
            exchange: Exchange::InteractiveBrokers,
            symbol,
            price: last_price,
            quantity: volume,
            side,
            timestamp: Utc::now(),
            trade_id: format!("ib-{}-{}", conid, Utc::now().timestamp_millis()),
        };

        trade_tx.send(trade).await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("failed to send trade: {}", e),
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse IB decimal strings, which may contain formatting characters
/// like commas or currency prefixes (e.g., "C150.25", "1,234.56").
fn parse_ib_decimal(s: &str) -> Option<Decimal> {
    // Strip any leading non-numeric characters (IB sometimes prefixes with
    // 'C' for close, 'H' for high, etc.) and remove commas.
    let cleaned: String = s
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    cleaned.parse::<Decimal>().ok()
}

/// Build a single-level price vector from optional price and size strings.
fn parse_level(price_str: Option<&str>, size_str: Option<&str>) -> Vec<PriceLevel> {
    if let Some(price) = price_str.and_then(parse_ib_decimal) {
        let quantity = size_str
            .and_then(parse_ib_decimal)
            .unwrap_or(Decimal::ZERO);
        vec![PriceLevel { price, quantity }]
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ib_decimal_basic() {
        assert_eq!(parse_ib_decimal("150.25"), Some(Decimal::new(15025, 2)));
    }

    #[test]
    fn parse_ib_decimal_with_prefix() {
        // IB sometimes prefixes values with letters like 'C' for close.
        assert_eq!(parse_ib_decimal("C150.25"), Some(Decimal::new(15025, 2)));
    }

    #[test]
    fn parse_ib_decimal_with_commas() {
        assert_eq!(
            parse_ib_decimal("1,234.56"),
            Some(Decimal::new(123456, 2))
        );
    }

    #[test]
    fn parse_ib_decimal_empty() {
        assert_eq!(parse_ib_decimal(""), None);
    }

    #[test]
    fn parse_level_both_present() {
        let levels = parse_level(Some("150.25"), Some("100"));
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, Decimal::new(15025, 2));
        assert_eq!(levels[0].quantity, Decimal::new(100, 0));
    }

    #[test]
    fn parse_level_no_price() {
        let levels = parse_level(None, Some("100"));
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_level_no_size_defaults_to_zero() {
        let levels = parse_level(Some("150.25"), None);
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].quantity, Decimal::ZERO);
    }

    #[test]
    fn resolve_conid_found() {
        let mut conid_map = HashMap::new();
        conid_map.insert("AAPL".to_string(), "265598".to_string());

        let config = IbConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            account_id: "U1234567".to_string(),
            conid_map,
        };

        let connector = IbConnector::new(config);
        let sym = Symbol::new("AAPL", "USD");
        assert_eq!(connector.resolve_conid(&sym).unwrap(), "265598");
    }

    #[test]
    fn resolve_conid_not_found() {
        let config = IbConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            account_id: "U1234567".to_string(),
            conid_map: HashMap::new(),
        };

        let connector = IbConnector::new(config);
        let sym = Symbol::new("AAPL", "USD");
        assert!(connector.resolve_conid(&sym).is_err());
    }

    #[test]
    fn symbol_for_conid_found() {
        let mut conid_map = HashMap::new();
        conid_map.insert("AAPL".to_string(), "265598".to_string());

        let config = IbConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            account_id: "U1234567".to_string(),
            conid_map,
        };

        let connector = IbConnector::new(config);
        let sym = connector.symbol_for_conid("265598").unwrap();
        assert_eq!(sym.base, "AAPL");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn symbol_for_conid_not_found() {
        let config = IbConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            account_id: "U1234567".to_string(),
            conid_map: HashMap::new(),
        };

        let connector = IbConnector::new(config);
        assert!(connector.symbol_for_conid("999999").is_none());
    }
}
