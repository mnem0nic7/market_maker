//! Uniswap V3 market data connector.
//!
//! Connects to an Ethereum node via JSON-RPC (HTTP and WebSocket) to stream
//! Uniswap V3 pool data as normalized order books and trades.
//!
//! # Approach
//!
//! - Subscribes to Uniswap V3 pool `Swap` events via `eth_subscribe` (logs)
//!   over the Ethereum node WebSocket endpoint.
//! - Polls pool state via `eth_call` to `slot0()` for current sqrtPriceX96
//!   and tick.
//! - Computes the real price from `sqrtPriceX96`:
//!   `price = (sqrtPriceX96 / 2^96)^2`, adjusted for token decimals.
//! - Builds a virtual order book from the current pool price with synthetic
//!   liquidity levels spaced at 10 bps intervals.
//!
//! # Wire format reference
//!
//! **Swap event** (topic `0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67`):
//! ```text
//! event Swap(
//!     address indexed sender,
//!     address indexed recipient,
//!     int256 amount0,
//!     int256 amount1,
//!     uint160 sqrtPriceX96,
//!     uint128 liquidity,
//!     int24 tick
//! )
//! ```
//!
//! **slot0** response (selector `0x3850c7bd`):
//! ```text
//! (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex,
//!  uint16 observationCardinality, uint16 observationCardinalityNext,
//!  uint8 feeProtocol, bool unlocked)
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::StreamExt;
use futures::SinkExt;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use mm_core::errors::{MmError, MmResult};
use mm_core::traits::MarketDataConnector;
use mm_core::types::{Exchange, OrderBook, PriceLevel, Side, Symbol, Trade};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Swap event topic hash (keccak256 of the Swap event signature).
const SWAP_EVENT_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

/// Function selector for `slot0()` on Uniswap V3 Pool contracts.
const SLOT0_SELECTOR: &str = "0x3850c7bd";

/// Spacing between synthetic order book levels (10 basis points = 0.1%).
const LEVEL_SPACING_BPS: f64 = 0.001;

/// Reconnect delay baseline in seconds (multiplied by attempt number).
const RECONNECT_DELAY_SECS: u64 = 5;

/// Maximum number of reconnection attempts before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Default poll interval for slot0 state (milliseconds).
const POLL_INTERVAL_MS: u64 = 2_000;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Uniswap V3 market data connector.
#[derive(Debug, Clone)]
pub struct UniswapConfig {
    /// Ethereum JSON-RPC HTTP URL (e.g., `https://eth-mainnet.g.alchemy.com/v2/KEY`).
    pub rpc_http_url: String,
    /// Ethereum JSON-RPC WebSocket URL (e.g., `wss://eth-mainnet.g.alchemy.com/v2/KEY`).
    pub rpc_ws_url: String,
    /// Chain ID (1 for mainnet).
    pub chain_id: u64,
    /// Mapping of canonical pair names to Uniswap V3 pool addresses.
    /// Example: `"WETH/USDC" -> "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"`.
    pub pool_addresses: HashMap<String, String>,
    /// Token decimal places. Example: `"WETH" -> 18`, `"USDC" -> 6`.
    pub token_decimals: HashMap<String, u8>,
}

// ---------------------------------------------------------------------------
// UniswapConnector
// ---------------------------------------------------------------------------

/// Uniswap V3 market data connector.
///
/// Spawns two background tasks:
/// 1. A WebSocket listener that subscribes to `Swap` events on pool contracts
///    and forwards them as normalized `Trade` objects.
/// 2. A polling task that periodically calls `slot0()` on each pool to build
///    a synthetic order book from the current sqrtPriceX96 and liquidity.
pub struct UniswapConnector {
    config: UniswapConfig,
    connected: Arc<AtomicBool>,
    /// Handle to the WebSocket event listener task.
    ws_handle: Option<tokio::task::JoinHandle<()>>,
    /// Handle to the slot0 polling task.
    poll_handle: Option<tokio::task::JoinHandle<()>>,
    /// HTTP client for JSON-RPC calls.
    http: reqwest::Client,
}

impl UniswapConnector {
    pub fn new(config: UniswapConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            ws_handle: None,
            poll_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl MarketDataConnector for UniswapConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Uniswap
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Uniswap connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        // Resolve pool addresses for requested symbols.
        let mut pool_map: Vec<(Symbol, String)> = Vec::new();
        for sym in symbols {
            let key = sym.canonical();
            if let Some(addr) = self.config.pool_addresses.get(&key) {
                pool_map.push((sym.clone(), addr.clone()));
            } else {
                warn!(
                    symbol = %key,
                    "no pool address configured for symbol, skipping"
                );
            }
        }

        if pool_map.is_empty() {
            return Err(MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: "no pool addresses found for requested symbols".into(),
            });
        }

        let connected = self.connected.clone();
        let ws_url = self.config.rpc_ws_url.clone();
        let token_decimals = self.config.token_decimals.clone();
        let pool_map_clone = pool_map.clone();

        // --- Task 1: WebSocket subscription to Swap events ---
        let ws_connected = connected.clone();
        let ws_handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _response)) => {
                        ws_connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("Uniswap: WebSocket connected to Ethereum node");

                        let (mut write, mut read) = ws_stream.split();

                        // Build subscription request for each pool's Swap events.
                        let pool_addresses: Vec<String> = pool_map_clone
                            .iter()
                            .map(|(_, addr)| addr.clone())
                            .collect();

                        let subscribe_msg = build_eth_subscribe_logs(&pool_addresses);
                        if let Err(e) = write.send(Message::Text(subscribe_msg)).await {
                            error!(error = %e, "Uniswap: failed to send eth_subscribe");
                            ws_connected.store(false, Ordering::SeqCst);
                            continue;
                        }

                        info!(
                            "Uniswap: subscribed to Swap events for {} pools",
                            pool_addresses.len()
                        );

                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = handle_ws_message(
                                        &text,
                                        &trade_tx,
                                        &pool_map_clone,
                                        &token_decimals,
                                    )
                                    .await
                                    {
                                        debug!(error = %e, "Uniswap: failed to handle WS message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "Uniswap: failed to send pong");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Uniswap: WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Uniswap: WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Uniswap: WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        ws_connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "Uniswap: failed to connect WebSocket");
                    }
                }

                // Reconnection logic.
                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "Uniswap: exceeded max reconnection attempts, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "Uniswap: reconnecting WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
            }

            ws_connected.store(false, Ordering::SeqCst);
        });

        // --- Task 2: Periodic slot0 polling for synthetic order book ---
        let rpc_http_url = self.config.rpc_http_url.clone();
        let poll_decimals = self.config.token_decimals.clone();
        let poll_connected = connected.clone();
        let poll_http = self.http.clone();

        let poll_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(POLL_INTERVAL_MS));

            loop {
                interval.tick().await;

                if !poll_connected.load(Ordering::SeqCst) {
                    // Wait for WS to connect first.
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    continue;
                }

                for (symbol, pool_addr) in &pool_map {
                    match fetch_slot0(&poll_http, &rpc_http_url, pool_addr).await {
                        Ok((sqrt_price_x96, _tick, liquidity)) => {
                            let base_decimals =
                                poll_decimals.get(&symbol.base).copied().unwrap_or(18);
                            let quote_decimals =
                                poll_decimals.get(&symbol.quote).copied().unwrap_or(6);

                            let price = sqrt_price_x96_to_price(
                                &sqrt_price_x96,
                                base_decimals,
                                quote_decimals,
                            );

                            if let Some(price) = price {
                                let depth = 20;
                                let book =
                                    build_virtual_order_book(symbol, price, liquidity, depth);

                                if let Err(e) = book_tx.send(book).await {
                                    warn!(
                                        error = %e,
                                        "Uniswap: failed to send order book update"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                pool = %pool_addr,
                                error = %e,
                                "Uniswap: slot0 poll failed"
                            );
                        }
                    }
                }
            }
        });

        self.ws_handle = Some(ws_handle);
        self.poll_handle = Some(poll_handle);

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Uniswap: disconnecting market data connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.ws_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.poll_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook> {
        let key = symbol.canonical();
        let pool_addr = self
            .config
            .pool_addresses
            .get(&key)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: format!("no pool address configured for {}", key),
            })?;

        let (sqrt_price_x96, _tick, liquidity) =
            fetch_slot0(&self.http, &self.config.rpc_http_url, pool_addr).await?;

        let base_decimals = self
            .config
            .token_decimals
            .get(&symbol.base)
            .copied()
            .unwrap_or(18);
        let quote_decimals = self
            .config
            .token_decimals
            .get(&symbol.quote)
            .copied()
            .unwrap_or(6);

        let price = sqrt_price_x96_to_price(&sqrt_price_x96, base_decimals, quote_decimals)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: "failed to compute price from sqrtPriceX96".into(),
            })?;

        Ok(build_virtual_order_book(symbol, price, liquidity, depth))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC helpers
// ---------------------------------------------------------------------------

/// Build an `eth_subscribe` JSON-RPC request for Swap event logs on the given
/// pool addresses.
fn build_eth_subscribe_logs(pool_addresses: &[String]) -> String {
    let _addresses_json: Vec<String> = pool_addresses
        .iter()
        .map(|a| format!("\"{}\"", a))
        .collect();

    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": serde_json::Value::Array(
                    pool_addresses.iter()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .collect()
                ),
                "topics": [SWAP_EVENT_TOPIC]
            }
        ]
    })
    .to_string()
}

/// Perform an `eth_call` to the pool contract's `slot0()` function.
///
/// Returns `(sqrtPriceX96, tick, liquidity)` as raw U256/I256 values
/// represented as `(f64, i32, f64)` for simplicity. The sqrtPriceX96 is kept
/// as a 256-bit hex string for precise price computation.
async fn fetch_slot0(
    http: &reqwest::Client,
    rpc_url: &str,
    pool_address: &str,
) -> MmResult<(String, i32, f64)> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": pool_address,
                "data": SLOT0_SELECTOR
            },
            "latest"
        ]
    });

    let resp = http
        .post(rpc_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!("slot0 eth_call request failed: {}", e),
        })?;

    let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Uniswap,
        message: format!("slot0 response parse failed: {}", e),
    })?;

    if let Some(err) = body.get("error") {
        return Err(MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!("slot0 RPC error: {}", err),
        });
    }

    let result = body["result"].as_str().ok_or_else(|| MmError::Exchange {
        exchange: Exchange::Uniswap,
        message: "slot0 result is not a string".into(),
    })?;

    // slot0 returns 7 ABI-encoded words (7 * 32 bytes = 448 hex chars after 0x).
    // Word 0: sqrtPriceX96 (uint160, packed in 32 bytes)
    // Word 1: tick (int24, packed in 32 bytes)
    // Word 2: observationIndex (uint16)
    // Word 3: observationCardinality (uint16)
    // Word 4: observationCardinalityNext (uint16)
    // Word 5: feeProtocol (uint8)
    // Word 6: unlocked (bool)
    let hex_data = result.strip_prefix("0x").unwrap_or(result);
    if hex_data.len() < 128 {
        return Err(MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!(
                "slot0 response too short: expected >= 128 hex chars, got {}",
                hex_data.len()
            ),
        });
    }

    // Word 0 (bytes 0..64): sqrtPriceX96 as hex.
    let sqrt_price_hex = &hex_data[0..64];

    // Word 1 (bytes 64..128): tick as int24 (sign-extended in 32 bytes).
    let tick_hex = &hex_data[64..128];
    let tick = parse_int256_as_i32(tick_hex);

    // We pass sqrtPriceX96 as a hex string for precise computation.
    // Extract liquidity from the pool via a separate call if needed, or
    // estimate from sqrtPriceX96 magnitude. For now we use a default.
    let liquidity = 1_000_000.0_f64; // Placeholder; real impl would call liquidity()

    Ok((sqrt_price_hex.to_string(), tick, liquidity))
}

/// Parse a 32-byte hex-encoded int256 value as an i32. Used for the `tick`
/// field which is actually an int24.
fn parse_int256_as_i32(hex: &str) -> i32 {
    // The tick is stored as a signed 256-bit integer. For int24, the value
    // fits in the last 3 bytes. Check the sign bit (first nibble >= 8).
    let is_negative = hex
        .chars()
        .next()
        .map(|c| matches!(c, '8'..='9' | 'a'..='f' | 'A'..='F'))
        .unwrap_or(false);

    if is_negative {
        // Two's complement: parse last 8 hex chars as u32, interpret as i32.
        let last_8 = &hex[hex.len().saturating_sub(8)..];
        let val = u32::from_str_radix(last_8, 16).unwrap_or(0);
        val as i32
    } else {
        let last_8 = &hex[hex.len().saturating_sub(8)..];
        let val = u32::from_str_radix(last_8, 16).unwrap_or(0);
        val as i32
    }
}

// ---------------------------------------------------------------------------
// Price computation
// ---------------------------------------------------------------------------

/// Convert `sqrtPriceX96` (a uint160 hex string) to a human-readable price,
/// adjusted for token decimals.
///
/// Formula:
///   raw_price = (sqrtPriceX96 / 2^96) ^ 2
///   price = raw_price * 10^(base_decimals - quote_decimals)
///
/// The price represents "how much quote token per 1 base token" (e.g., USDC
/// per WETH in a WETH/USDC pool).
///
/// We use `f64` intermediate arithmetic for the power-of-two division since
/// sqrtPriceX96 can be up to 160 bits. The final result is converted to
/// `Decimal` for downstream use.
fn sqrt_price_x96_to_price(
    sqrt_price_hex: &str,
    base_decimals: u8,
    quote_decimals: u8,
) -> Option<Decimal> {
    // Parse the hex string as a big unsigned integer. Since Rust's u128 is only
    // 128 bits and sqrtPriceX96 is up to 160 bits, we use f64 for the division.
    let sqrt_price_f64 = hex_to_f64(sqrt_price_hex)?;
    let two_pow_96: f64 = 2.0_f64.powi(96);

    let ratio = sqrt_price_f64 / two_pow_96;
    let raw_price = ratio * ratio;

    // Adjust for token decimals: token0 is base, token1 is quote in Uniswap convention.
    // price = raw_price * 10^(base_decimals - quote_decimals)
    let decimal_adjustment = 10.0_f64.powi(base_decimals as i32 - quote_decimals as i32);
    let adjusted_price = raw_price * decimal_adjustment;

    Decimal::from_f64(adjusted_price)
}

/// Parse a hexadecimal string (without 0x prefix) as an f64.
/// This loses precision for very large values but is sufficient for price
/// computation where we only need ~15 significant digits.
fn hex_to_f64(hex: &str) -> Option<f64> {
    let hex = hex.trim_start_matches('0');
    if hex.is_empty() {
        return Some(0.0);
    }

    // Process 8 hex chars (32 bits) at a time, accumulating into f64.
    let mut result: f64 = 0.0;
    for chunk_start in (0..hex.len()).step_by(8) {
        let chunk_end = (chunk_start + 8).min(hex.len());
        let chunk = &hex[chunk_start..chunk_end];
        let val = u32::from_str_radix(chunk, 16).ok()? as f64;

        let remaining_hex_chars = hex.len() - chunk_end;
        let shift = remaining_hex_chars as f64 * 4.0; // Each hex char = 4 bits.
        result += val * 2.0_f64.powf(shift);
    }

    Some(result)
}

// ---------------------------------------------------------------------------
// Virtual order book construction
// ---------------------------------------------------------------------------

/// Build a synthetic order book from the current pool price.
///
/// Uniswap V3 is a CFMM (Concentrated Liquidity Market Maker), not a CLOB,
/// so there is no traditional order book. We synthesize one by placing levels
/// at fixed percentage intervals from the current price. The quantity at each
/// level is derived from the pool's overall liquidity estimate.
///
/// - **Bids**: `P * (1 - i * 0.001)` for `i` in `1..=depth`
/// - **Asks**: `P * (1 + i * 0.001)` for `i` in `1..=depth`
fn build_virtual_order_book(
    symbol: &Symbol,
    price: Decimal,
    liquidity: f64,
    depth: usize,
) -> OrderBook {
    let base_quantity = Decimal::from_f64(liquidity / 1_000_000.0).unwrap_or(Decimal::ONE);
    let one = Decimal::ONE;
    let spacing = Decimal::from_f64(LEVEL_SPACING_BPS).unwrap_or_else(|| {
        Decimal::new(1, 3) // 0.001
    });

    let mut bids = Vec::with_capacity(depth);
    let mut asks = Vec::with_capacity(depth);

    for i in 1..=depth {
        let i_dec = Decimal::from(i as u64);
        let offset = spacing * i_dec;

        // Bid: price * (1 - offset), quantity decreases with distance.
        let bid_price = price * (one - offset);
        let bid_qty = base_quantity / i_dec; // Less liquidity farther from mid.
        if bid_price > Decimal::ZERO {
            bids.push(PriceLevel {
                price: bid_price,
                quantity: bid_qty,
            });
        }

        // Ask: price * (1 + offset).
        let ask_price = price * (one + offset);
        let ask_qty = base_quantity / i_dec;
        asks.push(PriceLevel {
            price: ask_price,
            quantity: ask_qty,
        });
    }

    OrderBook {
        exchange: Exchange::Uniswap,
        symbol: symbol.clone(),
        bids,
        asks,
        timestamp: Utc::now(),
        sequence: 0, // No sequence numbers on AMMs.
    }
}

// ---------------------------------------------------------------------------
// WebSocket message handling
// ---------------------------------------------------------------------------

/// Handle an incoming WebSocket message from the Ethereum node.
///
/// Messages may be:
/// - Subscription confirmations (`result` field with subscription ID)
/// - Log notifications (`method: "eth_subscription"`) containing Swap events
async fn handle_ws_message(
    text: &str,
    trade_tx: &mpsc::Sender<Trade>,
    pool_map: &[(Symbol, String)],
    token_decimals: &HashMap<String, u8>,
) -> MmResult<()> {
    let msg: serde_json::Value = serde_json::from_str(text).map_err(|e| MmError::Exchange {
        exchange: Exchange::Uniswap,
        message: format!("failed to parse WS JSON: {}", e),
    })?;

    // Check if this is a subscription confirmation.
    if msg.get("result").is_some() && msg.get("method").is_none() {
        debug!("Uniswap: subscription confirmed");
        return Ok(());
    }

    // Check for subscription notifications.
    let method = msg["method"].as_str().unwrap_or("");
    if method != "eth_subscription" {
        return Ok(());
    }

    let params = &msg["params"];
    let log = &params["result"];

    // Verify this is a Swap event by checking topics[0].
    let topics = log["topics"].as_array();
    if topics.is_none() || topics.unwrap().is_empty() {
        return Ok(());
    }
    let topic0 = topics.unwrap()[0].as_str().unwrap_or("");
    if topic0 != SWAP_EVENT_TOPIC {
        return Ok(());
    }

    // Extract the pool address from the log to identify the symbol.
    let log_address = log["address"].as_str().unwrap_or("").to_lowercase();

    let symbol = pool_map
        .iter()
        .find(|(_, addr)| addr.to_lowercase() == log_address)
        .map(|(sym, _)| sym.clone());

    let symbol = match symbol {
        Some(s) => s,
        None => {
            debug!(address = %log_address, "Uniswap: unknown pool address in Swap event");
            return Ok(());
        }
    };

    // Parse the Swap event data.
    // Data layout (non-indexed params, concatenated 32-byte words):
    //   word 0: int256 amount0
    //   word 1: int256 amount1
    //   word 2: uint160 sqrtPriceX96
    //   word 3: uint128 liquidity
    //   word 4: int24 tick
    let data = log["data"].as_str().unwrap_or("");
    let hex_data = data.strip_prefix("0x").unwrap_or(data);

    if hex_data.len() < 320 {
        return Err(MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!(
                "Swap event data too short: expected >= 320 hex chars, got {}",
                hex_data.len()
            ),
        });
    }

    // Parse amount0 (word 0: bytes 0..64) — int256, signed.
    let amount0_hex = &hex_data[0..64];
    let amount0 = parse_signed_hex_to_f64(amount0_hex);

    // Parse amount1 (word 1: bytes 64..128) — int256, signed.
    let amount1_hex = &hex_data[64..128];
    let _amount1 = parse_signed_hex_to_f64(amount1_hex);

    // Parse sqrtPriceX96 (word 2: bytes 128..192) — uint160.
    let sqrt_price_hex = &hex_data[128..192];

    // Determine trade direction: if amount0 is negative, the pool sent token0
    // out (buyer bought token0 = base), so this is a Buy. If amount0 is
    // positive, the pool received token0 (seller sold token0 = base), so Sell.
    let side = if amount0 < 0.0 { Side::Buy } else { Side::Sell };

    // Compute price from sqrtPriceX96.
    let base_decimals = token_decimals.get(&symbol.base).copied().unwrap_or(18);
    let quote_decimals = token_decimals.get(&symbol.quote).copied().unwrap_or(6);

    let price = sqrt_price_x96_to_price(sqrt_price_hex, base_decimals, quote_decimals)
        .unwrap_or(Decimal::ZERO);

    // Compute trade quantity (absolute value of amount0, adjusted for decimals).
    let quantity_raw = amount0.abs();
    let quantity_adjusted = quantity_raw / 10.0_f64.powi(base_decimals as i32);
    let quantity = Decimal::from_f64(quantity_adjusted).unwrap_or(Decimal::ZERO);

    // Use the transaction hash as the trade ID.
    let tx_hash = log["transactionHash"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let log_index = log["logIndex"].as_str().unwrap_or("0").to_string();
    let trade_id = format!("{}-{}", tx_hash, log_index);

    let trade = Trade {
        exchange: Exchange::Uniswap,
        symbol,
        price,
        quantity,
        side,
        timestamp: Utc::now(),
        trade_id,
    };

    trade_tx.send(trade).await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Uniswap,
        message: format!("failed to send trade: {}", e),
    })?;

    Ok(())
}

/// Parse a 64-char hex string representing a signed 256-bit integer to f64.
fn parse_signed_hex_to_f64(hex: &str) -> f64 {
    let is_negative = hex
        .chars()
        .next()
        .map(|c| matches!(c, '8'..='9' | 'a'..='f' | 'A'..='F'))
        .unwrap_or(false);

    if is_negative {
        // Two's complement: invert all bits and add 1, then negate.
        let inverted: String = hex
            .chars()
            .map(|c| {
                let nibble = u8::from_str_radix(&c.to_string(), 16).unwrap_or(0);
                let inv = !nibble & 0x0F;
                format!("{:x}", inv)
            })
            .collect();
        let abs_val = hex_to_f64(&inverted).unwrap_or(0.0) + 1.0;
        -abs_val
    } else {
        hex_to_f64(hex).unwrap_or(0.0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_to_f64_zero() {
        assert_eq!(hex_to_f64("0000000000000000"), Some(0.0));
    }

    #[test]
    fn test_hex_to_f64_one() {
        let result = hex_to_f64("1").unwrap();
        assert!((result - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_hex_to_f64_large() {
        // 0x100 = 256
        let result = hex_to_f64("100").unwrap();
        assert!((result - 256.0).abs() < 1e-10);
    }

    #[test]
    fn test_parse_int256_positive_tick() {
        // tick = 200 => last 8 hex chars = "000000c8"
        let hex = "00000000000000000000000000000000000000000000000000000000000000c8";
        let tick = parse_int256_as_i32(hex);
        assert_eq!(tick, 200);
    }

    #[test]
    fn test_parse_int256_negative_tick() {
        // tick = -200 => two's complement in 256 bits, last 8 hex chars: ffffff38
        let hex = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff38";
        let tick = parse_int256_as_i32(hex);
        assert_eq!(tick, -200);
    }

    #[test]
    fn test_sqrt_price_x96_to_price() {
        // For WETH/USDC (base=18 decimals, quote=6 decimals):
        // A sqrtPriceX96 of roughly 1.58e27 corresponds to ~$2000 ETH.
        // sqrtPriceX96 = sqrt(2000 / 10^(18-6)) * 2^96 = sqrt(2e-9) * 2^96
        // sqrt(2e-9) ≈ 4.4721e-5
        // 4.4721e-5 * 2^96 ≈ 3.5416e24
        // In hex this is roughly "2e76a41b2d3e3800000000000000" but we test
        // with a known value from mainnet.

        // Use a simple test: sqrtPriceX96 = 2^96 means ratio = 1.0,
        // raw_price = 1.0, adjusted = 10^(18-6) = 1e12
        let two_pow_96_hex = "0000000000000000000000000000000000000001000000000000000000000000";
        let price = sqrt_price_x96_to_price(two_pow_96_hex, 18, 6);
        assert!(price.is_some());
        let p = price.unwrap();
        // raw_price = 1.0, decimal_adjustment = 10^12
        // So price should be ~1e12
        let expected = Decimal::from_f64(1e12).unwrap();
        let diff = (p - expected).abs();
        // Allow some floating point tolerance.
        assert!(
            diff < Decimal::from(1_000_000),
            "price {} not close to expected {}",
            p,
            expected
        );
    }

    #[test]
    fn test_build_virtual_order_book() {
        let symbol = Symbol::new("WETH", "USDC");
        let price = Decimal::from(2000);
        let book = build_virtual_order_book(&symbol, price, 1_000_000.0, 5);

        assert_eq!(book.bids.len(), 5);
        assert_eq!(book.asks.len(), 5);

        // Bids should be below the price.
        for level in &book.bids {
            assert!(
                level.price < price,
                "bid {} >= price {}",
                level.price,
                price
            );
            assert!(level.quantity > Decimal::ZERO);
        }

        // Asks should be above the price.
        for level in &book.asks {
            assert!(
                level.price > price,
                "ask {} <= price {}",
                level.price,
                price
            );
            assert!(level.quantity > Decimal::ZERO);
        }

        // First bid should be closest to mid.
        assert!(book.bids[0].price > book.bids[1].price);
        // First ask should be closest to mid.
        assert!(book.asks[0].price < book.asks[1].price);
    }
}
