//! Raydium (Solana DEX) market data connector.
//!
//! Connects to Solana JSON-RPC to read Raydium AMM pool accounts and derives
//! virtual order book data from the constant-product AMM curve.
//!
//! # Approach
//!
//! Unlike traditional CEX connectors that receive discrete order book updates,
//! the Raydium connector:
//!
//! 1. Subscribes to AMM pool account changes via `accountSubscribe` WebSocket
//!    RPC method on the Solana validator.
//! 2. Parses raw pool account data to extract token reserves (base and quote).
//! 3. Computes the current spot price from the constant-product formula:
//!    `price = reserve_quote / reserve_base`
//! 4. Builds a synthetic order book by computing price impact at multiple
//!    size levels along the AMM bonding curve.
//!
//! # Raydium AMM V4 account layout
//!
//! The pool account is a Raydium-specific Solana account whose data layout
//! contains (among other fields):
//!
//! | Offset | Length | Field              |
//! |--------|--------|--------------------|
//! | 0      | 8      | Discriminator      |
//! | 64     | 8      | pool_coin_amount   |
//! | 72     | 8      | pool_pc_amount     |
//!
//! (Offsets are approximate for V4; real offsets depend on the Raydium IDL
//! version. The constants `RESERVE_BASE_OFFSET` and `RESERVE_QUOTE_OFFSET`
//! should be verified against the deployed program.)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::StreamExt;
use futures::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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

const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Number of synthetic order book levels to generate on each side.
const DEFAULT_BOOK_DEPTH: usize = 10;

/// Base trade size increment (in base token units) for computing AMM curve levels.
/// Each successive level uses `level_index * LEVEL_SIZE_INCREMENT` as the trade size.
const LEVEL_SIZE_INCREMENT: &str = "1.0";

// Raydium AMM V4 account data offsets (bytes) for token reserves.
// These must match the on-chain program layout. The discriminator occupies the
// first 8 bytes; status, nonce, and various pubkeys precede the reserves.
//
// NOTE: These offsets are for the standard Raydium AMM V4 layout. Different
// pool versions or forks may use different offsets. Verify against the deployed
// IDL before production use.
const RESERVE_BASE_OFFSET: usize = 64;
const RESERVE_QUOTE_OFFSET: usize = 72;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Raydium market data connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumConfig {
    /// Solana JSON-RPC HTTP endpoint.
    /// e.g., "https://api.mainnet-beta.solana.com"
    pub rpc_http_url: String,

    /// Solana JSON-RPC WebSocket endpoint.
    /// e.g., "wss://api.mainnet-beta.solana.com"
    pub rpc_ws_url: String,

    /// Mapping from canonical symbol string ("SOL/USDC") to the Raydium AMM
    /// pool account address (base58 encoded Solana pubkey).
    pub pool_addresses: HashMap<String, String>,

    /// Mapping from token ticker ("SOL", "USDC") to the number of decimals
    /// the SPL token uses on-chain.
    pub token_decimals: HashMap<String, u8>,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Parsed pool reserves from the on-chain account data.
#[derive(Debug, Clone)]
struct PoolReserves {
    /// Raw base token reserve (in smallest units, i.e. lamports for SOL).
    base_raw: u64,
    /// Raw quote token reserve (in smallest units).
    _quote_raw: u64,
    /// Human-readable base reserve (adjusted for decimals).
    base_amount: Decimal,
    /// Human-readable quote reserve (adjusted for decimals).
    quote_amount: Decimal,
}

/// Per-pool tracking state.
#[derive(Debug, Clone)]
struct PoolState {
    symbol: Symbol,
    pool_address: String,
    base_decimals: u8,
    quote_decimals: u8,
    reserves: Option<PoolReserves>,
}

// ---------------------------------------------------------------------------
// RaydiumConnector
// ---------------------------------------------------------------------------

/// Raydium AMM market data connector.
///
/// Subscribes to Solana account changes for configured Raydium pool accounts
/// and derives synthetic order books from the constant-product AMM curve.
pub struct RaydiumConnector {
    config: RaydiumConfig,
    connected: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    http: reqwest::Client,
}

impl RaydiumConnector {
    pub fn new(config: RaydiumConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl MarketDataConnector for RaydiumConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Raydium
    }

    async fn connect(
        &mut self,
        symbols: &[Symbol],
        book_tx: mpsc::Sender<OrderBook>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> MmResult<()> {
        if self.connected.load(Ordering::SeqCst) {
            warn!("Raydium connector already connected, disconnecting first");
            self.disconnect().await?;
        }

        // Build pool state for each requested symbol.
        let mut pool_states: Vec<PoolState> = Vec::new();
        for symbol in symbols {
            let canonical = symbol.canonical();
            let pool_address = self
                .config
                .pool_addresses
                .get(&canonical)
                .ok_or_else(|| MmError::Exchange {
                    exchange: Exchange::Raydium,
                    message: format!("no pool address configured for {}", canonical),
                })?
                .clone();

            let base_decimals = *self
                .config
                .token_decimals
                .get(&symbol.base)
                .ok_or_else(|| MmError::Exchange {
                    exchange: Exchange::Raydium,
                    message: format!("no decimals configured for token {}", symbol.base),
                })?;

            let quote_decimals =
                *self
                    .config
                    .token_decimals
                    .get(&symbol.quote)
                    .ok_or_else(|| MmError::Exchange {
                        exchange: Exchange::Raydium,
                        message: format!("no decimals configured for token {}", symbol.quote),
                    })?;

            pool_states.push(PoolState {
                symbol: symbol.clone(),
                pool_address,
                base_decimals,
                quote_decimals,
                reserves: None,
            });
        }

        let ws_url = self.config.rpc_ws_url.clone();
        let connected = self.connected.clone();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempts: u32 = 0;

            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _response)) => {
                        connected.store(true, Ordering::SeqCst);
                        reconnect_attempts = 0;
                        info!("Raydium: Solana WebSocket connected");

                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to accountSubscribe for each pool address.
                        // Maps subscription_id -> pool_state index.
                        let mut sub_id_to_pool: HashMap<u64, usize> = HashMap::new();

                        for (idx, pool) in pool_states.iter().enumerate() {
                            let subscribe_msg = serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": idx + 1,
                                "method": "accountSubscribe",
                                "params": [
                                    pool.pool_address,
                                    {
                                        "encoding": "base64",
                                        "commitment": "confirmed"
                                    }
                                ]
                            });

                            let msg_text = subscribe_msg.to_string();
                            if let Err(e) = write.send(Message::Text(msg_text)).await {
                                error!(
                                    error = %e,
                                    pool = %pool.pool_address,
                                    "Raydium: failed to send accountSubscribe"
                                );
                                break;
                            }
                            info!(
                                pool = %pool.pool_address,
                                symbol = %pool.symbol,
                                "Raydium: subscribed to pool account"
                            );
                        }

                        // Sequence counter for order book snapshots.
                        let mut sequence: u64 = 0;

                        loop {
                            match read.next().await {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = handle_solana_ws_message(
                                        &text,
                                        &mut pool_states,
                                        &mut sub_id_to_pool,
                                        &book_tx,
                                        &trade_tx,
                                        &mut sequence,
                                    )
                                    .await
                                    {
                                        warn!(error = %e, "Raydium: failed to handle WS message");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    if let Err(e) = write.send(Message::Pong(data)).await {
                                        warn!(error = %e, "Raydium: failed to send pong");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    info!(?frame, "Raydium: Solana WebSocket closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Raydium: Solana WebSocket error");
                                    break;
                                }
                                None => {
                                    info!("Raydium: Solana WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        connected.store(false, Ordering::SeqCst);
                    }
                    Err(e) => {
                        error!(error = %e, "Raydium: failed to connect to Solana WebSocket");
                    }
                }

                // Reconnection logic.
                reconnect_attempts += 1;
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    error!(
                        attempts = reconnect_attempts,
                        "Raydium: exceeded max reconnection attempts, giving up"
                    );
                    break;
                }

                let delay = RECONNECT_DELAY_SECS * reconnect_attempts as u64;
                warn!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "Raydium: reconnecting to Solana WebSocket"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
            }

            connected.store(false, Ordering::SeqCst);
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Raydium: disconnecting");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn fetch_order_book(&self, symbol: &Symbol, depth: usize) -> MmResult<OrderBook> {
        let canonical = symbol.canonical();
        let pool_address =
            self.config
                .pool_addresses
                .get(&canonical)
                .ok_or_else(|| MmError::Exchange {
                    exchange: Exchange::Raydium,
                    message: format!("no pool address configured for {}", canonical),
                })?;

        let base_decimals = *self
            .config
            .token_decimals
            .get(&symbol.base)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("no decimals configured for token {}", symbol.base),
            })?;

        let quote_decimals = *self
            .config
            .token_decimals
            .get(&symbol.quote)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("no decimals configured for token {}", symbol.quote),
            })?;

        // Fetch the pool account data via getAccountInfo RPC.
        let account_data =
            fetch_account_data(&self.http, &self.config.rpc_http_url, pool_address).await?;
        let reserves = parse_pool_reserves(&account_data, base_decimals, quote_decimals)?;

        let book = build_virtual_order_book(
            symbol, &reserves, depth, 0, // sequence
        );

        Ok(book)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Solana WebSocket message handling
// ---------------------------------------------------------------------------

/// Solana WebSocket subscription confirmation response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RpcSubscriptionResponse {
    jsonrpc: String,
    result: Option<u64>,
    id: Option<u64>,
}

/// Solana WebSocket account notification.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RpcAccountNotification {
    jsonrpc: String,
    method: Option<String>,
    params: Option<AccountNotificationParams>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AccountNotificationParams {
    result: AccountNotificationResult,
    subscription: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AccountNotificationResult {
    context: RpcContext,
    value: AccountValue,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RpcContext {
    slot: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AccountValue {
    /// Account data as [base64_string, "base64"] tuple.
    data: Vec<String>,
    #[serde(rename = "lamports")]
    lamports: u64,
    owner: String,
    executable: bool,
    #[serde(rename = "rentEpoch")]
    rent_epoch: u64,
}

async fn handle_solana_ws_message(
    text: &str,
    pool_states: &mut [PoolState],
    sub_id_to_pool: &mut HashMap<u64, usize>,
    book_tx: &mpsc::Sender<OrderBook>,
    trade_tx: &mpsc::Sender<Trade>,
    sequence: &mut u64,
) -> Result<(), MmError> {
    let msg: serde_json::Value = serde_json::from_str(text).map_err(|e| MmError::Exchange {
        exchange: Exchange::Raydium,
        message: format!("failed to parse Solana WS message: {}", e),
    })?;

    // Check if this is a subscription confirmation (has "result" and "id").
    if let (Some(result), Some(id)) = (msg.get("result"), msg.get("id")) {
        if let (Some(sub_id), Some(req_id)) = (result.as_u64(), id.as_u64()) {
            // Map subscription ID to the pool index (req_id is 1-based).
            let pool_idx = (req_id as usize).saturating_sub(1);
            if pool_idx < pool_states.len() {
                sub_id_to_pool.insert(sub_id, pool_idx);
                info!(
                    subscription_id = sub_id,
                    pool_idx = pool_idx,
                    symbol = %pool_states[pool_idx].symbol,
                    "Raydium: subscription confirmed"
                );
            }
        }
        return Ok(());
    }

    // Check if this is an account notification.
    if msg.get("method").and_then(|v| v.as_str()) == Some("accountNotification") {
        let notification: RpcAccountNotification =
            serde_json::from_value(serde_json::Value::Object(
                msg.as_object()
                    .ok_or_else(|| MmError::Exchange {
                        exchange: Exchange::Raydium,
                        message: "expected JSON object for notification".into(),
                    })?
                    .clone(),
            ))
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("failed to parse account notification: {}", e),
            })?;

        if let Some(params) = notification.params {
            let sub_id = params.subscription;
            if let Some(&pool_idx) = sub_id_to_pool.get(&sub_id) {
                let pool = &mut pool_states[pool_idx];

                // Decode base64 account data.
                let data_b64 =
                    params
                        .result
                        .value
                        .data
                        .first()
                        .ok_or_else(|| MmError::Exchange {
                            exchange: Exchange::Raydium,
                            message: "empty account data in notification".into(),
                        })?;

                let account_data = decode_base64(data_b64)?;
                let reserves =
                    parse_pool_reserves(&account_data, pool.base_decimals, pool.quote_decimals)?;

                // Emit a synthetic trade if we had previous reserves (price changed).
                if let Some(prev) = &pool.reserves {
                    let prev_price = prev.quote_amount / prev.base_amount;
                    let new_price = reserves.quote_amount / reserves.base_amount;

                    if prev_price != new_price {
                        // Determine trade side from reserve changes.
                        // If base reserves decreased, someone bought base (Buy).
                        // If base reserves increased, someone sold base (Sell).
                        let side = if reserves.base_raw < prev.base_raw {
                            Side::Buy
                        } else {
                            Side::Sell
                        };

                        // Estimate trade quantity from the change in base reserves.
                        let base_diff = if side == Side::Buy {
                            prev.base_amount - reserves.base_amount
                        } else {
                            reserves.base_amount - prev.base_amount
                        };

                        let trade = Trade {
                            exchange: Exchange::Raydium,
                            symbol: pool.symbol.clone(),
                            price: new_price,
                            quantity: base_diff.abs(),
                            side,
                            timestamp: Utc::now(),
                            trade_id: format!(
                                "raydium-{}-{}",
                                pool.pool_address,
                                Utc::now().timestamp_millis()
                            ),
                        };

                        if let Err(e) = trade_tx.send(trade).await {
                            warn!(error = %e, "Raydium: failed to send trade");
                        }
                    }
                }

                // Build and send the virtual order book.
                *sequence += 1;
                let book = build_virtual_order_book(
                    &pool.symbol,
                    &reserves,
                    DEFAULT_BOOK_DEPTH,
                    *sequence,
                );

                if let Err(e) = book_tx.send(book).await {
                    warn!(error = %e, "Raydium: failed to send order book");
                }

                pool.reserves = Some(reserves);
            } else {
                debug!(
                    subscription_id = sub_id,
                    "Raydium: received notification for unknown subscription"
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// AMM curve math: virtual order book from constant-product reserves
// ---------------------------------------------------------------------------

/// Build a synthetic order book from AMM pool reserves.
///
/// For a constant-product AMM with reserves `(R_base, R_quote)`:
///
/// - **Spot price** = `R_quote / R_base`
///
/// - **Ask levels** (cost to buy `dx` of base):
///   `dy = R_quote - (R_base * R_quote) / (R_base + dx)`
///   Effective price = `dy / dx`
///
/// - **Bid levels** (received for selling `dx` of base):
///   `dy = (R_base * R_quote) / (R_base - dx) - R_quote`
///   Wait — more precisely for selling base:
///   Output quote = `R_quote - (R_base * R_quote) / (R_base + dx)`
///   ... but the seller adds base, so:
///   `output_quote = R_quote * dx / (R_base + dx)`
///   Effective price = `output_quote / dx`
fn build_virtual_order_book(
    symbol: &Symbol,
    reserves: &PoolReserves,
    depth: usize,
    sequence: u64,
) -> OrderBook {
    let r_base = reserves.base_amount;
    let r_quote = reserves.quote_amount;
    let k = r_base * r_quote;

    let level_increment: Decimal = LEVEL_SIZE_INCREMENT
        .parse()
        .expect("LEVEL_SIZE_INCREMENT must be a valid Decimal");

    let mut asks: Vec<PriceLevel> = Vec::with_capacity(depth);
    let mut bids: Vec<PriceLevel> = Vec::with_capacity(depth);

    for i in 1..=depth {
        let dx = level_increment * Decimal::from(i as u64);

        // Ask side: buying `dx` base tokens from the pool.
        // Buying removes base from the pool: R_base decreases, R_quote increases.
        // New R_base = R_base - dx, New R_quote = k / (R_base - dx)
        // Quote cost = k / (R_base - dx) - R_quote
        if dx < r_base {
            let new_r_base_ask = r_base - dx;
            if new_r_base_ask > Decimal::ZERO {
                let new_r_quote_ask = k / new_r_base_ask;
                let quote_cost = new_r_quote_ask - r_quote;
                if quote_cost > Decimal::ZERO && dx > Decimal::ZERO {
                    let effective_price = quote_cost / dx;
                    asks.push(PriceLevel {
                        price: effective_price,
                        quantity: dx,
                    });
                }
            }
        }

        // Bid side: selling `dx` base tokens to the pool.
        // Selling adds base to the pool: R_base increases, R_quote decreases.
        // New R_base = R_base + dx, New R_quote = k / (R_base + dx)
        // Quote received = R_quote - k / (R_base + dx)
        let new_r_base_bid = r_base + dx;
        if new_r_base_bid > Decimal::ZERO {
            let new_r_quote_bid = k / new_r_base_bid;
            let quote_received = r_quote - new_r_quote_bid;
            if quote_received > Decimal::ZERO && dx > Decimal::ZERO {
                let effective_price = quote_received / dx;
                bids.push(PriceLevel {
                    price: effective_price,
                    quantity: dx,
                });
            }
        }
    }

    // Bids should be sorted descending by price (best bid first).
    bids.sort_by(|a, b| b.price.cmp(&a.price));
    // Asks should be sorted ascending by price (best ask first).
    asks.sort_by(|a, b| a.price.cmp(&b.price));

    OrderBook {
        exchange: Exchange::Raydium,
        symbol: symbol.clone(),
        bids,
        asks,
        timestamp: Utc::now(),
        sequence,
    }
}

// ---------------------------------------------------------------------------
// Solana JSON-RPC helpers
// ---------------------------------------------------------------------------

/// Fetch account data via `getAccountInfo` JSON-RPC call.
async fn fetch_account_data(
    http: &reqwest::Client,
    rpc_url: &str,
    account_address: &str,
) -> MmResult<Vec<u8>> {
    let request_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            account_address,
            {
                "encoding": "base64",
                "commitment": "confirmed"
            }
        ]
    });

    let resp = http
        .post(rpc_url)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!("getAccountInfo request failed: {}", e),
        })?;

    if !resp.status().is_success() {
        return Err(MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!("getAccountInfo returned status {}", resp.status()),
        });
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
        exchange: Exchange::Raydium,
        message: format!("failed to parse getAccountInfo response: {}", e),
    })?;

    // Check for RPC error.
    if let Some(error) = body.get("error") {
        return Err(MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!("getAccountInfo RPC error: {}", error),
        });
    }

    let data_array =
        body["result"]["value"]["data"]
            .as_array()
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "getAccountInfo: missing data array in response".into(),
            })?;

    let data_b64 =
        data_array
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "getAccountInfo: missing base64 data".into(),
            })?;

    decode_base64(data_b64)
}

/// Parse raw pool account data to extract token reserves.
///
/// Reads two little-endian u64 values at the configured offsets for
/// base (coin) and quote (pc) token amounts.
fn parse_pool_reserves(
    data: &[u8],
    base_decimals: u8,
    quote_decimals: u8,
) -> MmResult<PoolReserves> {
    let min_len = RESERVE_QUOTE_OFFSET + 8;
    if data.len() < min_len {
        return Err(MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!(
                "pool account data too short: {} bytes, need at least {}",
                data.len(),
                min_len
            ),
        });
    }

    let base_raw = u64::from_le_bytes(
        data[RESERVE_BASE_OFFSET..RESERVE_BASE_OFFSET + 8]
            .try_into()
            .map_err(|_| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "failed to read base reserve bytes".into(),
            })?,
    );

    let quote_raw = u64::from_le_bytes(
        data[RESERVE_QUOTE_OFFSET..RESERVE_QUOTE_OFFSET + 8]
            .try_into()
            .map_err(|_| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "failed to read quote reserve bytes".into(),
            })?,
    );

    if base_raw == 0 || quote_raw == 0 {
        return Err(MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!(
                "pool reserves are zero: base={}, quote={}",
                base_raw, quote_raw
            ),
        });
    }

    let base_divisor = Decimal::from(10u64.pow(base_decimals as u32));
    let quote_divisor = Decimal::from(10u64.pow(quote_decimals as u32));

    let base_amount = Decimal::from(base_raw) / base_divisor;
    let quote_amount = Decimal::from(quote_raw) / quote_divisor;

    Ok(PoolReserves {
        base_raw,
        _quote_raw: quote_raw,
        base_amount,
        quote_amount,
    })
}

// ---------------------------------------------------------------------------
// Base64 decoding helper
// ---------------------------------------------------------------------------

/// Decode a standard base64-encoded string to bytes.
///
/// Implements base64 decoding without external crates beyond what the workspace
/// already provides. Uses a simple lookup-table approach.
fn decode_base64(input: &str) -> MmResult<Vec<u8>> {
    // Standard base64 alphabet.
    const DECODE_TABLE: [i8; 256] = {
        let mut table = [-1i8; 256];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            table[alphabet[i] as usize] = i as i8;
            i += 1;
        }
        table
    };

    let input = input.trim();
    if input.is_empty() {
        return Err(MmError::Exchange {
            exchange: Exchange::Raydium,
            message: "empty base64 input".into(),
        });
    }

    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;

    for &byte in input.as_bytes() {
        if byte == b'=' {
            break;
        }
        if byte == b'\n' || byte == b'\r' || byte == b' ' {
            continue;
        }
        let val = DECODE_TABLE[byte as usize];
        if val < 0 {
            return Err(MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("invalid base64 character: '{}'", byte as char),
            });
        }
        buf = (buf << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_base64_simple() {
        let encoded = "SGVsbG8gV29ybGQ=";
        let decoded = decode_base64(encoded).unwrap();
        assert_eq!(decoded, b"Hello World");
    }

    #[test]
    fn test_decode_base64_no_padding() {
        let encoded = "YQ";
        let decoded = decode_base64(encoded).unwrap();
        assert_eq!(decoded, b"a");
    }

    #[test]
    fn test_decode_base64_empty_fails() {
        assert!(decode_base64("").is_err());
    }

    #[test]
    fn test_parse_pool_reserves() {
        // Construct synthetic account data with known reserves.
        let mut data = vec![0u8; 128];

        // Place base reserve = 1_000_000_000 (1 SOL in lamports) at offset 64.
        let base_val: u64 = 1_000_000_000;
        data[RESERVE_BASE_OFFSET..RESERVE_BASE_OFFSET + 8].copy_from_slice(&base_val.to_le_bytes());

        // Place quote reserve = 150_000_000 (150 USDC in smallest units, 6 decimals) at offset 72.
        let quote_val: u64 = 150_000_000;
        data[RESERVE_QUOTE_OFFSET..RESERVE_QUOTE_OFFSET + 8]
            .copy_from_slice(&quote_val.to_le_bytes());

        let reserves = parse_pool_reserves(&data, 9, 6).unwrap();

        // base_amount = 1_000_000_000 / 10^9 = 1.0
        assert_eq!(reserves.base_amount, Decimal::from(1));
        // quote_amount = 150_000_000 / 10^6 = 150.0
        assert_eq!(reserves.quote_amount, Decimal::from(150));
    }

    #[test]
    fn test_parse_pool_reserves_data_too_short() {
        let data = vec![0u8; 50]; // too short
        let result = parse_pool_reserves(&data, 9, 6);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pool_reserves_zero_reserves() {
        let data = vec![0u8; 128]; // all zeros
        let result = parse_pool_reserves(&data, 9, 6);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_virtual_order_book_structure() {
        let reserves = PoolReserves {
            base_raw: 1_000_000_000,
            _quote_raw: 150_000_000,
            base_amount: Decimal::from(1000),
            quote_amount: Decimal::from(150_000),
        };
        let symbol = Symbol::new("SOL", "USDC");

        let book = build_virtual_order_book(&symbol, &reserves, 5, 1);

        assert_eq!(book.exchange, Exchange::Raydium);
        assert_eq!(book.symbol, symbol);
        assert_eq!(book.sequence, 1);

        // Should have ask levels (price ascending).
        assert!(!book.asks.is_empty());
        for i in 1..book.asks.len() {
            assert!(
                book.asks[i].price >= book.asks[i - 1].price,
                "asks should be ascending"
            );
        }

        // Should have bid levels (price descending).
        assert!(!book.bids.is_empty());
        for i in 1..book.bids.len() {
            assert!(
                book.bids[i].price <= book.bids[i - 1].price,
                "bids should be descending"
            );
        }

        // Best ask should be > best bid (spread is positive).
        if let (Some(best_bid), Some(best_ask)) = (book.bids.first(), book.asks.first()) {
            assert!(
                best_ask.price > best_bid.price,
                "best ask {} should be > best bid {}",
                best_ask.price,
                best_bid.price
            );
        }
    }

    #[test]
    fn test_build_virtual_order_book_price_impact() {
        // With reserves 1000 SOL / 150000 USDC, spot price = 150.
        let reserves = PoolReserves {
            base_raw: 1_000_000_000_000,
            _quote_raw: 150_000_000_000,
            base_amount: Decimal::from(1000),
            quote_amount: Decimal::from(150_000),
        };
        let symbol = Symbol::new("SOL", "USDC");
        let book = build_virtual_order_book(&symbol, &reserves, 5, 1);

        // The first ask level (buying 1 SOL) should have a price slightly above spot.
        let spot = Decimal::from(150);
        if let Some(first_ask) = book.asks.first() {
            assert!(
                first_ask.price > spot,
                "first ask {} should be > spot {}",
                first_ask.price,
                spot
            );
        }

        // Each successive ask level should have a higher effective price (more slippage).
        for i in 1..book.asks.len() {
            assert!(
                book.asks[i].price > book.asks[i - 1].price,
                "ask prices should increase with size"
            );
        }
    }

    // -------------------------------------------------------------------
    // Connector creation and config tests
    // -------------------------------------------------------------------

    fn test_raydium_config() -> RaydiumConfig {
        RaydiumConfig {
            rpc_http_url: "https://api.mainnet-beta.solana.com".into(),
            rpc_ws_url: "wss://api.mainnet-beta.solana.com".into(),
            pool_addresses: HashMap::from([(
                "SOL/USDC".to_string(),
                "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2".to_string(),
            )]),
            token_decimals: HashMap::from([("SOL".to_string(), 9), ("USDC".to_string(), 6)]),
        }
    }

    #[test]
    fn test_connector_reports_correct_exchange() {
        let config = test_raydium_config();
        let connector = RaydiumConnector::new(config);
        assert_eq!(connector.exchange(), Exchange::Raydium);
    }

    #[test]
    fn test_connector_starts_disconnected() {
        let config = test_raydium_config();
        let connector = RaydiumConnector::new(config);
        assert!(
            !connector.is_connected(),
            "connector should start disconnected"
        );
    }

    #[test]
    fn test_config_construction() {
        let config = test_raydium_config();
        assert!(config.pool_addresses.contains_key("SOL/USDC"));
        assert_eq!(config.token_decimals.get("SOL").copied(), Some(9));
        assert_eq!(config.token_decimals.get("USDC").copied(), Some(6));
    }

    #[test]
    fn test_solana_address_format_base58() {
        let config = test_raydium_config();
        let base58_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
        for (_pair, addr) in &config.pool_addresses {
            for c in addr.chars() {
                assert!(
                    base58_chars.contains(c),
                    "pool address contains non-base58 character: '{}'",
                    c
                );
            }
            assert!(
                addr.len() >= 32 && addr.len() <= 44,
                "Solana address length should be 32-44 chars"
            );
        }
    }
}
