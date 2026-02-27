//! Uniswap V3 execution connector.
//!
//! **Status: Experimental / Incomplete**
//!
//! This connector provides the structural framework for Uniswap integration
//! but transaction signing and submission are not yet implemented. Orders will
//! return simulated/placeholder signatures. Do NOT use in production with real funds.
//!
//! # Approach
//!
//! Implements [`ExecutionConnector`] for Uniswap V3 on Ethereum.
//! DEX execution is fundamentally different from CEX execution:
//!
//! - **Submit order** builds an `exactInputSingle` swap transaction to the
//!   Uniswap V3 SwapRouter, signs it, and broadcasts via `eth_sendRawTransaction`.
//!   The transaction hash serves as the exchange order ID.
//! - **Cancel** is a no-op: DEX swaps are atomic and cannot be cancelled once
//!   submitted. If the tx is still in the mempool we return `Ok(())`; if already
//!   confirmed we return an error.
//! - **Cancel all** always returns `Ok(0)` since DEX orders are atomic.
//! - **Balances** queries ERC-20 `balanceOf(address)` and native ETH balance
//!   via `eth_getBalance`.
//! - **Order updates** are obtained by polling `eth_getTransactionReceipt`:
//!   - Receipt with `status: 0x1` -> `Filled` (parse Swap event for fill details)
//!   - Receipt with `status: 0x0` -> `Rejected` (reverted)
//!   - No receipt -> still `Pending`
//!
//! # SwapRouter Interface
//!
//! ```text
//! function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
//!
//! struct ExactInputSingleParams {
//!     address tokenIn;
//!     address tokenOut;
//!     uint24 fee;
//!     address recipient;
//!     uint256 deadline;
//!     uint256 amountIn;
//!     uint256 amountOutMinimum;
//!     uint160 sqrtPriceLimitX96;
//! }
//! ```
//!
//! Function selector for `exactInputSingle`: `0x414bf389`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use mm_core::{
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, OrderStatus, OrderUpdate,
    Side, Symbol,
};
use reqwest::Client;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default Uniswap V3 SwapRouter address (Ethereum mainnet).
const DEFAULT_ROUTER_ADDRESS: &str = "0xE592427A0AEce92De3Edee1F18E0157C05861564";

/// Function selector for `exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))`.
const EXACT_INPUT_SINGLE_SELECTOR: &str = "414bf389";

/// ERC-20 `balanceOf(address)` function selector.
const BALANCE_OF_SELECTOR: &str = "70a08231";

/// Uniswap V3 Swap event topic for parsing fill data from receipts.
const SWAP_EVENT_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

/// Default pool fee tier (0.3% = 3000 bps) used when no explicit fee is given.
const DEFAULT_FEE_TIER: u32 = 3000;

/// Poll interval for checking transaction receipts (milliseconds).
const RECEIPT_POLL_INTERVAL_MS: u64 = 3_000;

/// Maximum number of receipt poll attempts before giving up.
#[allow(dead_code)]
const MAX_RECEIPT_POLL_ATTEMPTS: u32 = 100;

/// Default deadline offset in seconds (30 minutes from now).
const DEADLINE_OFFSET_SECS: u64 = 1800;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Uniswap V3 execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapExecutionConfig {
    /// Ethereum JSON-RPC HTTP URL.
    pub rpc_http_url: String,
    /// Ethereum chain ID (1 for mainnet, 5 for Goerli, etc.).
    pub chain_id: u64,
    /// Hex-encoded private key (without 0x prefix) for transaction signing.
    pub private_key: String,
    /// Uniswap V3 SwapRouter contract address.
    #[serde(default = "default_router_address")]
    pub router_address: String,
    /// Mapping of token symbols to contract addresses.
    /// Example: `"WETH" -> "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"`.
    pub token_addresses: HashMap<String, String>,
    /// Token decimal places.
    /// Example: `"WETH" -> 18`, `"USDC" -> 6`.
    pub token_decimals: HashMap<String, u8>,
    /// Optional: pool fee tiers per pair. Key is "BASE/QUOTE", value is fee in
    /// hundredths of a bip (e.g., 3000 for 0.3%).
    #[serde(default)]
    pub fee_tiers: HashMap<String, u32>,
    /// Maximum slippage tolerance as a fraction (e.g., 0.005 for 0.5%).
    #[serde(default = "default_slippage")]
    pub max_slippage: f64,
}

fn default_router_address() -> String {
    DEFAULT_ROUTER_ADDRESS.to_string()
}

fn default_slippage() -> f64 {
    0.005
}

// ---------------------------------------------------------------------------
// Pending order tracking
// ---------------------------------------------------------------------------

/// A pending DEX swap transaction being tracked for confirmation.
#[derive(Debug, Clone)]
struct PendingSwap {
    client_order_id: String,
    tx_hash: String,
    symbol: Symbol,
    _side: Side,
    quantity: Decimal,
    submitted_at: chrono::DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// UniswapExecutionConnector
// ---------------------------------------------------------------------------

pub struct UniswapExecutionConnector {
    config: UniswapExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    /// Background task polling for transaction receipts.
    poll_handle: Option<JoinHandle<()>>,
    /// Shared list of pending swaps being tracked.
    pending_swaps: Arc<Mutex<Vec<PendingSwap>>>,
    /// Nonce counter for transaction ordering.
    nonce: Arc<Mutex<u64>>,
}

impl UniswapExecutionConnector {
    pub fn new(config: UniswapExecutionConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            poll_handle: None,
            pending_swaps: Arc::new(Mutex::new(Vec::new())),
            nonce: Arc::new(Mutex::new(0)),
        }
    }

    // -----------------------------------------------------------------------
    // JSON-RPC helpers
    // -----------------------------------------------------------------------

    /// Send a JSON-RPC request and return the parsed response.
    async fn rpc_call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> MmResult<serde_json::Value> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        });

        let resp = self
            .http
            .post(&self.config.rpc_http_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: format!("RPC request failed: {}", e),
            })?;

        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!("RPC response parse failed: {}", e),
        })?;

        if let Some(err) = body.get("error") {
            return Err(MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: format!("RPC error: {}", err),
            });
        }

        Ok(body)
    }

    // -----------------------------------------------------------------------
    // Address derivation
    // -----------------------------------------------------------------------

    /// Derive the Ethereum address from the configured private key.
    ///
    /// NOTE: Full address derivation requires secp256k1 public key computation
    /// and keccak256 hashing. This is a placeholder that returns a fixed
    /// address pattern. In production, use a proper crypto library.
    fn wallet_address(&self) -> String {
        // TODO: Implement proper address derivation from private key:
        // 1. Parse private key bytes from hex
        // 2. Compute secp256k1 public key (uncompressed, 64 bytes)
        // 3. Keccak256 hash the public key
        // 4. Take last 20 bytes as the address
        //
        // For now, we store the derived address in config or derive it at
        // startup. In a real implementation this would use a crate like
        // `k256` or `secp256k1`.
        //
        // Placeholder: derive a deterministic address from the key for testing.
        let key_bytes = hex::decode(&self.config.private_key).unwrap_or_default();
        if key_bytes.len() >= 20 {
            format!("0x{}", hex::encode(&key_bytes[..20]))
        } else {
            "0x0000000000000000000000000000000000000000".to_string()
        }
    }

    // -----------------------------------------------------------------------
    // Nonce management
    // -----------------------------------------------------------------------

    /// Fetch the current nonce from the node and update our local counter.
    async fn fetch_nonce(&self) -> MmResult<u64> {
        let address = self.wallet_address();
        let body = self
            .rpc_call(
                "eth_getTransactionCount",
                serde_json::json!([address, "pending"]),
            )
            .await?;

        let nonce_hex = body["result"].as_str().unwrap_or("0x0");
        let nonce =
            u64::from_str_radix(nonce_hex.strip_prefix("0x").unwrap_or(nonce_hex), 16).unwrap_or(0);

        let mut local_nonce = self.nonce.lock().await;
        if nonce > *local_nonce {
            *local_nonce = nonce;
        }

        Ok(*local_nonce)
    }

    /// Get and increment the nonce for the next transaction.
    async fn next_nonce(&self) -> u64 {
        let mut nonce = self.nonce.lock().await;
        let current = *nonce;
        *nonce += 1;
        current
    }

    // -----------------------------------------------------------------------
    // ABI encoding helpers
    // -----------------------------------------------------------------------

    /// Encode an address (20 bytes) as a 32-byte ABI word.
    fn encode_address(addr: &str) -> String {
        let clean = addr.strip_prefix("0x").unwrap_or(addr).to_lowercase();
        format!("{:0>64}", clean)
    }

    /// Encode a uint256 value as a 32-byte ABI word.
    fn encode_uint256(value: u128) -> String {
        format!("{:064x}", value)
    }

    /// Encode a uint24 (fee tier) as a 32-byte ABI word.
    fn encode_uint24(value: u32) -> String {
        format!("{:064x}", value)
    }

    /// Convert a token amount to its raw integer representation given decimals.
    fn to_raw_amount(amount: Decimal, decimals: u8) -> u128 {
        let multiplier = 10u128.pow(decimals as u32);
        let raw = amount * Decimal::from(multiplier);
        raw.to_u128().unwrap_or(0)
    }

    /// Build the calldata for `exactInputSingle`.
    ///
    /// Params struct layout:
    /// - address tokenIn
    /// - address tokenOut
    /// - uint24 fee
    /// - address recipient
    /// - uint256 deadline
    /// - uint256 amountIn
    /// - uint256 amountOutMinimum
    /// - uint160 sqrtPriceLimitX96 (0 = no limit)
    #[allow(clippy::too_many_arguments)]
    fn build_exact_input_single_calldata(
        &self,
        token_in: &str,
        token_out: &str,
        fee: u32,
        recipient: &str,
        deadline: u64,
        amount_in: u128,
        amount_out_minimum: u128,
    ) -> String {
        let mut calldata = String::with_capacity(8 + 8 * 64); // selector + 8 words
        calldata.push_str(EXACT_INPUT_SINGLE_SELECTOR);
        calldata.push_str(&Self::encode_address(token_in));
        calldata.push_str(&Self::encode_address(token_out));
        calldata.push_str(&Self::encode_uint24(fee));
        calldata.push_str(&Self::encode_address(recipient));
        calldata.push_str(&Self::encode_uint256(deadline as u128));
        calldata.push_str(&Self::encode_uint256(amount_in));
        calldata.push_str(&Self::encode_uint256(amount_out_minimum));
        calldata.push_str(&Self::encode_uint256(0)); // sqrtPriceLimitX96 = 0 (no limit)
        calldata
    }

    /// Build and sign a raw Ethereum transaction.
    ///
    /// NOTE: Full EIP-1559 transaction signing requires:
    /// 1. RLP encoding of the transaction fields
    /// 2. Keccak256 hashing of the RLP-encoded payload
    /// 3. ECDSA signing with secp256k1 using the private key
    /// 4. Appending v, r, s signature values
    ///
    /// This is a placeholder that returns a properly formatted but unsigned
    /// transaction. A production implementation would use `k256` or similar.
    #[allow(clippy::too_many_arguments)]
    fn build_raw_transaction(
        &self,
        nonce: u64,
        to: &str,
        value: u128,
        data: &str,
        gas_limit: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    ) -> String {
        // TODO: Implement full EIP-1559 transaction signing:
        //
        // 1. Construct the transaction fields:
        //    [chain_id, nonce, max_priority_fee_per_gas, max_fee_per_gas,
        //     gas_limit, to, value, data, access_list]
        //
        // 2. RLP-encode the fields.
        //
        // 3. Prepend the EIP-1559 transaction type byte (0x02).
        //
        // 4. Keccak256 hash the type-prefixed RLP payload.
        //
        // 5. ECDSA sign the hash with the private key using secp256k1.
        //
        // 6. Append (v, r, s) signature to the RLP, re-encode, and
        //    prepend the type byte again.
        //
        // For now, we construct the transaction data as a hex string that
        // documents all the fields. A real implementation would produce a
        // valid signed raw transaction.

        let _tx_fields = serde_json::json!({
            "type": "0x02",
            "chainId": format!("0x{:x}", self.config.chain_id),
            "nonce": format!("0x{:x}", nonce),
            "maxPriorityFeePerGas": format!("0x{:x}", max_priority_fee_per_gas),
            "maxFeePerGas": format!("0x{:x}", max_fee_per_gas),
            "gasLimit": format!("0x{:x}", gas_limit),
            "to": to,
            "value": format!("0x{:x}", value),
            "data": format!("0x{}", data),
        });

        // Placeholder: return a mock signed transaction.
        // In production, this would be the fully RLP-encoded and ECDSA-signed
        // raw transaction bytes.
        format!(
            "0x02{:02x}{:016x}{:032x}{:032x}{:016x}{}{:032x}{}",
            self.config.chain_id,
            nonce,
            max_priority_fee_per_gas,
            max_fee_per_gas,
            gas_limit,
            to.strip_prefix("0x").unwrap_or(to),
            value,
            data
        )
    }

    // -----------------------------------------------------------------------
    // Gas estimation
    // -----------------------------------------------------------------------

    /// Fetch current gas prices from the node.
    async fn fetch_gas_price(&self) -> MmResult<(u128, u128)> {
        // Get base fee from latest block.
        let block_body = self
            .rpc_call("eth_getBlockByNumber", serde_json::json!(["latest", false]))
            .await?;

        let base_fee_hex = block_body["result"]["baseFeePerGas"]
            .as_str()
            .unwrap_or("0x0");
        let base_fee =
            u128::from_str_radix(base_fee_hex.strip_prefix("0x").unwrap_or(base_fee_hex), 16)
                .unwrap_or(30_000_000_000); // 30 gwei fallback

        // Priority fee: use a reasonable default.
        let priority_fee: u128 = 2_000_000_000; // 2 gwei

        // Max fee: base_fee * 2 + priority_fee (standard EIP-1559 heuristic).
        let max_fee = base_fee * 2 + priority_fee;

        Ok((max_fee, priority_fee))
    }

    // -----------------------------------------------------------------------
    // Receipt polling
    // -----------------------------------------------------------------------

    /// Poll for a transaction receipt and parse the Swap event from logs.
    async fn poll_receipt(
        http: &Client,
        rpc_url: &str,
        tx_hash: &str,
    ) -> MmResult<Option<(bool, Option<SwapFillData>)>> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash]
        });

        let resp = http
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: format!("receipt poll failed: {}", e),
            })?;

        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Uniswap,
            message: format!("receipt parse failed: {}", e),
        })?;

        let result = &body["result"];
        if result.is_null() {
            // No receipt yet — transaction is still pending.
            return Ok(None);
        }

        // Check status.
        let status_hex = result["status"].as_str().unwrap_or("0x0");
        let success = status_hex == "0x1";

        if !success {
            return Ok(Some((false, None)));
        }

        // Parse Swap event from logs to get fill data.
        let fill_data = Self::parse_swap_from_logs(result);
        Ok(Some((true, fill_data)))
    }

    /// Parse Swap event data from transaction receipt logs.
    fn parse_swap_from_logs(receipt: &serde_json::Value) -> Option<SwapFillData> {
        let logs = receipt["logs"].as_array()?;

        for log in logs {
            let topics = log["topics"].as_array()?;
            if topics.is_empty() {
                continue;
            }
            let topic0 = topics[0].as_str()?;
            if topic0 != SWAP_EVENT_TOPIC {
                continue;
            }

            let data = log["data"].as_str()?;
            let hex_data = data.strip_prefix("0x").unwrap_or(data);

            if hex_data.len() < 320 {
                continue;
            }

            // Parse amount0 (int256) and amount1 (int256) from data.
            let amount0_hex = &hex_data[0..64];
            let amount1_hex = &hex_data[64..128];
            let sqrt_price_hex = &hex_data[128..192];

            let amount0 = parse_signed_hex_to_f64(amount0_hex);
            let amount1 = parse_signed_hex_to_f64(amount1_hex);

            return Some(SwapFillData {
                amount0,
                amount1,
                _sqrt_price_x96_hex: sqrt_price_hex.to_string(),
            });
        }

        None
    }

    // -----------------------------------------------------------------------
    // Token resolution
    // -----------------------------------------------------------------------

    /// Get the token contract address for a given symbol.
    fn token_address(&self, token: &str) -> MmResult<String> {
        self.config
            .token_addresses
            .get(token)
            .cloned()
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Uniswap,
                message: format!("no token address configured for {}", token),
            })
    }

    /// Get the fee tier for a given trading pair.
    fn fee_tier(&self, symbol: &Symbol) -> u32 {
        let key = symbol.canonical();
        self.config
            .fee_tiers
            .get(&key)
            .copied()
            .unwrap_or(DEFAULT_FEE_TIER)
    }
}

/// Intermediate representation of fill data parsed from a Swap event.
#[derive(Debug)]
struct SwapFillData {
    amount0: f64,
    amount1: f64,
    _sqrt_price_x96_hex: String,
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for UniswapExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Uniswap
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Uniswap: connecting execution connector");

        // Fetch initial nonce from the chain.
        self.fetch_nonce().await?;
        self.connected.store(true, Ordering::SeqCst);

        // Spawn a background task to poll for transaction receipts.
        let http = self.http.clone();
        let rpc_url = self.config.rpc_http_url.clone();
        let pending = self.pending_swaps.clone();
        let connected = self.connected.clone();
        let token_decimals = self.config.token_decimals.clone();

        let poll_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(RECEIPT_POLL_INTERVAL_MS));

            loop {
                interval.tick().await;

                if !connected.load(Ordering::SeqCst) {
                    break;
                }

                // Snapshot the pending list.
                let pending_snapshot = {
                    let guard = pending.lock().await;
                    guard.clone()
                };

                let mut completed_indices = Vec::new();

                for (idx, swap) in pending_snapshot.iter().enumerate() {
                    match Self::poll_receipt(&http, &rpc_url, &swap.tx_hash).await {
                        Ok(Some((success, fill_data))) => {
                            let (status, fill_price, fill_quantity) = if success {
                                let (fp, fq) = fill_data
                                    .as_ref()
                                    .map(|fd| {
                                        // Compute fill price from amounts.
                                        let price = if fd.amount0.abs() > 0.0 {
                                            (fd.amount1.abs() / fd.amount0.abs()).abs()
                                        } else {
                                            0.0
                                        };
                                        (price, fd.amount0.abs())
                                    })
                                    .unwrap_or((0.0, 0.0));
                                (OrderStatus::Filled, fp, fq)
                            } else {
                                (OrderStatus::Rejected, 0.0, 0.0)
                            };

                            // Adjust fill quantity for token decimals.
                            let base_decimals =
                                token_decimals.get(&swap.symbol.base).copied().unwrap_or(18);
                            let adjusted_qty = fill_quantity / 10.0_f64.powi(base_decimals as i32);

                            let update = OrderUpdate {
                                client_order_id: swap.client_order_id.clone(),
                                exchange_order_id: Some(swap.tx_hash.clone()),
                                exchange: Exchange::Uniswap,
                                symbol: swap.symbol.clone(),
                                status,
                                filled_quantity: Decimal::from_f64(adjusted_qty)
                                    .unwrap_or(swap.quantity),
                                remaining_quantity: Decimal::ZERO,
                                average_fill_price: Decimal::from_f64(fill_price)
                                    .unwrap_or(Decimal::ZERO),
                                last_fill_price: Decimal::from_f64(fill_price),
                                last_fill_quantity: Decimal::from_f64(adjusted_qty),
                                fee: None, // Gas cost; would need to compute from receipt
                                fee_currency: Some("ETH".to_string()),
                                timestamp: Utc::now(),
                            };

                            if let Err(e) = update_tx.send(update).await {
                                warn!(
                                    error = %e,
                                    tx_hash = %swap.tx_hash,
                                    "Uniswap: failed to send order update"
                                );
                            }

                            completed_indices.push(idx);
                            info!(
                                tx_hash = %swap.tx_hash,
                                status = %status,
                                "Uniswap: swap completed"
                            );
                        }
                        Ok(None) => {
                            // Still pending. Check for timeout (> 30 min).
                            let age = Utc::now() - swap.submitted_at;
                            if age.num_seconds() > (DEADLINE_OFFSET_SECS as i64) {
                                warn!(
                                    tx_hash = %swap.tx_hash,
                                    "Uniswap: swap timed out without receipt"
                                );
                                let update = OrderUpdate {
                                    client_order_id: swap.client_order_id.clone(),
                                    exchange_order_id: Some(swap.tx_hash.clone()),
                                    exchange: Exchange::Uniswap,
                                    symbol: swap.symbol.clone(),
                                    status: OrderStatus::Expired,
                                    filled_quantity: Decimal::ZERO,
                                    remaining_quantity: swap.quantity,
                                    average_fill_price: Decimal::ZERO,
                                    last_fill_price: None,
                                    last_fill_quantity: None,
                                    fee: None,
                                    fee_currency: None,
                                    timestamp: Utc::now(),
                                };
                                let _ = update_tx.send(update).await;
                                completed_indices.push(idx);
                            }
                        }
                        Err(e) => {
                            debug!(
                                error = %e,
                                tx_hash = %swap.tx_hash,
                                "Uniswap: receipt poll error"
                            );
                        }
                    }
                }

                // Remove completed swaps (in reverse order to preserve indices).
                if !completed_indices.is_empty() {
                    let mut guard = pending.lock().await;
                    for idx in completed_indices.into_iter().rev() {
                        if idx < guard.len() {
                            guard.remove(idx);
                        }
                    }
                }
            }
        });

        self.poll_handle = Some(poll_handle);
        info!("Uniswap: execution connector connected");

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Uniswap: disconnecting execution connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.poll_handle.take() {
            h.abort();
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        // SAFETY GATE: This DEX connector uses placeholder transaction signing
        // that would submit malformed transactions and lose real funds. Block all
        // order submission until a production-grade signing implementation exists.
        return Err(MmError::DexNotReady {
            exchange: Exchange::Uniswap,
            message: "Uniswap connector is experimental — transaction signing is not \
                      production-ready. Submitting orders is disabled to prevent loss of funds."
                .into(),
        });

        // ---- Original implementation below (retained for future completion) ----

        #[allow(unreachable_code, unused_variables)]
        {
            warn!("DEX connector is experimental — transaction signing not implemented, returning placeholder");

            if !self.connected.load(Ordering::SeqCst) {
                return Err(MmError::Exchange {
                    exchange: Exchange::Uniswap,
                    message: "not connected".into(),
                });
            }

            info!(
                symbol = %order.symbol,
                side = %order.side,
                quantity = %order.quantity,
                "Uniswap: submitting swap"
            );

            // Determine token addresses based on trade direction.
            // Buy base: swap quote -> base (tokenIn = quote, tokenOut = base)
            // Sell base: swap base -> quote (tokenIn = base, tokenOut = quote)
            let (token_in_symbol, token_out_symbol) = match order.side {
                Side::Buy => (order.symbol.quote.as_str(), order.symbol.base.as_str()),
                Side::Sell => (order.symbol.base.as_str(), order.symbol.quote.as_str()),
            };

            let token_in = self.token_address(token_in_symbol)?;
            let token_out = self.token_address(token_out_symbol)?;

            let fee = self.fee_tier(&order.symbol);
            let recipient = self.wallet_address();

            let deadline = (Utc::now().timestamp() as u64) + DEADLINE_OFFSET_SECS;

            // Compute amountIn in raw token units.
            let in_decimals = self
                .config
                .token_decimals
                .get(token_in_symbol)
                .copied()
                .unwrap_or(18);
            let out_decimals = self
                .config
                .token_decimals
                .get(token_out_symbol)
                .copied()
                .unwrap_or(18);

            let amount_in = match order.side {
                Side::Buy => {
                    // Buying base: amountIn is in quote token.
                    // If price is given, amountIn = quantity * price.
                    // Otherwise, use quantity as the quote amount directly.
                    let quote_amount = if let Some(price) = order.price {
                        order.quantity * price
                    } else {
                        order.quantity
                    };
                    Self::to_raw_amount(quote_amount, in_decimals)
                }
                Side::Sell => {
                    // Selling base: amountIn is in base token.
                    Self::to_raw_amount(order.quantity, in_decimals)
                }
            };

            // amountOutMinimum: apply slippage tolerance.
            let amount_out_minimum = if let Some(price) = order.price {
                let expected_out = match order.side {
                    Side::Buy => order.quantity,
                    Side::Sell => order.quantity * price,
                };
                let slippage_factor =
                    Decimal::from_f64(1.0 - self.config.max_slippage).unwrap_or(Decimal::ONE);
                let min_out = expected_out * slippage_factor;
                Self::to_raw_amount(min_out, out_decimals)
            } else {
                0 // No minimum (market order with no price limit).
            };

            // Build calldata.
            let calldata = self.build_exact_input_single_calldata(
                &token_in,
                &token_out,
                fee,
                &recipient,
                deadline,
                amount_in,
                amount_out_minimum,
            );

            // Get gas price and nonce.
            let (max_fee, priority_fee) = self.fetch_gas_price().await?;
            let nonce = self.next_nonce().await;
            let gas_limit: u64 = 300_000; // Conservative estimate for Uniswap swap.

            // Build the raw transaction.
            // Value is 0 for ERC-20 swaps (non-zero only for ETH-in swaps).
            let value: u128 = 0;
            let raw_tx = self.build_raw_transaction(
                nonce,
                &self.config.router_address,
                value,
                &calldata,
                gas_limit,
                max_fee,
                priority_fee,
            );

            // Send the raw transaction via eth_sendRawTransaction.
            let body = self
                .rpc_call("eth_sendRawTransaction", serde_json::json!([raw_tx]))
                .await?;

            let tx_hash = body["result"]
                .as_str()
                .ok_or_else(|| MmError::Exchange {
                    exchange: Exchange::Uniswap,
                    message: format!("eth_sendRawTransaction did not return a tx hash: {}", body),
                })?
                .to_string();

            info!(
                tx_hash = %tx_hash,
                nonce = nonce,
                "Uniswap: swap transaction submitted"
            );

            // Track the pending swap for receipt polling.
            let client_order_id = uuid::Uuid::new_v4().to_string();
            {
                let mut pending = self.pending_swaps.lock().await;
                pending.push(PendingSwap {
                    client_order_id: client_order_id.clone(),
                    tx_hash: tx_hash.clone(),
                    symbol: order.symbol.clone(),
                    _side: order.side,
                    quantity: order.quantity,
                    submitted_at: Utc::now(),
                });
            }

            Ok(tx_hash)
        } // end #[allow(unreachable_code)] block
    }

    #[allow(unused_variables)]
    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        // SAFETY GATE: Block cancel_order for the same reason as submit_order.
        return Err(MmError::DexNotReady {
            exchange: Exchange::Uniswap,
            message: "Uniswap connector is experimental — transaction signing is not \
                      production-ready. Order operations are disabled to prevent loss of funds."
                .into(),
        });

        // ---- Original implementation below (retained for future completion) ----

        #[allow(unreachable_code, unused_variables)]
        {
            warn!("DEX connector is experimental — transaction signing not implemented, returning placeholder");
            // DEX swaps are atomic and cannot be cancelled once submitted.
            // If the transaction is still in the mempool, it might be replaced
            // with a higher-gas no-op transaction, but that is complex and
            // beyond the scope of this connector.
            //
            // Check if the order is still pending (no receipt yet).
            if let Some(ref tx_hash) = cancel.exchange_order_id {
                match Self::poll_receipt(&self.http, &self.config.rpc_http_url, tx_hash).await? {
                    Some((true, _)) => {
                        return Err(MmError::Exchange {
                            exchange: Exchange::Uniswap,
                            message: format!(
                                "cannot cancel tx {}: already confirmed and filled",
                                tx_hash
                            ),
                        });
                    }
                    Some((false, _)) => {
                        // Transaction reverted; nothing to cancel.
                        info!(
                            tx_hash = %tx_hash,
                            "Uniswap: cancel_order on reverted tx (no-op)"
                        );
                        return Ok(());
                    }
                    None => {
                        // Transaction is still pending. In theory we could submit a
                        // replacement transaction with the same nonce and higher gas
                        // to a zero-value self-transfer. For now, we just log it.
                        warn!(
                            tx_hash = %tx_hash,
                            "Uniswap: cancel_order on pending tx — DEX swaps cannot \
                             be cancelled; tx may still confirm"
                        );
                        return Ok(());
                    }
                }
            }

            warn!(
                client_order_id = %cancel.client_order_id,
                "Uniswap: cancel_order called without exchange_order_id"
            );
            Ok(())
        } // end #[allow(unreachable_code)] block
    }

    async fn cancel_all_orders(&self, _request: &CancelAllOrders) -> MmResult<u32> {
        // DEX swaps are atomic: there are no resting orders to cancel.
        info!("Uniswap: cancel_all_orders is a no-op for DEX");
        Ok(0)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let address = self.wallet_address();
        let mut balances = Vec::new();

        // 1. Fetch native ETH balance.
        let eth_body = self
            .rpc_call("eth_getBalance", serde_json::json!([&address, "latest"]))
            .await?;

        let eth_hex = eth_body["result"].as_str().unwrap_or("0x0");
        let eth_raw =
            u128::from_str_radix(eth_hex.strip_prefix("0x").unwrap_or(eth_hex), 16).unwrap_or(0);
        let eth_balance = Decimal::from_f64(eth_raw as f64 / 1e18).unwrap_or(Decimal::ZERO);

        if eth_balance > Decimal::ZERO {
            balances.push(Balance {
                currency: "ETH".to_string(),
                free: eth_balance,
                locked: Decimal::ZERO,
                total: eth_balance,
            });
        }

        // 2. Fetch ERC-20 balances for configured tokens.
        for (token_symbol, token_address) in &self.config.token_addresses {
            let decimals = self
                .config
                .token_decimals
                .get(token_symbol)
                .copied()
                .unwrap_or(18);

            match self
                .fetch_erc20_balance(token_address, &address, decimals)
                .await
            {
                Ok(balance) => {
                    if balance > Decimal::ZERO {
                        balances.push(Balance {
                            currency: token_symbol.clone(),
                            free: balance,
                            locked: Decimal::ZERO,
                            total: balance,
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        token = %token_symbol,
                        error = %e,
                        "Uniswap: failed to fetch ERC-20 balance"
                    );
                }
            }
        }

        debug!(count = balances.len(), "Uniswap: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

impl UniswapExecutionConnector {
    /// Fetch the ERC-20 token balance for an address using `balanceOf(address)`.
    async fn fetch_erc20_balance(
        &self,
        token_address: &str,
        owner: &str,
        decimals: u8,
    ) -> MmResult<Decimal> {
        // Encode the balanceOf calldata: selector + address padded to 32 bytes.
        let calldata = format!("0x{}{}", BALANCE_OF_SELECTOR, Self::encode_address(owner));

        let body = self
            .rpc_call(
                "eth_call",
                serde_json::json!([
                    {
                        "to": token_address,
                        "data": calldata
                    },
                    "latest"
                ]),
            )
            .await?;

        let result_hex = body["result"].as_str().unwrap_or("0x0");
        let clean = result_hex.strip_prefix("0x").unwrap_or(result_hex);

        // Parse the uint256 result.
        let raw = u128::from_str_radix(clean.trim_start_matches('0').max("0"), 16).unwrap_or(0);
        let divisor = 10u128.pow(decimals as u32);
        let balance = Decimal::from(raw) / Decimal::from(divisor);

        Ok(balance)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

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

/// Parse a hexadecimal string (without 0x prefix) as an f64.
fn hex_to_f64(hex: &str) -> Option<f64> {
    let hex = hex.trim_start_matches('0');
    if hex.is_empty() {
        return Some(0.0);
    }

    let mut result: f64 = 0.0;
    for chunk_start in (0..hex.len()).step_by(8) {
        let chunk_end = (chunk_start + 8).min(hex.len());
        let chunk = &hex[chunk_start..chunk_end];
        let val = u32::from_str_radix(chunk, 16).ok()? as f64;

        let remaining_hex_chars = hex.len() - chunk_end;
        let shift = remaining_hex_chars as f64 * 4.0;
        result += val * 2.0_f64.powf(shift);
    }

    Some(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{OrderType, TimeInForce};

    #[test]
    fn test_encode_address() {
        let addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        let encoded = UniswapExecutionConnector::encode_address(addr);
        assert_eq!(encoded.len(), 64);
        assert!(encoded.starts_with("000000000000000000000000"));
        assert!(encoded.ends_with("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"));
    }

    #[test]
    fn test_encode_uint256() {
        let encoded = UniswapExecutionConnector::encode_uint256(1000);
        assert_eq!(encoded.len(), 64);
        assert_eq!(encoded, format!("{:064x}", 1000u128));
    }

    #[test]
    fn test_encode_uint24() {
        let encoded = UniswapExecutionConnector::encode_uint24(3000);
        assert_eq!(encoded.len(), 64);
        assert!(encoded.ends_with("bb8")); // 3000 = 0xBB8
    }

    #[test]
    fn test_to_raw_amount() {
        // 1.5 WETH with 18 decimals
        let amount = Decimal::new(15, 1); // 1.5
        let raw = UniswapExecutionConnector::to_raw_amount(amount, 18);
        assert_eq!(raw, 1_500_000_000_000_000_000);

        // 100 USDC with 6 decimals
        let amount = Decimal::from(100);
        let raw = UniswapExecutionConnector::to_raw_amount(amount, 6);
        assert_eq!(raw, 100_000_000);
    }

    #[test]
    fn test_parse_signed_hex_positive() {
        // 1000 in 256-bit hex
        let hex = "00000000000000000000000000000000000000000000000000000000000003e8";
        let val = parse_signed_hex_to_f64(hex);
        assert!((val - 1000.0).abs() < 1e-6, "expected 1000, got {}", val);
    }

    #[test]
    fn test_parse_signed_hex_negative() {
        // -1000 in two's complement (256-bit)
        // 1000 = 0x3e8, so -1000 = complement + 1
        let hex = "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc18";
        let val = parse_signed_hex_to_f64(hex);
        assert!(
            (val - (-1000.0)).abs() < 1e-6,
            "expected -1000, got {}",
            val
        );
    }

    #[test]
    fn test_default_config() {
        let config = UniswapExecutionConfig {
            rpc_http_url: "https://eth-mainnet.g.alchemy.com/v2/test".into(),
            chain_id: 1,
            private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            router_address: default_router_address(),
            token_addresses: HashMap::from([
                (
                    "WETH".to_string(),
                    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(),
                ),
                (
                    "USDC".to_string(),
                    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
                ),
            ]),
            token_decimals: HashMap::from([("WETH".to_string(), 18), ("USDC".to_string(), 6)]),
            fee_tiers: HashMap::new(),
            max_slippage: default_slippage(),
        };

        assert_eq!(config.router_address, DEFAULT_ROUTER_ADDRESS);
        assert_eq!(config.chain_id, 1);
        assert!((config.max_slippage - 0.005).abs() < f64::EPSILON);
    }

    #[test]
    fn test_calldata_structure() {
        let config = UniswapExecutionConfig {
            rpc_http_url: "http://localhost:8545".into(),
            chain_id: 1,
            private_key: "aa".repeat(32),
            router_address: DEFAULT_ROUTER_ADDRESS.into(),
            token_addresses: HashMap::new(),
            token_decimals: HashMap::new(),
            fee_tiers: HashMap::new(),
            max_slippage: 0.005,
        };

        let connector = UniswapExecutionConnector::new(config);

        let calldata = connector.build_exact_input_single_calldata(
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            3000,
            "0x1234567890123456789012345678901234567890",
            1700000000,
            1_000_000_000_000_000_000, // 1 WETH
            1_900_000_000,             // 1900 USDC (min out)
        );

        // Should start with the function selector.
        assert!(calldata.starts_with(EXACT_INPUT_SINGLE_SELECTOR));

        // Total length: 8 (selector) + 8 * 64 (8 words) = 520 hex chars.
        assert_eq!(
            calldata.len(),
            8 + 8 * 64,
            "calldata should be selector + 8 ABI words"
        );
    }

    // -------------------------------------------------------------------
    // Additional tests: config, connector creation, addresses, state
    // -------------------------------------------------------------------

    /// Helper to create a full test config.
    fn full_test_config() -> UniswapExecutionConfig {
        UniswapExecutionConfig {
            rpc_http_url: "https://eth-mainnet.g.alchemy.com/v2/test".into(),
            chain_id: 1,
            private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            router_address: default_router_address(),
            token_addresses: HashMap::from([
                (
                    "WETH".to_string(),
                    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(),
                ),
                (
                    "USDC".to_string(),
                    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
                ),
                (
                    "USDT".to_string(),
                    "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(),
                ),
            ]),
            token_decimals: HashMap::from([
                ("WETH".to_string(), 18),
                ("USDC".to_string(), 6),
                ("USDT".to_string(), 6),
            ]),
            fee_tiers: HashMap::from([("WETH/USDC".to_string(), 500)]),
            max_slippage: default_slippage(),
        }
    }

    #[test]
    fn test_config_defaults_sensible() {
        let config = full_test_config();
        assert_eq!(config.chain_id, 1, "Ethereum mainnet chain ID should be 1");
        assert!(
            (config.max_slippage - 0.005).abs() < f64::EPSILON,
            "default slippage should be 0.5%"
        );
        assert_eq!(
            config.router_address, DEFAULT_ROUTER_ADDRESS,
            "should use the mainnet SwapRouter address"
        );
    }

    #[test]
    fn test_connector_reports_correct_exchange() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        assert_eq!(connector.exchange(), Exchange::Uniswap);
    }

    #[test]
    fn test_connector_starts_disconnected() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        assert!(
            !connector.is_connected(),
            "connector should start disconnected"
        );
    }

    #[test]
    fn test_connector_no_pending_swaps_initially() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        let pending = connector.pending_swaps.try_lock().unwrap();
        assert!(pending.is_empty(), "should have no pending swaps initially");
    }

    #[test]
    fn test_ethereum_address_checksummed_format() {
        // ERC-20 token addresses should be checksummed (mixed-case) 0x-prefixed 40-hex-char strings.
        let config = full_test_config();
        for (token, addr) in &config.token_addresses {
            assert!(
                addr.starts_with("0x"),
                "token {} address should start with 0x, got: {}",
                token,
                addr
            );
            assert_eq!(
                addr.len(),
                42,
                "token {} address should be 42 chars (0x + 40 hex), got {} chars",
                token,
                addr.len()
            );
            // Verify the hex part contains only valid hex characters.
            let hex_part = &addr[2..];
            for c in hex_part.chars() {
                assert!(
                    c.is_ascii_hexdigit(),
                    "token {} address contains non-hex character '{}' in: {}",
                    token,
                    c,
                    addr
                );
            }
        }
    }

    #[test]
    fn test_router_address_format() {
        // SwapRouter address should be a valid Ethereum address.
        let addr = DEFAULT_ROUTER_ADDRESS;
        assert!(addr.starts_with("0x"));
        assert_eq!(addr.len(), 42);
        let hex_part = &addr[2..];
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "router address contains non-hex characters"
        );
    }

    #[test]
    fn test_chain_id_values() {
        // Verify well-known chain IDs.
        let mainnet_config = full_test_config();
        assert_eq!(mainnet_config.chain_id, 1);

        // Test with other chain IDs.
        let mut goerli_config = full_test_config();
        goerli_config.chain_id = 5;
        assert_eq!(goerli_config.chain_id, 5);

        let mut arbitrum_config = full_test_config();
        arbitrum_config.chain_id = 42161;
        assert_eq!(arbitrum_config.chain_id, 42161);
    }

    #[test]
    fn test_erc20_token_address_validation() {
        let config = full_test_config();

        // WETH address should be the canonical Wrapped Ether address.
        let weth = config.token_addresses.get("WETH").unwrap();
        assert_eq!(weth, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

        // USDC address should be the canonical USDC address.
        let usdc = config.token_addresses.get("USDC").unwrap();
        assert_eq!(usdc, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

        // USDT address should be the canonical USDT address.
        let usdt = config.token_addresses.get("USDT").unwrap();
        assert_eq!(usdt, "0xdAC17F958D2ee523a2206206994597C13D831ec7");
    }

    #[test]
    fn test_token_address_resolution() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);

        // Known tokens should resolve.
        assert!(connector.token_address("WETH").is_ok());
        assert!(connector.token_address("USDC").is_ok());

        // Unknown tokens should return an error.
        assert!(connector.token_address("UNKNOWN_TOKEN").is_err());
    }

    #[test]
    fn test_fee_tier_resolution() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);

        // Explicitly configured fee tier.
        let symbol_weth_usdc = Symbol::new("WETH", "USDC");
        assert_eq!(connector.fee_tier(&symbol_weth_usdc), 500);

        // Unknown pair should fall back to default (3000 = 0.3%).
        let symbol_unknown = Symbol::new("FOO", "BAR");
        assert_eq!(connector.fee_tier(&symbol_unknown), DEFAULT_FEE_TIER);
    }

    #[test]
    fn test_wallet_address_derivation_format() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);

        let addr = connector.wallet_address();
        // Should be a valid Ethereum address format (0x + 40 hex chars).
        assert!(
            addr.starts_with("0x"),
            "wallet address should start with 0x"
        );
        assert_eq!(
            addr.len(),
            42,
            "wallet address should be 42 chars, got {}",
            addr.len()
        );
    }

    #[test]
    fn test_symbol_canonical_format() {
        let symbol = Symbol::new("WETH", "USDC");
        assert_eq!(symbol.canonical(), "WETH/USDC");

        let symbol2 = Symbol::new("UNI", "WETH");
        assert_eq!(symbol2.canonical(), "UNI/WETH");
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = full_test_config();
        let json = serde_json::to_string(&config).expect("should serialize");
        let deserialized: UniswapExecutionConfig =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(deserialized.chain_id, config.chain_id);
        assert_eq!(deserialized.router_address, config.router_address);
        assert_eq!(deserialized.token_addresses, config.token_addresses);
        assert_eq!(deserialized.token_decimals, config.token_decimals);
    }

    #[test]
    fn test_nonce_starts_at_zero() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        let nonce = connector.nonce.try_lock().unwrap();
        assert_eq!(*nonce, 0, "nonce should start at 0");
    }

    // -------------------------------------------------------------------
    // Safety gate tests: DEX connector must refuse to submit real orders
    // -------------------------------------------------------------------

    /// Helper to build a test NewOrder for Uniswap.
    fn test_order() -> NewOrder {
        NewOrder {
            exchange: Exchange::Uniswap,
            symbol: Symbol::new("WETH", "USDC"),
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            price: None,
            quantity: Decimal::ONE,
            strategy_id: "test-strategy".into(),
            reduce_only: false,
        }
    }

    #[tokio::test]
    async fn test_submit_order_returns_dex_not_ready_error() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        // submit_order must fail even without connecting first — the safety
        // gate fires before any connectivity check.
        let result = connector.submit_order(&test_order()).await;
        assert!(
            result.is_err(),
            "submit_order should return an error, not submit a real transaction"
        );
        let err = result.unwrap_err();
        match &err {
            MmError::DexNotReady { exchange, message } => {
                assert_eq!(*exchange, Exchange::Uniswap);
                assert!(
                    message.contains("experimental"),
                    "error message should mention the connector is experimental, got: {}",
                    message
                );
                assert!(
                    message.contains("production-ready"),
                    "error message should mention production-readiness, got: {}",
                    message
                );
            }
            other => panic!("expected DexNotReady error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_cancel_order_returns_dex_not_ready_error() {
        let config = full_test_config();
        let connector = UniswapExecutionConnector::new(config);
        let cancel = CancelOrder {
            exchange: Exchange::Uniswap,
            symbol: Symbol::new("WETH", "USDC"),
            client_order_id: "test-order-123".into(),
            exchange_order_id: Some("0xdeadbeef".into()),
        };
        let result = connector.cancel_order(&cancel).await;
        assert!(
            result.is_err(),
            "cancel_order should return an error, not attempt a real cancellation"
        );
        let err = result.unwrap_err();
        match &err {
            MmError::DexNotReady { exchange, message } => {
                assert_eq!(*exchange, Exchange::Uniswap);
                assert!(
                    message.contains("experimental"),
                    "error message should mention the connector is experimental, got: {}",
                    message
                );
            }
            other => panic!("expected DexNotReady error, got: {:?}", other),
        }
    }
}
