//! Raydium (Solana DEX) execution connector.
//!
//! Implements [`ExecutionConnector`] for executing swaps on the Raydium AMM
//! via Solana JSON-RPC. Unlike traditional CEX connectors, DEX "orders" are
//! atomic blockchain transactions — there is no concept of resting limit orders.
//!
//! # Approach
//!
//! - **Submit order (swap)**: Constructs a Raydium AMM swap instruction and
//!   submits it via `sendTransaction` JSON-RPC. The instruction includes:
//!   - amountIn: quantity of the source token to swap
//!   - minimumAmountOut: minimum acceptable output (slippage protection)
//!   - The full set of accounts required by the Raydium swap instruction
//!
//! - **Cancel**: Solana transactions are atomic and final — cancellation is
//!   not possible. Returns `Ok(())` as a no-op.
//!
//! - **Cancel all**: Same as cancel — returns `Ok(0)`.
//!
//! - **Balances**: Uses `getTokenAccountsByOwner` RPC for SPL token balances
//!   and `getBalance` for native SOL.
//!
//! - **Order tracking**: Polls `getTransaction` to check confirmation status.
//!   The background task monitors pending transactions and sends `OrderUpdate`
//!   events through the provided channel.
//!
//! # Transaction building
//!
//! Full transaction serialization and Ed25519 signing require either the
//! `solana-sdk` crate or a manual implementation of the Solana transaction
//! wire format. This connector provides a structured placeholder that:
//! 1. Fetches the recent blockhash
//! 2. Constructs swap parameters (amountIn, minimumAmountOut)
//! 3. Documents the complete account list for the Raydium swap instruction
//! 4. Has clear TODOs for the actual serialization and signing steps

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use mm_core::{
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, OrderStatus, OrderUpdate,
    Side, Symbol,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Raydium AMM V4 program ID on Solana mainnet.
const RAYDIUM_AMM_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// SPL Token program ID.
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// System program ID.
#[allow(dead_code)]
const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Serum DEX program ID (used by Raydium for order book routing).
const SERUM_PROGRAM_ID: &str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX";

/// Default slippage tolerance in basis points (50 bps = 0.5%).
const DEFAULT_SLIPPAGE_BPS: u64 = 50;

/// Polling interval for transaction confirmation checks.
const TX_POLL_INTERVAL_SECS: u64 = 2;

/// Maximum number of polls before considering a transaction failed.
const MAX_TX_POLLS: u32 = 30;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Raydium execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumExecutionConfig {
    /// Solana JSON-RPC HTTP endpoint.
    pub rpc_http_url: String,

    /// Wallet address (base58-encoded Solana public key).
    pub wallet_address: String,

    /// Path to the Solana keypair JSON file for transaction signing.
    pub keypair_path: String,

    /// Mapping from canonical symbol string ("SOL/USDC") to Raydium AMM
    /// pool account address (base58).
    pub pool_addresses: HashMap<String, String>,

    /// Mapping from token ticker to its on-chain decimal count.
    pub token_decimals: HashMap<String, u8>,

    /// Slippage tolerance in basis points. Defaults to 50 (0.5%).
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u64,
}

fn default_slippage_bps() -> u64 {
    DEFAULT_SLIPPAGE_BPS
}

// ---------------------------------------------------------------------------
// Pending transaction tracking
// ---------------------------------------------------------------------------

/// Tracks a submitted transaction awaiting confirmation.
#[derive(Debug, Clone)]
struct PendingTransaction {
    /// Solana transaction signature (base58 encoded).
    signature: String,
    /// Our internal client order ID.
    client_order_id: String,
    /// The trading symbol.
    symbol: Symbol,
    /// The order side.
    side: Side,
    /// The quantity submitted.
    quantity: Decimal,
    /// The expected price (for order update reporting).
    expected_price: Option<Decimal>,
    /// Number of poll attempts so far.
    polls: u32,
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

/// Raydium AMM execution connector.
///
/// Executes swaps on the Raydium decentralized exchange by building and
/// submitting Solana transactions via JSON-RPC.
pub struct RaydiumExecutionConnector {
    config: RaydiumExecutionConfig,
    http: reqwest::Client,
    connected: Arc<AtomicBool>,
    /// Channel sender for order updates (set on connect).
    update_tx: Option<mpsc::Sender<OrderUpdate>>,
    /// Background task that polls for transaction confirmations.
    poll_handle: Option<JoinHandle<()>>,
    /// Pending transactions awaiting confirmation.
    pending_txs: Arc<Mutex<Vec<PendingTransaction>>>,
}

impl RaydiumExecutionConnector {
    pub fn new(config: RaydiumExecutionConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            update_tx: None,
            poll_handle: None,
            pending_txs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // -----------------------------------------------------------------------
    // Solana JSON-RPC helpers
    // -----------------------------------------------------------------------

    /// Call a Solana JSON-RPC method and return the parsed response.
    async fn rpc_call(
        http: &reqwest::Client,
        rpc_url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> MmResult<serde_json::Value> {
        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        });

        let resp = http
            .post(rpc_url)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("RPC request '{}' failed: {}", method, e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("RPC '{}' returned status {}", method, resp.status()),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Raydium,
            message: format!("failed to parse RPC '{}' response: {}", method, e),
        })?;

        if let Some(error) = body.get("error") {
            return Err(MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("RPC '{}' error: {}", method, error),
            });
        }

        Ok(body)
    }

    /// Fetch the latest blockhash via `getLatestBlockhash`.
    async fn get_latest_blockhash(
        http: &reqwest::Client,
        rpc_url: &str,
    ) -> MmResult<String> {
        let resp = Self::rpc_call(
            http,
            rpc_url,
            "getLatestBlockhash",
            serde_json::json!([{"commitment": "finalized"}]),
        )
        .await?;

        resp["result"]["value"]["blockhash"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "blockhash not found in response".into(),
            })
    }

    /// Check the status of a transaction via `getTransaction`.
    async fn get_transaction_status(
        http: &reqwest::Client,
        rpc_url: &str,
        signature: &str,
    ) -> MmResult<Option<bool>> {
        let resp = Self::rpc_call(
            http,
            rpc_url,
            "getTransaction",
            serde_json::json!([
                signature,
                {
                    "encoding": "json",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]),
        )
        .await?;

        let result = &resp["result"];
        if result.is_null() {
            // Transaction not yet found — still pending.
            return Ok(None);
        }

        // Check if the transaction had an error.
        let meta = &result["meta"];
        if meta.is_null() {
            return Ok(None);
        }

        let err = &meta["err"];
        if err.is_null() {
            // No error — transaction succeeded.
            Ok(Some(true))
        } else {
            // Transaction had an error — it was rejected/failed.
            Ok(Some(false))
        }
    }

    /// Fetch native SOL balance via `getBalance`.
    async fn get_sol_balance(
        http: &reqwest::Client,
        rpc_url: &str,
        wallet: &str,
    ) -> MmResult<Decimal> {
        let resp = Self::rpc_call(
            http,
            rpc_url,
            "getBalance",
            serde_json::json!([wallet, {"commitment": "confirmed"}]),
        )
        .await?;

        let lamports = resp["result"]["value"]
            .as_u64()
            .unwrap_or(0);

        // SOL has 9 decimal places.
        let sol_divisor = Decimal::from(1_000_000_000u64);
        Ok(Decimal::from(lamports) / sol_divisor)
    }

    /// Fetch SPL token balances via `getTokenAccountsByOwner`.
    async fn get_token_balances(
        http: &reqwest::Client,
        rpc_url: &str,
        wallet: &str,
    ) -> MmResult<Vec<TokenAccountInfo>> {
        let resp = Self::rpc_call(
            http,
            rpc_url,
            "getTokenAccountsByOwner",
            serde_json::json!([
                wallet,
                {
                    "programId": SPL_TOKEN_PROGRAM_ID
                },
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]),
        )
        .await?;

        let accounts = resp["result"]["value"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut result = Vec::new();
        for account in &accounts {
            let info = &account["account"]["data"]["parsed"]["info"];
            let mint = info["mint"].as_str().unwrap_or_default().to_string();
            let amount_str = info["tokenAmount"]["uiAmountString"]
                .as_str()
                .unwrap_or("0");
            let amount: Decimal = amount_str.parse().unwrap_or(Decimal::ZERO);
            let decimals = info["tokenAmount"]["decimals"]
                .as_u64()
                .unwrap_or(0) as u8;

            if amount > Decimal::ZERO {
                result.push(TokenAccountInfo {
                    mint,
                    amount,
                    decimals,
                });
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Swap instruction building
    // -----------------------------------------------------------------------

    /// Build and submit a Raydium AMM swap transaction.
    ///
    /// This method:
    /// 1. Fetches the latest blockhash
    /// 2. Computes amountIn and minimumAmountOut from order parameters
    /// 3. Documents all accounts needed for the Raydium swap instruction
    ///
    /// TODO: Complete the actual transaction serialization and signing.
    /// The Solana transaction wire format requires:
    /// - Compact-u16 encoding for array lengths
    /// - Ed25519 signature(s)
    /// - Message header (num_required_signatures, num_readonly_signed, num_readonly_unsigned)
    /// - Account addresses array
    /// - Recent blockhash
    /// - Instructions array (program_id_index, account_indices, data)
    async fn build_and_submit_swap(
        &self,
        order: &NewOrder,
    ) -> MmResult<String> {
        let canonical = order.symbol.canonical();

        let pool_address = self
            .config
            .pool_addresses
            .get(&canonical)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("no pool address configured for {}", canonical),
            })?;

        // Determine source/destination token based on swap direction.
        let (source_token, dest_token) = match order.side {
            Side::Buy => (&order.symbol.quote, &order.symbol.base),
            Side::Sell => (&order.symbol.base, &order.symbol.quote),
        };

        let source_decimals = *self
            .config
            .token_decimals
            .get(source_token)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("no decimals configured for token {}", source_token),
            })?;

        let _dest_decimals = *self
            .config
            .token_decimals
            .get(dest_token)
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("no decimals configured for token {}", dest_token),
            })?;

        // Compute the raw amount_in (in smallest token units).
        let amount_in_raw = if order.side == Side::Buy {
            // For a buy: we're spending quote to get base. If the order has a
            // price, amount_in = quantity * price (in quote tokens).
            let quote_amount = order.quantity
                * order.price.unwrap_or(Decimal::ZERO);
            (quote_amount * Decimal::from(10u64.pow(source_decimals as u32)))
                .to_string()
                .split('.')
                .next()
                .unwrap_or("0")
                .to_string()
        } else {
            // For a sell: we're spending base to get quote.
            (order.quantity * Decimal::from(10u64.pow(source_decimals as u32)))
                .to_string()
                .split('.')
                .next()
                .unwrap_or("0")
                .to_string()
        };

        // Compute minimum_amount_out with slippage tolerance.
        // minimum_out = expected_out * (1 - slippage_bps / 10000)
        let slippage_factor = Decimal::from(10_000 - self.config.slippage_bps)
            / Decimal::from(10_000u64);

        let expected_out = if order.side == Side::Buy {
            order.quantity
        } else {
            order.quantity * order.price.unwrap_or(Decimal::ZERO)
        };

        let minimum_out = expected_out * slippage_factor;
        let minimum_out_raw = (minimum_out
            * Decimal::from(10u64.pow(_dest_decimals as u32)))
            .to_string()
            .split('.')
            .next()
            .unwrap_or("0")
            .to_string();

        // Fetch the latest blockhash for the transaction.
        let blockhash = Self::get_latest_blockhash(&self.http, &self.config.rpc_http_url).await?;

        info!(
            pool = %pool_address,
            symbol = %canonical,
            side = %order.side,
            amount_in = %amount_in_raw,
            minimum_out = %minimum_out_raw,
            blockhash = %blockhash,
            "Raydium: prepared swap parameters"
        );

        // ---------------------------------------------------------------
        // Raydium AMM V4 swap instruction accounts (in order):
        //
        //  0. [signer]     User wallet (payer)
        //  1. []            SPL Token program
        //  2. []            Raydium AMM program
        //  3. [writable]    AMM pool account
        //  4. []            AMM authority (PDA)
        //  5. [writable]    AMM open orders (Serum)
        //  6. [writable]    AMM target orders
        //  7. [writable]    Pool coin token account (base)
        //  8. [writable]    Pool pc token account (quote)
        //  9. []            Serum DEX program
        // 10. [writable]    Serum market
        // 11. [writable]    Serum bids
        // 12. [writable]    Serum asks
        // 13. [writable]    Serum event queue
        // 14. [writable]    Serum coin vault
        // 15. [writable]    Serum pc vault
        // 16. []            Serum vault signer (PDA)
        // 17. [writable]    User source token account
        // 18. [writable]    User destination token account
        // 19. [signer]      User wallet (owner of token accounts)
        //
        // Instruction data (9 bytes):
        //   [0]   : instruction discriminator (9 = swap)
        //   [1..9] : amount_in as u64 LE
        //   [9..17]: minimum_amount_out as u64 LE
        // ---------------------------------------------------------------

        // TODO: Implement full transaction serialization.
        //
        // The steps required are:
        // 1. Load the Ed25519 keypair from `self.config.keypair_path`
        // 2. Look up the associated token accounts for the user's wallet
        //    (source and destination SPL token accounts)
        // 3. Look up the Serum market accounts associated with this AMM pool
        //    (market, bids, asks, event queue, coin vault, pc vault, vault signer)
        // 4. Derive the AMM authority PDA
        // 5. Build the instruction data:
        //    - discriminator byte (9)
        //    - amount_in as little-endian u64
        //    - minimum_amount_out as little-endian u64
        // 6. Assemble the transaction message:
        //    - Header: { num_required_signatures: 1, num_readonly_signed: 0,
        //                num_readonly_unsigned: 4 }
        //    - Account keys array (20 pubkeys, 32 bytes each)
        //    - Recent blockhash (32 bytes)
        //    - Instructions: [{ program_id_index, account_indices, data }]
        // 7. Serialize the message using Solana's compact wire format
        // 8. Sign the serialized message with Ed25519
        // 9. Prepend the signature to the serialized message
        // 10. Base58-encode the signed transaction
        // 11. Submit via `sendTransaction` RPC with `skipPreflight: false`
        //
        // For now, return a placeholder that simulates submission.
        // In production, replace this block with actual transaction building.

        let tx_signature = format!("raydium-pending-{}", Uuid::new_v4());

        warn!(
            signature = %tx_signature,
            "Raydium: transaction serialization not yet implemented — \
             returning placeholder signature. See TODO in build_and_submit_swap()."
        );

        Ok(tx_signature)
    }

    // -----------------------------------------------------------------------
    // Transaction confirmation polling
    // -----------------------------------------------------------------------

    /// Spawn a background task that polls for pending transaction confirmations.
    fn spawn_tx_poller(
        http: reqwest::Client,
        rpc_url: String,
        pending_txs: Arc<Mutex<Vec<PendingTransaction>>>,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(TX_POLL_INTERVAL_SECS));

            loop {
                interval.tick().await;

                if !connected.load(Ordering::SeqCst) {
                    info!("Raydium: tx poller stopping — not connected");
                    break;
                }

                let mut txs = pending_txs.lock().await;
                let mut completed_indices: Vec<usize> = Vec::new();

                for (idx, tx) in txs.iter_mut().enumerate() {
                    tx.polls += 1;

                    match Self::get_transaction_status(&http, &rpc_url, &tx.signature).await {
                        Ok(Some(true)) => {
                            // Transaction confirmed successfully.
                            info!(
                                signature = %tx.signature,
                                client_order_id = %tx.client_order_id,
                                "Raydium: transaction confirmed"
                            );

                            let update = OrderUpdate {
                                client_order_id: tx.client_order_id.clone(),
                                exchange_order_id: Some(tx.signature.clone()),
                                exchange: Exchange::Raydium,
                                symbol: tx.symbol.clone(),
                                status: OrderStatus::Filled,
                                filled_quantity: tx.quantity,
                                remaining_quantity: Decimal::ZERO,
                                average_fill_price: tx.expected_price.unwrap_or(Decimal::ZERO),
                                last_fill_price: tx.expected_price,
                                last_fill_quantity: Some(tx.quantity),
                                fee: None, // Fees would need to be parsed from tx meta
                                fee_currency: Some("SOL".to_string()),
                                timestamp: Utc::now(),
                            };

                            if let Err(e) = update_tx.send(update).await {
                                error!(error = %e, "Raydium: failed to send order update");
                            }

                            completed_indices.push(idx);
                        }
                        Ok(Some(false)) => {
                            // Transaction confirmed but failed.
                            warn!(
                                signature = %tx.signature,
                                client_order_id = %tx.client_order_id,
                                "Raydium: transaction failed on-chain"
                            );

                            let update = OrderUpdate {
                                client_order_id: tx.client_order_id.clone(),
                                exchange_order_id: Some(tx.signature.clone()),
                                exchange: Exchange::Raydium,
                                symbol: tx.symbol.clone(),
                                status: OrderStatus::Rejected,
                                filled_quantity: Decimal::ZERO,
                                remaining_quantity: tx.quantity,
                                average_fill_price: Decimal::ZERO,
                                last_fill_price: None,
                                last_fill_quantity: None,
                                fee: None,
                                fee_currency: None,
                                timestamp: Utc::now(),
                            };

                            if let Err(e) = update_tx.send(update).await {
                                error!(error = %e, "Raydium: failed to send order update");
                            }

                            completed_indices.push(idx);
                        }
                        Ok(None) => {
                            // Not yet confirmed.
                            if tx.polls >= MAX_TX_POLLS {
                                warn!(
                                    signature = %tx.signature,
                                    polls = tx.polls,
                                    "Raydium: transaction timed out waiting for confirmation"
                                );

                                let update = OrderUpdate {
                                    client_order_id: tx.client_order_id.clone(),
                                    exchange_order_id: Some(tx.signature.clone()),
                                    exchange: Exchange::Raydium,
                                    symbol: tx.symbol.clone(),
                                    status: OrderStatus::Expired,
                                    filled_quantity: Decimal::ZERO,
                                    remaining_quantity: tx.quantity,
                                    average_fill_price: Decimal::ZERO,
                                    last_fill_price: None,
                                    last_fill_quantity: None,
                                    fee: None,
                                    fee_currency: None,
                                    timestamp: Utc::now(),
                                };

                                if let Err(e) = update_tx.send(update).await {
                                    error!(error = %e, "Raydium: failed to send order update");
                                }

                                completed_indices.push(idx);
                            } else {
                                debug!(
                                    signature = %tx.signature,
                                    polls = tx.polls,
                                    "Raydium: transaction still pending"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                signature = %tx.signature,
                                "Raydium: error checking transaction status"
                            );
                        }
                    }
                }

                // Remove completed transactions in reverse order to preserve indices.
                completed_indices.sort_unstable();
                for idx in completed_indices.into_iter().rev() {
                    txs.swap_remove(idx);
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // Token address resolution helper
    // -----------------------------------------------------------------------

    /// Well-known SPL token mint addresses for common tokens.
    /// Used to map token tickers to their on-chain mint addresses
    /// when fetching balances.
    fn known_mint_to_ticker(mint: &str) -> Option<&'static str> {
        match mint {
            "So11111111111111111111111111111111111111112" => Some("SOL"),
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => Some("USDC"),
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => Some("USDT"),
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So" => Some("MSOL"),
            "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs" => Some("ETH"),
            "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj" => Some("STSOL"),
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" => Some("BONK"),
            "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN" => Some("JUP"),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// SPL token account info (for balance queries)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TokenAccountInfo {
    mint: String,
    amount: Decimal,
    #[allow(dead_code)]
    decimals: u8,
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for RaydiumExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Raydium
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Raydium: connecting execution connector");

        // Verify we can reach the RPC endpoint.
        Self::get_latest_blockhash(&self.http, &self.config.rpc_http_url)
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Raydium,
                message: format!("failed to connect to Solana RPC: {}", e),
            })?;

        self.connected.store(true, Ordering::SeqCst);
        self.update_tx = Some(update_tx.clone());

        // Start the transaction confirmation poller.
        self.poll_handle = Some(Self::spawn_tx_poller(
            self.http.clone(),
            self.config.rpc_http_url.clone(),
            Arc::clone(&self.pending_txs),
            update_tx,
            Arc::clone(&self.connected),
        ));

        info!("Raydium: execution connector connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Raydium: disconnecting execution connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.poll_handle.take() {
            h.abort();
        }
        self.update_tx = None;
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "not connected".into(),
            });
        }

        let client_order_id = Uuid::new_v4().to_string();

        info!(
            client_order_id = %client_order_id,
            symbol = %order.symbol,
            side = %order.side,
            quantity = %order.quantity,
            price = ?order.price,
            "Raydium: submitting swap order"
        );

        // Send initial "Submitted" status.
        if let Some(ref tx) = self.update_tx {
            let update = OrderUpdate {
                client_order_id: client_order_id.clone(),
                exchange_order_id: None,
                exchange: Exchange::Raydium,
                symbol: order.symbol.clone(),
                status: OrderStatus::Submitted,
                filled_quantity: Decimal::ZERO,
                remaining_quantity: order.quantity,
                average_fill_price: Decimal::ZERO,
                last_fill_price: None,
                last_fill_quantity: None,
                fee: None,
                fee_currency: None,
                timestamp: Utc::now(),
            };
            let _ = tx.send(update).await;
        }

        // Build and submit the swap transaction.
        let signature = self.build_and_submit_swap(order).await?;

        info!(
            client_order_id = %client_order_id,
            signature = %signature,
            "Raydium: swap transaction submitted"
        );

        // Track the pending transaction for confirmation polling.
        {
            let mut pending = self.pending_txs.lock().await;
            pending.push(PendingTransaction {
                signature: signature.clone(),
                client_order_id: client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: order.side,
                quantity: order.quantity,
                expected_price: order.price,
                polls: 0,
            });
        }

        // Send "New" status with the transaction signature.
        if let Some(ref tx) = self.update_tx {
            let update = OrderUpdate {
                client_order_id: client_order_id.clone(),
                exchange_order_id: Some(signature),
                exchange: Exchange::Raydium,
                symbol: order.symbol.clone(),
                status: OrderStatus::New,
                filled_quantity: Decimal::ZERO,
                remaining_quantity: order.quantity,
                average_fill_price: Decimal::ZERO,
                last_fill_price: None,
                last_fill_quantity: None,
                fee: None,
                fee_currency: None,
                timestamp: Utc::now(),
            };
            let _ = tx.send(update).await;
        }

        Ok(client_order_id)
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        // Solana transactions are atomic — once submitted, they either confirm
        // or expire. There is no way to cancel a pending transaction.
        info!(
            client_order_id = %cancel.client_order_id,
            "Raydium: cancel_order is a no-op (Solana transactions are atomic)"
        );
        Ok(())
    }

    async fn cancel_all_orders(&self, _request: &CancelAllOrders) -> MmResult<u32> {
        // Same as cancel_order — Solana AMM swaps are atomic transactions.
        info!("Raydium: cancel_all_orders is a no-op (Solana transactions are atomic)");
        Ok(0)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(MmError::Exchange {
                exchange: Exchange::Raydium,
                message: "not connected".into(),
            });
        }

        let mut balances: Vec<Balance> = Vec::new();

        // Fetch native SOL balance.
        let sol_balance = Self::get_sol_balance(
            &self.http,
            &self.config.rpc_http_url,
            &self.config.wallet_address,
        )
        .await?;

        if sol_balance > Decimal::ZERO {
            balances.push(Balance {
                currency: "SOL".to_string(),
                free: sol_balance,
                locked: Decimal::ZERO,
                total: sol_balance,
            });
        }

        // Fetch SPL token balances.
        let token_accounts = Self::get_token_balances(
            &self.http,
            &self.config.rpc_http_url,
            &self.config.wallet_address,
        )
        .await?;

        for account in token_accounts {
            let ticker = Self::known_mint_to_ticker(&account.mint)
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    // Use abbreviated mint address if ticker is unknown.
                    let mint = &account.mint;
                    if mint.len() > 8 {
                        format!("{}...", &mint[..8])
                    } else {
                        mint.clone()
                    }
                });

            balances.push(Balance {
                currency: ticker,
                free: account.amount,
                locked: Decimal::ZERO,
                total: account.amount,
            });
        }

        debug!(count = balances.len(), "Raydium: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}
