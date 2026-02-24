//! Kraken execution connector.
//!
//! Implements [`ExecutionConnector`] for the Kraken exchange.
//!
//! ## REST API
//!
//! | Endpoint                    | Method  | Description            |
//! |-----------------------------|---------|------------------------|
//! | `/0/private/AddOrder`       | `POST`  | Place a new order      |
//! | `/0/private/CancelOrder`    | `POST`  | Cancel an order        |
//! | `/0/private/CancelAll`      | `POST`  | Cancel all open orders |
//! | `/0/private/Balance`        | `POST`  | Fetch account balances |
//! | `/0/private/GetWebSocketsToken` | `POST` | Get WS auth token  |
//!
//! ## Authentication
//!
//! Kraken uses a nonce-based HMAC-SHA512 scheme:
//!
//! 1. `nonce` = ever-increasing integer (we use millisecond timestamp).
//! 2. `API-Sign` = Base64(HMAC-SHA512(Base64Decode(secret), path + SHA256(nonce + POST_data)))
//! 3. `API-Key` header = public API key.
//!
//! ## WebSocket v2
//!
//! The private WebSocket stream (`wss://ws-auth.kraken.com/v2`) provides
//! real-time order updates on the `executions` channel. Authentication is via
//! a token obtained from the REST endpoint `/0/private/GetWebSocketsToken`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use mm_core::{
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, OrderStatus, OrderUpdate,
    Side, Symbol,
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};

type HmacSha512 = Hmac<Sha512>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Kraken execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KrakenExecutionConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_base_url() -> String {
    "https://api.kraken.com".into()
}

fn default_ws_url() -> String {
    "wss://ws-auth.kraken.com/v2".into()
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct KrakenExecutionConnector {
    config: KrakenExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    ws_handle: Option<JoinHandle<()>>,
    /// Monotonically increasing nonce. Protected by parking_lot Mutex for
    /// minimal contention on the hot path.
    nonce: Arc<parking_lot::Mutex<u64>>,
}

impl KrakenExecutionConnector {
    pub fn new(config: KrakenExecutionConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            ws_handle: None,
            nonce: Arc::new(parking_lot::Mutex::new(
                Utc::now().timestamp_millis() as u64,
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Nonce
    // -----------------------------------------------------------------------

    fn next_nonce(&self) -> u64 {
        let mut n = self.nonce.lock();
        *n += 1;
        *n
    }

    // -----------------------------------------------------------------------
    // Signature
    // -----------------------------------------------------------------------

    /// Generate the `API-Sign` header.
    ///
    /// ```text
    /// API-Sign = Base64(HMAC-SHA512(Base64Decode(secret), path + SHA256(nonce + post_data)))
    /// ```
    fn sign(&self, path: &str, nonce: u64, post_data: &str) -> MmResult<String> {
        // Kraken secrets may be standard OR URL-safe base64 depending on how
        // they were generated. Try standard first, then URL-safe.
        let secret_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &self.config.secret_key,
        )
        .or_else(|_| {
            base64::Engine::decode(
                &base64::engine::general_purpose::URL_SAFE,
                &self.config.secret_key,
            )
        })
        .map_err(|e| MmError::AuthenticationFailed {
            exchange: Exchange::Kraken,
            message: format!("invalid base64 secret key: {}", e),
        })?;

        // SHA256(nonce + post_data)
        let mut sha = Sha256::new();
        sha.update(format!("{}{}", nonce, post_data).as_bytes());
        let sha_digest = sha.finalize();

        // HMAC-SHA512(decoded_secret, path + sha256_digest)
        let mut mac = HmacSha512::new_from_slice(&secret_bytes).map_err(|e| {
            MmError::AuthenticationFailed {
                exchange: Exchange::Kraken,
                message: format!("HMAC init error: {}", e),
            }
        })?;
        mac.update(path.as_bytes());
        mac.update(&sha_digest);

        let result = mac.finalize().into_bytes();
        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            result,
        ))
    }

    /// Execute an authenticated POST request to a Kraken private endpoint.
    async fn private_post(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> MmResult<serde_json::Value> {
        let nonce = self.next_nonce();

        let mut form_params: Vec<(&str, String)> = vec![("nonce", nonce.to_string())];
        form_params.extend(params.iter().map(|(k, v)| (*k, v.clone())));

        let post_data: String = form_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let signature = self.sign(path, nonce, &post_data)?;

        let url = format!("{}{}", self.config.base_url, path);
        let resp = self
            .http
            .post(&url)
            .header("API-Key", &self.config.api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("parse error: {}", e),
            })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("HTTP {} : {}", status, body),
            });
        }

        // Kraken wraps errors in the "error" array.
        if let Some(errors) = body["error"].as_array() {
            let real_errors: Vec<&str> = errors
                .iter()
                .filter_map(|e| e.as_str())
                .filter(|e| !e.starts_with("W")) // "W" prefix = warnings
                .collect();
            if !real_errors.is_empty() {
                return Err(MmError::Exchange {
                    exchange: Exchange::Kraken,
                    message: real_errors.join("; "),
                });
            }
        }

        Ok(body["result"].clone())
    }

    // -----------------------------------------------------------------------
    // WebSocket token
    // -----------------------------------------------------------------------

    async fn get_ws_token(&self) -> MmResult<String> {
        let result = self.private_post("/0/private/GetWebSocketsToken", &[]).await?;
        result["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: "no token in GetWebSocketsToken response".into(),
            })
    }

    // -----------------------------------------------------------------------
    // Symbol conversion helpers
    // -----------------------------------------------------------------------

    /// Convert our canonical `Symbol` to the Kraken pair string.
    ///
    /// Kraken uses various conventions. For the REST API we use the "X..Z.."
    /// form for major assets and plain concatenation otherwise. For simplicity,
    /// we send the concatenated form which the API also accepts.
    fn to_kraken_pair(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote)
    }

    /// Best-effort conversion from a Kraken pair back to our `Symbol`.
    fn from_kraken_pair(pair: &str) -> Symbol {
        // Strip the leading X/Z prefixes Kraken sometimes uses.
        let cleaned = pair
            .strip_prefix('X')
            .or_else(|| Some(pair))
            .unwrap_or(pair);

        for quote in &["USDT", "ZUSD", "USD", "EUR", "XBT", "ETH"] {
            if cleaned.ends_with(quote) {
                let base = &cleaned[..cleaned.len() - quote.len()];
                if !base.is_empty() {
                    let normalized_quote = match *quote {
                        "ZUSD" => "USD",
                        "XBT" => "BTC",
                        other => other,
                    };
                    let normalized_base = if base == "XXBT" || base == "XBT" {
                        "BTC"
                    } else {
                        base.strip_prefix('X').unwrap_or(base)
                    };
                    return Symbol::new(normalized_base, normalized_quote);
                }
            }
        }
        Symbol::new(pair, "UNKNOWN")
    }

    // -----------------------------------------------------------------------
    // WebSocket v2
    // -----------------------------------------------------------------------

    fn spawn_ws_listener(
        ws_url: String,
        token: String,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Kraken: connecting private WebSocket v2");

            let ws_stream = match tokio_tungstenite::connect_async(&ws_url).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(error = %e, "Kraken: WebSocket connection failed");
                    connected.store(false, Ordering::SeqCst);
                    return;
                }
            };

            let (mut write, mut read) = ws_stream.split();

            // Subscribe to the executions channel (WebSocket v2 format).
            let subscribe = serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "executions",
                    "token": token,
                    "snap_orders": false,
                }
            });

            if let Err(e) = write
                .send(WsMessage::Text(subscribe.to_string().into()))
                .await
            {
                error!(error = %e, "Kraken: failed to send subscribe");
                connected.store(false, Ordering::SeqCst);
                return;
            }

            info!("Kraken: subscribed to executions channel");

            while let Some(msg_result) = read.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "Kraken: WebSocket read error");
                        break;
                    }
                };

                match msg {
                    WsMessage::Text(text) => {
                        if let Err(e) = Self::handle_ws_message(&text, &update_tx).await {
                            warn!(error = %e, "Kraken: failed to process WS message");
                        }
                    }
                    WsMessage::Ping(data) => {
                        if let Err(e) = write.send(WsMessage::Pong(data)).await {
                            warn!(error = %e, "Kraken: failed to send pong");
                        }
                    }
                    WsMessage::Close(_) => {
                        info!("Kraken: WebSocket closed by server");
                        break;
                    }
                    _ => {}
                }
            }

            connected.store(false, Ordering::SeqCst);
            warn!("Kraken: private WebSocket disconnected");
        })
    }

    async fn handle_ws_message(
        text: &str,
        update_tx: &mpsc::Sender<OrderUpdate>,
    ) -> MmResult<()> {
        let msg: serde_json::Value =
            serde_json::from_str(text).map_err(|e| MmError::Exchange {
                exchange: Exchange::Kraken,
                message: format!("JSON parse error: {}", e),
            })?;

        let channel = msg["channel"].as_str().unwrap_or("");
        if channel != "executions" {
            return Ok(());
        }

        let msg_type = msg["type"].as_str().unwrap_or("");
        if msg_type != "update" && msg_type != "snapshot" {
            return Ok(());
        }

        let data = match msg["data"].as_array() {
            Some(d) => d,
            None => return Ok(()),
        };

        for item in data {
            if let Some(update) = Self::parse_execution(item)? {
                debug!(
                    client_order_id = %update.client_order_id,
                    status = %update.status,
                    "Kraken: execution update received"
                );
                update_tx
                    .send(update)
                    .await
                    .map_err(|e| MmError::Exchange {
                        exchange: Exchange::Kraken,
                        message: format!("channel send error: {}", e),
                    })?;
            }
        }

        Ok(())
    }

    fn parse_execution(item: &serde_json::Value) -> MmResult<Option<OrderUpdate>> {
        let order_id = item["order_id"].as_str().unwrap_or_default().to_string();
        let client_order_id = item["cl_ord_id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| order_id.clone());
        let exchange_order_id = Some(order_id);

        let symbol_str = item["symbol"].as_str().unwrap_or("");
        let symbol = Self::from_kraken_pair(symbol_str);

        let status_str = item["ord_status"]
            .as_str()
            .or_else(|| item["order_status"].as_str())
            .unwrap_or("");

        let status = match status_str {
            "new" => OrderStatus::New,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "filled" => OrderStatus::Filled,
            "canceled" | "cancelled" => OrderStatus::Cancelled,
            "expired" => OrderStatus::Expired,
            "pending" => OrderStatus::Submitted,
            _ => return Ok(None),
        };

        let filled_quantity = item["cum_qty"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let total_quantity = item["order_qty"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let remaining = total_quantity - filled_quantity;

        let average_fill_price = item["avg_price"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let last_fill_price = item["last_price"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let last_fill_quantity = item["last_qty"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let fee = item["fee_paid"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let fee_currency = item["fee_paid_asset"].as_str().map(|s| s.to_string());

        let timestamp = item["timestamp"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Ok(Some(OrderUpdate {
            client_order_id,
            exchange_order_id,
            exchange: Exchange::Kraken,
            symbol,
            status,
            filled_quantity,
            remaining_quantity: remaining,
            average_fill_price,
            last_fill_price,
            last_fill_quantity,
            fee,
            fee_currency,
            timestamp,
        }))
    }

    // -----------------------------------------------------------------------
    // Order helpers
    // -----------------------------------------------------------------------

    fn side_to_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    fn order_type_to_str(ot: mm_core::OrderType) -> &'static str {
        match ot {
            mm_core::OrderType::Market => "market",
            mm_core::OrderType::Limit => "limit",
            mm_core::OrderType::StopLimit => "stop-loss-limit",
            mm_core::OrderType::StopMarket => "stop-loss",
        }
    }
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for KrakenExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Kraken
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Kraken: connecting execution connector");

        let token = self.get_ws_token().await?;
        info!("Kraken: obtained WebSocket token");

        self.connected.store(true, Ordering::SeqCst);

        self.ws_handle = Some(Self::spawn_ws_listener(
            self.config.ws_url.clone(),
            token,
            update_tx,
            Arc::clone(&self.connected),
        ));

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Kraken: disconnecting");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        let pair = Self::to_kraken_pair(&order.symbol);
        let side = Self::side_to_str(order.side);
        let order_type = Self::order_type_to_str(order.order_type);
        let volume = order.quantity.to_string();

        let mut params: Vec<(&str, String)> = vec![
            ("pair", pair.clone()),
            ("type", side.to_string()),
            ("ordertype", order_type.to_string()),
            ("volume", volume),
        ];

        if let Some(ref price) = order.price {
            params.push(("price", price.to_string()));
        }

        // Use a client order ID so we can correlate.
        let cl_ord_id = uuid::Uuid::new_v4().to_string();
        params.push(("cl_ord_id", cl_ord_id.clone()));

        if order.reduce_only {
            params.push(("reduce_only", "true".to_string()));
        }

        let result = self.private_post("/0/private/AddOrder", &params).await?;

        let txid = result["txid"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(cl_ord_id);

        info!(
            exchange_order_id = %txid,
            pair = %pair,
            side = %side,
            "Kraken: order submitted"
        );

        Ok(txid)
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        let txid = cancel
            .exchange_order_id
            .as_deref()
            .unwrap_or(&cancel.client_order_id);

        let params = vec![("txid", txid.to_string())];
        self.private_post("/0/private/CancelOrder", &params).await?;

        info!(
            client_order_id = %cancel.client_order_id,
            "Kraken: order cancelled"
        );
        Ok(())
    }

    async fn cancel_all_orders(&self, _request: &CancelAllOrders) -> MmResult<u32> {
        let result = self.private_post("/0/private/CancelAll", &[]).await?;
        let count = result["count"].as_u64().unwrap_or(0) as u32;
        info!(count, "Kraken: cancel_all completed");
        Ok(count)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let result = self.private_post("/0/private/Balance", &[]).await?;

        let balances: Vec<Balance> = result
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter_map(|(currency, value)| {
                        let total: Decimal = value.as_str()?.parse().ok()?;
                        if total > Decimal::ZERO {
                            // Kraken reports only total; free/locked breakdown
                            // requires TradeBalance. We approximate here.
                            let normalized = match currency.as_str() {
                                "XXBT" | "XBT" => "BTC".to_string(),
                                "XETH" => "ETH".to_string(),
                                "ZUSD" => "USD".to_string(),
                                "ZEUR" => "EUR".to_string(),
                                other => other.to_string(),
                            };
                            Some(Balance {
                                currency: normalized,
                                free: total,
                                locked: Decimal::ZERO,
                                total,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        debug!(count = balances.len(), "Kraken: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}
