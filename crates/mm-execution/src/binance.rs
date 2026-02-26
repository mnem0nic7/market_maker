//! Binance execution connector.
//!
//! Implements [`ExecutionConnector`] for the Binance spot exchange.
//!
//! ## REST API
//!
//! | Endpoint                  | Method   | Description            |
//! |---------------------------|----------|------------------------|
//! | `/api/v3/order`           | `POST`   | Place a new order      |
//! | `/api/v3/order`           | `DELETE`  | Cancel an order        |
//! | `/api/v3/openOrders`      | `DELETE`  | Cancel all open orders |
//! | `/api/v3/account`         | `GET`    | Fetch account balances |
//! | `/api/v3/userDataStream`  | `POST`   | Create a listen key    |
//! | `/api/v3/userDataStream`  | `PUT`    | Keep-alive listen key  |
//!
//! ## Authentication
//!
//! - Every signed request appends `timestamp` and `signature` to the query string.
//! - The signature is `HMAC-SHA256(secret_key, query_string)`.
//! - The `X-MBX-APIKEY` header carries the public API key.
//!
//! ## WebSocket
//!
//! A single user-data stream delivers `executionReport` events for real-time
//! order updates. The listen key is obtained via REST and kept alive with a
//! periodic PUT every 30 minutes.

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
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Binance execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceExecutionConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_base_url() -> String {
    "https://api.binance.us".into()
}

fn default_ws_url() -> String {
    "wss://stream.binance.us:9443".into()
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct BinanceExecutionConnector {
    config: BinanceExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    ws_handle: Option<JoinHandle<()>>,
    keepalive_handle: Option<JoinHandle<()>>,
}

impl BinanceExecutionConnector {
    pub fn new(config: BinanceExecutionConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            ws_handle: None,
            keepalive_handle: None,
        }
    }

    // -----------------------------------------------------------------------
    // Signature
    // -----------------------------------------------------------------------

    /// Sign a query string with HMAC-SHA256 and append &signature=... .
    fn sign_query(&self, query: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.config.secret_key.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        format!("{}&signature={}", query, signature)
    }

    /// Build a query string with a timestamp and sign it.
    fn sign_params(&self, params: &[(&str, &str)]) -> String {
        let ts = Utc::now().timestamp_millis().to_string();
        let mut parts: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        parts.push(format!("timestamp={}", ts));
        let query = parts.join("&");
        self.sign_query(&query)
    }

    // -----------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------

    /// Obtain a listen key for the user-data WebSocket stream.
    async fn create_listen_key(&self) -> MmResult<String> {
        let url = format!("{}/api/v3/userDataStream", self.config.base_url);
        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("listen key request failed: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("listen key parse failed: {}", e),
            })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("listen key error ({}): {}", status, body),
            });
        }

        body["listenKey"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::Binance,
                message: "listenKey not found in response".into(),
            })
    }

    /// Keep-alive: PUT the listen key every 30 minutes.
    #[allow(dead_code)]
    async fn keepalive_listen_key(&self, listen_key: &str) -> MmResult<()> {
        let url = format!(
            "{}/api/v3/userDataStream?listenKey={}",
            self.config.base_url, listen_key
        );
        let resp = self
            .http
            .put(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("keepalive failed: {}", e),
            })?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("keepalive error: {}", body),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Symbol conversion helpers
    // -----------------------------------------------------------------------

    /// Convert our canonical `Symbol` to the Binance market string (e.g., "BTCUSDT").
    fn to_binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote)
    }

    /// Convert a Binance market string back to our canonical `Symbol`.
    /// This is best-effort: assumes the quote is one of the common Binance quotes.
    fn from_binance_symbol(s: &str) -> Symbol {
        // Try known quote assets in longest-first order.
        for quote in &["USDT", "BUSD", "USDC", "BTC", "ETH", "BNB", "USD"] {
            if let Some(base) = s.strip_suffix(quote) {
                if !base.is_empty() {
                    return Symbol::new(base, *quote);
                }
            }
        }
        // Fallback — treat entire string as base with unknown quote.
        Symbol::new(s, "UNKNOWN")
    }

    // -----------------------------------------------------------------------
    // WebSocket user-data stream
    // -----------------------------------------------------------------------

    fn spawn_ws_listener(
        ws_url: String,
        listen_key: String,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let endpoint = format!("{}/ws/{}", ws_url, listen_key);
            info!(endpoint = %endpoint, "Binance: connecting user data stream");

            let ws_stream = match tokio_tungstenite::connect_async(&endpoint).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(error = %e, "Binance: WebSocket connection failed");
                    connected.store(false, Ordering::SeqCst);
                    return;
                }
            };

            let (mut _write, mut read) = ws_stream.split();
            info!("Binance: user data stream connected");

            while let Some(msg_result) = read.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "Binance: WebSocket read error");
                        break;
                    }
                };

                match msg {
                    WsMessage::Text(text) => {
                        if let Err(e) =
                            Self::handle_ws_message(&text, &update_tx).await
                        {
                            warn!(error = %e, "Binance: failed to process WS message");
                        }
                    }
                    WsMessage::Ping(data) => {
                        if let Err(e) = _write.send(WsMessage::Pong(data)).await {
                            warn!(error = %e, "Binance: failed to send pong");
                        }
                    }
                    WsMessage::Close(_) => {
                        info!("Binance: WebSocket closed by server");
                        break;
                    }
                    _ => {}
                }
            }

            connected.store(false, Ordering::SeqCst);
            warn!("Binance: user data stream disconnected");
        })
    }

    async fn handle_ws_message(
        text: &str,
        update_tx: &mpsc::Sender<OrderUpdate>,
    ) -> MmResult<()> {
        let msg: serde_json::Value =
            serde_json::from_str(text).map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("JSON parse error: {}", e),
            })?;

        let event_type = msg["e"].as_str().unwrap_or("");

        if event_type == "executionReport" {
            let update = Self::parse_execution_report(&msg)?;
            debug!(
                client_order_id = %update.client_order_id,
                status = %update.status,
                "Binance: execution report received"
            );
            update_tx.send(update).await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("channel send error: {}", e),
            })?;
        }

        Ok(())
    }

    fn parse_execution_report(msg: &serde_json::Value) -> MmResult<OrderUpdate> {
        let client_order_id = msg["c"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let exchange_order_id = msg["i"]
            .as_u64()
            .map(|id| id.to_string());
        let symbol_str = msg["s"].as_str().unwrap_or("");
        let symbol = Self::from_binance_symbol(symbol_str);

        let status = match msg["X"].as_str().unwrap_or("") {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Cancelled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            other => {
                return Err(MmError::Exchange {
                    exchange: Exchange::Binance,
                    message: format!("unknown order status: {}", other),
                });
            }
        };

        let filled_quantity = msg["z"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let total_quantity = msg["q"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let remaining = total_quantity - filled_quantity;

        // Cumulative quote qty / filled qty = average fill price
        let cumulative_quote_qty = msg["Z"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let average_fill_price = if filled_quantity > Decimal::ZERO {
            cumulative_quote_qty / filled_quantity
        } else {
            Decimal::ZERO
        };

        let last_fill_price = msg["L"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let last_fill_quantity = msg["l"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let fee = msg["n"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());
        let fee_currency = msg["N"].as_str().map(|s| s.to_string());

        let timestamp_ms = msg["T"].as_i64().unwrap_or_else(|| Utc::now().timestamp_millis());
        let timestamp = chrono::DateTime::from_timestamp_millis(timestamp_ms)
            .unwrap_or_else(Utc::now);

        Ok(OrderUpdate {
            client_order_id,
            exchange_order_id,
            exchange: Exchange::Binance,
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
        })
    }

    fn spawn_keepalive(
        http: Client,
        base_url: String,
        api_key: String,
        listen_key: String,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
            loop {
                interval.tick().await;
                if !connected.load(Ordering::SeqCst) {
                    info!("Binance: keepalive stopping — not connected");
                    break;
                }
                let url = format!(
                    "{}/api/v3/userDataStream?listenKey={}",
                    base_url, listen_key
                );
                match http
                    .put(&url)
                    .header("X-MBX-APIKEY", &api_key)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        debug!("Binance: listen key keepalive sent");
                    }
                    Ok(resp) => {
                        warn!(
                            status = %resp.status(),
                            "Binance: listen key keepalive failed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "Binance: listen key keepalive error");
                    }
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // Order side/type mappers
    // -----------------------------------------------------------------------

    fn side_to_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    fn order_type_to_str(ot: mm_core::OrderType) -> &'static str {
        match ot {
            mm_core::OrderType::Market => "MARKET",
            mm_core::OrderType::Limit => "LIMIT",
            mm_core::OrderType::StopLimit => "STOP_LOSS_LIMIT",
            mm_core::OrderType::StopMarket => "STOP_LOSS",
        }
    }

    fn tif_to_str(tif: mm_core::TimeInForce) -> &'static str {
        match tif {
            mm_core::TimeInForce::GTC => "GTC",
            mm_core::TimeInForce::IOC => "IOC",
            mm_core::TimeInForce::FOK => "FOK",
            mm_core::TimeInForce::GTD => "GTC", // Binance has no GTD; fall back to GTC.
        }
    }
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for BinanceExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Binance: connecting execution connector");

        let listen_key = self.create_listen_key().await?;
        info!(listen_key = %listen_key, "Binance: obtained listen key");

        self.connected.store(true, Ordering::SeqCst);

        // Spawn WebSocket listener.
        self.ws_handle = Some(Self::spawn_ws_listener(
            self.config.ws_url.clone(),
            listen_key.clone(),
            update_tx,
            Arc::clone(&self.connected),
        ));

        // Spawn keep-alive.
        self.keepalive_handle = Some(Self::spawn_keepalive(
            self.http.clone(),
            self.config.base_url.clone(),
            self.config.api_key.clone(),
            listen_key,
            Arc::clone(&self.connected),
        ));

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Binance: disconnecting");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }
        if let Some(h) = self.keepalive_handle.take() {
            h.abort();
        }
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        let symbol = Self::to_binance_symbol(&order.symbol);
        let side = Self::side_to_str(order.side);
        let order_type = Self::order_type_to_str(order.order_type);
        let tif = Self::tif_to_str(order.time_in_force);
        let quantity = order.quantity.to_string();

        let mut params: Vec<(&str, &str)> = vec![
            ("symbol", &symbol),
            ("side", side),
            ("type", order_type),
            ("quantity", &quantity),
            ("newOrderRespType", "ACK"),
        ];

        let price_str;
        if let Some(ref price) = order.price {
            price_str = price.to_string();
            params.push(("timeInForce", tif));
            params.push(("price", &price_str));
        }

        let signed_query = self.sign_params(&params);
        let url = format!("{}/api/v3/order?{}", self.config.base_url, signed_query);

        debug!(
            symbol = %symbol,
            side = %side,
            quantity = %quantity,
            order_type = %order_type,
            "Binance: submitting order"
        );

        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("submit_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("submit_order parse error: {}", e),
            })?;

        if !status.is_success() {
            let code = body["code"].as_i64().unwrap_or(0);
            let msg = body["msg"].as_str().unwrap_or("unknown error");
            return Err(MmError::OrderRejected {
                reason: format!("Binance error {}: {}", code, msg),
            });
        }

        let order_id = body["orderId"]
            .as_u64()
            .map(|id| id.to_string())
            .unwrap_or_else(|| body["orderId"].to_string());

        info!(
            exchange_order_id = %order_id,
            symbol = %symbol,
            side = %side,
            "Binance: order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        let symbol = Self::to_binance_symbol(&cancel.symbol);

        let mut params: Vec<(&str, &str)> = vec![("symbol", &symbol)];

        let exch_id;
        if let Some(ref eid) = cancel.exchange_order_id {
            exch_id = eid.clone();
            params.push(("orderId", &exch_id));
        } else {
            params.push(("origClientOrderId", &cancel.client_order_id));
        }

        let signed_query = self.sign_params(&params);
        let url = format!("{}/api/v3/order?{}", self.config.base_url, signed_query);

        let resp = self
            .http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("cancel_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body: serde_json::Value =
                resp.json().await.unwrap_or(serde_json::Value::Null);
            let msg = body["msg"].as_str().unwrap_or("unknown error");
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("cancel_order failed ({}): {}", status, msg),
            });
        }

        info!(
            client_order_id = %cancel.client_order_id,
            "Binance: order cancelled"
        );
        Ok(())
    }

    async fn cancel_all_orders(&self, request: &CancelAllOrders) -> MmResult<u32> {
        let symbol_str;
        let mut params: Vec<(&str, &str)> = Vec::new();

        if let Some(ref sym) = request.symbol {
            symbol_str = Self::to_binance_symbol(sym);
            params.push(("symbol", &symbol_str));
        } else {
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: "Binance cancel_all requires a symbol".into(),
            });
        }

        let signed_query = self.sign_params(&params);
        let url = format!(
            "{}/api/v3/openOrders?{}",
            self.config.base_url, signed_query
        );

        let resp = self
            .http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("cancel_all HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("cancel_all parse error: {}", e),
            })?;

        if !status.is_success() {
            let msg = body["msg"].as_str().unwrap_or("unknown");
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("cancel_all failed ({}): {}", status, msg),
            });
        }

        let count = body.as_array().map(|a| a.len() as u32).unwrap_or(0);
        info!(count, "Binance: cancel_all completed");
        Ok(count)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let signed_query = self.sign_params(&[]);
        let url = format!(
            "{}/api/v3/account?{}",
            self.config.base_url, signed_query
        );

        let resp = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("fetch_balances HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("fetch_balances parse error: {}", e),
            })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Binance,
                message: format!("fetch_balances error ({}): {}", status, body),
            });
        }

        let balances: Vec<Balance> = body["balances"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| {
                        let asset = b["asset"].as_str()?;
                        let free: Decimal = b["free"].as_str()?.parse().ok()?;
                        let locked: Decimal = b["locked"].as_str()?.parse().ok()?;
                        let total = free + locked;
                        if total > Decimal::ZERO {
                            Some(Balance {
                                currency: asset.to_string(),
                                free,
                                locked,
                                total,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        debug!(count = balances.len(), "Binance: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}
