//! Alpaca execution connector.
//!
//! Implements [`ExecutionConnector`] for the Alpaca Trading API (v2).
//!
//! ## REST API
//!
//! | Endpoint            | Method   | Description            |
//! |---------------------|----------|------------------------|
//! | `/v2/orders`        | `POST`   | Place a new order      |
//! | `/v2/orders/{id}`   | `DELETE`  | Cancel an order        |
//! | `/v2/orders`        | `DELETE`  | Cancel all open orders |
//! | `/v2/account`       | `GET`    | Fetch account info     |
//!
//! ## Authentication
//!
//! Alpaca uses simple header-based authentication:
//! - `APCA-API-KEY-ID: {api_key}`
//! - `APCA-API-SECRET-KEY: {secret_key}`
//!
//! ## WebSocket
//!
//! The streaming endpoint at `wss://paper-api.alpaca.markets/stream`
//! (or `wss://api.alpaca.markets/stream` for live) delivers `trade_updates`
//! events for real-time order status changes.
//!
//! ### Trade update message format
//!
//! ```json
//! {
//!   "stream": "trade_updates",
//!   "data": {
//!     "event": "fill",
//!     "order": {
//!       "id": "...",
//!       "client_order_id": "...",
//!       "symbol": "AAPL",
//!       "qty": "100",
//!       "filled_qty": "100",
//!       "side": "buy",
//!       "type": "limit",
//!       "status": "filled",
//!       "filled_avg_price": "150.25",
//!       ...
//!     }
//!   }
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use mm_core::{
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, OrderStatus, OrderUpdate,
    Side, Symbol,
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Base delay in seconds for WebSocket reconnection backoff.
const WS_RECONNECT_DELAY_SECS: u64 = 5;

/// Maximum number of consecutive reconnection attempts before giving up.
const WS_MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Calculate exponential backoff delay for a given attempt, capped at 60 seconds.
fn ws_backoff_secs(attempt: u32) -> u64 {
    (WS_RECONNECT_DELAY_SECS.saturating_mul(2u64.saturating_pow(attempt))).min(60)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Alpaca execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaExecutionConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_base_url() -> String {
    "https://paper-api.alpaca.markets".into()
}

fn default_ws_url() -> String {
    "wss://paper-api.alpaca.markets/stream".into()
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct AlpacaExecutionConnector {
    config: AlpacaExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    ws_handle: Option<JoinHandle<()>>,
}

impl AlpacaExecutionConnector {
    pub fn new(config: AlpacaExecutionConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            ws_handle: None,
        }
    }

    // -----------------------------------------------------------------------
    // Symbol conversion helpers
    // -----------------------------------------------------------------------

    /// Convert our canonical `Symbol` to an Alpaca ticker (e.g., "AAPL").
    fn to_alpaca_symbol(symbol: &Symbol) -> String {
        symbol.base.clone()
    }

    /// Convert an Alpaca ticker back to our canonical `Symbol`.
    fn from_alpaca_symbol(ticker: &str) -> Symbol {
        Symbol::new(ticker, "USD")
    }

    // -----------------------------------------------------------------------
    // Order side/type/tif mappers
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
            mm_core::OrderType::StopLimit => "stop_limit",
            mm_core::OrderType::StopMarket => "stop",
        }
    }

    fn tif_to_str(tif: mm_core::TimeInForce) -> &'static str {
        match tif {
            mm_core::TimeInForce::GTC => "gtc",
            mm_core::TimeInForce::IOC => "ioc",
            mm_core::TimeInForce::FOK => "fok",
            mm_core::TimeInForce::GTD => "gtc", // Alpaca has no GTD; fall back to GTC.
        }
    }

    // -----------------------------------------------------------------------
    // WebSocket trade_updates stream
    // -----------------------------------------------------------------------

    fn spawn_ws_listener(
        config: AlpacaExecutionConfig,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut reconnect_attempts = 0u32;

            loop {
                info!("Alpaca: connecting trade updates WebSocket");

                let ws_stream = match tokio_tungstenite::connect_async(&config.ws_url).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        warn!(error = %e, "Alpaca: WebSocket connection failed");
                        connected.store(false, Ordering::SeqCst);

                        reconnect_attempts += 1;
                        if reconnect_attempts > WS_MAX_RECONNECT_ATTEMPTS {
                            error!("Alpaca: max WebSocket reconnection attempts exceeded");
                            break;
                        }
                        let delay = ws_backoff_secs(reconnect_attempts - 1);
                        info!(
                            attempt = reconnect_attempts,
                            delay_secs = delay,
                            "Alpaca: reconnecting trade updates stream"
                        );
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        continue;
                    }
                };

                let (mut write, mut read) = ws_stream.split();

                // --- Authenticate ---
                // Alpaca's trading stream expects `auth` with top-level key/secret.
                let auth_msg = serde_json::json!({
                    "action": "auth",
                    "key": config.api_key,
                    "secret": config.secret_key,
                });

                if let Err(e) = write.send(WsMessage::Text(auth_msg.to_string())).await {
                    warn!(error = %e, "Alpaca: failed to send auth message");
                    connected.store(false, Ordering::SeqCst);

                    reconnect_attempts += 1;
                    if reconnect_attempts > WS_MAX_RECONNECT_ATTEMPTS {
                        error!("Alpaca: max WebSocket reconnection attempts exceeded");
                        break;
                    }
                    let delay = ws_backoff_secs(reconnect_attempts - 1);
                    info!(
                        attempt = reconnect_attempts,
                        delay_secs = delay,
                        "Alpaca: reconnecting trade updates stream"
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    continue;
                }

                let mut authorized = false;
                let mut retryable_auth_failure = true;

                while let Some(auth_result) = read.next().await {
                    let message = match auth_result {
                        Ok(message) => message,
                        Err(e) => {
                            warn!(error = %e, "Alpaca: error reading auth response");
                            break;
                        }
                    };

                    match Self::decode_ws_json(&message) {
                        Ok(Some(msg)) => {
                            if let Some(status) = Self::authorization_status(&msg) {
                                if status == "authorized" {
                                    authorized = true;
                                    info!("Alpaca: WebSocket authenticated");
                                } else {
                                    error!(
                                        response = %msg,
                                        "Alpaca: WebSocket authentication failed"
                                    );
                                    retryable_auth_failure = false;
                                }
                                break;
                            }

                            if let Some(error_message) = Self::stream_error_message(&msg) {
                                warn!(error_message, "Alpaca: pre-auth stream error");
                            }
                        }
                        Ok(None) => match message {
                            WsMessage::Ping(data) => {
                                if let Err(e) = write.send(WsMessage::Pong(data)).await {
                                    warn!(error = %e, "Alpaca: failed to send pong");
                                    break;
                                }
                            }
                            WsMessage::Close(_) => {
                                warn!("Alpaca: stream ended before auth response");
                                break;
                            }
                            _ => {}
                        },
                        Err(e) => {
                            warn!(error = %e, "Alpaca: invalid auth response");
                            break;
                        }
                    }
                }

                if !authorized {
                    connected.store(false, Ordering::SeqCst);

                    if !retryable_auth_failure {
                        break;
                    }

                    reconnect_attempts += 1;
                    if reconnect_attempts > WS_MAX_RECONNECT_ATTEMPTS {
                        error!("Alpaca: max WebSocket reconnection attempts exceeded");
                        break;
                    }
                    let delay = ws_backoff_secs(reconnect_attempts - 1);
                    info!(
                        attempt = reconnect_attempts,
                        delay_secs = delay,
                        "Alpaca: reconnecting trade updates stream"
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    continue;
                }

                // --- Subscribe to trade_updates ---
                let listen_msg = serde_json::json!({
                    "action": "listen",
                    "data": {
                        "streams": ["trade_updates"]
                    }
                });

                if let Err(e) = write.send(WsMessage::Text(listen_msg.to_string())).await {
                    warn!(error = %e, "Alpaca: failed to send listen message");
                    connected.store(false, Ordering::SeqCst);

                    reconnect_attempts += 1;
                    if reconnect_attempts > WS_MAX_RECONNECT_ATTEMPTS {
                        error!("Alpaca: max WebSocket reconnection attempts exceeded");
                        break;
                    }
                    let delay = ws_backoff_secs(reconnect_attempts - 1);
                    info!(
                        attempt = reconnect_attempts,
                        delay_secs = delay,
                        "Alpaca: reconnecting trade updates stream"
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    continue;
                }

                reconnect_attempts = 0;
                connected.store(true, Ordering::SeqCst);
                info!("Alpaca: subscribed to trade_updates");

                // --- Main read loop ---
                while let Some(msg_result) = read.next().await {
                    let msg = match msg_result {
                        Ok(m) => m,
                        Err(e) => {
                            error!(error = %e, "Alpaca: WebSocket read error");
                            break;
                        }
                    };

                    match msg {
                        WsMessage::Text(text) => {
                            match serde_json::from_str(text.as_ref()).map_err(|e| MmError::Exchange {
                                exchange: Exchange::Alpaca,
                                message: format!("JSON parse error: {}", e),
                            }) {
                                Ok(value) => {
                                    if let Err(e) = Self::handle_ws_json(value, &update_tx).await {
                                        warn!(error = %e, "Alpaca: failed to process WS message");
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Alpaca: failed to parse WS text frame");
                                }
                            }
                        }
                        WsMessage::Binary(payload) => {
                            match serde_json::from_slice(payload.as_ref()).map_err(|e| MmError::Exchange {
                                exchange: Exchange::Alpaca,
                                message: format!("JSON parse error: {}", e),
                            }) {
                                Ok(value) => {
                                    if let Err(e) = Self::handle_ws_json(value, &update_tx).await {
                                        warn!(error = %e, "Alpaca: failed to process binary WS message");
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Alpaca: failed to parse WS binary frame");
                                }
                            }
                        }
                        WsMessage::Ping(data) => {
                            if let Err(e) = write.send(WsMessage::Pong(data)).await {
                                warn!(error = %e, "Alpaca: failed to send pong");
                            }
                        }
                        WsMessage::Close(_) => {
                            info!("Alpaca: WebSocket closed by server");
                            break;
                        }
                        _ => {}
                    }
                }

                connected.store(false, Ordering::SeqCst);
                warn!("Alpaca: trade updates stream disconnected, will attempt reconnect");

                reconnect_attempts += 1;
                if reconnect_attempts > WS_MAX_RECONNECT_ATTEMPTS {
                    error!("Alpaca: max WebSocket reconnection attempts exceeded");
                    break;
                }
                let delay = ws_backoff_secs(reconnect_attempts - 1);
                info!(
                    attempt = reconnect_attempts,
                    delay_secs = delay,
                    "Alpaca: reconnecting trade updates stream"
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
        })
    }

    fn decode_ws_json(message: &WsMessage) -> MmResult<Option<serde_json::Value>> {
        match message {
            WsMessage::Text(text) => serde_json::from_str(text.as_ref())
                .map(Some)
                .map_err(|e| MmError::Exchange {
                    exchange: Exchange::Alpaca,
                    message: format!("JSON parse error: {}", e),
                }),
            WsMessage::Binary(payload) => serde_json::from_slice(payload.as_ref())
                .map(Some)
                .map_err(|e| MmError::Exchange {
                    exchange: Exchange::Alpaca,
                    message: format!("JSON parse error: {}", e),
                }),
            _ => Ok(None),
        }
    }

    fn authorization_status(message: &serde_json::Value) -> Option<&str> {
        if message["stream"].as_str() == Some("authorization") {
            message["data"]["status"].as_str()
        } else {
            None
        }
    }

    fn stream_error_message(message: &serde_json::Value) -> Option<String> {
        if message["action"].as_str() == Some("error") {
            message["data"]["error_message"]
                .as_str()
                .map(std::string::ToString::to_string)
        } else {
            None
        }
    }

    async fn handle_ws_json(
        msg: serde_json::Value,
        update_tx: &mpsc::Sender<OrderUpdate>,
    ) -> MmResult<()> {
        if let Some(error_message) = Self::stream_error_message(&msg) {
            return Err(MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("stream error: {}", error_message),
            });
        }

        let stream = msg["stream"].as_str().unwrap_or("");

        if stream != "trade_updates" {
            return Ok(());
        }

        let data = &msg["data"];
        let update = Self::parse_trade_update(data)?;

        debug!(
            client_order_id = %update.client_order_id,
            status = %update.status,
            "Alpaca: trade update received"
        );

        update_tx
            .send(update)
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("channel send error: {}", e),
            })?;

        Ok(())
    }

    fn parse_trade_update(data: &serde_json::Value) -> MmResult<OrderUpdate> {
        let event = data["event"].as_str().unwrap_or("");
        let order = &data["order"];

        let client_order_id = order["client_order_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let exchange_order_id = order["id"].as_str().map(|s| s.to_string());
        let symbol_str = order["symbol"].as_str().unwrap_or("");
        let symbol = Self::from_alpaca_symbol(symbol_str);

        // Map the Alpaca event/status to our OrderStatus.
        let status = match event {
            "new" => OrderStatus::New,
            "partially_filled" | "partial_fill" => OrderStatus::PartiallyFilled,
            "fill" | "filled" => OrderStatus::Filled,
            "canceled" | "cancelled" => OrderStatus::Cancelled,
            "rejected" => OrderStatus::Rejected,
            "expired" => OrderStatus::Expired,
            "pending_new" => OrderStatus::Submitted,
            "accepted" => OrderStatus::New,
            "pending_cancel" => OrderStatus::PendingCancel,
            "replaced" => OrderStatus::New,
            other => {
                // Also try parsing from the order status field.
                let order_status = order["status"].as_str().unwrap_or("");
                match order_status {
                    "new" | "accepted" | "accepted_for_bidding" => OrderStatus::New,
                    "partially_filled" => OrderStatus::PartiallyFilled,
                    "filled" => OrderStatus::Filled,
                    "canceled" | "cancelled" => OrderStatus::Cancelled,
                    "rejected" => OrderStatus::Rejected,
                    "expired" => OrderStatus::Expired,
                    "pending_new" | "pending_replace" => OrderStatus::Submitted,
                    "pending_cancel" => OrderStatus::PendingCancel,
                    _ => {
                        return Err(MmError::Exchange {
                            exchange: Exchange::Alpaca,
                            message: format!(
                                "unknown order event '{}' / status '{}'",
                                other, order_status
                            ),
                        });
                    }
                }
            }
        };

        let filled_quantity = order["filled_qty"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let total_quantity = order["qty"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let remaining = total_quantity - filled_quantity;

        let average_fill_price = order["filled_avg_price"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);

        // Alpaca trade updates for fills include price/qty at the event level.
        let last_fill_price = data["price"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .or_else(|| {
                data["order"]["filled_avg_price"]
                    .as_str()
                    .and_then(|s| s.parse::<Decimal>().ok())
            });
        let last_fill_quantity = data["qty"].as_str().and_then(|s| s.parse::<Decimal>().ok());

        let timestamp = order["updated_at"]
            .as_str()
            .or_else(|| order["created_at"].as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Ok(OrderUpdate {
            client_order_id,
            exchange_order_id,
            exchange: Exchange::Alpaca,
            symbol,
            status,
            filled_quantity,
            remaining_quantity: remaining,
            average_fill_price,
            last_fill_price,
            last_fill_quantity,
            fee: None, // Alpaca does not report fees in trade updates.
            fee_currency: None,
            timestamp,
        })
    }
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for AlpacaExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Alpaca
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Alpaca: connecting execution connector");
        self.connected.store(true, Ordering::SeqCst);

        self.ws_handle = Some(Self::spawn_ws_listener(
            self.config.clone(),
            update_tx,
            Arc::clone(&self.connected),
        ));

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Alpaca: disconnecting");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        let symbol = Self::to_alpaca_symbol(&order.symbol);
        let side = Self::side_to_str(order.side);
        let order_type = Self::order_type_to_str(order.order_type);
        let tif = Self::tif_to_str(order.time_in_force);
        let client_order_id = Uuid::new_v4().to_string();

        let mut body = serde_json::json!({
            "symbol": symbol,
            "qty": order.quantity.to_string(),
            "side": side,
            "type": order_type,
            "time_in_force": tif,
            "client_order_id": client_order_id,
        });

        // Add limit_price for limit and stop-limit orders.
        if let Some(ref price) = order.price {
            match order.order_type {
                mm_core::OrderType::Limit | mm_core::OrderType::StopLimit => {
                    body["limit_price"] = serde_json::Value::String(price.to_string());
                }
                mm_core::OrderType::StopMarket => {
                    body["stop_price"] = serde_json::Value::String(price.to_string());
                }
                _ => {}
            }
            // StopLimit needs both limit_price and stop_price; for simplicity
            // we use the same price as both (caller should provide distinct
            // prices via a richer order struct if needed).
            if order.order_type == mm_core::OrderType::StopLimit {
                body["stop_price"] = serde_json::Value::String(price.to_string());
            }
        }

        let url = format!("{}/v2/orders", self.config.base_url);

        debug!(url = %url, %symbol, %side, %order_type, "Alpaca: submitting order");

        let resp = self
            .http
            .post(&url)
            .header("APCA-API-KEY-ID", &self.config.api_key)
            .header("APCA-API-SECRET-KEY", &self.config.secret_key)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("submit_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        let resp_body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("submit_order parse error: {}", e),
        })?;

        if !status.is_success() {
            let error_msg = resp_body["message"]
                .as_str()
                .or_else(|| resp_body["code"].as_str())
                .unwrap_or("unknown error");
            return Err(MmError::OrderRejected {
                reason: format!("Alpaca error ({}): {}", status, error_msg),
            });
        }

        let order_id = resp_body["id"]
            .as_str()
            .unwrap_or(&client_order_id)
            .to_string();

        info!(
            exchange_order_id = %order_id,
            %symbol,
            side = %side,
            "Alpaca: order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        // Prefer exchange_order_id; fall back to client_order_id.
        let order_id = cancel
            .exchange_order_id
            .as_deref()
            .unwrap_or(&cancel.client_order_id);

        let url = format!("{}/v2/orders/{}", self.config.base_url, order_id);

        let resp = self
            .http
            .delete(&url)
            .header("APCA-API-KEY-ID", &self.config.api_key)
            .header("APCA-API-SECRET-KEY", &self.config.secret_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("cancel_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            let msg = body["message"].as_str().unwrap_or("unknown error");
            return Err(MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("cancel_order failed ({}): {}", status, msg),
            });
        }

        info!(
            client_order_id = %cancel.client_order_id,
            "Alpaca: order cancelled"
        );
        Ok(())
    }

    async fn cancel_all_orders(&self, _request: &CancelAllOrders) -> MmResult<u32> {
        // Alpaca DELETE /v2/orders cancels all open orders and returns the list
        // of cancelled orders. No symbol filter is available at the API level.
        let url = format!("{}/v2/orders", self.config.base_url);

        let resp = self
            .http
            .delete(&url)
            .header("APCA-API-KEY-ID", &self.config.api_key)
            .header("APCA-API-SECRET-KEY", &self.config.secret_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("cancel_all HTTP error: {}", e),
            })?;

        let status = resp.status();
        // Alpaca returns 207 Multi-Status with an array of cancelled orders.
        if !status.is_success() && status.as_u16() != 207 {
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            let msg = body["message"].as_str().unwrap_or("unknown");
            return Err(MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("cancel_all failed ({}): {}", status, msg),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("cancel_all parse error: {}", e),
        })?;

        let count = body.as_array().map(|a| a.len() as u32).unwrap_or(0);
        info!(count, "Alpaca: cancel_all completed");
        Ok(count)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let url = format!("{}/v2/account", self.config.base_url);

        let resp = self
            .http
            .get(&url)
            .header("APCA-API-KEY-ID", &self.config.api_key)
            .header("APCA-API-SECRET-KEY", &self.config.secret_key)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("fetch_balances HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Alpaca,
            message: format!("fetch_balances parse error: {}", e),
        })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Alpaca,
                message: format!("fetch_balances error ({}): {}", status, body),
            });
        }

        // Alpaca account has `cash`, `buying_power`, `portfolio_value`.
        // We model the cash balance as a single USD balance.
        let cash: Decimal = body["cash"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);

        let buying_power: Decimal = body["buying_power"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);

        // `cash` = total, `buying_power` = available for trading.
        // The "locked" portion is cash minus buying power (margin held, etc.)
        // but buying_power can exceed cash in margin accounts. Clamp to zero.
        let locked = if cash > buying_power {
            cash - buying_power
        } else {
            Decimal::ZERO
        };

        let balance = Balance {
            currency: "USD".to_string(),
            free: buying_power,
            locked,
            total: cash,
        };

        debug!("Alpaca: balance fetched");
        Ok(vec![balance])
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn test_config() -> AlpacaExecutionConfig {
        AlpacaExecutionConfig {
            api_key: "test-api-key".into(),
            secret_key: "test-secret-key".into(),
            base_url: default_base_url(),
            ws_url: default_ws_url(),
        }
    }

    fn test_connector() -> AlpacaExecutionConnector {
        AlpacaExecutionConnector::new(test_config())
    }

    // -- Config defaults --

    #[test]
    fn test_config_defaults() {
        let json = r#"{"api_key":"k","secret_key":"s"}"#;
        let cfg: AlpacaExecutionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.base_url, "https://paper-api.alpaca.markets");
        assert_eq!(cfg.ws_url, "wss://paper-api.alpaca.markets/stream");
    }

    #[test]
    fn test_config_custom_urls() {
        let json = r#"{
            "api_key":"k",
            "secret_key":"s",
            "base_url":"https://api.alpaca.markets",
            "ws_url":"wss://api.alpaca.markets/stream"
        }"#;
        let cfg: AlpacaExecutionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.base_url, "https://api.alpaca.markets");
        assert_eq!(cfg.ws_url, "wss://api.alpaca.markets/stream");
    }

    // -- Symbol conversion --

    #[test]
    fn test_to_alpaca_symbol() {
        let sym = Symbol::new("AAPL", "USD");
        assert_eq!(AlpacaExecutionConnector::to_alpaca_symbol(&sym), "AAPL");
    }

    #[test]
    fn test_from_alpaca_symbol() {
        let sym = AlpacaExecutionConnector::from_alpaca_symbol("TSLA");
        assert_eq!(sym.base, "TSLA");
        assert_eq!(sym.quote, "USD");
    }

    // -- Side / OrderType / TimeInForce mapping --

    #[test]
    fn test_side_to_str() {
        assert_eq!(AlpacaExecutionConnector::side_to_str(Side::Buy), "buy");
        assert_eq!(AlpacaExecutionConnector::side_to_str(Side::Sell), "sell");
    }

    #[test]
    fn test_order_type_to_str() {
        assert_eq!(
            AlpacaExecutionConnector::order_type_to_str(mm_core::OrderType::Market),
            "market"
        );
        assert_eq!(
            AlpacaExecutionConnector::order_type_to_str(mm_core::OrderType::Limit),
            "limit"
        );
        assert_eq!(
            AlpacaExecutionConnector::order_type_to_str(mm_core::OrderType::StopLimit),
            "stop_limit"
        );
        assert_eq!(
            AlpacaExecutionConnector::order_type_to_str(mm_core::OrderType::StopMarket),
            "stop"
        );
    }

    #[test]
    fn test_tif_to_str() {
        assert_eq!(
            AlpacaExecutionConnector::tif_to_str(mm_core::TimeInForce::GTC),
            "gtc"
        );
        assert_eq!(
            AlpacaExecutionConnector::tif_to_str(mm_core::TimeInForce::IOC),
            "ioc"
        );
        assert_eq!(
            AlpacaExecutionConnector::tif_to_str(mm_core::TimeInForce::FOK),
            "fok"
        );
        // GTD falls back to GTC on Alpaca.
        assert_eq!(
            AlpacaExecutionConnector::tif_to_str(mm_core::TimeInForce::GTD),
            "gtc"
        );
    }

    // -- Trade update parsing --

    #[test]
    fn test_parse_trade_update_fill() {
        let data = serde_json::json!({
            "event": "fill",
            "price": "150.25",
            "qty": "10",
            "order": {
                "id": "order-uuid-123",
                "client_order_id": "my-client-456",
                "symbol": "AAPL",
                "qty": "100",
                "filled_qty": "100",
                "filled_avg_price": "150.25",
                "status": "filled",
                "updated_at": "2024-01-15T12:00:00Z"
            }
        });

        let update = AlpacaExecutionConnector::parse_trade_update(&data).unwrap();

        assert_eq!(update.client_order_id, "my-client-456");
        assert_eq!(update.exchange_order_id, Some("order-uuid-123".to_string()));
        assert_eq!(update.exchange, Exchange::Alpaca);
        assert_eq!(update.symbol, Symbol::new("AAPL", "USD"));
        assert_eq!(update.status, OrderStatus::Filled);
        assert_eq!(update.filled_quantity, dec!(100));
        assert_eq!(update.remaining_quantity, dec!(0));
        assert_eq!(update.average_fill_price, dec!(150.25));
        assert_eq!(update.last_fill_price, Some(dec!(150.25)));
        assert_eq!(update.last_fill_quantity, Some(dec!(10)));
        // Alpaca does not report fees in trade updates.
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_currency, None);
    }

    #[test]
    fn test_parse_trade_update_partial_fill() {
        let data = serde_json::json!({
            "event": "partial_fill",
            "price": "200.00",
            "qty": "25",
            "order": {
                "id": "order-789",
                "client_order_id": "client-789",
                "symbol": "MSFT",
                "qty": "100",
                "filled_qty": "50",
                "filled_avg_price": "200.00",
                "status": "partially_filled",
                "updated_at": "2024-01-15T12:00:00Z"
            }
        });

        let update = AlpacaExecutionConnector::parse_trade_update(&data).unwrap();
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.filled_quantity, dec!(50));
        assert_eq!(update.remaining_quantity, dec!(50));
    }

    #[test]
    fn test_parse_trade_update_all_event_statuses() {
        let events = vec![
            ("new", OrderStatus::New),
            ("partially_filled", OrderStatus::PartiallyFilled),
            ("partial_fill", OrderStatus::PartiallyFilled),
            ("fill", OrderStatus::Filled),
            ("filled", OrderStatus::Filled),
            ("canceled", OrderStatus::Cancelled),
            ("cancelled", OrderStatus::Cancelled),
            ("rejected", OrderStatus::Rejected),
            ("expired", OrderStatus::Expired),
            ("pending_new", OrderStatus::Submitted),
            ("accepted", OrderStatus::New),
            ("pending_cancel", OrderStatus::PendingCancel),
            ("replaced", OrderStatus::New),
        ];

        for (event, expected) in events {
            let data = serde_json::json!({
                "event": event,
                "order": {
                    "id": "oid",
                    "client_order_id": "coid",
                    "symbol": "AAPL",
                    "qty": "10",
                    "filled_qty": "0",
                    "filled_avg_price": "0",
                    "status": "",
                    "updated_at": "2024-01-15T12:00:00Z"
                }
            });
            let update = AlpacaExecutionConnector::parse_trade_update(&data).unwrap();
            assert_eq!(
                update.status, expected,
                "status mismatch for Alpaca event '{}'",
                event
            );
        }
    }

    #[test]
    fn test_parse_trade_update_fallback_to_order_status_field() {
        // When the event field is unrecognized, it falls back to order["status"].
        let data = serde_json::json!({
            "event": "some_unknown_event",
            "order": {
                "id": "oid",
                "client_order_id": "coid",
                "symbol": "TSLA",
                "qty": "10",
                "filled_qty": "10",
                "filled_avg_price": "250.00",
                "status": "filled",
                "updated_at": "2024-01-15T12:00:00Z"
            }
        });
        let update = AlpacaExecutionConnector::parse_trade_update(&data).unwrap();
        assert_eq!(update.status, OrderStatus::Filled);
    }

    #[test]
    fn test_parse_trade_update_unknown_event_and_status_errors() {
        let data = serde_json::json!({
            "event": "totally_unknown",
            "order": {
                "id": "oid",
                "client_order_id": "coid",
                "symbol": "AAPL",
                "qty": "10",
                "filled_qty": "0",
                "filled_avg_price": "0",
                "status": "also_unknown"
            }
        });
        let result = AlpacaExecutionConnector::parse_trade_update(&data);
        assert!(
            result.is_err(),
            "completely unknown event+status should error"
        );
    }

    #[test]
    fn test_parse_trade_update_uses_created_at_fallback() {
        let data = serde_json::json!({
            "event": "new",
            "order": {
                "id": "oid",
                "client_order_id": "coid",
                "symbol": "GOOG",
                "qty": "5",
                "filled_qty": "0",
                "filled_avg_price": "0",
                "status": "new",
                "created_at": "2024-06-01T09:30:00Z"
            }
        });
        let update = AlpacaExecutionConnector::parse_trade_update(&data).unwrap();
        // Should parse the created_at timestamp since updated_at is missing.
        assert_eq!(
            update.timestamp,
            chrono::DateTime::parse_from_rfc3339("2024-06-01T09:30:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    // -- WebSocket reconnection backoff --

    #[test]
    fn test_ws_reconnect_constants() {
        assert_eq!(WS_RECONNECT_DELAY_SECS, 5);
        assert_eq!(WS_MAX_RECONNECT_ATTEMPTS, 10);
    }

    #[test]
    fn test_ws_backoff_secs_exponential() {
        assert_eq!(ws_backoff_secs(0), 5); // 5 * 2^0 = 5
        assert_eq!(ws_backoff_secs(1), 10); // 5 * 2^1 = 10
        assert_eq!(ws_backoff_secs(2), 20); // 5 * 2^2 = 20
        assert_eq!(ws_backoff_secs(3), 40); // 5 * 2^3 = 40
    }

    #[test]
    fn test_ws_backoff_secs_capped_at_60() {
        assert_eq!(ws_backoff_secs(4), 60); // 5 * 2^4 = 80 -> capped to 60
        assert_eq!(ws_backoff_secs(5), 60); // 5 * 2^5 = 160 -> capped to 60
        assert_eq!(ws_backoff_secs(10), 60);
        assert_eq!(ws_backoff_secs(u32::MAX), 60);
    }

    // -- Connector state --

    #[test]
    fn test_connector_initial_state() {
        let connector = test_connector();
        assert!(!connector.is_connected());
        assert_eq!(connector.exchange(), Exchange::Alpaca);
    }
}
