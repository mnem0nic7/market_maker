//! Coinbase Advanced Trade API execution connector.
//!
//! Implements [`ExecutionConnector`] for the Coinbase Advanced Trade (v3) API.
//!
//! ## REST API
//!
//! | Endpoint                              | Method   | Description            |
//! |---------------------------------------|----------|------------------------|
//! | `/api/v3/brokerage/orders`            | `POST`   | Place a new order      |
//! | `/api/v3/brokerage/orders/batch_cancel` | `POST` | Cancel orders          |
//! | `/api/v3/brokerage/accounts`          | `GET`    | Fetch accounts/balances|
//!
//! ## Authentication
//!
//! Coinbase CDP keys use JWT/ES256 authentication:
//! - The `secret_key` field holds an EC private key in PEM format.
//! - Each request builds a short-lived JWT (120 s) signed with ES256.
//! - REST requests set `Authorization: Bearer <jwt>` with a `uri` claim.
//! - WebSocket auth sends a JWT **without** the `uri` claim.
//!
//! ## WebSocket
//!
//! The `user` channel on the Advanced Trade WebSocket provides real-time order
//! updates. Authentication uses JWT (no `uri` claim).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use p256::SecretKey;
use mm_core::{
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, OrderStatus, OrderUpdate,
    Side, Symbol,
};
use rand::Rng;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Coinbase execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinbaseExecutionConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_url: String,
}

fn default_base_url() -> String {
    "https://api.coinbase.com".into()
}

fn default_ws_url() -> String {
    "wss://advanced-trade-ws.coinbase.com".into()
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct CoinbaseExecutionConnector {
    config: CoinbaseExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    ws_handle: Option<JoinHandle<()>>,
}

impl CoinbaseExecutionConnector {
    pub fn new(config: CoinbaseExecutionConfig) -> Self {
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
    // JWT / ES256 Authentication
    // -----------------------------------------------------------------------

    /// Build a short-lived JWT signed with ES256 for Coinbase CDP auth.
    ///
    /// `uri` should be `Some("METHOD host/path")` for REST requests,
    /// or `None` for WebSocket authentication.
    fn build_jwt(api_key: &str, secret_pem: &str, uri: Option<&str>) -> Result<String, MmError> {
        let now = Utc::now().timestamp() as u64;
        let nonce: String = {
            let bytes: [u8; 16] = rand::thread_rng().gen();
            hex::encode(bytes)
        };

        // Env vars / config files may store the PEM with literal "\n" escapes
        // instead of real newlines. Normalize for parsing.
        let secret_pem = secret_pem.replace("\\n", "\n");

        // If the PEM looks empty or doesn't resemble a key, fail fast with a
        // helpful message rather than a cryptic "InvalidKeyFormat".
        if !secret_pem.contains("-----") {
            return Err(MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!(
                    "secret_key does not look like a PEM key (len={}, first_chars={:?})",
                    secret_pem.len(),
                    &secret_pem[..secret_pem.len().min(40)]
                ),
            });
        }

        // Coinbase issues SEC1 EC keys ("BEGIN EC PRIVATE KEY") but
        // jsonwebtoken only accepts PKCS#8 ("BEGIN PRIVATE KEY").
        // Detect and convert if necessary.
        let pkcs8_pem = if secret_pem.contains("BEGIN EC PRIVATE KEY") {
            use p256::pkcs8::EncodePrivateKey;
            let sk = SecretKey::from_sec1_pem(&secret_pem).map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse SEC1 EC key: {}", e),
            })?;
            sk.to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                .map_err(|e| MmError::Exchange {
                    exchange: Exchange::Coinbase,
                    message: format!("failed to convert EC key to PKCS#8: {}", e),
                })?
                .to_string()
        } else {
            secret_pem
        };

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(api_key.to_string());
        header.typ = Some("JWT".to_string());
        // Coinbase expects the nonce in the header.
        // jsonwebtoken doesn't have a built-in nonce field, so we use a
        // custom serialization approach via serde_json.
        // We'll build the header manually.
        drop(header);

        // Build header as a JSON value so we can include `nonce`.
        let header_value = serde_json::json!({
            "alg": "ES256",
            "kid": api_key,
            "nonce": nonce,
            "typ": "JWT"
        });

        let mut claims = serde_json::json!({
            "iss": "cdp",
            "sub": api_key,
            "aud": ["cdp_service"],
            "nbf": now,
            "exp": now + 120
        });

        if let Some(uri_val) = uri {
            claims["uri"] = serde_json::Value::String(uri_val.to_string());
        }

        // Use jsonwebtoken with a custom header.
        let header_obj: Header = serde_json::from_value(header_value).map_err(|e| {
            MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to build JWT header: {}", e),
            }
        })?;

        let encoding_key =
            EncodingKey::from_ec_pem(pkcs8_pem.as_bytes()).map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("failed to parse EC PEM key: {}", e),
            })?;

        encode(&header_obj, &claims, &encoding_key).map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("failed to encode JWT: {}", e),
        })
    }

    /// Build authenticated headers for a REST request.
    fn auth_headers(
        &self,
        method: &str,
        path: &str,
        _body: &str,
    ) -> Result<Vec<(String, String)>, MmError> {
        // Extract host from base_url for the URI claim.
        let host = self
            .config
            .base_url
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let uri = format!("{} {}{}", method, host, path);

        let jwt = Self::build_jwt(&self.config.api_key, &self.config.secret_key, Some(&uri))?;

        Ok(vec![
            ("Authorization".into(), format!("Bearer {}", jwt)),
            ("Content-Type".into(), "application/json".into()),
        ])
    }

    // -----------------------------------------------------------------------
    // Symbol conversion helpers
    // -----------------------------------------------------------------------

    /// Convert our canonical `Symbol` to a Coinbase product ID (e.g., "BTC-USD").
    fn to_coinbase_product_id(symbol: &Symbol) -> String {
        format!("{}-{}", symbol.base, symbol.quote)
    }

    /// Convert a Coinbase product ID back to our canonical `Symbol`.
    fn from_coinbase_product_id(product_id: &str) -> Symbol {
        let parts: Vec<&str> = product_id.splitn(2, '-').collect();
        if parts.len() == 2 {
            Symbol::new(parts[0], parts[1])
        } else {
            Symbol::new(product_id, "UNKNOWN")
        }
    }

    // -----------------------------------------------------------------------
    // WebSocket
    // -----------------------------------------------------------------------

    fn spawn_ws_listener(
        config: CoinbaseExecutionConfig,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Coinbase: connecting user data WebSocket");

            let ws_stream = match tokio_tungstenite::connect_async(&config.ws_url).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(error = %e, "Coinbase: WebSocket connection failed");
                    connected.store(false, Ordering::SeqCst);
                    return;
                }
            };

            let (mut write, mut read) = ws_stream.split();

            // Authenticate and subscribe to the user channel using JWT (no uri claim for WS).
            let jwt = match Self::build_jwt(&config.api_key, &config.secret_key, None) {
                Ok(t) => t,
                Err(e) => {
                    error!(error = %e, "Coinbase: failed to build WS JWT");
                    connected.store(false, Ordering::SeqCst);
                    return;
                }
            };

            let subscribe = serde_json::json!({
                "type": "subscribe",
                "product_ids": [],
                "channel": "user",
                "jwt": jwt,
            });

            if let Err(e) = write
                .send(WsMessage::Text(subscribe.to_string()))
                .await
            {
                error!(error = %e, "Coinbase: failed to send subscribe");
                connected.store(false, Ordering::SeqCst);
                return;
            }

            info!("Coinbase: subscribed to user channel");

            while let Some(msg_result) = read.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        error!(error = %e, "Coinbase: WebSocket read error");
                        break;
                    }
                };

                match msg {
                    WsMessage::Text(text) => {
                        if let Err(e) = Self::handle_ws_message(&text, &update_tx).await {
                            warn!(error = %e, "Coinbase: failed to process WS message");
                        }
                    }
                    WsMessage::Ping(data) => {
                        if let Err(e) = write.send(WsMessage::Pong(data)).await {
                            warn!(error = %e, "Coinbase: failed to send pong");
                        }
                    }
                    WsMessage::Close(_) => {
                        info!("Coinbase: WebSocket closed by server");
                        break;
                    }
                    _ => {}
                }
            }

            connected.store(false, Ordering::SeqCst);
            warn!("Coinbase: user data stream disconnected");
        })
    }

    async fn handle_ws_message(
        text: &str,
        update_tx: &mpsc::Sender<OrderUpdate>,
    ) -> MmResult<()> {
        let msg: serde_json::Value =
            serde_json::from_str(text).map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("JSON parse error: {}", e),
            })?;

        let channel = msg["channel"].as_str().unwrap_or("");
        if channel != "user" {
            return Ok(());
        }

        let events = match msg["events"].as_array() {
            Some(e) => e,
            None => return Ok(()),
        };

        for event in events {
            let event_type = event["type"].as_str().unwrap_or("");
            let orders = match event["orders"].as_array() {
                Some(o) => o,
                None => continue,
            };

            for order in orders {
                if let Some(update) = Self::parse_order_event(event_type, order)? {
                    debug!(
                        client_order_id = %update.client_order_id,
                        status = %update.status,
                        "Coinbase: order update received"
                    );
                    update_tx
                        .send(update)
                        .await
                        .map_err(|e| MmError::Exchange {
                            exchange: Exchange::Coinbase,
                            message: format!("channel send error: {}", e),
                        })?;
                }
            }
        }

        Ok(())
    }

    fn parse_order_event(
        _event_type: &str,
        order: &serde_json::Value,
    ) -> MmResult<Option<OrderUpdate>> {
        let client_order_id = order["client_order_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let exchange_order_id = order["order_id"].as_str().map(|s| s.to_string());
        let product_id = order["product_id"].as_str().unwrap_or("");
        let symbol = Self::from_coinbase_product_id(product_id);

        let status_str = order["status"].as_str().unwrap_or("");
        let status = match status_str {
            "OPEN" => OrderStatus::New,
            "FILLED" => OrderStatus::Filled,
            "CANCELLED" => OrderStatus::Cancelled,
            "EXPIRED" => OrderStatus::Expired,
            "FAILED" => OrderStatus::Rejected,
            "PENDING" => OrderStatus::Submitted,
            _ => return Ok(None),
        };

        let filled_quantity = order["cumulative_quantity"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let leaves_quantity = order["leaves_quantity"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let average_fill_price = order["average_filled_price"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let total_fees = order["total_fees"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok());

        let timestamp = order["creation_time"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Ok(Some(OrderUpdate {
            client_order_id,
            exchange_order_id,
            exchange: Exchange::Coinbase,
            symbol,
            status,
            filled_quantity,
            remaining_quantity: leaves_quantity,
            average_fill_price,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: total_fees,
            fee_currency: None,
            timestamp,
        }))
    }

    // -----------------------------------------------------------------------
    // Order side / type helpers
    // -----------------------------------------------------------------------

    fn side_to_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for CoinbaseExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Coinbase
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("Coinbase: connecting execution connector");
        self.connected.store(true, Ordering::SeqCst);

        self.ws_handle = Some(Self::spawn_ws_listener(
            self.config.clone(),
            update_tx,
            Arc::clone(&self.connected),
        ));

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("Coinbase: disconnecting");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        let client_order_id = Uuid::new_v4().to_string();
        let product_id = Self::to_coinbase_product_id(&order.symbol);
        let side = Self::side_to_str(order.side);

        // Build the order configuration based on order type.
        let order_configuration = match order.order_type {
            mm_core::OrderType::Market => {
                serde_json::json!({
                    "market_market_ioc": {
                        "base_size": order.quantity.to_string(),
                    }
                })
            }
            mm_core::OrderType::Limit => {
                let price = order
                    .price
                    .ok_or_else(|| MmError::OrderRejected {
                        reason: "Limit order requires a price".into(),
                    })?;
                serde_json::json!({
                    "limit_limit_gtc": {
                        "base_size": order.quantity.to_string(),
                        "limit_price": price.to_string(),
                        "post_only": false,
                    }
                })
            }
            mm_core::OrderType::StopLimit => {
                let price = order
                    .price
                    .ok_or_else(|| MmError::OrderRejected {
                        reason: "Stop-limit order requires a price".into(),
                    })?;
                serde_json::json!({
                    "stop_limit_stop_limit_gtc": {
                        "base_size": order.quantity.to_string(),
                        "limit_price": price.to_string(),
                        "stop_price": price.to_string(),
                        "stop_direction": if order.side == Side::Buy { "STOP_DIRECTION_STOP_UP" } else { "STOP_DIRECTION_STOP_DOWN" },
                    }
                })
            }
            mm_core::OrderType::StopMarket => {
                let price = order
                    .price
                    .ok_or_else(|| MmError::OrderRejected {
                        reason: "Stop-market order requires a stop price".into(),
                    })?;
                serde_json::json!({
                    "stop_limit_stop_limit_gtc": {
                        "base_size": order.quantity.to_string(),
                        "limit_price": price.to_string(),
                        "stop_price": price.to_string(),
                        "stop_direction": if order.side == Side::Buy { "STOP_DIRECTION_STOP_UP" } else { "STOP_DIRECTION_STOP_DOWN" },
                    }
                })
            }
        };

        let body = serde_json::json!({
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": side,
            "order_configuration": order_configuration,
        });

        let body_str = body.to_string();
        let path = "/api/v3/brokerage/orders";
        let url = format!("{}{}", self.config.base_url, path);

        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            // Regenerate auth headers each attempt (JWT includes a fresh timestamp).
            let headers = self.auth_headers("POST", path, &body_str)?;

            let mut req = self.http.post(&url).body(body_str.clone());
            for (k, v) in &headers {
                req = req.header(k, v);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(MmError::Exchange {
                        exchange: Exchange::Coinbase,
                        message: format!("submit_order HTTP error: {}", e),
                    });
                    if attempt < MAX_RETRIES {
                        warn!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            error = %e,
                            "Coinbase: submit_order request failed, retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                        continue;
                    }
                    return Err(last_error.unwrap());
                }
            };

            let status = resp.status();

            // Retry on 429 (rate limited) or 5xx (server error).
            if status.as_u16() == 429 || status.is_server_error() {
                let resp_text = resp.text().await.unwrap_or_default();
                warn!(
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    status = %status,
                    body = %resp_text,
                    "Coinbase: submit_order rate limited or server error, retrying"
                );
                last_error = Some(MmError::Exchange {
                    exchange: Exchange::Coinbase,
                    message: format!("submit_order HTTP {} : {}", status, resp_text),
                });
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    continue;
                }
                return Err(last_error.unwrap());
            }

            let resp_body: serde_json::Value =
                resp.json().await.map_err(|e| MmError::Exchange {
                    exchange: Exchange::Coinbase,
                    message: format!("submit_order parse error: {}", e),
                })?;

            if !status.is_success() {
                let error_msg = resp_body["error"]
                    .as_str()
                    .or_else(|| resp_body["message"].as_str())
                    .unwrap_or("unknown error");
                return Err(MmError::OrderRejected {
                    reason: format!("Coinbase error: {}", error_msg),
                });
            }

            let order_id = resp_body["success_response"]["order_id"]
                .as_str()
                .unwrap_or(&client_order_id)
                .to_string();

            info!(
                exchange_order_id = %order_id,
                %product_id,
                side = %side,
                "Coinbase: order submitted"
            );

            return Ok(order_id);
        }

        Err(last_error.unwrap_or_else(|| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: "submit_order failed after retries".into(),
        }))
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        let order_id = cancel
            .exchange_order_id
            .as_deref()
            .unwrap_or(&cancel.client_order_id);

        let body = serde_json::json!({
            "order_ids": [order_id],
        });

        let body_str = body.to_string();
        let path = "/api/v3/brokerage/orders/batch_cancel";
        let url = format!("{}{}", self.config.base_url, path);

        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            // Regenerate auth headers each attempt (JWT includes a fresh timestamp).
            let headers = self.auth_headers("POST", path, &body_str)?;

            let mut req = self.http.post(&url).body(body_str.clone());
            for (k, v) in &headers {
                req = req.header(k, v);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(MmError::Exchange {
                        exchange: Exchange::Coinbase,
                        message: format!("cancel_order HTTP error: {}", e),
                    });
                    if attempt < MAX_RETRIES {
                        warn!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            error = %e,
                            "Coinbase: cancel_order request failed, retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                        continue;
                    }
                    return Err(last_error.unwrap());
                }
            };

            let status = resp.status();

            // Retry on 429 (rate limited) or 5xx (server error).
            if status.as_u16() == 429 || status.is_server_error() {
                let resp_text = resp.text().await.unwrap_or_default();
                warn!(
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    status = %status,
                    body = %resp_text,
                    "Coinbase: cancel_order rate limited or server error, retrying"
                );
                last_error = Some(MmError::Exchange {
                    exchange: Exchange::Coinbase,
                    message: format!("cancel_order HTTP {} : {}", status, resp_text),
                });
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    continue;
                }
                return Err(last_error.unwrap());
            }

            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
                let msg = body["error"]
                    .as_str()
                    .or_else(|| body["message"].as_str())
                    .unwrap_or("unknown error");
                return Err(MmError::Exchange {
                    exchange: Exchange::Coinbase,
                    message: format!("cancel_order failed: {}", msg),
                });
            }

            info!(
                client_order_id = %cancel.client_order_id,
                "Coinbase: order cancelled"
            );
            return Ok(());
        }

        Err(last_error.unwrap_or_else(|| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: "cancel_order failed after retries".into(),
        }))
    }

    async fn cancel_all_orders(&self, request: &CancelAllOrders) -> MmResult<u32> {
        // Coinbase Advanced Trade doesn't have a native cancel-all by symbol.
        // We need to list open orders and cancel them in a batch.
        let path = "/api/v3/brokerage/orders/historical/batch";
        let mut query_params = vec![("order_status", "OPEN")];

        let product_id_str;
        if let Some(ref sym) = request.symbol {
            product_id_str = Self::to_coinbase_product_id(sym);
            query_params.push(("product_id", &product_id_str));
        }

        let query_string: String = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");
        let full_path = format!("{}?{}", path, query_string);

        let headers = self.auth_headers("GET", &full_path, "")?;
        let url = format!("{}{}", self.config.base_url, full_path);

        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }

        let resp = req.send().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("list orders HTTP error: {}", e),
        })?;

        let resp_body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("list orders parse error: {}", e),
            })?;

        let order_ids: Vec<String> = resp_body["orders"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|o| o["order_id"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        if order_ids.is_empty() {
            return Ok(0);
        }

        let count = order_ids.len() as u32;

        // Cancel them in batch.
        let cancel_body = serde_json::json!({ "order_ids": order_ids });
        let cancel_body_str = cancel_body.to_string();
        let cancel_path = "/api/v3/brokerage/orders/batch_cancel";
        let cancel_headers = self.auth_headers("POST", cancel_path, &cancel_body_str)?;
        let cancel_url = format!("{}{}", self.config.base_url, cancel_path);

        let mut cancel_req = self.http.post(&cancel_url).body(cancel_body_str);
        for (k, v) in &cancel_headers {
            cancel_req = cancel_req.header(k, v);
        }

        cancel_req.send().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("batch cancel HTTP error: {}", e),
        })?;

        info!(count, "Coinbase: cancel_all completed");
        Ok(count)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let path = "/api/v3/brokerage/accounts";
        let headers = self.auth_headers("GET", path, "")?;
        let url = format!("{}{}", self.config.base_url, path);

        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }

        let resp = req.send().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::Coinbase,
            message: format!("fetch_balances HTTP error: {}", e),
        })?;

        let status = resp.status();
        let body: serde_json::Value =
            resp.json().await.map_err(|e| MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("fetch_balances parse error: {}", e),
            })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::Coinbase,
                message: format!("fetch_balances error: {}", body),
            });
        }

        let balances: Vec<Balance> = body["accounts"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|acct| {
                        let currency = acct["currency"].as_str()?;
                        let available: Decimal = acct["available_balance"]["value"]
                            .as_str()?
                            .parse()
                            .ok()?;
                        let hold: Decimal = acct["hold"]["value"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(Decimal::ZERO);
                        let total = available + hold;
                        if total > Decimal::ZERO {
                            Some(Balance {
                                currency: currency.to_string(),
                                free: available,
                                locked: hold,
                                total,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        debug!(count = balances.len(), "Coinbase: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}
