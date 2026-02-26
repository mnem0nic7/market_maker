//! Interactive Brokers execution connector.
//!
//! Implements [`ExecutionConnector`] for IB via the Client Portal API gateway.
//!
//! ## REST API
//!
//! | Endpoint                                            | Method   | Description              |
//! |-----------------------------------------------------|----------|--------------------------|
//! | `/iserver/account/{accountId}/orders`                | `POST`   | Submit a new order       |
//! | `/iserver/reply/{replyId}`                           | `POST`   | Confirm an order prompt  |
//! | `/iserver/account/{accountId}/order/{orderId}`       | `DELETE`  | Cancel an order          |
//! | `/iserver/account/orders`                            | `GET`    | List open orders         |
//! | `/portfolio/{accountId}/summary`                     | `GET`    | Account summary/balances |
//!
//! ## Authentication
//!
//! The IB Client Portal gateway handles authentication via browser-based
//! login. API calls only need to send `accept: application/json`. The
//! gateway uses self-signed TLS certificates, so the HTTP client is
//! configured to accept invalid certs.
//!
//! ## Order Confirmations
//!
//! IB may return a confirmation prompt instead of directly placing an order.
//! When this happens the response contains a `replyId` which must be
//! confirmed via `POST /iserver/reply/{replyId}` with `{"confirmed": true}`.

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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_BASE_URL: &str = "https://localhost:5000/v1/api";
const ORDER_POLL_INTERVAL_SECS: u64 = 2;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the IB execution connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbExecutionConfig {
    /// Base URL for the IB Client Portal REST API.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// IB account identifier.
    pub account_id: String,
    /// Mapping of canonical symbol strings (e.g., "AAPL") to IB contract IDs.
    pub conid_map: HashMap<String, String>,
}

fn default_base_url() -> String {
    DEFAULT_BASE_URL.into()
}

// ---------------------------------------------------------------------------
// IB order submission types
// ---------------------------------------------------------------------------

/// Request body for POST `/iserver/account/{accountId}/orders`.
#[derive(Debug, Serialize)]
struct IbOrderRequest {
    orders: Vec<IbOrderBody>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IbOrderBody {
    conid: u64,
    order_type: String,
    side: String,
    quantity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    tif: String,
}

/// Confirmation request for POST `/iserver/reply/{replyId}`.
#[derive(Debug, Serialize)]
struct IbReplyConfirmation {
    confirmed: bool,
}

// ---------------------------------------------------------------------------
// IB order response types
// ---------------------------------------------------------------------------

/// IB may return either an order confirmation or a reply prompt.
/// We try to parse both shapes.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct IbOrderResponse {
    /// Present when the order is accepted.
    order_id: Option<String>,
    /// Present when IB requires a confirmation.
    id: Option<String>,
    /// Message (may be informational or error).
    message: Option<Vec<String>>,
    /// Alternate field for order status.
    order_status: Option<String>,
}

/// Order status from GET `/iserver/account/orders`.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct IbOpenOrder {
    #[serde(rename = "orderId")]
    order_id: Option<u64>,
    /// Client-assigned order reference.
    #[serde(rename = "order_ref", default)]
    order_ref: Option<String>,
    conid: Option<u64>,
    /// "BUY" or "SELL"
    side: Option<String>,
    /// Order status: Submitted, Filled, Cancelled, etc.
    status: Option<String>,
    /// Filled quantity.
    #[serde(rename = "filledQuantity", default)]
    filled_quantity: Option<String>,
    /// Remaining quantity.
    #[serde(rename = "remainingQuantity", default)]
    remaining_quantity: Option<String>,
    /// Average fill price.
    #[serde(rename = "avgPrice", default)]
    avg_price: Option<String>,
    /// Last fill price.
    #[serde(rename = "lastFillPrice", default)]
    last_fill_price: Option<String>,
}

/// Wrapper for the orders endpoint response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct IbOrdersResponse {
    orders: Option<Vec<IbOpenOrder>>,
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

pub struct IbExecutionConnector {
    config: IbExecutionConfig,
    http: Client,
    connected: Arc<AtomicBool>,
    /// Handle to the background order polling task.
    poll_handle: Option<JoinHandle<()>>,
}

impl IbExecutionConnector {
    pub fn new(config: IbExecutionConfig) -> Self {
        let http = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client for IB");

        Self {
            config,
            http,
            connected: Arc::new(AtomicBool::new(false)),
            poll_handle: None,
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Resolve a canonical [`Symbol`] to an IB contract ID.
    fn resolve_conid(&self, symbol: &Symbol) -> MmResult<u64> {
        self.config
            .conid_map
            .get(&symbol.base)
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("no conid mapping for symbol {}", symbol),
            })
    }

    /// Reverse-lookup: find the canonical [`Symbol`] for a given contract ID.
    fn symbol_for_conid(conid_map: &HashMap<String, String>, conid: u64) -> Symbol {
        let conid_str = conid.to_string();
        for (base, cid) in conid_map {
            if *cid == conid_str {
                return Symbol::new(base.as_str(), "USD");
            }
        }
        Symbol::new("UNKNOWN", "USD")
    }

    /// Map our [`Side`] to the IB API string.
    fn side_to_str(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    /// Map our [`OrderType`] to the IB API string.
    fn order_type_to_str(ot: mm_core::OrderType) -> &'static str {
        match ot {
            mm_core::OrderType::Market => "MKT",
            mm_core::OrderType::Limit => "LMT",
            mm_core::OrderType::StopLimit => "STP LMT",
            mm_core::OrderType::StopMarket => "STP",
        }
    }

    /// Map our [`TimeInForce`] to the IB API string.
    fn tif_to_str(tif: mm_core::TimeInForce) -> &'static str {
        match tif {
            mm_core::TimeInForce::GTC => "GTC",
            mm_core::TimeInForce::IOC => "IOC",
            mm_core::TimeInForce::FOK => "FOK",
            mm_core::TimeInForce::GTD => "GTC", // IB has GTD but requires expiry; fall back.
        }
    }

    /// Map an IB order status string to our [`OrderStatus`].
    fn parse_order_status(status: &str) -> OrderStatus {
        match status {
            "Submitted" | "PreSubmitted" => OrderStatus::New,
            "Filled" => OrderStatus::Filled,
            "Cancelled" | "ApiCancelled" => OrderStatus::Cancelled,
            "Inactive" => OrderStatus::Rejected,
            _ => OrderStatus::New,
        }
    }

    /// Handle IB's confirmation prompt workflow. When an order submission
    /// returns a reply prompt instead of a direct order ID, we must confirm it.
    async fn confirm_reply(&self, reply_id: &str) -> MmResult<String> {
        let url = format!("{}/iserver/reply/{}", self.config.base_url, reply_id);

        let body = IbReplyConfirmation { confirmed: true };

        let resp = self
            .http
            .post(&url)
            .header("accept", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("reply confirmation request failed: {}", e),
            })?;

        let status = resp.status();
        let resp_body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("reply confirmation parse error: {}", e),
        })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("reply confirmation failed ({}): {}", status, resp_body),
            });
        }

        // The confirmation response is typically an array. Extract the order ID.
        let order_id = if let Some(arr) = resp_body.as_array() {
            arr.first()
                .and_then(|v| {
                    v.get("order_id")
                        .and_then(|id| id.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            v.get("order_id")
                                .and_then(|id| id.as_u64().map(|n| n.to_string()))
                        })
                })
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            resp_body
                .get("order_id")
                .and_then(|id| id.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown".to_string())
        };

        Ok(order_id)
    }

    /// Fetch all open orders from IB.
    async fn fetch_open_orders(&self) -> MmResult<Vec<IbOpenOrder>> {
        let url = format!("{}/iserver/account/orders", self.config.base_url);

        let resp = self
            .http
            .get(&url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("fetch open orders request failed: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("fetch open orders parse error: {}", e),
        })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("fetch open orders error ({}): {}", status, body),
            });
        }

        // The response may be an object with "orders" field or a direct array.
        let orders: Vec<IbOpenOrder> = if let Some(obj) = body.as_object() {
            if let Some(orders_val) = obj.get("orders") {
                serde_json::from_value(orders_val.clone()).unwrap_or_default()
            } else {
                Vec::new()
            }
        } else if body.is_array() {
            serde_json::from_value(body).unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(orders)
    }

    /// Spawn a background task that polls the orders endpoint and emits
    /// [`OrderUpdate`] messages through the provided channel.
    fn spawn_order_poller(
        http: Client,
        base_url: String,
        conid_map: HashMap<String, String>,
        update_tx: mpsc::Sender<OrderUpdate>,
        connected: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(ORDER_POLL_INTERVAL_SECS));
            // Track previously seen order states to only emit changes.
            let mut known_states: HashMap<String, String> = HashMap::new();

            loop {
                interval.tick().await;

                if !connected.load(Ordering::SeqCst) {
                    info!("IB: order poller stopping -- not connected");
                    break;
                }

                let url = format!("{}/iserver/account/orders", base_url);
                let resp = match http
                    .get(&url)
                    .header("accept", "application/json")
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(error = %e, "IB: order poll request failed");
                        continue;
                    }
                };

                if !resp.status().is_success() {
                    warn!(status = %resp.status(), "IB: order poll returned error");
                    continue;
                }

                let body: serde_json::Value = match resp.json().await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "IB: order poll parse error");
                        continue;
                    }
                };

                let orders: Vec<IbOpenOrder> = if let Some(obj) = body.as_object() {
                    if let Some(orders_val) = obj.get("orders") {
                        serde_json::from_value(orders_val.clone()).unwrap_or_default()
                    } else {
                        Vec::new()
                    }
                } else if body.is_array() {
                    serde_json::from_value(body).unwrap_or_default()
                } else {
                    Vec::new()
                };

                for order in &orders {
                    let order_id_str = order.order_id.map(|id| id.to_string()).unwrap_or_default();

                    if order_id_str.is_empty() {
                        continue;
                    }

                    let current_status = order.status.as_deref().unwrap_or("Unknown");

                    // Only emit update if state changed.
                    let prev = known_states.get(&order_id_str);
                    if prev.map(|s| s.as_str()) == Some(current_status) {
                        continue;
                    }
                    known_states.insert(order_id_str.clone(), current_status.to_string());

                    let conid = order.conid.unwrap_or(0);
                    let symbol = Self::symbol_for_conid(&conid_map, conid);
                    let status = Self::parse_order_status(current_status);

                    let filled_quantity = order
                        .filled_quantity
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);

                    let remaining_quantity = order
                        .remaining_quantity
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);

                    let average_fill_price = order
                        .avg_price
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);

                    let last_fill_price = order
                        .last_fill_price
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok());

                    let client_order_id = order
                        .order_ref
                        .clone()
                        .unwrap_or_else(|| order_id_str.clone());

                    let update = OrderUpdate {
                        client_order_id,
                        exchange_order_id: Some(order_id_str.clone()),
                        exchange: Exchange::InteractiveBrokers,
                        symbol,
                        status,
                        filled_quantity,
                        remaining_quantity,
                        average_fill_price,
                        last_fill_price,
                        last_fill_quantity: None,
                        fee: None,
                        fee_currency: None,
                        timestamp: Utc::now(),
                    };

                    if let Err(e) = update_tx.send(update).await {
                        warn!(error = %e, "IB: failed to send order update");
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// ExecutionConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ExecutionConnector for IbExecutionConnector {
    fn exchange(&self) -> Exchange {
        Exchange::InteractiveBrokers
    }

    async fn connect(&mut self, update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
        info!("IB: connecting execution connector");

        // Verify connectivity by hitting the tickle endpoint (session keepalive).
        let tickle_url = format!("{}/tickle", self.config.base_url);
        let resp = self
            .http
            .post(&tickle_url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("IB session tickle failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!(
                    "IB session tickle returned status {}. Is the Client Portal gateway running?",
                    resp.status()
                ),
            });
        }

        self.connected.store(true, Ordering::SeqCst);
        info!("IB: execution connector connected");

        // Spawn the order polling task.
        self.poll_handle = Some(Self::spawn_order_poller(
            self.http.clone(),
            self.config.base_url.clone(),
            self.config.conid_map.clone(),
            update_tx,
            Arc::clone(&self.connected),
        ));

        Ok(())
    }

    async fn disconnect(&mut self) -> MmResult<()> {
        info!("IB: disconnecting execution connector");
        self.connected.store(false, Ordering::SeqCst);
        if let Some(h) = self.poll_handle.take() {
            h.abort();
        }
        Ok(())
    }

    async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
        let conid = self.resolve_conid(&order.symbol)?;
        let side = Self::side_to_str(order.side);
        let order_type = Self::order_type_to_str(order.order_type);
        let tif = Self::tif_to_str(order.time_in_force);

        let ib_order = IbOrderBody {
            conid,
            order_type: order_type.to_string(),
            side: side.to_string(),
            quantity: order.quantity.to_string(),
            price: order.price.map(|p| p.to_string()),
            tif: tif.to_string(),
        };

        let request_body = IbOrderRequest {
            orders: vec![ib_order],
        };

        let url = format!(
            "{}/iserver/account/{}/orders",
            self.config.base_url, self.config.account_id
        );

        debug!(url = %url, "IB: submitting order");

        let resp = self
            .http
            .post(&url)
            .header("accept", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("submit_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("submit_order parse error: {}", e),
        })?;

        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("message").and_then(|v| v.as_str()))
                .unwrap_or("unknown error");
            return Err(MmError::OrderRejected {
                reason: format!("IB error: {}", msg),
            });
        }

        // IB returns an array of response objects. Parse the first one.
        let responses: Vec<IbOrderResponse> = if body.is_array() {
            serde_json::from_value(body.clone()).unwrap_or_default()
        } else {
            vec![
                serde_json::from_value(body.clone()).unwrap_or(IbOrderResponse {
                    order_id: None,
                    id: None,
                    message: None,
                    order_status: None,
                }),
            ]
        };

        let first = responses.first().ok_or_else(|| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: "empty response from IB order submission".into(),
        })?;

        // Check if IB is asking for a confirmation.
        if let Some(ref reply_id) = first.id {
            if first.order_id.is_none() {
                info!(reply_id = %reply_id, "IB: order requires confirmation, sending reply");
                let order_id = self.confirm_reply(reply_id).await?;
                info!(
                    exchange_order_id = %order_id,
                    "IB: order confirmed and submitted"
                );
                return Ok(order_id);
            }
        }

        let order_id = first
            .order_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        info!(
            exchange_order_id = %order_id,
            side = %side,
            "IB: order submitted"
        );

        Ok(order_id)
    }

    async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
        let order_id = cancel
            .exchange_order_id
            .as_ref()
            .ok_or_else(|| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: "IB cancel_order requires an exchange_order_id".into(),
            })?;

        let url = format!(
            "{}/iserver/account/{}/order/{}",
            self.config.base_url, self.config.account_id, order_id
        );

        let resp = self
            .http
            .delete(&url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("cancel_order HTTP error: {}", e),
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            let msg = body
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("cancel_order failed ({}): {}", status, msg),
            });
        }

        info!(
            exchange_order_id = %order_id,
            "IB: order cancelled"
        );
        Ok(())
    }

    async fn cancel_all_orders(&self, _request: &CancelAllOrders) -> MmResult<u32> {
        // IB Client Portal does not provide a native cancel-all endpoint.
        // Fetch all open orders and cancel each individually.
        let open_orders = self.fetch_open_orders().await?;

        let mut cancelled: u32 = 0;

        for order in &open_orders {
            let order_id = match order.order_id {
                Some(id) => id.to_string(),
                None => continue,
            };

            // Skip already-terminal orders.
            let status = order.status.as_deref().unwrap_or("Unknown");
            if status == "Filled" || status == "Cancelled" || status == "ApiCancelled" {
                continue;
            }

            let url = format!(
                "{}/iserver/account/{}/order/{}",
                self.config.base_url, self.config.account_id, order_id
            );

            match self
                .http
                .delete(&url)
                .header("accept", "application/json")
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    cancelled += 1;
                    debug!(exchange_order_id = %order_id, "IB: cancelled order");
                }
                Ok(resp) => {
                    warn!(
                        exchange_order_id = %order_id,
                        status = %resp.status(),
                        "IB: failed to cancel order"
                    );
                }
                Err(e) => {
                    warn!(
                        exchange_order_id = %order_id,
                        error = %e,
                        "IB: cancel request error"
                    );
                }
            }
        }

        info!(count = cancelled, "IB: cancel_all completed");
        Ok(cancelled)
    }

    async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
        let url = format!(
            "{}/portfolio/{}/summary",
            self.config.base_url, self.config.account_id
        );

        let resp = self
            .http
            .get(&url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|e| MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("fetch_balances HTTP error: {}", e),
            })?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.map_err(|e| MmError::Exchange {
            exchange: Exchange::InteractiveBrokers,
            message: format!("fetch_balances parse error: {}", e),
        })?;

        if !status.is_success() {
            return Err(MmError::Exchange {
                exchange: Exchange::InteractiveBrokers,
                message: format!("fetch_balances error ({}): {}", status, body),
            });
        }

        // IB portfolio summary returns an object where each key is a metric name
        // and the value contains an "amount" field. We extract cash-related fields.
        //
        // Example structure:
        // {
        //   "totalcashvalue": {"amount": 50000.00, "currency": "USD", ...},
        //   "availablefunds": {"amount": 45000.00, "currency": "USD", ...},
        //   "buyingpower": {"amount": 180000.00, "currency": "USD", ...},
        //   ...
        // }
        let mut balances: Vec<Balance> = Vec::new();

        let total_cash = body
            .get("totalcashvalue")
            .and_then(|v| v.get("amount"))
            .and_then(decimal_from_json_value)
            .unwrap_or(Decimal::ZERO);

        let available_funds = body
            .get("availablefunds")
            .and_then(|v| v.get("amount"))
            .and_then(decimal_from_json_value)
            .unwrap_or(Decimal::ZERO);

        let currency = body
            .get("totalcashvalue")
            .and_then(|v| v.get("currency"))
            .and_then(|v| v.as_str())
            .unwrap_or("USD")
            .to_string();

        let held = total_cash - available_funds;
        let held = if held < Decimal::ZERO {
            Decimal::ZERO
        } else {
            held
        };

        if total_cash > Decimal::ZERO || available_funds > Decimal::ZERO {
            balances.push(Balance {
                currency,
                free: available_funds,
                locked: held,
                total: total_cash,
            });
        }

        debug!(count = balances.len(), "IB: balances fetched");
        Ok(balances)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a [`Decimal`] from a JSON value that may be a number or string.
fn decimal_from_json_value(v: &serde_json::Value) -> Option<Decimal> {
    if let Some(s) = v.as_str() {
        s.parse::<Decimal>().ok()
    } else if let Some(f) = v.as_f64() {
        // Fall back to converting via string to preserve precision.
        f.to_string().parse::<Decimal>().ok()
    } else {
        v.as_i64().map(Decimal::from)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IbExecutionConfig {
        let mut conid_map = HashMap::new();
        conid_map.insert("AAPL".to_string(), "265598".to_string());
        conid_map.insert("MSFT".to_string(), "272093".to_string());

        IbExecutionConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            account_id: "U1234567".to_string(),
            conid_map,
        }
    }

    #[test]
    fn resolve_conid_found() {
        let connector = IbExecutionConnector::new(test_config());
        let sym = Symbol::new("AAPL", "USD");
        assert_eq!(connector.resolve_conid(&sym).unwrap(), 265598);
    }

    #[test]
    fn resolve_conid_not_found() {
        let connector = IbExecutionConnector::new(test_config());
        let sym = Symbol::new("TSLA", "USD");
        assert!(connector.resolve_conid(&sym).is_err());
    }

    #[test]
    fn symbol_for_conid_found() {
        let config = test_config();
        let sym = IbExecutionConnector::symbol_for_conid(&config.conid_map, 265598);
        assert_eq!(sym.base, "AAPL");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn symbol_for_conid_not_found() {
        let config = test_config();
        let sym = IbExecutionConnector::symbol_for_conid(&config.conid_map, 999999);
        assert_eq!(sym.base, "UNKNOWN");
    }

    #[test]
    fn side_mapping() {
        assert_eq!(IbExecutionConnector::side_to_str(Side::Buy), "BUY");
        assert_eq!(IbExecutionConnector::side_to_str(Side::Sell), "SELL");
    }

    #[test]
    fn order_type_mapping() {
        assert_eq!(
            IbExecutionConnector::order_type_to_str(mm_core::OrderType::Market),
            "MKT"
        );
        assert_eq!(
            IbExecutionConnector::order_type_to_str(mm_core::OrderType::Limit),
            "LMT"
        );
        assert_eq!(
            IbExecutionConnector::order_type_to_str(mm_core::OrderType::StopLimit),
            "STP LMT"
        );
        assert_eq!(
            IbExecutionConnector::order_type_to_str(mm_core::OrderType::StopMarket),
            "STP"
        );
    }

    #[test]
    fn tif_mapping() {
        assert_eq!(
            IbExecutionConnector::tif_to_str(mm_core::TimeInForce::GTC),
            "GTC"
        );
        assert_eq!(
            IbExecutionConnector::tif_to_str(mm_core::TimeInForce::IOC),
            "IOC"
        );
        assert_eq!(
            IbExecutionConnector::tif_to_str(mm_core::TimeInForce::FOK),
            "FOK"
        );
        assert_eq!(
            IbExecutionConnector::tif_to_str(mm_core::TimeInForce::GTD),
            "GTC" // falls back
        );
    }

    #[test]
    fn parse_order_status_mapping() {
        assert_eq!(
            IbExecutionConnector::parse_order_status("Submitted"),
            OrderStatus::New
        );
        assert_eq!(
            IbExecutionConnector::parse_order_status("PreSubmitted"),
            OrderStatus::New
        );
        assert_eq!(
            IbExecutionConnector::parse_order_status("Filled"),
            OrderStatus::Filled
        );
        assert_eq!(
            IbExecutionConnector::parse_order_status("Cancelled"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            IbExecutionConnector::parse_order_status("ApiCancelled"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            IbExecutionConnector::parse_order_status("Inactive"),
            OrderStatus::Rejected
        );
    }

    #[test]
    fn decimal_from_json_number() {
        let v = serde_json::json!(150.25);
        let d = decimal_from_json_value(&v).unwrap();
        assert_eq!(d, Decimal::new(15025, 2));
    }

    #[test]
    fn decimal_from_json_string() {
        let v = serde_json::json!("150.25");
        let d = decimal_from_json_value(&v).unwrap();
        assert_eq!(d, Decimal::new(15025, 2));
    }

    #[test]
    fn decimal_from_json_integer() {
        let v = serde_json::json!(100);
        let d = decimal_from_json_value(&v).unwrap();
        assert_eq!(d, Decimal::from(100));
    }

    #[test]
    fn decimal_from_json_null() {
        let v = serde_json::json!(null);
        assert!(decimal_from_json_value(&v).is_none());
    }
}
