use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use mm_core::{Exchange, Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::rest::ApiState;

// ---------------------------------------------------------------------------
// Wire-protocol message types
// ---------------------------------------------------------------------------

/// Messages broadcast from the server to connected dashboard clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    PositionUpdate {
        exchange: Exchange,
        symbol: Symbol,
        quantity: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal,
    },
    OrderUpdate {
        client_order_id: String,
        exchange: Exchange,
        symbol: Symbol,
        status: String,
        filled_quantity: Decimal,
        remaining_quantity: Decimal,
    },
    RiskUpdate {
        net_exposure: Decimal,
        gross_exposure: Decimal,
        daily_pnl: Decimal,
        kill_switch_active: bool,
    },
    MarketDataTick {
        exchange: Exchange,
        symbol: Symbol,
        best_bid: Decimal,
        best_ask: Decimal,
        mid_price: Decimal,
        spread_bps: Decimal,
    },
    StrategyMetrics {
        strategy_id: String,
        metrics: mm_core::StrategyMetrics,
    },
    Error {
        message: String,
    },
    Subscribed {
        channel: String,
    },
    Unsubscribed {
        channel: String,
    },
}

/// Messages sent from dashboard clients to the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ClientMessage {
    #[serde(rename = "subscribe")]
    Subscribe { channel: String },

    #[serde(rename = "unsubscribe")]
    Unsubscribe { channel: String },

    #[serde(rename = "ping")]
    Ping,
}

/// Available subscription channels.
/// Clients subscribe to specific channels to filter the stream of updates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Channel {
    Positions,
    Orders,
    Risk,
    MarketData,
    All,
}

impl Channel {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "positions" => Some(Channel::Positions),
            "orders" => Some(Channel::Orders),
            "risk" => Some(Channel::Risk),
            "market_data" | "marketdata" | "market-data" => Some(Channel::MarketData),
            "all" | "*" => Some(Channel::All),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Dashboard Broadcaster
// ---------------------------------------------------------------------------

/// Central broadcaster that fans out system events to all connected WebSocket
/// clients.
///
/// # Architecture
///
/// ```text
/// mpsc::Sender ──> DashboardBroadcaster ──> broadcast::Sender ──> clients
///                    (internal task)
/// ```
///
/// System components push [`ServerMessage`]s into the mpsc channel.  The
/// broadcaster's internal task reads from that channel and forwards each
/// message over a tokio `broadcast` channel to every connected WebSocket
/// handler.
pub struct DashboardBroadcaster {
    /// System components send events here.
    event_tx: mpsc::Sender<ServerMessage>,

    /// Each WebSocket handler subscribes to this broadcast channel.
    broadcast_tx: broadcast::Sender<ServerMessage>,
}

impl DashboardBroadcaster {
    /// Create a new broadcaster.
    ///
    /// * `buffer_size` — capacity of the internal mpsc channel.
    /// * `broadcast_capacity` — capacity of the broadcast channel (how many
    ///   messages can be buffered before lagging receivers are dropped).
    pub fn new(buffer_size: usize, broadcast_capacity: usize) -> Arc<Self> {
        let (event_tx, event_rx) = mpsc::channel(buffer_size);
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

        let broadcaster = Arc::new(Self {
            event_tx,
            broadcast_tx,
        });

        // Spawn the forwarding task.
        let bc = broadcaster.clone();
        tokio::spawn(async move {
            bc.run(event_rx).await;
        });

        broadcaster
    }

    /// Obtain a sender handle that system components can use to push events
    /// into the broadcaster.
    pub fn sender(&self) -> mpsc::Sender<ServerMessage> {
        self.event_tx.clone()
    }

    /// Subscribe to the broadcast stream (used by each WebSocket handler).
    pub fn subscribe(&self) -> broadcast::Receiver<ServerMessage> {
        self.broadcast_tx.subscribe()
    }

    /// Internal forwarding loop: reads from mpsc and fans out via broadcast.
    async fn run(&self, mut event_rx: mpsc::Receiver<ServerMessage>) {
        info!("DashboardBroadcaster: forwarding task started");

        while let Some(msg) = event_rx.recv().await {
            // broadcast::send returns Err only when there are zero receivers,
            // which is a normal condition when no dashboards are connected.
            let _ = self.broadcast_tx.send(msg);
        }

        info!("DashboardBroadcaster: forwarding task stopped (sender dropped)");
    }
}

// ---------------------------------------------------------------------------
// WebSocket router & handler
// ---------------------------------------------------------------------------

/// Append the WebSocket route onto an existing router.
pub fn ws_router(state: Arc<ApiState>, broadcaster: Arc<DashboardBroadcaster>) -> Router {
    let ws_state = Arc::new(WsState {
        api_state: state,
        broadcaster,
    });

    Router::new()
        .route("/api/v1/ws", get(ws_upgrade))
        .with_state(ws_state)
}

/// Combined state available inside the WebSocket handler.
pub struct WsState {
    pub api_state: Arc<ApiState>,
    pub broadcaster: Arc<DashboardBroadcaster>,
}

/// Axum handler that upgrades an HTTP request to a WebSocket connection.
async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<Arc<WsState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state))
}

/// Per-connection WebSocket handler.
///
/// Each client maintains its own set of subscribed channels.  Incoming
/// subscribe / unsubscribe messages mutate the local subscription set.
/// Broadcast messages are forwarded only if the client is subscribed to the
/// relevant channel.
async fn handle_ws_connection(socket: WebSocket, state: Arc<WsState>) {
    let (mut ws_sink, mut ws_stream) = socket.split();

    // Default: subscribe to all channels.
    let subscriptions: Arc<RwLock<HashSet<Channel>>> = Arc::new(RwLock::new({
        let mut s = HashSet::new();
        s.insert(Channel::All);
        s
    }));

    let mut broadcast_rx = state.broadcaster.subscribe();

    info!("WebSocket client connected");

    // Spawn a task that forwards broadcast messages to this client.
    let subs_clone = subscriptions.clone();
    let mut send_task = tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(msg) => {
                    let subs = subs_clone.read().await;
                    if !should_send(&subs, &msg) {
                        continue;
                    }
                    drop(subs);

                    let text = match serde_json::to_string(&msg) {
                        Ok(t) => t,
                        Err(e) => {
                            error!("failed to serialize broadcast message: {e}");
                            continue;
                        }
                    };

                    if ws_sink.send(Message::Text(text.into())).await.is_err() {
                        // Client disconnected.
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("WebSocket client lagged, skipped {n} messages");
                    let warning = ServerMessage::Error {
                        message: format!("lagged: {n} messages skipped"),
                    };
                    if let Ok(text) = serde_json::to_string(&warning) {
                        let _ = ws_sink.send(Message::Text(text.into())).await;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    // Receive loop: handle client commands (subscribe / unsubscribe / ping).
    let subs_clone = subscriptions.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(result) = ws_stream.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    debug!("WebSocket recv error: {e}");
                    break;
                }
            };

            match msg {
                Message::Text(text) => {
                    handle_client_message(&text, &subs_clone).await;
                }
                Message::Close(_) => {
                    debug!("WebSocket client sent Close frame");
                    break;
                }
                // Ignore binary and ping/pong frames — axum handles pong
                // automatically.
                _ => {}
            }
        }
    });

    // When either task finishes, abort the other.
    tokio::select! {
        _ = &mut send_task => {
            recv_task.abort();
        }
        _ = &mut recv_task => {
            send_task.abort();
        }
    }

    info!("WebSocket client disconnected");
}

/// Process a text message from the client.
async fn handle_client_message(
    text: &str,
    subscriptions: &Arc<RwLock<HashSet<Channel>>>,
) {
    let parsed: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            debug!("invalid client message: {e}");
            // Note: error is logged but not sent back to any client.
            // The sender's ws_sink is owned by the send_task, so we can't
            // reply directly here. Broadcasting errors to all clients would
            // be a security issue.
            return;
        }
    };

    match parsed {
        ClientMessage::Subscribe { channel } => {
            if let Some(ch) = Channel::from_str(&channel) {
                let mut subs = subscriptions.write().await;
                subs.insert(ch);
                debug!(channel = %channel, "client subscribed");
            } else {
                debug!(channel = %channel, "unknown channel, ignoring subscribe");
            }
        }
        ClientMessage::Unsubscribe { channel } => {
            if let Some(ch) = Channel::from_str(&channel) {
                let mut subs = subscriptions.write().await;
                subs.remove(&ch);
                debug!(channel = %channel, "client unsubscribed");
            }
        }
        ClientMessage::Ping => {
            // The protocol-level ping/pong is handled by axum; this is an
            // application-level keepalive that we simply acknowledge by
            // doing nothing (or the client can listen for broadcast traffic).
            debug!("received application-level ping");
        }
    }
}

/// Determine whether a broadcast message should be forwarded to a client
/// based on its current subscription set.
fn should_send(subscriptions: &HashSet<Channel>, msg: &ServerMessage) -> bool {
    if subscriptions.contains(&Channel::All) {
        return true;
    }

    match msg {
        ServerMessage::PositionUpdate { .. } => subscriptions.contains(&Channel::Positions),
        ServerMessage::OrderUpdate { .. } => subscriptions.contains(&Channel::Orders),
        ServerMessage::RiskUpdate { .. } => subscriptions.contains(&Channel::Risk),
        ServerMessage::MarketDataTick { .. } => subscriptions.contains(&Channel::MarketData),
        // Strategy metrics, error, and ack messages are always forwarded.
        ServerMessage::StrategyMetrics { .. }
        | ServerMessage::Error { .. }
        | ServerMessage::Subscribed { .. }
        | ServerMessage::Unsubscribed { .. } => true,
    }
}
