//! Smart Order Router.
//!
//! The `OrderRouter` is the single entry point for all order actions. It:
//!
//! 1. Owns a mapping of `Exchange -> Box<dyn ExecutionConnector>`.
//! 2. Integrates with the [`OrderManager`] to track every order through its
//!    full lifecycle.
//! 3. Dispatches `Place`, `Cancel`, and `CancelAll` actions to the appropriate
//!    exchange connector.
//! 4. Spawns a background task that receives `OrderUpdate` messages from all
//!    connectors (via a shared `mpsc` channel) and feeds them into the OMS.

use std::collections::HashMap;
use std::sync::Arc;

use mm_core::{
    traits::ExecutionConnector, CancelAllOrders, CancelOrder, Exchange, MmError, MmResult,
    NewOrder, Order, OrderAction,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::oms::OrderManager;

// ---------------------------------------------------------------------------
// OrderRouter
// ---------------------------------------------------------------------------

/// Routes order actions to the appropriate exchange connector and tracks all
/// orders through the centralised [`OrderManager`].
pub struct OrderRouter {
    /// Per-exchange execution connectors.
    connectors: HashMap<Exchange, Box<dyn ExecutionConnector>>,
    /// Centralised order state tracker.
    oms: Arc<OrderManager>,
    /// Handle for the background update receiver task.
    update_task: Option<JoinHandle<()>>,
    /// Sender side of the shared update channel — cloned into each connector
    /// on `connect_all`.
    update_tx: mpsc::Sender<mm_core::OrderUpdate>,
    /// Receiver side — consumed by the background task.
    update_rx: Option<mpsc::Receiver<mm_core::OrderUpdate>>,
    /// Optional forwarding channel so the main loop can observe order updates
    /// (fills, cancellations, etc.) in addition to the OMS.
    forward_tx: Option<mpsc::Sender<mm_core::OrderUpdate>>,
}

impl OrderRouter {
    /// Create a new `OrderRouter`.
    ///
    /// `channel_capacity` controls the buffer size for the internal
    /// `OrderUpdate` channel (a sensible default is 4096).
    pub fn new(oms: Arc<OrderManager>, channel_capacity: usize) -> Self {
        let (update_tx, update_rx) = mpsc::channel(channel_capacity);
        Self {
            connectors: HashMap::new(),
            oms,
            update_task: None,
            update_tx,
            update_rx: Some(update_rx),
            forward_tx: None,
        }
    }

    /// Register an exchange connector. Must be called *before*
    /// [`connect_all`](Self::connect_all).
    pub fn register_connector(&mut self, connector: Box<dyn ExecutionConnector>) {
        let exchange = connector.exchange();
        info!(%exchange, "Router: connector registered");
        self.connectors.insert(exchange, connector);
    }

    /// Set an optional forwarding channel so the main loop can observe every
    /// `OrderUpdate` (fills, cancellations, etc.) in addition to the OMS.
    pub fn set_forward_channel(&mut self, tx: mpsc::Sender<mm_core::OrderUpdate>) {
        self.forward_tx = Some(tx);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Connect every registered connector and spawn the background update
    /// receiver task.
    pub async fn connect_all(&mut self) -> MmResult<()> {
        let mut first_error: Option<mm_core::MmError> = None;
        for (exchange, connector) in self.connectors.iter_mut() {
            info!(%exchange, "Router: connecting execution connector");
            if let Err(e) = connector.connect(self.update_tx.clone()).await {
                error!(%exchange, error = %e, "Router: failed to connect execution connector");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        // Take the receiver out — it can only be consumed once.
        let rx = self.update_rx.take().ok_or_else(|| {
            mm_core::MmError::Internal("connect_all called more than once".into())
        })?;

        let oms = Arc::clone(&self.oms);
        let forward_tx = self.forward_tx.clone();
        let handle = tokio::spawn(Self::run_update_loop(oms, rx, forward_tx));
        self.update_task = Some(handle);

        info!("Router: all connectors connected, update loop started");

        if let Some(e) = first_error {
            return Err(e);
        }

        Ok(())
    }

    /// Background loop that drains `OrderUpdate` messages and applies them to
    /// the OMS.
    async fn run_update_loop(
        oms: Arc<OrderManager>,
        mut rx: mpsc::Receiver<mm_core::OrderUpdate>,
        forward_tx: Option<mpsc::Sender<mm_core::OrderUpdate>>,
    ) {
        info!("Router: update loop started");
        while let Some(update) = rx.recv().await {
            debug!(
                client_order_id = %update.client_order_id,
                exchange_order_id = ?update.exchange_order_id,
                status = %update.status,
                "Router: received order update"
            );
            if oms.update_order(&update).is_none() {
                warn!(
                    client_order_id = %update.client_order_id,
                    "Router: OMS rejected the update"
                );
            }
            // Forward to main loop for strategy/position processing
            if let Some(ref fwd) = forward_tx {
                if fwd.try_send(update).is_err() {
                    warn!("Router: forward channel full or closed, dropping update");
                }
            }
        }
        info!("Router: update loop terminated (channel closed)");
    }

    /// Disconnect all connectors and abort the update loop.
    pub async fn disconnect_all(&mut self) -> MmResult<()> {
        for (exchange, connector) in self.connectors.iter_mut() {
            info!(%exchange, "Router: disconnecting execution connector");
            if let Err(e) = connector.disconnect().await {
                error!(%exchange, error = %e, "Router: error disconnecting connector");
            }
        }
        if let Some(handle) = self.update_task.take() {
            handle.abort();
        }
        info!("Router: all connectors disconnected");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Action routing
    // -----------------------------------------------------------------------

    /// Route a single `OrderAction` to the appropriate exchange connector.
    pub async fn route_action(&self, action: OrderAction) -> MmResult<()> {
        match action {
            OrderAction::Place(new_order) => {
                self.handle_place(new_order).await?;
            }
            OrderAction::Cancel(cancel) => {
                self.handle_cancel(cancel).await?;
            }
            OrderAction::CancelAll(cancel_all) => {
                self.handle_cancel_all(cancel_all).await?;
            }
            OrderAction::Amend {
                client_order_id,
                new_price,
                new_quantity,
            } => {
                self.handle_amend(&client_order_id, new_price, new_quantity)
                    .await?;
            }
        }
        Ok(())
    }

    /// Route a batch of order actions.
    pub async fn route_actions(&self, actions: Vec<OrderAction>) -> MmResult<()> {
        for action in actions {
            self.route_action(action).await?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn handle_place(&self, new_order: NewOrder) -> MmResult<Order> {
        let exchange = new_order.exchange;
        let connector = self.get_connector(exchange)?;

        // Register in OMS first.
        let order = self.oms.register_order(&new_order);
        let client_order_id = order.client_order_id.clone();

        info!(
            %client_order_id,
            %exchange,
            symbol = %new_order.symbol,
            side = %new_order.side,
            quantity = %new_order.quantity,
            price = ?new_order.price,
            "Router: placing order"
        );

        // Submit to exchange.
        match connector.submit_order(&new_order).await {
            Ok(returned_id) => {
                debug!(
                    %client_order_id,
                    exchange_returned_id = %returned_id,
                    "Router: order submitted to exchange"
                );
                self.oms.mark_submitted(&client_order_id);
                Ok(order)
            }
            Err(e) => {
                error!(%client_order_id, error = %e, "Router: order submission failed");
                // Mark the order as Rejected in the OMS.
                let reject_update = mm_core::OrderUpdate {
                    client_order_id: client_order_id.clone(),
                    exchange_order_id: None,
                    exchange,
                    symbol: new_order.symbol.clone(),
                    status: mm_core::OrderStatus::Rejected,
                    filled_quantity: rust_decimal::Decimal::ZERO,
                    remaining_quantity: new_order.quantity,
                    average_fill_price: rust_decimal::Decimal::ZERO,
                    last_fill_price: None,
                    last_fill_quantity: None,
                    fee: None,
                    fee_currency: None,
                    timestamp: chrono::Utc::now(),
                };
                self.oms.update_order(&reject_update);
                Err(e)
            }
        }
    }

    async fn handle_cancel(&self, cancel: CancelOrder) -> MmResult<()> {
        let connector = self.get_connector(cancel.exchange)?;

        info!(
            client_order_id = %cancel.client_order_id,
            exchange = %cancel.exchange,
            "Router: cancelling order"
        );

        self.oms.mark_pending_cancel(&cancel.client_order_id);
        connector.cancel_order(&cancel).await
    }

    async fn handle_cancel_all(&self, cancel_all: CancelAllOrders) -> MmResult<()> {
        match cancel_all.exchange {
            Some(exchange) => {
                let connector = self.get_connector(exchange)?;
                let count = connector.cancel_all_orders(&cancel_all).await?;
                info!(%exchange, cancelled = count, "Router: cancel-all completed");
            }
            None => {
                // Cancel on all connected exchanges.
                for (exchange, connector) in &self.connectors {
                    match connector.cancel_all_orders(&cancel_all).await {
                        Ok(count) => {
                            info!(%exchange, cancelled = count, "Router: cancel-all completed");
                        }
                        Err(e) => {
                            error!(%exchange, error = %e, "Router: cancel-all failed");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_amend(
        &self,
        client_order_id: &str,
        new_price: Option<rust_decimal::Decimal>,
        new_quantity: Option<rust_decimal::Decimal>,
    ) -> MmResult<()> {
        // Amend is typically cancel-and-replace. Look up the order, cancel it,
        // and place a new one with the updated parameters.
        let order = self.oms.get_order(client_order_id).ok_or_else(|| {
            MmError::OrderNotFound {
                order_id: client_order_id.to_string(),
            }
        })?;

        info!(
            %client_order_id,
            new_price = ?new_price,
            new_quantity = ?new_quantity,
            "Router: amending order (cancel-replace)"
        );

        // Cancel the existing order.
        let cancel = CancelOrder {
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
        };
        self.handle_cancel(cancel).await?;

        // Place a replacement.
        let replacement = NewOrder {
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            side: order.side,
            order_type: order.order_type,
            time_in_force: order.time_in_force,
            price: new_price.or(order.price),
            quantity: new_quantity.unwrap_or(order.remaining_quantity),
            strategy_id: order.strategy_id.clone(),
            reduce_only: order.reduce_only,
        };
        self.handle_place(replacement).await?;
        Ok(())
    }

    fn get_connector(&self, exchange: Exchange) -> MmResult<&dyn ExecutionConnector> {
        self.connectors
            .get(&exchange)
            .map(|c| c.as_ref())
            .ok_or_else(|| MmError::Exchange {
                exchange,
                message: "no connector registered for this exchange".into(),
            })
    }

    /// Access the underlying OMS (e.g., for queries).
    pub fn oms(&self) -> &Arc<OrderManager> {
        &self.oms
    }
}
