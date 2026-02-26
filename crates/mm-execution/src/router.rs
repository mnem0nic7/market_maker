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
    traits::{Balance, ExecutionConnector},
    CancelAllOrders, CancelOrder, Exchange, MmError, MmResult, NewOrder, Order, OrderAction,
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
        let order = self
            .oms
            .get_order(client_order_id)
            .ok_or_else(|| MmError::OrderNotFound {
                order_id: client_order_id.to_string(),
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

    /// Fetch balances from a specific exchange connector.
    ///
    /// Used by the reconciliation loop to compare exchange-reported balances
    /// against the local position state.
    pub async fn fetch_balances(&self, exchange: Exchange) -> MmResult<Vec<Balance>> {
        let connector = self.get_connector(exchange)?;
        connector.fetch_balances().await
    }

    /// Return the list of exchanges that have registered connectors.
    pub fn connected_exchanges(&self) -> Vec<Exchange> {
        self.connectors.keys().copied().collect()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mm_core::{
        traits::{Balance, ExecutionConnector},
        CancelAllOrders, CancelOrder, Exchange, MmResult, NewOrder, OrderAction, OrderType,
        OrderUpdate, Side, Symbol, TimeInForce,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    // -----------------------------------------------------------------------
    // Mock ExecutionConnector
    // -----------------------------------------------------------------------

    /// Records submitted and cancelled orders for assertion in tests.
    #[derive(Debug, Default, Clone)]
    struct MockState {
        submitted_orders: Vec<NewOrder>,
        cancelled_orders: Vec<CancelOrder>,
        cancel_all_calls: Vec<CancelAllOrders>,
    }

    struct MockExecutionConnector {
        exchange: Exchange,
        state: Arc<Mutex<MockState>>,
        connected: bool,
    }

    impl MockExecutionConnector {
        fn new(exchange: Exchange) -> (Self, Arc<Mutex<MockState>>) {
            let state = Arc::new(Mutex::new(MockState::default()));
            let connector = Self {
                exchange,
                state: state.clone(),
                connected: false,
            };
            (connector, state)
        }
    }

    #[async_trait]
    impl ExecutionConnector for MockExecutionConnector {
        fn exchange(&self) -> Exchange {
            self.exchange
        }

        async fn connect(&mut self, _update_tx: mpsc::Sender<OrderUpdate>) -> MmResult<()> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> MmResult<()> {
            self.connected = false;
            Ok(())
        }

        async fn submit_order(&self, order: &NewOrder) -> MmResult<String> {
            let mut s = self.state.lock().unwrap();
            s.submitted_orders.push(order.clone());
            Ok(format!("MOCK-{}", s.submitted_orders.len()))
        }

        async fn cancel_order(&self, cancel: &CancelOrder) -> MmResult<()> {
            let mut s = self.state.lock().unwrap();
            s.cancelled_orders.push(cancel.clone());
            Ok(())
        }

        async fn cancel_all_orders(&self, request: &CancelAllOrders) -> MmResult<u32> {
            let mut s = self.state.lock().unwrap();
            s.cancel_all_calls.push(request.clone());
            Ok(3) // pretend we cancelled 3 orders
        }

        async fn fetch_balances(&self) -> MmResult<Vec<Balance>> {
            Ok(vec![Balance {
                currency: "USD".into(),
                free: dec!(10000),
                locked: Decimal::ZERO,
                total: dec!(10000),
            }])
        }

        fn is_connected(&self) -> bool {
            self.connected
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn sample_new_order(exchange: Exchange) -> NewOrder {
        NewOrder {
            exchange,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50000)),
            quantity: dec!(0.1),
            strategy_id: "test-strategy".into(),
            reduce_only: false,
        }
    }

    fn make_router_with_mock(exchange: Exchange) -> (OrderRouter, Arc<Mutex<MockState>>) {
        let oms = Arc::new(OrderManager::new());
        let mut router = OrderRouter::new(oms, 128);
        let (connector, state) = MockExecutionConnector::new(exchange);
        router.register_connector(Box::new(connector));
        (router, state)
    }

    // -----------------------------------------------------------------------
    // Test 1: route_action with no connector returns an error
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_route_action_no_connector() {
        let oms = Arc::new(OrderManager::new());
        let router = OrderRouter::new(oms, 128);

        // No connectors registered — routing to Binance should fail.
        let action = OrderAction::Place(sample_new_order(Exchange::Binance));
        let result = router.route_action(action).await;

        assert!(
            result.is_err(),
            "expected error when no connector is registered"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no connector registered"),
            "error should mention missing connector, got: {}",
            err_msg
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: connect_all twice returns an error
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_connect_all_twice() {
        let (mut router, _state) = make_router_with_mock(Exchange::Binance);

        // First connect_all should succeed.
        let first = router.connect_all().await;
        assert!(first.is_ok(), "first connect_all should succeed");

        // Second connect_all should fail because the receiver has been consumed.
        let second = router.connect_all().await;
        assert!(second.is_err(), "second connect_all should fail");
        let err_msg = second.unwrap_err().to_string();
        assert!(
            err_msg.contains("connect_all called more than once"),
            "error should mention double-call, got: {}",
            err_msg
        );

        // Clean up the background task.
        router.disconnect_all().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 3: route a Place order action through the router
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_route_place_order() {
        let (mut router, state) = make_router_with_mock(Exchange::Binance);
        router.connect_all().await.unwrap();

        let new_order = sample_new_order(Exchange::Binance);
        let action = OrderAction::Place(new_order.clone());
        let result = router.route_action(action).await;

        assert!(result.is_ok(), "place order should succeed");

        // Verify the mock connector received the order.
        let s = state.lock().unwrap();
        assert_eq!(
            s.submitted_orders.len(),
            1,
            "connector should have received 1 order"
        );
        assert_eq!(s.submitted_orders[0].symbol, new_order.symbol);
        assert_eq!(s.submitted_orders[0].side, Side::Buy);
        assert_eq!(s.submitted_orders[0].quantity, dec!(0.1));

        // Verify the OMS tracked the order.
        let oms = router.oms();
        assert_eq!(oms.order_count(), 1, "OMS should track one order");

        drop(s);
        router.disconnect_all().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 4: route a Cancel order action
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_route_cancel_order() {
        let (mut router, state) = make_router_with_mock(Exchange::Binance);
        router.connect_all().await.unwrap();

        // First, place an order so we have something to cancel.
        let new_order = sample_new_order(Exchange::Binance);
        router
            .route_action(OrderAction::Place(new_order.clone()))
            .await
            .unwrap();

        // Get the client_order_id from the OMS.
        let oms = router.oms();
        let orders = oms.get_orders_by_strategy("test-strategy");
        assert_eq!(orders.len(), 1);
        let client_order_id = orders[0].client_order_id.clone();

        // Now cancel it.
        let cancel = CancelOrder {
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            client_order_id: client_order_id.clone(),
            exchange_order_id: None,
        };
        let result = router.route_action(OrderAction::Cancel(cancel)).await;
        assert!(result.is_ok(), "cancel order should succeed");

        // Verify the mock received the cancel request.
        let s = state.lock().unwrap();
        assert_eq!(
            s.cancelled_orders.len(),
            1,
            "connector should have received 1 cancel"
        );
        assert_eq!(s.cancelled_orders[0].client_order_id, client_order_id);

        drop(s);
        router.disconnect_all().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 5: route a CancelAll action
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_route_cancel_all() {
        let (mut router, state) = make_router_with_mock(Exchange::Binance);
        router.connect_all().await.unwrap();

        let cancel_all = CancelAllOrders {
            exchange: Some(Exchange::Binance),
            symbol: None,
            strategy_id: None,
        };
        let result = router
            .route_action(OrderAction::CancelAll(cancel_all))
            .await;
        assert!(result.is_ok(), "cancel-all should succeed");

        // Verify the mock received the cancel-all request.
        let s = state.lock().unwrap();
        assert_eq!(
            s.cancel_all_calls.len(),
            1,
            "connector should have received 1 cancel-all"
        );

        drop(s);
        router.disconnect_all().await.unwrap();
    }
}
