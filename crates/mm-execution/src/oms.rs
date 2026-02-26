//! Order Management System (OMS).
//!
//! Centralised in-memory store for all orders across exchanges. Provides:
//! - Primary index by `client_order_id` (our internal identifier).
//! - Secondary index by `exchange_order_id` (the identifier assigned by the venue).
//! - State-machine validation on every transition — rejects illegal moves such as
//!   `Filled -> New`.
//! - Tracing instrumentation for every state change.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use mm_core::{CancelOrder, Exchange, NewOrder, Order, OrderStatus, OrderUpdate, Position};
use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State transition rules
// ---------------------------------------------------------------------------

/// Returns `true` if the transition from `current` to `next` is legal.
fn is_valid_transition(current: OrderStatus, next: OrderStatus) -> bool {
    use OrderStatus::*;
    matches!(
        (current, next),
        // From Pending — we either submit, get rejected, or cancel before sending.
        (Pending, Submitted)
            | (Pending, Rejected)
            | (Pending, Cancelled)
            // From Submitted — exchange acks, rejects, or we cancel.
            | (Submitted, New)
            | (Submitted, Rejected)
            | (Submitted, Cancelled)
            | (Submitted, PartiallyFilled)
            | (Submitted, Filled)
            // From New — fills, cancels, or expiry.
            | (New, PartiallyFilled)
            | (New, Filled)
            | (New, PendingCancel)
            | (New, Cancelled)
            | (New, Expired)
            // From PartiallyFilled — more fills, cancel, or expiry.
            | (PartiallyFilled, PartiallyFilled)
            | (PartiallyFilled, Filled)
            | (PartiallyFilled, PendingCancel)
            | (PartiallyFilled, Cancelled)
            | (PartiallyFilled, Expired)
            // From PendingCancel — actually cancelled or filled first.
            | (PendingCancel, Cancelled)
            | (PendingCancel, Filled)
            | (PendingCancel, PartiallyFilled)
    )
}

// ---------------------------------------------------------------------------
// OrderManager
// ---------------------------------------------------------------------------

/// Thread-safe order management system backed by `DashMap`.
///
/// All public methods are lock-free at the per-key level — concurrent reads
/// and writes to *different* orders proceed without contention.
#[derive(Debug, Clone)]
pub struct OrderManager {
    /// Primary index: client_order_id -> Order
    orders: Arc<DashMap<String, Order>>,
    /// Secondary index: exchange_order_id -> client_order_id
    exchange_index: Arc<DashMap<String, String>>,
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderManager {
    /// Create a new, empty `OrderManager`.
    pub fn new() -> Self {
        Self {
            orders: Arc::new(DashMap::new()),
            exchange_index: Arc::new(DashMap::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------------

    /// Register a new order from a `NewOrder` request.
    ///
    /// Assigns a fresh `client_order_id`, sets status to `Pending`, and
    /// inserts the order into the primary index.
    #[instrument(skip(self, new_order), fields(exchange = %new_order.exchange, symbol = %new_order.symbol, side = %new_order.side, strategy_id = %new_order.strategy_id))]
    pub fn register_order(&self, new_order: &NewOrder) -> Order {
        let now = Utc::now();
        let client_order_id = Uuid::new_v4().to_string();

        let order = Order {
            client_order_id: client_order_id.clone(),
            exchange_order_id: None,
            exchange: new_order.exchange,
            symbol: new_order.symbol.clone(),
            side: new_order.side,
            order_type: new_order.order_type,
            time_in_force: new_order.time_in_force,
            price: new_order.price,
            quantity: new_order.quantity,
            status: OrderStatus::Pending,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: new_order.quantity,
            average_fill_price: Decimal::ZERO,
            strategy_id: new_order.strategy_id.clone(),
            reduce_only: new_order.reduce_only,
            created_at: now,
            updated_at: now,
        };

        info!(
            client_order_id = %order.client_order_id,
            exchange = %order.exchange,
            symbol = %order.symbol,
            side = %order.side,
            order_type = %order.order_type,
            quantity = %order.quantity,
            price = ?order.price,
            strategy_id = %order.strategy_id,
            "OMS: order registered"
        );

        self.orders.insert(client_order_id, order.clone());
        order
    }

    // -----------------------------------------------------------------------
    // Updates
    // -----------------------------------------------------------------------

    /// Apply an `OrderUpdate` to the tracked order.
    ///
    /// Returns the updated `Order` if the update was applied successfully, or
    /// `None` if the order was not found or the state transition is invalid.
    #[instrument(skip(self), fields(client_order_id = %update.client_order_id, exchange_order_id = ?update.exchange_order_id, status = %update.status))]
    pub fn update_order(&self, update: &OrderUpdate) -> Option<Order> {
        // Look up by client_order_id first; fall back to exchange_order_id.
        let client_id = if self.orders.contains_key(&update.client_order_id) {
            update.client_order_id.clone()
        } else if let Some(exch_id) = &update.exchange_order_id {
            match self.exchange_index.get(exch_id) {
                Some(cid) => cid.value().clone(),
                None => {
                    warn!(
                        client_order_id = %update.client_order_id,
                        exchange_order_id = ?update.exchange_order_id,
                        "OMS: order not found for update"
                    );
                    return None;
                }
            }
        } else {
            warn!(
                client_order_id = %update.client_order_id,
                "OMS: order not found for update"
            );
            return None;
        };

        let mut entry = match self.orders.get_mut(&client_id) {
            Some(e) => e,
            None => {
                warn!(client_order_id = %client_id, "OMS: order disappeared during update");
                return None;
            }
        };

        let order = entry.value_mut();
        let old_status = order.status;
        let new_status = update.status;

        // Validate state transition.
        if !is_valid_transition(old_status, new_status) {
            warn!(
                client_order_id = %client_id,
                old_status = %old_status,
                new_status = %new_status,
                "OMS: invalid state transition — update ignored"
            );
            return None;
        }

        // Apply the update.
        order.status = new_status;
        order.filled_quantity = update.filled_quantity;
        order.remaining_quantity = update.remaining_quantity;
        order.average_fill_price = update.average_fill_price;
        order.updated_at = update.timestamp;

        // Update exchange_order_id if provided and not yet set.
        if let Some(ref exch_id) = update.exchange_order_id {
            if order.exchange_order_id.is_none() {
                order.exchange_order_id = Some(exch_id.clone());
                self.exchange_index
                    .insert(exch_id.clone(), client_id.clone());
                debug!(
                    client_order_id = %client_id,
                    exchange_order_id = %exch_id,
                    "OMS: exchange order ID linked"
                );
            }
        }

        info!(
            client_order_id = %client_id,
            exchange_order_id = ?order.exchange_order_id,
            old_status = %old_status,
            new_status = %new_status,
            filled_qty = %order.filled_quantity,
            remaining_qty = %order.remaining_quantity,
            avg_fill_price = %order.average_fill_price,
            "OMS: order state transition"
        );

        Some(order.clone())
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /// Look up an order by its `client_order_id`.
    pub fn get_order(&self, client_order_id: &str) -> Option<Order> {
        self.orders.get(client_order_id).map(|r| r.value().clone())
    }

    /// Look up an order by its `exchange_order_id`.
    pub fn get_by_exchange_id(&self, exchange_order_id: &str) -> Option<Order> {
        self.exchange_index
            .get(exchange_order_id)
            .and_then(|cid| self.orders.get(cid.value()).map(|r| r.value().clone()))
    }

    /// Return all orders that are currently active (i.e., not in a terminal state).
    pub fn get_open_orders(&self) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|r| r.value().status.is_active())
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return all orders belonging to a given strategy.
    pub fn get_orders_by_strategy(&self, strategy_id: &str) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|r| r.value().strategy_id == strategy_id)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Generate `CancelOrder` requests for every open order belonging to `strategy_id`.
    pub fn cancel_all_for_strategy(&self, strategy_id: &str) -> Vec<CancelOrder> {
        self.orders
            .iter()
            .filter(|r| r.value().strategy_id == strategy_id && r.value().status.is_active())
            .map(|r| {
                let order = r.value();
                CancelOrder {
                    exchange: order.exchange,
                    symbol: order.symbol.clone(),
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id: order.exchange_order_id.clone(),
                }
            })
            .collect()
    }

    /// Mark an order as `Submitted`. Convenience helper called by the router
    /// right after it hands the order off to the exchange connector.
    pub fn mark_submitted(&self, client_order_id: &str) -> Option<Order> {
        let mut entry = self.orders.get_mut(client_order_id)?;
        let order = entry.value_mut();

        if !is_valid_transition(order.status, OrderStatus::Submitted) {
            warn!(
                client_order_id = %client_order_id,
                current_status = %order.status,
                "OMS: cannot mark as Submitted — invalid transition"
            );
            return None;
        }

        let old_status = order.status;
        order.status = OrderStatus::Submitted;
        order.updated_at = Utc::now();

        debug!(
            client_order_id = %client_order_id,
            old_status = %old_status,
            new_status = "Submitted",
            "OMS: order marked submitted"
        );

        Some(order.clone())
    }

    /// Mark an order as `PendingCancel`. Called by the router when a cancel
    /// request is issued.
    pub fn mark_pending_cancel(&self, client_order_id: &str) -> Option<Order> {
        let mut entry = self.orders.get_mut(client_order_id)?;
        let order = entry.value_mut();

        if !is_valid_transition(order.status, OrderStatus::PendingCancel) {
            warn!(
                client_order_id = %client_order_id,
                current_status = %order.status,
                "OMS: cannot mark as PendingCancel — invalid transition"
            );
            return None;
        }

        let old_status = order.status;
        order.status = OrderStatus::PendingCancel;
        order.updated_at = Utc::now();

        debug!(
            client_order_id = %client_order_id,
            old_status = %old_status,
            new_status = "PendingCancel",
            "OMS: order marked pending cancel"
        );

        Some(order.clone())
    }

    /// Return the total number of tracked orders (all states).
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Return all orders for a given exchange.
    pub fn get_orders_by_exchange(&self, exchange: Exchange) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|r| r.value().exchange == exchange)
            .map(|r| r.value().clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Position Reconciliation
// ---------------------------------------------------------------------------

/// Result of comparing a local position against exchange-reported balances.
///
/// Used by the periodic reconciliation check to detect drift between the
/// in-memory position state and the actual balances reported by the exchange.
#[derive(Debug, Clone)]
pub struct PositionReconciliation {
    pub exchange: Exchange,
    pub symbol: String,
    pub local_quantity: Decimal,
    pub exchange_quantity: Decimal,
    pub drift: Decimal,
    pub drift_pct: Decimal,
    pub is_reconciled: bool,
    pub timestamp: DateTime<Utc>,
}

impl PositionReconciliation {
    /// Compare a local [`Position`] against the exchange-reported balance.
    ///
    /// A position is considered reconciled when the percentage drift is less
    /// than 1%. The caller can override this threshold in its logging logic.
    pub fn check(local: &Position, exchange_balance: Decimal) -> Self {
        let drift = local.quantity - exchange_balance;
        let one_hundred = Decimal::from(100);
        let drift_pct = if exchange_balance != Decimal::ZERO {
            (drift / exchange_balance * one_hundred).abs()
        } else if local.quantity != Decimal::ZERO {
            one_hundred // 100% drift if exchange has 0 but local has quantity
        } else {
            Decimal::ZERO // both zero, reconciled
        };

        Self {
            exchange: local.exchange,
            symbol: local.symbol.canonical(),
            local_quantity: local.quantity,
            exchange_quantity: exchange_balance,
            drift,
            drift_pct,
            is_reconciled: drift_pct < Decimal::ONE, // < 1% drift threshold
            timestamp: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{OrderType, Side, Symbol, TimeInForce};
    use rust_decimal_macros::dec;

    fn sample_new_order() -> NewOrder {
        NewOrder {
            exchange: Exchange::Binance,
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

    #[test]
    fn test_register_order() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        assert_eq!(order.status, OrderStatus::Pending);
        assert_eq!(order.exchange, Exchange::Binance);
        assert_eq!(order.quantity, dec!(0.1));
        assert!(oms.get_order(&order.client_order_id).is_some());
    }

    #[test]
    fn test_valid_state_transitions() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        assert!(oms.mark_submitted(&cid).is_some());

        // Submitted -> New (via update)
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: Some("BINANCE-123".into()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        let updated = oms.update_order(&update);
        assert!(updated.is_some());
        assert_eq!(updated.unwrap().status, OrderStatus::New);

        // Exchange index should be populated
        let by_exch = oms.get_by_exchange_id("BINANCE-123");
        assert!(by_exch.is_some());
        assert_eq!(by_exch.unwrap().client_order_id, cid);
    }

    #[test]
    fn test_invalid_state_transition_rejected() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        // Move to terminal: Pending -> Rejected
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Rejected,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        assert!(oms.update_order(&update).is_some());

        // Try Rejected -> New — should fail
        let bad_update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        assert!(oms.update_order(&bad_update).is_none());
    }

    #[test]
    fn test_open_orders_and_strategy_filter() {
        let oms = OrderManager::new();

        let mut no1 = sample_new_order();
        no1.strategy_id = "strat-a".into();
        let o1 = oms.register_order(&no1);

        let mut no2 = sample_new_order();
        no2.strategy_id = "strat-b".into();
        let o2 = oms.register_order(&no2);

        // Move both to Submitted then New
        oms.mark_submitted(&o1.client_order_id);
        oms.mark_submitted(&o2.client_order_id);

        for cid in [&o1.client_order_id, &o2.client_order_id] {
            let update = OrderUpdate {
                client_order_id: cid.clone(),
                exchange_order_id: Some(format!("EX-{}", cid)),
                exchange: Exchange::Binance,
                symbol: Symbol::new("BTC", "USDT"),
                status: OrderStatus::New,
                filled_quantity: Decimal::ZERO,
                remaining_quantity: dec!(0.1),
                average_fill_price: Decimal::ZERO,
                last_fill_price: None,
                last_fill_quantity: None,
                fee: None,
                fee_currency: None,
                timestamp: Utc::now(),
            };
            oms.update_order(&update);
        }

        assert_eq!(oms.get_open_orders().len(), 2);
        assert_eq!(oms.get_orders_by_strategy("strat-a").len(), 1);
        assert_eq!(oms.cancel_all_for_strategy("strat-a").len(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 1: register_order creates a Pending order with correct fields
    // -----------------------------------------------------------------------
    #[test]
    fn test_register_order_creates_pending() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());

        assert_eq!(order.status, OrderStatus::Pending);
        // client_order_id should be a valid non-empty UUID
        assert!(!order.client_order_id.is_empty());
        assert!(Uuid::parse_str(&order.client_order_id).is_ok());
        // Quantity should match the request
        assert_eq!(order.quantity, dec!(0.1));
        // Filled quantity should be zero
        assert_eq!(order.filled_quantity, Decimal::ZERO);
        // Remaining should equal the full quantity
        assert_eq!(order.remaining_quantity, dec!(0.1));
        // Exchange order id should not be set yet
        assert!(order.exchange_order_id.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 2: mark_submitted transitions state from Pending to Submitted
    // -----------------------------------------------------------------------
    #[test]
    fn test_mark_submitted_transitions_state() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        let submitted = oms
            .mark_submitted(&cid)
            .expect("mark_submitted should succeed");

        assert_eq!(submitted.status, OrderStatus::Submitted);
        // Other fields should remain unchanged
        assert_eq!(submitted.exchange, Exchange::Binance);
        assert_eq!(submitted.symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(submitted.side, Side::Buy);
        assert_eq!(submitted.order_type, OrderType::Limit);
        assert_eq!(submitted.quantity, dec!(0.1));
        assert_eq!(submitted.filled_quantity, Decimal::ZERO);
        assert_eq!(submitted.remaining_quantity, dec!(0.1));
        assert_eq!(submitted.strategy_id, "test-strategy");
    }

    // -----------------------------------------------------------------------
    // Test 3: update_order with Filled status applies fill data
    // -----------------------------------------------------------------------
    #[test]
    fn test_update_order_fills() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        oms.mark_submitted(&cid);

        // Submitted -> Filled
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: Some("BINANCE-FILL-001".into()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(0.1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(50100),
            last_fill_price: Some(dec!(50100)),
            last_fill_quantity: Some(dec!(0.1)),
            fee: Some(dec!(0.00001)),
            fee_currency: Some("BTC".into()),
            timestamp: Utc::now(),
        };

        let filled = oms.update_order(&update).expect("update should succeed");

        assert_eq!(filled.status, OrderStatus::Filled);
        assert_eq!(filled.filled_quantity, dec!(0.1));
        assert_eq!(filled.remaining_quantity, Decimal::ZERO);
        assert_eq!(filled.average_fill_price, dec!(50100));
    }

    // -----------------------------------------------------------------------
    // Test 4: partial fill updates quantities correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_partial_fill_update() {
        let oms = OrderManager::new();
        let mut new_order = sample_new_order();
        new_order.quantity = dec!(1.0);
        let order = oms.register_order(&new_order);
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        oms.mark_submitted(&cid);

        // Submitted -> PartiallyFilled (0.3 of 1.0)
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: Some("BINANCE-PF-001".into()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::PartiallyFilled,
            filled_quantity: dec!(0.3),
            remaining_quantity: dec!(0.7),
            average_fill_price: dec!(49950),
            last_fill_price: Some(dec!(49950)),
            last_fill_quantity: Some(dec!(0.3)),
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };

        let partial = oms
            .update_order(&update)
            .expect("partial fill should succeed");

        assert_eq!(partial.status, OrderStatus::PartiallyFilled);
        assert_eq!(partial.filled_quantity, dec!(0.3));
        assert_eq!(partial.remaining_quantity, dec!(0.7));
        assert_eq!(partial.average_fill_price, dec!(49950));
    }

    // -----------------------------------------------------------------------
    // Test 5: cancel order transitions state and removes from open orders
    // -----------------------------------------------------------------------
    #[test]
    fn test_cancel_order() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        oms.mark_submitted(&cid);

        // Submitted -> Cancelled
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Cancelled,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };

        let cancelled = oms.update_order(&update).expect("cancel should succeed");

        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        // Cancelled orders should NOT appear in open orders
        let open = oms.get_open_orders();
        assert!(
            !open.iter().any(|o| o.client_order_id == cid),
            "cancelled order should not be in open orders"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: get_open_orders filters correctly across multiple states
    // -----------------------------------------------------------------------
    #[test]
    fn test_get_open_orders_filters_correctly() {
        let oms = OrderManager::new();

        // Register 3 orders
        let o1 = oms.register_order(&sample_new_order());
        let o2 = oms.register_order(&sample_new_order());
        let o3 = oms.register_order(&sample_new_order());

        // Mark all as submitted (Submitted is an active state)
        oms.mark_submitted(&o1.client_order_id);
        oms.mark_submitted(&o2.client_order_id);
        oms.mark_submitted(&o3.client_order_id);

        // Fill order 1
        let fill_update = OrderUpdate {
            client_order_id: o1.client_order_id.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Filled,
            filled_quantity: dec!(0.1),
            remaining_quantity: Decimal::ZERO,
            average_fill_price: dec!(50000),
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        oms.update_order(&fill_update);

        // Cancel order 2
        let cancel_update = OrderUpdate {
            client_order_id: o2.client_order_id.clone(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::Cancelled,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        oms.update_order(&cancel_update);

        // Only order 3 should remain open (Submitted is active)
        let open = oms.get_open_orders();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].client_order_id, o3.client_order_id);
    }

    // -----------------------------------------------------------------------
    // Test 7: get_order by client_order_id (found and not found)
    // -----------------------------------------------------------------------
    #[test]
    fn test_get_order_by_client_id() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());

        // Should find the order by its client_order_id
        let found = oms.get_order(&order.client_order_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().client_order_id, order.client_order_id);

        // Should return None for a random ID
        let not_found = oms.get_order(&Uuid::new_v4().to_string());
        assert!(not_found.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 8: dual index consistency (client_order_id ↔ exchange_order_id)
    // -----------------------------------------------------------------------
    #[test]
    fn test_dual_index_consistency() {
        let oms = OrderManager::new();
        let order = oms.register_order(&sample_new_order());
        let cid = order.client_order_id.clone();

        // Pending -> Submitted
        oms.mark_submitted(&cid);

        // Submitted -> New, with exchange_order_id set
        let exchange_id = "BINANCE-DUAL-999";
        let update = OrderUpdate {
            client_order_id: cid.clone(),
            exchange_order_id: Some(exchange_id.into()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        oms.update_order(&update);

        // Look up by client_order_id
        let by_client = oms.get_order(&cid).expect("should find by client_order_id");

        // Look up by exchange_order_id
        let by_exchange = oms
            .get_by_exchange_id(exchange_id)
            .expect("should find by exchange_order_id");

        // Both lookups should return the same order
        assert_eq!(by_client.client_order_id, by_exchange.client_order_id);
        assert_eq!(by_client.status, by_exchange.status);
        assert_eq!(
            by_client.exchange_order_id, by_exchange.exchange_order_id,
            "exchange_order_id should match via both lookups"
        );
        assert_eq!(by_client.exchange_order_id, Some(exchange_id.to_string()));
    }

    // -----------------------------------------------------------------------
    // Test 9: mark_submitted on nonexistent order returns None
    // -----------------------------------------------------------------------
    #[test]
    fn test_mark_submitted_nonexistent_returns_none() {
        let oms = OrderManager::new();
        let result = oms.mark_submitted(&Uuid::new_v4().to_string());
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 10: update_order on nonexistent order returns None
    // -----------------------------------------------------------------------
    #[test]
    fn test_update_nonexistent_order_returns_none() {
        let oms = OrderManager::new();
        let update = OrderUpdate {
            client_order_id: Uuid::new_v4().to_string(),
            exchange_order_id: None,
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: dec!(0.1),
            average_fill_price: Decimal::ZERO,
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        };
        let result = oms.update_order(&update);
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Position Reconciliation Tests
    // -----------------------------------------------------------------------

    /// Helper to create a position with a given quantity.
    fn position_with_qty(qty: Decimal) -> Position {
        let mut pos = Position::new(Exchange::Binance, Symbol::new("BTC", "USDT"));
        pos.quantity = qty;
        pos
    }

    #[test]
    fn test_reconciliation_check_exact_match() {
        let pos = position_with_qty(dec!(1.5));
        let recon = PositionReconciliation::check(&pos, dec!(1.5));

        assert_eq!(recon.drift, Decimal::ZERO);
        assert_eq!(recon.drift_pct, Decimal::ZERO);
        assert!(recon.is_reconciled);
        assert_eq!(recon.local_quantity, dec!(1.5));
        assert_eq!(recon.exchange_quantity, dec!(1.5));
    }

    #[test]
    fn test_reconciliation_check_small_drift() {
        // Local has 100.0, exchange has 100.5 => drift = -0.5, pct ~ 0.497%
        let pos = position_with_qty(dec!(100.0));
        let recon = PositionReconciliation::check(&pos, dec!(100.5));

        assert_eq!(recon.drift, dec!(-0.5));
        assert!(recon.drift_pct < Decimal::ONE, "small drift should be < 1%");
        assert!(recon.is_reconciled, "small drift should be reconciled");
    }

    #[test]
    fn test_reconciliation_check_large_drift() {
        // Local has 10.0, exchange has 8.0 => drift = 2.0, pct = 25%
        let pos = position_with_qty(dec!(10.0));
        let recon = PositionReconciliation::check(&pos, dec!(8.0));

        assert_eq!(recon.drift, dec!(2.0));
        assert_eq!(recon.drift_pct, dec!(25));
        assert!(!recon.is_reconciled, "large drift should NOT be reconciled");
    }

    #[test]
    fn test_reconciliation_check_zero_exchange_balance() {
        // Local has quantity but exchange has 0 => 100% drift
        let pos = position_with_qty(dec!(5.0));
        let recon = PositionReconciliation::check(&pos, Decimal::ZERO);

        assert_eq!(recon.drift, dec!(5.0));
        assert_eq!(recon.drift_pct, Decimal::from(100));
        assert!(!recon.is_reconciled, "100% drift should NOT be reconciled");
    }

    #[test]
    fn test_reconciliation_check_both_zero() {
        // Both local and exchange at zero => fully reconciled
        let pos = position_with_qty(Decimal::ZERO);
        let recon = PositionReconciliation::check(&pos, Decimal::ZERO);

        assert_eq!(recon.drift, Decimal::ZERO);
        assert_eq!(recon.drift_pct, Decimal::ZERO);
        assert!(recon.is_reconciled, "both zero should be reconciled");
    }
}
