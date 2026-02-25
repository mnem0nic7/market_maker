use chrono::{DateTime, Utc};
use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderType, Side,
    Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the TWAP (Time-Weighted Average Price) execution strategy.
///
/// Executes a target quantity over a fixed time window by splitting it into
/// equal-sized slices and placing limit orders at regular intervals. Each slice
/// is priced at the current mid price +/- a maximum deviation in basis points.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwapConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Total quantity to execute over the duration.
    pub target_quantity: Decimal,
    /// Total execution window in seconds.
    pub duration_secs: u64,
    /// Interval between slices in seconds.
    pub slice_interval_secs: u64,
    /// Direction of execution (Buy or Sell).
    pub side: Side,
    /// Maximum price deviation from mid in basis points. Buy orders are placed
    /// at `mid * (1 + max_deviation_bps / 10_000)` and sell orders at
    /// `mid * (1 - max_deviation_bps / 10_000)` to be attractive but bounded.
    pub max_deviation_bps: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

/// TWAP execution strategy.
///
/// On each tick, if the current slice interval has elapsed and remaining
/// quantity is greater than zero, the strategy places a limit order at the
/// mid price adjusted by `max_deviation_bps`. It tracks fills and adjusts
/// the remaining quantity accordingly, deactivating once the full target has
/// been filled.
pub struct TwapStrategy {
    config: TwapConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// When the strategy started executing (set on first tick).
    start_time: Option<DateTime<Utc>>,
    /// Timestamp of the last slice order sent.
    last_slice_time: Option<DateTime<Utc>>,
    /// Cumulative quantity filled so far.
    filled_quantity: Decimal,
    /// Number of slice orders sent.
    slices_sent: u64,
    /// Total number of slices to send.
    total_slices: u64,
}

impl TwapStrategy {
    /// Create a new TWAP strategy from the given configuration.
    ///
    /// Computes `total_slices = duration_secs / slice_interval_secs` and
    /// `slice_size = target_quantity / total_slices`.
    ///
    /// # Panics
    ///
    /// Panics if `slice_interval_secs` is zero or if `duration_secs` is not
    /// evenly divisible by `slice_interval_secs` in a way that would produce
    /// zero slices.
    pub fn new(config: TwapConfig) -> Self {
        assert!(
            config.slice_interval_secs > 0,
            "slice_interval_secs must be > 0"
        );

        let total_slices = config.duration_secs / config.slice_interval_secs;
        assert!(total_slices > 0, "total_slices must be > 0");

        let slice_size = config.target_quantity / Decimal::from(total_slices);

        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            side = %config.side,
            target_quantity = %config.target_quantity,
            duration_secs = config.duration_secs,
            slice_interval_secs = config.slice_interval_secs,
            total_slices = total_slices,
            slice_size = %slice_size,
            max_deviation_bps = %config.max_deviation_bps,
            "initializing TWAP execution strategy"
        );

        Self {
            config,
            active: true,
            start_time: None,
            last_slice_time: None,
            filled_quantity: Decimal::ZERO,
            slices_sent: 0,
            total_slices,
        }
    }

    /// Build the cancel-all action for this strategy's orders.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.exchange),
            symbol: Some(self.config.symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Compute the limit price for the current slice given the mid price.
    ///
    /// For buy orders the price is `mid * (1 + deviation)` (willing to pay
    /// slightly above mid). For sell orders the price is `mid * (1 - deviation)`
    /// (willing to sell slightly below mid).
    fn compute_limit_price(&self, mid: Decimal) -> Decimal {
        let deviation = self.config.max_deviation_bps / dec!(10_000);
        match self.config.side {
            Side::Buy => mid * (Decimal::ONE + deviation),
            Side::Sell => mid * (Decimal::ONE - deviation),
        }
    }

    /// Remaining quantity that has not yet been filled.
    fn remaining_quantity(&self) -> Decimal {
        self.config.target_quantity - self.filled_quantity
    }

    /// Determine whether it is time to send the next slice.
    fn should_send_slice(&self, now: DateTime<Utc>) -> bool {
        // Already sent all slices
        if self.slices_sent >= self.total_slices {
            return false;
        }

        match self.last_slice_time {
            // First slice is sent immediately
            None => true,
            Some(last) => {
                let elapsed_secs = (now - last).num_seconds();
                elapsed_secs >= self.config.slice_interval_secs as i64
            }
        }
    }
}

impl Strategy for TwapStrategy {
    fn name(&self) -> &str {
        "twap"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        let remaining = self.remaining_quantity();
        if remaining <= Decimal::ZERO {
            debug!(
                strategy_id = %self.config.strategy_id,
                "target quantity fully filled, deactivating"
            );
            self.active = false;
            // All slices use TimeInForce::IOC so there are never outstanding
            // resting orders to cancel. Simply deactivate.
            return vec![];
        }

        let now = snapshot.timestamp;

        // Record start time on first tick
        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        // Duration expiry: if the full execution window has elapsed and all
        // slices have been sent, deactivate gracefully (accept under-execution).
        // Because every slice uses IOC there are no resting orders to cancel.
        if let Some(start) = self.start_time {
            let elapsed = (now - start).num_seconds() as u64;
            if elapsed >= self.config.duration_secs && self.slices_sent >= self.total_slices {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    elapsed_secs = elapsed,
                    slices_sent = self.slices_sent,
                    filled_quantity = %self.filled_quantity,
                    target_quantity = %self.config.target_quantity,
                    "duration expired with all slices sent, deactivating"
                );
                self.active = false;
                return vec![];
            }
        }

        if !self.should_send_slice(now) {
            return Vec::new();
        }

        // Get mid price
        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping slice"
                );
                return Vec::new();
            }
        };

        // Determine order quantity by dividing the remaining (unfilled) quantity
        // evenly across the remaining slices. This naturally handles carry-forward:
        // if earlier IOC slices only partially fill, later slices get proportionally
        // larger to compensate.
        let remaining_slices = self.total_slices - self.slices_sent;
        let order_qty = remaining / Decimal::from(remaining_slices.max(1));

        let limit_price = self.compute_limit_price(mid);

        debug!(
            strategy_id = %self.config.strategy_id,
            slice = self.slices_sent + 1,
            total_slices = self.total_slices,
            order_qty = %order_qty,
            limit_price = %limit_price,
            mid_price = %mid,
            remaining = %remaining,
            "sending TWAP slice"
        );

        self.slices_sent += 1;
        self.last_slice_time = Some(now);

        // IOC (Immediate-Or-Cancel) ensures each slice either fills instantly
        // against resting liquidity or is cancelled — no stale resting orders
        // accumulate. The trade-off is that unfilled portions are "lost" for
        // that slice; we compensate by rebalancing the remaining target across
        // subsequent slices (see order_qty calculation above).
        vec![OrderAction::Place(NewOrder {
            exchange: self.config.exchange,
            symbol: self.config.symbol.clone(),
            side: self.config.side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::IOC,
            price: Some(limit_price),
            quantity: order_qty,
            strategy_id: self.config.strategy_id.clone(),
            reduce_only: false,
        })]
    }

    fn on_fill(&mut self, fill: &Fill) {
        self.filled_quantity += fill.quantity;

        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            filled_so_far = %self.filled_quantity,
            target = %self.config.target_quantity,
            remaining = %(self.config.target_quantity - self.filled_quantity),
            "TWAP strategy received fill"
        );

        if self.filled_quantity >= self.config.target_quantity {
            info!(
                strategy_id = %self.config.strategy_id,
                filled_quantity = %self.filled_quantity,
                target_quantity = %self.config.target_quantity,
                "TWAP target quantity reached, deactivating"
            );
            self.active = false;
        }
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "TWAP strategy received order update"
        );
        // No-op: TWAP tracks progress via on_fill
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            filled_quantity = %self.filled_quantity,
            target_quantity = %self.config.target_quantity,
            slices_sent = self.slices_sent,
            "stopping TWAP execution strategy"
        );
        self.active = false;
        vec![self.cancel_all_action()]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use mm_core::types::{OrderBook, Position, PriceLevel};

    /// Helper: build a default TwapConfig for testing.
    fn test_config() -> TwapConfig {
        TwapConfig {
            strategy_id: "twap-test-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            target_quantity: dec!(100),
            duration_secs: 100,
            slice_interval_secs: 10,
            side: Side::Buy,
            max_deviation_bps: dec!(10), // 10 bps
        }
    }

    /// Helper: build a MarketSnapshot with a given mid price and timestamp.
    fn make_snapshot(bid: Decimal, ask: Decimal, ts: DateTime<Utc>) -> MarketSnapshot {
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: OrderBook {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: bid,
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: ask,
                    quantity: dec!(10),
                }],
                timestamp: ts,
                sequence: 1,
            },
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Helper: build a Fill.
    fn make_fill(quantity: Decimal, price: Decimal) -> Fill {
        Fill {
            fill_id: "fill-1".to_string(),
            client_order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            timestamp: Utc::now(),
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: Constructor computes correct slice_size and total_slices
    // -----------------------------------------------------------------------
    #[test]
    fn test_new_computes_correct_slices() {
        let config = test_config();
        let strategy = TwapStrategy::new(config);

        // 100 secs / 10 secs = 10 slices
        assert_eq!(strategy.total_slices, 10);
        assert!(strategy.active);
        assert_eq!(strategy.filled_quantity, Decimal::ZERO);
        assert_eq!(strategy.slices_sent, 0);
        assert!(strategy.start_time.is_none());
        assert!(strategy.last_slice_time.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 2: on_tick places order when interval has elapsed
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_places_order_when_interval_elapsed() {
        let config = test_config();
        let mut strategy = TwapStrategy::new(config);

        let now = Utc::now();
        let snapshot = make_snapshot(dec!(50000), dec!(50010), now);

        // First tick should always place an order (no previous slice)
        let actions = strategy.on_tick(&snapshot);
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.quantity, dec!(10)); // slice_size
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.time_in_force, TimeInForce::IOC);
                assert_eq!(order.strategy_id, "twap-test-1");

                // Mid = (50000 + 50010) / 2 = 50005
                // Buy limit price = 50005 * (1 + 10/10000) = 50005 * 1.001 = 50055.005
                let expected_mid = dec!(50005);
                let expected_price = expected_mid * (Decimal::ONE + dec!(10) / dec!(10_000));
                assert_eq!(order.price, Some(expected_price));
            }
            _ => panic!("expected Place action"),
        }

        assert_eq!(strategy.slices_sent, 1);
        assert!(strategy.start_time.is_some());
        assert!(strategy.last_slice_time.is_some());

        // Advance time past slice interval and tick again
        let later = now + Duration::seconds(11);
        let snapshot2 = make_snapshot(dec!(50000), dec!(50010), later);
        let actions2 = strategy.on_tick(&snapshot2);
        assert_eq!(actions2.len(), 1);
        assert_eq!(strategy.slices_sent, 2);
    }

    // -----------------------------------------------------------------------
    // Test 3: on_tick does nothing when interval has not elapsed
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_does_nothing_when_interval_not_elapsed() {
        let config = test_config();
        let mut strategy = TwapStrategy::new(config);

        let now = Utc::now();
        let snapshot = make_snapshot(dec!(50000), dec!(50010), now);

        // First tick places order
        let actions = strategy.on_tick(&snapshot);
        assert_eq!(actions.len(), 1);
        assert_eq!(strategy.slices_sent, 1);

        // Tick again with only 5 seconds elapsed (interval is 10)
        let soon = now + Duration::seconds(5);
        let snapshot2 = make_snapshot(dec!(50000), dec!(50010), soon);
        let actions2 = strategy.on_tick(&snapshot2);
        assert!(actions2.is_empty());
        assert_eq!(strategy.slices_sent, 1); // unchanged
    }

    // -----------------------------------------------------------------------
    // Test 4: on_fill tracks filled_quantity and deactivates when complete
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_fill_tracks_quantity_and_deactivates() {
        let config = test_config(); // target = 100
        let mut strategy = TwapStrategy::new(config);

        // Partial fill
        let fill1 = make_fill(dec!(40), dec!(50000));
        strategy.on_fill(&fill1);
        assert_eq!(strategy.filled_quantity, dec!(40));
        assert!(strategy.is_active());

        // Another partial fill
        let fill2 = make_fill(dec!(50), dec!(50010));
        strategy.on_fill(&fill2);
        assert_eq!(strategy.filled_quantity, dec!(90));
        assert!(strategy.is_active());

        // Final fill reaches target
        let fill3 = make_fill(dec!(10), dec!(49990));
        strategy.on_fill(&fill3);
        assert_eq!(strategy.filled_quantity, dec!(100));
        assert!(!strategy.is_active()); // deactivated
    }

    // -----------------------------------------------------------------------
    // Test 5: stop() cancels all and deactivates
    // -----------------------------------------------------------------------
    #[test]
    fn test_stop_cancels_all_and_deactivates() {
        let config = test_config();
        let mut strategy = TwapStrategy::new(config);
        assert!(strategy.is_active());

        let actions = strategy.stop();
        assert!(!strategy.is_active());
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("twap-test-1".to_string()));
            }
            _ => panic!("expected CancelAll action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: on_tick returns empty when strategy is inactive
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_returns_empty_when_inactive() {
        let config = test_config();
        let mut strategy = TwapStrategy::new(config);
        strategy.stop();

        let snapshot = make_snapshot(dec!(50000), dec!(50010), Utc::now());
        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 7: on_tick returns empty when no mid price (empty order book)
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_no_mid_price() {
        let config = test_config();
        let mut strategy = TwapStrategy::new(config);

        let now = Utc::now();
        let mut snapshot = make_snapshot(dec!(50000), dec!(50010), now);
        // Clear the order book
        snapshot.order_book.bids.clear();
        snapshot.order_book.asks.clear();

        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
        assert_eq!(strategy.slices_sent, 0);
    }

    // -----------------------------------------------------------------------
    // Test 8: Sell side computes price correctly (mid - deviation)
    // -----------------------------------------------------------------------
    #[test]
    fn test_sell_side_price_deviation() {
        let mut config = test_config();
        config.side = Side::Sell;
        let mut strategy = TwapStrategy::new(config);

        let now = Utc::now();
        let snapshot = make_snapshot(dec!(50000), dec!(50010), now);

        let actions = strategy.on_tick(&snapshot);
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Sell);
                // Mid = 50005
                // Sell limit price = 50005 * (1 - 10/10000) = 50005 * 0.999 = 49954.995
                let expected_mid = dec!(50005);
                let expected_price = expected_mid * (Decimal::ONE - dec!(10) / dec!(10_000));
                assert_eq!(order.price, Some(expected_price));
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: Last slice sends remaining quantity, not slice_size
    // -----------------------------------------------------------------------
    #[test]
    fn test_last_slice_sends_remaining_quantity() {
        let mut config = test_config(); // target=100, 10 slices of 10
        config.target_quantity = dec!(100);
        config.duration_secs = 30;
        config.slice_interval_secs = 10;
        // 3 slices of ~33.33 each
        let mut strategy = TwapStrategy::new(config);
        assert_eq!(strategy.total_slices, 3);

        let now = Utc::now();

        // Fill some via on_fill after first two slices
        // Send slice 1
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);

        // Simulate a partial fill from slice 1
        let fill1 = make_fill(dec!(30), dec!(50005));
        strategy.on_fill(&fill1);

        // Send slice 2
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(11));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);

        // Simulate fill from slice 2
        let fill2 = make_fill(dec!(30), dec!(50005));
        strategy.on_fill(&fill2);
        // filled = 60, remaining = 40

        // Send slice 3 (last) — should send remaining (40), not slice_size (33.33)
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(22));
        let actions3 = strategy.on_tick(&snap3);
        assert_eq!(actions3.len(), 1);

        match &actions3[0] {
            OrderAction::Place(order) => {
                // remaining = 100 - 60 = 40
                assert_eq!(order.quantity, dec!(40));
            }
            _ => panic!("expected Place action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 10: name() and strategy_id() return correct values
    // -----------------------------------------------------------------------
    #[test]
    fn test_name_and_strategy_id() {
        let config = test_config();
        let strategy = TwapStrategy::new(config);
        assert_eq!(strategy.name(), "twap");
        assert_eq!(strategy.strategy_id(), "twap-test-1");
    }

    // -----------------------------------------------------------------------
    // Test 11: on_tick deactivates and cancels when remaining is zero
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_deactivates_when_fully_filled() {
        let config = test_config(); // target=100
        let mut strategy = TwapStrategy::new(config);

        // Fill the entire target
        let fill = make_fill(dec!(100), dec!(50000));
        strategy.on_fill(&fill);
        assert!(!strategy.is_active());

        // Next tick should return empty (already inactive)
        let snapshot = make_snapshot(dec!(50000), dec!(50010), Utc::now());
        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 12: No more slices sent after total_slices reached
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_slices_after_total_reached() {
        let mut config = test_config();
        config.target_quantity = dec!(20);
        config.duration_secs = 20;
        config.slice_interval_secs = 10;
        // 2 slices of 10
        let mut strategy = TwapStrategy::new(config);
        assert_eq!(strategy.total_slices, 2);

        let now = Utc::now();

        // Slice 1
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);

        // Slice 2
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(11));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);

        assert_eq!(strategy.slices_sent, 2);

        // Slice 3 — should be empty, all slices sent
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(22));
        let actions3 = strategy.on_tick(&snap3);
        assert!(actions3.is_empty());
        assert_eq!(strategy.slices_sent, 2); // unchanged
    }

    // -----------------------------------------------------------------------
    // Test 13: Duration expiry deactivates the strategy
    // -----------------------------------------------------------------------
    #[test]
    fn test_duration_expiry_deactivates() {
        let mut config = test_config();
        config.target_quantity = dec!(20);
        config.duration_secs = 20;
        config.slice_interval_secs = 10;
        // 2 slices of 10
        let mut strategy = TwapStrategy::new(config);
        assert_eq!(strategy.total_slices, 2);

        let now = Utc::now();

        // Slice 1 — no fill (IOC missed entirely)
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);

        // Slice 2 — still no fill
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(11));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        assert_eq!(strategy.slices_sent, 2);
        // Strategy is still "active" until we observe the duration expiry on the next tick
        assert!(strategy.is_active());

        // Tick after duration has elapsed (>= 20s) with all slices sent
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(25));
        let actions3 = strategy.on_tick(&snap3);
        assert!(actions3.is_empty());
        // Strategy should now be deactivated (accepts under-execution)
        assert!(!strategy.is_active());
        // Filled nothing — that's OK, duration expiry is graceful
        assert_eq!(strategy.filled_quantity, Decimal::ZERO);
    }

    // -----------------------------------------------------------------------
    // Test 14: Rebalancing — underfilled early slices cause later slices
    //          to compensate
    // -----------------------------------------------------------------------
    #[test]
    fn test_rebalancing_compensates_for_underfilled_slices() {
        let mut config = test_config();
        config.target_quantity = dec!(100);
        config.duration_secs = 40;
        config.slice_interval_secs = 10;
        // 4 slices; naive slice_size = 25
        let mut strategy = TwapStrategy::new(config);
        assert_eq!(strategy.total_slices, 4);

        let now = Utc::now();

        // Slice 1: remaining=100, remaining_slices=4, order_qty = 100/4 = 25
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);
        match &actions1[0] {
            OrderAction::Place(order) => assert_eq!(order.quantity, dec!(25)),
            _ => panic!("expected Place action"),
        }

        // Only 10 of 25 fills (partial IOC execution)
        let fill1 = make_fill(dec!(10), dec!(50005));
        strategy.on_fill(&fill1);
        // remaining = 90

        // Slice 2: remaining=90, remaining_slices=3, order_qty = 90/3 = 30
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(11));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        match &actions2[0] {
            OrderAction::Place(order) => assert_eq!(order.quantity, dec!(30)),
            _ => panic!("expected Place action"),
        }

        // Full fill this time
        let fill2 = make_fill(dec!(30), dec!(50005));
        strategy.on_fill(&fill2);
        // remaining = 60

        // Slice 3: remaining=60, remaining_slices=2, order_qty = 60/2 = 30
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(22));
        let actions3 = strategy.on_tick(&snap3);
        assert_eq!(actions3.len(), 1);
        match &actions3[0] {
            OrderAction::Place(order) => assert_eq!(order.quantity, dec!(30)),
            _ => panic!("expected Place action"),
        }

        // Full fill
        let fill3 = make_fill(dec!(30), dec!(50005));
        strategy.on_fill(&fill3);
        // remaining = 30

        // Slice 4 (last): remaining=30, remaining_slices=1, order_qty = 30/1 = 30
        let snap4 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(33));
        let actions4 = strategy.on_tick(&snap4);
        assert_eq!(actions4.len(), 1);
        match &actions4[0] {
            OrderAction::Place(order) => assert_eq!(order.quantity, dec!(30)),
            _ => panic!("expected Place action"),
        }

        // Final fill completes execution
        let fill4 = make_fill(dec!(30), dec!(50005));
        strategy.on_fill(&fill4);
        assert_eq!(strategy.filled_quantity, dec!(100));
        assert!(!strategy.is_active());
    }
}
