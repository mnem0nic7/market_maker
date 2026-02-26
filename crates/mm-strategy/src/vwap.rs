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

/// Configuration for the VWAP (Volume-Weighted Average Price) execution strategy.
///
/// Executes a target quantity over a fixed time window by splitting it into
/// volume-weighted slices based on a historical volume profile. Heavier slices
/// are placed during historically high-volume periods. Each slice is priced at
/// the current mid price +/- a maximum deviation in basis points.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VwapConfig {
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
    /// Direction of execution (Buy or Sell).
    pub side: Side,
    /// Bucket weights representing the historical volume profile. Each element
    /// is a relative weight; the constructor normalizes them so they sum to 1.
    /// The number of buckets determines the number of slices.
    pub volume_profile: Vec<Decimal>,
    /// Maximum price deviation from mid in basis points. Buy orders are placed
    /// at `mid * (1 + max_deviation_bps / 10_000)` and sell orders at
    /// `mid * (1 - max_deviation_bps / 10_000)` to be attractive but bounded.
    pub max_deviation_bps: Decimal,
}

impl VwapConfig {
    /// Validate logical constraints on the configuration parameters.
    pub fn validate(&self) -> Result<(), String> {
        if self.target_quantity <= Decimal::ZERO {
            return Err("target_quantity must be positive".to_string());
        }
        if self.duration_secs == 0 {
            return Err("duration_secs must be positive".to_string());
        }
        if self.volume_profile.is_empty() {
            return Err("volume_profile must not be empty".to_string());
        }
        if self.max_deviation_bps < Decimal::ZERO {
            return Err("max_deviation_bps must be non-negative".to_string());
        }
        // Ensure all volume profile weights are non-negative
        for (i, weight) in self.volume_profile.iter().enumerate() {
            if *weight < Decimal::ZERO {
                return Err(format!("volume_profile[{}] must be non-negative", i));
            }
        }
        // Ensure weights sum to > 0
        let weight_sum: Decimal = self.volume_profile.iter().copied().sum();
        if weight_sum <= Decimal::ZERO {
            return Err("volume_profile weights must sum to > 0".to_string());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

/// VWAP execution strategy.
///
/// On each tick, if the current bucket interval has elapsed and remaining
/// quantity is greater than zero, the strategy places a limit order sized
/// proportionally to the current bucket's volume weight. It tracks fills and
/// adjusts the remaining quantity accordingly, deactivating once the full
/// target has been filled.
pub struct VwapStrategy {
    config: VwapConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// When the strategy started executing (set on first tick).
    start_time: Option<DateTime<Utc>>,
    /// Cumulative quantity filled so far.
    filled_quantity: Decimal,
    /// Index of the current bucket in the volume profile.
    current_bucket: usize,
    /// Pre-computed ideal bucket sizes: `bucket_sizes[i] = weight[i] * target_quantity`.
    bucket_sizes: Vec<Decimal>,
    /// Timestamp of the last bucket order sent.
    last_bucket_time: Option<DateTime<Utc>>,
    /// Interval between buckets in seconds.
    bucket_interval_secs: u64,
    /// Number of bucket orders sent.
    buckets_sent: u64,
}

impl VwapStrategy {
    /// Create a new VWAP strategy from the given configuration.
    ///
    /// Computes `bucket_interval_secs = duration_secs / volume_profile.len()`
    /// and pre-computes each bucket's ideal size by normalizing the volume
    /// profile weights and multiplying by the target quantity.
    ///
    /// # Panics
    ///
    /// Panics if `volume_profile` is empty or if `duration_secs` is zero.
    pub fn new(config: VwapConfig) -> Self {
        assert!(
            !config.volume_profile.is_empty(),
            "volume_profile must not be empty"
        );
        assert!(config.duration_secs > 0, "duration_secs must be > 0");

        let num_buckets = config.volume_profile.len();
        let bucket_interval_secs = config.duration_secs / num_buckets as u64;
        assert!(
            bucket_interval_secs > 0,
            "bucket_interval_secs must be > 0 (duration too short for number of buckets)"
        );

        // Normalize weights so they sum to 1
        let weight_sum: Decimal = config.volume_profile.iter().copied().sum();
        assert!(
            weight_sum > Decimal::ZERO,
            "volume_profile weights must sum to > 0"
        );

        let normalized_weights: Vec<Decimal> = config
            .volume_profile
            .iter()
            .map(|w| *w / weight_sum)
            .collect();

        // Pre-compute bucket sizes
        let bucket_sizes: Vec<Decimal> = normalized_weights
            .iter()
            .map(|w| *w * config.target_quantity)
            .collect();

        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            side = %config.side,
            target_quantity = %config.target_quantity,
            duration_secs = config.duration_secs,
            num_buckets = num_buckets,
            bucket_interval_secs = bucket_interval_secs,
            max_deviation_bps = %config.max_deviation_bps,
            "initializing VWAP execution strategy"
        );

        Self {
            config,
            active: true,
            start_time: None,
            filled_quantity: Decimal::ZERO,
            current_bucket: 0,
            bucket_sizes,
            last_bucket_time: None,
            bucket_interval_secs,
            buckets_sent: 0,
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

    /// Compute the limit price for the current bucket given the mid price.
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

    /// Total number of buckets in the volume profile.
    fn total_buckets(&self) -> u64 {
        self.bucket_sizes.len() as u64
    }

    /// Determine whether it is time to send the next bucket order.
    fn should_send_bucket(&self, now: DateTime<Utc>) -> bool {
        // Already sent all buckets
        if self.buckets_sent >= self.total_buckets() {
            return false;
        }

        match self.last_bucket_time {
            // First bucket is sent immediately
            None => true,
            Some(last) => {
                let elapsed_secs = (now - last).num_seconds();
                elapsed_secs >= self.bucket_interval_secs as i64
            }
        }
    }

    /// Compute the volume-weighted order quantity for the current bucket.
    ///
    /// Uses the remaining quantity and remaining bucket weights to compute a
    /// proportional size: `remaining * (current_weight / remaining_weight_sum)`.
    /// This naturally handles carry-forward: if earlier IOC buckets only
    /// partially fill, later buckets get proportionally larger to compensate
    /// while preserving the volume profile shape.
    fn compute_bucket_order_qty(&self) -> Decimal {
        let remaining = self.remaining_quantity();
        let current_idx = self.buckets_sent as usize;
        let num_buckets = self.bucket_sizes.len();

        if current_idx >= num_buckets {
            return Decimal::ZERO;
        }

        // Sum the remaining bucket weights (from current bucket onward)
        let remaining_weight_sum: Decimal = self.bucket_sizes[current_idx..].iter().copied().sum();

        if remaining_weight_sum <= Decimal::ZERO {
            // Fallback: divide remaining evenly across remaining buckets
            let remaining_buckets = self.total_buckets() - self.buckets_sent;
            return remaining / Decimal::from(remaining_buckets.max(1));
        }

        let current_weight = self.bucket_sizes[current_idx];
        remaining * current_weight / remaining_weight_sum
    }
}

impl Strategy for VwapStrategy {
    fn name(&self) -> &str {
        "vwap"
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
            // All buckets use TimeInForce::IOC so there are never outstanding
            // resting orders to cancel. Simply deactivate.
            return vec![];
        }

        let now = snapshot.timestamp;

        // Record start time on first tick
        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        // Duration expiry: if the full execution window has elapsed and all
        // buckets have been sent, deactivate gracefully (accept under-execution).
        // Because every bucket uses IOC there are no resting orders to cancel.
        if let Some(start) = self.start_time {
            let elapsed = (now - start).num_seconds() as u64;
            if elapsed >= self.config.duration_secs && self.buckets_sent >= self.total_buckets() {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    elapsed_secs = elapsed,
                    buckets_sent = self.buckets_sent,
                    filled_quantity = %self.filled_quantity,
                    target_quantity = %self.config.target_quantity,
                    "duration expired with all buckets sent, deactivating"
                );
                self.active = false;
                return vec![];
            }
        }

        if !self.should_send_bucket(now) {
            return Vec::new();
        }

        // Get mid price
        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                debug!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping bucket"
                );
                return Vec::new();
            }
        };

        // Determine order quantity using volume-weighted proportional sizing
        let order_qty = self.compute_bucket_order_qty();

        let limit_price = self.compute_limit_price(mid);
        let bucket_idx = self.buckets_sent as usize;

        debug!(
            strategy_id = %self.config.strategy_id,
            bucket = bucket_idx + 1,
            total_buckets = self.total_buckets(),
            order_qty = %order_qty,
            limit_price = %limit_price,
            mid_price = %mid,
            remaining = %remaining,
            "sending VWAP bucket order"
        );

        self.buckets_sent += 1;
        self.current_bucket = bucket_idx + 1;
        self.last_bucket_time = Some(now);

        // IOC (Immediate-Or-Cancel) ensures each bucket either fills instantly
        // against resting liquidity or is cancelled — no stale resting orders
        // accumulate. The trade-off is that unfilled portions are "lost" for
        // that bucket; we compensate by rebalancing the remaining target across
        // subsequent buckets (see compute_bucket_order_qty).
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
            "VWAP strategy received fill"
        );

        if self.filled_quantity >= self.config.target_quantity {
            info!(
                strategy_id = %self.config.strategy_id,
                filled_quantity = %self.filled_quantity,
                target_quantity = %self.config.target_quantity,
                "VWAP target quantity reached, deactivating"
            );
            self.active = false;
        }
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "VWAP strategy received order update"
        );
        // No-op: VWAP tracks progress via on_fill
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            filled_quantity = %self.filled_quantity,
            target_quantity = %self.config.target_quantity,
            buckets_sent = self.buckets_sent,
            "stopping VWAP execution strategy"
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

    /// Helper: build a default VwapConfig for testing.
    fn test_config() -> VwapConfig {
        VwapConfig {
            strategy_id: "vwap-test-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            target_quantity: dec!(100),
            duration_secs: 100,
            side: Side::Buy,
            // 5 buckets with varying weights (already sums to 1)
            volume_profile: vec![dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.25), dec!(0.15)],
            max_deviation_bps: dec!(10), // 10 bps
        }
    }

    /// Helper: build a MarketSnapshot with a given mid price and timestamp.
    fn make_snapshot(bid: Decimal, ask: Decimal, ts: DateTime<Utc>) -> MarketSnapshot {
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: std::sync::Arc::new(OrderBook {
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
            }),
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
    // Test 1: Constructor correctly computes bucket_sizes from volume profile
    // -----------------------------------------------------------------------
    #[test]
    fn test_new_computes_correct_bucket_sizes() {
        let config = test_config();
        let strategy = VwapStrategy::new(config);

        // Weights [0.1, 0.2, 0.3, 0.25, 0.15] already sum to 1.0
        // bucket_sizes = weight * target_quantity (100)
        assert_eq!(strategy.bucket_sizes.len(), 5);
        assert_eq!(strategy.bucket_sizes[0], dec!(10)); // 0.1 * 100
        assert_eq!(strategy.bucket_sizes[1], dec!(20)); // 0.2 * 100
        assert_eq!(strategy.bucket_sizes[2], dec!(30)); // 0.3 * 100
        assert_eq!(strategy.bucket_sizes[3], dec!(25)); // 0.25 * 100
        assert_eq!(strategy.bucket_sizes[4], dec!(15)); // 0.15 * 100

        // bucket_interval_secs = 100 / 5 = 20
        assert_eq!(strategy.bucket_interval_secs, 20);
        assert!(strategy.active);
        assert_eq!(strategy.filled_quantity, Decimal::ZERO);
        assert_eq!(strategy.buckets_sent, 0);
        assert_eq!(strategy.current_bucket, 0);
        assert!(strategy.start_time.is_none());
        assert!(strategy.last_bucket_time.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 2: Constructor normalizes weights that do not sum to 1
    // -----------------------------------------------------------------------
    #[test]
    fn test_new_normalizes_weights() {
        let mut config = test_config();
        // Weights sum to 10, not 1 — constructor should normalize
        config.volume_profile = vec![dec!(1), dec!(2), dec!(3), dec!(2.5), dec!(1.5)];
        let strategy = VwapStrategy::new(config);

        // After normalization: [0.1, 0.2, 0.3, 0.25, 0.15] * 100
        assert_eq!(strategy.bucket_sizes[0], dec!(10));
        assert_eq!(strategy.bucket_sizes[1], dec!(20));
        assert_eq!(strategy.bucket_sizes[2], dec!(30));
        assert_eq!(strategy.bucket_sizes[3], dec!(25));
        assert_eq!(strategy.bucket_sizes[4], dec!(15));
    }

    // -----------------------------------------------------------------------
    // Test 3: Heavier bucket gets proportionally larger order
    // -----------------------------------------------------------------------
    #[test]
    fn test_heavier_bucket_gets_larger_order() {
        let mut config = test_config();
        config.target_quantity = dec!(100);
        config.duration_secs = 60;
        // 3 buckets: light, heavy, medium weights
        config.volume_profile = vec![dec!(0.1), dec!(0.6), dec!(0.3)];
        let mut strategy = VwapStrategy::new(config);

        // bucket_interval_secs = 60 / 3 = 20
        assert_eq!(strategy.bucket_interval_secs, 20);

        let now = Utc::now();

        // Bucket 1 (weight=0.1): remaining=100, remaining_weight_sum=100 (0.1+0.6+0.3)*100
        // order_qty = 100 * 10 / 100 = 10
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);
        let qty1 = match &actions1[0] {
            OrderAction::Place(order) => order.quantity,
            _ => panic!("expected Place action"),
        };
        assert_eq!(qty1, dec!(10));

        // Simulate full fill of bucket 1
        let fill1 = make_fill(dec!(10), dec!(50005));
        strategy.on_fill(&fill1);
        // remaining = 90

        // Bucket 2 (weight=0.6): remaining=90, remaining_weight_sum=90 (0.6+0.3)*100
        // order_qty = 90 * 60 / 90 = 60
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(21));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        let qty2 = match &actions2[0] {
            OrderAction::Place(order) => order.quantity,
            _ => panic!("expected Place action"),
        };
        assert_eq!(qty2, dec!(60));

        // Bucket 2 order (60) is 6x larger than bucket 1 (10), matching 0.6 vs 0.1 ratio
        assert_eq!(qty2 / qty1, dec!(6));
    }

    // -----------------------------------------------------------------------
    // Test 4: on_tick places order when bucket interval elapsed
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_places_order_when_interval_elapsed() {
        let config = test_config();
        let mut strategy = VwapStrategy::new(config);

        let now = Utc::now();
        let snapshot = make_snapshot(dec!(50000), dec!(50010), now);

        // First tick should always place an order (no previous bucket)
        let actions = strategy.on_tick(&snapshot);
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.order_type, OrderType::Limit);
                assert_eq!(order.time_in_force, TimeInForce::IOC);
                assert_eq!(order.strategy_id, "vwap-test-1");

                // Mid = (50000 + 50010) / 2 = 50005
                // Buy limit price = 50005 * (1 + 10/10000) = 50005 * 1.001 = 50055.005
                let expected_mid = dec!(50005);
                let expected_price = expected_mid * (Decimal::ONE + dec!(10) / dec!(10_000));
                assert_eq!(order.price, Some(expected_price));
            }
            _ => panic!("expected Place action"),
        }

        assert_eq!(strategy.buckets_sent, 1);
        assert!(strategy.start_time.is_some());
        assert!(strategy.last_bucket_time.is_some());

        // Tick within interval — should NOT place order
        let soon = now + Duration::seconds(10);
        let snap_soon = make_snapshot(dec!(50000), dec!(50010), soon);
        let actions_soon = strategy.on_tick(&snap_soon);
        assert!(actions_soon.is_empty());
        assert_eq!(strategy.buckets_sent, 1);

        // Advance time past bucket interval (20s) and tick again
        let later = now + Duration::seconds(21);
        let snapshot2 = make_snapshot(dec!(50000), dec!(50010), later);
        let actions2 = strategy.on_tick(&snapshot2);
        assert_eq!(actions2.len(), 1);
        assert_eq!(strategy.buckets_sent, 2);
    }

    // -----------------------------------------------------------------------
    // Test 5: on_fill tracks quantity, deactivates when complete
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_fill_tracks_quantity_and_deactivates() {
        let config = test_config(); // target = 100
        let mut strategy = VwapStrategy::new(config);

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
    // Test 6: Duration expiry deactivates strategy
    // -----------------------------------------------------------------------
    #[test]
    fn test_duration_expiry_deactivates() {
        let mut config = test_config();
        config.target_quantity = dec!(100);
        config.duration_secs = 40;
        config.volume_profile = vec![dec!(0.5), dec!(0.5)];
        // 2 buckets, bucket_interval_secs = 20
        let mut strategy = VwapStrategy::new(config);
        assert_eq!(strategy.total_buckets(), 2);

        let now = Utc::now();

        // Bucket 1 — no fill (IOC missed entirely)
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);

        // Bucket 2 — still no fill
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(21));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        assert_eq!(strategy.buckets_sent, 2);
        // Strategy is still "active" until we observe the duration expiry on the next tick
        assert!(strategy.is_active());

        // Tick after duration has elapsed (>= 40s) with all buckets sent
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(45));
        let actions3 = strategy.on_tick(&snap3);
        assert!(actions3.is_empty());
        // Strategy should now be deactivated (accepts under-execution)
        assert!(!strategy.is_active());
        // Filled nothing — that's OK, duration expiry is graceful
        assert_eq!(strategy.filled_quantity, Decimal::ZERO);
    }

    // -----------------------------------------------------------------------
    // Test 7: stop() cancels all and deactivates
    // -----------------------------------------------------------------------
    #[test]
    fn test_stop_cancels_all_and_deactivates() {
        let config = test_config();
        let mut strategy = VwapStrategy::new(config);
        assert!(strategy.is_active());

        let actions = strategy.stop();
        assert!(!strategy.is_active());
        assert_eq!(actions.len(), 1);

        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("vwap-test-1".to_string()));
            }
            _ => panic!("expected CancelAll action"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 8: on_tick returns empty when strategy is inactive
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_returns_empty_when_inactive() {
        let config = test_config();
        let mut strategy = VwapStrategy::new(config);
        strategy.stop();

        let snapshot = make_snapshot(dec!(50000), dec!(50010), Utc::now());
        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 9: on_tick returns empty when no mid price (empty order book)
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_no_mid_price() {
        let config = test_config();
        let mut strategy = VwapStrategy::new(config);

        let now = Utc::now();
        let mut snapshot = make_snapshot(dec!(50000), dec!(50010), now);
        // Clear the order book (Arc::make_mut gives mutable access in tests)
        std::sync::Arc::make_mut(&mut snapshot.order_book)
            .bids
            .clear();
        std::sync::Arc::make_mut(&mut snapshot.order_book)
            .asks
            .clear();

        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty());
        assert_eq!(strategy.buckets_sent, 0);
    }

    // -----------------------------------------------------------------------
    // Test 10: Sell side computes price correctly (mid - deviation)
    // -----------------------------------------------------------------------
    #[test]
    fn test_sell_side_price_deviation() {
        let mut config = test_config();
        config.side = Side::Sell;
        let mut strategy = VwapStrategy::new(config);

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
    // Test 11: name() and strategy_id() return correct values
    // -----------------------------------------------------------------------
    #[test]
    fn test_name_and_strategy_id() {
        let config = test_config();
        let strategy = VwapStrategy::new(config);
        assert_eq!(strategy.name(), "vwap");
        assert_eq!(strategy.strategy_id(), "vwap-test-1");
    }

    // -----------------------------------------------------------------------
    // Test 12: Rebalancing compensates for underfilled buckets while
    //          preserving volume profile proportions
    // -----------------------------------------------------------------------
    #[test]
    fn test_rebalancing_preserves_proportions() {
        let mut config = test_config();
        config.target_quantity = dec!(100);
        config.duration_secs = 60;
        // 4 buckets with known weights that divide cleanly
        // weights: [1, 2, 3, 4] -> normalized: [0.1, 0.2, 0.3, 0.4]
        // bucket_sizes: [10, 20, 30, 40]
        config.volume_profile = vec![dec!(1), dec!(2), dec!(3), dec!(4)];
        let mut strategy = VwapStrategy::new(config);

        // bucket_interval_secs = 60 / 4 = 15
        assert_eq!(strategy.bucket_interval_secs, 15);

        let now = Utc::now();

        // Bucket 1: remaining=100, remaining_weight_sum=100
        // order_qty = 100 * 10 / 100 = 10
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);
        let _qty1 = match &actions1[0] {
            OrderAction::Place(order) => {
                assert_eq!(order.quantity, dec!(10));
                order.quantity
            }
            _ => panic!("expected Place action"),
        };

        // Only 5 of 10 fills (partial IOC execution)
        let fill1 = make_fill(dec!(5), dec!(50005));
        strategy.on_fill(&fill1);
        // remaining = 95

        // Bucket 2: remaining=95, remaining_weight_sum=90 (20+30+40)
        // order_qty = 95 * 20 / 90 = 1900/90 = 21.111...
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(16));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        let qty2 = match &actions2[0] {
            OrderAction::Place(order) => order.quantity,
            _ => panic!("expected Place action"),
        };
        // The bucket 2 order should be larger than the original bucket_size (20)
        // because we are compensating for underfill in bucket 1
        assert!(
            qty2 > dec!(20),
            "bucket 2 should compensate for underfilled bucket 1"
        );

        // Full fill of bucket 2
        strategy.on_fill(&make_fill(qty2, dec!(50005)));
        let remaining_after_2 = strategy.remaining_quantity();

        // Bucket 3: remaining = 95 - qty2, remaining_weight_sum = 70 (30+40)
        // order_qty = remaining * 30 / 70
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(31));
        let actions3 = strategy.on_tick(&snap3);
        assert_eq!(actions3.len(), 1);
        let qty3 = match &actions3[0] {
            OrderAction::Place(order) => order.quantity,
            _ => panic!("expected Place action"),
        };

        // Bucket 3 and bucket 4 should maintain the 3:4 weight ratio
        // Bucket 3 order = remaining * 30/70 = remaining * 3/7
        // Bucket 4 would get = remaining * 40/70 = remaining * 4/7
        // So ratio qty3 : (remaining - qty3) should be 3:4
        let expected_qty3 = remaining_after_2 * dec!(30) / dec!(70);
        assert_eq!(qty3, expected_qty3);
    }

    // -----------------------------------------------------------------------
    // Test 13: No more buckets sent after total reached
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_buckets_after_total_reached() {
        let mut config = test_config();
        config.target_quantity = dec!(100);
        config.duration_secs = 40;
        config.volume_profile = vec![dec!(0.5), dec!(0.5)];
        // 2 buckets, bucket_interval_secs = 20
        let mut strategy = VwapStrategy::new(config);
        assert_eq!(strategy.total_buckets(), 2);

        let now = Utc::now();

        // Bucket 1
        let snap1 = make_snapshot(dec!(50000), dec!(50010), now);
        let actions1 = strategy.on_tick(&snap1);
        assert_eq!(actions1.len(), 1);

        // Bucket 2
        let snap2 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(21));
        let actions2 = strategy.on_tick(&snap2);
        assert_eq!(actions2.len(), 1);
        assert_eq!(strategy.buckets_sent, 2);

        // Bucket 3 — should be empty, all buckets sent
        let snap3 = make_snapshot(dec!(50000), dec!(50010), now + Duration::seconds(42));
        let actions3 = strategy.on_tick(&snap3);
        assert!(actions3.is_empty());
        assert_eq!(strategy.buckets_sent, 2); // unchanged
    }

    // -----------------------------------------------------------------------
    // Test 14: on_tick deactivates when fully filled before all buckets sent
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_tick_deactivates_when_fully_filled() {
        let config = test_config(); // target=100
        let mut strategy = VwapStrategy::new(config);

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
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_valid_config() {
        let config = test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_target_quantity() {
        let mut config = test_config();
        config.target_quantity = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("target_quantity must be positive"));
    }

    #[test]
    fn test_validate_empty_volume_profile() {
        let mut config = test_config();
        config.volume_profile = vec![];
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("volume_profile must not be empty"));
    }

    #[test]
    fn test_validate_negative_max_deviation() {
        let mut config = test_config();
        config.max_deviation_bps = dec!(-5);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("max_deviation_bps must be non-negative"));
    }
}
