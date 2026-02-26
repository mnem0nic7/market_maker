use std::collections::HashSet;

use chrono::{DateTime, Utc};
use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderStatus, OrderType,
    Side, Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the basic spread market making strategy.
///
/// The strategy places symmetric bid and ask orders around the mid price,
/// with each successive level offset by an additional `spread_bps` increment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Half-spread in basis points for the first level.
    /// Level N is offset by `spread_bps * N` from the mid price.
    pub spread_bps: Decimal,
    /// Order size (quantity) per level.
    pub order_size: Decimal,
    /// Number of price levels on each side.
    pub num_levels: usize,
    /// Minimum time between order refreshes (milliseconds).
    pub refresh_interval_ms: i64,
    /// Minimum acceptable spread in basis points. If the market spread is
    /// below this, the strategy will widen to this floor.
    pub min_spread_bps: Decimal,
    /// Threshold in basis points for mid-price movement that triggers a full
    /// cancel-and-replace cycle. If the mid price has moved by more than this
    /// many bps since the last quote, all orders are cancelled and re-placed.
    pub reprice_threshold_bps: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

pub struct SpreadStrategy {
    config: SpreadConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Set of client_order_ids for orders we have placed that are still live.
    live_order_ids: HashSet<String>,
    /// The mid price at which we last placed quotes.
    last_mid_price: Option<Decimal>,
    /// Timestamp of the last time we placed/refreshed quotes.
    last_refresh_time: Option<DateTime<Utc>>,
}

impl SpreadStrategy {
    pub fn new(config: SpreadConfig) -> Self {
        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            spread_bps = %config.spread_bps,
            num_levels = config.num_levels,
            order_size = %config.order_size,
            "initializing spread market making strategy"
        );
        Self {
            config,
            active: true,
            live_order_ids: HashSet::new(),
            last_mid_price: None,
            last_refresh_time: None,
        }
    }

    /// Check whether enough time has elapsed since the last refresh.
    fn should_refresh(&self, now: DateTime<Utc>) -> bool {
        match self.last_refresh_time {
            None => true,
            Some(last) => {
                let elapsed_ms = (now - last).num_milliseconds();
                elapsed_ms >= self.config.refresh_interval_ms
            }
        }
    }

    /// Check whether the mid price has moved beyond the reprice threshold
    /// since we last placed quotes.
    fn mid_price_moved_significantly(&self, current_mid: Decimal) -> bool {
        match self.last_mid_price {
            None => true,
            Some(prev_mid) => {
                if prev_mid.is_zero() {
                    return true;
                }
                let change_bps = ((current_mid - prev_mid) / prev_mid).abs() * dec!(10_000);
                change_bps > self.config.reprice_threshold_bps
            }
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

    /// Generate new limit orders around the given mid price.
    fn generate_quotes(&self, mid: Decimal) -> Vec<OrderAction> {
        let mut actions = Vec::with_capacity(self.config.num_levels * 2);

        // Use at least min_spread_bps for the base offset
        let effective_spread_bps = self.config.spread_bps.max(self.config.min_spread_bps);

        for level in 1..=self.config.num_levels {
            let level_dec = Decimal::from(level as u64);
            // Offset in bps for this level: effective_spread_bps * level
            let offset_bps = effective_spread_bps * level_dec;
            let offset_fraction = offset_bps / dec!(10_000);

            let bid_price = mid * (Decimal::ONE - offset_fraction);
            let ask_price = mid * (Decimal::ONE + offset_fraction);

            // Place bid
            actions.push(OrderAction::Place(NewOrder {
                exchange: self.config.exchange,
                symbol: self.config.symbol.clone(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                price: Some(bid_price),
                quantity: self.config.order_size,
                strategy_id: self.config.strategy_id.clone(),
                reduce_only: false,
            }));

            // Place ask
            actions.push(OrderAction::Place(NewOrder {
                exchange: self.config.exchange,
                symbol: self.config.symbol.clone(),
                side: Side::Sell,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                price: Some(ask_price),
                quantity: self.config.order_size,
                strategy_id: self.config.strategy_id.clone(),
                reduce_only: false,
            }));

            debug!(
                strategy_id = %self.config.strategy_id,
                level = level,
                bid_price = %bid_price,
                ask_price = %ask_price,
                offset_bps = %offset_bps,
                "generated quote level"
            );
        }

        actions
    }
}

impl Strategy for SpreadStrategy {
    fn name(&self) -> &str {
        "SpreadMarketMaker"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                warn!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping tick"
                );
                return Vec::new();
            }
        };

        let now = snapshot.timestamp;
        let needs_refresh = self.should_refresh(now);
        let price_moved = self.mid_price_moved_significantly(mid);

        if !needs_refresh && !price_moved {
            return Vec::new();
        }

        debug!(
            strategy_id = %self.config.strategy_id,
            mid_price = %mid,
            needs_refresh = needs_refresh,
            price_moved = price_moved,
            "repricing quotes"
        );

        let mut actions = Vec::new();

        // Cancel all existing orders before placing new ones
        if !self.live_order_ids.is_empty() {
            actions.push(self.cancel_all_action());
            self.live_order_ids.clear();
        }

        // Generate fresh quotes
        let quote_actions = self.generate_quotes(mid);

        actions.extend(quote_actions);

        self.last_mid_price = Some(mid);
        self.last_refresh_time = Some(now);

        actions
    }

    fn on_fill(&mut self, fill: &Fill) {
        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            symbol = %fill.symbol,
            "spread strategy received fill"
        );
        // Remove the filled order from tracking
        self.live_order_ids.remove(&fill.client_order_id);
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "spread strategy received order update"
        );

        // If the order reached a terminal state, remove it from tracking.
        if update.status.is_terminal() {
            self.live_order_ids.remove(&update.client_order_id);
        }

        // If the order was just acknowledged, add the real order id to tracking.
        if update.status == OrderStatus::New || update.status == OrderStatus::Submitted {
            self.live_order_ids.insert(update.client_order_id.clone());
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            "stopping spread market making strategy"
        );
        self.active = false;
        self.live_order_ids.clear();
        vec![self.cancel_all_action()]
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use mm_core::types::{
        Exchange, Fill, MarketSnapshot, OrderBook, OrderStatus, Position, PriceLevel, Side,
        Symbol,
    };
    use rust_decimal_macros::dec;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_config() -> SpreadConfig {
        SpreadConfig {
            strategy_id: "test-spread".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            spread_bps: dec!(10),
            order_size: dec!(0.01),
            num_levels: 3,
            refresh_interval_ms: 1000,
            min_spread_bps: dec!(5),
            reprice_threshold_bps: dec!(3),
        }
    }

    fn make_snapshot_at(mid_price: Decimal, timestamp: DateTime<Utc>) -> MarketSnapshot {
        // Build a simple 1-level book centered on mid_price.
        // bid = mid - 1, ask = mid + 1  =>  mid_price() = mid_price
        let order_book = OrderBook {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            bids: vec![PriceLevel {
                price: mid_price - dec!(1),
                quantity: dec!(1),
            }],
            asks: vec![PriceLevel {
                price: mid_price + dec!(1),
                quantity: dec!(1),
            }],
            timestamp,
            sequence: 1,
        };

        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book,
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: Vec::new(),
            timestamp,
        }
    }

    fn make_snapshot(mid_price: Decimal) -> MarketSnapshot {
        make_snapshot_at(mid_price, Utc::now())
    }

    fn make_empty_book_snapshot() -> MarketSnapshot {
        let order_book = OrderBook {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            bids: Vec::new(),
            asks: Vec::new(),
            timestamp: Utc::now(),
            sequence: 1,
        };

        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book,
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    fn make_fill(client_order_id: &str) -> Fill {
        Fill {
            fill_id: "fill-1".to_string(),
            client_order_id: client_order_id.to_string(),
            exchange_order_id: "exch-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            price: dec!(50000),
            quantity: dec!(0.01),
            fee: dec!(0),
            fee_currency: "USDT".to_string(),
            timestamp: Utc::now(),
        }
    }

    fn make_order_update(client_order_id: &str, status: OrderStatus) -> OrderUpdate {
        OrderUpdate {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: Some("exch-1".to_string()),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            status,
            filled_quantity: dec!(0),
            remaining_quantity: dec!(0.01),
            average_fill_price: dec!(0),
            last_fill_price: None,
            last_fill_quantity: None,
            fee: None,
            fee_currency: None,
            timestamp: Utc::now(),
        }
    }

    /// Count how many Place actions are bids vs asks.
    fn count_sides(actions: &[OrderAction]) -> (usize, usize) {
        let mut bids = 0;
        let mut asks = 0;
        for action in actions {
            if let OrderAction::Place(order) = action {
                match order.side {
                    Side::Buy => bids += 1,
                    Side::Sell => asks += 1,
                }
            }
        }
        (bids, asks)
    }

    /// Extract all Place actions from an action list.
    fn extract_places(actions: &[OrderAction]) -> Vec<&NewOrder> {
        actions
            .iter()
            .filter_map(|a| match a {
                OrderAction::Place(o) => Some(o),
                _ => None,
            })
            .collect()
    }

    /// Check whether an action list contains a CancelAll.
    fn has_cancel_all(actions: &[OrderAction]) -> bool {
        actions.iter().any(|a| matches!(a, OrderAction::CancelAll(_)))
    }

    // -----------------------------------------------------------------------
    // 1. First tick places orders
    // -----------------------------------------------------------------------

    #[test]
    fn test_first_tick_places_orders() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config.clone());
        let snapshot = make_snapshot(dec!(50000));

        let actions = strategy.on_tick(&snapshot);

        // First tick: no live orders, so no cancel. Should get 2 * num_levels = 6 place actions.
        let places = extract_places(&actions);
        assert_eq!(places.len(), config.num_levels * 2);

        let (bids, asks) = count_sides(&actions);
        assert_eq!(bids, config.num_levels);
        assert_eq!(asks, config.num_levels);

        // No CancelAll on first tick (live_order_ids is empty)
        assert!(!has_cancel_all(&actions));
    }

    // -----------------------------------------------------------------------
    // 2. Quote prices are symmetric around mid
    // -----------------------------------------------------------------------

    #[test]
    fn test_quote_prices_symmetric() {
        let mut config = make_config();
        config.spread_bps = dec!(10);
        config.num_levels = 2;
        config.min_spread_bps = dec!(5); // spread_bps > min_spread_bps, so spread_bps is used

        let mut strategy = SpreadStrategy::new(config);
        let mid = dec!(50000);
        let snapshot = make_snapshot(mid);

        let actions = strategy.on_tick(&snapshot);
        let places = extract_places(&actions);
        assert_eq!(places.len(), 4); // 2 bids + 2 asks

        // Level 1: offset = 10 bps => 10/10000 = 0.001
        // bid1 = 50000 * (1 - 0.001) = 50000 * 0.999 = 49950
        // ask1 = 50000 * (1 + 0.001) = 50000 * 1.001 = 50050
        let level1_offset = dec!(10) / dec!(10000);
        let expected_bid1 = mid * (Decimal::ONE - level1_offset);
        let expected_ask1 = mid * (Decimal::ONE + level1_offset);

        // Level 2: offset = 20 bps => 20/10000 = 0.002
        // bid2 = 50000 * (1 - 0.002) = 50000 * 0.998 = 49900
        // ask2 = 50000 * (1 + 0.002) = 50000 * 1.002 = 50100
        let level2_offset = dec!(20) / dec!(10000);
        let expected_bid2 = mid * (Decimal::ONE - level2_offset);
        let expected_ask2 = mid * (Decimal::ONE + level2_offset);

        // Orders come in pairs: bid1, ask1, bid2, ask2
        assert_eq!(places[0].side, Side::Buy);
        assert_eq!(places[0].price, Some(expected_bid1));

        assert_eq!(places[1].side, Side::Sell);
        assert_eq!(places[1].price, Some(expected_ask1));

        assert_eq!(places[2].side, Side::Buy);
        assert_eq!(places[2].price, Some(expected_bid2));

        assert_eq!(places[3].side, Side::Sell);
        assert_eq!(places[3].price, Some(expected_ask2));

        // Verify symmetry: each level's bid and ask are equidistant from mid
        let bid1_distance = mid - places[0].price.unwrap();
        let ask1_distance = places[1].price.unwrap() - mid;
        assert_eq!(bid1_distance, ask1_distance);

        let bid2_distance = mid - places[2].price.unwrap();
        let ask2_distance = places[3].price.unwrap() - mid;
        assert_eq!(bid2_distance, ask2_distance);
    }

    // -----------------------------------------------------------------------
    // 3. No action before refresh interval (when price hasn't moved)
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_action_before_refresh_interval() {
        let config = make_config(); // refresh_interval_ms = 1000
        let mut strategy = SpreadStrategy::new(config);

        let now = Utc::now();
        let snapshot1 = make_snapshot_at(dec!(50000), now);

        // First tick: places orders
        let actions1 = strategy.on_tick(&snapshot1);
        assert!(!actions1.is_empty());

        // Second tick 100ms later, same mid price => within refresh interval and
        // price hasn't moved significantly => should return empty
        let snapshot2 = make_snapshot_at(dec!(50000), now + Duration::milliseconds(100));
        let actions2 = strategy.on_tick(&snapshot2);
        assert!(actions2.is_empty(), "Expected no actions before refresh interval");
    }

    // -----------------------------------------------------------------------
    // 4. Reprice on mid price move (before refresh interval)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reprice_on_mid_price_move() {
        let mut config = make_config();
        config.refresh_interval_ms = 10_000; // very long so time-based refresh won't trigger
        config.reprice_threshold_bps = dec!(3); // 3 bps threshold

        let mut strategy = SpreadStrategy::new(config);

        let now = Utc::now();
        let snapshot1 = make_snapshot_at(dec!(50000), now);
        let _actions1 = strategy.on_tick(&snapshot1);

        // Move mid price by > 3 bps (3 bps of 50000 = 15).
        // Let's move it by 20 (4 bps) — should trigger reprice.
        let snapshot2 = make_snapshot_at(dec!(50020), now + Duration::milliseconds(100));
        let actions2 = strategy.on_tick(&snapshot2);

        assert!(
            !actions2.is_empty(),
            "Expected reprice actions when mid moves beyond threshold"
        );

        // Should have place actions for the new quotes
        let places = extract_places(&actions2);
        assert!(!places.is_empty());
    }

    // -----------------------------------------------------------------------
    // 5. min_spread_bps is enforced as a floor
    // -----------------------------------------------------------------------

    #[test]
    fn test_min_spread_bps_enforced() {
        let mut config = make_config();
        config.spread_bps = dec!(5);       // 5 bps configured
        config.min_spread_bps = dec!(10);  // but minimum is 10 bps
        config.num_levels = 1;

        let mut strategy = SpreadStrategy::new(config);
        let mid = dec!(50000);
        let snapshot = make_snapshot(mid);

        let actions = strategy.on_tick(&snapshot);
        let places = extract_places(&actions);
        assert_eq!(places.len(), 2); // 1 bid + 1 ask

        // Effective spread should use min_spread_bps (10 bps), not spread_bps (5 bps)
        // Level 1: offset = 10 bps = 10/10000 = 0.001
        let effective_offset = dec!(10) / dec!(10000);
        let expected_bid = mid * (Decimal::ONE - effective_offset);
        let expected_ask = mid * (Decimal::ONE + effective_offset);

        assert_eq!(
            places[0].price,
            Some(expected_bid),
            "Bid should use min_spread_bps as floor"
        );
        assert_eq!(
            places[1].price,
            Some(expected_ask),
            "Ask should use min_spread_bps as floor"
        );

        // Verify the spread is NOT based on the configured 5 bps
        let wrong_offset = dec!(5) / dec!(10000);
        let wrong_bid = mid * (Decimal::ONE - wrong_offset);
        assert_ne!(
            places[0].price,
            Some(wrong_bid),
            "Should NOT use spread_bps when it's below min_spread_bps"
        );
    }

    // -----------------------------------------------------------------------
    // 6. Inactive strategy returns empty on tick
    // -----------------------------------------------------------------------

    #[test]
    fn test_inactive_returns_empty() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);

        // Stop the strategy first
        let _ = strategy.stop();
        assert!(!strategy.is_active());

        // on_tick should return empty when inactive
        let snapshot = make_snapshot(dec!(50000));
        let actions = strategy.on_tick(&snapshot);
        assert!(actions.is_empty(), "Inactive strategy should return empty on_tick");
    }

    // -----------------------------------------------------------------------
    // 7. stop() returns CancelAll and sets active=false
    // -----------------------------------------------------------------------

    #[test]
    fn test_stop_returns_cancel_all() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config.clone());

        // Verify initially active
        assert!(strategy.is_active());

        let actions = strategy.stop();

        // Should no longer be active
        assert!(!strategy.is_active());

        // Should contain a CancelAll action
        assert_eq!(actions.len(), 1);
        assert!(has_cancel_all(&actions));

        // Verify CancelAll targets the correct exchange/symbol/strategy
        if let OrderAction::CancelAll(cancel) = &actions[0] {
            assert_eq!(cancel.exchange, Some(Exchange::Simulated));
            assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
            assert_eq!(cancel.strategy_id, Some(config.strategy_id.clone()));
        } else {
            panic!("Expected CancelAll action from stop()");
        }
    }

    // -----------------------------------------------------------------------
    // 8. on_fill removes order from tracking
    // -----------------------------------------------------------------------

    #[test]
    fn test_on_fill_removes_from_tracking() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);

        // Simulate an order being acknowledged (adds to live_order_ids)
        let order_id = "order-abc";
        strategy.on_order_update(&make_order_update(order_id, OrderStatus::New));

        assert!(
            strategy.live_order_ids.contains(order_id),
            "Order should be tracked after New status update"
        );

        // Simulate a fill on that order
        let fill = make_fill(order_id);
        strategy.on_fill(&fill);

        assert!(
            !strategy.live_order_ids.contains(order_id),
            "Order should be removed from tracking after fill"
        );
    }

    // -----------------------------------------------------------------------
    // 9. No mid price (empty book) returns empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_mid_price_returns_empty() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);

        let snapshot = make_empty_book_snapshot();
        let actions = strategy.on_tick(&snapshot);

        assert!(
            actions.is_empty(),
            "Should return empty when order book has no mid price"
        );
    }

    // -----------------------------------------------------------------------
    // Additional coverage: on_order_update tracking lifecycle
    // -----------------------------------------------------------------------

    #[test]
    fn test_order_update_tracking_lifecycle() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);

        let order_id = "order-xyz";

        // Submitted status should add to tracking
        strategy.on_order_update(&make_order_update(order_id, OrderStatus::Submitted));
        assert!(strategy.live_order_ids.contains(order_id));

        // Terminal status (Cancelled) should remove from tracking
        strategy.on_order_update(&make_order_update(order_id, OrderStatus::Cancelled));
        assert!(!strategy.live_order_ids.contains(order_id));
    }

    #[test]
    fn test_cancel_all_emitted_when_live_orders_exist() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);

        // First tick: places orders
        let snapshot1 = make_snapshot(dec!(50000));
        let _ = strategy.on_tick(&snapshot1);

        // Simulate orders being acknowledged so live_order_ids is non-empty
        strategy.on_order_update(&make_order_update("ord-1", OrderStatus::New));
        strategy.on_order_update(&make_order_update("ord-2", OrderStatus::New));

        // Wait for refresh interval to expire, then tick again
        let later = Utc::now() + Duration::seconds(2);
        let snapshot2 = make_snapshot_at(dec!(50000), later);
        let actions = strategy.on_tick(&snapshot2);

        // Should have CancelAll first, then new place orders
        assert!(has_cancel_all(&actions), "Should cancel existing orders before re-placing");
        let places = extract_places(&actions);
        assert!(!places.is_empty(), "Should place new orders after cancel");
    }

    #[test]
    fn test_refresh_after_interval_elapsed() {
        let config = make_config(); // refresh_interval_ms = 1000
        let mut strategy = SpreadStrategy::new(config.clone());

        let now = Utc::now();
        let snapshot1 = make_snapshot_at(dec!(50000), now);
        let _ = strategy.on_tick(&snapshot1);

        // After refresh interval elapses, same price should still trigger refresh
        let snapshot2 =
            make_snapshot_at(dec!(50000), now + Duration::milliseconds(config.refresh_interval_ms + 1));
        let actions2 = strategy.on_tick(&snapshot2);

        assert!(
            !actions2.is_empty(),
            "Should refresh orders after interval elapsed"
        );
    }

    #[test]
    fn test_order_size_matches_config() {
        let mut config = make_config();
        config.order_size = dec!(0.05);
        config.num_levels = 2;

        let mut strategy = SpreadStrategy::new(config);
        let snapshot = make_snapshot(dec!(50000));
        let actions = strategy.on_tick(&snapshot);
        let places = extract_places(&actions);

        for order in places {
            assert_eq!(
                order.quantity,
                dec!(0.05),
                "Every placed order should use the configured order_size"
            );
        }
    }

    #[test]
    fn test_all_orders_are_limit_gtc() {
        let config = make_config();
        let mut strategy = SpreadStrategy::new(config);
        let snapshot = make_snapshot(dec!(50000));
        let actions = strategy.on_tick(&snapshot);
        let places = extract_places(&actions);

        for order in places {
            assert_eq!(order.order_type, OrderType::Limit);
            assert_eq!(order.time_in_force, TimeInForce::GTC);
            assert!(!order.reduce_only);
        }
    }
}
