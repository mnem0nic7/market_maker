//! Local order book manager.
//!
//! Maintains a sorted order book in memory from WebSocket depth snapshots and
//! incremental deltas. Provides efficient best-bid/ask queries and converts
//! to the canonical [`mm_core::OrderBook`] type on demand.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use mm_core::types::{Exchange, OrderBook, PriceLevel, Symbol};

// ---------------------------------------------------------------------------
// LocalOrderBook
// ---------------------------------------------------------------------------

/// A locally-maintained order book built from exchange WebSocket updates.
///
/// Bids are stored in descending price order (highest first) and asks in
/// ascending price order (lowest first). A quantity of zero means the price
/// level should be removed.
#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    pub exchange: Exchange,
    pub symbol: Symbol,

    /// Bids keyed by `Reverse<Decimal>` so that iteration yields highest
    /// price first.
    bids: BTreeMap<Reverse<Decimal>, Decimal>,
    /// Asks keyed by `Decimal` — natural ascending order.
    asks: BTreeMap<Decimal, Decimal>,

    /// Last update timestamp.
    pub timestamp: DateTime<Utc>,
    /// Exchange-specific sequence/update-id for gap detection.
    pub sequence: u64,
}

impl LocalOrderBook {
    /// Create an empty local order book.
    pub fn new(exchange: Exchange, symbol: Symbol) -> Self {
        Self {
            exchange,
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            timestamp: Utc::now(),
            sequence: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot & delta application
    // -----------------------------------------------------------------------

    /// Replace the entire book with a full depth snapshot.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
        timestamp: DateTime<Utc>,
        sequence: u64,
    ) {
        self.bids.clear();
        self.asks.clear();

        for &(price, qty) in bids {
            if qty > Decimal::ZERO {
                self.bids.insert(Reverse(price), qty);
            }
        }
        for &(price, qty) in asks {
            if qty > Decimal::ZERO {
                self.asks.insert(price, qty);
            }
        }

        self.timestamp = timestamp;
        self.sequence = sequence;
    }

    /// Apply an incremental delta to the book.
    ///
    /// A quantity of zero signals removal of the price level.
    pub fn apply_delta(
        &mut self,
        bid_updates: &[(Decimal, Decimal)],
        ask_updates: &[(Decimal, Decimal)],
        timestamp: DateTime<Utc>,
        sequence: u64,
    ) {
        for &(price, qty) in bid_updates {
            if qty == Decimal::ZERO {
                self.bids.remove(&Reverse(price));
            } else {
                self.bids.insert(Reverse(price), qty);
            }
        }

        for &(price, qty) in ask_updates {
            if qty == Decimal::ZERO {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }

        self.timestamp = timestamp;
        self.sequence = sequence;
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /// The highest bid (best bid).
    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids
            .iter()
            .next()
            .map(|(Reverse(price), qty)| PriceLevel {
                price: *price,
                quantity: *qty,
            })
    }

    /// The lowest ask (best ask).
    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.iter().next().map(|(price, qty)| PriceLevel {
            price: *price,
            quantity: *qty,
        })
    }

    /// Mid-price: average of best bid and best ask.
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::from(2)),
            _ => None,
        }
    }

    /// Spread (best ask - best bid).
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    /// Spread in basis points.
    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > Decimal::ZERO => {
                Some(spread / mid * Decimal::from(10_000))
            }
            _ => None,
        }
    }

    /// Whether the order book data is stale (too old to trade on safely).
    ///
    /// Returns `true` if:
    /// - The book is not initialized (missing bids or asks), OR
    /// - The book's timestamp is older than `max_age` from the current time.
    pub fn is_stale(&self, max_age: std::time::Duration) -> bool {
        if !self.is_initialized() {
            return true;
        }
        let age = Utc::now() - self.timestamp;
        let max_age_chrono =
            chrono::Duration::from_std(max_age).unwrap_or(chrono::Duration::MAX);
        age > max_age_chrono
    }

    /// How long ago the book was last updated.
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.timestamp
    }

    /// Number of bid levels.
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask levels.
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    /// Total bid volume across all levels.
    pub fn total_bid_volume(&self) -> Decimal {
        self.bids.values().copied().sum()
    }

    /// Total ask volume across all levels.
    pub fn total_ask_volume(&self) -> Decimal {
        self.asks.values().copied().sum()
    }

    /// Return the top `n` bid levels (highest price first).
    pub fn top_bids(&self, n: usize) -> Vec<PriceLevel> {
        self.bids
            .iter()
            .take(n)
            .map(|(Reverse(price), qty)| PriceLevel {
                price: *price,
                quantity: *qty,
            })
            .collect()
    }

    /// Return the top `n` ask levels (lowest price first).
    pub fn top_asks(&self, n: usize) -> Vec<PriceLevel> {
        self.asks
            .iter()
            .take(n)
            .map(|(price, qty)| PriceLevel {
                price: *price,
                quantity: *qty,
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Conversion
    // -----------------------------------------------------------------------

    /// Convert the local book to the canonical [`OrderBook`] type,
    /// taking the top `depth` levels from each side.
    pub fn to_order_book(&self, depth: usize) -> OrderBook {
        OrderBook {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            bids: self.top_bids(depth),
            asks: self.top_asks(depth),
            timestamp: self.timestamp,
            sequence: self.sequence,
        }
    }

    /// Whether the book has been initialized (has at least one level on each side).
    pub fn is_initialized(&self) -> bool {
        !self.bids.is_empty() && !self.asks.is_empty()
    }

    /// Clear all levels.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.sequence = 0;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_book() -> LocalOrderBook {
        LocalOrderBook::new(Exchange::Binance, Symbol::new("BTC", "USDT"))
    }

    #[test]
    fn snapshot_builds_book() {
        let mut book = make_book();
        let bids = vec![
            (dec!(100.0), dec!(1.0)),
            (dec!(99.0), dec!(2.0)),
            (dec!(98.0), dec!(3.0)),
        ];
        let asks = vec![(dec!(101.0), dec!(1.5)), (dec!(102.0), dec!(2.5))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        assert_eq!(book.best_bid().unwrap().price, dec!(100.0));
        assert_eq!(book.best_ask().unwrap().price, dec!(101.0));
        assert_eq!(book.bid_depth(), 3);
        assert_eq!(book.ask_depth(), 2);
    }

    #[test]
    fn delta_updates_levels() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0)), (dec!(99.0), dec!(2.0))];
        let asks = vec![(dec!(101.0), dec!(1.5))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        // Update existing bid, add new bid, remove an ask
        let bid_deltas = vec![(dec!(100.0), dec!(5.0)), (dec!(100.5), dec!(0.5))];
        let ask_deltas = vec![(dec!(101.0), dec!(0.0)), (dec!(101.5), dec!(3.0))];
        book.apply_delta(&bid_deltas, &ask_deltas, Utc::now(), 2);

        // Best bid should now be 100.5
        assert_eq!(book.best_bid().unwrap().price, dec!(100.5));
        assert_eq!(book.best_bid().unwrap().quantity, dec!(0.5));

        // 101.0 was removed; best ask should be 101.5
        assert_eq!(book.best_ask().unwrap().price, dec!(101.5));
        assert_eq!(book.best_ask().unwrap().quantity, dec!(3.0));
    }

    #[test]
    fn zero_quantity_removes_level() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0))];
        let asks = vec![(dec!(101.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        book.apply_delta(&[(dec!(100.0), dec!(0.0))], &[], Utc::now(), 2);
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn mid_price_and_spread() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0))];
        let asks = vec![(dec!(102.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        assert_eq!(book.mid_price().unwrap(), dec!(101.0));
        assert_eq!(book.spread().unwrap(), dec!(2.0));
    }

    #[test]
    fn to_order_book_limits_depth() {
        let mut book = make_book();
        let bids = vec![
            (dec!(100.0), dec!(1.0)),
            (dec!(99.0), dec!(2.0)),
            (dec!(98.0), dec!(3.0)),
        ];
        let asks = vec![
            (dec!(101.0), dec!(1.0)),
            (dec!(102.0), dec!(2.0)),
            (dec!(103.0), dec!(3.0)),
        ];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        let ob = book.to_order_book(2);
        assert_eq!(ob.bids.len(), 2);
        assert_eq!(ob.asks.len(), 2);
        assert_eq!(ob.bids[0].price, dec!(100.0));
        assert_eq!(ob.bids[1].price, dec!(99.0));
        assert_eq!(ob.asks[0].price, dec!(101.0));
        assert_eq!(ob.asks[1].price, dec!(102.0));
    }

    #[test]
    fn snapshot_ignores_zero_quantities() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(0.0)), (dec!(99.0), dec!(1.0))];
        let asks = vec![(dec!(101.0), dec!(0.0)), (dec!(102.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.ask_depth(), 1);
        assert_eq!(book.best_bid().unwrap().price, dec!(99.0));
        assert_eq!(book.best_ask().unwrap().price, dec!(102.0));
    }

    #[test]
    fn total_volume() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0)), (dec!(99.0), dec!(2.0))];
        let asks = vec![(dec!(101.0), dec!(3.0)), (dec!(102.0), dec!(4.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        assert_eq!(book.total_bid_volume(), dec!(3.0));
        assert_eq!(book.total_ask_volume(), dec!(7.0));
    }

    #[test]
    fn test_is_stale_empty_book() {
        let book = make_book();
        // An uninitialized book (no bids or asks) should always be considered stale.
        assert!(book.is_stale(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn test_is_stale_fresh_book() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0))];
        let asks = vec![(dec!(101.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        // A book that was just updated should NOT be stale with a 5-second threshold.
        assert!(!book.is_stale(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn test_is_stale_old_book() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0))];
        let asks = vec![(dec!(101.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        // Manually set the timestamp to 10 seconds ago.
        book.timestamp = Utc::now() - chrono::Duration::seconds(10);

        // With a 5-second threshold the book should be stale.
        assert!(book.is_stale(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn test_age_returns_correct_duration() {
        let mut book = make_book();
        let bids = vec![(dec!(100.0), dec!(1.0))];
        let asks = vec![(dec!(101.0), dec!(1.0))];
        book.apply_snapshot(&bids, &asks, Utc::now(), 1);

        // Manually set the timestamp to 10 seconds ago.
        book.timestamp = Utc::now() - chrono::Duration::seconds(10);

        let age = book.age();
        // The age should be approximately 10 seconds (allow some tolerance for
        // execution time).
        assert!(age.num_seconds() >= 9 && age.num_seconds() <= 11);
    }
}
