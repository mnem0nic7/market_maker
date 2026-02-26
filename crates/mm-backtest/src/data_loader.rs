//! Historical data loading for backtesting.
//!
//! Provides three data sources:
//! 1. **CSV loader** — load OHLCV data from CSV files and convert to [`MarketEvent`]s.
//! 2. **Binance downloader** — fetch klines (OHLCV) or aggregated trades via REST API.
//! 3. **Local cache** — store downloaded data in `data/cache/` to avoid re-downloading.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use mm_core::{Exchange, PriceLevel, Side, Symbol};

use crate::replay::MarketEvent;

// ---------------------------------------------------------------------------
// CSV Loader
// ---------------------------------------------------------------------------

/// A single OHLCV row from a CSV file.
#[derive(Debug, Clone, Deserialize)]
pub struct OhlcvRow {
    /// Unix timestamp in milliseconds (or seconds, auto-detected).
    pub timestamp: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

/// Load OHLCV data from a CSV file and convert to MarketEvents.
///
/// Expected CSV columns: `timestamp,open,high,low,close,volume`
/// (header row is optional — auto-detected if the first field is not numeric).
///
/// Each row produces an `OrderBookSnapshot` with synthetic bid/ask levels
/// derived from the close price and a configurable spread.
pub fn load_csv(
    path: &Path,
    exchange: Exchange,
    symbol: Symbol,
    spread_bps: Decimal,
) -> anyhow::Result<Vec<MarketEvent>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut events = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Skip header row if present.
        if line_no == 0 && !trimmed.chars().next().unwrap_or('x').is_ascii_digit() {
            continue;
        }

        let fields: Vec<&str> = trimmed.split(',').collect();
        if fields.len() < 6 {
            warn!(line = line_no + 1, "skipping CSV line with < 6 fields");
            continue;
        }

        let row = OhlcvRow {
            timestamp: fields[0]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid timestamp: {e}", line_no + 1))?,
            open: fields[1]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid open: {e}", line_no + 1))?,
            high: fields[2]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid high: {e}", line_no + 1))?,
            low: fields[3]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid low: {e}", line_no + 1))?,
            close: fields[4]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid close: {e}", line_no + 1))?,
            volume: fields[5]
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("line {}: invalid volume: {e}", line_no + 1))?,
        };

        let event =
            ohlcv_to_market_event(&row, exchange, symbol.clone(), spread_bps, line_no as u64);
        events.push(event);
    }

    info!(rows = events.len(), path = %path.display(), "loaded CSV data");
    Ok(events)
}

/// Convert a single OHLCV row into an OrderBookSnapshot.
fn ohlcv_to_market_event(
    row: &OhlcvRow,
    exchange: Exchange,
    symbol: Symbol,
    spread_bps: Decimal,
    sequence: u64,
) -> MarketEvent {
    // Auto-detect timestamp format: seconds vs milliseconds.
    let ts = if row.timestamp > 1_000_000_000_000 {
        // Milliseconds
        Utc.timestamp_millis_opt(row.timestamp)
            .single()
            .unwrap_or_else(Utc::now)
    } else {
        // Seconds
        Utc.timestamp_opt(row.timestamp, 0)
            .single()
            .unwrap_or_else(Utc::now)
    };

    let close = row.close;
    let half_spread = close * spread_bps / dec!(10000) / dec!(2);

    let best_bid = close - half_spread;
    let best_ask = close + half_spread;

    // Create 3 synthetic levels using OHLCV data.
    let level_step = half_spread;

    let bids = vec![
        PriceLevel {
            price: best_bid.round_dp(8),
            quantity: row.volume / dec!(6),
        },
        PriceLevel {
            price: (best_bid - level_step).round_dp(8),
            quantity: row.volume / dec!(4),
        },
        PriceLevel {
            price: (best_bid - level_step * dec!(2)).round_dp(8),
            quantity: row.volume / dec!(3),
        },
    ];

    let asks = vec![
        PriceLevel {
            price: best_ask.round_dp(8),
            quantity: row.volume / dec!(6),
        },
        PriceLevel {
            price: (best_ask + level_step).round_dp(8),
            quantity: row.volume / dec!(4),
        },
        PriceLevel {
            price: (best_ask + level_step * dec!(2)).round_dp(8),
            quantity: row.volume / dec!(3),
        },
    ];

    MarketEvent::OrderBookSnapshot {
        timestamp: ts,
        exchange,
        symbol,
        bids,
        asks,
        sequence,
    }
}

// ---------------------------------------------------------------------------
// Binance Kline Downloader
// ---------------------------------------------------------------------------

/// A single Binance kline (candlestick) record.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceKline {
    open_time: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    close_time: i64,
}

/// A Binance aggregated trade record.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "a")]
    agg_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

/// Download OHLCV klines from Binance and convert to MarketEvents.
///
/// # Arguments
/// * `symbol_str` — Binance symbol format (e.g., "BTCUSDT")
/// * `interval` — Candle interval (e.g., "1m", "5m", "1h", "1d")
/// * `start_time` — Start timestamp in milliseconds
/// * `end_time` — End timestamp in milliseconds
/// * `canonical_symbol` — Canonical symbol for the MarketEvent
/// * `spread_bps` — Spread in basis points for synthetic order book
pub async fn download_binance_klines(
    symbol_str: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
    canonical_symbol: Symbol,
    spread_bps: Decimal,
) -> anyhow::Result<Vec<MarketEvent>> {
    let cache_path = cache_path("binance", symbol_str, interval, start_time, end_time);

    // Check cache first.
    if cache_path.exists() {
        info!(path = %cache_path.display(), "loading from cache");
        let data = std::fs::read_to_string(&cache_path)?;
        let klines: Vec<BinanceKline> = serde_json::from_str(&data)?;
        return Ok(klines_to_events(&klines, canonical_symbol, spread_bps));
    }

    let client = reqwest::Client::new();
    let mut all_klines: Vec<BinanceKline> = Vec::new();
    let mut current_start = start_time;

    info!(
        symbol = symbol_str,
        interval = interval,
        "downloading Binance klines"
    );

    loop {
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit=1000",
            symbol_str, interval, current_start, end_time
        );

        let resp = client.get(&url).send().await?;
        let body: Vec<Vec<serde_json::Value>> = resp.json().await?;

        if body.is_empty() {
            break;
        }

        for raw in &body {
            if raw.len() >= 7 {
                all_klines.push(BinanceKline {
                    open_time: raw[0].as_i64().unwrap_or(0),
                    open: raw[1].as_str().unwrap_or("0").to_string(),
                    high: raw[2].as_str().unwrap_or("0").to_string(),
                    low: raw[3].as_str().unwrap_or("0").to_string(),
                    close: raw[4].as_str().unwrap_or("0").to_string(),
                    volume: raw[5].as_str().unwrap_or("0").to_string(),
                    close_time: raw[6].as_i64().unwrap_or(0),
                });
            }
        }

        // Move start to after the last candle.
        if let Some(last) = body.last() {
            let last_close_time = last[6].as_i64().unwrap_or(end_time);
            if last_close_time >= end_time || body.len() < 1000 {
                break;
            }
            current_start = last_close_time + 1;
        } else {
            break;
        }
    }

    info!(klines = all_klines.len(), "download complete");

    // Cache the result.
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let json = serde_json::to_string(&all_klines)?;
    std::fs::write(&cache_path, &json).ok();

    Ok(klines_to_events(&all_klines, canonical_symbol, spread_bps))
}

/// Download aggregated trades from Binance and convert to MarketEvents.
pub async fn download_binance_trades(
    symbol_str: &str,
    start_time: i64,
    end_time: i64,
    canonical_symbol: Symbol,
) -> anyhow::Result<Vec<MarketEvent>> {
    let cache_path = cache_path("binance", symbol_str, "trades", start_time, end_time);

    if cache_path.exists() {
        info!(path = %cache_path.display(), "loading trades from cache");
        let data = std::fs::read_to_string(&cache_path)?;
        let trades: Vec<BinanceAggTrade> = serde_json::from_str(&data)?;
        return Ok(agg_trades_to_events(&trades, canonical_symbol));
    }

    let client = reqwest::Client::new();
    let mut all_trades: Vec<BinanceAggTrade> = Vec::new();
    let mut current_start = start_time;

    info!(symbol = symbol_str, "downloading Binance aggregated trades");

    loop {
        let url = format!(
            "https://api.binance.com/api/v3/aggTrades?symbol={}&startTime={}&endTime={}&limit=1000",
            symbol_str, current_start, end_time
        );

        let resp = client.get(&url).send().await?;
        let trades: Vec<BinanceAggTrade> = resp.json().await?;

        if trades.is_empty() {
            break;
        }

        let last_ts = trades.last().map(|t| t.timestamp).unwrap_or(end_time);
        let count = trades.len();
        all_trades.extend(trades);

        if last_ts >= end_time || count < 1000 {
            break;
        }
        current_start = last_ts + 1;
    }

    info!(trades = all_trades.len(), "trade download complete");

    // Cache.
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let json = serde_json::to_string(&all_trades)?;
    std::fs::write(&cache_path, &json).ok();

    Ok(agg_trades_to_events(&all_trades, canonical_symbol))
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn klines_to_events(
    klines: &[BinanceKline],
    symbol: Symbol,
    spread_bps: Decimal,
) -> Vec<MarketEvent> {
    klines
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let row = OhlcvRow {
                timestamp: k.open_time,
                open: k.open.parse().unwrap_or(dec!(0)),
                high: k.high.parse().unwrap_or(dec!(0)),
                low: k.low.parse().unwrap_or(dec!(0)),
                close: k.close.parse().unwrap_or(dec!(0)),
                volume: k.volume.parse().unwrap_or(dec!(0)),
            };
            ohlcv_to_market_event(
                &row,
                Exchange::Binance,
                symbol.clone(),
                spread_bps,
                i as u64,
            )
        })
        .collect()
}

fn agg_trades_to_events(trades: &[BinanceAggTrade], symbol: Symbol) -> Vec<MarketEvent> {
    trades
        .iter()
        .map(|t| {
            let price: Decimal = t.price.parse().unwrap_or(dec!(0));
            let quantity: Decimal = t.quantity.parse().unwrap_or(dec!(0));
            let side = if t.is_buyer_maker {
                Side::Sell
            } else {
                Side::Buy
            };

            let timestamp = Utc
                .timestamp_millis_opt(t.timestamp)
                .single()
                .unwrap_or_else(Utc::now);

            MarketEvent::TradeEvent {
                timestamp,
                exchange: Exchange::Binance,
                symbol: symbol.clone(),
                price,
                quantity,
                side,
                trade_id: t.agg_trade_id.to_string(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Cache management
// ---------------------------------------------------------------------------

fn cache_path(exchange: &str, symbol: &str, interval: &str, start: i64, end: i64) -> PathBuf {
    PathBuf::from(format!(
        "data/cache/{}_{}_{}_{}_{}.json",
        exchange,
        symbol.to_lowercase(),
        interval,
        start,
        end
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_csv_basic() {
        // Create a temporary CSV file.
        let dir = std::env::temp_dir().join("mm_test_csv");
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("test_ohlcv.csv");

        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "timestamp,open,high,low,close,volume").unwrap();
        writeln!(f, "1700000000000,50000,50100,49900,50050,10.5").unwrap();
        writeln!(f, "1700000060000,50050,50150,49950,50100,8.2").unwrap();

        let symbol = Symbol::new("BTC", "USDT");
        let events = load_csv(&csv_path, Exchange::Simulated, symbol, dec!(10)).unwrap();

        assert_eq!(events.len(), 2);
        match &events[0] {
            MarketEvent::OrderBookSnapshot { bids, asks, .. } => {
                assert!(!bids.is_empty());
                assert!(!asks.is_empty());
                // Best bid should be below close, best ask above.
                assert!(bids[0].price < dec!(50050));
                assert!(asks[0].price > dec!(50050));
            }
            _ => panic!("expected OrderBookSnapshot"),
        }

        // Clean up.
        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn test_ohlcv_to_market_event_spread() {
        let row = OhlcvRow {
            timestamp: 1700000000,
            open: dec!(100),
            high: dec!(105),
            low: dec!(95),
            close: dec!(100),
            volume: dec!(1000),
        };

        let event = ohlcv_to_market_event(
            &row,
            Exchange::Simulated,
            Symbol::new("TEST", "USD"),
            dec!(100), // 100 bps = 1% spread
            0,
        );

        match event {
            MarketEvent::OrderBookSnapshot { bids, asks, .. } => {
                // 100 bps spread on price 100 → half spread = 0.5
                // best_bid ≈ 99.5, best_ask ≈ 100.5
                assert!(bids[0].price > dec!(99) && bids[0].price < dec!(100));
                assert!(asks[0].price > dec!(100) && asks[0].price < dec!(101));
            }
            _ => panic!("expected OrderBookSnapshot"),
        }
    }

    #[test]
    fn test_klines_to_events() {
        let klines = vec![BinanceKline {
            open_time: 1700000000000,
            open: "50000".into(),
            high: "50100".into(),
            low: "49900".into(),
            close: "50050".into(),
            volume: "100".into(),
            close_time: 1700000059999,
        }];

        let events = klines_to_events(&klines, Symbol::new("BTC", "USDT"), dec!(10));
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_agg_trades_to_events() {
        let trades = vec![BinanceAggTrade {
            agg_trade_id: 12345,
            price: "50000.50".into(),
            quantity: "0.1".into(),
            timestamp: 1700000000000,
            is_buyer_maker: false,
        }];

        let events = agg_trades_to_events(&trades, Symbol::new("BTC", "USDT"));
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::TradeEvent { price, side, .. } => {
                assert_eq!(*price, dec!(50000.50));
                assert_eq!(*side, Side::Buy);
            }
            _ => panic!("expected TradeEvent"),
        }
    }

    #[test]
    fn test_agg_trades_buyer_maker_is_sell() {
        let trades = vec![BinanceAggTrade {
            agg_trade_id: 99,
            price: "40000".into(),
            quantity: "0.5".into(),
            timestamp: 1700000000000,
            is_buyer_maker: true,
        }];

        let events = agg_trades_to_events(&trades, Symbol::new("BTC", "USDT"));
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::TradeEvent { side, .. } => {
                assert_eq!(*side, Side::Sell, "is_buyer_maker=true should map to Sell");
            }
            _ => panic!("expected TradeEvent"),
        }
    }

    #[test]
    fn test_ohlcv_to_market_event_produces_three_levels() {
        let row = OhlcvRow {
            timestamp: 1700000000,
            open: dec!(100),
            high: dec!(105),
            low: dec!(95),
            close: dec!(100),
            volume: dec!(1200),
        };

        let event = ohlcv_to_market_event(
            &row,
            Exchange::Simulated,
            Symbol::new("ETH", "USD"),
            dec!(20), // 20 bps spread
            42,
        );

        match event {
            MarketEvent::OrderBookSnapshot {
                bids,
                asks,
                exchange,
                symbol,
                sequence,
                ..
            } => {
                assert_eq!(bids.len(), 3, "Should produce 3 bid levels");
                assert_eq!(asks.len(), 3, "Should produce 3 ask levels");
                assert_eq!(exchange, Exchange::Simulated);
                assert_eq!(symbol, Symbol::new("ETH", "USD"));
                assert_eq!(sequence, 42);

                // Bids should be in descending price order
                assert!(bids[0].price > bids[1].price, "Bid levels should descend");
                assert!(bids[1].price > bids[2].price, "Bid levels should descend");

                // Asks should be in ascending price order
                assert!(asks[0].price < asks[1].price, "Ask levels should ascend");
                assert!(asks[1].price < asks[2].price, "Ask levels should ascend");

                // All prices should be positive
                for bid in &bids {
                    assert!(bid.price > Decimal::ZERO, "Bid price must be positive");
                    assert!(
                        bid.quantity > Decimal::ZERO,
                        "Bid quantity must be positive"
                    );
                }
                for ask in &asks {
                    assert!(ask.price > Decimal::ZERO, "Ask price must be positive");
                    assert!(
                        ask.quantity > Decimal::ZERO,
                        "Ask quantity must be positive"
                    );
                }

                // Best bid < best ask (no crossed book)
                assert!(
                    bids[0].price < asks[0].price,
                    "Best bid {} should be less than best ask {}",
                    bids[0].price,
                    asks[0].price
                );
            }
            _ => panic!("expected OrderBookSnapshot"),
        }
    }

    #[test]
    fn test_ohlcv_timestamp_seconds_autodetect() {
        // Timestamp in seconds (< 1_000_000_000_000)
        let row = OhlcvRow {
            timestamp: 1700000000, // seconds
            open: dec!(100),
            high: dec!(110),
            low: dec!(90),
            close: dec!(105),
            volume: dec!(500),
        };

        let event = ohlcv_to_market_event(
            &row,
            Exchange::Simulated,
            Symbol::new("BTC", "USDT"),
            dec!(10),
            0,
        );

        let ts = event.timestamp();
        // 1700000000 seconds = 2023-11-14T22:13:20Z
        assert_eq!(ts.timestamp(), 1700000000);
    }

    #[test]
    fn test_ohlcv_timestamp_millis_autodetect() {
        // Timestamp in milliseconds (> 1_000_000_000_000)
        let row = OhlcvRow {
            timestamp: 1700000000000, // milliseconds
            open: dec!(100),
            high: dec!(110),
            low: dec!(90),
            close: dec!(105),
            volume: dec!(500),
        };

        let event = ohlcv_to_market_event(
            &row,
            Exchange::Simulated,
            Symbol::new("BTC", "USDT"),
            dec!(10),
            0,
        );

        let ts = event.timestamp();
        assert_eq!(ts.timestamp(), 1700000000);
    }

    #[test]
    fn test_load_csv_no_header() {
        // CSV without a header row (first char is digit)
        let dir = std::env::temp_dir().join("mm_test_csv_noheader");
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("test_no_header.csv");

        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "1700000000000,50000,50100,49900,50050,10.5").unwrap();
        writeln!(f, "1700000060000,50050,50150,49950,50100,8.2").unwrap();
        writeln!(f, "1700000120000,50100,50200,50000,50150,12.0").unwrap();

        let symbol = Symbol::new("BTC", "USDT");
        let events = load_csv(&csv_path, Exchange::Simulated, symbol, dec!(10)).unwrap();

        assert_eq!(events.len(), 3, "Should load all 3 rows without header");

        // Verify events are OrderBookSnapshots with correct data
        for event in &events {
            match event {
                MarketEvent::OrderBookSnapshot {
                    bids,
                    asks,
                    exchange,
                    ..
                } => {
                    assert!(!bids.is_empty());
                    assert!(!asks.is_empty());
                    assert_eq!(*exchange, Exchange::Simulated);
                }
                _ => panic!("expected OrderBookSnapshot"),
            }
        }

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn test_load_csv_skips_short_lines() {
        let dir = std::env::temp_dir().join("mm_test_csv_short");
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("test_short_lines.csv");

        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "timestamp,open,high,low,close,volume").unwrap();
        writeln!(f, "1700000000000,50000,50100,49900,50050,10.5").unwrap();
        writeln!(f, "1700000060000,50050,50150").unwrap(); // too few fields
        writeln!(f, "1700000120000,50100,50200,50000,50150,12.0").unwrap();

        let symbol = Symbol::new("BTC", "USDT");
        let events = load_csv(&csv_path, Exchange::Simulated, symbol, dec!(10)).unwrap();

        // Should load 2 valid rows, skip the short one
        assert_eq!(events.len(), 2, "Should skip line with < 6 fields");

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn test_load_csv_empty_file() {
        let dir = std::env::temp_dir().join("mm_test_csv_empty");
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("test_empty.csv");

        let f = std::fs::File::create(&csv_path).unwrap();
        drop(f);

        let symbol = Symbol::new("BTC", "USDT");
        let events = load_csv(&csv_path, Exchange::Simulated, symbol, dec!(10)).unwrap();

        assert_eq!(events.len(), 0, "Empty file should produce no events");

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn test_load_csv_events_have_correct_exchange_and_symbol() {
        let dir = std::env::temp_dir().join("mm_test_csv_fields");
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("test_fields.csv");

        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "timestamp,open,high,low,close,volume").unwrap();
        writeln!(f, "1700000000000,50000,50100,49900,50050,10.5").unwrap();

        let symbol = Symbol::new("ETH", "BTC");
        let events = load_csv(&csv_path, Exchange::Binance, symbol.clone(), dec!(5)).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].exchange(), Exchange::Binance);
        assert_eq!(*events[0].symbol(), symbol);

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn test_load_csv_nonexistent_file_returns_error() {
        let result = load_csv(
            std::path::Path::new("/tmp/nonexistent_file_12345.csv"),
            Exchange::Simulated,
            Symbol::new("BTC", "USDT"),
            dec!(10),
        );
        assert!(result.is_err(), "Loading nonexistent file should error");
    }

    #[test]
    fn test_klines_to_events_multiple() {
        let klines = vec![
            BinanceKline {
                open_time: 1700000000000,
                open: "50000".into(),
                high: "50100".into(),
                low: "49900".into(),
                close: "50050".into(),
                volume: "100".into(),
                close_time: 1700000059999,
            },
            BinanceKline {
                open_time: 1700000060000,
                open: "50050".into(),
                high: "50200".into(),
                low: "49950".into(),
                close: "50150".into(),
                volume: "120".into(),
                close_time: 1700000119999,
            },
            BinanceKline {
                open_time: 1700000120000,
                open: "50150".into(),
                high: "50300".into(),
                low: "50000".into(),
                close: "50250".into(),
                volume: "80".into(),
                close_time: 1700000179999,
            },
        ];

        let events = klines_to_events(&klines, Symbol::new("BTC", "USDT"), dec!(10));
        assert_eq!(events.len(), 3);

        // Events should be in chronological order (same order as klines)
        for i in 1..events.len() {
            assert!(
                events[i].timestamp() >= events[i - 1].timestamp(),
                "Events should be in chronological order"
            );
        }

        // All events should be for the Binance exchange
        for event in &events {
            assert_eq!(event.exchange(), Exchange::Binance);
        }
    }

    #[test]
    fn test_agg_trades_multiple_with_ordering() {
        let trades = vec![
            BinanceAggTrade {
                agg_trade_id: 1,
                price: "50000".into(),
                quantity: "0.1".into(),
                timestamp: 1700000000000,
                is_buyer_maker: false,
            },
            BinanceAggTrade {
                agg_trade_id: 2,
                price: "50010".into(),
                quantity: "0.2".into(),
                timestamp: 1700000001000,
                is_buyer_maker: true,
            },
            BinanceAggTrade {
                agg_trade_id: 3,
                price: "49990".into(),
                quantity: "0.15".into(),
                timestamp: 1700000002000,
                is_buyer_maker: false,
            },
        ];

        let events = agg_trades_to_events(&trades, Symbol::new("BTC", "USDT"));
        assert_eq!(events.len(), 3);

        // Verify chronological order
        for i in 1..events.len() {
            assert!(events[i].timestamp() >= events[i - 1].timestamp());
        }

        // Verify individual trade details
        match &events[1] {
            MarketEvent::TradeEvent {
                price,
                quantity,
                side,
                trade_id,
                ..
            } => {
                assert_eq!(*price, dec!(50010));
                assert_eq!(*quantity, dec!(0.2));
                assert_eq!(*side, Side::Sell); // is_buyer_maker = true
                assert_eq!(trade_id, "2");
            }
            _ => panic!("expected TradeEvent"),
        }
    }

    #[test]
    fn test_cache_path_format() {
        let path = cache_path("binance", "BTCUSDT", "1m", 1000, 2000);
        assert_eq!(
            path,
            PathBuf::from("data/cache/binance_btcusdt_1m_1000_2000.json")
        );
    }

    #[test]
    fn test_ohlcv_spread_math() {
        // Verify the spread calculation for a known close price and spread_bps.
        let row = OhlcvRow {
            timestamp: 1700000000,
            open: dec!(10000),
            high: dec!(10100),
            low: dec!(9900),
            close: dec!(10000),
            volume: dec!(600),
        };

        let event = ohlcv_to_market_event(
            &row,
            Exchange::Simulated,
            Symbol::new("BTC", "USDT"),
            dec!(200), // 200 bps = 2% total spread
            0,
        );

        match event {
            MarketEvent::OrderBookSnapshot { bids, asks, .. } => {
                // half_spread = 10000 * 200 / 10000 / 2 = 100
                // best_bid = 10000 - 100 = 9900
                // best_ask = 10000 + 100 = 10100
                assert_eq!(bids[0].price, dec!(9900));
                assert_eq!(asks[0].price, dec!(10100));
            }
            _ => panic!("expected OrderBookSnapshot"),
        }
    }
}
