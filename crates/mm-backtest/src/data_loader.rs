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
}
