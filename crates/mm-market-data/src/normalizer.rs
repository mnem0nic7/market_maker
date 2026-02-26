//! Symbol normalization for each supported exchange.
//!
//! Every exchange uses its own conventions for representing trading pairs.
//! This module provides bidirectional mapping between the canonical
//! [`Symbol`] type (e.g. `BTC/USDT`) and the exchange-native string
//! representation.

use std::collections::HashMap;
use std::sync::LazyLock;

use mm_core::types::{Exchange, Symbol};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Convert a canonical [`Symbol`] into the exchange-native string.
///
/// # Examples
/// ```ignore
/// let sym = Symbol::new("BTC", "USDT");
/// assert_eq!(to_exchange_symbol(Exchange::Binance, &sym), "BTCUSDT");
/// assert_eq!(to_exchange_symbol(Exchange::Coinbase, &sym), "BTC-USDT");
/// ```
pub fn to_exchange_symbol(exchange: Exchange, symbol: &Symbol) -> String {
    match exchange {
        Exchange::Binance => binance_to_exchange(symbol),
        Exchange::Coinbase => coinbase_to_exchange(symbol),
        Exchange::Kraken => kraken_to_exchange(symbol),
        Exchange::Alpaca => alpaca_to_exchange(symbol),
        Exchange::InteractiveBrokers => ib_to_exchange(symbol),
        Exchange::Uniswap => uniswap_to_exchange(symbol),
        Exchange::Raydium => raydium_to_exchange(symbol),
        Exchange::Simulated => format!("{}/{}", symbol.base, symbol.quote),
    }
}

/// Convert an exchange-native symbol string into a canonical [`Symbol`].
///
/// Returns `None` if the string cannot be parsed for the given exchange.
pub fn from_exchange_symbol(exchange: Exchange, raw: &str) -> Option<Symbol> {
    match exchange {
        Exchange::Binance => binance_from_exchange(raw),
        Exchange::Coinbase => coinbase_from_exchange(raw),
        Exchange::Kraken => kraken_from_exchange(raw),
        Exchange::Alpaca => alpaca_from_exchange(raw),
        Exchange::InteractiveBrokers => ib_from_exchange(raw),
        Exchange::Uniswap => uniswap_from_exchange(raw),
        Exchange::Raydium => raydium_from_exchange(raw),
        Exchange::Simulated => {
            let parts: Vec<&str> = raw.splitn(2, '/').collect();
            if parts.len() == 2 {
                Some(Symbol::new(parts[0], parts[1]))
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binance — concatenated, no separator: "BTCUSDT"
// ---------------------------------------------------------------------------

/// Well-known quote assets on Binance (checked in order — longest first).
const BINANCE_QUOTES: &[&str] = &[
    "BUSD", "USDT", "USDC", "TUSD", "FDUSD", "BKRW", "IDRT", "BIDR", "BVND", "DOGE", "SHIB", "EUR",
    "GBP", "AUD", "BRL", "TRY", "PLN", "RON", "ARS", "UAH", "NGN", "BTC", "ETH", "BNB", "XRP",
    "SOL", "DAI", "USD", "VAI", "UST",
];

fn binance_to_exchange(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base, symbol.quote)
}

fn binance_from_exchange(raw: &str) -> Option<Symbol> {
    let upper = raw.to_uppercase();
    for quote in BINANCE_QUOTES {
        if upper.ends_with(quote) {
            let base = &upper[..upper.len() - quote.len()];
            if !base.is_empty() {
                return Some(Symbol::new(base, *quote));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Coinbase — dash-separated: "BTC-USD"
// ---------------------------------------------------------------------------

fn coinbase_to_exchange(symbol: &Symbol) -> String {
    format!("{}-{}", symbol.base, symbol.quote)
}

fn coinbase_from_exchange(raw: &str) -> Option<Symbol> {
    let parts: Vec<&str> = raw.splitn(2, '-').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Some(Symbol::new(parts[0], parts[1]))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Kraken — non-trivial prefix mapping
//
// Kraken uses X/Z prefixes for certain currencies:
//   XBT  -> BTC          XXBT  -> BTC
//   ZUSD -> USD          ZEUR  -> EUR
//   XETH -> ETH          XLTC  -> LTC
//   XXRP -> XRP          XXLM  -> XLM
//   etc.
//
// For the v2 WebSocket API, Kraken also accepts "BTC/USD" style, but the
// REST API and some WS messages still emit the legacy prefixed form.
// ---------------------------------------------------------------------------

/// Mapping from Kraken-native prefixed identifiers to canonical names.
static KRAKEN_TO_CANONICAL: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("XBT", "BTC");
    m.insert("XXBT", "BTC");
    m.insert("XETH", "ETH");
    m.insert("XLTC", "LTC");
    m.insert("XXRP", "XRP");
    m.insert("XXLM", "XLM");
    m.insert("XDOGE", "DOGE");
    m.insert("XREP", "REP");
    m.insert("XMLN", "MLN");
    m.insert("XZEC", "ZEC");
    m.insert("XXMR", "XMR");
    m.insert("XETC", "ETC");
    m.insert("ZUSD", "USD");
    m.insert("ZEUR", "EUR");
    m.insert("ZGBP", "GBP");
    m.insert("ZCAD", "CAD");
    m.insert("ZJPY", "JPY");
    m.insert("ZAUD", "AUD");
    m.insert("ZCHF", "CHF");
    m
});

/// Reverse mapping: canonical -> Kraken v2 symbol.
///
/// Kraken's v2 WebSocket API and modern REST endpoints accept canonical names
/// (BTC, ETH, USD, etc.) directly — no XBT/ZUSD legacy prefixes needed.
/// We keep this map so unknown assets pass through unchanged, but BTC is NOT
/// mapped to XBT.
static CANONICAL_TO_KRAKEN: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("BTC", "BTC");
    m.insert("ETH", "ETH");
    m.insert("LTC", "LTC");
    m.insert("XRP", "XRP");
    m.insert("XLM", "XLM");
    m.insert("DOGE", "DOGE");
    m.insert("REP", "REP");
    m.insert("MLN", "MLN");
    m.insert("ZEC", "ZEC");
    m.insert("XMR", "XMR");
    m.insert("ETC", "ETC");
    m.insert("USD", "USD");
    m.insert("EUR", "EUR");
    m.insert("GBP", "GBP");
    m.insert("CAD", "CAD");
    m.insert("JPY", "JPY");
    m.insert("AUD", "AUD");
    m.insert("CHF", "CHF");
    m
});

fn kraken_to_exchange(symbol: &Symbol) -> String {
    // Kraken v2 WS API accepts "BTC/USD" style, so we use the canonical
    // representation with the Kraken-specific base mapping.
    let base = CANONICAL_TO_KRAKEN
        .get(symbol.base.as_str())
        .copied()
        .unwrap_or(symbol.base.as_str());
    let quote = CANONICAL_TO_KRAKEN
        .get(symbol.quote.as_str())
        .copied()
        .unwrap_or(symbol.quote.as_str());
    format!("{}/{}", base, quote)
}

fn kraken_from_exchange(raw: &str) -> Option<Symbol> {
    // Kraken v2 WS sends "BTC/USD" or "XBT/USD" style with a slash.
    if let Some((base_raw, quote_raw)) = raw.split_once('/') {
        let base = normalize_kraken_asset(base_raw.trim());
        let quote = normalize_kraken_asset(quote_raw.trim());
        return Some(Symbol::new(base, quote));
    }

    // Legacy concatenated format: "XXBTZUSD", "XETHZEUR"
    // Try to split by looking for known quote suffixes.
    let upper = raw.to_uppercase();
    for (k_quote, canonical_quote) in KRAKEN_TO_CANONICAL.iter() {
        // Only try assets that look like quote currencies (start with Z or are fiat-like).
        let is_quote = k_quote.starts_with('Z')
            || ["USD", "EUR", "GBP", "CAD", "JPY", "AUD", "CHF"].contains(canonical_quote);
        if is_quote && upper.ends_with(k_quote) {
            let base_part = &upper[..upper.len() - k_quote.len()];
            if !base_part.is_empty() {
                let base = normalize_kraken_asset(base_part);
                return Some(Symbol::new(base, *canonical_quote));
            }
        }
    }

    None
}

/// Map a single Kraken asset identifier to its canonical form.
fn normalize_kraken_asset(asset: &str) -> String {
    let upper = asset.to_uppercase();
    if let Some(canonical) = KRAKEN_TO_CANONICAL.get(upper.as_str()) {
        return canonical.to_string();
    }
    // Strip a single leading X/Z prefix if the result is >= 3 chars and maps
    // to something recognizable.
    if upper.len() > 3 && (upper.starts_with('X') || upper.starts_with('Z')) {
        let stripped = &upper[1..];
        if let Some(canonical) = KRAKEN_TO_CANONICAL.get(stripped) {
            return canonical.to_string();
        }
        // Even if not in the table, the stripped form is likely correct.
        return stripped.to_string();
    }
    upper
}

// ---------------------------------------------------------------------------
// Alpaca — plain ticker symbol: "AAPL"
// ---------------------------------------------------------------------------

fn alpaca_to_exchange(symbol: &Symbol) -> String {
    // Alpaca uses just the base ticker, e.g., "AAPL"
    symbol.base.to_uppercase()
}

fn alpaca_from_exchange(raw: &str) -> Option<Symbol> {
    let ticker = raw.trim().to_uppercase();
    if ticker.is_empty() {
        return None;
    }
    // Alpaca equities are quoted in USD.
    Some(Symbol::new(ticker, "USD"))
}

// ---------------------------------------------------------------------------
// Interactive Brokers — conid-based, but we use ticker for normalization
// ---------------------------------------------------------------------------

fn ib_to_exchange(symbol: &Symbol) -> String {
    // IB symbol normalization uses the base ticker.
    // Actual conid resolution happens in the connector config.
    symbol.base.to_uppercase()
}

fn ib_from_exchange(raw: &str) -> Option<Symbol> {
    let ticker = raw.trim().to_uppercase();
    if ticker.is_empty() {
        return None;
    }
    Some(Symbol::new(ticker, "USD"))
}

// ---------------------------------------------------------------------------
// Uniswap — token pair with slash: "WETH/USDC"
// ---------------------------------------------------------------------------

fn uniswap_to_exchange(symbol: &Symbol) -> String {
    format!(
        "{}/{}",
        symbol.base.to_uppercase(),
        symbol.quote.to_uppercase()
    )
}

fn uniswap_from_exchange(raw: &str) -> Option<Symbol> {
    let parts: Vec<&str> = raw.splitn(2, '/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Some(Symbol::new(
            parts[0].trim().to_uppercase(),
            parts[1].trim().to_uppercase(),
        ))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Raydium — token pair with slash: "SOL/USDC"
// ---------------------------------------------------------------------------

fn raydium_to_exchange(symbol: &Symbol) -> String {
    format!(
        "{}/{}",
        symbol.base.to_uppercase(),
        symbol.quote.to_uppercase()
    )
}

fn raydium_from_exchange(raw: &str) -> Option<Symbol> {
    let parts: Vec<&str> = raw.splitn(2, '/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Some(Symbol::new(
            parts[0].trim().to_uppercase(),
            parts[1].trim().to_uppercase(),
        ))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Binance ---------------------------------------------------------------

    #[test]
    fn binance_roundtrip_btcusdt() {
        let sym = Symbol::new("BTC", "USDT");
        let raw = to_exchange_symbol(Exchange::Binance, &sym);
        assert_eq!(raw, "BTCUSDT");
        let back = from_exchange_symbol(Exchange::Binance, &raw).unwrap();
        assert_eq!(back, sym);
    }

    #[test]
    fn binance_roundtrip_ethbtc() {
        let sym = Symbol::new("ETH", "BTC");
        let raw = to_exchange_symbol(Exchange::Binance, &sym);
        assert_eq!(raw, "ETHBTC");
        let back = from_exchange_symbol(Exchange::Binance, &raw).unwrap();
        assert_eq!(back, sym);
    }

    #[test]
    fn binance_from_lowercase() {
        let sym = from_exchange_symbol(Exchange::Binance, "solusdt").unwrap();
        assert_eq!(sym.base, "SOL");
        assert_eq!(sym.quote, "USDT");
    }

    // Coinbase --------------------------------------------------------------

    #[test]
    fn coinbase_roundtrip_btcusd() {
        let sym = Symbol::new("BTC", "USD");
        let raw = to_exchange_symbol(Exchange::Coinbase, &sym);
        assert_eq!(raw, "BTC-USD");
        let back = from_exchange_symbol(Exchange::Coinbase, &raw).unwrap();
        assert_eq!(back, sym);
    }

    #[test]
    fn coinbase_roundtrip_ethusd() {
        let sym = Symbol::new("ETH", "USD");
        let raw = to_exchange_symbol(Exchange::Coinbase, &sym);
        assert_eq!(raw, "ETH-USD");
        let back = from_exchange_symbol(Exchange::Coinbase, &raw).unwrap();
        assert_eq!(back, sym);
    }

    // Kraken ----------------------------------------------------------------

    #[test]
    fn kraken_to_exchange_btc_usd() {
        let sym = Symbol::new("BTC", "USD");
        let raw = to_exchange_symbol(Exchange::Kraken, &sym);
        assert_eq!(raw, "BTC/USD");
    }

    #[test]
    fn kraken_from_exchange_slash_form() {
        let sym = from_exchange_symbol(Exchange::Kraken, "XBT/USD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn kraken_from_exchange_legacy() {
        let sym = from_exchange_symbol(Exchange::Kraken, "XXBTZUSD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }

    #[test]
    fn kraken_from_exchange_eth_eur() {
        let sym = from_exchange_symbol(Exchange::Kraken, "XETHZEUR").unwrap();
        assert_eq!(sym.base, "ETH");
        assert_eq!(sym.quote, "EUR");
    }

    #[test]
    fn kraken_from_exchange_v2_btc_usd() {
        let sym = from_exchange_symbol(Exchange::Kraken, "BTC/USD").unwrap();
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USD");
    }
}
