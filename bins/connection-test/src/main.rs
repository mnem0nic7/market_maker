//! Lightweight connectivity test for exchange API keys.
//!
//! Loads `.env` via dotenvy, reads exchange credentials directly from
//! `MM_EXCHANGES__<NAME>__API_KEY` / `SECRET_KEY` env vars, then calls
//! `fetch_balances()` — a read-only authenticated REST call — on each.
//! Reports success or failure. No orders are placed.
//!
//! Usage:
//!   cargo run --bin connection-test

use std::env;
use std::time::Instant;

use mm_core::traits::ExecutionConnector;
use mm_execution::{
    AlpacaExecutionConfig, AlpacaExecutionConnector, BinanceExecutionConfig,
    BinanceExecutionConnector, CoinbaseExecutionConfig, CoinbaseExecutionConnector,
    KrakenExecutionConfig, KrakenExecutionConnector,
};

struct ExchangeDef {
    name: &'static str,
    base_url: &'static str,
    ws_url: &'static str,
}

const EXCHANGES: &[ExchangeDef] = &[
    ExchangeDef {
        name: "BINANCE",
        base_url: "https://api.binance.us",
        ws_url: "wss://stream.binance.us:9443",
    },
    ExchangeDef {
        name: "COINBASE",
        base_url: "https://api.coinbase.com",
        ws_url: "wss://advanced-trade-ws.coinbase.com",
    },
    ExchangeDef {
        name: "KRAKEN",
        base_url: "https://api.kraken.com",
        ws_url: "wss://ws-auth.kraken.com/v2",
    },
    ExchangeDef {
        name: "ALPACA",
        base_url: "https://paper-api.alpaca.markets",
        ws_url: "wss://paper-api.alpaca.markets/stream",
    },
];

/// Returns true if the key looks like a real credential (not a placeholder).
fn has_real_key(key: &str) -> bool {
    !key.is_empty() && key != "ROTATE_ME" && !key.starts_with("your-")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("warn")
        .init();

    // Load .env file into process environment (ignore if missing).
    let _ = dotenvy::dotenv();

    println!();
    println!("══════════════════════════════════════════════════════════");
    println!("  Exchange Connection Test");
    println!("══════════════════════════════════════════════════════════");
    println!();

    let mut passed = 0u32;
    let mut failed = 0u32;

    for ex in EXCHANGES {
        let api_key_var = format!("MM_EXCHANGES__{}__API_KEY", ex.name);
        let secret_key_var = format!("MM_EXCHANGES__{}__SECRET_KEY", ex.name);

        let api_key = env::var(&api_key_var).unwrap_or_default();
        let secret_key = env::var(&secret_key_var).unwrap_or_default();

        let display_name = ex.name.to_lowercase();

        if !has_real_key(&api_key) {
            println!("  ⊘  {:<12} no API key configured, skipping", display_name);
            continue;
        }

        let start = Instant::now();

        let outcome: Result<usize, String> = match ex.name {
            "BINANCE" => {
                test_exchange(BinanceExecutionConnector::new(BinanceExecutionConfig {
                    api_key,
                    secret_key,
                    base_url: ex.base_url.into(),
                    ws_url: ex.ws_url.into(),
                }))
                .await
            }
            "COINBASE" => {
                test_exchange(CoinbaseExecutionConnector::new(CoinbaseExecutionConfig {
                    api_key,
                    secret_key,
                    base_url: ex.base_url.into(),
                    ws_url: ex.ws_url.into(),
                }))
                .await
            }
            "KRAKEN" => {
                test_exchange(KrakenExecutionConnector::new(KrakenExecutionConfig {
                    api_key,
                    secret_key,
                    base_url: ex.base_url.into(),
                    ws_url: ex.ws_url.into(),
                }))
                .await
            }
            "ALPACA" => {
                test_exchange(AlpacaExecutionConnector::new(AlpacaExecutionConfig {
                    api_key,
                    secret_key,
                    base_url: ex.base_url.into(),
                    ws_url: ex.ws_url.into(),
                }))
                .await
            }
            _ => continue,
        };

        let elapsed = start.elapsed();

        match outcome {
            Ok(count) => {
                println!(
                    "  ✓  {:<12} OK  ({} assets, {:.0?})",
                    display_name, count, elapsed
                );
                passed += 1;
            }
            Err(e) => {
                let short = if e.len() > 120 {
                    format!("{}...", &e[..120])
                } else {
                    e
                };
                println!("  ✗  {:<12} FAIL", display_name);
                println!("     {}", short);
                failed += 1;
            }
        }
    }

    println!();
    println!("──────────────────────────────────────────────────────────");

    let total = passed + failed;
    if failed == 0 && total > 0 {
        println!("  All {} exchanges connected successfully.", total);
    } else if total == 0 {
        println!("  No exchanges configured with real API keys.");
    } else {
        println!("  {}/{} passed, {} failed.", passed, total, failed);
    }
    println!("══════════════════════════════════════════════════════════");
    println!();

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Call `fetch_balances()` and return the number of non-zero balances.
async fn test_exchange(connector: impl ExecutionConnector) -> Result<usize, String> {
    let balances = connector
        .fetch_balances()
        .await
        .map_err(|e| format!("{}", e))?;
    Ok(balances.len())
}
