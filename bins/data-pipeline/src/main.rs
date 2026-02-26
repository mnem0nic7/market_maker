//! Data pipeline service binary.
//!
//! Connects to NATS JetStream and TimescaleDB, runs database migrations, then
//! consumes events from the stream and persists them to time-series tables.
//! Also starts the tamper-evident AuditLogger for hash-chained audit trails.

use anyhow::{Context, Result};
use futures::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{error, info, warn};

use mm_core::{Envelope, Event};
use mm_data_pipeline::{
    AuditLogger, NatsConfig, NatsEventPublisher, NatsEventSubscriber, TimescaleStore,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PipelineConfig {
    /// NATS server URL (e.g. "nats://127.0.0.1:4222").
    nats_url: String,
    /// TimescaleDB connection URL (e.g. "postgresql://user:pass@localhost:5432/market_maker").
    database_url: String,
    /// Path to the append-only JSONL audit file.
    audit_file_path: String,
    /// Name of the NATS JetStream stream (default: "EVENTS").
    #[serde(default = "default_stream_name")]
    stream_name: String,
}

fn default_stream_name() -> String {
    "EVENTS".to_string()
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            database_url: "postgresql://postgres:postgres@localhost:5432/market_maker".to_string(),
            audit_file_path: "data/audit.jsonl".to_string(),
            stream_name: default_stream_name(),
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing (structured JSON logging, respects RUST_LOG).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .json()
        .init();

    info!("data-pipeline starting");

    // Load configuration from environment / config files.
    let cfg = load_config().context("failed to load configuration")?;
    info!(
        nats_url = %cfg.nats_url,
        database_url = mask_password(&cfg.database_url),
        audit_file = %cfg.audit_file_path,
        stream = %cfg.stream_name,
        "configuration loaded"
    );

    // ------------------------------------------------------------------
    // 1. Connect to TimescaleDB and run migrations
    // ------------------------------------------------------------------
    let store = TimescaleStore::connect(&cfg.database_url)
        .await
        .context("failed to connect to TimescaleDB")?;
    info!("connected to TimescaleDB");

    store
        .run_migrations()
        .await
        .context("failed to run TimescaleDB migrations")?;
    info!("database migrations complete");

    // ------------------------------------------------------------------
    // 2. Ensure the NATS JetStream stream exists (via publisher connect)
    // ------------------------------------------------------------------
    let nats_config = NatsConfig {
        url: cfg.nats_url.clone(),
        stream_name: cfg.stream_name.clone(),
        max_age_secs: 0,
        max_bytes: 0,
    };

    // Publisher connect creates/binds the stream so the subscriber can find it.
    let _publisher = NatsEventPublisher::connect(&nats_config)
        .await
        .context("failed to connect NATS publisher (stream setup)")?;
    info!("NATS JetStream stream ensured");

    // ------------------------------------------------------------------
    // 3. Connect the subscriber for the AuditLogger
    // ------------------------------------------------------------------
    let audit_subscriber = NatsEventSubscriber::connect(&nats_config)
        .await
        .context("failed to connect NATS subscriber for audit logger")?;

    // Ensure the audit file directory exists.
    if let Some(parent) = std::path::Path::new(&cfg.audit_file_path).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("failed to create audit file directory")?;
    }

    // ------------------------------------------------------------------
    // 4. Start the AuditLogger (background task)
    // ------------------------------------------------------------------
    let audit_logger = AuditLogger::start(audit_subscriber, store.clone(), &cfg.audit_file_path)
        .await
        .context("failed to start audit logger")?;
    info!(
        audit_file = %cfg.audit_file_path,
        "audit logger started"
    );

    // ------------------------------------------------------------------
    // 5. Start additional consumer for structured persistence
    // ------------------------------------------------------------------
    let persist_subscriber = NatsEventSubscriber::connect(&nats_config)
        .await
        .context("failed to connect NATS subscriber for persistence consumer")?;

    let persist_store = store.clone();
    let persist_handle = tokio::spawn(async move {
        if let Err(e) = run_persistence_consumer(persist_subscriber, persist_store).await {
            error!(error = %e, "persistence consumer terminated with error");
        }
    });

    info!("persistence consumer started — subscribing to events.>");

    // ------------------------------------------------------------------
    // 6. Wait for shutdown signal
    // ------------------------------------------------------------------
    info!("data-pipeline is running. Press Ctrl+C to stop.");
    shutdown_signal().await;
    info!("shutdown signal received, stopping...");

    // Abort the persistence consumer.
    persist_handle.abort();
    let _ = persist_handle.await;

    // Shut down the audit logger gracefully.
    let stats = audit_logger.stats().clone();
    audit_logger.shutdown().await;
    info!(
        events_processed = stats.events_processed(),
        errors = stats.errors(),
        "data-pipeline shut down"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Persistence consumer
// ---------------------------------------------------------------------------

/// Subscribes to all events and routes each one to the appropriate typed
/// TimescaleDB table based on the event payload variant.
async fn run_persistence_consumer(
    subscriber: NatsEventSubscriber,
    store: TimescaleStore,
) -> Result<()> {
    let event_stream = subscriber
        .subscribe("events.>")
        .await
        .context("failed to subscribe to events.>")?;

    let mut event_stream = Box::pin(event_stream);
    let mut count: u64 = 0;

    while let Some(envelope) = event_stream.next().await {
        count += 1;

        if let Err(e) = persist_event(&store, &envelope).await {
            warn!(
                error = %e,
                event_id = %envelope.event_id,
                "failed to persist event to typed table (will still appear in events table via audit)"
            );
        }

        if count.is_multiple_of(1000) {
            info!(events_persisted = count, "persistence consumer checkpoint");
        }
    }

    info!(total = count, "persistence consumer event stream ended");
    Ok(())
}

/// Route a single envelope to the appropriate typed table(s).
async fn persist_event(store: &TimescaleStore, envelope: &Envelope) -> Result<()> {
    let ts = envelope.timestamp;

    match &envelope.payload {
        // -- Market data ticks --
        Event::MarketDataUpdate {
            exchange,
            symbol,
            best_bid,
            best_ask,
            mid_price,
            spread_bps,
        } => {
            store
                .insert_market_tick(
                    ts,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    *best_bid,
                    *best_ask,
                    *mid_price,
                    *spread_bps,
                    Decimal::ZERO, // volume not available on this event type
                )
                .await
                .context("insert_market_tick")?;
        }

        // -- Order submitted (insert new order row) --
        Event::OrderSubmitted {
            client_order_id,
            strategy_id,
            exchange,
            symbol,
            side,
            order_type,
            price,
            quantity,
            ..
        } => {
            store
                .upsert_order(
                    client_order_id,
                    None,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    &side.to_string(),
                    &order_type.to_string(),
                    "Submitted",
                    *price,
                    *quantity,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    strategy_id,
                    ts,
                    ts,
                )
                .await
                .context("upsert_order (submitted)")?;
        }

        // -- Order acknowledged --
        Event::OrderAcknowledged {
            client_order_id,
            exchange_order_id,
            exchange,
            symbol,
        } => {
            store
                .upsert_order(
                    client_order_id,
                    Some(exchange_order_id.as_str()),
                    &exchange.to_string(),
                    &symbol.canonical(),
                    "",    // side unknown here, preserved from original insert
                    "",    // order_type preserved from original insert
                    "New", // acknowledged = New on exchange
                    None,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    "",
                    ts,
                    ts,
                )
                .await
                .context("upsert_order (acknowledged)")?;
        }

        // -- Fills (partial and full) --
        Event::OrderFilled {
            client_order_id,
            exchange_order_id: _,
            exchange,
            symbol,
            fill_price,
            fill_quantity,
            fee,
            fee_currency,
            ..
        }
        | Event::OrderPartiallyFilled {
            client_order_id,
            exchange_order_id: _,
            exchange,
            symbol,
            fill_price,
            fill_quantity,
            fee,
            fee_currency,
            ..
        } => {
            let fill_id = format!("{}-{}", envelope.event_id, client_order_id);
            let side_str = match &envelope.payload {
                Event::OrderFilled { .. } => "fill",
                Event::OrderPartiallyFilled { .. } => "partial_fill",
                _ => "unknown",
            };

            store
                .insert_fill(
                    ts,
                    &fill_id,
                    client_order_id,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    side_str,
                    *fill_price,
                    *fill_quantity,
                    *fee,
                    fee_currency,
                )
                .await
                .context("insert_fill")?;

            // Also insert as a trade record.
            store
                .insert_trade(
                    ts,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    side_str,
                    *fill_price,
                    *fill_quantity,
                    &fill_id,
                )
                .await
                .context("insert_trade")?;
        }

        // -- Order cancelled --
        Event::OrderCancelled {
            client_order_id,
            exchange_order_id: _,
            exchange,
            symbol,
            ..
        } => {
            store
                .upsert_order(
                    client_order_id,
                    None,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    "",
                    "",
                    "Cancelled",
                    None,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    "",
                    ts,
                    ts,
                )
                .await
                .context("upsert_order (cancelled)")?;
        }

        // -- Order rejected --
        Event::OrderRejected {
            client_order_id,
            exchange,
            symbol,
            ..
        } => {
            store
                .upsert_order(
                    client_order_id,
                    None,
                    &exchange.to_string(),
                    &symbol.canonical(),
                    "",
                    "",
                    "Rejected",
                    None,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    "",
                    ts,
                    ts,
                )
                .await
                .context("upsert_order (rejected)")?;
        }

        // -- Position updates --
        Event::PositionUpdated {
            exchange,
            symbol,
            quantity,
            average_entry_price,
            unrealized_pnl,
            realized_pnl,
        } => {
            store
                .upsert_position(
                    &exchange.to_string(),
                    &symbol.canonical(),
                    *quantity,
                    *average_entry_price,
                    *unrealized_pnl,
                    *realized_pnl,
                    ts,
                )
                .await
                .context("upsert_position")?;
        }

        // -- All other events are persisted only to the generic events table
        //    (handled by the audit logger). No additional typed persistence needed.
        Event::RiskLimitBreached { .. }
        | Event::KillSwitchActivated { .. }
        | Event::CircuitBreakerTriggered { .. }
        | Event::StrategySignal { .. }
        | Event::SystemHealthCheck { .. } => {
            // These events are stored in the generic events hypertable by the
            // AuditLogger. No typed table persistence required.
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Configuration loading
// ---------------------------------------------------------------------------

fn load_config() -> Result<PipelineConfig> {
    let settings = config::Config::builder()
        // Start with defaults
        .set_default("nats_url", "nats://127.0.0.1:4222")?
        .set_default(
            "database_url",
            "postgresql://postgres:postgres@localhost:5432/market_maker",
        )?
        .set_default("audit_file_path", "data/audit.jsonl")?
        .set_default("stream_name", "EVENTS")?
        // Layer on config file if present
        .add_source(config::File::with_name("config/data-pipeline").required(false))
        .add_source(config::File::with_name("config/default").required(false))
        // Override with environment variables (prefixed with PIPELINE_)
        .add_source(config::Environment::with_prefix("PIPELINE").separator("_"))
        .build()
        .context("failed to build config")?;

    settings
        .try_deserialize::<PipelineConfig>()
        .context("failed to deserialize config")
}

/// Mask the password in a database URL for safe logging.
fn mask_password(url: &str) -> String {
    // Simple masking: replace password between :password@ with :***@
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            let prefix_end = url[..colon_pos].rfind("//").map(|p| p + 2).unwrap_or(0);
            if colon_pos > prefix_end {
                return format!("{}:***{}", &url[..colon_pos], &url[at_pos..]);
            }
        }
    }
    url.to_string()
}

// ---------------------------------------------------------------------------
// Shutdown signal
// ---------------------------------------------------------------------------

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received SIGINT");
        }
        _ = terminate => {
            info!("received SIGTERM");
        }
    }
}
