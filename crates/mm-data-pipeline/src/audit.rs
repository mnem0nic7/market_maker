//! Tamper-evident audit trail.
//!
//! [`AuditLogger`] consumes events from NATS JetStream and writes every event
//! to both the TimescaleDB `events` hypertable and a local append-only JSON
//! lines file. Each line is hash-chained using SHA-256 so that any retroactive
//! modification of the file can be detected.
//!
//! # File format (one JSON object per line)
//!
//! ```json
//! {"seq":1,"hash":"ab12...","envelope":{...}}
//! ```
//!
//! where `hash = hex(SHA256(previous_hash || envelope_json))`.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt;
use mm_core::Envelope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::event_store::NatsEventSubscriber;
use crate::timescale::{self, TimescaleStore};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single line in the audit log file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLine {
    /// Monotonically increasing sequence number within this file.
    pub seq: u64,
    /// `hex(SHA256(previous_hash_bytes || envelope_json_bytes))`.
    pub hash: String,
    /// The original event envelope (verbatim).
    pub envelope: serde_json::Value,
}

/// Statistics tracked by the audit logger.
#[derive(Debug, Default)]
pub struct AuditStats {
    /// Total events successfully processed (file + DB).
    pub events_processed: AtomicU64,
    /// Total errors encountered (non-fatal; logged and skipped).
    pub errors: AtomicU64,
}

impl AuditStats {
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Retry / buffer constants
// ---------------------------------------------------------------------------

/// Maximum number of retry attempts for audit DB writes.
pub const AUDIT_DB_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff on audit DB retries (500ms).
pub const AUDIT_DB_BASE_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// Default maximum capacity for the in-memory buffer of failed events.
pub const FAILED_EVENT_BUFFER_CAPACITY: usize = 10_000;

// ---------------------------------------------------------------------------
// FailedEventBuffer
// ---------------------------------------------------------------------------

/// A bounded in-memory buffer for events that failed to persist to the
/// database after exhausting retries. These events are re-attempted on the
/// next successful write cycle.
///
/// When the buffer is at capacity, the oldest events are dropped to make
/// room for newer ones (this is a last-resort measure — operators should be
/// alerted when this happens).
pub struct FailedEventBuffer {
    buffer: VecDeque<Envelope>,
    capacity: usize,
    dropped_count: u64,
}

impl FailedEventBuffer {
    /// Create a new buffer with the given maximum capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
            dropped_count: 0,
        }
    }

    /// Push an event into the buffer. If full, the oldest event is dropped.
    pub fn push(&mut self, envelope: Envelope) {
        if self.buffer.len() >= self.capacity {
            self.buffer.pop_front();
            self.dropped_count += 1;
            error!(
                buffer_capacity = self.capacity,
                total_dropped = self.dropped_count,
                "audit failed-event buffer full, dropping oldest event — operator alert required"
            );
        }
        self.buffer.push_back(envelope);
    }

    /// Drain all events currently in the buffer for re-processing.
    pub fn drain_all(&mut self) -> Vec<Envelope> {
        self.buffer.drain(..).collect()
    }

    /// Number of events currently buffered.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Total number of events that were dropped because the buffer was full.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Maximum capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// AuditLogger
// ---------------------------------------------------------------------------

/// Orchestrates the consumption of events and their dual-write to the
/// TimescaleDB events table and a local JSONL audit file.
pub struct AuditLogger {
    stats: Arc<AuditStats>,
    handle: Option<JoinHandle<()>>,
}

impl AuditLogger {
    /// Spawn the audit logger as a background tokio task.
    ///
    /// * `nats_subscriber` -- a connected [`NatsEventSubscriber`].
    /// * `timescale_store` -- a connected [`TimescaleStore`].
    /// * `audit_file_path` -- path to the append-only JSONL file (created if
    ///   missing).
    ///
    /// Returns `Self` which exposes live statistics and a handle to the
    /// background task.
    #[instrument(skip_all)]
    pub async fn start(
        nats_subscriber: NatsEventSubscriber,
        timescale_store: TimescaleStore,
        audit_file_path: impl AsRef<Path>,
    ) -> Result<Self, crate::errors::PipelineError> {
        let path = audit_file_path.as_ref().to_path_buf();
        tracing::Span::current().record("path", tracing::field::display(path.display()));
        let stats = Arc::new(AuditStats::default());
        let stats_inner = Arc::clone(&stats);

        // Determine the last hash and sequence from the existing file so we
        // can continue the chain (single read instead of two).
        let (last_hash, sequence_start) = Self::read_last_state(&path).await;

        // Verify the existing audit file integrity before continuing the chain.
        if sequence_start > 0 {
            match verify_audit_file(&path).await {
                Ok(verified_count) => {
                    info!(verified_count, "audit file integrity verified");
                }
                Err(e) => {
                    error!(error = %e, "audit file integrity check failed");
                    return Err(crate::errors::PipelineError::Internal(format!(
                        "audit file integrity check failed: {}",
                        e
                    )));
                }
            }
        }

        info!(
            sequence_start,
            chain_continues = last_hash.is_some(),
            "audit logger starting"
        );

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::run_loop(
                nats_subscriber,
                timescale_store,
                path,
                last_hash,
                sequence_start,
                stats_inner,
            )
            .await
            {
                error!(error = %e, "audit logger terminated with error");
            }
        });

        Ok(Self {
            stats,
            handle: Some(handle),
        })
    }

    /// Access live statistics.
    pub fn stats(&self) -> &Arc<AuditStats> {
        &self.stats
    }

    /// Consume the logger and await the background task (blocks until the NATS
    /// stream ends or the task is cancelled).
    pub async fn shutdown(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        info!(
            events = self.stats.events_processed(),
            errors = self.stats.errors(),
            "audit logger shut down"
        );
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Main event loop: subscribe to `events.>` and dual-write.
    async fn run_loop(
        subscriber: NatsEventSubscriber,
        store: TimescaleStore,
        path: PathBuf,
        initial_hash: Option<Vec<u8>>,
        start_seq: u64,
        stats: Arc<AuditStats>,
    ) -> Result<(), crate::errors::PipelineError> {
        let event_stream = subscriber.subscribe("events.>").await?;
        // The unfold-based stream is `!Unpin`, so we pin it on the heap.
        let mut event_stream = Box::pin(event_stream);

        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?,
        ));

        let mut prev_hash = initial_hash.unwrap_or_else(|| vec![0u8; 32]);
        let mut seq = start_seq;
        let mut failed_buffer = FailedEventBuffer::new(FAILED_EVENT_BUFFER_CAPACITY);

        while let Some(envelope) = event_stream.next().await {
            seq += 1;
            let received_at = Utc::now();
            let lag_ms = (received_at - envelope.timestamp).num_milliseconds();

            // Serialize envelope to JSON.
            let envelope_json = match serde_json::to_value(&envelope) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "failed to serialize envelope, skipping");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let envelope_bytes = match serde_json::to_vec(&envelope_json) {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "failed to serialize envelope bytes, skipping");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Compute chained hash: SHA256(prev_hash || envelope_json_bytes).
            let current_hash = {
                let mut hasher = Sha256::new();
                hasher.update(&prev_hash);
                hasher.update(&envelope_bytes);
                hasher.finalize().to_vec()
            };
            let hash_hex = hex::encode(&current_hash);

            let audit_line = AuditLine {
                seq,
                hash: hash_hex,
                envelope: envelope_json,
            };

            // Write to file.
            let line = match serde_json::to_string(&audit_line) {
                Ok(mut s) => {
                    s.push('\n');
                    s
                }
                Err(e) => {
                    warn!(error = %e, seq, "failed to serialize audit line");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            {
                let mut f = file.lock().await;
                if let Err(e) = f.write_all(line.as_bytes()).await {
                    error!(error = %e, seq, "failed to write audit line to file");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    // Continue to try DB write.
                }
                // Flush to ensure durability.
                if let Err(e) = f.flush().await {
                    warn!(error = %e, seq, "flush failed");
                }
            }

            // Write to TimescaleDB with retry.
            let db_ok = Self::persist_with_retry(&store, &envelope, &stats).await;

            if db_ok {
                // DB is healthy — attempt to flush any previously buffered events.
                if !failed_buffer.is_empty() {
                    let buffered = failed_buffer.drain_all();
                    let count = buffered.len();
                    info!(
                        buffered_count = count,
                        "attempting to flush buffered failed events to TimescaleDB"
                    );
                    let mut re_failed = 0u64;
                    for buffered_envelope in buffered {
                        if !Self::persist_with_retry(&store, &buffered_envelope, &stats).await {
                            re_failed += 1;
                            failed_buffer.push(buffered_envelope);
                        }
                    }
                    if re_failed > 0 {
                        warn!(
                            re_failed,
                            remaining = failed_buffer.len(),
                            "some buffered events still failed to persist"
                        );
                    } else {
                        info!(count, "all buffered events flushed successfully");
                    }
                }
            } else {
                // DB write failed after retries — buffer the event.
                failed_buffer.push(envelope.clone());
                warn!(
                    buffered = failed_buffer.len(),
                    dropped = failed_buffer.dropped_count(),
                    "event buffered for later retry"
                );
            }

            prev_hash = current_hash;
            stats.events_processed.fetch_add(1, Ordering::Relaxed);

            if seq.is_multiple_of(1000) {
                info!(
                    seq,
                    events = stats.events_processed(),
                    errors = stats.errors(),
                    buffered = failed_buffer.len(),
                    dropped = failed_buffer.dropped_count(),
                    lag_ms,
                    "audit logger checkpoint"
                );
            } else {
                debug!(seq, lag_ms, event_id = %envelope.event_id, "event audited");
            }
        }

        info!(seq, "event stream ended, audit logger stopping");
        Ok(())
    }

    /// Persist an envelope to TimescaleDB with retry on transient errors.
    ///
    /// Returns `true` if the write succeeded (possibly after retries), `false`
    /// if it failed after exhausting all retry attempts.
    async fn persist_with_retry(
        store: &TimescaleStore,
        envelope: &Envelope,
        stats: &Arc<AuditStats>,
    ) -> bool {
        let payload = match serde_json::to_value(&envelope.payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to serialize envelope payload for DB");
                stats.errors.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };

        let event_type = payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let pool = store.pool();
        let result = timescale::retry_db_operation(
            "audit_persist_envelope",
            AUDIT_DB_MAX_RETRIES,
            AUDIT_DB_BASE_DELAY,
            || {
                let payload = payload.clone();
                let event_type = event_type.clone();
                async move {
                    sqlx::query(
                        r#"
                        INSERT INTO events (time, event_id, correlation_id, source, event_type, payload)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        "#,
                    )
                    .bind(envelope.timestamp)
                    .bind(&envelope.event_id)
                    .bind(&envelope.correlation_id)
                    .bind(&envelope.source)
                    .bind(&event_type)
                    .bind(&payload)
                    .execute(pool)
                    .await
                    .map(|_| ())
                }
            },
        )
        .await;

        match result {
            Ok(()) => true,
            Err(e) => {
                error!(
                    error = %e,
                    event_id = %envelope.event_id,
                    "failed to persist envelope to TimescaleDB after retries"
                );
                stats.errors.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Read the last hash and sequence number from the existing audit file in
    /// a single pass, avoiding the cost of reading the file twice.
    async fn read_last_state(path: &Path) -> (Option<Vec<u8>>, u64) {
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(c) => c,
            Err(_) => return (None, 0),
        };
        let last_line = contents.lines().rev().find(|l| !l.trim().is_empty());
        match last_line {
            Some(line) => match serde_json::from_str::<AuditLine>(line) {
                Ok(audit_line) => {
                    let hash = hex::decode(&audit_line.hash).ok();
                    (hash, audit_line.seq)
                }
                Err(_) => (None, 0),
            },
            None => (None, 0),
        }
    }
}

// ---------------------------------------------------------------------------
// Verification helper
// ---------------------------------------------------------------------------

/// Verify the integrity of an audit file by replaying the hash chain.
///
/// Returns `Ok(n)` with the number of verified lines, or an error describing
/// the first line where the chain breaks.
pub async fn verify_audit_file(path: impl AsRef<Path>) -> Result<u64, String> {
    let contents = tokio::fs::read_to_string(path.as_ref())
        .await
        .map_err(|e| format!("failed to read audit file: {e}"))?;

    let mut prev_hash: Vec<u8> = vec![0u8; 32];
    let mut count: u64 = 0;

    for (line_no, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let audit_line: AuditLine = serde_json::from_str(line)
            .map_err(|e| format!("line {}: parse error: {e}", line_no + 1))?;

        let envelope_bytes = serde_json::to_vec(&audit_line.envelope).map_err(|e| {
            format!(
                "line {}: envelope re-serialization failed: {e}",
                line_no + 1
            )
        })?;

        let expected_hash = {
            let mut hasher = Sha256::new();
            hasher.update(&prev_hash);
            hasher.update(&envelope_bytes);
            hex::encode(hasher.finalize())
        };

        if audit_line.hash != expected_hash {
            return Err(format!(
                "line {}: hash mismatch (expected {}, got {})",
                line_no + 1,
                expected_hash,
                audit_line.hash
            ));
        }

        prev_hash = hex::decode(&audit_line.hash)
            .map_err(|e| format!("line {}: bad hex hash: {e}", line_no + 1))?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_chain_deterministic() {
        let prev = vec![0u8; 32];
        let data = b"test-event-payload";

        let mut hasher = Sha256::new();
        hasher.update(&prev);
        hasher.update(data);
        let h1 = hex::encode(hasher.finalize());

        let mut hasher2 = Sha256::new();
        hasher2.update(&prev);
        hasher2.update(data);
        let h2 = hex::encode(hasher2.finalize());

        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // 32 bytes -> 64 hex chars
    }

    #[test]
    fn audit_line_serialization() {
        let line = AuditLine {
            seq: 1,
            hash: "aabbcc".to_string(),
            envelope: serde_json::json!({"type": "test"}),
        };
        let json = serde_json::to_string(&line).unwrap();
        let back: AuditLine = serde_json::from_str(&json).unwrap();
        assert_eq!(back.seq, 1);
        assert_eq!(back.hash, "aabbcc");
    }

    // -----------------------------------------------------------------------
    // Retry / buffer constants
    // -----------------------------------------------------------------------

    #[test]
    fn audit_retry_constants_are_reasonable() {
        assert!(
            AUDIT_DB_MAX_RETRIES >= 1 && AUDIT_DB_MAX_RETRIES <= 10,
            "max retries should be between 1 and 10"
        );
        assert!(
            AUDIT_DB_BASE_DELAY >= std::time::Duration::from_millis(100)
                && AUDIT_DB_BASE_DELAY <= std::time::Duration::from_secs(10),
            "base delay should be between 100ms and 10s"
        );
    }

    #[test]
    fn buffer_capacity_constant_is_reasonable() {
        assert!(
            FAILED_EVENT_BUFFER_CAPACITY >= 100 && FAILED_EVENT_BUFFER_CAPACITY <= 1_000_000,
            "buffer capacity should be between 100 and 1,000,000"
        );
    }

    // -----------------------------------------------------------------------
    // FailedEventBuffer
    // -----------------------------------------------------------------------

    /// Create a minimal test Envelope for buffer tests.
    fn test_envelope(id: &str) -> Envelope {
        Envelope {
            event_id: id.to_string(),
            correlation_id: "corr-1".to_string(),
            causation_id: None,
            source: "test".to_string(),
            timestamp: Utc::now(),
            payload: mm_core::Event::SystemHealthCheck {
                component: "test".to_string(),
                status: "ok".to_string(),
                details: serde_json::json!({"id": id}),
            },
        }
    }

    #[test]
    fn buffer_push_and_drain() {
        let mut buf = FailedEventBuffer::new(10);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);

        buf.push(test_envelope("e1"));
        buf.push(test_envelope("e2"));
        assert_eq!(buf.len(), 2);

        let drained = buf.drain_all();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].event_id, "e1");
        assert_eq!(drained[1].event_id, "e2");
        assert!(buf.is_empty());
    }

    #[test]
    fn buffer_drops_oldest_when_full() {
        let mut buf = FailedEventBuffer::new(3);

        buf.push(test_envelope("e1"));
        buf.push(test_envelope("e2"));
        buf.push(test_envelope("e3"));
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.dropped_count(), 0);

        // This should drop e1 (oldest).
        buf.push(test_envelope("e4"));
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.dropped_count(), 1);

        let drained = buf.drain_all();
        assert_eq!(drained[0].event_id, "e2");
        assert_eq!(drained[1].event_id, "e3");
        assert_eq!(drained[2].event_id, "e4");
    }

    #[test]
    fn buffer_drops_multiple_oldest() {
        let mut buf = FailedEventBuffer::new(2);

        buf.push(test_envelope("e1"));
        buf.push(test_envelope("e2"));
        buf.push(test_envelope("e3"));
        buf.push(test_envelope("e4"));
        buf.push(test_envelope("e5"));

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.dropped_count(), 3);

        let drained = buf.drain_all();
        assert_eq!(drained[0].event_id, "e4");
        assert_eq!(drained[1].event_id, "e5");
    }

    #[test]
    fn buffer_capacity_one() {
        let mut buf = FailedEventBuffer::new(1);

        buf.push(test_envelope("e1"));
        assert_eq!(buf.len(), 1);

        buf.push(test_envelope("e2"));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.dropped_count(), 1);

        let drained = buf.drain_all();
        assert_eq!(drained[0].event_id, "e2");
    }

    #[test]
    fn buffer_reports_capacity() {
        let buf = FailedEventBuffer::new(42);
        assert_eq!(buf.capacity(), 42);
    }

    #[test]
    fn drain_on_empty_buffer_returns_empty_vec() {
        let mut buf = FailedEventBuffer::new(10);
        let drained = buf.drain_all();
        assert!(drained.is_empty());
    }
}
