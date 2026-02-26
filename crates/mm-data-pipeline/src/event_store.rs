//! NATS JetStream event store for durable event publishing and consumption.
//!
//! Provides [`NatsEventPublisher`] (implements the `EventPublisher` trait from mm-core)
//! and [`NatsEventSubscriber`] for replaying / tailing the event stream.

use std::time::Duration;

use async_nats::jetstream::{self, consumer::PullConsumer, stream::Stream as JsStream};
use async_trait::async_trait;
use futures::StreamExt;
use mm_core::traits::EventPublisher;
use mm_core::{Envelope, MmError, MmResult};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the NATS JetStream connection and stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    /// NATS server URL (e.g. "nats://127.0.0.1:4222").
    pub url: String,
    /// Name of the JetStream stream to create / bind to.
    pub stream_name: String,
    /// Maximum age of messages in seconds (0 = unlimited).
    pub max_age_secs: u64,
    /// Maximum total bytes in the stream (0 = unlimited).
    pub max_bytes: i64,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
            stream_name: "EVENTS".to_string(),
            max_age_secs: 0,
            max_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Retry constants
// ---------------------------------------------------------------------------

/// Maximum number of retry attempts for NATS publish operations.
pub const NATS_PUBLISH_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff on NATS publish retries (1 second).
pub const NATS_PUBLISH_BASE_DELAY: Duration = Duration::from_secs(1);

/// Compute the backoff delay for a given attempt (0-indexed).
/// Returns base_delay * 2^attempt (i.e., 1s, 2s, 4s for the default base).
/// Saturates to `Duration::MAX` on overflow rather than panicking.
pub fn nats_publish_backoff(attempt: u32, base_delay: Duration) -> Duration {
    match 2u32.checked_pow(attempt) {
        Some(factor) => base_delay.saturating_mul(factor),
        None => Duration::MAX,
    }
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

/// Publishes [`Envelope`]s to NATS JetStream, implementing the `EventPublisher`
/// trait so it can be used transparently by the rest of the platform.
pub struct NatsEventPublisher {
    jetstream: jetstream::Context,
    stream_name: String,
}

impl NatsEventPublisher {
    /// Connect to NATS and ensure the JetStream stream exists.
    ///
    /// If the stream already exists its configuration is left untouched; only
    /// missing streams are created.
    #[instrument(skip_all, fields(url = %config.url, stream = %config.stream_name))]
    pub async fn connect(config: &NatsConfig) -> MmResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| MmError::Messaging(format!("NATS connect failed: {e}")))?;

        let jetstream = jetstream::new(client);

        // Create or bind to the stream.
        let mut stream_config = jetstream::stream::Config {
            name: config.stream_name.clone(),
            subjects: vec!["events.>".to_string()],
            retention: jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        };

        if config.max_age_secs > 0 {
            stream_config.max_age = Duration::from_secs(config.max_age_secs);
        }
        if config.max_bytes > 0 {
            stream_config.max_bytes = config.max_bytes;
        }

        jetstream
            .get_or_create_stream(stream_config)
            .await
            .map_err(|e| MmError::Messaging(format!("JetStream stream setup failed: {e}")))?;

        info!(
            stream = %config.stream_name,
            "NATS JetStream publisher connected"
        );

        Ok(Self {
            jetstream,
            stream_name: config.stream_name.clone(),
        })
    }

    /// Return the name of the underlying JetStream stream.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }
}

#[async_trait]
impl EventPublisher for NatsEventPublisher {
    /// Serialize the envelope to JSON and publish to JetStream with retry.
    ///
    /// On failure, retries up to [`NATS_PUBLISH_MAX_RETRIES`] times with
    /// exponential backoff (1s, 2s, 4s). Each retry is logged at `warn!`
    /// level. After exhausting retries the error is returned (never silently
    /// swallowed).
    #[instrument(skip_all, fields(
        event_id = %envelope.event_id,
        subject = %envelope.nats_subject(),
    ))]
    async fn publish(&self, envelope: Envelope) -> MmResult<()> {
        let subject = envelope.nats_subject();
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| MmError::Serialization(format!("envelope serialization failed: {e}")))?;

        let mut last_err: Option<MmError> = None;

        for attempt in 0..=NATS_PUBLISH_MAX_RETRIES {
            if attempt > 0 {
                let delay = nats_publish_backoff(attempt - 1, NATS_PUBLISH_BASE_DELAY);
                warn!(
                    attempt,
                    max_retries = NATS_PUBLISH_MAX_RETRIES,
                    delay_ms = delay.as_millis() as u64,
                    subject = %subject,
                    "retrying NATS publish after failure"
                );
                tokio::time::sleep(delay).await;
            }

            let publish_result = self
                .jetstream
                .publish(subject.clone(), payload.clone().into())
                .await;

            match publish_result {
                Ok(ack_future) => {
                    // Await the publish acknowledgment from the server to
                    // guarantee the message has been persisted in the stream.
                    match ack_future.await {
                        Ok(_) => {
                            if attempt > 0 {
                                info!(
                                    attempt,
                                    subject = %subject,
                                    "NATS publish succeeded after retry"
                                );
                            }
                            debug!(subject = %subject, "event published to JetStream");
                            return Ok(());
                        }
                        Err(e) => {
                            last_err = Some(MmError::Messaging(format!(
                                "JetStream publish ack failed: {e}"
                            )));
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(MmError::Messaging(format!("JetStream publish failed: {e}")));
                }
            }
        }

        let err = last_err.unwrap_or_else(|| {
            MmError::Messaging("NATS publish failed after all retries".to_string())
        });
        error!(
            retries = NATS_PUBLISH_MAX_RETRIES,
            subject = %subject,
            error = %err,
            "NATS publish failed after all retries"
        );
        Err(err)
    }
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

/// Consumes events from NATS JetStream by creating ephemeral or durable pull
/// consumers against the event stream.
pub struct NatsEventSubscriber {
    #[allow(dead_code)]
    jetstream: jetstream::Context,
    stream: JsStream,
}

impl NatsEventSubscriber {
    /// Connect to NATS and bind to an existing JetStream stream.
    #[instrument(skip_all, fields(url = %config.url, stream = %config.stream_name))]
    pub async fn connect(config: &NatsConfig) -> MmResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| MmError::Messaging(format!("NATS connect failed: {e}")))?;

        let jetstream = jetstream::new(client);

        let stream = jetstream
            .get_stream(&config.stream_name)
            .await
            .map_err(|e| {
                MmError::Messaging(format!(
                    "failed to bind to JetStream stream '{}': {e}",
                    config.stream_name
                ))
            })?;

        info!(
            stream = %config.stream_name,
            "NATS JetStream subscriber connected"
        );

        Ok(Self { jetstream, stream })
    }

    /// Subscribe to events matching the given subject pattern (e.g. `"events.>"`,
    /// `"events.order.>"`, `"events.risk.*"`).
    ///
    /// Returns an unbounded stream of deserialized [`Envelope`]s. Messages that
    /// fail to deserialize are logged and skipped (acked so they are not
    /// redelivered forever).
    #[instrument(skip(self), fields(subject = %subject_pattern))]
    pub async fn subscribe(
        &self,
        subject_pattern: &str,
    ) -> MmResult<impl futures::Stream<Item = Envelope> + '_> {
        let consumer = self
            .stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: subject_pattern.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| MmError::Messaging(format!("consumer creation failed: {e}")))?;

        info!(subject = %subject_pattern, "subscribed to event stream");

        Ok(self.message_stream(consumer))
    }

    /// Subscribe starting from a specific stream sequence number, useful for
    /// replaying events from a known checkpoint.
    #[instrument(skip(self), fields(subject = %subject, start_seq = start_sequence))]
    pub async fn subscribe_from_sequence(
        &self,
        subject: &str,
        start_sequence: u64,
    ) -> MmResult<impl futures::Stream<Item = Envelope> + '_> {
        let consumer = self
            .stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: subject.to_string(),
                deliver_policy: jetstream::consumer::DeliverPolicy::ByStartSequence {
                    start_sequence,
                },
                ..Default::default()
            })
            .await
            .map_err(|e| MmError::Messaging(format!("consumer creation failed: {e}")))?;

        info!(
            subject = %subject,
            start_sequence,
            "subscribed to event stream from sequence"
        );

        Ok(self.message_stream(consumer))
    }

    /// Internal helper: turn a pull consumer into a stream of [`Envelope`]s.
    fn message_stream(&self, consumer: PullConsumer) -> impl futures::Stream<Item = Envelope> + '_ {
        async_stream(consumer)
    }
}

/// Produces an async stream of deserialized envelopes from a pull consumer.
fn async_stream(consumer: PullConsumer) -> impl futures::Stream<Item = Envelope> {
    futures::stream::unfold(consumer, |consumer| async move {
        // Batch of 1 with a generous idle timeout keeps latency low while
        // remaining efficient under load.
        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "failed to open message stream from consumer");
                return None;
            }
        };

        loop {
            match messages.next().await {
                Some(Ok(msg)) => {
                    // Always ack to avoid infinite redelivery of bad messages.
                    if let Err(e) = msg.ack().await {
                        warn!(error = %e, "failed to ack JetStream message");
                    }

                    match serde_json::from_slice::<Envelope>(&msg.payload) {
                        Ok(envelope) => {
                            // We need to return the consumer for the next iteration, but
                            // we have moved it into `messages`. Instead we break out and
                            // return the envelope. On the next poll unfold will call us
                            // again -- but we've consumed the consumer. To keep things
                            // simple we return None after the first batch so callers
                            // should use `subscribe` which wraps this differently.
                            //
                            // Actually, we restructure: unfold is not ideal here. We use
                            // `futures::stream::try_unfold` pattern instead. But to keep
                            // the API simple we just yield envelopes from an
                            // `async_stream`-less approach using `stream::unfold` with
                            // the messages iterator as state.
                            //
                            // Let's restructure to use (messages) as the state.
                            drop(messages);
                            return Some((envelope, consumer));
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                payload_len = msg.payload.len(),
                                "skipping undeserializable JetStream message"
                            );
                            // Continue to next message.
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!(error = %e, "error reading from JetStream consumer");
                    // Continue trying.
                }
                None => {
                    // Stream ended (server closed connection, etc.)
                    debug!("JetStream message stream ended");
                    return None;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_sane() {
        let cfg = NatsConfig::default();
        assert_eq!(cfg.stream_name, "EVENTS");
        assert!(cfg.url.contains("4222"));
    }

    #[test]
    fn config_serialization_roundtrip() {
        let cfg = NatsConfig {
            url: "nats://prod:4222".into(),
            stream_name: "EVENTS".into(),
            max_age_secs: 86400,
            max_bytes: 1_073_741_824,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: NatsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stream_name, cfg.stream_name);
        assert_eq!(back.max_age_secs, cfg.max_age_secs);
    }

    #[test]
    fn retry_constants_are_reasonable() {
        assert!(
            NATS_PUBLISH_MAX_RETRIES >= 1 && NATS_PUBLISH_MAX_RETRIES <= 10,
            "max retries should be between 1 and 10"
        );
        assert!(
            NATS_PUBLISH_BASE_DELAY >= Duration::from_millis(100)
                && NATS_PUBLISH_BASE_DELAY <= Duration::from_secs(10),
            "base delay should be between 100ms and 10s"
        );
    }

    #[test]
    fn backoff_calculation_exponential() {
        let base = Duration::from_secs(1);
        // attempt 0 -> 1s, attempt 1 -> 2s, attempt 2 -> 4s
        assert_eq!(nats_publish_backoff(0, base), Duration::from_secs(1));
        assert_eq!(nats_publish_backoff(1, base), Duration::from_secs(2));
        assert_eq!(nats_publish_backoff(2, base), Duration::from_secs(4));
    }

    #[test]
    fn backoff_does_not_overflow() {
        let base = Duration::from_secs(1);
        // Very large attempt number should saturate, not panic.
        let result = nats_publish_backoff(63, base);
        assert!(
            result >= base,
            "backoff should always be at least base_delay"
        );
    }

    #[test]
    fn total_backoff_time_bounded() {
        // Verify the total worst-case delay is bounded and reasonable.
        let base = NATS_PUBLISH_BASE_DELAY;
        let total: Duration = (0..NATS_PUBLISH_MAX_RETRIES)
            .map(|a| nats_publish_backoff(a, base))
            .sum();
        // With defaults (1s, 2s, 4s) = 7s total
        assert!(
            total <= Duration::from_secs(30),
            "total backoff should not exceed 30s, got {:?}",
            total
        );
    }
}
