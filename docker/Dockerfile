# =============================================================================
# Stage 1: Build the Rust binaries
# =============================================================================
FROM rust:1.88-slim AS builder

WORKDIR /app

# Install only minimal build dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Build optimizations: thin LTO for faster link times, parallel codegen units.
ENV CARGO_PROFILE_RELEASE_LTO=thin
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16

# ---------------------------------------------------------------------------
# Copy manifests first (changes less often) for better layer caching.
# ---------------------------------------------------------------------------
COPY Cargo.toml Cargo.lock ./

# Copy all crate manifests so cargo can resolve the dependency graph.
COPY crates/mm-core/Cargo.toml crates/mm-core/Cargo.toml
COPY crates/mm-market-data/Cargo.toml crates/mm-market-data/Cargo.toml
COPY crates/mm-strategy/Cargo.toml crates/mm-strategy/Cargo.toml
COPY crates/mm-execution/Cargo.toml crates/mm-execution/Cargo.toml
COPY crates/mm-risk/Cargo.toml crates/mm-risk/Cargo.toml
COPY crates/mm-data-pipeline/Cargo.toml crates/mm-data-pipeline/Cargo.toml
COPY crates/mm-backtest/Cargo.toml crates/mm-backtest/Cargo.toml
COPY crates/mm-gateway/Cargo.toml crates/mm-gateway/Cargo.toml
COPY crates/mm-research/Cargo.toml crates/mm-research/Cargo.toml
COPY bins/trading-core/Cargo.toml bins/trading-core/Cargo.toml
COPY bins/data-pipeline/Cargo.toml bins/data-pipeline/Cargo.toml
COPY bins/gateway/Cargo.toml bins/gateway/Cargo.toml
COPY bins/backtester/Cargo.toml bins/backtester/Cargo.toml
COPY bins/connection-test/Cargo.toml bins/connection-test/Cargo.toml

# ---------------------------------------------------------------------------
# Create stub source files for dependency resolution.
# This layer is cached as long as Cargo.toml / Cargo.lock don't change.
# ---------------------------------------------------------------------------
RUN mkdir -p crates/mm-core/src && echo "pub fn _stub() {}" > crates/mm-core/src/lib.rs && \
    mkdir -p crates/mm-market-data/src && echo "pub fn _stub() {}" > crates/mm-market-data/src/lib.rs && \
    mkdir -p crates/mm-market-data/benches && echo "fn main() {}" > crates/mm-market-data/benches/normalizer_bench.rs && \
    mkdir -p crates/mm-strategy/src && echo "pub fn _stub() {}" > crates/mm-strategy/src/lib.rs && \
    mkdir -p crates/mm-strategy/benches && echo "fn main() {}" > crates/mm-strategy/benches/strategy_bench.rs && \
    mkdir -p crates/mm-execution/src && echo "pub fn _stub() {}" > crates/mm-execution/src/lib.rs && \
    mkdir -p crates/mm-execution/benches && echo "fn main() {}" > crates/mm-execution/benches/oms_bench.rs && \
    mkdir -p crates/mm-risk/src && echo "pub fn _stub() {}" > crates/mm-risk/src/lib.rs && \
    mkdir -p crates/mm-data-pipeline/src && echo "pub fn _stub() {}" > crates/mm-data-pipeline/src/lib.rs && \
    mkdir -p crates/mm-backtest/src && echo "pub fn _stub() {}" > crates/mm-backtest/src/lib.rs && \
    mkdir -p crates/mm-gateway/src && echo "pub fn _stub() {}" > crates/mm-gateway/src/lib.rs && \
    mkdir -p crates/mm-research/src && echo "pub fn _stub() {}" > crates/mm-research/src/lib.rs && \
    mkdir -p bins/trading-core/src && echo "pub fn _stub() {}" > bins/trading-core/src/lib.rs && \
    mkdir -p bins/trading-core/src && echo "fn main() {}" > bins/trading-core/src/main.rs && \
    mkdir -p bins/data-pipeline/src && echo "fn main() {}" > bins/data-pipeline/src/main.rs && \
    mkdir -p bins/gateway/src && echo "fn main() {}" > bins/gateway/src/main.rs && \
    mkdir -p bins/backtester/src && echo "fn main() {}" > bins/backtester/src/main.rs && \
    mkdir -p bins/connection-test/src && echo "fn main() {}" > bins/connection-test/src/main.rs && \
    mkdir -p src && echo "" > src/lib.rs

# Build dependencies only (cached if Cargo.toml/Cargo.lock unchanged).
RUN cargo build --release \
    -p trading-core \
    -p data-pipeline \
    -p gateway \
    -p backtester

# ---------------------------------------------------------------------------
# Now copy actual source and rebuild.
# ---------------------------------------------------------------------------
COPY crates/ crates/
COPY bins/ bins/
COPY config/ config/
COPY proto/ proto/
COPY src/ src/

# Touch source files to invalidate only code compilation.
RUN find crates bins src -name "*.rs" -exec touch {} +

# Full release build of the deployable workspace packages.
RUN cargo build --release \
    -p trading-core \
    -p data-pipeline \
    -p gateway \
    -p backtester

# =============================================================================
# Stage 2: Minimal runtime image
# =============================================================================
FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.title="Market Maker"
LABEL org.opencontainers.image.description="Multi-market automated trading platform"

# Install only runtime dependencies (no dev headers).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the application.
RUN groupadd -r app && useradd -r -g app -d /app -s /sbin/nologin app

# Copy the compiled binaries from the builder stage with proper ownership.
COPY --from=builder --chown=app:app /app/target/release/trading-core /usr/local/bin/
COPY --from=builder --chown=app:app /app/target/release/data-pipeline /usr/local/bin/
COPY --from=builder --chown=app:app /app/target/release/gateway /usr/local/bin/
COPY --from=builder --chown=app:app /app/target/release/backtester /usr/local/bin/

# Copy default configuration.
COPY --chown=app:app config/ /app/config/

# Create data directory for runtime artifacts.
RUN mkdir -p /app/data && chown -R app:app /app

USER app
WORKDIR /app

# Health check targeting the gateway endpoint.
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8080/api/v1/health || exit 1

# Default: run the trading-core binary.
CMD ["trading-core"]
