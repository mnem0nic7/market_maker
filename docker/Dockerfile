# =============================================================================
# Stage 1: Build the Rust binaries
# =============================================================================
FROM rust:latest AS builder

WORKDIR /app

# Copy the workspace manifest and lock file first for better layer caching.
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
COPY bins/trading-core/Cargo.toml bins/trading-core/Cargo.toml
COPY bins/data-pipeline/Cargo.toml bins/data-pipeline/Cargo.toml
COPY bins/gateway/Cargo.toml bins/gateway/Cargo.toml
COPY bins/backtester/Cargo.toml bins/backtester/Cargo.toml

# Create stub source files so the initial dependency-only build can succeed.
# This layer is cached as long as Cargo.toml / Cargo.lock don't change.
RUN mkdir -p crates/mm-core/src && echo "pub fn _stub() {}" > crates/mm-core/src/lib.rs && \
    mkdir -p crates/mm-market-data/src && echo "pub fn _stub() {}" > crates/mm-market-data/src/lib.rs && \
    mkdir -p crates/mm-strategy/src && echo "pub fn _stub() {}" > crates/mm-strategy/src/lib.rs && \
    mkdir -p crates/mm-execution/src && echo "pub fn _stub() {}" > crates/mm-execution/src/lib.rs && \
    mkdir -p crates/mm-risk/src && echo "pub fn _stub() {}" > crates/mm-risk/src/lib.rs && \
    mkdir -p crates/mm-data-pipeline/src && echo "pub fn _stub() {}" > crates/mm-data-pipeline/src/lib.rs && \
    mkdir -p crates/mm-backtest/src && echo "pub fn _stub() {}" > crates/mm-backtest/src/lib.rs && \
    mkdir -p crates/mm-gateway/src && echo "pub fn _stub() {}" > crates/mm-gateway/src/lib.rs && \
    mkdir -p bins/trading-core/src && echo "fn main() {}" > bins/trading-core/src/main.rs && \
    mkdir -p bins/data-pipeline/src && echo "fn main() {}" > bins/data-pipeline/src/main.rs && \
    mkdir -p bins/gateway/src && echo "fn main() {}" > bins/gateway/src/main.rs && \
    mkdir -p bins/backtester/src && echo "fn main() {}" > bins/backtester/src/main.rs

# Build dependencies only (this layer is cached).
RUN cargo build --release 2>/dev/null || true

# Now copy the real source code.
COPY crates/ crates/
COPY bins/ bins/

# Touch all source files to invalidate the build cache for the actual code.
RUN find crates bins -name "*.rs" -exec touch {} +

# Build all binaries in release mode.
RUN cargo build --release \
    --bin trading-core \
    --bin data-pipeline \
    --bin gateway \
    --bin backtester

# =============================================================================
# Stage 2: Minimal runtime image
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install required runtime libraries.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the application.
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --create-home app

# Copy the compiled binaries from the builder stage.
COPY --from=builder /app/target/release/trading-core /usr/local/bin/trading-core
COPY --from=builder /app/target/release/data-pipeline /usr/local/bin/data-pipeline
COPY --from=builder /app/target/release/gateway /usr/local/bin/gateway
COPY --from=builder /app/target/release/backtester /usr/local/bin/backtester

# Create directories for config and data.
RUN mkdir -p /app/config /app/data && chown -R app:app /app

WORKDIR /app
USER app

# Default: run the trading-core binary.
CMD ["trading-core"]
