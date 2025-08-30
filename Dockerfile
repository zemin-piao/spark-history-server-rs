# Multi-stage build for Spark History Server (Rust)
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -r -s /bin/false -m -d /app spark-history

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/target/release/spark-history-server /usr/local/bin/
COPY config/ ./config/

# Set ownership
RUN chown -R spark-history:spark-history /app

USER spark-history

# Expose the default port
EXPOSE 18080

# Create volume for event logs
VOLUME ["/tmp/spark-events"]

# Set default config path
ENV CONFIG_PATH="/app/config/settings.toml"

CMD ["spark-history-server", "--config", "/app/config/settings.toml"]