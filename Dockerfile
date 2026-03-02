# syntax=docker/dockerfile:1.4

# Stage 1: Build Rust application
FROM --platform=linux/amd64 rust:1.93-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY docs ./docs

ARG GIT_HASH=0
ENV GIT_HASH=$GIT_HASH

# Build all binaries in release mode (mount SSH for private git dependencies)
# graze-like-streamer package includes migration binaries (graze-migrate-dates, etc.)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    cargo build --release \
    -p graze-api \
    -p graze-like-streamer \
    -p graze-candidate-sync \
    -p graze-frontdoor

# Stage 2: Final runtime image
FROM --platform=linux/amd64 gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy all binaries
COPY --from=builder /app/target/release/graze-api /app/graze-api
COPY --from=builder /app/target/release/graze-like-streamer /app/graze-like-streamer
COPY --from=builder /app/target/release/graze-candidate-sync /app/graze-candidate-sync
COPY --from=builder /app/target/release/graze-backfill-ula /app/graze-backfill-ula
COPY --from=builder /app/target/release/graze-backfill /app/graze-backfill
COPY --from=builder /app/target/release/graze-frontdoor /app/graze-frontdoor
COPY --from=builder /app/target/release/graze-migrate-tranches /app/graze-migrate-tranches
COPY --from=builder /app/target/release/graze-migrate-dates /app/graze-migrate-dates
COPY --from=builder /app/target/release/graze-verify-migration /app/graze-verify-migration

# Copy Lua scripts (used by Redis)
COPY lua ./lua

ENV HTTP_HOST=0.0.0.0
ENV HTTP_PORT=8080
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

EXPOSE 8080

# Default to API server, can be overridden
ENTRYPOINT ["/app/graze-api"]
