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
    -p graze-frontdoor \
    -p graze-feed-stats \
    -p graze-lens-builder \
    -p graze-lens-fold \
    -p graze-lens-bootstrap

# Stage 2: Final runtime image
FROM --platform=linux/amd64 gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy all binaries
COPY --from=builder /app/target/release/graze-api /app/graze-api
COPY --from=builder /app/target/release/graze-like-streamer /app/graze-like-streamer
COPY --from=builder /app/target/release/graze-candidate-sync /app/graze-candidate-sync
# Phase A of the durable co-liker profile design; built by -p graze-candidate-sync.
COPY --from=builder /app/target/release/graze-build-coliker-profiles /app/graze-build-coliker-profiles
COPY --from=builder /app/target/release/graze-build-follow-seeds /app/graze-build-follow-seeds
COPY --from=builder /app/target/release/graze-backfill-ula /app/graze-backfill-ula
COPY --from=builder /app/target/release/graze-backfill /app/graze-backfill
COPY --from=builder /app/target/release/graze-frontdoor /app/graze-frontdoor
COPY --from=builder /app/target/release/graze-feed-stats /app/graze-feed-stats
# Viewer-graph lenses; selected by `command:` in kube/lens-builder-deployment.yaml.
COPY --from=builder /app/target/release/graze-lens-builder /app/graze-lens-builder
COPY --from=builder /app/target/release/graze-lens-fold /app/graze-lens-fold
COPY --from=builder /app/target/release/graze-lens-bootstrap /app/graze-lens-bootstrap
# Both built by -p graze-lens-fold; selected by `command:` in their CronJobs.
# A binary missing here does not fail the build — it fails at container start,
# on a schedule, as an OCI "no such file or directory" nobody is watching for.
COPY --from=builder /app/target/release/graze-lens-rev-rebuild /app/graze-lens-rev-rebuild
COPY --from=builder /app/target/release/graze-lens-project /app/graze-lens-project
COPY --from=builder /app/target/release/graze-lens-lpa /app/graze-lens-lpa
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
