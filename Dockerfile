FROM rust:1.89.0-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release --workspace --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash tracedb
RUN mkdir -p /data/tracedb && chown -R tracedb:tracedb /data

COPY --from=builder /workspace/target/release/tracedb /usr/local/bin/tracedb
COPY --from=builder /workspace/target/release/tracedb-server /usr/local/bin/tracedb-server

ENV TRACEDB_DATA_DIR=/data/tracedb

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD curl -fsS http://localhost:8080/v1/health || exit 1

USER tracedb
CMD ["tracedb-server"]
