FROM rust:1-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release --workspace --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/tracedb /usr/local/bin/tracedb
COPY --from=builder /workspace/target/release/tracedb-server /usr/local/bin/tracedb-server
COPY --from=builder /workspace/target/release/tracedb-worker /usr/local/bin/tracedb-worker
COPY --from=builder /workspace/target/release/tracedb-bench /usr/local/bin/tracedb-bench

ENV TRACEDB_SERVICE_MODE=engine
ENV TRACEDB_DATA_DIR=/data/tracedb

EXPOSE 8080
CMD ["tracedb-server"]
