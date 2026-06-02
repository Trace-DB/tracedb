---
title: "Local Cloud"
aliases:
  - TraceDB Local Cloud
  - Docker Compose Local Cloud
tags:
  - tracedb/operations
  - tracedb/local-cloud
status: active
type: runbook
updated: 2026-05-21
---

# TraceDB Local Cloud

TraceDB now has two alpha run paths that intentionally share the same engine code:

- Embedded/local daemon: one process owns one data directory, similar to SQLite.
- Docker Compose local cloud: gateway, engine, worker, catalog, queue, and bucket services mirror the future hosted/serverless shape.

The hard local-cloud rule is that only `tracedb-engine` mounts and writes `/data/tracedb`. Gateway and worker services call the engine HTTP API.

## Embedded

```bash
cargo run -p tracedb-cli -- --data .tracedb init
cargo run -p tracedb-cli -- --data .tracedb schema apply schema.json
cargo run -p tracedb-cli -- --data .tracedb put record.json
cargo run -p tracedb-cli -- --data .tracedb scan docs tenant-a 100
```

The embedded path supports schema apply, put, get, patch, delete/tombstone, scan, query, compact, snapshot, restore, inspect, and doctor commands without external services.

## Local Daemon

```bash
cargo run -p tracedb-cli -- --data .tracedb serve 127.0.0.1:8080
curl -fsS http://127.0.0.1:8080/ready
```

The daemon exposes the same HTTP routes used by the SDK and local cloud:

- `GET /v1/health`
- `GET /v1/ready`
- `GET /v1/databases`
- `GET /v1/branches`
- `GET /v1/metrics/public-safe`
- `POST /v1/schema/apply`
- `POST /v1/insert`
- `POST /v1/records/put`
- `POST /v1/records/put-batch`
- `POST /v1/records/patch`
- `POST /v1/records/delete`
- `POST /v1/records/get`
- `POST /v1/records/scan`
- `POST /v1/query`
- `POST /v1/explain`
- `POST /v1/admin/compact`
- `POST /v1/admin/snapshot`
- `POST /v1/admin/restore`
- `GET /v1/admin/jobs`

The current checked API references are `docs/api/v1-http.md` and the generated
`docs/api/v1-openapi.json` artifact in the TraceDB repo.

## Docker Compose

Engine-only lite mode:

```bash
docker compose --profile lite up -d tracedb-engine
curl -fsS http://127.0.0.1:18081/ready
```

Full local cloud:

```bash
docker compose --profile full up -d
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18082/health
```

Service roles:

- `tracedb-gateway`: public HTTP edge, routing and metering surface.
- `tracedb-engine`: private authoritative owner of WAL, manifests, records, indexes, segments, and recovery.
- `tracedb-worker`: background worker surface that talks to the engine private API.
- `postgres-catalog`: local metadata service for org/project/database/branch/API-key shape.
- `valkey-queue`: local queue substrate for compaction, snapshot, feature, and index jobs.
- `minio-bucket`: local object bucket for snapshot/export/restore bundles.

## Doctor

```bash
cargo run -p tracedb-cli -- --data .tracedb doctor
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 cargo run -p tracedb-cli -- doctor http
cargo run -p tracedb-cli -- compose status
```

`tracedb doctor` checks the local directory layout, engine recovery state, Docker Compose availability, catalog file presence, and the queue/bucket mode expected for local cloud.

`tracedb doctor http --url URL` checks a running HTTP endpoint without mutating
data. It covers health, readiness, database catalog, branch catalog,
public-safe metrics, and admin jobs, returning one JSON summary with per-route
responses or SDK error details. It is useful before and after local daemon or
managed-style deploy smoke tests; it is not a SQL probe or benchmark.
For CI/deployed endpoint checks, prefer the env-driven form with
`TRACEDB_URL`, `TRACEDB_TOKEN`, optional `TRACEDB_DATABASE_ID` /
`TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, and `TRACEDB_SAFE_RETRIES` so tokens
do not need to appear as command-line arguments.

## Product Regression

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
cargo run -p tracedb-cli -- durability-faults
cargo run -p tracedb-cli -- product-regression --report-file /tmp/tracedb-product-reports/full.json
cargo run -p tracedb-cli -- product-regression --list-steps
cargo run -p tracedb-cli -- product-regression --list-steps --report-file /tmp/tracedb-product-reports/steps.json
cargo run -p tracedb-cli -- product-regression --only embedded_demo
cargo run -p tracedb-cli -- product-regression --data-root /tmp/tracedb-product-targeted-embedded --only embedded_demo
cargo run -p tracedb-cli -- product-regression --data-root /tmp/tracedb-product-targeted-embedded --only embedded_verify
cargo run -p tracedb-cli -- product-regression --only http_demo
cargo run -p tracedb-cli -- product-regression --only local_doctor
```

`product-regression` is the consolidated local core gate for embedded
demo/verify, HTTP demo, and endpoint doctor paths. SDK conformance is owned by
`../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`.
`product-regression --list-steps` is an operator-discovery mode for gate
wiring only: it emits JSON step metadata, including `human_summary` and
`only_supported`, should not start servers, should not mutate data directories,
should not invoke Node/TypeScript tooling, and should not be treated as product
evidence by itself. Normal full and targeted product-regression runs also keep a
top-level `human_summary` object inside the JSON stdout for quick operator
status/count/message scanning. Operators can add `--report-file PATH` to full,
targeted, injected-failure, or step-list runs to write the same JSON summary to
a predictable CLI-created file while preserving JSON stdout; parent directories
are created automatically.
`product-quickstart` runs the same local gate and defaults that report artifact
to `target/tracedb/product-quickstart.json`, while preserving JSON stdout,
recording the resolved artifact path in top-level `report_file`, and accepting
the same product-regression options. Treat that artifact as the local
quickstart receipt: it should report `ok: true`, `mode:
"local-product-regression"`, `scope: "local_only"`,
`human_summary.status: "passed"`, SQL as `not_implemented`, and managed-cloud
and benchmark claims as `not_checked`.
Use `cargo run -q -p tracedb-cli -- product-quickstart --inject-failure
embedded_demo` to validate the failure receipt path. It exits nonzero while
still writing the default artifact, preserving `report_file`, reporting a failed
`human_summary`, recording `failure_injection: "embedded_demo"`, and keeping
SQL/managed-cloud/benchmark claims parked.
`durability-faults` writes `target/tracedb/durability-faults.json` and reports
local TDE/WAL durability evidence for wrong or missing master keys, torn WAL
tails, manifest/checkpoint corruption, stale PID lock recovery, encrypted
snapshot restore, and WAL-backed idempotency replay after reopen. It is local
durability evidence, not Railway backup/DR evidence.
SDK verification is externally owned by `../tracedb-rust`, `../tracedb-python`,
and `../tracedb-js`; the core product gate no longer has SDK or TypeScript
fallback lanes.

Operators can use `product-regression --only embedded_demo` for a local
embedded-only product check with one-step JSON output and an `only_step` marker
in `human_summary.message`. This is not managed-cloud verification and not full
product gate coverage.

For dependency-aware embedded verification, run `--only embedded_demo` and then
`--only embedded_verify` with the same `--data-root`; the verify step checks
the existing embedded demo data root and still does not run HTTP or SDK paths.

Operators can use `product-regression --only http_demo` for a self-contained
local HTTP demo check with one-step JSON output. It starts its own loopback
server inside the child `http-demo` command and does not run local
`doctor http`, SDK conformance, managed-cloud checks, benchmark controls, or SQL
compatibility checks.

Operators can use `product-regression --only local_doctor` for managed local
endpoint diagnostics with one-step JSON output. It manages the local server
lifecycle for the doctor step; manual `doctor http` still targets an
already-running endpoint. This does not run `http_demo`, SDK conformance,
managed-cloud checks, benchmark controls, or SQL compatibility checks.

SDK verification is externally owned by the sibling standalone repos:
`../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`. Run SDK package
checks, SDK HTTP smokes, and SDK conformance from those repositories rather than
from the core product-regression gate.

## Future Modes

TraceDB should keep these deployment modes compatible:

- Embedded library: in-process database over one directory.
- Local daemon: one binary exposing HTTP over one directory.
- Docker Compose: cloud-shaped local services for development and observability.
- Kubernetes/serverless: future hosted deployment using the same gateway, engine, worker, catalog, queue, and bucket roles.

Postgres wire, MySQL wire, Mongo protocol, full SQL compatibility, public ANN tuning knobs, and production Kubernetes manifests stay behind the HTTP/CLI/SDK alpha until the local cloud behavior is reliable. Segment-local HNSW vector artifacts are internal alpha infrastructure and are not exposed as schema/query parameters yet.
