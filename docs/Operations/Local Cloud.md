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
cargo run -p tracedb-cli -- product-regression --only rust_sdk_quickstart
cargo run -p tracedb-cli -- product-regression --only python_sdk_smoke
cargo run -p tracedb-cli -- product-regression --only typescript_check
cargo run -p tracedb-cli -- product-regression --only typescript_http_smoke
cargo run -p tracedb-cli -- product-regression --only typescript_gateway_smoke
```

`product-regression` is the consolidated local gate for embedded demo/verify,
HTTP demo, endpoint doctor, Rust SDK quickstart, Python sync SDK smoke, and
generated TypeScript smoke paths. `product-regression --list-steps` is an
operator-discovery mode for gate
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
Use `cargo run -q -p tracedb-cli -- product-quickstart --skip-typescript` only
as the reduced fallback receipt for machines without Node tooling. It should
still write `target/tracedb/product-quickstart.json`, preserve `report_file`,
report `typescript_enabled: false`, pass the six non-TypeScript local steps
including `python_sdk_smoke`, and omit `typescript_check`,
`typescript_http_smoke`, and
`typescript_gateway_smoke`. This is a reduced local evidence path, not full
product-gate evidence and not generated TypeScript client evidence.

When local macOS executable policy or workstation resources block the product
gate, use Modal for remote Linux product verification from the current checkout:

```bash
cd /Users/zgrogan/Repos/tracedb
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
modal run scripts/modal_product_verify.py --mode workspace --summary-json /tmp/tracedb-modal-product-workspace.json
```

The Modal runner excludes `.git`, `target/`, local env files, benchmark reports,
caches, and Node modules from the upload. `quickstart` runs fmt, the focused
quickstart receipt regression, the docs contract, and
`product-quickstart --skip-typescript`, then validates
`target/tracedb/product-quickstart.json` against stdout. `workspace` additionally
installs TypeScript dependencies with `npm ci`, runs the full CLI demo test file,
the usability acceptance test file, and `cargo test --workspace --all-targets`.
2026-05-21 evidence: quickstart passed in 15.334s and workspace passed in
38.706s on Modal. This is remote Linux product verification, not proof that this
Mac can execute the same binaries after the AMFI/taskgated blocker.
`--skip-typescript` is for the full product gate and non-TypeScript selectors.
If `--only` selects a `typescript_*` step, combining it with `--skip-typescript`
is a parse-time conflict and should fail before data root creation or product
JSON output.

Operators can use `product-regression --only embedded_demo` for a local
embedded-only product check with one-step JSON output and an `only_step` marker
in `human_summary.message`. This is not managed-cloud verification and not full
product gate coverage.

For dependency-aware embedded verification, run `--only embedded_demo` and then
`--only embedded_verify` with the same `--data-root`; the verify step checks
the existing embedded demo data root and still does not run HTTP, SDK, or
TypeScript paths.

Operators can use `product-regression --only http_demo` for a self-contained
local HTTP demo check with one-step JSON output. It starts its own loopback
server inside the child `http-demo` command and does not run local
`doctor http`, the Rust SDK quickstart, generated TypeScript smoke steps,
managed-cloud checks, benchmark controls, or SQL compatibility checks.

Operators can use `product-regression --only local_doctor` for managed local
endpoint diagnostics with one-step JSON output. It manages the local server
lifecycle for the doctor step; manual `doctor http` still targets an
already-running endpoint. This does not run `http_demo`, the Rust SDK
quickstart, generated TypeScript smoke steps, managed-cloud checks, benchmark
controls, or SQL compatibility checks.

Operators can use `product-regression --only rust_sdk_quickstart` for local
Rust SDK quickstart verification with one-step JSON output. It manages the
local server lifecycle and SDK admin directory for the quickstart step. This
does not run `http_demo`, local `doctor http`, generated TypeScript smoke
steps, managed-cloud checks, benchmark controls, or SQL compatibility checks.

Rust SDK application code can use `TraceDbClientConfig::from_env()` to read
`TRACEDB_URL`, optional `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`,
`TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES` before constructing a `TraceDbClient`.

Operators can use `product-regression --only python_sdk_smoke` for sync Python
SDK HTTP verification with one-step JSON output. It runs
`python3 clients/python/http_smoke.py` from the workspace root; that smoke
starts its own local server and exercises the Python SDK through schema,
writes, reads, query/explain, idempotency, errors, and admin helpers. This does
not run embedded demo/verify, `http_demo`, local `doctor http`, Rust SDK
quickstart, TypeScript smoke steps, managed-cloud checks, benchmark controls,
or SQL compatibility checks.

For package/config checks without starting a server, run
`python3 -m unittest discover -s clients/python/tests`. This guards the
`pyproject.toml` package shape and `TraceDB.from_env()` environment helper
before the heavier HTTP smoke or Modal workspace lane.

Operators can use `product-regression --only typescript_check` for generated
TypeScript package check verification with one-step JSON output. It runs only
`npm run check` in `clients/typescript`, so it does not run `http_demo`, local
`doctor http`, Rust SDK quickstart, TypeScript HTTP smoke, TypeScript gateway
smoke, managed-cloud checks, benchmark controls, or SQL compatibility checks.

Operators can use `product-regression --only typescript_http_smoke` for local
public TypeScript SDK HTTP smoke verification with one-step JSON output. It
runs only `npm run public-http-smoke` in `clients/typescript`; that smoke starts
its own local server and exercises the public `TraceDB` wrapper over the
generated transport, including idempotency replay/conflict and parsed
error-envelope evidence. `scripts/platform_conformance.py --surface
typescript_sdk` maps the same smoke summary to the required Platform Contract
v0 scenario list; in the current 13-scenario harness, native TraceQL is checked
and passes through the public `TraceDB.traceql()` helper.
This does not run embedded demo/verify, `http_demo`, local
doctor, Rust SDK quickstart, `typescript_check`, TypeScript gateway smoke,
managed-cloud checks, benchmark controls, or SQL compatibility checks.

Operators can use `product-regression --only typescript_gateway_smoke` for
local public TypeScript SDK gateway auth/routing verification with one-step JSON
output. It runs only `npm run gateway-smoke` in `clients/typescript`; that smoke
starts a local engine plus gateway-mode server, requires bearer auth, verifies
missing-token and bad-branch rejection, and runs the public `TraceDB` wrapper
through the gateway with managed routing metadata plus a local admin dir. The
gateway port is allocated after engine readiness so it cannot reuse the engine
port in broad workspace runs. This does not run embedded
demo/verify, `http_demo`, local doctor, Rust SDK quickstart, `typescript_check`,
TypeScript HTTP smoke, managed-cloud checks, benchmark controls, or SQL
compatibility checks.

## Future Modes

TraceDB should keep these deployment modes compatible:

- Embedded library: in-process database over one directory.
- Local daemon: one binary exposing HTTP over one directory.
- Docker Compose: cloud-shaped local services for development and observability.
- Kubernetes/serverless: future hosted deployment using the same gateway, engine, worker, catalog, queue, and bucket roles.

Postgres wire, MySQL wire, Mongo protocol, full SQL compatibility, ANN/HNSW, and production Kubernetes manifests stay behind the HTTP/CLI/SDK alpha until the local cloud behavior is reliable.
