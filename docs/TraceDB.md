---
title: TraceDB
aliases:
  - TraceDB Home
tags:
  - tracedb
  - docs
status: current-public-handoff
type: repo-handoff
updated: 2026-06-02
---

# TraceDB

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

This repo-local file is the public TraceDB handoff for the current product
surface: local/embedded engine, HTTP/CLI, SDKs, durability semantics, platform
contract, and benchmark evidence boundaries.

Read `docs/project-intent.md` first when creating a new project, prompt,
automation, or handoff for this folder. It preserves the memory-derived intent
behind the current docs: product identity, architecture boundaries, evidence
rules, Modal/Railway verification policy, SDK/MCP direction, and repo/vault
operating boundaries.

TraceField is the memory/runtime research program and a future runtime
direction, not the current TraceDB product and not an implemented runtime in
this repo. Agent Memory Flight Recorder is a concrete local demo wedge built on
TraceDB records, query/explain output, and replayable receipts; it is not the
product identity. Tensor artifacts are future governed derived-artifact/module
work; TraceDB does not currently provide tensor compute or tensor storage
services.
`crates/tracedb-memory-runtime` is placeholder/scaffolding only; memory calculus
is not implemented.

## Durability Semantics

`docs/durability-semantics-v0.md` is the current local-first durability
boundary. It documents WAL commit frames, manifest and checkpoint checksums,
torn-tail recovery, hard corruption failures, snapshot/restore copy semantics,
write/WAL lock-file behavior, TDE artifact behavior, and local
WAL/checkpoint-backed idempotency receipts. It also states the non-guarantees:
no managed-cloud backup/DR, no cross-replica idempotency, no crash-atomic
exactly-once semantics, and no multi-process active writer claim for one data
directory.

## HTTP Stack Boundary

The current HTTP stack boundary is explicit. `tracedb-server` and
`tracedb-gateway` default to Tokio/Axum product paths with Tower body limits,
timeouts, load shedding, concurrency limits, graceful shutdown, structured JSON
tracing, and private engine-token enforcement where configured. Engine mode
uses an async handle with serialized writes/admin work and cheap read snapshots.
Legacy stdlib listener helpers remain for compatibility tests and local
harnesses. The current server path does not provide TLS or HTTP/2 and is not a
complete managed-service runtime.

## Local Product Smoke

Run from the repo root:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

The demo exercises schema apply, batch ingest, query/explain, scan, delete,
compact, snapshot, and restore through the embedded engine. SQL compatibility is
not implemented.

The consolidated local product regression gate is:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
cargo run -p tracedb-cli -- durability-faults
```

It emits one machine-readable `local-product-regression` summary for the
embedded demo/verify path, `http-demo`, and local `doctor http`, with a compact
top-level `human_summary` for quick operator scanning. It is local
product regression evidence only: SQL remains not implemented, managed-cloud is
not checked, and benchmarks are not checked. The local product regression gate
also has test-only `--inject-failure STEP` coverage for nonzero process status
and machine-readable failed-step JSON. Use
`--report-file PATH` to write the same product-regression JSON summary to a
predictable file while preserving JSON stdout; this applies to full runs,
`--only`, `--inject-failure`, and `--list-steps`, and creates parent
directories. Use
`--list-steps` prints the machine-readable list of core product-regression step
names with `human_summary` and `only_supported` metadata for operators and CI
wiring without executing any product checks. `product-quickstart` runs the same
local core product gate with a default report file at
`target/tracedb/product-quickstart.json`; it accepts the same product-regression
options, including `--only`, still writes JSON to stdout, and includes a
top-level `report_file` field. Operators can validate the local quickstart
receipt by checking that artifact for `ok: true`, `mode:
"local-product-regression"`, `scope: "local_only"`, `human_summary.status:
"passed"`, `claims.sql_module: "not_implemented"`,
`claims.managed_cloud: "not_checked"`, and `claims.benchmark: "not_checked"`.
SDK conformance is externally owned by `../tracedb-rust`, `../tracedb-python`,
and `../tracedb-js`.
`product-quickstart --inject-failure embedded_demo` is the quick failure receipt
check: it exits nonzero, writes the same default report artifact, keeps
`report_file`, reports `human_summary.status: "failed"`, and records
`failure_injection: "embedded_demo"` with an injected failed `embedded_demo`
step.
`--only embedded_demo` currently runs just the embedded demo step and emits the
normal one-step product-regression JSON summary. `--only embedded_verify`
verifies an existing embedded demo data root, usually with the same
`--data-root` used by `--only embedded_demo`. `--only http_demo` runs the
self-contained local HTTP demo step and emits the normal one-step
`local-product-regression` JSON summary. It does not run local `doctor http`,
SDK conformance, managed-cloud checks, benchmark controls, or SQL compatibility
checks. `--only local_doctor` starts a managed-style local loopback
`tracedb-server` child process and runs only the existing local `doctor http`
product-regression step with readiness wait, `database_id`, and `branch_id`
metadata. It emits the normal one-step `local-product-regression` JSON summary
with `only_step: "local_doctor"`. This is read-only local endpoint diagnostics
against a managed local server, not a mutating product smoke, not SDK
conformance, not managed-cloud proof, not benchmark evidence, and not SQL
compatibility.

SDK conformance is externally owned by sibling standalone repositories:
`../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`. Run SDK
quickstarts, package checks, HTTP smokes, and SDK conformance from those repos,
not from the core product-regression gate. Go SDK status remains planned until
`../tracedb-go` contains a minimal implementation and a failing-on-error smoke.

Current platform conformance checkpoint: `scripts/platform_conformance.py` maps
HTTP direct, Rust SDK, TypeScript SDK, Python SDK, native TraceQL, and native
GraphQL into all 13 Platform Contract v0 scenario IDs, plus the partial
TraceQL/SQL-ish compatibility lane. Modal workspace run
`ap-YBjqjv9hV5dHkVb2AgJSud` passed 20/20 commands in 96.9s, including
`platform-conformance-quick`, `traceql-sqlish-conformance`,
`typescript-sdk-conformance`, `python-sdk-conformance`, Python unit/install
smokes, TypeScript package/HTTP/gateway lanes, TypeScript public SDK GraphQL
schema export evidence, Python public SDK GraphQL schema export evidence,
TypeScript and Python public SDK GraphQL result/explain smoke coverage, and
`cargo test --workspace --all-targets`. The API parity branch promotes native
TraceQL and native GraphQL to full 13/13 local conformance lanes; bounded
GraphQL and SQL-ish SELECT remain compatibility evidence. The workspace tests
included the generated GraphQL SDL unit
test, HTTP GraphQL schema export test, Rust SDK `GraphQlQueryRequest`,
sync/async `graphql_typed`, generated schema helper, and GraphQL safe retry
coverage; the TypeScript public SDK smoke now also checks
`TraceDB.graphqlSchema()` against the generated SDL route, and the Python SDK
smoke now checks `TraceDB.graphql_schema()` against the same route. The Rust SDK
`http_client` suite reported 49/49 passed. This is platform conformance
evidence, not managed-cloud proof, SQL compatibility, full GraphQL adapter
parity, or benchmark evidence.

The local HTTP plus SDK smoke is also available as one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

It starts a loopback `tracedb-server` child process, drives the core HTTP
product path through ready, schema apply, batch ingest, scan, query, explain,
delete, compact, snapshot, and restore, and reports `sql_module:
not_implemented`. This proves the local HTTP product path; it does not claim
managed-cloud deployment health, backup/DR semantics, cross-replica
idempotency, SDK conformance, or SQL compatibility. SDK quickstarts live in the
sibling standalone repos `../tracedb-rust`, `../tracedb-python`, and
`../tracedb-js`.

Endpoint diagnostics are available without mutating data:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --timeout-ms 1000 --safe-retries 1 --wait-ready-ms 5000 --database-id db_local --branch-id db_local:main
```

The HTTP doctor checks health, readiness, catalog, public-safe metrics, and
admin-jobs routes and reports `sql_module: not_implemented`. Failed checks
include parsed `server_error` and `server_error_code` fields when an endpoint
returns the current coded JSON error shape. Optional `--database-id` and
`--branch-id` add managed-routing metadata for gateway diagnostics, including
the bodyless admin-jobs route. Optional `--wait-ready-ms` polls readiness before
the normal checks and reports the readiness wait in the JSON summary. The
command exits non-zero when any check fails while keeping the JSON summary on
stdout. It is a local/managed-style endpoint diagnostic, not a SQL probe,
benchmark, or managed deployment proof.

For CI or deployed endpoint checks, the doctor can read the same endpoint
configuration from `TRACEDB_URL`, `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`,
`TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_WAIT_READY_MS`:

```bash
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 TRACEDB_WAIT_READY_MS=5000 cargo run -p tracedb-cli -- doctor http
```

## Railway-Lab Evidence

The current live Railway lab receipts live in the sibling
`../tracedb-benchmarks` repository under `benchmarks/realworld/reports/`:

- `railway-live-preflight-20260602T015455Z` reached
  `suite-gate.status=usable` with endpoint health, stateful marker write/read,
  admin snapshot/restore, and restored-marker read checks passed.
- `railway-live-postrestart-20260602T015850Z` reached
  `suite-gate.status=usable` with `railway_persistence=passed` after a live
  `tracedb-engine` Railway restart and read-only marker check.
- `railway-live-preflight-20260602T015455Z/railway-runbook-verification.json`
  records `status=complete` for the preflight, pre-operation marker, operation
  receipt, and post-operation marker evidence chain.
- `railway-live-final-20260602T020051Z` reached `suite-gate.status=usable`
  with `railway_runbook_verification=complete`.

This is internal Railway-lab evidence for the hosted shape. It is not a public
production/SLA claim, not managed backup/DR proof, and not a performance win:
external controls and backup receipts remain unchecked.

The SDK quickstart uses typed convenience response methods, including typed
health, catalog, metrics, admin-jobs, single-record put, batch ingest, patch,
and query rows, over the current HTTP JSON shapes. It accepts `--timeout-ms` for
the blocking SDK request timeout, `--safe-retries` for bounded health/read route
retries, `--idempotency-retries` / `TRACEDB_IDEMPOTENCY_RETRIES` for
idempotency-key-gated write/admin retries, and optional `--admin-dir` to run
compact/snapshot/restore against an absolute server-side local scratch
directory. The quickstart checks diagnostics before the write path, writes one
record through typed `put`, batch-ingests two more records, patches a record,
verifies patched visibility before scan/query/delete, and generates per-run
idempotency keys for those mutations when idempotency retries are enabled. It
also intentionally exercises one non-2xx server response and reports the parsed
`error_envelope` so SDK error behavior is conformance-visible. The admin path is
interpreted by the
`tracedb-server` process, and restore creates a separate database directory
instead of replacing the running server. Restore requests may include
`verify_record` to read a marker from the restored target and return a
`verification` object without promoting that target to the live service.
`TraceDbRequestOptions` can manually attach `Idempotency-Key` to individual
write/admin requests. The SDK also exposes typed local admin helpers for
compact, snapshot, and restore. The quickstart JSON summary now uses the same
operator-facing envelope shape as the TypeScript endpoint quickstart: `mode`,
`server_url`, optional `database_id` / `branch_id`, `table`, `tenant_id`, and a
structured `admin` object for requested/skipped compact, snapshot, and restore.
Invalid quickstart configuration, including malformed URLs, invalid retry counts,
and non-absolute admin scratch paths, exits nonzero but still emits a parseable
`ok: false` JSON summary on stdout with `error.kind`, `error.message`, false
step statuses, and `sql_module: not_implemented`.
SQL compatibility remains unimplemented.

The current versioned HTTP API route reference is `docs/api/v1-http.md`. The
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`, generated by
`scripts/generate_openapi_v1.py`. During the split, `../tracedb-protocol` is the
canonical contract repository for `platform-contract-v0`, HTTP `/v1` reference
docs/OpenAPI, and conformance harness behavior; the core repo keeps local
mirrors only where core validation still needs them. SDK clients, SDK
quickstarts, generated transport artifacts, package checks, and SDK conformance
evidence live in the sibling standalone repositories `../tracedb-rust`,
`../tracedb-python`, and `../tracedb-js`.
Managed-routing SDK helpers default `branch_id` to `<database_id>:main` when a
configured `database_id` has no explicit branch and the copied request body does
not already define one.

Native TraceQL now executes through the canonical HTTP surface:
`POST /v1/traceql` accepts `{ "query": string }`, parses line-oriented `FROM`,
`TENANT`, `WHERE`, `MATCH`, `NEAR`, `FRESHNESS`, `LIMIT`, and `EXPLAIN`
directives with `traceql_query_from_str`, and compiles them into the existing
`HybridQuery` model before returning the same result shape as `POST /v1/query`.
The same parser also accepts the bounded SQL-ish adapter form
`EXPLAIN? SELECT * FROM <table> WHERE tenant_id = <value> [AND field = value]*
[LIMIT n]`, compiling it into the same `HybridQuery` path and returning
`invalid SQL-ish` bad-request errors for unsupported constructs such as `JOIN`.
The conformance harness now includes `traceql_string_execution`; HTTP direct
passes that scenario through `/v1/traceql`, the Rust SDK lane passes it through
`TraceDbClient::traceql_typed`, the TypeScript SDK lane passes it through
`TraceDB.traceql()`, and the Python SDK lane passes it through the sync
`TraceDB.traceql()` helper. It also has a dedicated partial `traceql_sqlish`
lane that checks bounded SQL-ish query, explain, and error behavior while
leaving schema/write/admin scenarios `not_checked`. This is
TraceQL/query-adapter execution evidence only; SQL compatibility and PostgreSQL
compatibility remain unimplemented. `GET /v1/graphql/schema` exports generated
SDL from applied TraceDB table schema, `POST /v1/graphql` exposes native
GraphQL `data`/`errors` operations, and `POST /v1/graphql/bounded` preserves the
bounded adapter over the same `HybridQuery` model. The Rust SDK exposes the generated
schema route through `TraceDbClient::graphql_schema`,
`TraceDbClient::graphql_schema_typed`, and
`TraceDbAsyncClient::graphql_schema_typed`, native execution through
`TraceDbClient::graphql_typed`, and compatibility execution through
`bounded_graphql_typed`. The TypeScript SDK exposes schema export through
`TraceDB.graphqlSchema()`, native execution through `TraceDB.graphql()`, and
compatibility execution through `TraceDB.boundedGraphql()`; the Python SDK
mirrors those as `TraceDB.graphql_schema()`, `TraceDB.graphql()`, and
`TraceDB.bounded_graphql()`. GraphQL subscriptions remain unsupported.
Mutation and admin routes accept optional `Idempotency-Key` for local
data-dir-backed engine replay, and the gateway forwards that header. Replay
survives a clean engine reopen from the same data directory through
WAL/checkpoint-backed idempotency receipts. Cross-replica idempotency,
crash-atomic exactly-once semantics, and managed-cloud exactly-once guarantees
remain future work. SDK write/admin retries are opt-in, bounded, transient-only,
and require an idempotency key.
