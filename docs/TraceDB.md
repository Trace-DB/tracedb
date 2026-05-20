---
title: TraceDB
aliases:
  - TraceDB Home
tags:
  - tracedb
  - docs
status: stub
type: repo-handoff
updated: 2026-05-19
---

# TraceDB

Canonical TraceDB architecture, strategy, benchmark, implementation, operations, and decision notes now live in the Grogan Development Vault:

```text
/Users/zgrogan/Repos/grogan-development-vault/10_Projects/TraceField Suite/TraceDB/
```

This repo-local file is intentionally a stub so code, tests, and handoff paths do not depend on the vault checkout.

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
```

It emits one machine-readable `local-product-regression` summary for the
embedded demo/verify path, `http-demo`, local `doctor http`, Rust SDK
quickstart, and generated TypeScript check/http/gateway smoke paths. It is
local product regression evidence only: SQL remains not implemented,
managed-cloud is not checked, and benchmarks are not checked. The local product
regression gate also has test-only `--inject-failure STEP` coverage for
nonzero process status and machine-readable failed-step JSON. Use
`--list-steps` to print the machine-readable list of product-regression step
names for operators and CI wiring without executing any product checks.

The local HTTP plus SDK smoke is also available as one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

It starts a loopback `tracedb-server` child process, drives the Rust SDK over
HTTP through ready, schema apply, batch ingest, scan, query, explain, delete,
compact, snapshot, and restore, uses keyed SDK write/admin retries for
mutation/admin steps, and reports `sql_module: not_implemented`. This proves the
local HTTP SDK product path; it does not claim managed-cloud deployment health,
backup/DR semantics, cross-replica idempotency, or SQL compatibility.

The SDK quickstart is also runnable against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo/data TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000 --safe-retries 1 --idempotency-retries 1 --admin-dir /tmp/tracedb-sdk-demo/admin
```

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

The SDK quickstart uses typed convenience response methods, including typed
query rows, over the current HTTP JSON shapes. It accepts `--timeout-ms` for the
blocking SDK request timeout, `--safe-retries` for bounded health/read route
retries, `--idempotency-retries` / `TRACEDB_IDEMPOTENCY_RETRIES` for
idempotency-key-gated write/admin retries, and optional `--admin-dir` to run
compact/snapshot/restore against an absolute server-side local scratch
directory. The admin path is interpreted by the `tracedb-server` process, and
restore creates a separate database directory instead of replacing the running
server. `TraceDbRequestOptions` can manually attach `Idempotency-Key` to
individual write/admin requests; the quickstart generates per-run keys for its
write/admin steps when idempotency retries are enabled. The SDK also exposes
typed local admin helpers for compact, snapshot, and restore. SQL compatibility
remains unimplemented.

The current versioned HTTP API route reference is `docs/api/v1-http.md`. The
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`, generated by
`scripts/generate_openapi_v1.py`.
The Rust SDK exposes `TraceDbAsyncClient` as a first async facade over the
current HTTP API. It uses a background thread per request and preserves the
blocking client's timeout, retry, managed-routing, and error semantics. It now
exposes async typed write/admin helpers for schema apply, record
put/batch/patch/delete, compact, snapshot, and restore, including option-aware
idempotency helpers. This is an async integration surface for the current
product path, not a final runtime-native Tokio/async-std transport.
The checked generated TypeScript `fetch` client artifact is
`clients/typescript/src/client.ts`, generated by
`scripts/generate_typescript_client.py` from the OpenAPI artifact. It covers the
current v1 HTTP routes, can add configured `database_id` and `branch_id` to
copied JSON POST bodies when absent, and can send caller-supplied
`Idempotency-Key` through `TraceDbRequestOptions`. It now includes
OpenAPI-derived schema aliases and typed method signatures for the current HTTP
surface while preserving the permissive `additionalProperties` boundary: known
fields are optional, unknown JSON fields remain allowed, and runtime validation
stays server-side. The generated `RecordPutBody` alias matches the current
server route by allowing either direct `RecordInput` or `{ record: RecordInput
}`, and `GetRecordResponse.record` is now typed as `RecordOutput | null` with
the serialized `version_id` field. `HybridQuery` now explicitly includes
`scalar_eq`, `graph_seed`, and `temporal_as_of` request fields. `RecordScanOutput`,
`QueryResponse`, `HybridQueryRow`, `HybridScoreComponents`, and `HybridExplain`
now expose the current server response shape for scan/query/explain, including
access-path, candidate, counter, and timing explain fields. Health, readiness,
catalog, metrics, and admin-jobs responses now have concrete generated aliases
as well; fields stay optional where local-engine and gateway shapes differ. It
is not a published npm package, not a hand-maintained managed SDK, not a strict
runtime validator, and not a SQL compatibility claim.
`node --experimental-strip-types clients/typescript/smoke.ts` verifies the
artifact imports and executes in the local Node runtime with fake-fetch coverage
for representative generated aliases, GET no-body behavior, POST routing
metadata injection, explicit routing field precedence, idempotency headers, and
HTTP error shape, including parsed `{ "error": string, "code"?: string }`
envelopes on `TraceDbHttpError`. Stable machine-readable error `code` values
are preserved when present. It rejects empty or CR/LF-containing
`idempotencyKey` request options before `fetchImpl` as `TraceDbRequestError`.
`cd clients/typescript && npm ci && npm run check` installs the locked private
tooling and typechecks the generated artifact plus smoke script. The package is
private and does not declare publishing fields. `cd clients/typescript && npm
run http-smoke` starts a local `tracedb-server` child process with an isolated
temporary data directory and drives the generated TypeScript client over real
HTTP routes for ready, health, catalog, metrics, schema apply, direct put, batch
ingest, get, scan, query, explain, delete, compact, snapshot, restore, and admin
jobs. `TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run
quickstart` runs the generated TypeScript client against an existing HTTP
endpoint. Optional `TRACEDB_DATABASE_ID` / `TRACEDB_BRANCH_ID` add managed
routing metadata, and optional absolute `TRACEDB_ADMIN_DIR` enables local
compact/snapshot/restore against server-side scratch paths. The quickstart
reports `sql_module: not_implemented` and is endpoint example evidence, not a
package publishing claim, SQL compatibility, managed-cloud backup/DR, or
benchmark evidence. `cd clients/typescript && npm run gateway-smoke` starts a
local engine plus gateway-mode server with `TRACEDB_REQUIRE_API_KEY=true`,
`TRACEDB_API_TOKEN=dev-token`, and `TRACEDB_ENGINE_URL` pointing at the engine,
proves missing-token `401` and bad-branch `400` enforcement, then runs the
endpoint quickstart through the gateway with `TRACEDB_DATABASE_ID=db_local` and
`TRACEDB_BRANCH_ID=db_local:main`. This is local gateway auth/routing evidence
for the generated artifact, not managed-cloud proof or benchmark evidence.
Mutation and admin routes accept optional `Idempotency-Key` for local
data-dir-backed engine replay, and the gateway forwards that header. Replay
survives a clean engine reopen from the same data directory after a successful
local cache write; filesystem cache-write failures are logged and do not roll
back the original mutation. Cross-replica idempotency, crash-atomic exactly-once
semantics, and managed-cloud exactly-once guarantees remain future work. SDK
write/admin retries are opt-in, bounded, transient-only, and require an
idempotency key.
