# TraceDB

[![License: FSL-1.1-ALv2](https://img.shields.io/badge/license-FSL--1.1--ALv2-blue)](LICENSE)
[![Protocol: platform-contract-v0](https://img.shields.io/badge/protocol-platform--contract--v0-informational)](docs/platform-contract-v0.md)
[![Docs](https://img.shields.io/badge/docs-trace--db.com-informational)](https://docs.trace-db.com)

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

This repository is the downloadable TraceDB database distribution. The core
engine is source-available under FSL-1.1-ALv2 with an Apache-2.0 future grant;
see `LICENSE` for the exact terms. It is public product code, not the
proprietary hosted TraceDB service implementation.

The current core product surface is a local/embedded engine with CLI, HTTP,
direct adapters, an OpenAPI mirror, product quickstart, durability semantics,
and protocol-lock validation. Native views currently means record/table writes
plus lexical, vector, graph, temporal, freshness, policy, and explainable
candidate streams over the same committed records, not external sync jobs.
The authoritative protocol contract lives in `../tracedb-protocol`; standalone
SDKs live in `../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`;
benchmark/proof harnesses live in `../tracedb-benchmarks`.

Hosted TraceDB is a separate proprietary service that runs a stable TraceDB
deployment behind the public API. The hosted service uses the same HTTP
contract and SDKs documented here, but its operator console, control plane,
deployment automation, account management, and production operations code are
not part of this repository.

TraceField is the memory/runtime research program that informs future runtime
directions. It is not the current product and is not an implemented runtime in
this repo. Agent Memory Flight Recorder is a concrete local demo wedge built on
TraceDB records, query/explain output, and replayable receipts; it is not the
product identity. Tensor artifacts are future governed derived-artifact/module
work; TraceDB does not currently provide tensor compute or tensor storage
services.
`crates/tracedb-memory-runtime` is placeholder/scaffolding only; memory calculus
is not implemented.

The DX-facing platform contract starts at `docs/platform-contract-v0.md`, with
the machine-readable conformance manifest in `docs/platform-contract-v0.json`.
This is the checklist future SDKs and adapters must pass before TraceDB claims
maintenance-mode parity across Rust, TypeScript, Python, TraceQL/SQL-ish, and
GraphQL.

The current local durability boundary is `docs/durability-semantics-v0.md`.
It states the WAL, manifest, checkpoint, snapshot/restore, lock-file, and
WAL/checkpoint-backed idempotency semantics for the local-first engine,
including the current non-guarantees around cross-replica idempotency,
crash-atomic exactly-once, and managed-cloud backup/DR.

The current HTTP stack boundary is also explicit. `tracedb-server` exposes the
local engine HTTP product path with Tokio/Axum, Tower body limits, timeouts,
load shedding, concurrency limits, graceful shutdown, and structured JSON
tracing. The server uses an async handle with serialized writes/admin work and
cheap read snapshots. Legacy stdlib listener helpers remain for compatibility
tests and local harnesses. The current server path does not provide TLS or
HTTP/2 and is not a complete managed-service runtime.

Run the core executable contract harness with:

```bash
python3 scripts/platform_conformance.py --surface http_direct --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface traceql_sqlish --summary-json /tmp/tracedb-traceql-sqlish-conformance.json
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
python3 scripts/validate_protocol_locks.py --repo-root ..
```

It reads `docs/platform-contract-v0.json`, drives a raw HTTP `http_direct` lane
against `tracedb-server`, runs direct adapter lanes for TraceQL/SQL-ish and
GraphQL, and validates that local `tracedb-protocol.lock` files stay pinned to
the protocol repo. SDK conformance lanes remain declared in the mirror so
sibling SDK repos can compare against the same scenario IDs, but they are owned
and run from the standalone SDK repos.
GraphQL has a generated SDL export from applied `TableSchema` definitions
through `GET /v1/graphql/schema`, plus a bounded `graphql_query_from_str`
compiler primitive in `tracedb-query` and a bounded `POST /v1/graphql` HTTP
adapter that compiles the query string into the same `HybridQuery` path as
`/v1/query`. The `graphql` conformance lane checks schema export, query,
explain, and error-envelope behavior, while write/admin scenarios remain
explicit `not_checked` results. This is not GraphQL mutation support,
subscription support, resolver runtime, GraphQL data-envelope execution, or
full adapter parity. Rust SDK callers can use `TraceDbClient::graphql_schema`
or `graphql_schema_typed` to inspect the generated SDL, then
`TraceDbClient::graphql_typed` or `graphql_request_typed` with
`GraphQlQueryRequest` to exercise the bounded query adapter. TypeScript SDK
callers can use `TraceDB.graphqlSchema()` to inspect the generated SDL, then
`TraceDB.graphql()` or `graphqlRequest({ query })`; Python SDK callers can use
`TraceDB.graphql_schema()` to inspect the generated SDL, then
`TraceDB.graphql()` or `graphql_request({"query": query})` to exercise the same
bounded wire contract.

## Quickstart

Run the local product demo from the repo root:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
```

The command exercises the embedded engine path end to end:

- opens a local TraceDB data directory
- applies a demo schema
- batch-ingests records
- runs hybrid query and explain
- scans and deletes records
- compacts
- creates and restores a snapshot
- reports that SQL is not implemented

Verify the same data directory afterward:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

If local macOS Rust binaries hang before printing CLI output, classify the
machine-level launch state before treating it as a TraceDB runtime failure:

```bash
python3 scripts/local_rust_launch_doctor.py
```

Run the local HTTP product smoke with one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

The command starts a local loopback `tracedb-server` child process, drives the
HTTP product path through ready, schema apply, batch ingest, scan, query,
explain, delete, compact, snapshot, and restore, then emits a JSON summary. It
uses `Idempotency-Key` for mutation/admin steps and still reports `sql_module:
not_implemented`. This is local product-path evidence, not managed-cloud
deployment or backup/DR evidence; see
`docs/durability-semantics-v0.md` for the precise local durability boundary.

Run the consolidated local product regression gate:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
cargo run -p tracedb-cli -- durability-faults
```

The product gate emits one JSON summary with `mode:
"local-product-regression"`, `scope: "local_only"`, a compact top-level
`human_summary`, and explicit `not_checked` markers for managed-cloud and
benchmark claims. It orchestrates the core embedded demo/verify path,
`http-demo`, and local `doctor http` diagnostics. SDK conformance is externally
owned by `../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`. The core
product gate is local product regression evidence, not SQL compatibility,
managed-cloud proof, SDK conformance, or benchmark evidence.

For CI failure-path coverage, use test-only `--inject-failure STEP` to verify
that the gate exits nonzero while still emitting the failed-step JSON summary.
Use `--report-file PATH` to write the same JSON summary to a predictable file
while preserving JSON stdout for automation; this applies to full runs,
`--only`, `--inject-failure`, and `--list-steps`, and creates parent
directories. Use `--list-steps` to print JSON step metadata, including
`human_summary` and `only_supported`, for operator and CI wiring without running
demo, HTTP, or SDK smoke steps.

`product-quickstart` is the same local core product gate with a default report
file at `target/tracedb/product-quickstart.json`; it accepts the same
product-regression options, including `--only`, still writes JSON to stdout, and
includes a top-level `report_file` field when a report artifact is configured.
Use `durability-faults` for the local durability closeout receipt at
`target/tracedb/durability-faults.json`; it reports `mode:
"local-durability-faults"` and `claims.tde_scope:
"local_artifacts_when_configured"` for the TDE/WAL recovery scenarios. A
copy-paste local receipt check is:

```bash
cargo run -q -p tracedb-cli -- product-quickstart
python3 - <<'PY'
import json
from pathlib import Path

receipt = Path("target/tracedb/product-quickstart.json")
summary = json.loads(receipt.read_text())
assert summary["ok"] is True
assert summary["mode"] == "local-product-regression"
assert summary["scope"] == "local_only"
assert summary["report_file"] == str(receipt.resolve())
assert summary["human_summary"]["status"] == "passed"
assert summary["claims"] == {
    "sql_module": "not_implemented",
    "managed_cloud": "not_checked",
    "benchmark": "not_checked",
}
print(summary["human_summary"]["message"])
print(summary["report_file"])
PY
```

When local executable policy or machine resources block product verification,
run the same quickstart path on Modal as remote Linux product verification:

```bash
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

The Modal runner uploads the current checkout with `.git`, `target/`, local env
files, benchmark reports, caches, and Node modules excluded, then runs `cargo
fmt --all -- --check`, the focused quickstart receipt test, the docs contract
test, and `product-quickstart`. It validates
`target/tracedb/product-quickstart.json` against stdout and confirms the core
receipt has SQL as `not_implemented` and managed-cloud/benchmark claims as
`not_checked`. Use `--mode workspace` for the heavier remote lane that also runs
the full CLI demo test file, usability acceptance test file, and `cargo test
--workspace --all-targets`. This is remote Linux product verification; it does
not replace a final macOS-local quickstart check when the question is whether
this workstation can execute binaries.

To validate the failure receipt path without waiting for the full gate, use:

```bash
cargo run -q -p tracedb-cli -- product-quickstart --inject-failure embedded_demo
```

The command exits nonzero, still writes
`target/tracedb/product-quickstart.json`, preserves the top-level `report_file`
field, reports `human_summary.status: "failed"`, and records
`failure_injection: "embedded_demo"` with an injected failed `embedded_demo`
step. For narrow local iteration,
`--only embedded_demo` currently runs just the embedded demo step and emits the
normal one-step `local-product-regression` JSON summary. After that, use the
same `--data-root` with `--only embedded_verify` to verify the existing
embedded demo data without running HTTP, SDK, or TypeScript steps.
`--only http_demo` runs the self-contained local HTTP demo step and emits the
normal one-step `local-product-regression` JSON summary. It does not run local
`doctor http`, SDK conformance, managed-cloud checks, benchmark controls, or
SQL compatibility checks.
`--only local_doctor` starts a managed-style local loopback `tracedb-server`
child process and runs only the existing local `doctor http` product-regression
step with readiness wait, `database_id`, and `branch_id` metadata. It emits the
normal one-step `local-product-regression` JSON summary with `only_step:
"local_doctor"`. This is local endpoint diagnostics evidence only; it does not
run `http_demo`, SDK conformance, managed-cloud checks, benchmark controls, or
SQL compatibility checks.

SDK conformance is no longer part of the core product-regression gate. Run SDK
quickstarts, package checks, HTTP smokes, and SDK conformance in the sibling
standalone repositories: `../tracedb-rust`, `../tracedb-python`, and
`../tracedb-js`. The core repo should not shell into legacy in-tree SDK locations or SDK package commands.

For a read-only endpoint diagnostic against that running server:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --timeout-ms 1000 --safe-retries 1 --wait-ready-ms 5000 --database-id db_local --branch-id db_local:main
```

The HTTP doctor checks the current health, readiness, catalog, public-safe
metrics, and admin-jobs routes, returns a JSON summary with per-route responses
or parsed error envelopes, including `server_error` and `server_error_code`
when an endpoint returns the current coded JSON error shape, and reports
`sql_module: not_implemented`. Optional `--database-id` and `--branch-id` add
managed-routing metadata for gateway diagnostics, including the bodyless
admin-jobs route. Optional `--wait-ready-ms` polls readiness before the normal
checks, reports `ready_wait_timeout_ms` and `ready_wait`, and keeps immediate
post-start local checks scriptable. The command exits non-zero when any check
fails while keeping the JSON summary on stdout. It is a local/managed-style
endpoint diagnostic, not a SQL probe or benchmark.

For CI or deployed endpoint checks, the same command can read endpoint config
from environment variables instead of flags:

```bash
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 TRACEDB_WAIT_READY_MS=5000 cargo run -p tracedb-cli -- doctor http
```

The current versioned HTTP route reference is in `docs/api/v1-http.md`; the
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`. SDK clients and
SDK quickstarts are maintained in sibling standalone repositories:
`../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`. Those repos own
their package metadata, generated/client artifacts, SDK smokes, and SDK
conformance evidence against this core HTTP contract.
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
The platform conformance harness now includes `traceql_string_execution`; HTTP
direct passes that scenario through `/v1/traceql`; SDK lanes in the sibling
standalone repos map the same scenario through their public clients. The dedicated `traceql_sqlish`
lane checks the bounded SQL-ish adapter against the same scenario manifest as a
partial surface. `GET /v1/graphql/schema` now exports generated SDL from
applied TraceDB table schema, and `POST /v1/graphql` exposes a bounded GraphQL
query adapter over the same `HybridQuery` model. The GraphQL conformance lane
checks schema export, query, explain, and error behavior. Standalone SDK repos expose their own typed helpers for GraphQL schema export,
native GraphQL operations, and bounded GraphQL adapter execution. This is TraceQL/query-adapter,
GraphQL SDL export, and bounded GraphQL query-adapter evidence only; SQL compatibility,
PostgreSQL compatibility, GraphQL mutation support, subscription support,
resolver runtime, GraphQL data-envelope execution, and full adapter parity
remain unimplemented.

The OpenAPI artifact is the source for generated SDK transport types in the
standalone SDK repos. Core server-side runtime validation remains authoritative
for schema identifiers, duplicate columns, index declarations, reserved result
metadata fields, and vector source columns before WAL append.

## Current Boundaries

- SQL compatibility is not implemented.
- SDK implementations are external to this core repo. Rust SDK work lives in
  `../tracedb-rust`; Python SDK work lives in `../tracedb-python`; and
  TypeScript/JavaScript SDK work lives in `../tracedb-js`. Core docs and
  contract manifests may point to those sibling repos as externally owned
  evidence, but core product regression does not run SDK package checks or SDK
  smokes.
- HTTP mutation and admin routes accept optional `Idempotency-Key` for local
  data-dir replay from WAL/checkpoint-backed idempotency receipts on the
  engine, and the gateway forwards that header. Replay survives a clean engine
  reopen from the same data directory. This is not cross-replica, not
  crash-atomic exactly-once, and not a managed-cloud exactly-once guarantee.
  The Rust SDK can manually send the header per request through
  `TraceDbRequestOptions`, and opt-in SDK idempotent retries require that
  header.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require external controls and a number to beat.
