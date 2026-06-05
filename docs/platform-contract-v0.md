---
title: TraceDB Platform Contract v0
tags:
  - tracedb
  - platform-contract
  - sdk
status: contract-freeze-draft
type: platform-contract
updated: 2026-05-22
---

# TraceDB Platform Contract v0

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

This is the DX-facing contract that SDKs and future adapters must converge on.
It sits above the current HTTP route inventory and below language-specific SDK
ergonomics:

```text
TraceDB SDK Contract
  -> canonical HTTP/wire contract
  -> shared TraceQuery/result/error model
  -> Rust reference SDK
  -> TypeScript platform SDK
  -> Python AI/data SDK
  -> TraceQL / SQL-ish adapter
  -> GraphQL adapter
```

MCP is optional glue later. It does not drive this architecture.
TraceField is the memory/runtime research program that informs future runtime
directions, not the current product or an implemented runtime in this repo.
Agent Memory Flight Recorder is a concrete local demo wedge built on TraceDB
records, query/explain output, and replayable receipts; it is not a conformance
surface yet. Tensor artifacts are future governed derived-artifact/module work,
not current tensor compute or tensor storage services.
`crates/tracedb-memory-runtime` is placeholder/scaffolding only; memory calculus
is not implemented.

The companion machine-readable manifest is `docs/platform-contract-v0.json`.
The current wire contract is `docs/api/v1-http.md`; the current generated route
artifact is `docs/api/v1-openapi.json`.
The local durability boundary is `docs/durability-semantics-v0.md`.
The initial executable conformance runner is `scripts/platform_conformance.py`.
During the repo split, `../tracedb-protocol` is the authoritative source for
`platform-contract-v0` and the HTTP `/v1` contract artifacts. This core copy is
a validation mirror pinned by `tracedb-protocol.lock`.

## Boundaries

- Status is `contract-freeze-draft`. During the split this file is the core
  validation mirror for the protocol-owned source-of-truth checklist, not a
  managed-cloud SLA.
- SQL compatibility is not implemented; the bounded SQL-ish `SELECT` adapter
  under `/v1/traceql` is not a SQL engine.
- TraceDB is not PostgreSQL-compatible. Future SQL-ish work must compile into
  TraceDB's native query model instead of becoming a PostgreSQL emulation layer.
- GraphQL has a native `data`/`errors` envelope through `POST /v1/graphql`
  for TraceDB query, mutation, and admin operations. The bounded GraphQL query
  adapter remains compatibility-only at `POST /v1/graphql/bounded`.
  Subscriptions remain unsupported.
- Exported performance claims still require an external control and a number to
  beat. Internal TraceDB-only runs are development evidence only.
- Local WAL/checkpoint/snapshot behavior is governed by
  `docs/durability-semantics-v0.md`; it is not a managed-cloud SLA, not
  cross-replica idempotency, and not crash-atomic exactly-once semantics.
- The current HTTP stack boundary is local/development product proof:
  `tracedb-server` and `tracedb-gateway` default to Tokio/Axum product paths
  with Tower body limits, timeouts, load shedding, concurrency limits, graceful
  shutdown, structured JSON tracing, and private engine-token enforcement where
  configured. Legacy stdlib listener helpers remain for compatibility tests and
  local harnesses. The HTTP routes remain the canonical wire contract, but the
  server implementation does not provide TLS or HTTP/2 and is not a complete
  managed-service runtime.

## Developer Model

Every product surface must map to these contract components:

| Component | Contract |
| --- | --- |
| `connection_config` | URL, token, timeout, retry policy, and optional local/admin paths belong in connection configuration, not per-call reinvention. |
| `database_branch_config` | Managed database and branch routing use `database_id` and `branch_id` metadata. Direct local-engine calls can omit them. SDKs default `branch_id` to `<database_id>:main` on copied object-shaped POST bodies when a configured database omits an explicit branch. |
| `table_handles` | SDKs should expose table-scoped handles so application code can bind a table once before writes and queries. |
| `schema_migrations` | v0 requires schema apply. Migration planning/versioning is future but must remain part of the contract vocabulary. |
| `record_writes` | Single-record put/patch/delete must share record identity, tenant identity, and field semantics across surfaces. |
| `batch_ingest` | Batch writes are first-class and should preserve `record_count`, epoch, and optional write timing where exposed. |
| `query_builder` | SDK builders should compile into the same `HybridQuery`/TraceQuery model as direct JSON calls, including preserving text/vector field selection as `text_field` and `vector_field` instead of treating SDK field arguments as placeholders. They also canonicalize `strict`, `lazy`, and `allow_dirty` freshness inputs to the `Strict`, `Lazy`, and `AllowDirty` wire modes used by direct HTTP, TraceQL, native GraphQL, and the bounded GraphQL adapter. |
| `traceql_string_execution` | Native TraceQL strings, TraceDB command statements, and the bounded SQL-ish `SELECT * FROM ... WHERE tenant_id = ... [AND field = value]* [LIMIT n]` adapter execute through `POST /v1/traceql` after compiling into TraceDB's operation/query model; neither form is a separate SQL engine. |
| `result_envelope` | Success responses are route-specific JSON; errors preserve the current `{ "error": string, "code"?: string }` envelope plus SDK context. |
| `explain_provenance_freshness_jobs` | Query/explain surfaces share `HybridExplain` fields for access paths, planner candidates, counters, timings, and freshness/provenance evidence as they mature. |
| `errors_retries_idempotency` | Safe retries stay read-only. Mutation/admin retries require caller-provided `Idempotency-Key`; same key/body replays, body mismatch returns `409`. |
| `pagination_cursors` | Scan/query pagination returns `next_cursor` when additional results exist. Production routes can use opaque signed actor/query-bound cursors when cursor signing is configured, with legacy offset parsing retained for compatibility. |
| `admin_compact_snapshot_restore` | Compact, snapshot, restore, and admin-jobs visibility are part of the platform path, with local filesystem boundaries explicit. |

## Current Surface Matrix

| Surface | ID | Current status | Contract role |
| --- | --- | --- | --- |
| HTTP direct | `http_direct` | Current | Canonical wire contract. |
| Rust SDK | `rust_sdk` | Reference candidate with env config and row batch ingestion | Ergonomic reference SDK over the wire contract while preserving raw HTTP methods. |
| TypeScript SDK | `typescript_sdk` | Public wrapper conformance checked with raw-contract and row batch ingestion, env config, safe retries, and idempotency retries | Hand-written `TraceDB` table/query wrapper over the generated transport. |
| Python SDK | `python_sdk` | Sync HTTP smoked from installed package with row batch ingestion, native TraceQL, safe retries, and idempotency retries | Sync-first AI/data/notebook SDK over the canonical HTTP contract. |
| TraceQL | `traceql` | Native TraceDB command/query statements checked for every blocking scenario | Native command/query surface over the canonical operation model. |
| TraceQL / SQL-ish | `traceql_sqlish` | Bounded SQL-ish `SELECT` adapter checked as compatibility lane | Adapter into the same TraceQuery/query model, not SQL compatibility. |
| GraphQL | `graphql` | Native `POST /v1/graphql` data/errors surface checked for every blocking scenario | Native production API over the canonical operation model. |

Maintenance mode means a platform project can use TraceDB through Rust, TypeScript, Python, TraceQL/SQL-ish, or GraphQL and receive the same behavior, same errors, same result shape, and same explain/freshness semantics.

## Conformance Harness v0

The first harness should use the machine-readable manifest as the shared
scenario list. Each surface reports pass/fail/skipped for the same scenario IDs
and must not invent surface-specific semantics.

| Scenario | ID | Current wire path | Required behavior |
| --- | --- | --- | --- |
| Schema apply | `schema_apply` | `POST /v1/schema/apply` | Applies `TableSchema` and returns an epoch. Schema validation rejects non-GraphQL-safe identifiers, duplicate columns, overlapping scalar/text/vector columns, reserved TraceDB result metadata fields, and invalid vector source columns before WAL append. |
| Put | `put` | `POST /v1/records/put` | Writes or replaces one record and returns an epoch. |
| Batch ingest | `batch` | `POST /v1/records/put-batch` | Writes multiple records and returns `record_count` plus epoch. |
| Patch | `patch` | `POST /v1/records/patch` | Updates selected fields while preserving untouched fields. |
| Get | `get` | `POST /v1/records/get` | Returns `RecordOutput` or `null`. |
| Scan | `scan` | `POST /v1/records/scan` | Returns `records` and `returned_count`; cursor metadata is future. |
| Query | `query` | `POST /v1/query` | Returns `HybridQueryRow` results with typed score components. |
| TraceQL string execution | `traceql_string_execution` | `POST /v1/traceql` | Parses native TraceQL strings into the shared `HybridQuery` model and preserves query result/error envelope behavior. |
| Explain | `explain` | `POST /v1/explain` | Returns `HybridExplain` access paths, planner candidates, counters, and timings. |
| Delete | `delete` | `POST /v1/records/delete` | Hides deleted records from get, scan, query, and explain materialization. |
| Idempotency | `idempotency` | `Idempotency-Key` on mutation/admin routes | Same key plus same method/path/body replays; same key with different body returns `409`. |
| Errors | `errors` | Current JSON error envelope | SDKs expose status/method/path/body plus parsed `error` and optional `code`. |
| Snapshot/restore | `snapshot_restore` | `POST /v1/admin/snapshot`, `POST /v1/admin/restore` | Snapshot local state, restore into a separate target, and optionally verify a restored record with `verify_record`. |

Future harness lanes: `pagination_cursors` and `durable_jobs`. These stay out
of the v0 pass/fail contract until the engine exposes concrete behavior.

Run the current executable lanes with:

```bash
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/platform_conformance.py --surface traceql --summary-json /tmp/tracedb-traceql-conformance.json
python3 scripts/platform_conformance.py --surface traceql_sqlish --summary-json /tmp/tracedb-traceql-sqlish-conformance.json
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
```

The `http_direct` lane uses raw stdlib HTTP requests against `tracedb-server`
and now checks all 13 current v0 scenario IDs, including native
`traceql_string_execution` through `POST /v1/traceql`. SDK conformance lanes are
externally owned by sibling standalone repositories: `../tracedb-rust`,
`../tracedb-js`, and `../tracedb-python`. Those repos map the same manifest
scenario IDs through their public clients and contribute SDK evidence back to
this contract through external evidence paths. The core repo validates its
HTTP, TraceQL, GraphQL, and SQL-ish compatibility lanes against the protocol
lock; it does not shell into SDK packages or in-tree SDK paths for product
regression.

The `traceql` lane starts the HTTP server and exercises native TraceDB command
statements for all 13 scenario IDs. The `graphql` lane starts the HTTP server
and exercises native GraphQL operations for all 13 scenario IDs. The
`traceql_sqlish` lane starts the HTTP server, seeds minimal records through
canonical wire calls, and checks the bounded SQL-ish adapter through
`/v1/traceql`. It reports `query`, `traceql_string_execution`, `explain`, and
`errors` as passed, while schema/write/admin scenarios remain explicit
`not_checked` results. Future surfaces must report unimplemented scenarios as
`not_checked` rather than silently treating them as success. The bounded GraphQL
compatibility lane remains available through `POST /v1/graphql/bounded`, but it
is no longer the production GraphQL gate.

Current verified SDK checkpoints are tracked in their standalone repos. This
core validation mirror keeps SDK surface IDs so conformance can be compared across
languages, but evidence paths for `rust_sdk`, `typescript_sdk`, and
`python_sdk` point to `../tracedb-rust`, `../tracedb-js`, and
`../tracedb-python` respectively.

Native TraceQL v0 now executes through `POST /v1/traceql` with a
`TraceQlQueryRequest` body of `{ "query": string }`. The server parses
read-only line-oriented strings with `traceql_query_from_str`, accepts
directives such as `FROM`, `TENANT`, `WHERE`, `MATCH`, `NEAR`, `FRESHNESS`,
`LIMIT`, and `EXPLAIN`, and compiles them into the existing `HybridQuery` model
before using the same query execution and response shaping as `POST /v1/query`.
The same route also accepts TraceDB command statements: `GET`, `SCAN`, `QUERY`,
`EXPLAIN`, and `JOBS LIST` are read-only, while `SCHEMA APPLY`, `PUT`, `BATCH`,
`PATCH`, `DELETE`, `SNAPSHOT`, and `RESTORE` mutate data or admin state. This
polymorphic route is not blanket safe-retry; mutating/admin commands require
`Idempotency-Key` for idempotency retries. This is native TraceQL evidence only.
It is not SQL compatibility, PostgreSQL
compatibility, GraphQL execution/runtime evidence, or a separate query engine.
`MATCH <field> "..."` preserves `<field>` as `HybridQuery.text_field`; `NEAR
<field> [...]` preserves `<field>` as `HybridQuery.vector_field`.

The same `/v1/traceql` parser now accepts a deliberately bounded SQL-ish form:
`EXPLAIN? SELECT * FROM <table> WHERE tenant_id = <value> [AND field = value]*
[LIMIT n]`. It maps `tenant_id`/`tenant` to `HybridQuery.tenant_id`, additional
equality predicates to `scalar_eq`, `LIMIT` to `top_k`, and `EXPLAIN` to the
shared explain flag. Unsupported SQL constructs such as `JOIN`, `GROUP`,
`ORDER`, `UNION`, mutation DDL/DML, and PostgreSQL-specific behavior fail as
`invalid SQL-ish` bad-request responses rather than implying compatibility.

The query crate exposes `graphql_schema_sdl_from_tables` to generate SDL from
applied `TableSchema` definitions and exposes the bounded GraphQL adapter
compiler primitive `graphql_query_from_str`. The HTTP server exposes the
generated SDL through `GET /v1/graphql/schema` with `GraphQlSchemaResponse`.
Native GraphQL executes through polymorphic `POST /v1/graphql` with
`GraphQlQueryRequest` and returns the GraphQL-style `data`/`errors` envelope:
`get`, `scan`, `query`, `explain`, and `jobs` root fields are read-only,
while `schemaApply`, `put`, `batch`, `patch`, `delete`, `compact`, `snapshot`,
`restore`, and `jobRun` mutate data or admin state. This route is not blanket safe-retry; mutating/admin root fields require `Idempotency-Key` for idempotency retries.
The bounded adapter remains explicit at `POST /v1/graphql/bounded`; it accepts
one root table field with arguments such as `tenant_id`, `where`/`filter`,
`match`/`text`, `near`/`vector`, `limit`, `freshness`, and `explain`, then
compiles directly into `HybridQuery` and returns the same `QueryResponse` shape
as `/v1/query`. The Rust SDK mirrors the schema route
with `TraceDbClient::graphql_schema`, `TraceDbClient::graphql_schema_typed`,
and `TraceDbAsyncClient::graphql_schema_typed`, and mirrors the bounded query
route with `GraphQlQueryRequest`, `TraceDbClient::graphql_typed`, and
`TraceDbAsyncClient::graphql_typed`; the TypeScript SDK mirrors schema export
with `TraceDB.graphqlSchema()` and bounded execution with `TraceDB.graphql()`
and `TraceDB.graphqlRequest()`; the Python SDK mirrors schema export with
`TraceDB.graphql_schema()` and native execution with `TraceDB.graphql()` and
`graphql_request({"query": query})`. Bounded adapter execution is explicit
through `/v1/graphql/bounded` helpers. Subscriptions remain unsupported.

## Versioning Strategy

### Current Status

This document is `contract-freeze-draft` (v0). In core it is a validation mirror
for protocol-owned SDK/adapter work, not a managed-cloud SLA.

### v0 Rules

During v0, **all changes must be additive-only**: new fields, new routes, new
scenario IDs, and new surfaces are allowed. Removing or renaming existing
fields, changing error-envelope shapes, or altering wire semantics is not
permitted until v1.

### v0 → v1 Transition

Breaking changes require a new contract version (v1, v2, etc.). The transition
from v0 to v1 will:

1. Freeze the v0 contract document and manifest at their final state.
2. Create `docs/platform-contract-v1.md` and `docs/platform-contract-v1.json`.
3. SDKs update their lock files (see below) to the new version.
4. v0 surfaces enter a deprecation window (minimum 2 release cycles) before
   removal.

### SDK Lock Files

Each SDK maintains a `tracedb-protocol.lock` file (TOML) that pins to a
specific contract version and revision:

```toml
repo = "https://github.com/Trace-DB/tracedb-protocol"
revision = "51c33f416918d4b31ae1c17b3321497d6114623e"
contract = "platform-contract-v0"
```

The `scripts/validate_protocol_locks.py` script verifies that all discovered
lock files reference the same contract and revision, and returns non-zero if
they have drifted. In the multi-repo workspace, run it from `tracedb/` to scan
siblings by default, or pass `--repo-root /path/to/Trace-DB/tracedb` for a
single-repo check.

### Deprecation Policy

Deprecated fields, routes, or scenarios must survive a **minimum of 2 release
cycles** before removal. During the deprecation window:

- The field/route continues to function.
- Documentation marks it as deprecated with the version that deprecated it.
- SDKs emit a warning when deprecated paths are used (where feasible).
- After the deprecation window, removal requires a contract version bump.

## Surface Implementation Rules

- HTTP direct remains the source of wire truth.
- Rust SDK is the reference ergonomic SDK. It may add table handles and builder
  APIs, but raw HTTP methods must remain available.
- TypeScript's generated client is a transport layer, not the final public
  product shape.
- Python starts sync-first for ingestion, AI workflows, notebooks, and platform
  conformance tests. Async can follow after sync parity.
- TraceQL / SQL-ish must parse into the same query model. Do not claim SQL
  compatibility or PostgreSQL compatibility. The current HTTP surface is
  `/v1/traceql`; the SQL-ish surface is limited to the checked bounded
  `SELECT` adapter until a broader TraceQL adapter is explicitly designed.
- GraphQL schema export is generated from TraceDB schema, native execution uses
  GraphQL `data`/`errors`, and all operations still compile into TraceDB's
  canonical operation/query model. The bounded `graphql_query_from_str`
  adapter lives at `POST /v1/graphql/bounded` for compatibility only.

## Verification Ladder

Use the smallest ladder that proves the touched surface:

```bash
cargo test -p tracedb-testkit --test usability_acceptance platform_contract_v0_declares_sdk_conformance_harness -- --exact
cargo test -p tracedb-query graphql_schema_sdl_is_generated_from_table_schema --no-run
cargo test -p tracedb-testkit --test usability_acceptance http_graphql_endpoint_executes_bounded_query_through_hybrid_query --no-run
cargo test -p tracedb-testkit --test usability_acceptance http_graphql_schema_exports_sdl_from_applied_table_schema --no-run
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
python3 scripts/platform_conformance.py --surface http_direct --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/generate_openapi_v1.py --check
cargo run -p tracedb-cli -- product-quickstart
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

Run SDK conformance from the sibling standalone repositories instead of this
core repo: `../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`.


Local macOS executable-policy failures are local environment evidence, not a
contract failure. Use Modal for remote Linux product verification when the local
machine cannot execute Rust binaries.
