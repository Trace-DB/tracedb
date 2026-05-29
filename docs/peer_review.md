# TraceDB Peer Review Remediation Ledger

**Status**: Remediated on `codex/prod-remediation-one-pass`
**Base**: `codex/prod-api-parity@3898d23`
**Verdict**: The reviewed production blockers are addressed in this branch, pending the full release gate.

This ledger tracks the validated peer-review findings and the code paths that now carry the remediation. It is not a marketing claim; the release bar remains the machine-readable gates under `target/tracedb/` plus the full workspace test suite.

## Critical Fixes

### Gateway Authentication
Gateway metadata routes now require bearer authorization unless the route is explicitly health/readiness exempt. Covered by `gateway_requires_auth_for_metadata_routes`.

Affected behavior:
- `/health`, `/healthz`, `/v1/health`, `/ready`, and `/v1/ready` remain unauthenticated.
- `/v1/databases`, `/v1/branches`, `/metrics`, `/v1/metrics/public-safe`, and proxied stateful/admin routes require the configured gateway token.

### Raw-Byte Async Gateway Proxy
The production Axum gateway path no longer rebuilds requests as UTF-8 strings. It proxies raw request bodies through a pooled `reqwest::Client` and returns raw response bytes. Compatibility text helpers remain test-only support for existing direct unit tests.

Covered by:
- `axum_gateway_proxies_binary_body_without_utf8_loss`
- `gateway_injects_actor_context_and_private_engine_token`
- `gateway_preserves_inbound_actor_context_for_command_surfaces`

### WAL Torn-Tail Recovery
WAL open now truncates torn tails at the last valid frame offset, syncs the repaired file, and resumes future appends at the repaired tail. This prevents valid post-recovery commits from being hidden behind stale corrupt bytes.

Covered by `tracedb-log` unit tests.

## Isolation, Durability, And Jobs

### Physical Database/Branch Shards
Engine requests select a physical shard from `ActorContext.database_id` and `ActorContext.branch_id`. Non-default shards live below `shards/{database_id}/{branch_id}` using safe path encoding and a `shard.receipt.json` metadata file. The legacy local/default shard remains mapped to the root data directory for compatibility.

Covered by `engine_handle_physically_isolates_database_branch_shards`.

### Durable Job Runtime Without Data-Plane Locking
Server job lease/heartbeat/complete/fail operations now use a per-shard job runtime under `jobs/`, not the data-plane `TraceDb` write lock. Job state is checkpointed and replayable across reopen.

Covered by:
- `job_catalog_does_not_wait_on_data_plane_read_lock_and_replays`
- private worker job endpoint tests

### Keeper And Compaction
Keeper persistence now fsyncs the temp file before rename and fsyncs the parent directory after rename. Compaction now removes compacted source segment manifests and allows vacuum to remove superseded artifacts safely.

Covered by `tracedb-keeper`, `tracedb-query`, and acceptance tests.

## Query, Index, Parser, And SDKs

### Policy Enforcement
Query materialization now derives tenant-visible default policy for legacy records, applies `VisibilityOracle`, and preserves final materialization guards. Hidden, ACL-denied, wrong-tenant, and `suppress_from_ai` records are filtered.

### Bounded Lexical Cache
The lexical corpus cache is bounded with LRU-style behavior at 64 entries and still clears on writes.

### HNSW Vector Search
Segment-local vector artifacts now use deterministic HNSW traversal through the existing neighbor graph, with exact-score verification and explicit exact fallback where artifact coverage is insufficient.

### Parser Hardening
TraceQL command tokenization now respects JSON string and bracket boundaries. GraphQL delimiter matching skips comments and strings. SQL-ish `OR` now fails with a stable unsupported-keyword error instead of being silently misparsed.

### SDK Retry Behavior
Rust, Python, and TypeScript clients retry retryable network/transport failures and HTTP 5xx responses with exponential backoff plus jitter. Nonretryable 4xx responses remain terminal.

Covered by focused SDK tests in Rust, Python, and TypeScript.

## Remaining Boundaries

- PostgreSQL wire compatibility remains a non-goal.
- SQL-ish `SELECT` remains a bounded adapter, not SQL compatibility.
- External ANN/search sidecars remain benchmark controls only.
- Rust SDK async now uses a pooled native `reqwest::Client`; the blocking client remains available for synchronous callers.
- Full production release still requires the managed Railway gate and `production_1m` evidence.
