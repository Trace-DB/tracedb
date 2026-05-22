# TraceDB Python SDK

This directory contains the sync-first Python SDK surface for the current
TraceDB Platform Contract v0 lane.

Current public DX:

```python
from tracedb import TraceDB

db = TraceDB.from_env()
docs = db.table("docs").tenant("tenant-a")

docs.insert("intro", {
    "body": "TraceDB Python SDK",
    "embedding": [1, 0, 0],
    "status": "published",
})

rows = (
    db.table("docs")
    .where({"tenant_id": "tenant-a", "status": "published"})
    .match_text("body", "TraceDB")
    .near("embedding", [1, 0, 0])
    .with_options(explain=True, freshness="lazy")
    .limit(20)
    .all()
)

traceql_rows = db.traceql("""
FROM docs
TENANT tenant-a
WHERE status = "published"
MATCH body "TraceDB"
LIMIT 20
""")

graphql_rows = db.graphql(
    'query { docs(tenant_id: "tenant-a", match: "TraceDB", limit: 20) { record_id } }'
)
```

`TraceDB.from_env()` reads `TRACEDB_URL`, optional `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, and
`TRACEDB_SAFE_RETRIES`, and `TRACEDB_IDEMPOTENCY_RETRIES`. Explicit keyword
arguments override matching environment values. Direct construction with
`TraceDB(url, token="dev-token")` remains supported.

The client uses only the Python standard library today. It preserves the raw
HTTP escape hatch with `request_json(...)`, exposes `TraceDBHTTPError` with
method, path, status, response body, parsed `error`, and optional `code`, and
supports caller-provided `Idempotency-Key` values on mutation/admin calls.
`TraceDB.traceql(query)` and `traceql_request({"query": query})` execute native
TraceQL strings through `POST /v1/traceql`.
`TraceDB.graphql(query)` and `graphql_request({"query": query})` execute bounded
GraphQL query-adapter strings through `POST /v1/graphql`.
`safe_retries` retries transient HTTP 5xx responses only for read-only routes:
health, ready, get, scan, query, native TraceQL, bounded GraphQL, and explain.
It does not retry writes or admin mutations. `idempotency_retries` is
default-off and retries transient HTTP 5xx responses for mutation/admin routes
only when that request carries a caller-provided `Idempotency-Key`; unkeyed
writes and 4xx/conflict responses are not retried.

Run the local unit/package checks:

```bash
python3 -m unittest discover -s clients/python/tests
python3 clients/python/install_smoke.py
```

`install_smoke.py` prefers a temporary venv, installs this directory as the
`tracedb` package with pip `--no-deps`, and runs a consumer script from outside
the repo so source-path imports cannot hide package drift. On remote images
where Python can run tests but `ensurepip` is unavailable, it falls back to an
isolated temporary pip `--target` install. It emits `python sdk install smoke
ok`.

Run the local HTTP smoke:

```bash
python3 clients/python/http_smoke.py
```

The smoke starts a local `tracedb-server` and drives schema apply, single put,
batch ingest, patch, get, scan, query, TraceQL string execution, explain,
bounded GraphQL result/explain, delete, idempotency replay and conflict, error
envelope parsing, compact, snapshot, restore, and admin jobs. It emits
`python sdk http smoke ok`.

This is sync Python SDK product-path evidence. The package metadata is local
project/package-shape evidence only. The platform conformance lane installs a
copied package into an isolated temporary pip `--target` and runs this HTTP
smoke with source-path imports disabled, so SDK conformance cannot pass by
accidentally importing the repo copy. It is not async support, PyPI release
readiness, managed-cloud proof, SQL compatibility, full GraphQL adapter parity,
or benchmark evidence.
