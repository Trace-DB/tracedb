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
```

`TraceDB.from_env()` reads `TRACEDB_URL`, optional `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, and `TRACEDB_TIMEOUT_MS`. Explicit
keyword arguments override matching environment values. Direct construction
with `TraceDB(url, token="dev-token")` remains supported.

The client uses only the Python standard library today. It preserves the raw
HTTP escape hatch with `request_json(...)`, exposes `TraceDBHTTPError` with
method, path, status, response body, parsed `error`, and optional `code`, and
supports caller-provided `Idempotency-Key` values on mutation/admin calls.

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
batch ingest, patch, get, scan, query, explain, delete, idempotency replay and
conflict, error envelope parsing, compact, snapshot, restore, and admin jobs. It
emits `python sdk http smoke ok`.

This is sync Python SDK product-path evidence. The package metadata is local
project/package-shape evidence only. It is not async support, PyPI release
readiness, managed-cloud proof, SQL compatibility, TraceQL, GraphQL, or
benchmark evidence.
