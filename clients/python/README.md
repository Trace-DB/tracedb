# TraceDB Python SDK

This directory contains the sync-first Python SDK surface for the current
TraceDB Platform Contract v0 lane.

Current public DX:

```python
from tracedb import TraceDB

db = TraceDB(url, token="dev-token")
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

The client uses only the Python standard library today. It preserves the raw
HTTP escape hatch with `request_json(...)`, exposes `TraceDBHTTPError` with
method, path, status, response body, parsed `error`, and optional `code`, and
supports caller-provided `Idempotency-Key` values on mutation/admin calls.

Run the local HTTP smoke:

```bash
cd clients/python
python3 http_smoke.py
```

The smoke starts a local `tracedb-server` and drives schema apply, single put,
batch ingest, patch, get, scan, query, explain, delete, idempotency replay and
conflict, error envelope parsing, compact, snapshot, restore, and admin jobs. It
emits `python sdk http smoke ok`.

This is sync Python SDK product-path evidence. It is not async support, package
publishing readiness, managed-cloud proof, SQL compatibility, TraceQL, GraphQL,
or benchmark evidence.
