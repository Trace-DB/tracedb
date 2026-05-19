---
title: TraceDB
aliases:
  - TraceDB Home
tags:
  - tracedb
  - docs
status: stub
type: repo-handoff
updated: 2026-05-18
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

The SDK quickstart is also runnable against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000
```

The SDK quickstart uses typed convenience response methods over the current HTTP
JSON shapes and accepts `--timeout-ms` for the blocking SDK request timeout. SQL
compatibility remains unimplemented.
