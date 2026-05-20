---
title: TraceDB Repo Docs Stub
tags:
  - tracedb
  - docs
status: stub
type: repo-handoff
updated: 2026-05-19
---

# TraceDB Repo Docs

Canonical TraceDB technical docs moved to the Grogan Development Vault:

```text
/Users/zgrogan/Repos/grogan-development-vault/10_Projects/TraceField Suite/TraceDB/
```

This repo keeps only lightweight stubs required for local handoff and test stability.

Start with:

- `README.md`
- `docs/TraceDB.md`
- `docs/api/v1-http.md`
- `docs/api/v1-openapi.json`
- `docs/Operations/Local Cloud.md`

Local product smoke:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

Consolidated local product regression:

```bash
cargo run -p tracedb-cli -- product-regression
```

This is local product regression evidence only; it does not claim SQL
compatibility, managed-cloud proof, or benchmark results.
