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
compatibility, managed-cloud proof, or benchmark results. The command also has
test-only `--inject-failure STEP` coverage for JSON failure output and nonzero
exit behavior. For product-regression step discovery, `--list-steps` emits JSON
step metadata and exits without running product steps. The first single-step
execution mode is `--only embedded_demo`, which runs only the embedded demo
step and emits the normal local product-regression JSON summary. The
dependency-aware `--only embedded_verify` mode verifies an existing embedded
demo data root, typically after `--only embedded_demo` with the same
`--data-root`. `--only http_demo` runs the self-contained local HTTP demo step
and emits the normal one-step `local-product-regression` JSON summary. It does
not run local `doctor http`, the Rust SDK quickstart, generated TypeScript
smoke steps, managed-cloud checks, benchmark controls, or SQL compatibility
checks.
`--only local_doctor` runs only the local HTTP doctor diagnostic against a
managed local server and emits one-step `local-product-regression` JSON. It is
endpoint diagnostics evidence only, not full product-regression gate coverage.
