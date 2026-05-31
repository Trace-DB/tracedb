# Hosted Alpha Blocker Closure Handoff - 2026-05-30

## Scope

This handoff summarizes the current `codex/prod-managed-railway-gate` checkpoint after the multi-agent review and blocker-closure pass.

TraceDB is still framed as an internal Railway hosted alpha candidate, not a public production managed database. The local branch evidence is green, but Railway deployment receipts are still required before exposing `api.trace-db.com`.

## Branch State

- `main`, `origin/main`, `codex/prod-managed-railway-gate`, and `codex/prod-remediation-one-pass` all pointed at `8cb7bfa` before this dirty checkpoint.
- Older production branches are already ancestors of `main`: `prod-runtime-context`, `prod-durability-tde`, `prod-storage-index-jobs`, `prod-api-parity`, and `prod-remediation-one-pass`.
- The remaining work before user-side changes was a large uncommitted hosted-alpha blocker patch on `codex/prod-managed-railway-gate`.

## What This Checkpoint Contains

- Gateway hosted-alpha hardening:
  - Public gateway requires auth except health/readiness routes.
  - Public gateway ignores caller-supplied actor identity headers for tenant, token identity, scopes, and policy epoch.
  - Internal engine token is mandatory when hosted-alpha/private mode is enabled.
  - Cheap rate-limit rejection runs before bcrypt verification.
  - Hosted-alpha public gateway blocks admin snapshot/restore/job routes until object-backed receipts exist.

- Snapshot and restore safety:
  - `TRACEDB_ADMIN_SNAPSHOT_ROOT` gates managed snapshot and restore paths.
  - REST, TraceQL, and GraphQL admin paths share the same managed-root validation.
  - Escape attempts and unmanaged targets return stable errors.

- WAL and durability truthfulness:
  - Unsafe WAL rotation is disabled until multi-file scan/replay is fully wired.
  - Legacy WAL v1 opens fail with a stable unsupported-version error.
  - Torn-tail repair and post-repair append behavior remain covered.

- SDK and CLI release blockers:
  - Python sync import no longer requires optional async dependencies.
  - Python async support is lazy/optional.
  - TypeScript smoke scripts rebuild before running `dist` scripts to avoid stale artifact races.
  - CLI `get`, `delete`, and `scan` positional argument regressions are covered.
  - Rust, Python, and TypeScript retry behavior treats mutating TraceQL/GraphQL requests as unsafe unless idempotent.

- CI and ops hygiene:
  - GitHub Actions workflow covers Rust fmt/clippy/test, TypeScript, Python, benchmark gate tests, generated contract drift, Docker lite smoke, platform conformance smoke, and cargo audit.
  - Railway env examples are split by role: gateway, engine, worker, and benchmark.
  - Docs describe internal Railway lab topology, not public production ingress.

## Local Evidence

Generated local artifacts under `target/tracedb/`:

- `product-quickstart.json`
- `product-quickstart-encrypted.json`
- `platform-conformance-all.json`
- `api-parity.json`
- `api-parity-conformance.json`
- `storage-index-jobs.json`
- `durability-faults.json`

Known local green gate from the blocker-closure pass:

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --all-targets
cd clients/typescript && npm run check
python3 -m unittest discover -s clients/python/tests
python3 -m unittest benchmarks.realworld.tests.test_modal_bench benchmarks.realworld.tests.test_suite_gate
cargo run -p tracedb-cli -- product-quickstart --report-file target/tracedb/product-quickstart.json
TRACEDB_MASTER_KEY_B64=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= cargo run -p tracedb-cli -- product-quickstart --report-file target/tracedb/product-quickstart-encrypted.json
cargo run -p tracedb-cli -- storage-index-jobs --report-file target/tracedb/storage-index-jobs.json
cargo run -p tracedb-cli -- api-parity --report-file target/tracedb/api-parity.json
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --surface typescript_sdk --surface python_sdk --surface traceql --surface graphql --summary-json target/tracedb/platform-conformance-all.json
```

## Still Missing

- Live Railway receipt proving public gateway/private engine/private worker topology.
- Railway marker write/read, restart readback, and redeploy readback.
- Encrypted snapshot/restore receipt from the hosted topology.
- Object-backed backup/export receipt and restore validation.
- Hosted-alpha suite-gate artifact. Local evidence is not a Railway hosted-alpha claim.

## Recommended Next Step

After this checkpoint is committed and pushed, start the Railway hosted-alpha gate from a clean tree:

1. Deploy gateway, engine, and worker as separate Railway services.
2. Confirm engine and worker have no public domain.
3. Set required secrets through Railway variables, not repo files.
4. Generate redacted deployment receipts under `target/tracedb/`.
5. Only then consider binding `api.trace-db.com` to the gateway.
