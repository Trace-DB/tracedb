---
title: TraceDB Repo Docs Stub
tags:
  - tracedb
  - docs
status: stub
type: repo-handoff
updated: 2026-05-22
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
- `docs/platform-contract-v0.md`
- `docs/platform-contract-v0.json`
- `docs/api/v1-http.md`
- `docs/api/v1-openapi.json`
- `docs/Operations/Local Cloud.md`

`docs/platform-contract-v0.md` is the DX-facing SDK/adaptor contract freeze
draft. `docs/platform-contract-v0.json` is the machine-readable conformance
manifest for HTTP direct, Rust SDK, TypeScript SDK, Python SDK, TraceQL/SQL-ish,
and GraphQL parity work.
`scripts/platform_conformance.py` is the first executable harness over that
manifest. It currently runs `http_direct` through raw HTTP requests and maps
the existing Rust SDK quickstart product path into `rust_sdk` scenario results.
It also maps the public TypeScript SDK HTTP smoke into `typescript_sdk` scenario
results and installs the Python SDK package before running the sync HTTP smoke
for `python_sdk` scenario results. The `traceql_sqlish` lane now executes the
bounded SQL-ish adapter through `/v1/traceql` and reports query, TraceQL string
execution, explain, and error-envelope behavior against the same manifest.
The `graphql` lane starts local HTTP, exports generated SDL through
`GET /v1/graphql/schema`, drives the bounded `POST /v1/graphql` adapter, and
reports schema apply, query, explain, and error-envelope behavior as checked;
write/admin scenarios remain explicit `not_checked` results.
The current HTTP direct, Rust SDK, TypeScript SDK, and Python SDK lanes cover
all required v0 scenarios; unimplemented future lanes must keep explicit
`not_checked` results until they reach parity. The SQL-ish adapter lane remains
partial, with schema/write/admin scenarios intentionally `not_checked`.
The `traceql_sqlish` lane now has native HTTP execution evidence through
`POST /v1/traceql`, which parses line-oriented TraceQL with
`traceql_query_from_str` and compiles it into `HybridQuery`. The same route now
accepts the bounded SQL-ish adapter form `EXPLAIN? SELECT * FROM <table> WHERE
tenant_id = <value> [AND field = value]* [LIMIT n]`; broader SQL,
PostgreSQL compatibility, GraphQL mutations, subscriptions, resolver runtime,
GraphQL data-envelope execution, and full GraphQL adapter parity remain
unimplemented. The current GraphQL evidence is limited to generated
`graphql_schema_sdl_from_tables` SDL export through `GET /v1/graphql/schema`,
bounded `graphql_query_from_str` compilation, `POST /v1/graphql`
query/explain/error conformance, the Rust SDK schema helper methods
`TraceDbClient::graphql_schema_typed` and
`TraceDbAsyncClient::graphql_schema_typed`, the TypeScript public SDK
`TraceDB.graphqlSchema()` helper for generated SDL export, the Python sync SDK
`TraceDB.graphql_schema()` helper for generated SDL export, and the Rust SDK
execution helpers `TraceDbClient::graphql_typed` and
`TraceDbAsyncClient::graphql_typed` plus the TypeScript public SDK
`TraceDB.graphql()` helper and Python sync SDK `TraceDB.graphql()` helper over
that same HTTP route.

Local product smoke:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

Consolidated local product regression:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
```

This is local product regression evidence only; it does not claim SQL
compatibility, managed-cloud proof, or benchmark results. The command emits a
compact top-level `human_summary` in its JSON output and also has test-only
`--inject-failure STEP` coverage for JSON failure output and nonzero exit
behavior. Use `--report-file PATH` to write the same JSON summary to a
predictable file while preserving JSON stdout; this applies to full runs,
`--only`, `--inject-failure`, and `--list-steps`, and creates parent
directories. For product-regression step discovery, `--list-steps` emits JSON
step metadata including `human_summary` and `only_supported` and exits without
running product steps. `--skip-typescript` is for the full product gate and
non-TypeScript selectors; a TypeScript `--only` selector conflicts with --skip-typescript.
The first single-step execution mode is `--only embedded_demo`, which runs only the
embedded demo step and emits the normal local product-regression JSON summary.
`product-quickstart` runs the same local product gate with a default report file
at `target/tracedb/product-quickstart.json`; it accepts the same
product-regression options, still writes JSON to stdout, and includes a
top-level `report_file` field when a report artifact is configured. Treat
`target/tracedb/product-quickstart.json` as the local quickstart receipt: it
should report `ok: true`, `mode: "local-product-regression"`,
`scope: "local_only"`, `human_summary.status: "passed"`, SQL as
`not_implemented`, and managed-cloud/benchmark claims as `not_checked`.
`product-quickstart --skip-typescript` is the reduced fallback receipt for
machines without Node tooling: it still writes the same default report artifact,
keeps `report_file`, reports `typescript_enabled: false`, passes the six
non-TypeScript local steps including `python_sdk_smoke`, and omits `typescript_check`,
`typescript_http_smoke`, and `typescript_gateway_smoke`. Treat it as a
reduced local evidence path, not the full product gate.
When local executable policy or machine resources block product verification,
use Modal for remote Linux product verification:

```bash
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

The Modal lane uploads the current checkout without `.git`, `target/`, local
env files, benchmark reports, caches, or Node modules, then runs
`cargo fmt --all -- --check`, the focused quickstart receipt test, the docs
contract test, and `product-quickstart --skip-typescript`. It validates the
default quickstart receipt against stdout, including `report_file`,
`typescript_enabled: false`, the six non-TypeScript steps including
`python_sdk_smoke`, SQL as
`not_implemented`, and managed-cloud/benchmark claims as `not_checked`.
`--mode workspace` additionally runs the full CLI demo test file, usability
acceptance test file, and `cargo test --workspace --all-targets`. This remains remote Linux product verification, not proof that the local Mac can execute the
same binaries.
`product-quickstart --inject-failure embedded_demo` is the quick failure receipt
check: it exits nonzero, writes the same default report artifact, keeps
`report_file`, reports `human_summary.status: "failed"`, and marks the injected
`embedded_demo` step as failed. The
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
`--only rust_sdk_quickstart` starts a managed local server, creates/uses the
quickstart admin dir, runs only the Rust SDK quickstart step, and emits
one-step `local-product-regression` JSON. It is local Rust SDK quickstart
evidence only, not full product-regression gate coverage.
`--only python_sdk_smoke` runs only `python3 clients/python/http_smoke.py`
from the workspace root and emits one-step `local-product-regression` JSON. It
is local sync Python SDK HTTP smoke evidence only, not full product-regression
gate coverage, not embedded demo/verify, not local HTTP demo, not endpoint
diagnostics, not Rust SDK quickstart, not TypeScript smoke coverage, not
managed-cloud proof, not benchmark evidence, and not SQL compatibility.
`--only typescript_check` runs only `(cd clients/typescript && npm run check)`
and emits one-step `local-product-regression` JSON. It is generated TypeScript
check evidence only, not full product-regression gate coverage, not local HTTP
demo, not endpoint diagnostics, not Rust SDK quickstart, not TypeScript HTTP or
gateway smoke coverage, not managed-cloud proof, not benchmark evidence, and
not SQL compatibility.
`--only typescript_http_smoke` runs only `(cd clients/typescript && npm run
http-smoke)` and emits one-step `local-product-regression` JSON. It is local
generated TypeScript HTTP smoke evidence only, not full product-regression gate
coverage, not embedded demo/verify, not local HTTP demo, not endpoint
diagnostics, not Rust SDK quickstart, not `typescript_check`, not TypeScript
gateway smoke coverage, not managed-cloud proof, not benchmark evidence, and
not SQL compatibility.
`--only typescript_gateway_smoke` runs only `(cd clients/typescript && npm run
gateway-smoke)` and emits one-step `local-product-regression` JSON. It is local
public TypeScript SDK gateway auth/routing evidence only, not full
product-regression gate coverage, not embedded demo/verify, not local HTTP
demo, not endpoint diagnostics, not Rust SDK quickstart, not `typescript_check`,
not TypeScript HTTP smoke coverage, not managed-cloud proof, not benchmark
evidence, and not SQL compatibility.
