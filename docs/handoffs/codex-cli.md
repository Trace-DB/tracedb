# TraceDB Codex CLI Handoff & Developer Reference

This document summarizes the TraceDB developer environment, command-line interfaces, and verification runbooks. It acts as a concise handoff guide for maintaining, testing, and verifying the TraceDB engine and its SDKs.

---

## Developer Environment & Constraints

Developers working on TraceDB must navigate several environment-specific constraints:

1. **macOS AMFI/taskgated Executable Blocker:**
   - **Constraint:** Local macOS security policies block the launch/execution of local Rust test and server binaries, causing them to stall and time out.
   - **Workaround:** Local compilation checks (`cargo test --no-run`) and light unittests are supported, but **Modal** (remote Linux) is the authoritative platform for running integration tests, HTTP servers, and end-to-end conformance validation.
2. **Homebrew Python PEP 668 System Pip Blocking Policy:**
   - **Constraint:** Homebrew prevents direct `pip install` modifications on the system Python environment.
   - **Workaround:** All Python package testing, editable installs, and conformance runner environments must use temporary virtual environments (e.g., `benchmarks/realworld/.venv/` or `/tmp/` virtualenvs).
3. **Node/npm Tooling:**
   - **Constraint:** Required for compiling and type-checking the generated TypeScript SDK client (`clients/typescript/`).
   - **Workaround:** If Node is unavailable in the execution context, commands can be run with `--skip-typescript` to bypass JS/TS steps.

---

## CLI Command Index

The primary entry point is the Rust-based `tracedb` CLI (compiled from `crates/tracedb-cli`).

### Core Engine & Demo Commands
- **`tracedb demo`:** Starts an embedded local database instance, writes a demo record, scans and retrieves it, and prints a JSON result summary. Runs in-memory.
- **`tracedb verify`:** Verifies the embedded database state written by a prior `demo` command in the same data directory.
- **`tracedb http-demo`:** Starts a managed local loopback server, drives the Rust SDK client through standard HTTP routes (schema apply, batch ingest, query, explain, delete, compact, snapshot, restore), and prints a unified JSON summary.
- **`tracedb serve`:** Launches the HTTP engine daemon (e.g., `tracedb serve --data <dir> --bind 127.0.0.1:8090`).

### Diagnostics & Verification
- **`tracedb doctor http`:** Drives read-only diagnostic checks against a live TraceDB server endpoint. Retrieves `/v1/health`, `/v1/ready`, `/v1/databases`, `/v1/branches`, `/v1/metrics/public-safe`, and `/v1/admin/jobs`.
- **`tracedb product-regression`:** Runs the local product regression gate (embedded demo, HTTP demo, local doctor diagnostics, Rust SDK quickstart, TypeScript typechecks/smokes).
- **`tracedb product-quickstart`:** A wrapper for `product-regression` that defaults the output report to `target/tracedb/product-quickstart.json` and returns parseable JSON stdout.

---

## Verification Runbooks

### Runbook 1: Local Embedded Demo & Verification
Validate embedded engine basic operations (scan/put/delete) without launching HTTP servers.
```bash
# 1. Run the local embedded demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-embedded-demo demo

# 2. Verify the written state
cargo run -p tracedb-cli -- --data /tmp/tracedb-embedded-demo verify
```

### Runbook 2: One-Command Local HTTP SDK Demo
Orchestrate a server lifecycle and drive the Rust SDK over HTTP using a single command:
```bash
cargo run -q -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

### Runbook 3: Diagnostic Endpoint Checks (Doctor)
Check the health, routing catalogs, metrics, and background jobs on an active server:
```bash
# Basic HTTP check
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token

# Check with routing attributes and a pre-check readiness wait
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token \
  --database-id db_local --branch-id db_local:main --wait-ready-ms 5000
```

### Runbook 4: Product Regression Testing (Local Gate)
Execute the complete set of local smoketests. This command is run on every commit in non-blocked environments.
```bash
# Run all checks, writing a JSON report to a predictable location
cargo run -p tracedb-cli -- product-regression --report-file target/tracedb/regression-report.json

# Skip TypeScript verification steps (if Node.js is not installed)
cargo run -p tracedb-cli -- product-regression --skip-typescript

# Execute a single targeted regression step
cargo run -p tracedb-cli -- product-regression --only rust_sdk_quickstart

# Inject a failure into a step to verify CI/CD error-reporting behavior
cargo run -p tracedb-cli -- product-regression --inject-failure embedded_demo
```
*Supported step names for `--only` and `--inject-failure`: `embedded_demo`, `embedded_verify`, `http_demo`, `local_doctor`, `rust_sdk_quickstart`, `typescript_check`, `typescript_http_smoke`, `typescript_gateway_smoke`.*

### Runbook 5: Remote Linux Product Verification (Modal)
When local macOS binary execution is blocked, use Modal to verify the workspace in a Linux container:
```bash
# Run the fast quickstart verifier lane (fmt, quickstart receipt, etc.)
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json

# Run the complete workspace verifier (builds SDKs, runs full unit & integration tests)
modal run scripts/modal_product_verify.py --mode workspace --summary-json /tmp/tracedb-modal-product-workspace.json
```

### Runbook 6: SDK & Language Conformance Testing

#### TypeScript SDK (`clients/typescript/`)
```bash
cd clients/typescript/
npm ci

# Typecheck and run the local generated client fake-fetch smoke
npm run check

# Run the local HTTP smoke tests (starts a child engine server)
npm run http-smoke

# Run the gateway smoke tests (tests token authorization and routing catalogs)
npm run gateway-smoke

# Run the TypeScript endpoint-driven quickstart
npm run quickstart -- --url http://127.0.0.1:8090 --token dev-token --admin-dir /tmp/ts-admin-scratch
```

#### Python SDK (`clients/python/`)
```bash
# Establish a clean virtualenv
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# Run Python client unit tests
python3 -m unittest clients/python/tests/test_client.py

# Run Python client HTTP smoke tests
python3 clients/python/http_smoke.py
```

#### Platform Conformance Suite
Validate scenario-based JSON contracts (idempotency, errors, routing) across the HTTP direct interface and all client SDKs:
```bash
# Test direct HTTP endpoints against a local server
python3 scripts/platform_conformance.py --surface http_direct --url http://127.0.0.1:8090

# Test Python SDK client conformance
python3 scripts/platform_conformance.py --surface python_sdk --url http://127.0.0.1:8090
```

---

## Real-World Benchmark Lab Runbooks
Benchmark suites are executed from the `benchmarks/realworld/` subdirectory.

```bash
cd benchmarks/realworld/
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Run a 1000-record benchmark suite on local/mock targets
python3 -m runner suite --profile smoke --dataset generated --records 1000 --openrouter-mode off

# 2. Run the conversational AI memory / flight-recorder demo
python3 -m runner chat-demo --output-json /tmp/tracedb-v0-chat-demo.json --output-md /tmp/tracedb-v0-chat-demo.md
```
