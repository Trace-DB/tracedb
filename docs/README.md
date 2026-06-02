# TraceDB Documentation

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync drift. Explain every candidate.

This directory contains the canonical documentation for the TraceDB engine, SDKs, deployment operations, and performance benchmarks, migrated from the Grogan Development Vault.

**New here?** Start with the **[Getting Started Guide](getting-started.md)**.

---

## Documentation Map

### 0. [Project Intent Ledger](project-intent.md)
*   **Purpose:** Memory-derived intent for future project setup, agents, automations, and handoffs.
*   **Preserves:** product identity, architecture boundaries, evidence rules, Modal/Railway verification policy, SDK/MCP intent, and repo/vault operating boundaries.

### 1. [Product Thesis](product/thesis.md)
*   **Vision:** Single logical record, transactional consistency, and native views.
*   **What it Replaces:** Collapses complex stacks like Postgres + pgvector + Qdrant + Elasticsearch + Neo4j.
*   **Scope:** defines alpha constraints (what TraceDB is not) and defines winning criteria.
*   **User Personas:** AI App Developers, Coding Agents, Business Software Developers, and Memory Runtime researchers.

### 2. Architecture
*   **[Top-Down System Architecture](architecture/top-down.md):** 6 serverless planes (Control, Gateway, Compute, Storage, Index, Feature), crate/workspace topology, repository directory layouts, and epoch-based MVCC visibility.
*   **[Kernel & Module Design](architecture/kernel-and-modules.md):** Minimality boundaries of the kernel, extension module interfaces (Levels 0-6), and strict cryptographic trust/conformance levels.
*   **[Candidate Stream Planner](architecture/candidate-streams.md):** Transactional candidate stream scheduling, `AccessPath` trait interface, Reciprocal Rank Fusion (RRF), feature freshness lifecycles, and policy-safe retrieval pushdowns.

### 3. API & Protocols
*   **[Platform Contract v0](platform-contract-v0.md):** The DX-facing contract that SDKs and query adapters (TraceQL, SQL-ish, GraphQL) must converge on.
*   **[Platform Contract v0 Manifest](platform-contract-v0.json):** Machine-readable conformance scenario and surface manifest consumed by `scripts/platform_conformance.py`.
*   **Contract Paths:** `docs/platform-contract-v0.md`, `docs/platform-contract-v0.json`, and `scripts/platform_conformance.py` are the stable repository paths for contract review and automation.
*   **[Durability Semantics v0](durability-semantics-v0.md):** Local-first engine durability specifications, WAL framing, TDE artifact behavior, recovery checkpoints, snapshot copies, and WAL/checkpoint-backed idempotency.
*   **[v1 HTTP API Reference](api/v1-http.md):** Direct REST route reference.
*   **[OpenAPI v1 Spec](api/v1-openapi.json):** Machine-readable OpenAPI schema.

### 4. Operations
*   **[Railway Operations & Topology](Operations/railway-lab.md):** Multi-service Railway architecture, internal routing domains, volume mount rules, buckets, and backup/restore workflows.
*   **[Architecture Decisions (Docker over Railpack)](decisions/docker-over-railpack.md):** ADR justifying Dockerfile container builds over Railpack autoconfig for engine portability.

### 5. Benchmarks
*   **[KPI Closeout & Benchmarks](benchmarks/kpi-closeout.md):** Median benchmarks vs pgvector at 1024 records, store-apply latency attribution breakdowns, footprint metrics, and 10-stage scientific loop logs.

### 6. Developer Guides
*   **[Codex CLI Developer Handoff](handoffs/codex-cli.md):** System setup workarounds (macOS binary AMFI bypass via Modal, PEP 668 virtualenv rules), CLI commands index, and comprehensive validation runbooks for local and remote lanes.
*   **Local Product Regression:** `cargo run -p tracedb-cli -- product-regression` is the local product regression smoke runner. It supports `--inject-failure STEP`, `--list-steps`, and `--report-file PATH`, and reports `only_supported`, `human_summary`, and `report_file` metadata. `product-quickstart` writes `target/tracedb/product-quickstart.json` by default. For remote Linux product verification, run `modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json`.
*   **Product Gate Selectors:** `product-quickstart --inject-failure embedded_demo` verifies failure receipts. The local core gate supports `--only embedded_demo`, `--only embedded_verify`, `--only http_demo`, and `--only local_doctor`. SDK conformance is owned by the sibling standalone repos `../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`; the core repo no longer shells into legacy in-tree SDK locations during product regression.
*   **Durability Fault Harness:** `cargo run -p tracedb-cli -- durability-faults` writes `target/tracedb/durability-faults.json` and checks wrong/missing TDE keys, torn WAL tail handling, manifest/checkpoint corruption, stale-lock recovery, encrypted snapshot restore, and WAL-backed idempotency replay after reopen.
