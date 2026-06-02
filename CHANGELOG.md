# Changelog

This file tracks changes to the TraceDB engine, SDKs, and tooling.
TraceDB follows [semantic versioning](https://semver.org/).

## [0.1.0] — 2026-05-31

Initial development release.

### Engine

- Local/embedded transactional engine with epoch-based MVCC visibility.
- Hybrid query fusion: lexical, vector, graph, temporal, and freshness
  candidate streams over committed records.
- Query explain and provenance — every candidate stream includes access-path,
  planner, and freshness evidence.
- Policy-based visibility oracle for tenant-scoped retrieval.
- Local durability: WAL framing, manifest, checkpoint, snapshot/restore,
  lock-file semantics, and WAL/checkpoint-backed idempotency receipts.
- Schema apply with record writes, batch ingest, patch, scan, and delete.

### HTTP & API

- Tokio/Axum server with Tower middleware (body limits, timeouts, load
  shedding, concurrency limits, graceful shutdown).
- Versioned HTTP routes: `/v1/query`, `/v1/explain`, `/v1/records/*`,
  `/v1/traceql`, `/v1/graphql`, `/v1/graphql/schema`, health, readiness,
  catalog, metrics, and admin-jobs.
- Bounded SQL-ish `SELECT` adapter under `/v1/traceql`.
- Bounded GraphQL query adapter under `/v1/graphql`.
- OpenAPI v1 spec and generated TypeScript transport client.

### SDKs

- **Rust SDK** — blocking and async HTTP clients, table/query builder,
  typed responses, safe read retries, opt-in idempotency-key retries,
  local admin helpers (compact, snapshot, restore).
- **TypeScript SDK** — hand-written `TraceDB` wrapper over generated
  transport, table/query builder, env config, safe retries, idempotency
  retries.
- **Python SDK** — sync-first client for ingestion, AI workflows, and
  notebooks, with native TraceQL and idempotency support.

### Tooling

- CLI with `demo`, `verify`, `http-demo`, `doctor`, `product-quickstart`,
  `product-regression`, and `durability-faults` commands.
- Platform conformance harness (`scripts/platform_conformance.py`) validating
  SDK surfaces against the v0 contract manifest.
- Modal-based remote Linux product verification.
