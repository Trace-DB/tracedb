---
title: TraceDB Project Intent
aliases:
  - TraceDB Intent Ledger
  - TraceDB Memory-Derived Intent
tags:
  - tracedb
  - intent
  - handoff
status: current
type: intent-ledger
updated: 2026-05-25
---

# TraceDB Project Intent

This file preserves the durable intent that previously lived mostly in Codex
memory. Read it first when creating a new project, prompt, automation, issue, or
handoff for this repository.

It is not a replacement for the architecture, API, durability, operations, or
benchmark references. It explains the decisions those docs are supposed to
protect.

## Product Identity

TraceDB is an AI-native transactional candidate-stream database. The invariant
wording to preserve is:

```text
One logical record.
One transaction epoch.
Many native views.
No external sync drift.
Explain every candidate.
```

TraceDB is serious infrastructure for semantic, stateful, and agentic
applications. It should not drift into whimsical AI cognition branding or a
generic "memory app" frame. The durable category is closer to semantic database,
memory substrate, and policy-aware retrieval engine.

TraceField is the broader memory/runtime research program and future interface
around the substrate. It informs TraceDB, but TraceField is not the current
TraceDB product and not an implemented runtime inside this repo.

`tracedb-memory-runtime` remains optional/scaffolding unless implementation and
evidence prove otherwise. Memory calculus, tensor compute, and tensor storage
are future derived-artifact or module work, not current product claims.

## Architecture Intent

The top-down architecture is the full-system contract, not a phase plan. When
architecture work is requested, write decisively and invariant-first. Do not
collapse the architecture into "pieces, phases, MVPs" unless the user asks for a
build plan.

Keep these documents conceptually separate:

- `docs/architecture/top-down.md`: what the whole system must become.
- `docs/product/thesis.md`: product thesis, alpha constraints, and winning
  criteria.
- `docs/architecture/kernel-and-modules.md`: kernel minimality, module law, and
  trust levels.
- `docs/architecture/candidate-streams.md`: policy-safe candidate-stream
  planning.
- `docs/durability-semantics-v0.md`: current local-first durability boundary.
- `docs/platform-contract-v0.md`: cross-surface SDK/API conformance contract.

The durable architecture center is a small WAL/epoch/MVCC kernel with native
modules, candidate streams, policy-before-retrieval, freshness-aware ranking,
and explainable results. Serverless-first managed TraceDB is the product
direction, but embedded/local correctness is still the foundation.

Railway is the persistent hosted proving ground. Modal is the remote Linux
execution and report plane. Local macOS success alone is not sufficient when the
claim is platform, deployment, or benchmark readiness.

## Implementation Intent

For active engineering, prefer bounded implementation slices with explicit
verification gates. The v0 sequence intentionally put deterministic correctness
before ANN/HNSW work:

- lexical/vector determinism
- visible erasure
- replay-minimum explain facts
- pending/failed feature lifecycle
- policy visibility boundary
- sealed-segment and hot-materialization reality checks
- local product/demo harnesses

Do not force production code churn when a test-only checkpoint locks the current
contract. When behavior already satisfies the contract, preserve it with
coverage and move on.

When a slice graduates from correctness into integration, switch to realistic
product gates: local product smoke, SDK/API conformance, remote Linux
verification, and benchmark evidence with external controls.

The current productization direction is "boringly runnable." The next useful
work usually improves installability, one-command quickstart receipts, SDK
ergonomics, route conformance, or honest operator diagnostics before adding more
research surface.

## Evidence Rules

Internal TraceDB-only benchmark wins are development evidence. Exported
performance claims require an external control and a number to beat.

Benchmark and platform gates should classify runs as:

- `usable`
- `degraded`
- `blocked`
- `claim-ready`

Each classification needs concrete reasons. A green local command is not enough
for a managed-cloud, durability, benchmark, or product claim.

The benchmark direction is app-shaped workloads, durability, concurrency,
restart/persistence, multi-tenant pressure, competitor baselines, and usability
evidence. Do not answer benchmark criticism with more smoke coverage alone.

Suspicious benchmark deltas are evidence problems first. Rerun against the old
commit or external control under comparable conditions before inventing engine
fixes.

Known benchmark checkpoint framing:

- The 1024-record Modal/pgvector closeout is a development checkpoint, not a
  product win.
- pgvector beat TraceDB on query latency, ingest latency, and storage footprint
  in that closeout, while retrieval quality tied.
- Store-apply attribution narrowed bottlenecks to feature/source-hash work,
  in-memory version install, field cloning, and key construction.

## Platform And Ops Intent

Modal is the default remote verification lane when local macOS executable policy
or workstation state blocks integration tests. Do not bypass verification
because the local machine cannot launch Rust binaries.

Railway is the persistent product lab for hosted TraceDB shape. A Railway run
should prove its runbook, stateful behavior, backup/snapshot/restore receipts,
and suite gate status before it is used for broader claims.

The local/cloud split must stay honest:

- local/embedded and local daemon paths prove the engine and SDK wire contract;
- Docker Compose mirrors the hosted topology for development;
- Railway proves a persistent hosted lab;
- Modal proves reproducible remote execution and report artifacts.

The current HTTP stack is a product proof path, not the final production web
server runtime. The production `serve()` paths now run on Tokio/Axum with Tower
body limits, timeouts, load shedding, concurrency limits, graceful shutdown,
structured JSON tracing, private engine-token enforcement where configured, and
an async engine handle with cheap read snapshots plus serialized write/admin
work. Legacy stdlib listener helpers remain only for compatibility tests and
local harnesses.

## SDK, API, And MCP Intent

The platform contract should keep Rust, TypeScript, Python, direct HTTP,
TraceQL/SQL-ish, and GraphQL surfaces aligned around the same behavior, errors,
result shapes, and explain/freshness semantics.

SQL compatibility is not implemented. Postgres wire compatibility is not the
goal. SQL-ish and GraphQL adapters must compile into the native TraceDB query
model instead of creating separate database semantics.

MCP is a first-class integration boundary for code-intelligence and Codex
workflows, but it is optional glue for the main product architecture. The
durable code-intelligence model is:

- source files are root records;
- symbols, AST nodes, chunks, embeddings, graph edges, edit plans, and reasoning
  artifacts are derived views;
- everything is epoched, snapshot-based, and explainable;
- edit application is approval-gated and blocked by stale file digests or stale
  snapshot epochs.

Useful future MCP surfaces include `tracedb_query`, `tracedb_plan_rename`,
`tracedb_apply_edit_plan`, and `tracedb_export_reasoning`.

## Research And Knowledge Boundaries

Technical source of truth belongs in the git-backed Obsidian vault and repo
docs, not Notion. Notion is for project management.

The repo-local docs must stay sufficient for project creation, tests, and
handoff stability. The wider vault keeps long-form technical memory, synthesis,
research corpus, benchmark ledgers, and cross-project context.

`/Users/zgrogan/Repos` is a container folder, not a repo. Do not version the
whole folder. Project repos and the private vault are separate boundaries.

`tracedb-forge` is a separate private dev-infra repo, not part of the TraceDB
product repo. It exists to import vault/repo knowledge, build context packets,
and run local Codex App Server based workflows. Do not fold TraceDB itself into
that infrastructure unless explicitly directed.

## Naming And Positioning Intent

Naming and positioning should keep serious infrastructure energy. Avoid fantasy
names, "AI mind" language, and overloaded generic terms like Trace, Vector,
Cortex, neural, graph, forge, memory, sync, or atlas unless there is a strong
reason.

If public naming changes, preserve the corrected framing: TraceDB is an
AI-native semantic database and memory substrate; TraceField is the broader
runtime/interface around that substrate for agents, developers, and tools.

## What To Protect In Future Work

- Keep full-system architecture separate from first-build execution plans.
- Prefer exact correctness and policy visibility before approximate retrieval.
- Require non-vacuous tests: prove candidates exist before asserting policy,
  visibility, or ranking behavior.
- Keep explanation/provenance/freshness data visible in query and benchmark
  artifacts.
- Treat external controls and numbers-to-beat as mandatory for performance
  claims.
- Preserve repo-local handoff docs even when the vault is canonical.
- Keep secrets and local auth state out of repo artifacts.
- Push coherent verified checkpoints during long-running loops instead of
  letting all progress sit locally.
