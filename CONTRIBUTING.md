# Contributing to TraceDB

Thank you for contributing to TraceDB. This document covers build/test
conventions, project architecture, and where your help is most valuable.

This repository is the downloadable TraceDB database distribution. It is not the
proprietary hosted TraceDB service, operator console, deployment control plane,
or production operations system.

## Code of Conduct

Be respectful, constructive, and professional. Disagreements are fine;
personal attacks, harassment, and exclusionary language are not. If you see
a problem, raise it directly or open an issue.

## Architecture Overview

TraceDB is a local/embedded transactional candidate-stream database written in
Rust. The engine uses epoch-based MVCC visibility, hybrid query fusion (lexical,
vector, graph, temporal), and a policy-based visibility oracle. SDKs are thin
ergonomic layers over the versioned HTTP API and live in sibling standalone
repositories: `../tracedb-rust`, `../tracedb-python`, and `../tracedb-js`.
The authoritative protocol contract lives in `../tracedb-protocol`; this core
repo keeps a validation mirror in `docs/platform-contract-v0.md`,
`docs/platform-contract-v0.json`, `docs/api/v1-http.md`, and
`docs/api/v1-openapi.json`.

For the full architecture breakdown, see
[docs/architecture/](docs/architecture/).

## Building and Testing

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

For a full local product regression:

```bash
cargo run -p tracedb-cli -- product-quickstart
```

## Pull Request Requirements

- All tests pass: `cargo test --workspace`
- Clippy is clean: `cargo clippy --workspace -- -D warnings`
- Code is formatted: `cargo fmt --check`
- New functionality includes tests
- Documentation updated for API changes

## Commit Message Conventions

- `feat:` — New feature
- `fix:` — Bug fix
- `docs:` — Documentation changes
- `style:` — Code style changes
- `refactor:` — Code refactoring
- `perf:` — Performance improvements
- `test:` — Adding or updating tests
- `chore:` — Build process or auxiliary tool changes

## Where to Contribute

### Protocol & Conformance

The protocol repo owns the source-of-truth HTTP contract. In core, the mirrored
contract and OpenAPI files define the engine/CLI/HTTP/direct-adapter validation
surface. Help is welcome extending core conformance coverage, adding new
scenario IDs in lockstep with `../tracedb-protocol`, or tightening the
verification ladder.

### Language Clients

Language clients are developed outside this core repo:

- **Rust SDK** — `../tracedb-rust`
- **Python SDK** — `../tracedb-python`
- **TypeScript/JavaScript SDK** — `../tracedb-js`
- **Go SDK** — not yet started; it should follow the same HTTP contract and
  conformance boundaries when added.

Core changes that affect SDKs should update `docs/platform-contract-v0.md`,
`docs/platform-contract-v0.json`, and `docs/api/v1-http.md` here, then update
the standalone SDK repos in separate changes.

### Documentation

- API reference completeness (`docs/api/v1-http.md`).
- Architecture deep-dives in `docs/architecture/`.
- Examples, tutorials, and the getting-started guide.

### Benchmarks

Active benchmark/proof harnesses live in `../tracedb-benchmarks`. Core may keep
historical benchmark notes and local diagnostic adapters, but exported
performance claims must come from the benchmark repo with an external control
and a number to beat.

### Engine & Storage

- Vector index implementation (deterministic segment-local HNSW is available; IVFFlat and tuning profiles remain future work).
- Async I/O (replacing raw `TcpListener` + `thread::spawn`).
- Binary storage format (currently JSON on the hot path).
- Read/write lock separation (`RwLock` over `Mutex`).

## License

TraceDB is licensed under the **Apache License, Version 2.0**. Contributions
are subject to the same license. See the repository root for the full license
text.
