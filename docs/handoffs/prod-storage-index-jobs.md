# prod-storage-index-jobs Handoff

Branch: `codex/prod-storage-index-jobs`
Base commit before implementation: `d37db04`

This branch is the storage/index/jobs foundation pass. It adds delta write planning, versioned binary segment/index artifacts, in-process text/vector/bitmap access-path artifacts, WAL-backed durable job events, worker lease endpoints, artifact vacuum, and the `storage-index-jobs` evidence harness.

## Evidence Artifacts

Local artifacts generated under `target/tracedb/`:

- `storage-index-jobs.json`
- `platform-conformance-all.json`
- `product-quickstart.json`
- `product-quickstart-encrypted.json`
- `prod-storage-index-jobs-evidence.md`

## Verified Commands

- `cargo fmt --all -- --check`
- `cargo test --workspace --all-targets`
- `(cd ../tracedb-js && npm run check)`
- `(cd ../tracedb-python && python3 -m unittest discover -s tests)`
- `(cd ../tracedb-benchmarks/benchmarks/realworld && python3 -m unittest tests.test_modal_bench tests.test_suite_gate)`
- `cargo run -p tracedb-cli -- product-quickstart --report-file target/tracedb/product-quickstart.json`
- `TRACEDB_MASTER_KEY_B64=... cargo run -p tracedb-cli -- product-quickstart --report-file target/tracedb/product-quickstart-encrypted.json`
- `python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --surface typescript_sdk --surface python_sdk --surface traceql_sqlish --surface graphql --summary-json target/tracedb/platform-conformance-all.json`
- `cargo run -p tracedb-cli -- storage-index-jobs --report-file target/tracedb/storage-index-jobs.json`

## Remaining Non-Goals

- Native GraphQL parity, TraceQL command expansion, and Railway managed gates remain later branches.
- `traceql_sqlish` and bounded `graphql` adapter lanes still report intentional `not_checked` scenarios in platform conformance.
- pgvector, Qdrant, and OpenSearch remain benchmark controls only; they are not TraceDB execution sidecars.
