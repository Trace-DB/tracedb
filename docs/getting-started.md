# Getting Started

This guide takes you from a fresh checkout to your first local TraceDB query.

## Prerequisites

- Rust toolchain from [rustup](https://rustup.rs/)
- Python 3.11+ for validation scripts
- Node 22+ only if you are working in the sibling TypeScript SDK repo

## Build the Core Repo

```bash
git clone https://github.com/Trace-DB/tracedb.git
cd tracedb
cargo build --workspace
```

Useful local checks:

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

## Run the Embedded Demo

The CLI demo exercises schema apply, batch ingest, scan/query/explain, delete,
compact, and snapshot/restore against a local data directory.

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

For the broader local product gate:

```bash
cargo run -p tracedb-cli -- product-quickstart
```

## Start the HTTP Server

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-http-demo/data \
TRACEDB_BIND=127.0.0.1:8090 \
TRACEDB_SERVICE_MODE=engine \
cargo run -p tracedb-server
```

In another terminal, set the endpoint:

```bash
export TRACEDB_URL=http://127.0.0.1:8090
export TRACEDB_TOKEN=dev-token
```

## First HTTP Query

```bash
curl -sS -H "Authorization: Bearer $TRACEDB_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$TRACEDB_URL/v1/schema/apply" \
  -d '{"name":"docs","primary_id_column":"id","tenant_id_column":"tenant","scalar_columns":["status"],"text_indexed_columns":["body"],"vector_columns":[{"name":"embedding","dimensions":3,"source_columns":["body"]}]}'

curl -sS -H "Authorization: Bearer $TRACEDB_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: getting-started-put-intro" \
  -X POST "$TRACEDB_URL/v1/records/put" \
  -d '{"table":"docs","id":"intro","tenant_id":"tenant-a","fields":{"id":"intro","tenant":"tenant-a","body":"hello TraceDB","embedding":[1,0,0],"status":"published"}}'

curl -sS -H "Authorization: Bearer $TRACEDB_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$TRACEDB_URL/v1/query" \
  -d '{"table":"docs","tenant_id":"tenant-a","text_field":"body","text":"TraceDB","vector_field":"embedding","vector":[1,0,0],"top_k":5,"freshness":"Strict","explain":true}'
```

## SDK Repos

Python, TypeScript/JavaScript, and Rust SDK quickstarts are maintained in the
sibling standalone repos `../tracedb-python`, `../tracedb-js`, and
`../tracedb-rust`. This core repo owns the HTTP contract mirror those SDKs test
against.

## Run Conformance Lanes

```bash
python3 scripts/platform_conformance.py --surface http_direct \
  --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/validate_protocol_locks.py --repo-root ..
```

Additional core surfaces include `traceql`, `traceql_sqlish`, and `graphql`.
SDK conformance surfaces are run from the sibling SDK repositories.

## Where Next

- [Documentation map](README.md)
- [Platform contract v0](platform-contract-v0.md)
- [HTTP API reference](api/v1-http.md)
- [Kernel and module architecture](architecture/kernel-and-modules.md)
- [Durability semantics](durability-semantics-v0.md)
- [Contributing guide](../CONTRIBUTING.md)
