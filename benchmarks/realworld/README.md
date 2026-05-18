# TraceDB Real-World Benchmark Lab

This lab compares TraceDB against real database shapes instead of checking only
that the project compiles. The first baseline set is Search/RAG 6:

- TraceDB
- PostgreSQL
- PostgreSQL + pgvector
- MongoDB
- Qdrant
- OpenSearch

The CI-safe path uses deterministic generated data and does not require network
downloads. Larger local runs can opt into pinned Hugging Face datasets such as
`MongoDB/embedded_movies`, BEIR/SciFact, and CodeSearchNet retrieval.

CodeSearchNet has two explicit local lanes:

- `codesearchnet_body`: body-only external-qrels retrieval, matching the plain
  source-text baseline.
- `codesearchnet_codeaware`: benchmark-only code-aware lexical materialization
  that indexes normalized record id/path, title, and source body terms. This is
  for measuring code-retrieval modeling effects before changing the canonical
  TraceDB tokenizer.

## Smoke Run

```bash
python3 -m runner run --profile smoke --dataset generated --records 1000 --openrouter-mode off
```

By default, unavailable competitor services are reported as unavailable instead
of making the run fail. Use `--require-services` when a Compose stack is expected
to be running and unavailable baselines should fail the run.

## OpenRouter Scientific Runs

OpenRouter is automatically used when `OPENROUTER_API_KEY` is configured. For
local use, create `benchmarks/realworld/.env.local` from `.env.local.example`.
That file is ignored by git.

```bash
python3 -m runner doctor openrouter
python3 -m runner run \
  --profile smoke \
  --dataset generated \
  --records 1000 \
  --target tracedb \
  --surface sdk,cli,http,curl \
  --openrouter-mode required \
  --openrouter-cap moderate
```

Defaults:

- Embeddings: `qwen/qwen3-embedding-8b`
- Benchmark embedding dimensions: `1536`
- Comparison embeddings: `perplexity/pplx-embed-v1-0.6b`
- Judge/diagnostic model: `openrouter/owl-alpha`
- Reranker: `cohere/rerank-4-fast`

Provider models may return larger native vectors. The runner caps benchmark
vectors to 1,536 dimensions by default and records both native and used
dimensions in reports. Override with `--embedding-dimensions <n>` or
`OPENROUTER_EMBED_DIMENSIONS=<n>`. Use `0`, `native`, or `auto` only when you
intentionally want provider-native dimensions.

Provider-backed runs write a scientific artifact bundle under
`reports/<run_id>/`:

- `manifest.json` records the hypothesis, seed, dataset digest, model IDs, caps,
  surfaces, service URLs, and adapter version labels.
- `observations.jsonl` records raw observations such as OpenRouter batches,
  retries, adapter starts/completions, and TraceDB explain summaries.
- `summary.json`, `report.md`, and `failures.md` provide the stable report
  surfaces.

Reports include a `Simulated Scenarios` section that names what the benchmark is
actually exercising: tenant-filtered semantic retrieval, mixed document and
relational data shapes, SDK/CLI/API surface coverage, OpenRouter embedding
provider behavior, retrieve-then-rerank RAG precision, and TraceDB HTTP
falsification checks when the HTTP surface is selected.

Use `--openrouter-mode off` for fully offline CI-safe runs, `auto` to use a key
when present, and `required` when missing or unhealthy provider access should
fail the run.

## Loop/Falsification Run

```bash
python3 -m runner loop \
  --profile local \
  --dataset generated \
  --records 1000 \
  --iterations 20 \
  --target tracedb \
  --surface sdk,cli,http,curl \
  --stop-on-failure
```

Loop mode varies the seed per iteration, reuses cached embeddings, and writes a
minimized `failure-iteration-<n>.json` case on the first invariant failure.

## TraceDB CLI Open/Recovery Scaling

Use this lane when Docker or external baselines are unavailable, or when the
question is specifically local WAL replay/open-time pressure. It drives the
real `tracedb` CLI against a fresh data directory and records write, reopen, and
query p95 at multiple record counts.

```bash
cargo build -p tracedb-cli
python3 -m runner tracedb-scaling \
  --records 128,256,512 \
  --data-dir /tmp/tracedb-cli-scaling-db \
  --output-json /tmp/tracedb-cli-scaling/summary.json \
  --output-md /tmp/tracedb-cli-scaling/report.md
```

The numbers include process startup plus `TraceDb::open` WAL replay. Use the
HTTP falsification lane for in-process server query/write latency.

## TraceDB In-Process Engine Scaling

Use this lane when the CLI scaling curve is too coarse and you need to separate
process startup from engine costs. It runs inserts, opens, checkpoints, and
queries inside the `tracedb-bench` process and writes one JSON report to stdout.

```bash
TRACEDB_BENCH_MODE=inprocess-scaling \
TRACEDB_BENCH_RECORD_TARGETS=1024,2048,4096 \
TRACEDB_BENCH_OPEN_REPETITIONS=5 \
TRACEDB_BENCH_QUERY_REPETITIONS=3 \
TRACEDB_BENCH_CHECKPOINT_AT_POINTS=1 \
cargo run -p tracedb-bench > /tmp/tracedb-inprocess-scaling.json
```

Use this before changing checkpoint layout or store internals. It captures
engine insert p95, engine open p95, engine query p95, checkpoint latency, and
checkpointed open/query p95 without paying a new process startup per operation.
Write phase timings include lock acquisition, stale-refresh checks, WAL append,
store clone/install, and manifest write phases so write-path changes can be
attributed before they are promoted into optimization claims.

Compare candidate scaling reports against same-machine parent reports before
accepting write-path or storage-layout changes:

```bash
python3 -m runner tracedb-scaling-compare \
  --baseline-json /tmp/baseline-r1.json /tmp/baseline-r2.json \
  --candidate-json /tmp/candidate-r1.json /tmp/candidate-r2.json \
  --baseline-label main-before-change \
  --candidate-label candidate-branch \
  --output-json /tmp/tracedb-scaling-compare/comparison.json \
  --output-md /tmp/tracedb-scaling-compare/comparison.md
```

The default guard requires at least two reports per side/record target, matching
record targets, a 25% recent-write p95 improvement, and no hot/checkpoint query
p95 regression beyond `max(10%, 5ms)`. The comparison artifact is internal
development evidence; exported benchmark claims still need an external control
and number to beat.

Comparison reports also include a Phase Headroom section when recent write phase
timings are present. That section estimates whether removing manifest write cost
could clear the write gate, but it is sizing evidence only; any runtime change
still needs a fresh candidate benchmark and recovery tests.

## Local Compose Lab

```bash
docker compose -f benchmarks/realworld/docker-compose.yml --profile lab up -d
python3 -m runner run --profile local --dataset generated --records 10000 --require-services
```

The Compose runner reads `OPENROUTER_*` values from the caller environment, and
the Python runner also loads the mounted `.env.local` file. Secrets are never
baked into images or committed YAML.

The runner accepts a comma-separated `--target` list such as
`--target tracedb,pgvector,qdrant`, and TraceDB-specific API surface checks can be
selected with `--surface sdk,cli,http,curl`.

## Railway-Targeted Run

When local disk is constrained, deploy TraceDB to Railway and keep this Mac as
the benchmark control plane:

```bash
RAILWAY_TRACEDB_URL=https://<railway-domain> \
TRACEDB_HTTP_BEARER_TOKEN=<optional-gateway-token> \
OPENROUTER_MODE=required \
RECORDS=1000 \
benchmarks/realworld/scripts/run_railway_target.sh
```

This defaults to the TraceDB HTTP falsification scenario against the remote URL.
Use `SCENARIOS=all TARGET=all` only when the competitor services are also
reachable through the `BENCH_*` environment variables.

## Modal CPU/RAM Smoke

Use this lane to verify that the benchmark suite can run on Modal and return a
bundled report artifact before starting large-scale or external-control runs.
The default is intentionally small, CPU/RAM only, OpenRouter off, no GPU, and
TraceDB-only SDK surface:

```bash
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-remote-smoke-16 \
  --records 16 \
  --seed 42 \
  --summary-json /tmp/tracedb-modal-summaries/modal-remote-smoke-16.json
```

Local dry run without Modal:

```bash
python3 benchmarks/realworld/modal_bench.py \
  --run-id modal-local-smoke \
  --records 16 \
  --seed 42 \
  --summary-json /tmp/tracedb-modal-summaries/modal-local-smoke.json \
  --min-free-mb 1000
```

PostgreSQL external-control smoke on Modal:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-postgres-smoke-a \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-postgres-smoke-a \
  --records 128 \
  --seed 42 \
  --target tracedb,postgres \
  --surface sdk \
  --scenarios search_rag_6 \
  --allow-external-controls \
  --require-services \
  --postgres-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-postgres-smoke-a.json
```

pgvector external-control smoke on Modal:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-pgvector-smoke-a \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-pgvector-smoke-a \
  --records 128 \
  --seed 42 \
  --target pgvector \
  --surface sdk \
  --scenarios search_rag_6 \
  --allow-external-controls \
  --require-services \
  --pgvector-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-pgvector-smoke-a.json
```

TraceDB actual-engine HTTP smoke on Modal:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-engine-http-smoke-a \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-tracedb-engine-http-a \
  --records 128 \
  --seed 42 \
  --target tracedb \
  --surface http \
  --scenarios search_rag_6 \
  --tracedb-engine-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-tracedb-engine-http-a.json
```

Use `--tracedb-engine-control` when TraceDB needs to be measured through the
real server process instead of SDK request-builder smoke. The Modal image builds
the release `tracedb-server` binary, starts it on loopback, sets
`TRACEDB_HTTP_URL`, and records `TRACEDB_HTTP_DATA_DIR` so HTTP runs can report
actual data-directory bytes. The Modal wrapper selects one image family per run
from the requested flags, so a TraceDB-only smoke does not build pgvector and a
side-by-side run can still use a combined TraceDB+pgvector image. Side-by-side
pgvector comparisons should use this flag together with `--pgvector-control`;
otherwise TraceDB results remain development evidence rather than an exported
product benchmark claim.

Reports are bundled into one `tar.gz` containing `suite.json`, `suite.md`, and
`manifest.json`. The manifest records the run config, seed, Modal app name,
resource class, redacted benchmark environment, and git commit/dirty state. Use
`--summary-json` for clean local per-run evidence instead of scraping Modal logs.
By default this lane reports `control_status=internal_only_smoke`; it is
development evidence, not a product benchmark claim.

## Reports

Reports are written to `reports/` by default:

- `reports/latest.json`
- `reports/latest.md`

Generated report files are intentionally ignored by git, while the runner and
workload definitions are tracked.
