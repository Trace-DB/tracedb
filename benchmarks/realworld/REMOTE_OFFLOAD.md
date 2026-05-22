# TraceDB Benchmark Offload Runbook

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

The real-world benchmark lab can exceed a small development machine very quickly:
OpenSearch, MongoDB, Qdrant, two Postgres instances, TraceDB WAL/segments, release
build artifacts, and 4,096-dimensional provider embeddings all compete for disk.
When free disk drops below several GB, benchmark results stop measuring TraceDB
and start measuring the host.

Use this runbook for the full scientific suite. Keep the Mac path for code edits,
unit tests, and small smoke runs.

## Recommended Host

Minimum practical host:

- Linux x86_64 or arm64, Ubuntu 22.04/24.04 preferred
- 4 vCPU
- 16 GB RAM
- 100 GB SSD/NVMe
- Docker Engine with Compose plugin
- Python 3.11+
- Rust stable toolchain

Preferred benchmark host:

- 8 vCPU
- 32 GB RAM
- 200 GB SSD/NVMe
- Swap enabled
- Unmetered or generous egress if pulling external datasets

The suite does not need a GPU for the current generated/OpenRouter path.

## Scientific Flow

Each run should answer a specific question:

- Hypothesis: TraceDB can behave as a transactional candidate-stream database
  for record, lexical, vector, document-shaped, tenant-filtered, and
  API-surface workloads.
- Controlled variable: one dataset digest, one seed, one record count, one model
  configuration.
- Baselines: TraceDB, PostgreSQL, PostgreSQL+pgvector, MongoDB, Qdrant,
  OpenSearch, and Milvus Lite where the Modal/image family supports it.
- Raw evidence: `manifest.json`, `observations.jsonl`, `summary.json`, `report.md`,
  and `failures.md`.
- Interpretation: the suite-level `suite.md` explains scenarios, database roles,
  metrics, unavailable baselines, provider/rerank evidence, and scenario findings.

Do not treat a run as valid if the report records disk-full, killed process,
unhealthy Docker daemon, or interrupted TraceDB service failures.

## Scenario Coverage

The aggregate suite currently runs:

- `sdk_cli_surface`: TraceDB embedded-style request builder plus CLI schema/put/get.
- `http_falsification`: TraceDB HTTP/curl API correctness checks for fresh writes,
  patch visibility, tenant isolation, strict/lazy/allow-dirty freshness requests,
  explain fields, compaction, snapshot, restore, and tombstone hiding.
- `search_rag_6`: side-by-side database comparison across TraceDB, PostgreSQL,
  pgvector, MongoDB, Qdrant, OpenSearch, and Milvus Lite-capable lanes on the
  same tenant-filtered RAG corpus.

Provider-backed runs add OpenRouter embedding metadata and
`cohere/rerank-4-fast` retrieve-then-rerank metrics.

## Ingest Semantics

Do not collapse all ingest numbers into one claim. TraceDB exposes two HTTP
ingest modes:

- `per_record` (default): one durable TraceDB commit per record.
- `batch`: one `put-batch` request, one epoch, and one WAL commit for the whole
  dataset load.

PostgreSQL and pgvector controls in this suite currently use one bulk
transaction. Treat `ingest_transaction_total_latency_ms` as the transaction-shape
comparison and keep `ingest_latency_p95_ms` as the per-operation timing inside
each adapter's chosen ingest mode.

Current closeout checkpoint: `88c9223` has three clean 1024-record Modal
TraceDB+pgvector batch repeats with source_dirty=false, verified exported
bundles, and `control_status=external_control_available`. The checkpoint is
development evidence, not a product win: pgvector still beats TraceDB on median
query p95, transaction ingest, and storage while generated quality ties. The
next write-path target is store_apply features/install/fields and key
construction, not WAL or manifest.

TraceField runtime work, Agent Memory Flight Recorder, and tensor artifacts are
future research/demo or governed-module directions. They are not part of the
current benchmark product claim.

## Move the Repo to the Host

If the repo has a remote:

```bash
git clone <repo-url> TraceDB
cd TraceDB
```

If not, create a secret-free bundle from the Mac:

```bash
benchmarks/realworld/scripts/make_offload_bundle.sh
scp /tmp/tracedb-offload-*.tar.gz user@host:/tmp/
ssh user@host
mkdir -p ~/TraceDB
tar -xzf /tmp/tracedb-offload-*.tar.gz -C ~/TraceDB
cd ~/TraceDB
```

The bundle script excludes `target/`, benchmark caches, reports, `.env`, and
`.env.local`.

## Configure Secrets

On the remote host:

```bash
cd benchmarks/realworld
cp .env.local.example .env.local
$EDITOR .env.local
```

Set at least:

```bash
OPENROUTER_API_KEY=...
OPENROUTER_EMBED_MODEL=qwen/qwen3-embedding-8b
OPENROUTER_EMBED_DIMENSIONS=1536
OPENROUTER_COMPARE_EMBED_MODELS=perplexity/pplx-embed-v1-0.6b
OPENROUTER_JUDGE_MODEL=openrouter/owl-alpha
OPENROUTER_RERANK_MODEL=cohere/rerank-4-fast
```

The benchmark lab rejects requested dimensions above 2,048 and defaults to
1,536. That keeps WAL payloads, HTTP bodies, vector indexes, and competitor
adapter memory in a sane alpha range while still matching common production
embedding sizes.

## Run the Full Suite

From the repo root:

```bash
benchmarks/realworld/scripts/run_remote_suite.sh
```

Useful overrides:

```bash
RECORDS=1000 RUN_ID=remote-generated-1000 benchmarks/realworld/scripts/run_remote_suite.sh
RECORDS=10000 OPENROUTER_CAP=moderate RUN_ID=remote-generated-10000 benchmarks/realworld/scripts/run_remote_suite.sh
OPENROUTER_MODE=off RECORDS=1000 RUN_ID=offline-control benchmarks/realworld/scripts/run_remote_suite.sh
```

The script:

1. Refuses to run if free disk is below `MIN_FREE_MB` (default `20000`).
2. Builds the release TraceDB binary.
3. Creates/updates a Python venv for the benchmark runner.
4. Starts the Search/RAG 6 Compose services.
5. Starts a release TraceDB engine on a local port.
6. Runs `python -m runner suite`.
7. Writes reports under `benchmarks/realworld/reports/<run_id>/`.
8. Creates `benchmarks/realworld/report-bundles/<run_id>.tar.gz`.
9. Stops services unless `KEEP_SERVICES=1`.

## Bring Reports Back

```bash
scp user@host:~/TraceDB/benchmarks/realworld/report-bundles/<run_id>.tar.gz ./benchmarks/realworld/report-bundles/
mkdir -p benchmarks/realworld/reports/imported-<run_id>
tar -xzf benchmarks/realworld/report-bundles/<run_id>.tar.gz -C benchmarks/realworld/reports/imported-<run_id>
```

Read the aggregate report first:

```bash
open benchmarks/realworld/reports/imported-<run_id>/<run_id>/suite.md
```

## Mac-Safe Commands

Use these locally when disk is tight:

```bash
python3 -m runner run --profile smoke --dataset generated --records 128 --target tracedb --surface sdk,cli --openrouter-mode off
python3 -m runner doctor openrouter --openrouter-mode off
docker compose -f benchmarks/realworld/docker-compose.yml --profile lab config
```

Avoid full Compose baselines, release rebuilds, and compaction-heavy HTTP runs on
the Mac until free disk is comfortably above 20 GB.
