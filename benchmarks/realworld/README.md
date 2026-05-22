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

## Agent Memory Flight Recorder Receipt

The local chat-memory demo emits an Agent Memory Flight Recorder receipt over
real TraceDB CLI commands:

```bash
cargo build -p tracedb-cli
python3 -m runner chat-demo \
  --output-json reports/chat-demo/latest.json \
  --output-md reports/chat-demo/latest.md
```

The JSON report includes `flight_recorder_receipt` with inserted record ids,
tenant scope, query/explain summary, replay command count, delete visibility
evidence, and explicit non-guarantees. This is a local TraceDB receipt demo, not
a TraceField runtime claim, tensor artifact claim, managed-cloud proof, legal
export/purge claim, or product benchmark win.

When local macOS executable policy blocks direct CLI execution, run the targeted
Modal product lane instead:

```bash
modal run scripts/modal_product_verify.py \
  --mode quickstart \
  --only agent_memory_flight_recorder
```

The Modal summary includes `flight_recorder_receipt_check`, which validates the
generated JSON receipt shape and the explicit non-guarantees before marking the
lane passed.

Use the local Rust launch doctor when a macOS checkout appears to hang before
the CLI reaches `main`:

```bash
python3 scripts/local_rust_launch_doctor.py \
  --summary-json /tmp/tracedb-local-rust-launch-doctor.json
```

The doctor reports `passed`, `blocked`, `failed`, or `missing`. A
`pre_main_launch_timeout` classification means local Rust binary execution is
blocked for that checkout; use Modal for runtime evidence until the machine
policy/toolchain issue is fixed.

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

## TraceDB Batch Write Attribution

Use this lane after a batch ingest external-control run shows a write gap. It
isolates in-process engine phases for one batch transaction, including store
clone/apply/install, WAL, manifest, and cache-clear timing. Compare this with
actual HTTP batch totals to estimate HTTP/server overhead before changing engine
semantics.

```bash
TRACEDB_BENCH_MODE=batch-write-attribution \
TRACEDB_BENCH_RECORD_TARGETS=1024,4096 \
TRACEDB_BENCH_BATCH_REPETITIONS=3 \
cargo run -p tracedb-bench > /tmp/tracedb-batch-write-attribution.json
```

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
`--target tracedb,pgvector,qdrant,milvus`, and TraceDB-specific API surface checks can be
selected with `--surface sdk,cli,http,curl`.

TraceDB HTTP ingest has two explicitly separate lanes:

- `--tracedb-ingest-mode per_record` (default): one durable HTTP `put` and one
  TraceDB WAL commit per record. This is the product durability lane and should
  not be compared directly to PostgreSQL or pgvector bulk transactions.
- `--tracedb-ingest-mode batch`: one HTTP `put-batch` request, one TraceDB epoch,
  and one WAL commit for all records. This is the fairer transaction-shape lane
  for pgvector/PostgreSQL controls that insert many rows before one `COMMIT`.

Reports keep both concepts visible with `ingest_transaction_count`,
`ingest_transaction_total_latency_ms`, `per_record_durable_transaction_count`,
and `batch_transaction_*` metrics. The control ledger now includes
`ingest_transaction_total_ms` as the batch/transaction number to beat.

## Suite Specs and Gate Artifact

Named suite specs live in `benchmarks/realworld/suites/`:

- `platform_pr.json` - fast 128-record PR/push lane, with 1000 records listed
  as the next scale step.
- `platform_push_10k.json` - 10k lane for Modal/Railway product readiness and
  core controls.
- `railway_stateful.json` - persistent TraceDB Railway volume/restart/redeploy
  lane.
- `release_100k.json` - release lane that requires external controls before
  `claim-ready`.
- `soak_railway.json` - scheduled repeated Railway-volume lane.
- `manual_1m.json` - explicit opt-in cliff-finding lane.

Run a spec locally with:

```bash
cd benchmarks/realworld
BENCH_DISABLE_ENV_FILE=1 python3 -m runner suite \
  --suite-spec suites/platform_pr.json \
  --openrouter-mode off \
  --target tracedb \
  --surface sdk \
  --scenarios sdk_cli_surface
```

`runner suite` always writes `suite-gate.json`. The gate statuses are `usable`,
`degraded`, `blocked`, and `claim-ready`. Release-style specs only become
`claim-ready` when required external controls produce a number to beat.
Pass `--suite-baseline-json <prior-suite.json>` to compare the current TraceDB
scenario metrics against a previous suite artifact. The gate records
`regressions` for p95 latency, transaction ingest, storage, and admin metrics
that exceed `--regression-tolerance-pct` or
`--regression-tolerance-absolute`. Specs with
`performance_regression_policy: rolling_regression_blocking` block on those
regressions; PR-oriented warning policies degrade instead of blocking. This
keeps performance gates tied to same-suite artifact deltas rather than a single
noisy run.
Pass `--suite-baseline-dir <reports-root>` when the runner should discover the
latest compatible prior `suite.json` automatically. Auto-selection only accepts
artifacts with the same `suite_spec`, `dataset`, and `records`, and skips the
current `suite_id`; the selected path and suite id are recorded in
`suite-gate.json` artifact metadata. If no compatible artifact exists, the run
continues without regression comparison so a first scheduled lane can seed the
history.

Unsupported SQL and GraphQL coverage is reported as explicit
`unsupported_coverage` in the gate. It must not be counted as passing behavior.

Modal presets map to the same specs:

```bash
modal run benchmarks/realworld/modal_bench.py \
  --suite-preset platform_pr \
  --run-id modal-platform-pr-<commit>
```

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

Railway stateful suite runs expect a dedicated Railway project/environment and
least-privilege credentials:

- `RAILWAY_API_TOKEN` or `RAILWAY_TOKEN`
- `RAILWAY_PROJECT_ID`
- `RAILWAY_ENVIRONMENT_ID`
- `TRACEDB_RAILWAY_SERVICE_ID`
- `TRACEDB_RAILWAY_PRIVATE_URL`
- Optional public fallback for local control-plane checks:
  `TRACEDB_RAILWAY_URL` or `RAILWAY_TRACEDB_URL`
- `TRACEDB_RAILWAY_VOLUME_PATH`, usually `/data/tracedb`
- `TRACEDB_RAILWAY_SNAPSHOT_ROOT` for snapshot/restore checks, an absolute
  server-side scratch path that must differ from the active volume/data path
- Optional control service IDs: `POSTGRES_RAILWAY_SERVICE_ID`,
  `PGVECTOR_RAILWAY_SERVICE_ID`, `MONGODB_RAILWAY_SERVICE_ID`,
  `QDRANT_RAILWAY_SERVICE_ID`, and `OPENSEARCH_RAILWAY_SERVICE_ID`

`railway_bench.py` validates this config, redacts tokens, and produces a
manifest for suite gates. Use `--railway-config-from-env` to write
`railway-manifest.json` beside `suite.json`, `suite.md`, and
`suite-gate.json`:

```bash
cd benchmarks/realworld
BENCH_DISABLE_ENV_FILE=1 python3 -m runner suite \
  --suite-spec suites/railway_stateful.json \
  --railway-config-from-env \
  --railway-health-check \
  --railway-stateful-smoke \
  --railway-snapshot-restore-check \
  --railway-verify-restored-marker \
  --railway-restart-redeploy-plan \
  --openrouter-mode off \
  --target tracedb \
  --surface sdk \
  --scenarios sdk_cli_surface
```

Missing Railway config blocks Railway-required specs. A configured manifest
makes the gate usable. `--railway-health-check` additionally probes the TraceDB
endpoint `/ready`, writes the result into `railway-manifest.json`, and blocks the
gate if the requested probe is unhealthy or unreachable. This is live endpoint
evidence only. `--railway-stateful-smoke` applies a small marker schema, writes
one marker record through `POST /v1/records/put`, reads it back through
`POST /v1/records/get`, records the result as `stateful_smoke`, and blocks the
gate if the requested marker is not visible. This proves live write/read
behavior for the current endpoint only: it still does not create services,
restart services, redeploy images, or prove volume survival across a restart.
`--railway-snapshot-restore-check` posts to `POST /v1/admin/snapshot` and
`POST /v1/admin/restore` using `TRACEDB_RAILWAY_SNAPSHOT_ROOT` or
`--railway-snapshot-root`, records `snapshot_restore` in
`railway-manifest.json`, feeds `railway_snapshot_restore` into
`suite-gate.json`, and blocks failed requested checks. With
`--railway-verify-restored-marker`, the restore request also includes the
stateful smoke marker as `verify_record`, records `snapshot_restore.restored_read`,
feeds `railway_restored_read` into `suite-gate.json`, and blocks if the restored
target cannot read that marker. The scratch root must be an explicit absolute
server-side path and must not equal
`TRACEDB_RAILWAY_VOLUME_PATH`; this prevents the harness from guessing remote
filesystem layout or writing snapshot targets directly into the active data
path. A passed check proves the HTTP admin snapshot/restore routes accepted the
requested paths and returned matching confirmations. A passed restored-read
check additionally proves the restored target can read the expected marker
through the admin restore response. It is still not managed backup/DR proof and
does not prove a live service has reopened the restored target.
After a manually executed Railway restart/redeploy, rerun the same suite with
`--railway-stateful-smoke --railway-stateful-read-only
--railway-stateful-marker-id <marker-id>` to read the original marker without
schema apply or `put`; this is the non-mutating post-operation visibility probe
used by the next persistence gate.
Add `--railway-persistence-pre-manifest-json <pre-manifest>` and
`--railway-operation-receipt-json <receipt>` to that postcheck to write a
`persistence_verdict` into `railway-manifest.json` and
`railway_persistence` into `suite-gate.json`. The verdict passes only when the
pre-manifest has a passed write/read marker, the postcheck has a passed read-only
marker with the same identity, and the receipt says a restart/redeploy actually
executed successfully. The receipt is intentionally strict so a loose JSON blob
cannot become persistence proof. It must include `kind:
railway_operation_receipt`, `operation: restart|redeploy`, `status`,
`executed: true`, `confirmed: true`, and the TraceDB Railway `service_id`; any
sensitive fields in the receipt are redacted before artifact export.
After the operator manually executes the Railway restart or redeploy, generate
that receipt with:

```bash
python3 -m runner railway-receipt \
  --operation restart \
  --status passed \
  --suite-id <suite-id> \
  --confirm-executed \
  --operator <operator-name> \
  --command "railway restart --service <tracedb-service-id>" \
  --output-json reports/railway-operation-receipt.json
```

The helper only writes the receipt artifact. It does not call Railway, restart
services, or redeploy code.
For specs that declare `railway.backup_required`, add
`--railway-backup-receipt-json <receipt>` to write `backup_verdict` into
`railway-manifest.json` and `railway_backup` into `suite-gate.json`. The backup
receipt is deliberately strict so a generic note cannot become backup/DR proof:
it must include `kind: railway_backup_receipt`, successful `status`,
`confirmed: true`, `backup_created: true`, `restore_validated: true`, the
TraceDB Railway `service_id`, a `backup_id`, and a non-empty
`restore_validation_method`. Generate the receipt after the operator validates
the managed backup and restore drill:

```bash
python3 -m runner railway-backup-receipt \
  --status passed \
  --suite-id <suite-id> \
  --backup-id <railway-backup-or-snapshot-id> \
  --confirm-created \
  --restore-validated \
  --restore-validation-method "restored marker smoke" \
  --operator <operator-name> \
  --output-json reports/railway-backup-receipt.json
```

The helper only writes the receipt artifact. It does not create Railway backups,
restore Railway volumes, or mutate services.
Generate the complete non-mutating operator command chain with
`railway-runbook` before scheduling a backup/restart/redeploy lane. It writes
JSON and Markdown artifacts with the suite preflight command, backup receipt
command when required, pre-operation marker command, manual Railway operation
hint, operation receipt command, post-operation read-only persistence check,
runbook verification command, and final verified suite command:

```bash
python3 -m runner railway-runbook \
  --suite-spec suites/soak_railway.json \
  --suite-id soak-railway-operator-check \
  --reports-dir reports \
  --runbook-verification-json reports/soak-railway-operator-check/railway-runbook-verification.json \
  --suite-baseline-dir reports \
  --output-json reports/soak-railway-operator-check/railway-runbook.json \
  --output-md reports/soak-railway-operator-check/railway-runbook.md
```

The runbook is an artifact plan only. It never creates backups, restarts
services, redeploys code, or proves persistence until the listed commands and
operator receipts are actually run.
After the listed commands run, verify the evidence chain without mutating
Railway:

```bash
python3 -m runner railway-runbook-verify \
  --runbook-json reports/soak-railway-operator-check/railway-runbook.json \
  --output-json reports/soak-railway-operator-check/railway-runbook-verification.json \
  --output-md reports/soak-railway-operator-check/railway-runbook-verification.md
```

The verifier reads existing `suite-gate.json`, receipt, and manifest artifacts,
then reports `complete` or `blocked` with `missing_steps`, `failed_steps`, and
`stale_steps`. It is proof-of-artifacts only: it never creates backups,
restarts services, redeploys code, or turns plan-only evidence into persistence
proof.
The generated runbook now includes this verification command plus a
`verified_suite_gate` command that consumes the verifier with
`--railway-runbook-verification-json`, requires it with
`--railway-require-runbook-verification`, and includes `--suite-baseline-dir`
when configured so the final lane compares against rolling suite history.
Feed a completed verification artifact into a suite gate with
`--railway-runbook-verification-json`. Add
`--railway-require-runbook-verification` when the lane must stop before child
scenario execution unless that artifact reports `status: complete`:

```bash
BENCH_DISABLE_ENV_FILE=1 python3 -m runner suite \
  --suite-spec suites/soak_railway.json \
  --run-id soak-railway-verified-preflight \
  --reports-dir reports \
  --railway-config-from-env \
  --railway-backup-receipt-json reports/soak-railway-operator-check/railway-backup-receipt.json \
  --railway-runbook-verification-json reports/soak-railway-operator-check/railway-runbook-verification.json \
  --railway-require-runbook-verification \
  --openrouter-mode off \
  --target tracedb \
  --surface sdk \
  --scenarios sdk_cli_surface
```

Use `--preflight-only` when the goal is to validate configured Railway evidence
before starting expensive scenarios. The command writes the normal
`suite.json`, `suite.md`, `suite-gate.json`, optional `railway-manifest.json`,
and optional `railway-artifacts.json`, but skips child scenario execution. For
backup-required lanes such as `soak_railway` and `release_100k`, a missing or
failed `--railway-backup-receipt-json` exits blocked before any benchmark child
run is created:

```bash
BENCH_DISABLE_ENV_FILE=1 python3 -m runner suite \
  --preflight-only \
  --suite-spec suites/soak_railway.json \
  --railway-config-from-env \
  --railway-backup-receipt-json reports/railway-backup-receipt.json \
  --openrouter-mode off \
  --target tracedb \
  --surface sdk \
  --scenarios sdk_cli_surface
```

`--railway-restart-redeploy-plan` adds a non-mutating `operation_plan` to the
manifest and reports `railway_restart_redeploy: plan_only` in the gate. The plan
lists safe preflight commands such as `railway status --json`,
`railway service status --all --json`, and service logs, plus guarded operator
hints for restart/redeploy. It does not execute Railway mutations and must not
be counted as persistence proof. Usage limits, SSH key setup,
restart/redeploy execution, and live service promotion of restored targets
remain required before the `railway_stateful`, `soak_railway`, or
`release_100k` specs should be treated as full Railway product proof.

The Modal `railway_stateful`, `soak_railway`, and `release_100k` presets pass
`--railway-config-from-env`, `--railway-health-check`, and
`--railway-stateful-smoke`, `--railway-snapshot-restore-check`, and
`--railway-verify-restored-marker`, and `--railway-restart-redeploy-plan`, so
scheduled or remote Railway lanes fail fast when the persistent lab is not
reachable, cannot accept a marker write/read, cannot execute the admin
snapshot/restore route check, or cannot read the marker from the restored target
while also carrying an explicit operator plan for the next persistence gate.
`soak_railway` and `release_100k` also require a passed backup receipt; without
`--railway-backup-receipt-json`, their gates stay blocked on
`railway_backup: not_checked`. Modal also accepts `--suite-preflight-only` and
passes it through to the runner as `--preflight-only`; use it when the receipt
artifact path is available inside the Modal container and you want the scheduled
job to validate evidence artifacts before launching the heavy suite work.
The `soak_railway` and `release_100k` Modal presets now also pass
`--railway-require-runbook-verification`; provide
`--railway-runbook-verification-json` or those lanes block before child scenario
execution. For Modal remote runs, local evidence inputs passed through
`--suite-baseline-json`,
`--railway-persistence-pre-manifest-json`, `--railway-operation-receipt-json`,
`--railway-backup-receipt-json`, and `--railway-runbook-verification-json` are
copied into `benchmarks/realworld/.modal-input-artifacts/<run-id>/` before the
remote call and the container receives the mounted `/workspace/TraceDB/...`
path. This keeps generated reports excluded from the Modal source mount while
still letting scheduled soak/release jobs consume explicit operator evidence.
Modal also accepts `--suite-baseline-dir`: the local entrypoint resolves the
latest compatible prior suite artifact before staging, then sends the remote
container an explicit mounted `--suite-baseline-json` path. This makes scheduled
push/soak/release lanes use rolling history without mounting the whole reports
tree into Modal.
The staging directory is ignored by git; it is transport for evidence artifacts,
not new proof by itself.
When a Railway manifest is present, `runner suite` also writes
`railway-artifacts.json` and records it in `suite-gate.json` as
`artifact_paths.railway_artifacts_json`. That file indexes the suite artifacts
by relative path, existence, size, and SHA-256 without copying artifact contents,
so Modal bundles carry a compact review manifest for Railway evidence. It is not
backup or live recovery proof; when snapshot/restore passes, the artifact
manifest still marks that proof as admin-route-only rather than managed
backup/DR. When restored-read verification is not requested or does not pass,
the artifact manifest keeps `restored_read_not_checked` as an open proof gap.
When backup evidence is missing or failed, it keeps
`backup_validation_not_checked` as an open proof gap.

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
  --summary-json /tmp/tracedb-modal-summaries/modal-remote-smoke-16.json \
  --bundle-output /tmp/tracedb-modal-bundles/modal-remote-smoke-16.exported.tar.gz
```

TraceDB actual-engine batch lane against pgvector control:

```bash
TRACEDB_MODAL_IMAGE_KIND=tracedb_pgvector \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-tracedb-pgvector-batch-r1024-a \
  --records 1024 \
  --allow-large \
  --target tracedb,pgvector \
  --surface http \
  --scenarios search_rag_6 \
  --openrouter-mode off \
  --tracedb-ingest-mode batch \
  --allow-external-controls \
  --require-services \
  --tracedb-engine-control \
  --pgvector-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-tracedb-pgvector-batch-r1024-a.json
```

Local dry run without Modal:

```bash
python3 benchmarks/realworld/modal_bench.py \
  --run-id modal-local-smoke \
  --records 16 \
  --seed 42 \
  --summary-json /tmp/tracedb-modal-summaries/modal-local-smoke.json \
  --bundle-output /tmp/tracedb-modal-bundles/modal-local-smoke.exported.tar.gz \
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

MongoDB external-control smoke on Modal:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-mongodb-smoke-a \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-mongodb-smoke-a \
  --records 128 \
  --seed 42 \
  --target mongodb \
  --surface sdk \
  --scenarios search_rag_6 \
  --allow-external-controls \
  --require-services \
  --mongodb-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-mongodb-smoke-a.json
```

MongoDB reports both footprint and dbStats storage surfaces. `disk_bytes`,
`disk_bytes_after_ingest`, and `disk_bytes_after_workload` keep the existing
data-dir footprint behavior when `BENCH_MONGO_STORAGE_DIR` is available, with a
fallback to dbStats `storageSize`/`dataSize`. The raw dbStats fields are exported
as `mongodb_dbstats_data_size_bytes`,
`mongodb_dbstats_storage_size_bytes`, `mongodb_dbstats_index_size_bytes`, and
`mongodb_dbstats_total_size_bytes`. Treat `storageSize` as allocation and
`disk_bytes` as data-dir footprint, not logical payload size.

Milvus Lite external-control smoke on Modal:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-milvus-smoke-a \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-milvus-smoke-a \
  --records 128 \
  --seed 42 \
  --target milvus \
  --surface sdk \
  --scenarios search_rag_6 \
  --allow-external-controls \
  --require-services \
  --milvus-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-milvus-smoke-a.json
```

The first Milvus lane uses Milvus Lite through `pymilvus` and a local
`BENCH_MILVUS_URI` file. It is an embedded vector-control smoke, not a
standalone or distributed Milvus product benchmark. `disk_bytes` measures the
local Lite DB file/directory, not a server-reported Milvus storage metric.

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

Replicated TraceDB actual-engine batch plus pgvector control run:

```bash
TRACEDB_MODAL_APP_NAME=tracedb-batch-pgvector-1024-a \
TRACEDB_MODAL_IMAGE_KIND=tracedb_pgvector \
modal run benchmarks/realworld/modal_bench.py \
  --run-id modal-tracedb-pgvector-batch-<commit>-r1024-a \
  --records 1024 \
  --allow-large \
  --seed 42 \
  --target tracedb,pgvector \
  --surface http \
  --scenarios search_rag_6 \
  --tracedb-ingest-mode batch \
  --allow-external-controls \
  --require-services \
  --tracedb-engine-control \
  --pgvector-control \
  --summary-json /tmp/tracedb-modal-summaries/modal-tracedb-pgvector-batch-<commit>-r1024-a.json
```

Use separate `TRACEDB_MODAL_APP_NAME` values and repeat suffixes for variance
runs. Treat `query_latency_*`, `ingest_latency_*`, `disk_bytes`,
`disk_bytes_after_workload`, `freshness_query_*`, and admin split metrics as
separate KPI surfaces. pgvector ingest is per-row insert inside one bulk
transaction; TraceDB per-record durable HTTP write remains the default pressure
lane, while `--tracedb-ingest-mode batch` measures one TraceDB batch
transaction. Use TraceDB `batch_transaction_total_latency_ms` against pgvector
`ingest_transaction_total_latency_ms` for the fair transaction-total ingest
comparison.

Current closeout checkpoint:

- `88c9223 bench: split store apply write timing` adds store-apply subphase
  attribution for `validate_identity`, `validate_vector`, `key`, `fields`,
  `finalize_identity`, `features`, and `install`.
- Three clean 1024-record Modal TraceDB+pgvector repeats with
  `TRACEDB_MODAL_IMAGE_KIND=tracedb_pgvector`, `--surface http`,
  `--tracedb-ingest-mode batch`, `--tracedb-engine-control`, and
  `--pgvector-control` preserved source_dirty=false, verified exported bundles,
  and `control_status=external_control_available`.
- The checkpoint is development evidence, not a performance win: pgvector
  remained faster on median query p95 (`1.348 ms` vs TraceDB `2.355 ms`),
  faster on transaction ingest (`184.992 ms` vs TraceDB `216.380 ms`), and
  smaller on storage (`335872 B` vs TraceDB after-ingest `495401 B`).
  Generated-label quality tied at `0.233 / 0.375 / 1.000`.
- TraceDB median Modal store_apply was `138.739 ms`, mostly features
  `47.314 ms`, install `46.981 ms`, fields `26.969 ms`, and key `16.284 ms`.
  Future optimization work should isolate those families with same-machine
  parent/branch controls before another Modal claim.

Replicated CodeSearchNet actual-engine triple-control run:

```bash
for r in a b c; do
  TRACEDB_MODAL_APP_NAME="tracedb-controls-codesearch-<commit>-r1000-${r}" \
  modal run benchmarks/realworld/modal_bench.py \
    --run-id "modal-tracedb-controls-codesearch-<commit>-r1000-${r}" \
    --dataset codesearchnet_codeaware \
    --records 1000 \
    --seed 42 \
    --target tracedb,opensearch,pgvector \
    --surface http \
    --scenarios search_rag_6 \
    --openrouter-mode off \
    --tracedb-ingest-mode batch \
    --allow-external-controls \
    --require-services \
    --tracedb-engine-control \
    --opensearch-control \
    --pgvector-control \
    --summary-json "/tmp/tracedb-modal-summaries/modal-tracedb-controls-codesearch-<commit>-r1000-${r}.json" \
    --bundle-output "/tmp/tracedb-modal-bundles/modal-tracedb-controls-codesearch-<commit>-r1000-${r}.exported.tar.gz"
done
```

This command relies on automatic image-family selection. With TraceDB plus more
than one external control, the wrapper should select `modal_image_kind =
tracedb_controls`; do not override `TRACEDB_MODAL_IMAGE_KIND` unless you are
intentionally debugging image selection. A valid external-qrels run must report
`control_status=external_control_available`, `failure_count=0`,
`source_dirty=false`, `relevance_label_mode=external_qrels`, distinct Modal app
names, and no unavailable controls. Treat the local `--summary-json` files as
the durable aggregate evidence surface unless the remote Modal bundle tarballs
are explicitly persisted with `--bundle-output`. Current summaries also carry
per-baseline `query_results` with query IDs, expected IDs, top-k actual IDs,
exact recall, same-file recall, nDCG, and MRR for adapters that expose query
result lists.

Reports are bundled into one `tar.gz` containing `suite.json`, `suite.md`,
`suite-gate.json`, optional `railway-manifest.json`, optional
`railway-artifacts.json`, and `manifest.json`. The
manifest records the run config, seed, Modal app name, resource class, redacted
benchmark environment, and git commit/dirty state. Use `--summary-json` for
clean local per-run evidence instead of scraping Modal logs, and use
`--bundle-output` when the full tarball must survive the remote Modal container.
The saved summary records `exported_bundle_path` and `exported_bundle_sha256`;
transient returned bundle bytes are stripped before summary JSON is written.
`--bundle-output` is guarded by `--bundle-export-max-mb` (default `64`) because
this path returns the bundle through the Modal function result; use a durable
object store or Modal Volume for larger archives. By default this lane reports
`control_status=internal_only_smoke`; it is development evidence, not a product
benchmark claim.

## Reports

Reports are written to `reports/` by default:

- `reports/latest.json`
- `reports/latest.md`

Generated report files are intentionally ignored by git, while the runner and
workload definitions are tracked.
